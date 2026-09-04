//! Pure retry/backoff decisions (SRS §23.1, §25.1, ADR 0009).

use std::hash::{BuildHasher, Hasher};
use std::time::Duration;

use crate::error::ConfigError;
use crate::publisher::FailureKind;
use crate::store::{DeadReason, FailureOutcome};

/// Decides what happens after a publish failure. **Pure**: I/O-free and clock-free — it returns
/// a [`Duration`], never a timestamp. The store applies it as `available_at = now() + delay` in
/// SQL (ADR 0009), so a worker with a skewed clock can neither hot-loop a row nor park it in the
/// future.
///
/// `attempts` is the count **before** this outcome; it increments on outcome, not on claim, so
/// the budget counts observed *publish* failures, not claims.
pub trait RetryPolicy: Send + Sync {
    /// Decides the next step for a failure observed at `attempts` (the count before this
    /// outcome) classified as `kind`.
    fn next(&self, attempts: u32, kind: FailureKind) -> FailureOutcome;
}

/// The house [`RetryPolicy`]: exponential backoff with a cap, a jittered delay, and a bounded
/// number of attempts.
///
/// Rules: [`FailureKind::Permanent`] always returns
/// [`DeadReason::PermanentError`](FailureOutcome::Dead), whatever `attempts` is;
/// `attempts + 1 >= max_attempts` returns [`DeadReason::AttemptsExhausted`]; otherwise
/// `delay = min(max_delay, base × 2^attempts) × jitter_factor`, monotonic in `attempts` without
/// jitter, capped at `max_delay × (1 + jitter)`, and never zero (given `base > 0`).
///
/// Fields are `pub` for readability, but the values are validated by [`Self::validate`], called
/// from the dispatcher builder rather than a constructor — a struct a host mutates directly is
/// checked where it is used, not where it is created.
///
/// `PartialEq` lets `OutboxDispatcherBuilder::build` detect whether a host left this at its
/// default while also supplying a custom `RetryPolicy` — a conflict it rejects rather than
/// silently ignore one of the two (`ConfigError::RetryPolicyConflict`, §3.6).
#[derive(Clone, Copy, Debug, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[non_exhaustive]
pub struct ExponentialBackoff {
    /// The first retry's delay, before any cap or jitter. Default 1 s.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "base_ms", with = "crate::duration_serde::millis")
    )]
    pub base: Duration,
    /// The maximum delay, before jitter. Default 5 min.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "max_delay_ms", with = "crate::duration_serde::millis")
    )]
    pub max_delay: Duration,
    /// The number of publish attempts, including the first, before a failure goes dead. Default
    /// 10.
    pub max_attempts: u32,
    /// The jitter fraction: `0.2` multiplies the delay by a value drawn uniformly from
    /// `[0.8, 1.2]`. `0.0` disables jitter entirely (deterministic delay). Default 0.2.
    pub jitter: f64,
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            base: Duration::from_secs(1),
            max_delay: Duration::from_secs(5 * 60),
            max_attempts: 10,
            jitter: 0.2,
        }
    }
}

impl ExponentialBackoff {
    /// Sets [`Self::base`].
    #[must_use]
    pub const fn base(mut self, base: Duration) -> Self {
        self.base = base;
        self
    }

    /// Sets [`Self::max_delay`].
    #[must_use]
    pub const fn max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Sets [`Self::max_attempts`].
    #[must_use]
    pub const fn max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets [`Self::jitter`].
    #[must_use]
    pub const fn jitter(mut self, jitter: f64) -> Self {
        self.jitter = jitter;
        self
    }

    /// Validates the invariants the dispatcher builder relies on: `jitter` in `[0.0, 1.0)`,
    /// `max_attempts >= 1`, `base > 0`. Independently testable without a dispatcher.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidJitter`], [`ConfigError::ZeroMaxAttempts`] or
    /// [`ConfigError::ZeroRetryBase`].
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !(0.0..1.0).contains(&self.jitter) {
            return Err(ConfigError::InvalidJitter { value: self.jitter });
        }
        if self.max_attempts == 0 {
            return Err(ConfigError::ZeroMaxAttempts);
        }
        if self.base.is_zero() {
            return Err(ConfigError::ZeroRetryBase);
        }
        Ok(())
    }

    /// A factor in `[1.0 - jitter, 1.0 + jitter]`: `1.0` when [`Self::jitter`] is `0.0` (or, as a
    /// defensive fallback, non-finite or out of `validate`'s range — this must never let
    /// [`Duration::mul_f64`] see a negative or `NaN` multiplier and panic). `UUIDv7`'s
    /// per-millisecond monotonic counter correlates same-ms draws, so a hashed
    /// [`std::collections::hash_map::RandomState`] value is used instead;
    /// `jitter_varies_the_delay_across_repeated_calls` pins the behavior.
    fn jitter_factor(jitter: f64) -> f64 {
        let jitter = if jitter.is_finite() {
            jitter.clamp(0.0, 1.0)
        } else {
            0.0
        };
        if jitter <= 0.0 {
            return 1.0;
        }
        let entropy = std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish();
        #[allow(
            clippy::cast_precision_loss,
            reason = "jitter is a thundering-herd nicety, not a correctness input (§25.1); losing \
                      a few low bits of a 64-bit value when producing a [0.0, 1.0] fraction is fine"
        )]
        let unit = entropy as f64 / u64::MAX as f64; // in [0.0, 1.0]
        (1.0 + jitter * (2.0 * unit - 1.0)).max(0.0)
    }
}

impl RetryPolicy for ExponentialBackoff {
    fn next(&self, attempts: u32, kind: FailureKind) -> FailureOutcome {
        if kind == FailureKind::Permanent {
            return FailureOutcome::Dead {
                reason: DeadReason::PermanentError,
            };
        }
        if attempts.saturating_add(1) >= self.max_attempts {
            return FailureOutcome::Dead {
                reason: DeadReason::AttemptsExhausted,
            };
        }

        let exponent = 2u32.saturating_pow(attempts);
        let uncapped = self.base.saturating_mul(exponent);
        let capped = uncapped.min(self.max_delay);
        let delay = capped.mul_f64(Self::jitter_factor(self.jitter));
        FailureOutcome::Retry { delay }
    }
}
