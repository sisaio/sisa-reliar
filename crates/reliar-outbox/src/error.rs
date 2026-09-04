//! Configuration errors (SRS §23.1, §26.1, ADR 0014).

use core::fmt;
use std::time::Duration;

use crate::ordering::Ordering;

/// A dispatcher configuration that cannot be started. Returned by
/// `OutboxDispatcherBuilder::build` (S4) — **never a panic**.
///
/// [`Ordering::validate`] and [`crate::ExponentialBackoff::validate`] surface the same variants
/// independently of a dispatcher, so each rejection is unit-testable on its own.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum ConfigError {
    /// `max_in_flight` was zero: no publish task could ever run.
    ZeroInFlight,
    /// `batch_size` was zero: `acquire` would only ever be asked for zero rows, so nothing
    /// would ever be claimed (S4 review 3, minor).
    ZeroBatchSize,
    /// `poll_interval` or `idle_poll_interval` was zero (S4 review 7): with nothing to wait on,
    /// the claim loop would poll the store at CPU speed, and — since `outcome_retry_interval` is
    /// derived from `poll_interval` — a zero `poll_interval` would also re-enable the
    /// outcome-write CPU spin `outcome_retry_interval`'s floor otherwise prevents.
    ZeroPollInterval {
        /// The zero-valued field, e.g. `"poll_interval"` or `"idle_poll_interval"`.
        field: &'static str,
    },
    /// `store_timeout` was not comfortably shorter than half the lease-renewal period
    /// (`store_timeout >= lease / 2`, S4 review 4, major 3). `retry_unwritten_outcomes` races
    /// directly against the lease-renewal tick inside `run`'s own `select!`, so a `store_timeout`
    /// this long would let a single hung outcome-write attempt occupy the whole gap between two
    /// renewal ticks, starving renewal for a batch that a shorter `store_timeout` would have let
    /// interrupt in time.
    StoreTimeoutTooLong {
        /// The configured client-side bound on every `OutboxStore` call.
        store_timeout: Duration,
        /// The configured claim lease — renewal ticks every `lease / 2`.
        lease: Duration,
    },
    /// `lease` was not longer than `publish_timeout`: a healthy, in-budget publish could still
    /// lose its lease before completing.
    LeaseTooShort {
        /// The configured claim lease.
        lease: Duration,
        /// The configured per-publish timeout.
        publish_timeout: Duration,
    },
    /// The selected [`Ordering`] is not implemented yet.
    UnsupportedOrdering {
        /// The rejected ordering strategy.
        ordering: Ordering,
        /// The release that will implement it, e.g. `"0.2"`.
        available_in: &'static str,
    },
    /// `ExponentialBackoff::jitter` was outside `[0.0, 1.0)`.
    InvalidJitter {
        /// The rejected value.
        value: f64,
    },
    /// `ExponentialBackoff::max_attempts` was zero: every failure would be immediately dead.
    ZeroMaxAttempts,
    /// `ExponentialBackoff::base` was zero: the first retry would be scheduled immediately.
    ZeroRetryBase,
    /// A custom `RetryPolicy` was supplied via `OutboxDispatcherBuilder::retry_policy` **and**
    /// `DispatcherSettings::retry` is not [`crate::ExponentialBackoff::default`] — one of the two
    /// would otherwise be silently ignored (S4 review; ADR-lite K2).
    RetryPolicyConflict,
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroInFlight => f.write_str("max_in_flight must be greater than zero"),
            Self::ZeroBatchSize => f.write_str("batch_size must be greater than zero"),
            Self::ZeroPollInterval { field } => write!(f, "{field} must be greater than zero"),
            Self::StoreTimeoutTooLong { store_timeout, lease } => write!(
                f,
                "store_timeout ({store_timeout:?}) must be shorter than half the lease \
                 ({lease:?}) — a hung outcome write must not be able to occupy a whole \
                 renewal-tick gap"
            ),
            Self::LeaseTooShort {
                lease,
                publish_timeout,
            } => write!(
                f,
                "lease ({lease:?}) must be longer than publish_timeout ({publish_timeout:?})"
            ),
            Self::UnsupportedOrdering {
                ordering,
                available_in,
            } => write!(
                f,
                "{ordering:?} ordering is not implemented until {available_in}"
            ),
            Self::InvalidJitter { value } => {
                write!(f, "jitter {value} must be in the range [0.0, 1.0)")
            }
            Self::ZeroMaxAttempts => f.write_str("max_attempts must be at least 1"),
            Self::ZeroRetryBase => f.write_str("base delay must be greater than zero"),
            Self::RetryPolicyConflict => f.write_str(
                "a custom retry policy was supplied together with a non-default                  DispatcherSettings::retry; leave settings.retry at its default or feed it to                  the custom policy yourself",
            ),
        }
    }
}

impl std::error::Error for ConfigError {}
