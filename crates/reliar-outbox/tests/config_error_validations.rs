//! Where each `ConfigError` rejection lives before there is a dispatcher builder to call it from
//! (phase1-contract.md §3.1, §3.6, §3.9; `OutboxDispatcherBuilder::build` itself ships in S4).
//! `Ordering::validate` and `ExponentialBackoff::validate` surface the same variants
//! independently, so each rejection is unit-testable on its own.

use std::time::Duration;

use reliar_outbox::{ConfigError, ExponentialBackoff, Ordering};

#[test]
fn unordered_validates_without_error() {
    assert_eq!(Ordering::Unordered.validate(), Ok(()));
}

#[test]
fn per_key_is_rejected_naming_the_release_that_implements_it() {
    let err = Ordering::PerKey.validate().unwrap_err();
    assert_eq!(
        err,
        ConfigError::UnsupportedOrdering {
            ordering: Ordering::PerKey,
            available_in: "0.2",
        }
    );
}

#[test]
fn default_backoff_validates_without_error() {
    assert_eq!(ExponentialBackoff::default().validate(), Ok(()));
}

#[test]
fn jitter_at_or_above_one_is_invalid() {
    let backoff = ExponentialBackoff::default().jitter(1.0);
    assert_eq!(
        backoff.validate(),
        Err(ConfigError::InvalidJitter { value: 1.0 })
    );
}

#[test]
fn negative_jitter_is_invalid() {
    let backoff = ExponentialBackoff::default().jitter(-0.1);
    assert_eq!(
        backoff.validate(),
        Err(ConfigError::InvalidJitter { value: -0.1 })
    );
}

#[test]
fn jitter_just_below_one_is_valid() {
    let backoff = ExponentialBackoff::default().jitter(0.999);
    assert_eq!(backoff.validate(), Ok(()));
}

#[test]
fn zero_max_attempts_is_invalid() {
    let backoff = ExponentialBackoff::default().max_attempts(0);
    assert_eq!(backoff.validate(), Err(ConfigError::ZeroMaxAttempts));
}

#[test]
fn zero_base_delay_is_invalid() {
    let backoff = ExponentialBackoff::default().base(Duration::ZERO);
    assert_eq!(backoff.validate(), Err(ConfigError::ZeroRetryBase));
}
