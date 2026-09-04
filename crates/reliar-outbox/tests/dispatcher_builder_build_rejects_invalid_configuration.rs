//! [`OutboxDispatcherBuilder::build`] validates the whole configuration before a dispatcher can
//! exist — never a panic (phase1-contract.md §3.9, SRS §22.2, §23.1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    ConfigError, DispatcherSettings, ExponentialBackoff, InMemoryOutboxStore, Ordering,
    OutboxDispatcher, RecordingPublisher,
};

fn builder() -> reliar_outbox::OutboxDispatcherBuilder<InMemoryOutboxStore, RecordingPublisher> {
    OutboxDispatcher::builder(
        InMemoryOutboxStore::default(),
        RecordingPublisher::default(),
    )
}

#[test]
fn valid_settings_build_successfully() {
    assert!(builder().build().is_ok());
}

#[test]
fn zero_max_in_flight_is_rejected() {
    let err = builder()
        .settings(DispatcherSettings::default().max_in_flight(0))
        .build()
        .unwrap_err();
    assert_eq!(err, ConfigError::ZeroInFlight);
}

#[test]
fn zero_poll_interval_is_rejected() {
    let err = builder()
        .settings(DispatcherSettings::default().poll_interval(Duration::ZERO))
        .build()
        .unwrap_err();
    assert_eq!(
        err,
        ConfigError::ZeroPollInterval {
            field: "poll_interval"
        }
    );
}

#[test]
fn zero_idle_poll_interval_is_rejected() {
    let err = builder()
        .settings(DispatcherSettings::default().idle_poll_interval(Duration::ZERO))
        .build()
        .unwrap_err();
    assert_eq!(
        err,
        ConfigError::ZeroPollInterval {
            field: "idle_poll_interval"
        }
    );
}

#[test]
fn lease_not_longer_than_publish_timeout_is_rejected() {
    let settings = DispatcherSettings::default()
        .lease(Duration::from_secs(5))
        .publish_timeout(Duration::from_secs(5));
    let err = builder().settings(settings).build().unwrap_err();
    assert_eq!(
        err,
        ConfigError::LeaseTooShort {
            lease: Duration::from_secs(5),
            publish_timeout: Duration::from_secs(5),
        }
    );
}

#[test]
fn store_timeout_not_shorter_than_half_the_lease_is_rejected() {
    // A hung `complete`/`fail` must not be able to occupy a whole renewal-tick gap (S4 review 4,
    // major 3) — `store_timeout` has to leave real headroom under `lease / 2`.
    let settings = DispatcherSettings::default()
        .lease(Duration::from_secs(30))
        .store_timeout(Duration::from_secs(15));
    let err = builder().settings(settings).build().unwrap_err();
    assert_eq!(
        err,
        ConfigError::StoreTimeoutTooLong {
            store_timeout: Duration::from_secs(15),
            lease: Duration::from_secs(30),
        }
    );
}

#[test]
fn store_timeout_exactly_half_the_lease_is_rejected() {
    // The boundary itself is inclusive, not just values past it.
    let settings = DispatcherSettings::default()
        .lease(Duration::from_secs(20))
        .store_timeout(Duration::from_secs(10));
    let err = builder().settings(settings).build().unwrap_err();
    assert_eq!(
        err,
        ConfigError::StoreTimeoutTooLong {
            store_timeout: Duration::from_secs(10),
            lease: Duration::from_secs(20),
        }
    );
}

#[test]
fn store_timeout_comfortably_under_half_the_lease_builds_fine() {
    let settings = DispatcherSettings::default()
        .lease(Duration::from_secs(30))
        .store_timeout(Duration::from_secs(5));
    assert!(builder().settings(settings).build().is_ok());
}

#[test]
fn per_key_ordering_is_rejected_before_0_2() {
    let err = builder().ordering(Ordering::PerKey).build().unwrap_err();
    assert_eq!(
        err,
        ConfigError::UnsupportedOrdering {
            ordering: Ordering::PerKey,
            available_in: "0.2",
        }
    );
}

#[test]
fn invalid_retry_jitter_is_rejected() {
    let settings = DispatcherSettings::default().retry(ExponentialBackoff::default().jitter(1.0));
    let err = builder().settings(settings).build().unwrap_err();
    assert_eq!(err, ConfigError::InvalidJitter { value: 1.0 });
}

#[test]
fn zero_retry_max_attempts_is_rejected() {
    let settings =
        DispatcherSettings::default().retry(ExponentialBackoff::default().max_attempts(0));
    let err = builder().settings(settings).build().unwrap_err();
    assert_eq!(err, ConfigError::ZeroMaxAttempts);
}

#[test]
fn zero_retry_base_is_rejected() {
    let settings =
        DispatcherSettings::default().retry(ExponentialBackoff::default().base(Duration::ZERO));
    let err = builder().settings(settings).build().unwrap_err();
    assert_eq!(err, ConfigError::ZeroRetryBase);
}

#[test]
fn a_custom_policy_together_with_non_default_settings_retry_is_a_conflict() {
    // A custom `.retry_policy(..)` and a non-default `settings.retry` would leave one of the two
    // silently ignored (K2, ADR-lite K2) — `build()` rejects the combination instead.
    let settings = DispatcherSettings::default()
        .retry(ExponentialBackoff::default().base(Duration::from_millis(1)));
    let err = builder()
        .settings(settings)
        .retry_policy(ExponentialBackoff::default())
        .build()
        .unwrap_err();
    assert_eq!(err, ConfigError::RetryPolicyConflict);
}

#[test]
fn retry_policy_conflict_display_names_the_problem() {
    let text = ConfigError::RetryPolicyConflict.to_string();
    assert!(text.contains("custom retry policy"));
    assert!(text.contains("settings.retry") || text.contains("default"));
}

#[test]
fn a_custom_policy_with_default_settings_retry_builds_fine() {
    // No conflict: `settings.retry` was left untouched.
    assert!(
        builder()
            .retry_policy(ExponentialBackoff::default())
            .build()
            .is_ok()
    );
}

#[test]
fn a_custom_exponential_backoff_policy_is_itself_validated() {
    // The supplied policy happens to be an `ExponentialBackoff` too (not just `settings.retry`)
    // — its own bounds are checked (S4 review 3, minor).
    let err = builder()
        .retry_policy(ExponentialBackoff::default().max_attempts(0))
        .build()
        .unwrap_err();
    assert_eq!(err, ConfigError::ZeroMaxAttempts);
}
