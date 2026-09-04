//! `OutboxSettings::from_env` overrides only the variables present under its prefix and parses
//! durations as integer milliseconds (SRS §7.2, §43.A.29).
//!
//! See `tests/common::run_scenario_in_child` for why each scenario runs in a child process
//! rather than mutating this process's own environment.

mod common;

use std::time::Duration;

use reliar_outbox::{Ordering, OutboxSettings};

const PREFIX: &str = "RELIAR_OUTBOX_TEST_OVERRIDE_";

#[test]
fn overrides_only_present_variables_and_parses_ms_durations() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("valid overrides parse");

        // Present: overridden, and the millisecond value is parsed into a `Duration`.
        assert_eq!(settings.dispatcher.batch_size, 250);
        assert_eq!(settings.dispatcher.lease, Duration::from_secs(45));
        assert_eq!(
            settings.retention.published_retention,
            Duration::from_secs(3_600)
        );

        // Absent: every other field keeps `Default`'s value, not a value derived from the
        // present ones.
        let defaults = OutboxSettings::default();
        assert_eq!(
            settings.dispatcher.max_in_flight,
            defaults.dispatcher.max_in_flight
        );
        assert_eq!(
            settings.dispatcher.publish_timeout,
            defaults.dispatcher.publish_timeout
        );
        assert_eq!(settings.dispatcher.ordering, defaults.dispatcher.ordering);
        assert_eq!(settings.dispatcher.worker_id, None);
        assert_eq!(
            settings.retention.dead_retention,
            defaults.retention.dead_retention
        );
        return;
    }

    let ok = common::run_scenario_in_child(
        "overrides_only_present_variables_and_parses_ms_durations",
        &[
            (&format!("{PREFIX}BATCH_SIZE"), "250"),
            (&format!("{PREFIX}LEASE_MS"), "45000"),
            (&format!("{PREFIX}PUBLISHED_RETENTION_MS"), "3600000"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn overrides_the_retry_policy_fields_individually() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("valid overrides parse");
        assert_eq!(settings.dispatcher.retry.max_attempts, 5);
        assert!((settings.dispatcher.retry.jitter - 0.0).abs() < f64::EPSILON);
        // Untouched retry fields keep their default.
        let defaults = OutboxSettings::default();
        assert_eq!(
            settings.dispatcher.retry.base,
            defaults.dispatcher.retry.base
        );
        assert_eq!(
            settings.dispatcher.retry.max_delay,
            defaults.dispatcher.retry.max_delay
        );
        return;
    }

    let ok = common::run_scenario_in_child(
        "overrides_the_retry_policy_fields_individually",
        &[
            (&format!("{PREFIX}RETRY_MAX_ATTEMPTS"), "5"),
            (&format!("{PREFIX}RETRY_JITTER"), "0.0"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn overrides_max_in_flight_ordering_dead_retention_and_worker_id() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("valid overrides parse");

        assert_eq!(settings.dispatcher.max_in_flight, 4);
        assert_eq!(settings.dispatcher.ordering, Ordering::PerKey);
        assert_eq!(
            settings.retention.dead_retention,
            Some(Duration::from_secs(600))
        );
        assert_eq!(
            settings.dispatcher.worker_id.map(|id| id.to_string()),
            Some("operator-assigned-1".to_string())
        );
        return;
    }

    let ok = common::run_scenario_in_child(
        "overrides_max_in_flight_ordering_dead_retention_and_worker_id",
        &[
            (&format!("{PREFIX}MAX_IN_FLIGHT"), "4"),
            (&format!("{PREFIX}ORDERING"), "per_key"),
            (&format!("{PREFIX}DEAD_RETENTION_MS"), "600000"),
            (&format!("{PREFIX}WORKER_ID"), "operator-assigned-1"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn overrides_the_remaining_timing_and_purge_keys() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("valid overrides parse");

        assert_eq!(settings.dispatcher.publish_timeout, Duration::from_secs(8));
        assert_eq!(
            settings.dispatcher.poll_interval,
            Duration::from_millis(750)
        );
        assert_eq!(
            settings.dispatcher.idle_poll_interval,
            Duration::from_secs(9)
        );
        assert_eq!(settings.dispatcher.drain_timeout, Duration::from_secs(20));
        assert_eq!(settings.dispatcher.store_timeout, Duration::from_secs(25));
        assert_eq!(settings.dispatcher.stats_interval, Duration::from_secs(12));
        assert_eq!(settings.dispatcher.retry.base, Duration::from_millis(250));
        assert_eq!(settings.dispatcher.retry.max_delay, Duration::from_secs(90));
        assert_eq!(settings.retention.purge_batch_size, 42);

        // Untouched fields still keep their default, proving these eight keys are read
        // independently rather than as one all-or-nothing block.
        let defaults = OutboxSettings::default();
        assert_eq!(
            settings.dispatcher.batch_size,
            defaults.dispatcher.batch_size
        );
        assert_eq!(settings.dispatcher.lease, defaults.dispatcher.lease);
        return;
    }

    let ok = common::run_scenario_in_child(
        "overrides_the_remaining_timing_and_purge_keys",
        &[
            (&format!("{PREFIX}PUBLISH_TIMEOUT_MS"), "8000"),
            (&format!("{PREFIX}POLL_INTERVAL_MS"), "750"),
            (&format!("{PREFIX}IDLE_POLL_INTERVAL_MS"), "9000"),
            (&format!("{PREFIX}DRAIN_TIMEOUT_MS"), "20000"),
            (&format!("{PREFIX}STORE_TIMEOUT_MS"), "25000"),
            (&format!("{PREFIX}STATS_INTERVAL_MS"), "12000"),
            (&format!("{PREFIX}RETRY_BASE_MS"), "250"),
            (&format!("{PREFIX}RETRY_MAX_DELAY_MS"), "90000"),
            (&format!("{PREFIX}PURGE_BATCH_SIZE"), "42"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
