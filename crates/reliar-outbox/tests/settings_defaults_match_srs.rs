//! `OutboxSettings::default()` matches the accepted v0.1 defaults table (SRS §23.1, §43.A.29).

use std::time::Duration;

use reliar_outbox::{ExponentialBackoff, Ordering, OutboxSettings};

#[test]
fn dispatcher_defaults_match_the_srs_23_1_table() {
    let settings = OutboxSettings::default();
    let dispatcher = settings.dispatcher;

    assert_eq!(dispatcher.batch_size, 100);
    assert_eq!(dispatcher.lease, Duration::from_secs(30));
    assert_eq!(dispatcher.max_in_flight, 16);
    assert_eq!(dispatcher.publish_timeout, Duration::from_secs(10));
    assert_eq!(dispatcher.poll_interval, Duration::from_millis(500));
    assert_eq!(dispatcher.idle_poll_interval, Duration::from_secs(5));
    assert_eq!(dispatcher.drain_timeout, Duration::from_secs(30));
    assert_eq!(dispatcher.store_timeout, Duration::from_secs(10));
    assert_eq!(dispatcher.stats_interval, Duration::from_secs(15));
    assert_eq!(dispatcher.ordering, Ordering::Unordered);
    assert_eq!(dispatcher.worker_id, None);

    let retry = dispatcher.retry;
    let default_retry = ExponentialBackoff::default();
    assert_eq!(retry.base, default_retry.base);
    assert_eq!(retry.max_delay, default_retry.max_delay);
    assert_eq!(retry.max_attempts, default_retry.max_attempts);
    assert!((retry.jitter - default_retry.jitter).abs() < f64::EPSILON);
}

#[test]
fn retry_backoff_defaults_match_the_srs_23_1_table() {
    let retry = ExponentialBackoff::default();

    assert_eq!(retry.base, Duration::from_secs(1));
    assert_eq!(retry.max_delay, Duration::from_secs(5 * 60));
    assert_eq!(retry.max_attempts, 10);
    assert!((retry.jitter - 0.2).abs() < f64::EPSILON);
}

#[test]
fn retention_defaults_match_the_srs_23_1_table() {
    let retention = OutboxSettings::default().retention;

    assert_eq!(
        retention.published_retention,
        Duration::from_secs(7 * 24 * 60 * 60)
    );
    assert_eq!(retention.dead_retention, None);
    assert_eq!(retention.purge_batch_size, 1_000);
}
