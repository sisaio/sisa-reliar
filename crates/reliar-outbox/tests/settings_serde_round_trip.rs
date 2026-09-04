//! The `serde` feature: `_ms` renames on every [`std::time::Duration`] field, integer
//! milliseconds via `duration_serde::{millis, optional_millis}`, `#[serde(default,
//! deny_unknown_fields)]` on every settings struct, and `WorkerId`'s own `Serialize`/
//! `Deserialize` (phase1-contract.md §3.7, §3.1). Compiles to nothing without the `serde`
//! feature — this whole file is that feature's test.

#![cfg(feature = "serde")]

use std::time::Duration;

use reliar_outbox::{DispatcherSettings, OutboxSettings, RetentionSettings, WorkerId};

#[test]
fn dispatcher_settings_round_trips_with_ms_renamed_duration_fields() {
    let json = serde_json::json!({
        "batch_size": 50,
        "lease_ms": 1_000,
        "max_in_flight": 4,
        "publish_timeout_ms": 2_000,
        "poll_interval_ms": 100,
        "idle_poll_interval_ms": 500,
        "drain_timeout_ms": 3_000,
        "store_timeout_ms": 4_000,
        "stats_interval_ms": 7_000,
        "ordering": "unordered",
        "retry": {
            "base_ms": 10,
            "max_delay_ms": 20,
            "max_attempts": 3,
            "jitter": 0.1
        },
        "worker_id": "custom-1"
    });

    let settings: DispatcherSettings = serde_json::from_value(json).expect("valid shape");

    assert_eq!(settings.batch_size, 50);
    assert_eq!(settings.lease, Duration::from_secs(1));
    assert_eq!(settings.max_in_flight, 4);
    assert_eq!(settings.publish_timeout, Duration::from_secs(2));
    assert_eq!(settings.poll_interval, Duration::from_millis(100));
    assert_eq!(settings.idle_poll_interval, Duration::from_millis(500));
    assert_eq!(settings.drain_timeout, Duration::from_secs(3));
    assert_eq!(settings.store_timeout, Duration::from_secs(4));
    assert_eq!(settings.stats_interval, Duration::from_secs(7));
    assert_eq!(settings.retry.base, Duration::from_millis(10));
    assert_eq!(settings.retry.max_delay, Duration::from_millis(20));
    assert_eq!(settings.retry.max_attempts, 3);
    assert!((settings.retry.jitter - 0.1).abs() < f64::EPSILON);
    assert_eq!(
        settings.worker_id.as_ref().map(ToString::to_string),
        Some("custom-1".to_string())
    );

    // Serializing back out must use the same `_ms` keys, not the Rust field names.
    let round_tripped = serde_json::to_value(&settings).expect("serialize");
    assert_eq!(round_tripped["lease_ms"], serde_json::json!(1_000));
    assert_eq!(round_tripped["retry"]["base_ms"], serde_json::json!(10));
}

#[test]
fn dispatcher_settings_rejects_an_unknown_field() {
    let json = serde_json::json!({ "batch_size": 1, "bogus_field": true });
    let result: Result<DispatcherSettings, _> = serde_json::from_value(json);
    assert!(
        result.is_err(),
        "deny_unknown_fields must reject `bogus_field`"
    );
}

#[test]
fn dispatcher_settings_fills_missing_fields_from_default() {
    let settings: DispatcherSettings =
        serde_json::from_value(serde_json::json!({})).expect("every field has #[serde(default)]");
    let defaults = DispatcherSettings::default();

    assert_eq!(settings.batch_size, defaults.batch_size);
    assert_eq!(settings.lease, defaults.lease);
    assert_eq!(settings.worker_id, None);
}

#[test]
fn retention_settings_round_trips_optional_dead_retention() {
    let with_dead_retention: RetentionSettings = serde_json::from_value(serde_json::json!({
        "published_retention_ms": 86_400_000u64,
        "dead_retention_ms": 3_600_000,
        "purge_batch_size": 500
    }))
    .expect("valid shape");
    assert_eq!(
        with_dead_retention.published_retention,
        Duration::from_secs(86_400)
    );
    assert_eq!(
        with_dead_retention.dead_retention,
        Some(Duration::from_secs(3_600))
    );

    let without_dead_retention: RetentionSettings =
        serde_json::from_value(serde_json::json!({})).expect("every field has a default");
    assert_eq!(without_dead_retention.dead_retention, None);
}

#[test]
fn outbox_settings_round_trips_the_nested_structs_and_denies_unknown_fields() {
    let settings = OutboxSettings::default();
    let json = serde_json::to_value(&settings).expect("serialize");
    let round_tripped: OutboxSettings = serde_json::from_value(json).expect("deserialize");

    assert_eq!(
        round_tripped.dispatcher.batch_size,
        settings.dispatcher.batch_size
    );
    assert_eq!(
        round_tripped.retention.published_retention,
        settings.retention.published_retention
    );

    let with_bogus_top_level = serde_json::json!({ "not_a_real_field": 1 });
    let result: Result<OutboxSettings, _> = serde_json::from_value(with_bogus_top_level);
    assert!(result.is_err());
}

#[test]
fn worker_id_serializes_as_a_plain_string() {
    let id = WorkerId::parse("worker-42").expect("valid id");
    let json = serde_json::to_value(&id).expect("serialize");
    assert_eq!(json, serde_json::json!("worker-42"));
}

#[test]
fn worker_id_deserialize_validates_like_parse() {
    let ok: Result<WorkerId, _> = serde_json::from_value(serde_json::json!("worker-42"));
    assert_eq!(ok.expect("valid id").as_str(), "worker-42");

    let empty: Result<WorkerId, _> = serde_json::from_value(serde_json::json!(""));
    assert!(
        empty.is_err(),
        "an empty worker id must fail validation, not just parsing"
    );
}
