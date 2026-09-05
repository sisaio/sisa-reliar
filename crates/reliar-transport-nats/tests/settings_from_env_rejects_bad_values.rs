//! `NatsSettings::from_env` returns an error — never a silent fallback to `Default` — for a
//! present variable that is unparseable or out of its documented range (SRS §7.2). See
//! `tests/common::run_scenario_in_child` for why each scenario runs in a child process.

mod common;

use reliar_transport_nats::{NatsSettings, SettingsError};

const PREFIX: &str = "RELIAR_NATS_TEST_BAD_";

#[test]
fn unparseable_batch_pipeline_depth_is_a_parse_error_naming_the_key() {
    if common::is_child() {
        let err = NatsSettings::from_env(PREFIX).expect_err("not a number");
        let SettingsError::Parse {
            key, value_kind, ..
        } = err
        else {
            panic!("expected Parse, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}BATCH_PIPELINE_DEPTH"));
        assert_eq!(value_kind, "usize");
        return;
    }

    let ok = common::run_scenario_in_child(
        "unparseable_batch_pipeline_depth_is_a_parse_error_naming_the_key",
        &[(&format!("{PREFIX}BATCH_PIPELINE_DEPTH"), "not-a-number")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn zero_batch_pipeline_depth_is_an_out_of_range_error() {
    if common::is_child() {
        let err = NatsSettings::from_env(PREFIX).expect_err("batch_pipeline_depth must be > 0");
        let SettingsError::OutOfRange { key, .. } = err else {
            panic!("expected OutOfRange, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}BATCH_PIPELINE_DEPTH"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "zero_batch_pipeline_depth_is_an_out_of_range_error",
        &[(&format!("{PREFIX}BATCH_PIPELINE_DEPTH"), "0")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn zero_publish_timeout_is_an_out_of_range_error() {
    if common::is_child() {
        let err = NatsSettings::from_env(PREFIX).expect_err("publish_timeout must be > 0");
        let SettingsError::OutOfRange { key, .. } = err else {
            panic!("expected OutOfRange, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}PUBLISH_TIMEOUT_MS"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "zero_publish_timeout_is_an_out_of_range_error",
        &[(&format!("{PREFIX}PUBLISH_TIMEOUT_MS"), "0")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn an_illegal_subject_prefix_is_an_out_of_range_error() {
    if common::is_child() {
        let err = NatsSettings::from_env(PREFIX).expect_err("a wildcard prefix is not legal");
        let SettingsError::OutOfRange { key, .. } = err else {
            panic!("expected OutOfRange, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}SUBJECT_PREFIX"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "an_illegal_subject_prefix_is_an_out_of_range_error",
        &[(&format!("{PREFIX}SUBJECT_PREFIX"), "a.*")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn unparseable_max_payload_bytes_is_a_parse_error() {
    if common::is_child() {
        let err = NatsSettings::from_env(PREFIX).expect_err("not a number");
        let SettingsError::Parse {
            key, value_kind, ..
        } = err
        else {
            panic!("expected Parse, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}MAX_PAYLOAD_BYTES"));
        assert_eq!(value_kind, "usize");
        return;
    }

    let ok = common::run_scenario_in_child(
        "unparseable_max_payload_bytes_is_a_parse_error",
        &[(&format!("{PREFIX}MAX_PAYLOAD_BYTES"), "not-a-number")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

/// A zero `max_payload` would reject every message (S1 review, minor 10) — `from_env` rejects it
/// the same way it rejects the other zero guards, rather than silently accepting a
/// misconfiguration. The infallible builder method (`NatsSettings::max_payload`) cannot perform
/// this check itself (contract §4.1 fixes its signature as `Self -> Self`, never `Result`); a host
/// calling the builder directly with `Some(0)` is not guarded here — only `from_env` is.
#[test]
fn zero_max_payload_bytes_is_an_out_of_range_error() {
    if common::is_child() {
        let err = NatsSettings::from_env(PREFIX).expect_err("max_payload must be > 0");
        let SettingsError::OutOfRange { key, .. } = err else {
            panic!("expected OutOfRange, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}MAX_PAYLOAD_BYTES"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "zero_max_payload_bytes_is_an_out_of_range_error",
        &[(&format!("{PREFIX}MAX_PAYLOAD_BYTES"), "0")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
