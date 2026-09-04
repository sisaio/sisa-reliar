//! `OutboxSettings::from_env` returns an error — never a silent fallback to `Default` — for a
//! present variable that is unparseable or out of its documented range (SRS §7.2, §43.A.29).
//!
//! See `tests/common::run_scenario_in_child` for why each scenario runs in a child process.

mod common;

use reliar_outbox::{OutboxSettings, SettingsError};

const PREFIX: &str = "RELIAR_OUTBOX_TEST_BAD_";

#[test]
fn unparseable_batch_size_is_a_parse_error_naming_the_key() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("not a number");
        let SettingsError::Parse {
            key, value_kind, ..
        } = err
        else {
            panic!("expected Parse, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}BATCH_SIZE"));
        assert_eq!(value_kind, "u32");
        return;
    }

    let ok = common::run_scenario_in_child(
        "unparseable_batch_size_is_a_parse_error_naming_the_key",
        &[(&format!("{PREFIX}BATCH_SIZE"), "not-a-number")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn out_of_range_jitter_is_an_out_of_range_error() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("jitter must be in [0.0, 1.0)");
        let SettingsError::OutOfRange { key, .. } = err else {
            panic!("expected OutOfRange, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}RETRY_JITTER"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "out_of_range_jitter_is_an_out_of_range_error",
        &[(&format!("{PREFIX}RETRY_JITTER"), "1.5")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn overlong_worker_id_is_an_out_of_range_error() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("worker id too long");
        let SettingsError::OutOfRange { key, .. } = err else {
            panic!("expected OutOfRange, got {err:?}");
        };
        assert_eq!(key, format!("{PREFIX}WORKER_ID"));
        return;
    }

    let overlong = "w".repeat(reliar_outbox::WorkerId::MAX_LEN + 1);
    let ok = common::run_scenario_in_child(
        "overlong_worker_id_is_an_out_of_range_error",
        &[(&format!("{PREFIX}WORKER_ID"), &overlong)],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}
