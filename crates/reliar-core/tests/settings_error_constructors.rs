//! `SettingsError::{parse, out_of_range, key}` — public constructors so a crate other than the
//! one defining a given `Settings` type (e.g. `reliar-store-postgres`) can build this type for
//! its own `from_env` instead of shipping a parallel, unrelated error (contract §7 row I3,
//! ADR 0019, ADR 0032). Moved here from `reliar-outbox` when `SettingsError` moved to
//! `reliar-core`.

use std::error::Error;

use reliar_core::SettingsError;

#[test]
fn parse_builds_a_parse_error_naming_the_key_and_expected_shape() {
    let err = SettingsError::parse("RELIAR_OUTBOX_BATCH_SIZE", "u32");

    assert_eq!(err.key(), "RELIAR_OUTBOX_BATCH_SIZE");
    assert_eq!(
        err,
        SettingsError::parse("RELIAR_OUTBOX_BATCH_SIZE".to_string(), "u32")
    );
    assert_eq!(
        err.to_string(),
        "RELIAR_OUTBOX_BATCH_SIZE could not be parsed as u32"
    );
}

#[test]
fn out_of_range_builds_an_out_of_range_error_naming_the_key_and_bound() {
    let err = SettingsError::out_of_range(
        "RELIAR_OUTBOX_RETRY_JITTER",
        "jitter must be in the range [0.0, 1.0)",
    );

    assert_eq!(err.key(), "RELIAR_OUTBOX_RETRY_JITTER");
    assert_eq!(
        err.to_string(),
        "RELIAR_OUTBOX_RETRY_JITTER is out of range: jitter must be in the range [0.0, 1.0)"
    );
}

#[test]
fn key_reads_the_full_variable_name_from_either_variant() {
    let parse_err = SettingsError::parse("KEY_A", "u32");
    let range_err = SettingsError::out_of_range("KEY_B", "bound violated");

    assert_eq!(parse_err.key(), "KEY_A");
    assert_eq!(range_err.key(), "KEY_B");
}

#[test]
fn constructors_accept_both_a_string_and_a_str() {
    let from_owned = SettingsError::parse(String::from("KEY"), "u32");
    let from_borrowed = SettingsError::parse("KEY", "u32");
    assert_eq!(from_owned, from_borrowed);
}

#[test]
fn settings_error_is_clone_and_partial_eq() {
    let err = SettingsError::parse("KEY", "u32");
    let cloned = err.clone();
    assert_eq!(err, cloned);
    assert_ne!(err, SettingsError::parse("OTHER_KEY", "u32"));
    assert_ne!(err, SettingsError::out_of_range("KEY", "u32"));
}

#[test]
fn settings_error_carries_no_source_by_design() {
    // Contract §7 I3's `SettingsError` is two plain fields per variant (`key` + `value_kind`/
    // `message`), not a boxed cause: `source()` is the trait default, `None`, for both
    // constructors. `Display` already carries every operator-actionable detail.
    let parse_err = SettingsError::parse("KEY", "u32");
    let range_err = SettingsError::out_of_range("KEY", "bound");

    assert!(Error::source(&parse_err).is_none());
    assert!(Error::source(&range_err).is_none());
}
