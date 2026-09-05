//! Overlap between `allowed_types` and `disallowed_types` is rejected on every construction
//! path — the builder (both orders), `from_env`, and `serde` — never a silent tie-break (§43.D13,
//! ADR 0033 Amendment C).

mod common;

use reliar_outbox::{MessageTypeNames, OutboxSettings, SettingsError};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn names(list: &[&str]) -> MessageTypeNames {
    MessageTypeNames::try_from_iter("test", list.iter().copied()).expect("valid test names")
}

#[test]
fn allowed_after_disallowed_is_rejected_naming_allowed_types() {
    let settings = OutboxSettings::default()
        .disallowed_types(names(&["a"]))
        .expect("no overlap yet");
    let err = settings
        .allowed_types(names(&["a"]))
        .expect_err("must overlap");
    match err {
        SettingsError::OutOfRange { key, .. } => assert_eq!(key, "allowed_types"),
        other => panic!("expected OutOfRange, got {other:?}"),
    }
}

#[test]
fn disallowed_after_allowed_is_rejected_naming_disallowed_types() {
    let settings = OutboxSettings::default()
        .allowed_types(names(&["a"]))
        .expect("no overlap yet");
    let err = settings
        .disallowed_types(names(&["a"]))
        .expect_err("must overlap");
    match err {
        SettingsError::OutOfRange { key, .. } => assert_eq!(key, "disallowed_types"),
        other => panic!("expected OutOfRange, got {other:?}"),
    }
}

const PREFIX: &str = "RELIAR_OUTBOX_TEST_ROUTING_OVERLAP_";

#[test]
fn from_env_overlap_is_rejected_naming_the_disallowed_key() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("overlap must be rejected");
        match err {
            SettingsError::OutOfRange { key, .. } => {
                assert_eq!(key, format!("{PREFIX}DISALLOWED_TYPES"));
            }
            other => panic!("expected OutOfRange, got {other:?}"),
        }
        return;
    }

    let ok = common::run_scenario_in_child(
        "from_env_overlap_is_rejected_naming_the_disallowed_key",
        &[
            (&format!("{PREFIX}ALLOWED_TYPES"), "a"),
            (&format!("{PREFIX}DISALLOWED_TYPES"), "a"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
#[cfg(feature = "serde")]
fn serde_document_with_the_same_name_in_both_lists_fails_deserialization() {
    let json = serde_json::json!({ "allowed_types": ["a"], "disallowed_types": ["a"] });
    let result: Result<OutboxSettings, _> = serde_json::from_value(json);
    let err = result.expect_err("an overlapping document must not deserialize");
    let text = err.to_string();
    assert!(
        text.contains("disallowed_types"),
        "the rejection must name the offending field: {text}"
    );
}
