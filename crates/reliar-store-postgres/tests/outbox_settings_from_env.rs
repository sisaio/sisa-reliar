#![allow(clippy::unwrap_used, clippy::expect_used)]

//! Review 3 gap — `PostgresOutboxSettings::from_env` had no test at all: neither the happy path
//! (present variables override exactly, absent ones keep the default) nor its parse-error path
//! (a present-but-unparseable value returns `Err`, never a silent fallback, per its own rustdoc
//! contract).
//!
//! See `tests/common::run_scenario_in_child` for why each scenario runs in a child process
//! rather than mutating this process's own environment (`std::env::set_var` is `unsafe` since
//! edition 2024, and this workspace forbids `unsafe_code` everywhere, `tests/` included).

mod common;

use std::time::Duration;

use reliar_store_postgres::PostgresOutboxSettings;

const PREFIX: &str = "RELIAR_STORE_POSTGRES_TEST_";

#[test]
fn absent_variables_keep_every_default() {
    if common::is_child() {
        let settings = PostgresOutboxSettings::from_env(PREFIX).expect("no variables set");
        let default = PostgresOutboxSettings::default();
        assert_eq!(settings.schema, default.schema);
        assert_eq!(
            settings.enqueue_sets_search_path,
            default.enqueue_sets_search_path
        );
        assert_eq!(settings.statement_timeout, default.statement_timeout);
        return;
    }

    let ok = common::run_scenario_in_child("absent_variables_keep_every_default", &[])
        .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn present_variables_override_exactly() {
    if common::is_child() {
        let settings = PostgresOutboxSettings::from_env(PREFIX).expect("valid overrides parse");
        assert_eq!(settings.schema, "custom_schema");
        assert!(settings.enqueue_sets_search_path);
        assert_eq!(settings.statement_timeout, Duration::from_millis(1500));
        return;
    }

    let ok = common::run_scenario_in_child(
        "present_variables_override_exactly",
        &[
            (&format!("{PREFIX}SCHEMA"), "custom_schema"),
            (&format!("{PREFIX}ENQUEUE_SETS_SEARCH_PATH"), "true"),
            (&format!("{PREFIX}STATEMENT_TIMEOUT_MS"), "1500"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn an_unparseable_bool_is_a_clean_error_naming_the_key() {
    if common::is_child() {
        let err = PostgresOutboxSettings::from_env(PREFIX).expect_err("\"maybe\" is not a bool");
        assert!(err.to_string().contains("ENQUEUE_SETS_SEARCH_PATH"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "an_unparseable_bool_is_a_clean_error_naming_the_key",
        &[(&format!("{PREFIX}ENQUEUE_SETS_SEARCH_PATH"), "maybe")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn an_unparseable_duration_is_a_clean_error_naming_the_key() {
    if common::is_child() {
        let err =
            PostgresOutboxSettings::from_env(PREFIX).expect_err("\"not-a-number\" is not u64 ms");
        assert!(err.to_string().contains("STATEMENT_TIMEOUT_MS"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "an_unparseable_duration_is_a_clean_error_naming_the_key",
        &[(&format!("{PREFIX}STATEMENT_TIMEOUT_MS"), "not-a-number")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

/// Review 4 minor — `PostgresOutboxSettings`'s `serde` wire shape had no test: the JSON keys a
/// deployment actually writes into a config file, and that `deny_unknown_fields` rejects a typo
/// rather than silently ignoring it (the same "never a silent fallback" contract `from_env`
/// documents above, applied to the `serde` path instead).
#[cfg(feature = "serde")]
#[test]
fn serde_wire_shape_matches_the_documented_keys_and_rejects_unknown_fields() {
    use reliar_store_postgres::PostgresOutboxSettings;

    let json = serde_json::to_value(PostgresOutboxSettings::default()).unwrap();
    assert_eq!(
        json,
        serde_json::json!({
            "schema": "reliar",
            "enqueue_sets_search_path": false,
            "statement_timeout_ms": 0,
        }),
        "the wire shape is a public contract for hosts hand-writing config files — a field \
         rename or an accidental new field must be a deliberate, reviewed change"
    );

    let round_tripped: PostgresOutboxSettings = serde_json::from_value(json).unwrap();
    assert_eq!(round_tripped.schema, "reliar");
    assert!(!round_tripped.enqueue_sets_search_path);
    assert_eq!(round_tripped.statement_timeout, std::time::Duration::ZERO);

    let unknown_field_rejected =
        serde_json::from_value::<PostgresOutboxSettings>(serde_json::json!({"typo_key": 1}))
            .is_err();
    assert!(
        unknown_field_rejected,
        "deny_unknown_fields must reject a typo'd key, never silently ignore it"
    );
}
