//! `OutboxSettings`'s routing fields: defaults, builder round-trip, `MessageTypeNames::parse`/
//! `try_from_iter`, `from_env`, and `serde` (§43.D5, §43.D13, ADR 0033).
//!
//! See `tests/common::run_scenario_in_child` for why the `from_env` scenarios run in a child
//! process rather than mutating this process's own environment.

mod common;

use reliar_outbox::{MessageTypeNames, OutboxSettings, SettingsError};

#[test]
fn default_is_enabled_with_both_lists_empty() {
    let settings = OutboxSettings::default();
    assert!(settings.enabled);
    assert!(settings.allowed_types.is_empty());
    assert!(settings.disallowed_types.is_empty());
}

#[test]
fn builder_round_trips_and_leaves_dispatcher_and_retention_untouched() {
    let defaults = OutboxSettings::default();
    let settings = OutboxSettings::default()
        .enabled(false)
        .allowed_types(MessageTypeNames::parse("allowed_types", "a,b").expect("valid"))
        .expect("no overlap")
        .disallowed_types(MessageTypeNames::parse("disallowed_types", "c").expect("valid"))
        .expect("no overlap");

    assert!(!settings.enabled);
    assert_eq!(
        settings.allowed_types.names(),
        ["a".to_string(), "b".to_string()]
    );
    assert_eq!(settings.disallowed_types.names(), ["c".to_string()]);
    assert_eq!(
        settings.dispatcher.batch_size,
        defaults.dispatcher.batch_size
    );
    assert_eq!(
        settings.retention.purge_batch_size,
        defaults.retention.purge_batch_size
    );
}

#[test]
fn parse_drops_empty_entries_and_tolerates_duplicates() {
    assert!(MessageTypeNames::parse("f", "").expect("valid").is_empty());
    let parsed = MessageTypeNames::parse("f", "a,,b, c ").expect("valid");
    assert_eq!(parsed.names(), ["a", "b", "c"]);
    let with_dup = MessageTypeNames::parse("f", "a,a").expect("valid");
    assert_eq!(with_dup.names(), ["a", "a"]);
}

#[test]
fn parse_rejects_a_versioned_name_and_names_the_field_without_echoing_the_value() {
    let err = MessageTypeNames::parse("allowed_types", "orders.created.v1")
        .expect_err("a .v<digits> entry must be rejected");
    let text = err.to_string();
    match err {
        SettingsError::Parse { key, value_kind } => {
            assert_eq!(key, "allowed_types");
            assert_eq!(value_kind, "message type names without a version suffix");
        }
        other => panic!("expected Parse, got {other:?}"),
    }
    assert!(!text.contains("orders.created.v1"));
}

#[test]
fn try_from_iter_rejects_an_explicitly_empty_name() {
    let err =
        MessageTypeNames::try_from_iter("f", [" ", "a"]).expect_err("an empty name must fail");
    match err {
        SettingsError::Parse { value_kind, .. } => {
            assert_eq!(value_kind, "non-empty message type names");
        }
        other => panic!("expected Parse, got {other:?}"),
    }
}

const PREFIX: &str = "RELIAR_OUTBOX_TEST_ROUTING_";

#[test]
fn from_env_overrides_only_present_keys() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("valid overrides parse");
        assert!(!settings.enabled);
        assert_eq!(settings.allowed_types.names(), ["a"]);
        assert_eq!(settings.disallowed_types.names(), ["b"]);
        return;
    }

    let ok = common::run_scenario_in_child(
        "from_env_overrides_only_present_keys",
        &[
            (&format!("{PREFIX}ENABLED"), "false"),
            (&format!("{PREFIX}ALLOWED_TYPES"), "a"),
            (&format!("{PREFIX}DISALLOWED_TYPES"), "b"),
        ],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn from_env_enabled_accepts_true_false_1_0_case_insensitive() {
    // The harness passes the expected boolean through its own env var (outside `PREFIX`, so it
    // never reaches `from_env`), because each of the six representations below must resolve to
    // the *correct* boolean, not merely parse without error (review round 1, minor: only
    // `" FALSE "` was ever exercised).
    const EXPECTED_ENV: &str = "RELIAR_OUTBOX_TEST_EXPECTED_ENABLED";
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("valid overrides parse");
        let expected: bool = std::env::var(EXPECTED_ENV)
            .expect("harness always sets the expected value")
            .parse()
            .expect("harness always writes \"true\" or \"false\"");
        assert_eq!(settings.enabled, expected);
        return;
    }

    for (raw, expected) in [
        (" FALSE ", false),
        ("false", false),
        ("0", false),
        (" TRUE ", true),
        ("true", true),
        ("1", true),
    ] {
        let ok = common::run_scenario_in_child(
            "from_env_enabled_accepts_true_false_1_0_case_insensitive",
            &[
                (&format!("{PREFIX}ENABLED"), raw),
                (EXPECTED_ENV, if expected { "true" } else { "false" }),
            ],
        )
        .expect("spawn a child copy of this test binary");
        assert!(
            ok,
            "child scenario failed for input {raw:?} — see captured output above"
        );
    }
}

#[test]
fn from_env_enabled_rejects_an_unparseable_value() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("must reject an invalid boolean");
        match err {
            SettingsError::Parse { key, .. } => assert_eq!(key, format!("{PREFIX}ENABLED")),
            other => panic!("expected Parse, got {other:?}"),
        }
        return;
    }

    let ok = common::run_scenario_in_child(
        "from_env_enabled_rejects_an_unparseable_value",
        &[(&format!("{PREFIX}ENABLED"), "maybe")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
fn from_env_allowed_types_rejects_a_versioned_entry_without_echoing_the_value() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("must reject the versioned entry");
        let text = err.to_string();
        match err {
            SettingsError::Parse { key, .. } => {
                assert_eq!(key, format!("{PREFIX}ALLOWED_TYPES"));
            }
            other => panic!("expected Parse, got {other:?}"),
        }
        assert!(!text.contains("orders.created.v1"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "from_env_allowed_types_rejects_a_versioned_entry_without_echoing_the_value",
        &[(&format!("{PREFIX}ALLOWED_TYPES"), "orders.created.v1")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

/// D13, the `DISALLOWED_TYPES` half: the same rejection, on the other env key (review round 1,
/// minor — only `ALLOWED_TYPES` was covered).
#[test]
fn from_env_disallowed_types_rejects_a_versioned_entry_without_echoing_the_value() {
    if common::is_child() {
        let err = OutboxSettings::from_env(PREFIX).expect_err("must reject the versioned entry");
        let text = err.to_string();
        match err {
            SettingsError::Parse { key, .. } => {
                assert_eq!(key, format!("{PREFIX}DISALLOWED_TYPES"));
            }
            other => panic!("expected Parse, got {other:?}"),
        }
        assert!(!text.contains("audit.logged.v2"));
        return;
    }

    let ok = common::run_scenario_in_child(
        "from_env_disallowed_types_rejects_a_versioned_entry_without_echoing_the_value",
        &[(&format!("{PREFIX}DISALLOWED_TYPES"), "audit.logged.v2")],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

/// D13 + the absent-keys default (review round 1, minor): with none of the three routing env
/// keys set, `from_env` keeps `OutboxSettings::default`'s routing fields untouched — proven in
/// the same child-process harness as the override tests, so a regression that made `from_env`
/// silently invent a routing default would show up here rather than only in-process defaults.
#[test]
fn from_env_keeps_routing_defaults_when_none_of_the_three_keys_are_set() {
    if common::is_child() {
        let settings = OutboxSettings::from_env(PREFIX).expect("no overrides present");
        let defaults = OutboxSettings::default();
        assert_eq!(settings.enabled, defaults.enabled);
        assert_eq!(settings.allowed_types, defaults.allowed_types);
        assert_eq!(settings.disallowed_types, defaults.disallowed_types);
        return;
    }

    let ok = common::run_scenario_in_child(
        "from_env_keeps_routing_defaults_when_none_of_the_three_keys_are_set",
        &[],
    )
    .expect("spawn a child copy of this test binary");
    assert!(ok, "child scenario failed — see its captured output above");
}

#[test]
#[cfg(feature = "serde")]
fn serde_round_trips_top_level_routing_fields() {
    let json = serde_json::json!({
        "enabled": false,
        "allowed_types": ["a", "b"],
        "disallowed_types": ["c"]
    });
    let settings: OutboxSettings = serde_json::from_value(json).expect("valid shape");
    assert!(!settings.enabled);
    assert_eq!(settings.allowed_types.names(), ["a", "b"]);
    assert_eq!(settings.disallowed_types.names(), ["c"]);

    let round_tripped = serde_json::to_value(&settings).expect("serialize");
    assert_eq!(round_tripped["enabled"], serde_json::json!(false));
    assert_eq!(
        round_tripped["allowed_types"],
        serde_json::json!(["a", "b"])
    );
    assert_eq!(round_tripped["disallowed_types"], serde_json::json!(["c"]));
}

#[test]
#[cfg(feature = "serde")]
fn serde_still_deserializes_a_legacy_document_without_routing_fields() {
    let json = serde_json::json!({ "dispatcher": {}, "retention": {} });
    let settings: OutboxSettings = serde_json::from_value(json).expect("legacy document");
    assert!(settings.enabled);
    assert!(settings.allowed_types.is_empty());
    assert!(settings.disallowed_types.is_empty());
}

#[test]
#[cfg(feature = "serde")]
fn serde_denies_an_unknown_top_level_field() {
    let json = serde_json::json!({ "bogus_field": true });
    let result: Result<OutboxSettings, _> = serde_json::from_value(json);
    assert!(
        result.is_err(),
        "deny_unknown_fields must reject bogus_field"
    );
}

#[test]
#[cfg(feature = "serde")]
fn serde_rejects_a_versioned_entry_in_allowed_types() {
    let json = serde_json::json!({ "allowed_types": ["orders.created.v1"] });
    let result: Result<OutboxSettings, _> = serde_json::from_value(json);
    assert!(
        result.is_err(),
        "validation must not be bypassable via a config document"
    );
}

/// D13, the `disallowed_types` half (review round 1, minor — only `allowed_types` was covered).
#[test]
#[cfg(feature = "serde")]
fn serde_rejects_a_versioned_entry_in_disallowed_types() {
    let json = serde_json::json!({ "disallowed_types": ["audit.logged.v2"] });
    let result: Result<OutboxSettings, _> = serde_json::from_value(json);
    assert!(
        result.is_err(),
        "validation must not be bypassable via a config document"
    );
}
