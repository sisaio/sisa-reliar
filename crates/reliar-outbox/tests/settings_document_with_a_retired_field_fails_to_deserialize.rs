//! A `serde` document still carrying the retired `enabled` field fails to deserialize, naming the
//! field — a retired durability key must never be silently ignored (ADR 0036 §7, contract §7,
//! E13).
#![cfg(feature = "serde")]

use reliar_outbox::OutboxSettings;

#[test]
fn a_document_carrying_enabled_fails_and_names_the_field() {
    let json = serde_json::json!({ "enabled": false });
    let err = serde_json::from_value::<OutboxSettings>(json)
        .expect_err("deny_unknown_fields must reject the retired `enabled` key");
    let text = err.to_string();
    assert!(
        text.contains("enabled"),
        "the error must name the offending field: {text}"
    );
}

#[test]
fn documents_carrying_the_other_two_retired_fields_also_fail() {
    for field in ["allowed_types", "disallowed_types"] {
        let json = serde_json::json!({ field: [] });
        let err = serde_json::from_value::<OutboxSettings>(json)
            .expect_err("deny_unknown_fields must reject every retired key");
        let text = err.to_string();
        assert!(text.contains(field), "must name `{field}`: {text}");
    }
}
