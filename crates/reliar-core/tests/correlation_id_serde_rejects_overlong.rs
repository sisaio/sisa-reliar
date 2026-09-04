//! Review 1, minor 7 — `CorrelationId`'s `Deserialize` impl reuses [`CorrelationId::parse`], so an
//! over-length value read back from a persisted blob is rejected exactly like one passed to
//! `parse` directly, rather than being accepted verbatim. Requires the `serde` feature.

use reliar_core::CorrelationId;

#[test]
fn deserializing_an_overlong_value_fails() {
    let overlong = "x".repeat(CorrelationId::MAX_LEN + 1);
    let json = serde_json::to_string(&overlong).unwrap();

    let result: Result<CorrelationId, _> = serde_json::from_str(&json);

    assert!(
        result.is_err(),
        "a correlation id over MAX_LEN must not deserialize"
    );
}

#[test]
fn deserializing_a_value_at_the_cap_succeeds() {
    let at_cap = "x".repeat(CorrelationId::MAX_LEN);
    let json = serde_json::to_string(&at_cap).unwrap();

    let parsed: CorrelationId = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.as_str(), at_cap);
}
