//! §43.A.5 — `Headers::insert` rejects any key with the reserved `reliar-` prefix
//! (case-insensitive) and any key/value/count above the caps with an error; no framework
//! metadata value is ever stored in `Headers` (§13.1, §14).

use proptest::prelude::*;
use reliar_core::{HeaderError, Headers};

#[test]
fn rejects_the_reserved_prefix_case_insensitively() {
    for key in [
        "reliar-correlation-id",
        "Reliar-Correlation-Id",
        "RELIAR-TENANT-ID",
        "reliar-anything-not-yet-invented",
    ] {
        let mut headers = Headers::default();
        assert!(matches!(
            headers.insert(key, "value"),
            Err(HeaderError::Reserved { key: rejected }) if rejected == key
        ));
        assert!(
            headers.is_empty(),
            "a rejected insert must not land in the map"
        );
    }
}

#[test]
fn accepts_a_non_reserved_key() {
    let mut headers = Headers::default();
    assert!(
        headers
            .insert("x-import-batch", "2026-09-04")
            .unwrap()
            .is_none()
    );
    assert_eq!(headers.get("x-import-batch"), Some("2026-09-04"));
}

#[test]
fn remove_returns_the_value_and_drops_the_key() {
    let mut headers = Headers::default();
    headers.insert("x-a", "1").unwrap();

    assert_eq!(headers.remove("x-a"), Some("1".to_string()));
    assert_eq!(headers.get("x-a"), None);
    assert!(headers.is_empty());

    assert_eq!(headers.remove("x-a"), None, "removing twice yields None");
}

#[test]
fn iter_yields_every_stored_key_and_value_exactly_once() {
    let mut headers = Headers::default();
    headers.insert("x-a", "1").unwrap();
    headers.insert("x-b", "2").unwrap();

    let mut pairs: Vec<(&str, &str)> = headers.iter().collect();
    pairs.sort_unstable();

    assert_eq!(pairs, vec![("x-a", "1"), ("x-b", "2")]);
}

#[test]
fn rejects_a_control_character_in_the_key() {
    let mut headers = Headers::default();
    assert!(matches!(
        headers.insert("x-a\r\nX-Injected: true", "v"),
        Err(HeaderError::ControlCharacterInKey { .. })
    ));
}

#[test]
fn rejects_a_control_character_in_the_value() {
    let mut headers = Headers::default();
    assert!(matches!(
        headers.insert("x-a", "v\r\nX-Injected: true"),
        Err(HeaderError::ControlCharacterInValue { key }) if key == "x-a"
    ));
    assert!(
        headers.is_empty(),
        "a rejected insert must not land in the map"
    );
}

#[test]
fn accepts_near_miss_keys_that_are_not_the_reserved_prefix() {
    // Only the exact case-insensitive `reliar-` prefix is reserved; a near-miss must not be
    // caught by an overly broad match.
    for key in ["reliar", "reliar_x", "relia-x", "reliars-tenant"] {
        let mut headers = Headers::default();
        assert!(
            headers.insert(key, "value").is_ok(),
            "{key:?} is not the reserved prefix and must be accepted"
        );
    }
}

/// [`Headers::MAX_KEY_LEN`]/[`Headers::MAX_VALUE_LEN`] are enforced in **bytes**, not `char`s.
#[test]
fn key_and_value_caps_are_enforced_in_bytes_not_characters() {
    // 65 two-byte characters = 130 bytes: over `MAX_KEY_LEN` (128) in bytes, only 65 in chars.
    let multi_byte_key = "é".repeat(65);
    assert_eq!(multi_byte_key.chars().count(), 65);
    assert!(multi_byte_key.len() > Headers::MAX_KEY_LEN);

    let mut headers = Headers::default();
    assert!(matches!(
        headers.insert(multi_byte_key, "v"),
        Err(HeaderError::KeyTooLong { .. })
    ));
}

#[test]
fn rejects_an_empty_key() {
    let mut headers = Headers::default();
    assert!(matches!(
        headers.insert("", "value"),
        Err(HeaderError::EmptyKey)
    ));
}

#[test]
fn rejects_a_key_over_the_length_cap() {
    let mut headers = Headers::default();
    let key = "x".repeat(Headers::MAX_KEY_LEN + 1);
    assert!(matches!(
        headers.insert(key, "value"),
        Err(HeaderError::KeyTooLong { len }) if len == Headers::MAX_KEY_LEN + 1
    ));
}

#[test]
fn rejects_a_value_over_the_length_cap() {
    let mut headers = Headers::default();
    let value = "x".repeat(Headers::MAX_VALUE_LEN + 1);
    assert!(matches!(
        headers.insert("k", value),
        Err(HeaderError::ValueTooLong { len }) if len == Headers::MAX_VALUE_LEN + 1
    ));
}

#[test]
fn rejects_a_new_key_once_at_the_count_cap() {
    let mut headers = Headers::default();
    for i in 0..Headers::MAX_COUNT {
        headers.insert(format!("k{i}"), "v").unwrap();
    }
    assert!(matches!(
        headers.insert("one-too-many", "v"),
        Err(HeaderError::TooManyHeaders { limit }) if limit == Headers::MAX_COUNT
    ));
}

#[test]
fn replacing_an_existing_key_at_the_count_cap_succeeds() {
    let mut headers = Headers::default();
    for i in 0..Headers::MAX_COUNT {
        headers.insert(format!("k{i}"), "v").unwrap();
    }
    // Replacing a key that is already present must never be rejected as "too many": the count
    // does not change.
    let previous = headers.insert("k0", "v2").unwrap();
    assert_eq!(previous.as_deref(), Some("v"));
    assert_eq!(headers.get("k0"), Some("v2"));
    assert_eq!(headers.len(), Headers::MAX_COUNT);
}

proptest! {
    /// Any key matching `(?i)^reliar-` is rejected; any other key within the caps is accepted.
    #[test]
    fn reserved_prefix_is_rejected_regardless_of_case(
        suffix in "[a-zA-Z0-9-]{0,32}",
        mixed_case_prefix in "[rR][eE][lL][iI][aA][rR]-",
    ) {
        let key = format!("{mixed_case_prefix}{suffix}");
        let mut headers = Headers::default();
        let result = headers.insert(key, "v");
        let was_rejected_as_reserved = matches!(result, Err(HeaderError::Reserved { .. }));
        prop_assert!(was_rejected_as_reserved);
    }

    #[test]
    fn non_reserved_keys_within_caps_are_always_accepted(
        key in "[a-qs-zA-QS-Z0-9-]{1,128}",
        value in "[ -~]{0,64}",
    ) {
        // Excludes keys starting with `r`/`R` so the generated key can never accidentally spell
        // the reserved prefix.
        let mut headers = Headers::default();
        prop_assert!(headers.insert(key, value).is_ok());
    }
}
