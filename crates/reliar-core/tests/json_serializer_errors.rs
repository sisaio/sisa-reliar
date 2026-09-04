//! Contract re-check finding — `JsonError::Display` must never print a fragment of the
//! rejected value. `serde_json::Error`'s own `Display` embeds the offending value for a data
//! error (e.g. `invalid type: string "sk-live-XYZ", expected u64`), so `JsonError` renders only
//! the error's classification and line/column, keeping the full `serde_json::Error` reachable
//! via `source()` for a caller that deliberately wants it (§33).

mod common;

use std::error::Error as _;

use common::OrderCreated;
use reliar_core::{JsonSerializer, Serializer};

#[test]
fn deserialize_error_display_never_contains_the_rejected_value() {
    let secret = "sk-live-not-a-real-secret-but-treat-it-like-one";
    let malformed = format!(r#"{{"order_id":"{secret}"}}"#);

    let error = JsonSerializer
        .deserialize::<OrderCreated>(malformed.as_bytes())
        .expect_err("a string where a u64 is expected must fail to deserialize");

    let rendered = error.to_string();
    assert!(
        !rendered.contains(secret),
        "JsonError::Display must never embed the rejected value: {rendered}"
    );
    // A string where a u64 is expected is a `serde_json::error::Category::Data` error — assert
    // the actual classification, not just that *some* word from the format string is present.
    assert!(
        rendered.contains("data error"),
        "expected the `data` classification, got: {rendered}"
    );
    assert!(
        rendered.contains("line") && rendered.contains("column"),
        "expected a line/column position, got: {rendered}"
    );
}

#[test]
fn deserialize_error_exposes_the_full_serde_error_via_source() {
    let malformed = b"not json at all";

    let error = JsonSerializer
        .deserialize::<OrderCreated>(malformed)
        .expect_err("non-JSON bytes must fail to deserialize");

    assert!(
        error.source().is_some(),
        "the underlying serde_json::Error must remain reachable via source()"
    );
}

#[test]
fn serialize_is_infallible_for_a_well_formed_message() {
    let bytes = JsonSerializer
        .serialize(&OrderCreated { order_id: 1 })
        .unwrap();
    assert_eq!(bytes.as_ref(), br#"{"order_id":1}"#);
}
