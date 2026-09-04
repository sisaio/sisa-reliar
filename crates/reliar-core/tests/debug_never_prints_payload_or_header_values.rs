//! Review 1, blocker 1 + majors 5/9 — no `Debug` impl on a Reliar type that holds a payload or a
//! custom header value may print it (SRS §33, conventions §9). This is a security guarantee, not
//! an incidental formatting choice, so it gets its own regression test rather than relying on
//! visual inspection of other tests' output.
//!
//! Review 2, blocker 1 — a body-field assertion must never use a marker that could accidentally
//! appear inside a `MessageId`'s rendered `UUIDv7` (all hex digits + hyphens). Every marker below
//! contains a letter outside `[0-9a-f-]` (e.g. `z`), so it can never be a substring of a UUID.

mod common;

use common::OrderCreated;
use reliar_core::{Envelope, Headers, JsonError, Message, Serializer};

const SECRET_VALUE: &str = "sk-live-not-a-real-secret-but-treat-it-like-one";
const SECRET_PAYLOAD_MARKER: &str = "top-secret-order-42-marker";
/// Deliberately contains `z`/`q`/`w`, none of which are hex digits, so it can never collide with
/// a rendered `UUIDv7`.
const SECRET_BODY_MARKER: &str = "top-secret-quiz-answer-zzz";

/// A message body carrying a non-numeric, non-hex marker string, so an assertion that the body
/// never leaks cannot be confused with an unrelated `UUIDv7` id also present in the rendered
/// `Debug` output.
#[derive(serde::Serialize, serde::Deserialize)]
struct MarkerBody {
    marker: String,
}

impl Message for MarkerBody {
    const TYPE: &'static str = "test.marker-body";
    const VERSION: u16 = 1;
}

#[test]
fn headers_debug_shows_keys_but_redacts_every_value() {
    let mut headers = Headers::default();
    headers.insert("x-api-key", SECRET_VALUE).unwrap();

    let rendered = format!("{headers:?}");

    assert!(
        rendered.contains("x-api-key"),
        "keys stay visible: {rendered}"
    );
    assert!(
        !rendered.contains(SECRET_VALUE),
        "a header value must never appear in Debug output: {rendered}"
    );
    assert!(
        rendered.contains("<redacted>"),
        "expected the redaction placeholder in {rendered}"
    );
}

#[test]
fn typed_envelope_debug_elides_the_body_and_redacts_header_values() {
    let envelope = Envelope::builder(MarkerBody {
        marker: SECRET_BODY_MARKER.to_string(),
    })
    .header("x-api-key", SECRET_VALUE)
    .unwrap()
    .build();

    let rendered = format!("{envelope:?}");

    assert!(
        rendered.contains("<elided>"),
        "body must be elided: {rendered}"
    );
    assert!(
        !rendered.contains(SECRET_BODY_MARKER),
        "a typed body field must never leak through Debug: {rendered}"
    );
    assert!(
        !rendered.contains(SECRET_VALUE),
        "a header value must never leak through Envelope's Debug: {rendered}"
    );
}

#[test]
fn serialized_envelope_debug_elides_raw_payload_bytes() {
    let serialized = Envelope::builder(OrderCreated { order_id: 7 })
        .build()
        .map_body(|_| bytes::Bytes::from_static(SECRET_PAYLOAD_MARKER.as_bytes()));

    let rendered = format!("{serialized:?}");

    assert!(
        rendered.contains("<elided>"),
        "serialized body must be elided: {rendered}"
    );
    assert!(
        !rendered.contains(SECRET_PAYLOAD_MARKER),
        "raw payload bytes must never leak through Debug: {rendered}"
    );
}

#[test]
fn envelope_builder_debug_elides_the_in_progress_body() {
    let builder = Envelope::builder(MarkerBody {
        marker: SECRET_BODY_MARKER.to_string(),
    });

    let rendered = format!("{builder:?}");

    assert!(
        rendered.contains("<elided>"),
        "an in-progress builder's body must be elided: {rendered}"
    );
    assert!(
        !rendered.contains(SECRET_BODY_MARKER),
        "an in-progress builder must never leak the body's field values: {rendered}"
    );
}

/// Review 2, blocker 2 — `JsonError` derives `Debug`, and a naive derive over
/// `serde_json::Error` would embed that error's own message, which for a data error contains a
/// fragment of the rejected value. `JsonError`'s manual `Debug` must print only the variant name
/// and the classification/position, never the payload.
#[test]
fn json_error_debug_never_contains_the_rejected_value() {
    let malformed = format!(r#"{{"order_id":"{SECRET_VALUE}"}}"#);

    let error = reliar_core::JsonSerializer
        .deserialize::<OrderCreated>(malformed.as_bytes())
        .expect_err("a string where a u64 is expected must fail to deserialize");

    let rendered = format!("{error:?}");
    assert!(
        !rendered.contains(SECRET_VALUE),
        "JsonError::Debug must never embed the rejected value: {rendered}"
    );
    let _: &JsonError = &error;
}
