//! No `Debug`, `Display`, or captured `tracing` transcript on any failure path in this crate ever
//! contains payload bytes or a header **value** (§17.1, §43.A.26). These scenarios exercise the
//! pure `NatsEnvelopeMapper`/`SubjectResolver` paths, which emit no `tracing` spans of their own
//! — `NatsPublisher`'s span/`warn` coverage (with a live `Context`) lives in `tests/nats/`
//! (U13/N10); the substantive assertions here are the direct `Debug`/`Display` checks.

mod common;

use bytes::Bytes;
use reliar_core::{EnvelopeMapper, Headers, MessageId, MessageType, Metadata, SerializedEnvelope};
use reliar_transport_nats::{
    NatsEnvelopeMapper, NatsWireMessage, PrefixSubjects, SubjectResolver, headers,
};

use common::header_name;

const SECRET_PAYLOAD_MARKER: &str = "sk_live_RELIAR_PAYLOAD_MUST_NEVER_APPEAR_IN_A_LOG";
const SECRET_HEADER_VALUE: &str = "RELIAR_HEADER_VALUE_MUST_NEVER_APPEAR_IN_A_LOG";

#[test]
fn nats_wire_message_debug_elides_header_values_and_the_payload() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        header_name("X-Secret"),
        common::header_value(SECRET_HEADER_VALUE),
    );
    let wire = NatsWireMessage::new(
        headers,
        Bytes::from(format!("{{\"secret\":\"{SECRET_PAYLOAD_MARKER}\"}}")),
    );

    let debug = format!("{wire:?}");
    assert!(
        debug.contains("X-Secret"),
        "the header name is expected to be visible:\n{debug}"
    );
    assert!(
        !debug.contains(SECRET_HEADER_VALUE),
        "a header value leaked into Debug:\n{debug}"
    );
    assert!(
        !debug.contains(SECRET_PAYLOAD_MARKER),
        "the payload leaked into Debug:\n{debug}"
    );

    let text = recorder.text();
    assert!(
        !text.contains(SECRET_HEADER_VALUE),
        "a header value leaked into the tracing transcript:\n{text}"
    );
    assert!(
        !text.contains(SECRET_PAYLOAD_MARKER),
        "the payload leaked into the tracing transcript:\n{text}"
    );
}

#[test]
fn a_rejected_custom_header_error_never_prints_its_value() {
    // `RejectedHeader` carries the offending *key* (intended — configuration, not data) but must
    // never carry, in `Debug` or `Display`, the value core's `Headers::insert` refused.
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_ID),
        common::header_value("0198f1c0-0000-7000-8000-000000000001"),
    );
    headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_TYPE),
        common::header_value("orders.created"),
    );
    headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_VERSION),
        common::header_value("1"),
    );
    headers.insert(
        async_nats::HeaderName::from_static(headers::CONTENT_TYPE),
        common::header_value("application/json"),
    );
    let overlong_value = "v".repeat(Headers::MAX_VALUE_LEN + 1);
    headers.insert(
        header_name("x-custom"),
        common::header_value(&overlong_value),
    );
    let wire = NatsWireMessage::new(headers, Bytes::from_static(b"{}"));

    let err = NatsEnvelopeMapper::default()
        .decode(wire)
        .expect_err("an over-length custom header value is rejected");

    let debug = format!("{err:?}");
    let display = err.to_string();
    assert!(
        !debug.contains(&overlong_value),
        "the value leaked into Debug:\n{debug}"
    );
    assert!(
        !display.contains(&overlong_value),
        "the value leaked into Display:\n{display}"
    );
    assert!(
        debug.contains("x-custom"),
        "the key is expected to be visible:\n{debug}"
    );
}

#[test]
fn a_malformed_header_error_never_prints_the_offending_value() {
    let mut headers = async_nats::HeaderMap::new();
    headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_ID),
        common::header_value(SECRET_HEADER_VALUE),
    );
    headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_TYPE),
        common::header_value("orders.created"),
    );
    headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_VERSION),
        common::header_value("1"),
    );
    headers.insert(
        async_nats::HeaderName::from_static(headers::CONTENT_TYPE),
        common::header_value("application/json"),
    );
    let wire = NatsWireMessage::new(headers, Bytes::from_static(b"{}"));

    let err = NatsEnvelopeMapper::default()
        .decode(wire)
        .expect_err("not a UUID");

    let debug = format!("{err:?}");
    let display = err.to_string();
    assert!(
        !debug.contains(SECRET_HEADER_VALUE),
        "the value leaked into Debug:\n{debug}"
    );
    assert!(
        !display.contains(SECRET_HEADER_VALUE),
        "the value leaked into Display:\n{display}"
    );
}

/// The `SubjectError` must come from a call that actually saw the secret-carrying envelope, not
/// an unrelated construction error (S1 review, major 6) — a wildcard token in the message type
/// makes `PrefixSubjects::subject` reject the very envelope it was just handed.
#[test]
fn a_subject_error_prints_the_subject_but_never_a_header_value() {
    let mut headers = Headers::default();
    headers
        .insert("x-secret", SECRET_HEADER_VALUE)
        .expect("a plain custom header is accepted");
    let envelope = SerializedEnvelope::from_parts(
        MessageId::new(),
        MessageType::from_parts("orders.*", 1),
        Bytes::from(format!("{{\"secret\":\"{SECRET_PAYLOAD_MARKER}\"}}")),
        Metadata::default(),
        Some(headers),
    );

    let resolver = PrefixSubjects::default();
    let err = resolver
        .subject(&envelope)
        .expect_err("a wildcard token in the message type is rejected");
    let display = err.to_string();

    // Positive half (ADR 0030): the subject is routing configuration, not user data, and is
    // intended to be visible — this is not merely a negative, non-leakage assertion.
    assert!(
        display.contains("reliar.orders.*.v1"),
        "the resolved subject is expected to be visible:\n{display}"
    );
    assert!(!display.contains(SECRET_HEADER_VALUE));
    assert!(!display.contains(SECRET_PAYLOAD_MARKER));
}
