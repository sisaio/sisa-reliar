//! U14 (contract §7, ADR 0026 Amendment A, S1 review blocker 1): a custom header whose key
//! case-insensitively names one of the two unprefixed W3C trace headers — the one collision core's
//! `reliar-` prefix reservation does not catch — is arbitrated by `encode` itself, never left for
//! `decode` to reject as a `DuplicateHeader`.

mod common;

use bytes::Bytes;
use reliar_core::{Envelope, EnvelopeMapper};
use reliar_transport_nats::{NatsEnvelopeMapper, NatsMapError, headers};

use common::OrderCreated;

/// Branch 1: `metadata.trace.traceparent` is `Some` **and** a case-variant custom `TraceParent` is
/// also set — the framework value wins, the custom entry is dropped, and `decode(encode(e))`
/// succeeds with exactly one `traceparent` header on the wire (never `DuplicateHeader`).
#[test]
fn a_case_variant_custom_traceparent_is_dropped_when_the_framework_value_is_set() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .trace("00-framework-trace", None)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("TraceParent", "00-custom-trace")
        .expect("a plain ASCII key/value is always accepted by core's Headers");

    let mapper = NatsEnvelopeMapper::default();
    let wire = mapper
        .encode(&envelope)
        .expect("the collision is arbitrated, never a hard error");

    let mut traceparent_count = 0usize;
    for (name, values) in wire.headers.iter() {
        let name: &str = name.as_ref();
        if name.eq_ignore_ascii_case(headers::TRACEPARENT) {
            traceparent_count += 1;
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].as_str(), "00-framework-trace");
        }
    }
    assert_eq!(
        traceparent_count, 1,
        "encode must write at most one header per framework name"
    );

    let decoded = mapper
        .decode(wire)
        .expect("a message this mapper encoded always decodes — never DuplicateHeader");
    assert_eq!(
        decoded.metadata.trace.traceparent.as_deref(),
        Some("00-framework-trace")
    );
    assert!(
        decoded
            .headers()
            .is_none_or(|h| h.get("TraceParent").is_none())
    );
}

/// Branch 2: `metadata.trace.traceparent` is unset and a single custom `TraceParent` is set — the
/// custom value reaches the wire, but under the **canonical lowercase** name, and decodes into
/// `metadata.trace`, never into `Headers`.
#[test]
fn a_case_variant_custom_traceparent_is_written_under_the_canonical_name_when_unset() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("TraceParent", "00-custom-trace")
        .expect("a plain ASCII key/value is always accepted by core's Headers");
    assert!(envelope.metadata.trace.traceparent.is_none());

    let mapper = NatsEnvelopeMapper::default();
    let wire = mapper
        .encode(&envelope)
        .expect("no framework value to override it");

    let mut found_canonical = false;
    for (name, values) in wire.headers.iter() {
        let name: &str = name.as_ref();
        if name == headers::TRACEPARENT {
            found_canonical = true;
            assert_eq!(values.len(), 1);
            assert_eq!(values[0].as_str(), "00-custom-trace");
        } else {
            assert!(
                !name.eq_ignore_ascii_case(headers::TRACEPARENT),
                "the caller's original casing {name:?} must not also reach the wire"
            );
        }
    }
    assert!(
        found_canonical,
        "the canonical lowercase name must be written"
    );

    let decoded = mapper.decode(wire).expect("round trips");
    assert_eq!(
        decoded.metadata.trace.traceparent.as_deref(),
        Some("00-custom-trace")
    );
    assert!(decoded.headers().is_none());
}

/// Branch 3: two custom keys normalising to the same framework name, with no framework value to
/// arbitrate — a permanent `DuplicateHeader` naming the canonical spelling. `Headers` is a map, so
/// picking one arbitrarily would make the wire depend on hash-iteration order.
#[test]
fn two_case_variant_custom_traceparents_with_no_framework_value_is_a_duplicate_header_error() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("traceparent", "00-a")
        .expect("legal");
    envelope
        .headers_mut()
        .insert("TRACEPARENT", "00-b")
        .expect("legal — a distinct key in core's Headers, which is case-sensitive");

    let err = NatsEnvelopeMapper::default()
        .encode(&envelope)
        .expect_err("two spellings of one framework header with nothing to arbitrate them");
    assert_eq!(
        err,
        NatsMapError::DuplicateHeader {
            header: headers::TRACEPARENT
        }
    );
}

/// Decode is unchanged: a case-variant framework header arriving from a **foreign** producer (not
/// this mapper's own `encode`) is recognised into `Metadata`, never treated as a custom header.
#[test]
fn a_foreign_case_variant_framework_header_decodes_into_metadata_not_headers() {
    let mut wire_headers = async_nats::HeaderMap::new();
    wire_headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_ID),
        "0198f1c0-0000-7000-8000-000000000001",
    );
    wire_headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_TYPE),
        "orders.created",
    );
    wire_headers.insert(
        async_nats::HeaderName::from_static(headers::MESSAGE_VERSION),
        "1",
    );
    wire_headers.insert(
        async_nats::HeaderName::from_static(headers::CONTENT_TYPE),
        "application/json",
    );
    wire_headers.insert(
        async_nats::HeaderName::from_static(headers::NATS_MSG_ID),
        "0198f1c0-0000-7000-8000-000000000001",
    );
    wire_headers.insert(common::header_name("TraceParent"), "00-foreign");

    let decoded = NatsEnvelopeMapper::default()
        .decode(reliar_transport_nats::NatsWireMessage::new(
            wire_headers,
            Bytes::from_static(b"{}"),
        ))
        .expect("a case-variant framework header from a foreign producer is recognised");

    assert_eq!(
        decoded.metadata.trace.traceparent.as_deref(),
        Some("00-foreign")
    );
    assert!(decoded.headers().is_none());
}

/// Same three branches for `tracestate`, the other unprefixed W3C header (ADR 0026 Amendment A
/// covers both by the same rule).
#[test]
fn a_case_variant_custom_tracestate_is_dropped_when_the_framework_value_is_set() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .trace("00-trace", Some("framework-state".to_string()))
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("TraceState", "custom-state")
        .expect("legal");

    let mapper = NatsEnvelopeMapper::default();
    let wire = mapper
        .encode(&envelope)
        .expect("arbitrated, not a hard error");
    let decoded = mapper.decode(wire).expect("no DuplicateHeader");
    assert_eq!(
        decoded.metadata.trace.tracestate.as_deref(),
        Some("framework-state")
    );
}

#[test]
fn distinct_metadata_traceparent_and_tracestate_collisions_are_independent() {
    // Regression guard: setting `metadata` at all must not accidentally mark both trace fields
    // as "framework value set" — only the one actually populated overrides its own collision.
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .trace("00-framework-trace", None)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    assert!(envelope.metadata.trace.tracestate.is_none());
    envelope
        .headers_mut()
        .insert("TraceState", "custom-state")
        .expect("legal");

    let mapper = NatsEnvelopeMapper::default();
    let wire = mapper
        .encode(&envelope)
        .expect("tracestate has no framework value, so it is written under the canonical name");
    let decoded = mapper.decode(wire).expect("round trips");
    assert_eq!(
        decoded.metadata.trace.traceparent.as_deref(),
        Some("00-framework-trace")
    );
    assert_eq!(
        decoded.metadata.trace.tracestate.as_deref(),
        Some("custom-state")
    );
}
