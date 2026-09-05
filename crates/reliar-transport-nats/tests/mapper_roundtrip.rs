//! `decode(encode(e)) == e` for any envelope whose custom header keys are in §2.5's canonical
//! class (contract §2.5 "Round-trip"), every canonical field appears exactly once and never in
//! `Headers`, and the documented normalisations (U1, U2, U4, U8 — story C1). Contract §2.5 lists
//! **three**: (1) a `deduplication_id` equal to the message id, (2) a custom `traceparent`/
//! `tracestate` (any casing) moving into `metadata.trace` — both exercised below and in
//! `mapper_framework_collision.rs` — and (3) `Some(<empty Headers>)` decoding as `None`, which is
//! a **core** artefact (`Envelope::headers_mut()` lazily allocates `Some(empty)`, tracked as
//! RELIAR-36) rather than a mapper behaviour. This generator never emits `Some(empty)` in the
//! first place — `arb_envelope`'s custom-header map can be empty, but the builder loop below only
//! calls `.header(...)` for entries that actually exist, so an empty map never allocates
//! `envelope.headers` at all — which is what keeps `decode(encode(e)) == e` an exact identity
//! rather than needing a third special case here.
//!
//! `allow-expect-in-tests` (clippy.toml) only recognises `#[test]`-attributed functions; the
//! proptest generator below is plain functions/closures, so the same allowance is granted here
//! explicitly instead.
#![allow(clippy::expect_used)]

mod common;

use bytes::Bytes;
use proptest::prelude::*;
use reliar_core::{
    ContentType, ConversationId, CorrelationId, EndpointAddress, Envelope, EnvelopeMapper, Headers,
    MessageId, Metadata, RequestId,
};
use reliar_transport_nats::{NatsEnvelopeMapper, headers};
use uuid::Uuid;

use common::OrderCreated;

/// Printable ASCII, no `\r`/`\n`/control characters — legal for any NATS header *value* and for
/// every unvalidated core `String` field this crate writes onto the wire.
fn safe_value() -> impl Strategy<Value = String> {
    "[ -~]{0,24}"
}

/// Same alphabet as [`safe_value`] but never empty — required for the core newtypes
/// (`CorrelationId`, `EndpointAddress`) that reject an empty string.
fn safe_nonempty_value() -> impl Strategy<Value = String> {
    "[ -~]{1,24}"
}

/// A custom header key: ASCII-graphic without `:` (NATS's own requirement), and never starting
/// with `reliar-`/`Nats-` case-insensitively, and never case-insensitively equal to `traceparent`/
/// `tracestate` (§2.5's canonical key class — core already forbids `reliar-`; the rest is this
/// generator's own job so a *valid, identity* round trip is what gets tested here). A collision
/// with a framework name — or with `Nats-`, which core does not forbid either — is a documented
/// *normalisation*, not an identity, and is covered by its own dedicated tests instead
/// (`mapper_framework_collision.rs`, `mapper_encode_errors.rs`, minor 11).
fn safe_custom_key() -> impl Strategy<Value = String> {
    "[a-zA-Z][a-zA-Z0-9_-]{0,15}".prop_filter("must not collide with a reserved prefix", |k| {
        let lower = k.to_ascii_lowercase();
        !lower.starts_with("nats-")
            && !lower.starts_with("reliar-")
            && lower != "traceparent"
            && lower != "tracestate"
    })
}

/// A custom header key, occasionally exercising the exact [`Headers::MAX_KEY_LEN`] boundary — the
/// round trip must hold there too, not only for short keys (minor 11).
fn custom_key() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => safe_custom_key(),
        1 => Just("k".repeat(Headers::MAX_KEY_LEN)),
    ]
}

/// A custom header value, occasionally exercising the exact [`Headers::MAX_VALUE_LEN`] boundary
/// (minor 11).
fn custom_value() -> impl Strategy<Value = String> {
    prop_oneof![
        4 => safe_value(),
        1 => Just("v".repeat(Headers::MAX_VALUE_LEN)),
    ]
}

/// A handful of representative content types, including one with a media-type parameter and a
/// non-JSON type — the round trip must hold for more than this crate's own JSON default
/// (minor 11).
fn arb_content_type() -> impl Strategy<Value = ContentType> {
    prop_oneof![
        Just(ContentType::JSON),
        Just(ContentType::parse("application/json; charset=utf-8").expect("legal content type")),
        Just(ContentType::parse("application/x-protobuf").expect("legal content type")),
        Just(ContentType::parse("text/plain; charset=utf-8").expect("legal content type")),
    ]
}

fn arb_uuid() -> impl Strategy<Value = Uuid> {
    any::<[u8; 16]>().prop_map(Uuid::from_bytes)
}

/// A handful of representative RFC 3339 instants, including a non-UTC offset and sub-second
/// digits (U8) — full arbitrary-instant coverage is not the point of this generator, the fixed
/// conversion-to-UTC behaviour is.
fn arb_offset_date_time() -> impl Strategy<Value = time::OffsetDateTime> {
    prop_oneof![
        Just(time::macros::datetime!(2026-09-04 12:00:00 UTC)),
        Just(time::macros::datetime!(2026-01-01 00:00:00.123456 UTC)),
        Just(time::macros::datetime!(2026-09-04 12:00:00 +05:30)),
        Just(time::macros::datetime!(2025-12-31 23:59:59.9 -08:00)),
    ]
}

/// Builds an arbitrary [`reliar_core::SerializedEnvelope`] exercising every field the mapper
/// projects, restricted to the canonical key/value classes §2.5 promises a lossless round trip
/// for.
fn arb_envelope() -> impl Strategy<Value = reliar_core::SerializedEnvelope> {
    (
        arb_uuid(),
        proptest::option::of(safe_nonempty_value()),
        proptest::option::of(arb_uuid()),
        proptest::option::of(arb_uuid()),
        proptest::option::of(arb_uuid()),
        proptest::option::of(safe_value()),
    )
        .prop_flat_map(
            |(id_bytes, correlation_id, conversation_id, causation_id, request_id, tenant_id)| {
                (
                    proptest::option::of(arb_offset_date_time()),
                    proptest::option::of(arb_offset_date_time()),
                    proptest::option::of(safe_nonempty_value()),
                    proptest::option::of(safe_nonempty_value()),
                    proptest::option::of(safe_nonempty_value()),
                    proptest::option::of(safe_value()),
                    proptest::option::of(safe_value()),
                    proptest::option::of(safe_value()),
                    // A `HashMap`, not a `Vec`, so up to `Headers::MAX_COUNT` (32) keys are
                    // always distinct — exercising the count boundary the crate documents, not
                    // merely a handful of headers every run (minor 11).
                    proptest::collection::hash_map(
                        custom_key(),
                        custom_value(),
                        0..=Headers::MAX_COUNT,
                    ),
                    safe_value(),
                    arb_content_type(),
                )
                    .prop_map(
                        move |(
                            sent_at,
                            expires_at,
                            source,
                            destination,
                            reply_to,
                            traceparent,
                            tracestate,
                            deduplication_id,
                            custom,
                            body,
                            content_type,
                        )| {
                            let mut metadata = Metadata::default();
                            metadata.correlation.correlation_id = correlation_id.clone().map(|v| {
                                CorrelationId::parse(v)
                                    .expect("safe_value is a legal correlation id")
                            });
                            metadata.correlation.conversation_id = conversation_id
                                .map_or(ConversationId::UNSET, ConversationId::from_uuid);
                            metadata.correlation.causation_id =
                                causation_id.map(MessageId::from_uuid);
                            metadata.correlation.request_id = request_id.map(RequestId::from_uuid);
                            metadata.tenant_id.clone_from(&tenant_id);
                            metadata.delivery.content_type = content_type;
                            metadata.delivery.sent_at = sent_at;
                            metadata.delivery.expires_at = expires_at;
                            metadata.delivery.deduplication_id = deduplication_id;
                            metadata.routing.source = source
                                .map(|v| EndpointAddress::parse(v).expect("safe_value is legal"));
                            metadata.routing.destination = destination
                                .map(|v| EndpointAddress::parse(v).expect("safe_value is legal"));
                            metadata.routing.reply_to = reply_to
                                .map(|v| EndpointAddress::parse(v).expect("safe_value is legal"));
                            metadata.trace.traceparent = traceparent;
                            metadata.trace.tracestate = tracestate;

                            let mut builder =
                                Envelope::builder(OrderCreated { order_id: 1 }).metadata(metadata);
                            for (key, value) in &custom {
                                builder = builder
                                    .header(key.clone(), value.clone())
                                    .expect("custom_key/custom_value are always at or under the limits Headers::insert enforces");
                            }
                            builder
                                .id(MessageId::from_uuid(id_bytes))
                                .build()
                                .map_body(|_| Bytes::from(body.into_bytes()))
                        },
                    )
            },
        )
}

proptest! {
    #[test]
    fn decode_of_encode_is_the_identity(envelope in arb_envelope()) {
        let mapper = NatsEnvelopeMapper::default();
        let wire = mapper.encode(&envelope).expect("arb_envelope only generates encodable envelopes");
        let decoded = mapper.decode(wire).expect("a message this mapper encoded always decodes");
        prop_assert_eq!(decoded, envelope);
    }

    /// U2: every canonical field is written exactly once as a header (framework headers use
    /// `HeaderMap::insert`, which replaces rather than appends), and none of it ever lands in
    /// the decoded custom `Headers`.
    #[test]
    fn every_canonical_field_is_written_exactly_once_and_never_in_headers(envelope in arb_envelope()) {
        let mapper = NatsEnvelopeMapper::default();
        let wire = mapper.encode(&envelope).expect("arb_envelope only generates encodable envelopes");

        for name in [
            headers::MESSAGE_ID,
            headers::MESSAGE_TYPE,
            headers::MESSAGE_VERSION,
            headers::CONTENT_TYPE,
            headers::NATS_MSG_ID,
        ] {
            let mut count = 0usize;
            for (found, values) in wire.headers.iter() {
                let found: &str = found.as_ref();
                if found.eq_ignore_ascii_case(name) {
                    count += 1;
                    prop_assert_eq!(values.len(), 1);
                }
            }
            prop_assert_eq!(count, 1, "header {:?} must appear exactly once", name);
        }

        let decoded = mapper.decode(wire).expect("a message this mapper encoded always decodes");
        if let Some(custom) = decoded.headers() {
            for name in [
                headers::MESSAGE_ID,
                headers::MESSAGE_TYPE,
                headers::MESSAGE_VERSION,
                headers::CONTENT_TYPE,
                headers::CORRELATION_ID,
                headers::CONVERSATION_ID,
                headers::CAUSATION_ID,
                headers::REQUEST_ID,
                headers::TENANT_ID,
                headers::SENT_AT,
                headers::EXPIRES_AT,
                headers::SOURCE,
                headers::DESTINATION,
                headers::REPLY_TO,
                headers::TRACEPARENT,
                headers::TRACESTATE,
            ] {
                prop_assert!(custom.get(name).is_none());
            }
        }
    }

    /// U3/major 3: the wire payload is byte-identical to `envelope.body` — never re-wrapped,
    /// never re-encoded (SRS §16).
    #[test]
    fn wire_payload_is_byte_identical_to_the_body(envelope in arb_envelope()) {
        let mapper = NatsEnvelopeMapper::default();
        let wire = mapper.encode(&envelope).expect("arb_envelope only generates encodable envelopes");
        prop_assert_eq!(wire.payload, envelope.body);
    }
}

/// U3/major 3: byte-identical also holds for a body that is not valid UTF-8 — the payload is raw
/// bytes, never assumed to be text.
#[test]
fn wire_payload_is_byte_identical_for_a_non_utf8_body() {
    let mapper = NatsEnvelopeMapper::default();
    let non_utf8 = Bytes::from_static(&[0xFF, 0x00, 0xC0, 0xAF, 0xFE]);
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| non_utf8.clone());

    let wire = mapper
        .encode(&envelope)
        .expect("a non-UTF-8 body is not a mapper concern");
    assert_eq!(wire.payload, non_utf8);

    let decoded = mapper.decode(wire).expect("round trips");
    assert_eq!(decoded.body, non_utf8);
}

/// U3: with no `deduplication_id` set, `Nats-Msg-Id`'s **value** is the message id (not merely
/// present) — this is what `JetStream` actually deduplicates on.
#[test]
fn nats_msg_id_value_is_the_message_id_when_no_dedup_id_is_set() {
    let mapper = NatsEnvelopeMapper::default();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    assert!(envelope.metadata.delivery.deduplication_id.is_none());

    let wire = mapper.encode(&envelope).expect("encodable");
    let nats_msg_id = wire
        .headers
        .get(headers::NATS_MSG_ID)
        .expect("always written")
        .as_str();
    assert_eq!(nats_msg_id, envelope.id.to_string());
}

/// U3: with a `deduplication_id` set, `Nats-Msg-Id`'s value is that id, not the message id — and a
/// dedup id genuinely distinct from the message id survives the round trip unchanged (major 4;
/// complements `a_dedup_id_equal_to_the_message_id_decodes_as_unset`, which covers the
/// normalisation case).
#[test]
fn nats_msg_id_value_is_the_dedup_id_when_set_and_it_survives_the_round_trip() {
    let mapper = NatsEnvelopeMapper::default();
    let mut metadata = Metadata::default();
    metadata.delivery.deduplication_id = Some("distinct-dedup-key".to_string());
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .metadata(metadata)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let wire = mapper.encode(&envelope).expect("encodable");
    let nats_msg_id = wire
        .headers
        .get(headers::NATS_MSG_ID)
        .expect("always written")
        .as_str();
    assert_eq!(nats_msg_id, "distinct-dedup-key");
    assert_ne!(nats_msg_id, envelope.id.to_string());

    let decoded = mapper.decode(wire).expect("decodable");
    assert_eq!(
        decoded.metadata.delivery.deduplication_id.as_deref(),
        Some("distinct-dedup-key")
    );
}

/// U4: a `traceparent` supplied as a *custom* header (not through `Metadata::trace`) still comes
/// back in `metadata.trace`, never in `Headers` — the one collision the projection table allows.
#[test]
fn a_custom_traceparent_header_decodes_into_metadata_trace() {
    let mapper = NatsEnvelopeMapper::default();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .header("traceparent", "00-trace-01")
        .expect("a plain ASCII key/value is always accepted")
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let wire = mapper
        .encode(&envelope)
        .expect("no framework traceparent set, so the custom one reaches the wire");
    let decoded = mapper.decode(wire).expect("round trips");

    assert_eq!(
        decoded.metadata.trace.traceparent.as_deref(),
        Some("00-trace-01")
    );
    assert!(
        decoded
            .headers()
            .is_none_or(|h| h.get("traceparent").is_none())
    );
}

/// U4: a `deduplication_id` equal to the message id decodes back as `None` — encode always
/// writes `Nats-Msg-Id`, so this is the only way "no dedup id was set" survives the round trip.
#[test]
fn a_dedup_id_equal_to_the_message_id_decodes_as_unset() {
    let mapper = NatsEnvelopeMapper::default();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    assert!(envelope.metadata.delivery.deduplication_id.is_none());

    let wire = mapper.encode(&envelope).expect("encodable");
    let decoded = mapper.decode(wire).expect("decodable");
    assert_eq!(decoded.metadata.delivery.deduplication_id, None);
}
