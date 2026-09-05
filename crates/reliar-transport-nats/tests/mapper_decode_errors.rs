//! `decode` never panics: a missing or malformed required header is a permanent
//! [`NatsMapError`], an unrecognised `reliar-*`/`Nats-*` header is ignored, a repeated framework
//! header is a `DuplicateHeader`, and a repeated custom header keeps its first value (contract
//! §2.5, U5, U6 — story C3).
//!
//! `allow-expect-in-tests`/`allow-unwrap-in-tests` (clippy.toml) only recognise `#[test]`-
//! attributed functions; this file's fixture helpers are plain functions, so the same allowance
//! is granted here explicitly instead.
#![allow(clippy::expect_used)]

mod common;

use std::error::Error;

use async_nats::{HeaderMap, HeaderName};
use bytes::Bytes;
use reliar_core::{EnvelopeMapper, SerializedEnvelope};
use reliar_transport_nats::{NatsEnvelopeMapper, NatsMapError, NatsWireMessage, headers};

use common::header_name;

/// A header block carrying every field `decode` requires, so a single field can be removed or
/// corrupted per scenario without rebuilding the whole thing by hand.
fn complete_header_map() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        HeaderName::from_static(headers::MESSAGE_ID),
        common::header_value("0198f1c0-0000-7000-8000-000000000001"),
    );
    headers.insert(
        HeaderName::from_static(headers::MESSAGE_TYPE),
        common::header_value("orders.created"),
    );
    headers.insert(
        HeaderName::from_static(headers::MESSAGE_VERSION),
        common::header_value("1"),
    );
    headers.insert(
        HeaderName::from_static(headers::CONTENT_TYPE),
        common::header_value("application/json"),
    );
    headers.insert(
        HeaderName::from_static(headers::NATS_MSG_ID),
        common::header_value("0198f1c0-0000-7000-8000-000000000001"),
    );
    headers
}

fn decode(headers: HeaderMap) -> Result<SerializedEnvelope, NatsMapError> {
    let wire = NatsWireMessage::new(headers, Bytes::from_static(b"{}"));
    NatsEnvelopeMapper::default().decode(wire)
}

/// Builds a `HeaderMap` equal to `from` minus every entry named `key` (case-insensitively) — the
/// crate's own `HeaderMap` has no `remove`, so this rebuilds one via `insert`/`append`.
fn remove(from: &HeaderMap, key: &str) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, values) in from.iter() {
        let name_str: &str = name.as_ref();
        if name_str.eq_ignore_ascii_case(key) {
            continue;
        }
        for value in values {
            out.append(header_name(name_str), value.clone());
        }
    }
    out
}

#[test]
fn a_complete_header_block_decodes_successfully() {
    decode(complete_header_map()).expect("every required header is present and well formed");
}

#[test]
fn missing_message_id_is_a_permanent_missing_header_error() {
    let h = remove(&complete_header_map(), headers::MESSAGE_ID);
    let err = decode(h).expect_err("required header is absent");
    assert_eq!(
        err,
        NatsMapError::MissingHeader {
            header: headers::MESSAGE_ID
        }
    );
}

#[test]
fn missing_message_type_is_a_permanent_missing_header_error() {
    let h = remove(&complete_header_map(), headers::MESSAGE_TYPE);
    let err = decode(h).expect_err("required header is absent");
    assert_eq!(
        err,
        NatsMapError::MissingHeader {
            header: headers::MESSAGE_TYPE
        }
    );
}

#[test]
fn missing_message_version_is_a_permanent_missing_header_error() {
    let h = remove(&complete_header_map(), headers::MESSAGE_VERSION);
    let err = decode(h).expect_err("required header is absent");
    assert_eq!(
        err,
        NatsMapError::MissingHeader {
            header: headers::MESSAGE_VERSION
        }
    );
}

#[test]
fn missing_content_type_is_a_permanent_missing_header_error() {
    let h = remove(&complete_header_map(), headers::CONTENT_TYPE);
    let err = decode(h).expect_err("required header is absent");
    assert_eq!(
        err,
        NatsMapError::MissingHeader {
            header: headers::CONTENT_TYPE
        }
    );
}

#[test]
fn malformed_message_id_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::MESSAGE_ID);
    h.insert(
        HeaderName::from_static(headers::MESSAGE_ID),
        common::header_value("not-a-uuid"),
    );
    let err = decode(h).expect_err("not a UUID");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::MESSAGE_ID
        }
    );
}

#[test]
fn malformed_message_version_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::MESSAGE_VERSION);
    h.insert(
        HeaderName::from_static(headers::MESSAGE_VERSION),
        common::header_value("not-a-number"),
    );
    let err = decode(h).expect_err("not a u16");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::MESSAGE_VERSION
        }
    );
}

#[test]
fn malformed_content_type_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::CONTENT_TYPE);
    h.insert(
        HeaderName::from_static(headers::CONTENT_TYPE),
        common::header_value("not-a-mime-type"),
    );
    let err = decode(h).expect_err("not `type/subtype`");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::CONTENT_TYPE
        }
    );
}

#[test]
fn malformed_conversation_id_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(
        HeaderName::from_static(headers::CONVERSATION_ID),
        common::header_value("not-a-uuid"),
    );
    let err = decode(h).expect_err("not a UUID");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::CONVERSATION_ID
        }
    );
}

#[test]
fn malformed_sent_at_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(
        HeaderName::from_static(headers::SENT_AT),
        common::header_value("not-a-timestamp"),
    );
    let err = decode(h).expect_err("not RFC 3339");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::SENT_AT
        }
    );
}

#[test]
fn an_unrecognised_reliar_prefixed_header_is_ignored() {
    let mut h = complete_header_map();
    h.insert(
        header_name("reliar-a-future-field"),
        common::header_value("anything"),
    );
    let envelope = decode(h).expect("an unknown reliar-* header is forward-compatible, not fatal");
    assert!(envelope.headers().is_none());
}

#[test]
fn another_nats_bookkeeping_header_is_ignored() {
    let mut h = complete_header_map();
    h.insert(
        async_nats::header::NATS_STREAM,
        common::header_value("ORDERS"),
    );
    let envelope = decode(h).expect("broker bookkeeping headers are not framework fields");
    assert!(envelope.headers().is_none());
}

#[test]
fn a_framework_header_repeated_under_a_different_casing_is_a_duplicate_header_error() {
    let mut h = complete_header_map();
    // `reliar-tenant-id` and `Reliar-Tenant-Id` hash to two distinct `HeaderName`s (custom names
    // are matched byte-for-byte), so both reach `decode`'s scan and collide on the same slot.
    h.insert(
        header_name("reliar-tenant-id"),
        common::header_value("acme"),
    );
    h.insert(
        header_name("Reliar-Tenant-Id"),
        common::header_value("acme-2"),
    );
    let err = decode(h).expect_err("the same framework header under two casings");
    assert_eq!(
        err,
        NatsMapError::DuplicateHeader {
            header: headers::TENANT_ID
        }
    );
}

#[test]
fn a_framework_header_appended_twice_under_one_name_is_a_duplicate_header_error() {
    let mut h = complete_header_map();
    h.append(
        HeaderName::from_static(headers::TENANT_ID),
        common::header_value("acme"),
    );
    h.append(
        HeaderName::from_static(headers::TENANT_ID),
        common::header_value("acme-2"),
    );
    let err = decode(h).expect_err("two values under one header name");
    assert_eq!(
        err,
        NatsMapError::DuplicateHeader {
            header: headers::TENANT_ID
        }
    );
}

#[test]
fn a_custom_header_with_several_values_keeps_the_first() {
    let mut h = complete_header_map();
    h.append(header_name("x-custom"), common::header_value("first"));
    h.append(header_name("x-custom"), common::header_value("second"));
    let envelope = decode(h).expect("a multi-value custom header is not an error");
    assert_eq!(
        envelope.headers().and_then(|custom| custom.get("x-custom")),
        Some("first")
    );
}

/// U6/minor 12: every optional framework header that fails to parse is `MalformedHeader`, never
/// silently dropped — losing a correlation id quietly would be worse than failing loudly
/// (contract §2.5).
#[test]
fn malformed_correlation_id_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    // `CorrelationId::parse` rejects an empty string (`IdError::Empty`).
    h.insert(
        header_name(headers::CORRELATION_ID),
        common::header_value(""),
    );
    let err = decode(h).expect_err("an empty correlation id is not legal");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::CORRELATION_ID
        }
    );
}

#[test]
fn malformed_causation_id_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(
        header_name(headers::CAUSATION_ID),
        common::header_value("not-a-uuid"),
    );
    let err = decode(h).expect_err("not a UUID");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::CAUSATION_ID
        }
    );
}

#[test]
fn malformed_request_id_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(
        header_name(headers::REQUEST_ID),
        common::header_value("not-a-uuid"),
    );
    let err = decode(h).expect_err("not a UUID");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::REQUEST_ID
        }
    );
}

#[test]
fn malformed_expires_at_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(
        header_name(headers::EXPIRES_AT),
        common::header_value("not-a-timestamp"),
    );
    let err = decode(h).expect_err("not RFC 3339");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::EXPIRES_AT
        }
    );
}

#[test]
fn malformed_source_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    // `EndpointAddress::parse` rejects an empty string (`IdError::Empty`).
    h.insert(header_name(headers::SOURCE), common::header_value(""));
    let err = decode(h).expect_err("an empty endpoint address is not legal");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::SOURCE
        }
    );
}

#[test]
fn malformed_destination_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(header_name(headers::DESTINATION), common::header_value(""));
    let err = decode(h).expect_err("an empty endpoint address is not legal");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::DESTINATION
        }
    );
}

#[test]
fn malformed_reply_to_is_a_permanent_malformed_header_error() {
    let mut h = complete_header_map();
    h.insert(header_name(headers::REPLY_TO), common::header_value(""));
    let err = decode(h).expect_err("an empty endpoint address is not legal");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::REPLY_TO
        }
    );
}

/// Minor 12: `RejectedHeader`'s variant identity and its `source()` chain, not merely that decode
/// fails — a dead row's `last_error` is built from exactly this information.
#[test]
fn an_over_length_custom_header_value_is_a_rejected_header_error_with_a_source() {
    let mut h = complete_header_map();
    let overlong_value = "v".repeat(reliar_core::Headers::MAX_VALUE_LEN + 1);
    h.insert(
        header_name("x-custom"),
        common::header_value(overlong_value.as_str()),
    );
    let err = decode(h).expect_err("an over-length custom header value is rejected");

    match &err {
        NatsMapError::RejectedHeader { key, source } => {
            assert_eq!(key, "x-custom");
            assert!(matches!(
                source,
                reliar_core::HeaderError::ValueTooLong { .. }
            ));
        }
        other => panic!("expected RejectedHeader, got {other:?}"),
    }

    assert!(
        err.source().is_some(),
        "RejectedHeader must expose the HeaderError as its source()"
    );
}

/// U17: emptiness is the only name rule `decode` enforces on `reliar-message-type` —
/// `MessageType::from_parts` is core's deliberately unvalidated rehydration path (ADR 0011) and
/// would otherwise happily accept `""`, producing a `MessageType` that renders as `".v1"`
/// (ADR 0026 Amendment C).
#[test]
fn an_empty_message_type_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::MESSAGE_TYPE);
    h.insert(
        HeaderName::from_static(headers::MESSAGE_TYPE),
        common::header_value(""),
    );
    let err = decode(h).expect_err("an empty message type is not legal");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::MESSAGE_TYPE
        }
    );
}

/// U17 tail (review minor 4): the same emptiness question for `reliar-message-id` and
/// `reliar-message-version` — each is present-but-empty (not absent), so `decode` must reach the
/// parse stage and reject it there (`MalformedHeader`), never `MissingHeader`, since the `Option`
/// scan only sees the header's presence, not its content.
#[test]
fn an_empty_message_id_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::MESSAGE_ID);
    h.insert(
        HeaderName::from_static(headers::MESSAGE_ID),
        common::header_value(""),
    );
    let err = decode(h).expect_err("an empty message id is not a legal UUID");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::MESSAGE_ID
        }
    );
}

#[test]
fn an_empty_message_version_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::MESSAGE_VERSION);
    h.insert(
        HeaderName::from_static(headers::MESSAGE_VERSION),
        common::header_value(""),
    );
    let err = decode(h).expect_err("an empty message version does not parse as u16");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::MESSAGE_VERSION
        }
    );
}

/// The same for `reliar-content-type` — an empty string is not `type/subtype`, so `ContentType::
/// parse` rejects it exactly as it rejects `"not-a-mime-type"` above.
#[test]
fn an_empty_content_type_is_a_permanent_malformed_header_error() {
    let mut h = remove(&complete_header_map(), headers::CONTENT_TYPE);
    h.insert(
        HeaderName::from_static(headers::CONTENT_TYPE),
        common::header_value(""),
    );
    let err = decode(h).expect_err("an empty content type is not a legal mime type");
    assert_eq!(
        err,
        NatsMapError::MalformedHeader {
            header: headers::CONTENT_TYPE
        }
    );
}

/// U17's other half: a non-empty foreign name is accepted **verbatim** — a producer's naming
/// convention is not this crate's to police, and the mapper never validates beyond what core's
/// own constructors do.
#[test]
fn a_non_empty_foreign_message_type_name_is_accepted_verbatim() {
    let mut h = remove(&complete_header_map(), headers::MESSAGE_TYPE);
    h.insert(
        HeaderName::from_static(headers::MESSAGE_TYPE),
        common::header_value("some.other.producers.naming.convention"),
    );
    let envelope = decode(h).expect("a non-empty foreign name is not this crate's to police");
    assert_eq!(
        envelope.message_type.name(),
        "some.other.producers.naming.convention"
    );
}
