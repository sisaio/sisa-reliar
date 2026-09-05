//! `encode` never panics on a custom header NATS cannot express, or on a `\r`/`\n` hiding in an
//! unvalidated core `String` field (contract §2.4, U7 — story C3, C8).

mod common;

use bytes::Bytes;
use reliar_core::{Envelope, EnvelopeMapper, Metadata};
use reliar_transport_nats::{NatsEnvelopeMapper, NatsMapError, headers};

use common::OrderCreated;

fn encode(envelope: &reliar_core::SerializedEnvelope) -> Result<(), NatsMapError> {
    NatsEnvelopeMapper::default().encode(envelope).map(|_| ())
}

#[test]
fn a_custom_key_with_a_space_is_unsupported_header_name() {
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    // `Headers::insert` itself only rejects `reliar-`/control-char/length; a space in a key is
    // legal there but not a legal NATS header name, so the mapper — not core — is what rejects it.
    let mut envelope = envelope;
    envelope
        .headers_mut()
        .insert("x custom", "value")
        .expect("a space is not rejected by core's Headers");

    let err = encode(&envelope).expect_err("a space is not a legal NATS header name");
    assert_eq!(
        err,
        NatsMapError::UnsupportedHeaderName {
            key: "x custom".to_string()
        }
    );
}

#[test]
fn a_custom_key_with_a_colon_is_unsupported_header_name() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("x-custom:bad", "value")
        .expect("a colon is not rejected by core's Headers");

    let err = encode(&envelope).expect_err("a colon is not a legal NATS header name");
    assert_eq!(
        err,
        NatsMapError::UnsupportedHeaderName {
            key: "x-custom:bad".to_string()
        }
    );
}

#[test]
fn a_custom_key_with_non_ascii_is_unsupported_header_name() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("x-caf\u{e9}", "value")
        .expect("non-ASCII is not rejected by core's Headers");

    let err = encode(&envelope).expect_err("non-ASCII is not a legal NATS header name");
    assert_eq!(
        err,
        NatsMapError::UnsupportedHeaderName {
            key: "x-caf\u{e9}".to_string()
        }
    );
}

#[test]
fn a_nats_prefixed_custom_key_is_a_reserved_header_name() {
    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    envelope
        .headers_mut()
        .insert("Nats-Custom", "value")
        .expect("`Nats-` is not `reliar-`, so core's Headers accepts it");

    let err = encode(&envelope).expect_err("`Nats-` is NATS's own reserved namespace");
    assert_eq!(
        err,
        NatsMapError::ReservedHeaderName {
            key: "Nats-Custom".to_string()
        }
    );
    // Case-insensitive, per §2.4.
    let mut lowercase = Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));
    lowercase
        .headers_mut()
        .insert("nats-custom", "value")
        .expect("accepted by core's Headers");
    assert_eq!(
        encode(&lowercase).expect_err("still `Nats-` under a different casing"),
        NatsMapError::ReservedHeaderName {
            key: "nats-custom".to_string()
        }
    );
}

#[test]
fn crlf_in_the_message_type_name_is_an_invalid_header_value() {
    // `MessageType::name` is an unvalidated `Cow<'static, str>` (ADR 0010) — this crate is the
    // one place that must not panic when it carries a `\r`/`\n`.
    #[derive(serde::Serialize, serde::Deserialize)]
    struct Injected;
    impl reliar_core::Message for Injected {
        const TYPE: &'static str = "orders.created\r\nInjected: true";
        const VERSION: u16 = 1;
    }
    let envelope = Envelope::builder(Injected)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let err = encode(&envelope).expect_err("a CRLF in the message type name");
    assert_eq!(
        err,
        NatsMapError::InvalidHeaderValue {
            header: headers::MESSAGE_TYPE.to_string()
        }
    );
}

#[test]
fn crlf_in_tenant_id_is_an_invalid_header_value() {
    let mut metadata = Metadata::default();
    metadata.tenant_id = Some("acme\r\nInjected: true".to_string());
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .metadata(metadata)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let err = encode(&envelope).expect_err("a CRLF in tenant_id");
    assert_eq!(
        err,
        NatsMapError::InvalidHeaderValue {
            header: headers::TENANT_ID.to_string()
        }
    );
}

#[test]
fn crlf_in_traceparent_is_an_invalid_header_value() {
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .trace("00-trace\r\nInjected: true", None)
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let err = encode(&envelope).expect_err("a CRLF in traceparent");
    assert_eq!(
        err,
        NatsMapError::InvalidHeaderValue {
            header: headers::TRACEPARENT.to_string()
        }
    );
}

#[test]
fn crlf_in_tracestate_is_an_invalid_header_value() {
    let envelope = Envelope::builder(OrderCreated { order_id: 1 })
        .trace("00-trace", Some("state\r\nInjected: true".to_string()))
        .build()
        .map_body(|_| Bytes::from_static(b"{}"));

    let err = encode(&envelope).expect_err("a CRLF in tracestate");
    assert_eq!(
        err,
        NatsMapError::InvalidHeaderValue {
            header: headers::TRACESTATE.to_string()
        }
    );
}
