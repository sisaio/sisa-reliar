//! §43.A.2 — an `Envelope<T>` is built through the public builder with a typed body and typed
//! `Metadata`; `map_body` converts it into a `SerializedEnvelope` without touching any field but
//! the body (§9, ADR 0003). Whether a *persisted* row carries the configured `Serializer`'s
//! `ContentType` is proved in the store slice (§12.1, `reliar-store-postgres`, → RELIAR-16) — a
//! provider, not `reliar-core`, is what actually sets `metadata.delivery.content_type` at
//! enqueue.

mod common;

use common::OrderCreated;
use reliar_core::{CorrelationId, Envelope, JsonSerializer, Serializer};

#[test]
fn builder_carries_typed_body_and_metadata() {
    let correlation_id = CorrelationId::parse("checkout-42").unwrap();

    let envelope = Envelope::builder(OrderCreated { order_id: 42 })
        .correlation_id(correlation_id.clone())
        .tenant("acme")
        .build();

    assert_eq!(envelope.body.order_id, 42);
    assert_eq!(
        envelope.metadata.correlation.correlation_id,
        Some(correlation_id)
    );
    assert_eq!(envelope.metadata.tenant_id.as_deref(), Some("acme"));
    // The builder never invents an unrelated conversation: an un-correlated message is the
    // root of its own conversation (ADR 0011).
    assert_eq!(
        envelope.metadata.correlation.conversation_id.as_uuid(),
        envelope.id.as_uuid()
    );
}

#[test]
fn map_body_serializes_without_touching_any_other_field() {
    let envelope = Envelope::builder(OrderCreated { order_id: 7 })
        .tenant("acme")
        .build();
    let serializer = JsonSerializer;

    let bytes = serializer.serialize(&envelope.body).unwrap();
    let (id, message_type, metadata) = (
        envelope.id,
        envelope.message_type.clone(),
        envelope.metadata.clone(),
    );

    let on_the_wire = envelope.map_body(|_| bytes);

    assert_eq!(on_the_wire.id, id);
    assert_eq!(on_the_wire.message_type, message_type);
    assert_eq!(on_the_wire.metadata, metadata);
    assert_eq!(on_the_wire.body.as_ref(), br#"{"order_id":7}"#);
}
