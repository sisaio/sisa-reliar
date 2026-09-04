//! `Envelope<T>` ↔ `SerializedEnvelope` ↔ `Envelope<T>` round-trips through the default
//! `JsonSerializer` without losing the body (§12.1, ADR 0003). The full row round-trip through a
//! provider's promoted-column + `MetadataRest` merge is `pg`-layer (§43.A.4,
//! `reliar-store-postgres`); this is the `reliar-core`-only half of that guarantee.

mod common;

use common::OrderCreated;
use reliar_core::{Envelope, JsonSerializer, Serializer};

#[test]
fn body_survives_a_serialize_deserialize_round_trip() {
    let original = Envelope::builder(OrderCreated { order_id: 99 })
        .tenant("acme")
        .header("x-import-batch", "2026-09-04")
        .unwrap()
        .build();

    let serializer = JsonSerializer;
    let bytes = serializer.serialize(&original.body).unwrap();

    // The provider's rehydration path: build a `SerializedEnvelope` from the columns/JSONB it
    // read back, using `from_parts` rather than the typed builder (ADR 0011).
    let on_the_wire = reliar_core::SerializedEnvelope::from_parts(
        original.id,
        original.message_type.clone(),
        bytes,
        original.metadata.clone(),
        original.headers().cloned(),
    );

    let rehydrated: Envelope<OrderCreated> = on_the_wire
        .try_map_body(|bytes| serializer.deserialize::<OrderCreated>(&bytes))
        .expect("valid JSON produced by the same serializer must deserialize");

    assert_eq!(rehydrated, original);
}
