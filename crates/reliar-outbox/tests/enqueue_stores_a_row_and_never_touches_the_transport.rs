//! `OutboxPublisher::enqueue` writes exactly one row through `OutboxEnqueue` in the caller's
//! transaction, and the transport publisher is never called — the mirror mutation guard of
//! `publish_forwards_byte_identical_and_never_touches_the_store.rs` (ADR 0036, contract §2.1,
//! E3/E4).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::Envelope;
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPublisher, RecordingPublisher,
};

#[tokio::test]
async fn enqueue_writes_exactly_one_row_and_the_transport_sees_nothing() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 3 }).build();
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    outbox
        .enqueue(&mut tx, &serialized)
        .await
        .expect("enqueue succeeds");

    // Mutation guard: the store saw exactly one call, the transport saw none at all.
    assert_eq!(store.enqueue_call_count(), 1);
    assert_eq!(store.records().len(), 1);
    let row = store.record(serialized.id).expect("row enqueued");
    assert_eq!(row.envelope.body, serialized.body);
    assert!(
        publisher.published().is_empty(),
        "enqueue must never call Publisher::publish"
    );
}
