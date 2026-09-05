//! With the default policy (every type routed), a routed publish is staged and the transport
//! publisher is never called (§43.D2, ADR 0033 Amendment D).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, RecordingPublisher,
};

#[tokio::test]
async fn publish_stages_and_never_calls_the_publisher() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), OutboxPolicy::default());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let id = envelope.id;
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("stage succeeds");

    assert!(publisher.published().is_empty());
    assert_eq!(store.records().len(), 1);
    assert_eq!(store.record(id).expect("row staged").envelope.id, id);
}
