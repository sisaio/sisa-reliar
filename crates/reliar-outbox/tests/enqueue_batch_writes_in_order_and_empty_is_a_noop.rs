//! `OutboxPublisher::enqueue_batch` enqueues every envelope in order, and an empty slice is `Ok(())`
//! issuing no statement at all (ADR 0036 §5, contract §2.1, E5).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::Envelope;
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPublisher, RecordingPublisher,
};

#[tokio::test]
async fn enqueues_every_envelope_in_order() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher);

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    let c = common::serialize(Envelope::builder(common::TypeC).build());
    let ids = [a.id, b.id, c.id];

    let mut tx = InMemoryTransaction;
    outbox
        .enqueue_batch(&mut tx, &[a, b, c])
        .await
        .expect("the whole batch enqueues");

    assert_eq!(store.enqueue_call_count(), 3);
    let enqueued_ids: Vec<_> = store.records().into_iter().map(|r| r.envelope.id).collect();
    assert_eq!(
        enqueued_ids, ids,
        "rows must be enqueued in the input's own order"
    );
}

#[tokio::test]
async fn an_empty_batch_is_ok_and_issues_no_statement() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher);

    let mut tx = InMemoryTransaction;
    outbox
        .enqueue_batch(&mut tx, &[])
        .await
        .expect("an empty slice is Ok");

    assert_eq!(store.enqueue_call_count(), 0);
    assert!(store.records().is_empty());
}
