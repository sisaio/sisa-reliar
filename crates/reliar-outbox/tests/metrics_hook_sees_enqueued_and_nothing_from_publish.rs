//! `RecordingMetrics` sees one `enqueued(1, type)` per enqueued envelope, through `enqueue` and
//! through `enqueue_batch`, and **nothing** from `publish`/`publish_batch` — the same sink is
//! routinely wired to the dispatcher, which already counts `published`, so a forward would
//! double-count (ADR 0036 §6, contract §6, E15).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, MessageType, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPublisher, RecordingMetrics, RecordingPublisher,
};

#[tokio::test]
async fn enqueue_and_enqueue_batch_each_call_the_hook_once_per_envelope() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let metrics = RecordingMetrics::default();
    let outbox = OutboxPublisher::with_metrics(store, publisher, metrics.clone());

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    let c = common::serialize(Envelope::builder(common::TypeC).build());

    let mut tx = InMemoryTransaction;
    outbox.enqueue(&mut tx, &a).await.expect("enqueue succeeds");
    outbox
        .enqueue_batch(&mut tx, &[b, c])
        .await
        .expect("batch enqueues");

    assert_eq!(
        metrics.enqueued(),
        vec![
            MessageType::new("a", 1),
            MessageType::new("b", 1),
            MessageType::new("c", 1),
        ]
    );
}

#[tokio::test]
async fn publish_and_publish_batch_never_call_the_hook() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let metrics = RecordingMetrics::default();
    let outbox = OutboxPublisher::with_metrics(store, publisher, metrics.clone());

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    outbox.publish(&a).await.expect("publish succeeds");
    outbox.publish_batch(&[b]).await;

    assert!(metrics.enqueued().is_empty());
}
