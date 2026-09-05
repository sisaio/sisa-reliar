//! `OutboxPublisher::publish` forwards every envelope to the transport exactly once,
//! byte-identical, and never touches the enqueue-capable store — the mutation guard that proves
//! `publish` really bypasses the outbox rather than accidentally routing through it (ADR 0036,
//! contract §2.2, E1/E3).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{InMemoryOutboxStore, OutboxPublisher, RecordingPublisher};

#[tokio::test]
async fn publish_forwards_exactly_once_byte_identical_and_the_store_sees_nothing() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 7 }).build();
    let serialized = common::serialize(envelope);

    outbox
        .publish(&serialized)
        .await
        .expect("the transport accepts the publish");

    // Mutation guard: the publisher saw exactly one call, the store saw none at all.
    let sent = publisher
        .envelopes()
        .into_iter()
        .next()
        .expect("exactly one publish call");
    assert_eq!(sent.body, serialized.body, "body must be byte-identical");
    assert_eq!(
        sent.metadata.delivery.content_type, serialized.metadata.delivery.content_type,
        "content_type must be byte-identical"
    );
    assert_eq!(publisher.published().len(), 1);
    assert_eq!(
        store.enqueue_call_count(),
        0,
        "publish must never call OutboxEnqueue::enqueue"
    );
    assert!(store.records().is_empty(), "the store must stay empty");
}
