//! With a disabled policy, `OutboxPublisher` never touches the store — every envelope reaches the
//! transport publisher exactly once, through both the scoped view and `publish_direct` (§43.D1,
//! ADR 0033 Amendment D).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, OutboxSettings,
    RecordingPublisher,
};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn disabled_outbox() -> (
    OutboxPublisher<InMemoryOutboxStore, RecordingPublisher>,
    InMemoryOutboxStore,
    RecordingPublisher,
) {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("disabled settings never overlap");
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);
    (outbox, store, publisher)
}

#[tokio::test]
async fn scoped_publish_goes_direct_and_never_touches_the_store() {
    let (outbox, store, publisher) = disabled_outbox();
    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let id = envelope.id;
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("direct publish succeeds");

    assert_eq!(publisher.count(id), 1);
    assert!(store.records().is_empty());
}

#[tokio::test]
async fn publish_direct_goes_direct_and_never_touches_the_store() {
    let (outbox, store, publisher) = disabled_outbox();
    let envelope = Envelope::builder(common::OrderCreated { order_id: 2 }).build();
    let id = envelope.id;
    let serialized = common::serialize(envelope);

    outbox
        .publish_direct(&serialized)
        .await
        .expect("direct publish succeeds");

    assert_eq!(publisher.count(id), 1);
    assert!(store.records().is_empty());
}
