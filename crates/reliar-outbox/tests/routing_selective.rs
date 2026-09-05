//! With `allowed_types = [a, b]`, only those types route through the outbox — everything else
//! goes straight to the transport (§43.D3, ADR 0033). Only the scoped view reaches the store:
//! `routing_requires_transaction.rs` (R6) proves the runtime side of that at `publish_direct`
//! (§43.D4).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, MessageTypeNames, OutboxPolicy, OutboxPublisher,
    OutboxSettings, RecordingPublisher, RouteKind,
};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn selective_outbox() -> (
    OutboxPublisher<InMemoryOutboxStore, RecordingPublisher>,
    InMemoryOutboxStore,
    RecordingPublisher,
) {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let settings = OutboxSettings::default()
        .allowed_types(MessageTypeNames::try_from_iter("test", ["a", "b"]).expect("valid"))
        .expect("no overlap");
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);
    (outbox, store, publisher)
}

#[tokio::test]
async fn allowed_types_reach_the_store_others_reach_the_publisher() {
    let (outbox, store, publisher) = selective_outbox();
    let mut tx = InMemoryTransaction;
    let scoped = outbox.in_transaction(&mut tx);

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    assert_eq!(outbox.policy().decide(&a.message_type), RouteKind::Outbox);
    scoped
        .publish(&a)
        .await
        .expect("a routes through the outbox");

    let b = common::serialize(Envelope::builder(common::TypeB).build());
    assert_eq!(outbox.policy().decide(&b.message_type), RouteKind::Outbox);
    scoped
        .publish(&b)
        .await
        .expect("b routes through the outbox");

    let c = common::serialize(Envelope::builder(common::TypeC).build());
    assert_eq!(outbox.policy().decide(&c.message_type), RouteKind::Direct);
    scoped
        .publish(&c)
        .await
        .expect("c is not allowed, so it goes direct");

    // Assert on both collaborators so swapping the two arms of the outbox/direct branch fails.
    assert_eq!(store.records().len(), 2);
    assert!(store.record(a.id).is_some());
    assert!(store.record(b.id).is_some());
    assert!(store.record(c.id).is_none());
    assert_eq!(publisher.published(), vec![c.id]);
}
