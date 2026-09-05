//! `OutboxPublisher::publish_direct` on a routed type fails before touching the store or the
//! publisher — it never silently downgrades to a direct publish (§43.D4, ADR 0033).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::Envelope;
use reliar_outbox::{
    DirectPublishError, InMemoryOutboxStore, OutboxPolicy, OutboxPublisher, RecordingPublisher,
};

#[tokio::test]
async fn publish_direct_on_a_routed_type_returns_transaction_required_and_touches_nothing() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), OutboxPolicy::default());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let expected_type = envelope.message_type.clone();
    let serialized = common::serialize(envelope);

    let err = outbox
        .publish_direct(&serialized)
        .await
        .expect_err("a routed type has no transaction at this call site");

    match err {
        DirectPublishError::TransactionRequired { message_type } => {
            assert_eq!(message_type, expected_type);
        }
        other => panic!("expected TransactionRequired, got {other:?}"),
    }

    // No silent downgrade: neither collaborator was ever touched.
    assert!(store.records().is_empty());
    assert!(publisher.published().is_empty());
}
