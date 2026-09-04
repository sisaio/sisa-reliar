//! `insert_with`'s `ordering_key` lands on the resulting `OutboxRecord.ordering_key` unchanged,
//! and survives a claim — `insert` (no explicit key) leaves it `None`.

#![cfg(feature = "test-support")]

mod common;

use reliar_outbox::{AcquireRequest, InMemoryOutboxStore, OutboxStore, WorkerId};

#[tokio::test]
async fn ordering_key_round_trips_through_insert_with_and_acquire() {
    let store = InMemoryOutboxStore::default();

    let keyed = store.insert_with(
        common::serialized_envelope(),
        time::OffsetDateTime::UNIX_EPOCH,
        Some("customer-42".to_string()),
    );
    let unkeyed = store.insert(common::serialized_envelope());

    assert_eq!(
        store.record(keyed.id).unwrap().ordering_key.as_deref(),
        Some("customer-42")
    );
    assert_eq!(store.record(unkeyed.id).unwrap().ordering_key, None);

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(10))
        .await
        .expect("acquire succeeds");
    let acquired_keyed = batch
        .records
        .iter()
        .find(|record| record.envelope.id == keyed.id)
        .expect("the keyed row was claimed");
    assert_eq!(acquired_keyed.ordering_key.as_deref(), Some("customer-42"));
}
