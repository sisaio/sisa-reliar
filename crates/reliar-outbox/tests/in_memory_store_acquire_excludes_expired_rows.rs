//! `acquire` never claims a row whose `expires_at` has already passed (§43.A.17) — such a row is
//! excluded from every claim until `purge` sweeps it to dead (ADR 0009).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::Envelope;
use reliar_outbox::{AcquireRequest, InMemoryOutboxStore, OutboxStore, WorkerId};

#[tokio::test]
async fn acquire_never_claims_an_expired_pending_row() {
    let store = InMemoryOutboxStore::default();

    let expired = Envelope::builder(common::OrderCreated { order_id: 1 })
        .expires_at(time::OffsetDateTime::UNIX_EPOCH)
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"));
    store.insert(expired);

    let live = store.insert(common::serialized_envelope());

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(10))
        .await
        .expect("acquire succeeds");

    assert_eq!(
        batch.records.len(),
        1,
        "only the unexpired row is claimable"
    );
    assert_eq!(batch.records[0].envelope.id, live.id);
}
