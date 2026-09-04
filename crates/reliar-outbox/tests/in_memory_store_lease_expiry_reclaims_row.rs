//! A row's lease is only a promise for `lease` — once [`InMemoryOutboxStore::advance`] moves the
//! fake's clock past `locked_until`, the row becomes claimable again, exactly like a crashed
//! worker's row in Postgres.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{AcquireRequest, InMemoryOutboxStore, OutboxStore, WorkerId};

#[tokio::test]
async fn expired_lease_lets_another_worker_reclaim() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());

    let first = WorkerId::generate();
    let second = WorkerId::generate();

    let batch = store
        .acquire(
            AcquireRequest::new(first.clone())
                .batch_size(1)
                .lease(Duration::from_secs(30)),
        )
        .await
        .expect("acquire succeeds");
    assert_eq!(batch.records.len(), 1);

    // Still within the lease: nobody else can claim it.
    let empty = store
        .acquire(AcquireRequest::new(second.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert!(empty.is_empty(), "the lease has not expired yet");

    // Advance by exactly the lease duration: `locked_until` now equals "now", and the boundary
    // is strict (`locked_until < now`, matching the SQL claim's `locked_until IS NULL OR
    // locked_until < now()`) — the lease is held *through* the instant it names, so the row is
    // still not claimable.
    store.advance(Duration::from_secs(30));
    let still_locked = store
        .acquire(AcquireRequest::new(second.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert!(
        still_locked.is_empty(),
        "locked_until == now is still held, the boundary is strict"
    );

    store.advance(Duration::from_secs(1));

    let reclaimed = store
        .acquire(AcquireRequest::new(second.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert_eq!(reclaimed.records.len(), 1, "the expired lease is claimable");
    assert_eq!(reclaimed.records[0].envelope.id, message.id);

    let record = store.record(message.id).expect("row exists");
    assert_eq!(record.locked_by, Some(second));
}
