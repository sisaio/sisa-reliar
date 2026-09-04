//! `release` clears a lease without touching `attempts` or `available_at`; `extend_lease` renews
//! `locked_until = now + lease` for a row the caller still owns. Both are worker-guarded and
//! benign on a shortfall — this file proves the positive path, where the owner does hold the row.

#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{AcquireRequest, InMemoryOutboxStore, OutboxStore, WorkerId};

#[tokio::test]
async fn release_clears_the_lease_and_leaves_attempts_and_available_at_untouched() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());
    let worker = WorkerId::generate();

    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    let before = store.record(message.id).expect("row exists");

    let affected = store
        .release(&worker, &[message])
        .await
        .expect("release never errors");
    assert_eq!(affected, 1);

    let after = store.record(message.id).expect("row exists");
    assert_eq!(after.locked_by, None);
    assert_eq!(after.locked_until, None);
    assert_eq!(
        after.attempts, before.attempts,
        "release never counts as an outcome"
    );
    assert_eq!(
        after.available_at, before.available_at,
        "release does not reschedule the row"
    );

    // Released rows are claimable again.
    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert_eq!(batch.records.len(), 1);
}

#[tokio::test]
async fn extend_lease_renews_locked_until_from_now() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());
    let worker = WorkerId::generate();

    store
        .acquire(
            AcquireRequest::new(worker.clone())
                .batch_size(1)
                .lease(Duration::from_secs(30)),
        )
        .await
        .expect("acquire succeeds");

    store.advance(Duration::from_secs(20));

    let new_lease = Duration::from_secs(60);
    let affected = store
        .extend_lease(&worker, &[message], new_lease)
        .await
        .expect("extend_lease never errors");
    assert_eq!(affected, 1);

    let record = store.record(message.id).expect("row exists");
    // `now` at the time of `extend_lease` is `created_at + 20s` (the store's own clock, not the
    // original acquire's lease deadline).
    assert_eq!(
        record.locked_until,
        Some(message.created_at + Duration::from_secs(20) + new_lease)
    );
}
