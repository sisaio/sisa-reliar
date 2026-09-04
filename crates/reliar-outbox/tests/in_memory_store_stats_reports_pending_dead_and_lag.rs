//! [`OutboxStore::stats`] feeds the outbox-lag and dead-count gauges: `pending` and
//! `oldest_pending_available_at` use the same claim predicate as `acquire` (an expired row is
//! excluded and counted in `expired_pending` instead), and `as_of` is the store's own clock.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_core::Envelope;
use reliar_outbox::{
    AcquireRequest, DeadReason, FailedMessage, FailureOutcome, InMemoryOutboxStore, OutboxStore,
    WorkerId,
};

#[tokio::test]
async fn stats_counts_pending_dead_and_expired_separately() {
    let store = InMemoryOutboxStore::default();

    // A dead row — claimed and failed before any other row exists, so `acquire` cannot also
    // sweep up the pending row seeded below.
    let worker = WorkerId::generate();
    let dead = store.insert(common::serialized_envelope());
    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    store
        .fail(
            &worker,
            &[FailedMessage::new(
                dead,
                "boom",
                FailureOutcome::Dead {
                    reason: DeadReason::PermanentError,
                },
            )],
        )
        .await
        .expect("fail never errors");

    // A leased row: claimed by another worker and never completed/failed/released, seeded and
    // claimed before any other pending row exists so this `acquire` cannot claim anything else
    // (mirrors the dead-row setup above). `pending` must use the *same* claim predicate
    // `acquire` does — including the lock check — or this row would be double-counted as both
    // claimed and pending (M5).
    let leased = store.insert(common::serialized_envelope());
    store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert!(
        store.record(leased.id).unwrap().locked_by.is_some(),
        "the fixture row is actually leased"
    );

    // A pending row, due now and never claimed.
    let due = store.insert(common::serialized_envelope());

    // A pending row not due yet: excluded from `pending`, does not pin `oldest_pending_available_at`.
    store.insert_with(
        common::serialized_envelope(),
        time::OffsetDateTime::UNIX_EPOCH + Duration::from_secs(3600),
        None,
    );

    // A row already past `expires_at`: excluded from `pending`, counted in `expired_pending`.
    let expiring = Envelope::builder(common::OrderCreated { order_id: 2 })
        .expires_at(time::OffsetDateTime::UNIX_EPOCH)
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"));
    store.insert(expiring);

    let stats = store.stats().await.expect("stats never errors");
    assert_eq!(stats.pending, 1, "only the due row counts as pending");
    assert_eq!(stats.dead, 1);
    assert_eq!(stats.expired_pending, 1);
    assert_eq!(stats.oldest_pending_available_at, Some(due.created_at));
    assert_eq!(stats.as_of, time::OffsetDateTime::UNIX_EPOCH);
    assert_eq!(stats.lag(), Some(Duration::ZERO));
}

#[tokio::test]
async fn stats_reports_no_lag_over_an_empty_backlog() {
    let store = InMemoryOutboxStore::default();
    let stats = store.stats().await.expect("stats never errors");
    assert_eq!(stats.pending, 0);
    assert!(stats.oldest_pending_available_at.is_none());
    assert!(stats.lag().is_none(), "no claimable row means no lag value");
}
