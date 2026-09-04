//! [`OutboxStore::fail`] applies a [`FailureOutcome`] exactly as SQL would: `Retry` schedules
//! `available_at = now + delay`, clears the lease and never touches `dead_at`; `Dead` sets
//! `dead_at`/`dead_reason` and the row is never claimed again. Both increment `attempts` — claims
//! never do (ADR 0009).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, DeadReason, FailedMessage, FailureOutcome, InMemoryOutboxStore, OutboxStore,
    WorkerId,
};

#[tokio::test]
async fn retry_schedules_available_at_and_frees_the_lease() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());
    let worker = WorkerId::generate();

    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");

    // Advance *before* failing: `created_at` and "now at the time of `fail`" must differ, or an
    // implementation that wrongly computed `available_at = created_at + delay` instead of `now +
    // delay` would pass this assertion by coincidence (M9).
    let elapsed = Duration::from_secs(100);
    store.advance(elapsed);

    let delay = Duration::from_secs(5);
    let affected = store
        .fail(
            &worker,
            &[FailedMessage::new(
                message,
                "connection reset",
                FailureOutcome::Retry { delay },
            )],
        )
        .await
        .expect("fail never errors");
    assert_eq!(affected, 1);

    let record = store.record(message.id).expect("row exists");
    assert_eq!(record.attempts, 1, "attempts counts observed outcomes");
    assert_eq!(record.locked_by, None, "the lease is freed on failure");
    assert!(record.dead_at.is_none());
    assert_eq!(
        record.available_at,
        message.created_at + elapsed + delay,
        "available_at is now() + delay, not created_at + delay"
    );

    // Not due yet: claiming with a fresh worker returns nothing.
    let too_soon = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert!(too_soon.is_empty());

    store.advance(delay);

    let due = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert_eq!(due.records.len(), 1, "the row is due after the delay");
    assert_eq!(
        due.records[0].attempts, 1,
        "the reclaim does not bump attempts on its own"
    );
}

#[tokio::test]
async fn permanent_outcome_goes_dead_and_is_never_reclaimed() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());
    let worker = WorkerId::generate();

    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");

    store
        .fail(
            &worker,
            &[FailedMessage::new(
                message,
                "payload rejected",
                FailureOutcome::Dead {
                    reason: DeadReason::PermanentError,
                },
            )],
        )
        .await
        .expect("fail never errors");

    let record = store.record(message.id).expect("row exists");
    assert_eq!(record.attempts, 1);
    assert!(record.dead_at.is_some());
    assert_eq!(record.dead_reason, Some(DeadReason::PermanentError));
    assert_eq!(record.locked_by, None);

    store.advance(Duration::from_secs(365 * 24 * 60 * 60));
    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(10))
        .await
        .expect("acquire succeeds");
    assert!(batch.is_empty(), "a dead row is never reclaimed");
}
