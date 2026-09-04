//! [`OutboxDeadLetters::retry_dead`] and [`OutboxDeadLetters::purge_dead`] are the operator's
//! explicit tools over dead rows, and neither is worker-guarded — a dead row holds no lease.
#![cfg(feature = "test-support")]

mod common;

use reliar_outbox::{
    AcquireRequest, DeadReason, FailedMessage, FailureOutcome, InMemoryOutboxStore,
    OutboxDeadLetters, OutboxStore, WorkerId,
};

#[tokio::test]
async fn retry_dead_resets_attempts_and_clears_dead_state() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();
    let message = store.insert(common::serialized_envelope());

    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    store
        .fail(
            &worker,
            &[FailedMessage::new(
                message,
                "boom",
                FailureOutcome::Dead {
                    reason: DeadReason::AttemptsExhausted,
                },
            )],
        )
        .await
        .expect("fail never errors");
    assert_eq!(store.record(message.id).unwrap().attempts, 1);

    let affected = store
        .retry_dead(&[message])
        .await
        .expect("retry_dead never errors");
    assert_eq!(affected, 1);

    let record = store.record(message.id).expect("row exists");
    assert!(record.dead_at.is_none());
    assert!(record.dead_reason.is_none());
    assert_eq!(
        record.attempts, 0,
        "retry_dead is the only reset of attempts"
    );
    assert!(record.last_error.is_some(), "last_error is kept for audit");

    // The row is claimable again.
    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert_eq!(batch.records.len(), 1);
}

#[tokio::test]
async fn retry_dead_ignores_rows_that_are_not_dead() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());

    let affected = store
        .retry_dead(&[message])
        .await
        .expect("retry_dead never errors");
    assert_eq!(affected, 0, "a pending row holds no dead state to clear");
}

#[tokio::test]
async fn purge_dead_deletes_regardless_of_retention() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();
    let message = store.insert(common::serialized_envelope());

    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    store
        .fail(
            &worker,
            &[FailedMessage::new(
                message,
                "boom",
                FailureOutcome::Dead {
                    reason: DeadReason::PermanentError,
                },
            )],
        )
        .await
        .expect("fail never errors");

    // No `PurgeRequest::dead_retention` was ever configured — `purge_dead` deletes anyway.
    let deleted = store
        .purge_dead(&[message])
        .await
        .expect("purge_dead never errors");
    assert_eq!(deleted, 1);
    assert!(store.record(message.id).is_none());
}
