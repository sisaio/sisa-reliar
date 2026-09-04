//! Every state-changing `OutboxStore` method (other than `acquire`) matches `locked_by =
//! $worker`. A worker that does not hold a row's lease affects zero rows — benign, never an
//! error — and the row is left untouched for its actual owner.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, CompletedMessage, FailedMessage, FailureOutcome, InMemoryOutboxStore,
    OutboxStore, WorkerId,
};

#[tokio::test]
async fn other_worker_affects_zero_rows_and_owner_still_can() {
    let store = InMemoryOutboxStore::default();
    let message = store.insert(common::serialized_envelope());

    let owner = WorkerId::generate();
    let intruder = WorkerId::generate();

    let batch = store
        .acquire(AcquireRequest::new(owner.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    assert_eq!(batch.records.len(), 1, "the row was claimable");

    // The intruder never held this lease: every guarded call reports zero rows affected.
    assert_eq!(
        store
            .complete(&intruder, &[CompletedMessage::new(message)])
            .await
            .expect("complete never errors"),
        0
    );
    assert_eq!(
        store
            .fail(
                &intruder,
                &[FailedMessage::new(
                    message,
                    "boom",
                    FailureOutcome::Retry {
                        delay: Duration::from_secs(1)
                    }
                )]
            )
            .await
            .expect("fail never errors"),
        0
    );
    assert_eq!(
        store
            .release(&intruder, &[message])
            .await
            .expect("release never errors"),
        0
    );
    assert_eq!(
        store
            .extend_lease(&intruder, &[message], Duration::from_secs(30))
            .await
            .expect("extend_lease never errors"),
        0
    );

    // The row is untouched: still locked by the real owner, no error recorded, not published.
    let record = store.record(message.id).expect("row exists");
    assert_eq!(record.locked_by, Some(owner.clone()));
    assert!(record.published_at.is_none());
    assert!(record.last_error.is_none());
    assert_eq!(record.attempts, 0);

    // The real owner's call still works.
    assert_eq!(
        store
            .complete(&owner, &[CompletedMessage::new(message)])
            .await
            .expect("complete never errors"),
        1
    );
    let record = store.record(message.id).expect("row exists");
    assert!(record.published_at.is_some());
    assert_eq!(record.locked_by, None);
}
