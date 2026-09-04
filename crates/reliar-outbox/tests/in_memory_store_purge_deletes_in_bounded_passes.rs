//! [`OutboxStore::purge`] is **one bounded pass**: it deletes at most `batch_size` published
//! rows, at most `batch_size` dead rows, and sweeps expired pending rows to dead — the caller
//! repeats while `!report.is_complete(batch_size)`.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_core::Envelope;
use reliar_outbox::{
    AcquireRequest, CompletedMessage, DeadReason, InMemoryOutboxStore, OutboxStore, PurgeRequest,
    WorkerId,
};

fn expiring_envelope(
    order_id: u64,
    expires_at: time::OffsetDateTime,
) -> reliar_core::SerializedEnvelope {
    Envelope::builder(common::OrderCreated { order_id })
        .expires_at(expires_at)
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"))
}

#[tokio::test]
async fn published_purge_is_bounded_and_reports_completion() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();

    let mut refs = Vec::new();
    for _ in 0..3 {
        refs.push(store.insert(common::serialized_envelope()));
    }
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(10))
        .await
        .expect("acquire succeeds");
    assert_eq!(batch.records.len(), 3);
    for message in &refs {
        store
            .complete(&worker, &[CompletedMessage::new(*message)])
            .await
            .expect("complete never errors");
    }

    // Retention deletes are strict (`published_at < now - retention`, a minor from review 1): a
    // row published in the same instant as the cutoff is not yet "older than" it, so the clock
    // must move past that instant before a zero retention deletes anything.
    store.advance(Duration::from_secs(1));

    let request = PurgeRequest::default()
        .published_retention(Some(Duration::ZERO))
        .batch_size(2);

    let first = store.purge(request.clone()).await.expect("purge succeeds");
    assert_eq!(first.published_deleted, 2);
    assert!(!first.is_complete(2), "two of three rows were deleted");
    assert_eq!(store.records().len(), 1);

    let second = store.purge(request).await.expect("purge succeeds");
    assert_eq!(second.published_deleted, 1);
    assert!(second.is_complete(2), "the remaining row cleared the pass");
    assert!(store.records().is_empty());
}

#[tokio::test]
async fn purge_sweeps_expired_pending_rows_to_dead() {
    let store = InMemoryOutboxStore::default();

    let expiring = expiring_envelope(1, time::OffsetDateTime::UNIX_EPOCH - Duration::from_secs(1));
    let message = store.insert(expiring);

    let report = store
        .purge(PurgeRequest::default())
        .await
        .expect("purge succeeds");
    assert_eq!(report.expired_to_dead, 1);

    let record = store.record(message.id).expect("row exists");
    assert_eq!(record.dead_reason, Some(DeadReason::Expired));
    assert!(record.dead_at.is_some());
    assert_eq!(
        record.last_error.as_deref(),
        Some("reliar: expired before publication"),
        "M1: the sweep records why the row died"
    );
}

#[tokio::test]
async fn expiry_sweep_is_bounded_by_batch_size() {
    let store = InMemoryOutboxStore::default();
    let expired_at = time::OffsetDateTime::UNIX_EPOCH - Duration::from_secs(1);

    for id in 0..3 {
        store.insert(expiring_envelope(id, expired_at));
    }

    let report = store
        .purge(PurgeRequest::default().batch_size(2))
        .await
        .expect("purge succeeds");
    assert_eq!(
        report.expired_to_dead, 2,
        "one pass caps the sweep at batch_size"
    );
    assert!(
        !report.is_complete(2),
        "ruling G1: is_complete considers expired_to_dead too"
    );

    let second = store
        .purge(PurgeRequest::default().batch_size(2))
        .await
        .expect("purge succeeds");
    assert_eq!(second.expired_to_dead, 1);
    assert!(second.is_complete(2));
}

#[tokio::test]
async fn expiry_sweep_never_transitions_a_row_with_a_live_lease() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();

    let expiring = expiring_envelope(
        1,
        time::OffsetDateTime::UNIX_EPOCH + Duration::from_secs(10),
    );
    let message = store.insert(expiring);

    store
        .acquire(
            AcquireRequest::new(worker.clone())
                .batch_size(1)
                .lease(Duration::from_secs(30)),
        )
        .await
        .expect("acquire succeeds");

    // The row's `expires_at` has now passed, but its lease has not: ruling G2 says the sweep
    // must leave it alone — it still belongs to `worker`.
    store.advance(Duration::from_secs(11));
    let report = store
        .purge(PurgeRequest::default())
        .await
        .expect("purge succeeds");
    assert_eq!(
        report.expired_to_dead, 0,
        "a leased row is not swept, expired or not"
    );

    let affected = store
        .complete(&worker, &[CompletedMessage::new(message)])
        .await
        .expect("complete never errors");
    assert_eq!(affected, 1, "the owner's complete still wins");
}
