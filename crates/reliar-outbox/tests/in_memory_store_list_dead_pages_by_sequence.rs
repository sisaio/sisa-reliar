//! [`OutboxDeadLetters::list_dead`] orders strictly by `sequence` and its keyset cursor
//! (`next_after_sequence`) is only `Some` when the page was full — the caller's termination
//! condition.
//!
//! `InMemoryOutboxStore` never poisons a row (it stores already-decoded records, not raw bytes),
//! so `DeadLetterPage::poisoned` is exercised only by `reliar-store-postgres`'s real-decode-failure
//! tests, not here.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_core::Envelope;
use reliar_outbox::{
    AcquireRequest, DeadQuery, DeadReason, FailedMessage, FailureOutcome, InMemoryOutboxStore,
    MessageRef, OutboxDeadLetters, OutboxStore, WorkerId,
};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
async fn kill_envelope(
    store: &InMemoryOutboxStore,
    worker: &WorkerId,
    envelope: reliar_core::SerializedEnvelope,
) -> MessageRef {
    let message = store.insert(envelope);
    store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(1))
        .await
        .expect("acquire succeeds");
    store
        .fail(
            worker,
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
    message
}

async fn kill(store: &InMemoryOutboxStore, worker: &WorkerId) -> MessageRef {
    kill_envelope(store, worker, common::serialized_envelope()).await
}

#[tokio::test]
async fn pages_are_ordered_by_sequence_with_a_full_page_cursor() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();

    let mut dead = Vec::new();
    for _ in 0..5 {
        dead.push(kill(&store, &worker).await);
    }

    let first = store
        .list_dead(DeadQuery::default().limit(2))
        .await
        .expect("list_dead succeeds");
    assert_eq!(first.records.len(), 2);
    assert!(first.poisoned.is_empty());
    let first_ids: Vec<_> = first.records.iter().map(|r| r.envelope.id).collect();
    assert_eq!(first_ids, vec![dead[0].id, dead[1].id]);
    let cursor = first
        .next_after_sequence
        .expect("a full page carries a cursor");

    let second = store
        .list_dead(DeadQuery::default().limit(2).after_sequence(cursor))
        .await
        .expect("list_dead succeeds");
    let second_ids: Vec<_> = second.records.iter().map(|r| r.envelope.id).collect();
    assert_eq!(second_ids, vec![dead[2].id, dead[3].id]);
    let cursor = second
        .next_after_sequence
        .expect("a full page carries a cursor");

    let third = store
        .list_dead(DeadQuery::default().limit(2).after_sequence(cursor))
        .await
        .expect("list_dead succeeds");
    assert_eq!(third.records.len(), 1, "the tail page is not full");
    assert_eq!(third.records[0].envelope.id, dead[4].id);
    assert!(
        third.next_after_sequence.is_none(),
        "a page shorter than the limit carries no cursor"
    );
}

#[tokio::test]
async fn message_type_filter_excludes_non_matching_rows() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();
    let dead = kill(&store, &worker).await;

    let matching = store
        .list_dead(DeadQuery::default().message_type("orders.created"))
        .await
        .expect("list_dead succeeds");
    assert_eq!(matching.records.len(), 1);
    assert_eq!(matching.records[0].envelope.id, dead.id);

    let empty = store
        .list_dead(DeadQuery::default().message_type("orders.cancelled"))
        .await
        .expect("list_dead succeeds");
    assert!(empty.is_empty());
}

#[tokio::test]
async fn tenant_id_filter_excludes_non_matching_rows() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();

    let tenant_a = Envelope::builder(common::OrderCreated { order_id: 1 })
        .tenant("tenant-a")
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"));
    let dead_a = kill_envelope(&store, &worker, tenant_a).await;

    let tenant_b = Envelope::builder(common::OrderCreated { order_id: 2 })
        .tenant("tenant-b")
        .build()
        .map_body(|_| bytes::Bytes::from_static(b"{}"));
    kill_envelope(&store, &worker, tenant_b).await;

    let matching = store
        .list_dead(DeadQuery::default().tenant_id("tenant-a"))
        .await
        .expect("list_dead succeeds");
    assert_eq!(matching.records.len(), 1);
    assert_eq!(matching.records[0].envelope.id, dead_a.id);
}

#[tokio::test]
async fn dead_before_filter_excludes_rows_that_died_later() {
    let store = InMemoryOutboxStore::default();
    let worker = WorkerId::generate();

    let early = kill(&store, &worker).await;
    let cutoff = time::OffsetDateTime::UNIX_EPOCH + Duration::from_secs(100);
    store.advance(Duration::from_secs(200));
    let late = kill(&store, &worker).await;

    let matching = store
        .list_dead(DeadQuery::default().dead_before(cutoff))
        .await
        .expect("list_dead succeeds");
    assert_eq!(matching.records.len(), 1);
    assert_eq!(matching.records[0].envelope.id, early.id);
    assert_ne!(matching.records[0].envelope.id, late.id);
}
