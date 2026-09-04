//! `fail_next(n)` makes the next `n` calls to any `OutboxStore` method fail with
//! `InMemoryStoreError::Injected` (§43.A.18), then decrements to zero and stops; it never
//! affects `OutboxDeadLetters` methods. `InMemoryStoreError` self-classifies as transient.

#![cfg(feature = "test-support")]

use reliar_outbox::{
    AcquireRequest, Classify, DeadQuery, FailureKind, InMemoryOutboxStore, OutboxDeadLetters,
    OutboxStore, WorkerId,
};

#[tokio::test]
async fn fail_next_counts_down_and_then_stops() {
    let store = InMemoryOutboxStore::default();
    store.fail_next(2);

    let first = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await;
    assert!(first.is_err(), "the first injected failure fires");

    let second = store.stats().await;
    assert!(
        second.is_err(),
        "fail_next applies across different OutboxStore methods, not just the one it was set before"
    );

    let third = store.stats().await;
    assert!(
        third.is_ok(),
        "the count reached zero: the third call succeeds"
    );
}

#[tokio::test]
async fn fail_next_never_affects_dead_letter_methods() {
    let store = InMemoryOutboxStore::default();
    store.fail_next(10);

    let result = store.list_dead(DeadQuery::default()).await;
    assert!(
        result.is_ok(),
        "OutboxDeadLetters methods are never failed by fail_next"
    );

    // The injected count is still untouched — an `OutboxStore` call still fails.
    let acquired = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await;
    assert!(acquired.is_err());
}

#[tokio::test]
async fn injected_error_classifies_as_transient() {
    let store = InMemoryOutboxStore::default();
    store.fail_next(1);

    let err = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .expect_err("injected failure");
    assert_eq!(err.kind(), FailureKind::Transient);
}
