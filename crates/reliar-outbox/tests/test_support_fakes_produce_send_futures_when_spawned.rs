//! `InMemoryOutboxStore`, `RecordingPublisher` and `ScriptedPublisher` are ordinary
//! implementors — no `async-trait` — so their futures must stay `Send` and safe to
//! `tokio::spawn`, exactly like any other `OutboxStore`/`Publisher`. Their methods never hold a
//! `std::sync::MutexGuard` across an `.await`, which is what makes that true.
#![cfg(feature = "test-support")]

mod common;

use std::sync::Arc;

use reliar_outbox::{
    AcquireRequest, InMemoryOutboxStore, OutboxStore, Publisher, RecordingPublisher, WorkerId,
};

fn assert_send<T: Send>(_: &T) {}

#[tokio::test]
async fn store_calls_spawn_cleanly() {
    let store = Arc::new(InMemoryOutboxStore::default());
    store.insert(common::serialized_envelope());

    let acquire_future = store.acquire(AcquireRequest::new(WorkerId::generate()));
    assert_send(&acquire_future);
    let batch = acquire_future.await.expect("acquire succeeds");
    assert_eq!(batch.records.len(), 1);

    let handle = tokio::spawn({
        let store = Arc::clone(&store);
        async move {
            store
                .acquire(AcquireRequest::new(WorkerId::generate()))
                .await
        }
    });
    let empty = handle
        .await
        .expect("spawned task joins")
        .expect("acquire succeeds");
    assert!(empty.is_empty(), "the only row is already leased");
}

#[tokio::test]
async fn recording_publisher_calls_spawn_cleanly() {
    let publisher = Arc::new(RecordingPublisher::default());
    let envelope = Arc::new(common::serialized_envelope());

    let handle = tokio::spawn({
        let publisher = Arc::clone(&publisher);
        let envelope = Arc::clone(&envelope);
        async move { publisher.publish(&envelope).await }
    });

    handle
        .await
        .expect("spawned task joins")
        .expect("publish never fails");
    assert_eq!(publisher.published().len(), 1);
}
