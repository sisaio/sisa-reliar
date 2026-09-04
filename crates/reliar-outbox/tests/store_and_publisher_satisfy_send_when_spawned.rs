//! An `OutboxStore`/`Publisher` implemented with plain `async fn`s (no `async-trait`) produces
//! `Send` futures, so the dispatcher (S4) can `tokio::spawn` its calls. A compile-level guarantee
//! — if either trait's `impl Future<Output = …> + Send` bound stopped holding for an ordinary
//! implementor, this file would fail to build, not just fail at runtime.
//!
//! Each method genuinely suspends (`tokio::task::yield_now().await`) **after** touching its
//! borrowed arguments, so the future's captured state — including those borrows — really does
//! have to be `Send` across a suspend point, not just at the call site (review 1 major 7: a
//! `std::future::ready`-based fake never proves this, because it never actually suspends).

mod common;

use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use reliar_core::MessageId;
use reliar_outbox::{
    AcquireRequest, AcquiredBatch, Classify, CompletedMessage, FailedMessage, FailureKind,
    MessageRef, OutboxStats, OutboxStore, Publisher, PurgeReport, PurgeRequest, WorkerId,
};

#[derive(Debug)]
struct FakeError;

impl fmt::Display for FakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("fake failure")
    }
}

impl std::error::Error for FakeError {}

impl Classify for FakeError {
    fn kind(&self) -> FailureKind {
        FailureKind::Transient
    }
}

/// The minimum `OutboxStore` an `async fn`-only implementor can be: every method touches its
/// arguments (a `Mutex` increment, a slice length) and then actually suspends, just enough to
/// prove the trait's `Send` futures compile, cross a real await point, and spawn.
#[derive(Default)]
struct MinimalStore {
    acquired: Mutex<usize>,
}

impl OutboxStore for MinimalStore {
    type Error = FakeError;

    async fn acquire(&self, _request: AcquireRequest) -> Result<AcquiredBatch, Self::Error> {
        *self
            .acquired
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) += 1;
        tokio::task::yield_now().await;
        Ok(AcquiredBatch::default())
    }

    async fn complete(
        &self,
        _worker: &WorkerId,
        items: &[CompletedMessage],
    ) -> Result<u64, Self::Error> {
        let count = items.len();
        tokio::task::yield_now().await;
        Ok(count as u64)
    }

    async fn fail(&self, _worker: &WorkerId, items: &[FailedMessage]) -> Result<u64, Self::Error> {
        let count = items.len();
        tokio::task::yield_now().await;
        Ok(count as u64)
    }

    async fn release(&self, _worker: &WorkerId, items: &[MessageRef]) -> Result<u64, Self::Error> {
        let count = items.len();
        tokio::task::yield_now().await;
        Ok(count as u64)
    }

    async fn extend_lease(
        &self,
        _worker: &WorkerId,
        items: &[MessageRef],
        _lease: Duration,
    ) -> Result<u64, Self::Error> {
        let count = items.len();
        tokio::task::yield_now().await;
        Ok(count as u64)
    }

    async fn purge(&self, request: PurgeRequest) -> Result<PurgeReport, Self::Error> {
        let batch_size = request.batch_size;
        tokio::task::yield_now().await;
        Ok(PurgeReport::new(0, 0, batch_size.into()))
    }

    async fn stats(&self) -> Result<OutboxStats, Self::Error> {
        tokio::task::yield_now().await;
        Ok(OutboxStats::new(
            0,
            0,
            0,
            None,
            time::OffsetDateTime::now_utc(),
        ))
    }
}

#[derive(Default)]
struct MinimalPublisher;

impl Publisher for MinimalPublisher {
    type Error = FakeError;

    async fn publish(&self, envelope: &reliar_core::SerializedEnvelope) -> Result<(), Self::Error> {
        std::hint::black_box(envelope.id);
        tokio::task::yield_now().await;
        Ok(())
    }
}

fn assert_send<T: Send>(_: &T) {}

#[tokio::test]
async fn store_calls_produce_send_futures_and_spawn_cleanly() {
    let store = std::sync::Arc::new(MinimalStore::default());

    let acquire_future = store.acquire(AcquireRequest::new(WorkerId::generate()));
    assert_send(&acquire_future);
    acquire_future.await.expect("acquire succeeds");

    let handle = tokio::spawn({
        let store = std::sync::Arc::clone(&store);
        async move {
            store
                .acquire(AcquireRequest::new(WorkerId::generate()))
                .await
        }
    });
    let batch = handle
        .await
        .expect("spawned task joins")
        .expect("acquire succeeds");
    assert!(batch.is_empty());
    assert_eq!(
        *store.acquired.lock().unwrap(),
        2,
        "the awaited future ran too"
    );
}

#[tokio::test]
async fn publisher_calls_produce_send_futures_and_spawn_cleanly() {
    let publisher = std::sync::Arc::new(MinimalPublisher);
    let envelope = std::sync::Arc::new(common::serialized_envelope());

    let handle = tokio::spawn({
        let publisher = std::sync::Arc::clone(&publisher);
        let envelope = std::sync::Arc::clone(&envelope);
        async move { publisher.publish(&envelope).await }
    });

    handle
        .await
        .expect("spawned task joins")
        .expect("publish succeeds");

    // `MessageId` participates in `MessageRef`, which every store call above takes by
    // reference — confirms the shared reference type is `Send` too, not just the futures.
    let message_ref = MessageRef::new(MessageId::new(), time::OffsetDateTime::now_utc());
    assert_send(&message_ref);
}
