//! [`EnqueueBatchError`]'s `Display`/`Debug`/`source()`/`Classify` behave as contract §3 says
//! (ADR 0036 §3, E10). `#[non_exhaustive]` keeps this crate from constructing the type directly,
//! so every assertion here is driven off an error a real `enqueue_batch` call produced.

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Classify, Envelope, FailureKind, MessageId, SerializedEnvelope};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryStoreError, InMemoryTransaction, OutboxEnqueue, OutboxPublisher,
    RecordingPublisher,
};

/// Wraps no store at all — always fails `enqueue` with
/// [`InMemoryStoreError::InjectedPermanent`]. `InMemoryOutboxStore::fail_next_enqueue` can only
/// inject the `Transient` variant, so without this the `Classify` forwarding claim below only
/// ever reaches `FailureKind::Transient` and would still pass if `Classify` for
/// `EnqueueBatchError` collapsed to a constant `Transient` verdict — the same pattern as
/// `FailsAtId` next door, in `enqueue_batch_fails_fast_and_names_the_index.rs` (review round 1,
/// minor).
#[derive(Clone, Copy, Default)]
struct FailsPermanent;

impl OutboxEnqueue<InMemoryTransaction> for FailsPermanent {
    type Error = InMemoryStoreError;

    fn enqueue(
        &self,
        _tx: &mut InMemoryTransaction,
        _envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send {
        std::future::ready(Err(InMemoryStoreError::InjectedPermanent))
    }
}

#[tokio::test]
async fn display_names_the_index_and_forwards_the_sources_message() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_enqueue(1);
    let outbox = OutboxPublisher::new(store, RecordingPublisher::default());
    let envelope =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue_batch(&mut tx, &[envelope])
        .await
        .expect_err("the store was armed to fail this enqueue");

    assert_eq!(err.index, 0);
    let text = err.to_string();
    assert!(text.contains('0'), "must name the failing index: {text}");
    assert!(
        text.contains(&InMemoryStoreError::Injected.to_string()),
        "must forward the source's own message: {text}"
    );
}

#[tokio::test]
async fn source_is_wired_to_the_provider_error() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_enqueue(1);
    let outbox = OutboxPublisher::new(store, RecordingPublisher::default());
    let envelope =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue_batch(&mut tx, &[envelope])
        .await
        .expect_err("the store was armed to fail this enqueue");

    let source = std::error::Error::source(&err).expect("source() must be wired");
    assert!(matches!(
        source.downcast_ref::<InMemoryStoreError>(),
        Some(InMemoryStoreError::Injected)
    ));
}

#[tokio::test]
async fn classify_forwards_transient_to_the_source() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_enqueue(1);
    let outbox = OutboxPublisher::new(store, RecordingPublisher::default());
    let envelope =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue_batch(&mut tx, &[envelope])
        .await
        .expect_err("the store was armed to fail this enqueue");

    assert_eq!(err.kind(), FailureKind::Transient);
}

#[tokio::test]
async fn classify_forwards_permanent_to_the_source() {
    // `InMemoryOutboxStore::fail_next_enqueue` can only inject `InMemoryStoreError::Injected`
    // (`Transient`), so the sibling test above alone would still pass if `Classify` for
    // `EnqueueBatchError` ever collapsed to a constant `Transient` verdict instead of forwarding.
    // `FailsPermanent` closes that gap.
    let outbox = OutboxPublisher::new(FailsPermanent, RecordingPublisher::default());
    let envelope =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue_batch(&mut tx, &[envelope])
        .await
        .expect_err("FailsPermanent always fails");

    assert_eq!(err.kind(), FailureKind::Permanent);
}

#[tokio::test]
async fn debug_names_the_index_field() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_enqueue(1);
    let outbox = OutboxPublisher::new(store, RecordingPublisher::default());
    let envelope =
        common::serialize(Envelope::builder(common::OrderCreated { order_id: 1 }).build());

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue_batch(&mut tx, &[envelope])
        .await
        .expect_err("the store was armed to fail this enqueue");

    let text = format!("{err:?}");
    assert!(text.contains("index"));
    assert!(text.contains('0'));
}
