//! `OutboxPublisher::enqueue_batch` fails fast: the first enqueue failure returns
//! [`EnqueueBatchError`] naming its position, the store saw exactly that many calls, and no
//! envelope after the failure is attempted (ADR 0036 §5, contract §2.1, E6).
//!
//! This fake enqueues eagerly with no surrounding transaction to roll back, so the earlier `Ok`
//! staying visible here is a property of the fake, not a durability claim — the real-Postgres
//! companion test (`crates/reliar-store-postgres/tests/postgres/outbox_publisher_enqueue.rs`, E18)
//! is what proves a later failure can undo an earlier `Ok` inside a real transaction.

#![cfg(feature = "test-support")]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use reliar_core::{Envelope, MessageId, SerializedEnvelope};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryStoreError, InMemoryTransaction, OutboxEnqueue, OutboxPublisher,
    RecordingPublisher,
};

/// Wraps [`InMemoryOutboxStore`] to fail exactly one, by-id, chosen `enqueue` call, and count
/// every call it receives (whether it goes on to fail or succeed) — precise control the shared
/// `fail_next_enqueue` counter can't give on its own.
#[derive(Clone, Default)]
struct FailsAtId {
    inner: InMemoryOutboxStore,
    fails: Option<MessageId>,
    calls: Arc<AtomicUsize>,
}

impl OutboxEnqueue<InMemoryTransaction> for FailsAtId {
    type Error = InMemoryStoreError;

    fn enqueue(
        &self,
        tx: &mut InMemoryTransaction,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send {
        self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        let fails = self.fails == Some(envelope.id);
        let inner = self.inner.clone();
        let envelope = envelope.clone();
        let mut tx = *tx;
        async move {
            if fails {
                return Err(InMemoryStoreError::Injected);
            }
            inner.enqueue(&mut tx, &envelope).await
        }
    }
}

#[tokio::test]
async fn a_mid_batch_failure_names_its_index_and_stops_the_batch() {
    let first = common::serialize(Envelope::builder(common::TypeA).build());
    let second = common::serialize(Envelope::builder(common::TypeB).build());
    let third = common::serialize(Envelope::builder(common::TypeC).build());

    let store = FailsAtId {
        inner: InMemoryOutboxStore::default(),
        fails: Some(second.id),
        calls: Arc::default(),
    };
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher);

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue_batch(&mut tx, &[first.clone(), second.clone(), third.clone()])
        .await
        .expect_err("the second envelope was armed to fail");

    assert_eq!(err.index, 1, "the failing position is 0-indexed");
    assert!(matches!(err.source, InMemoryStoreError::Injected));

    assert_eq!(
        store.calls.load(AtomicOrdering::SeqCst),
        2,
        "exactly index + 1 calls reached the enqueue capability"
    );
    assert!(
        store.inner.record(first.id).is_some(),
        "the entry before the failure was enqueued"
    );
    assert!(
        store.inner.record(second.id).is_none(),
        "the failing entry itself was never enqueued"
    );
    assert!(
        store.inner.record(third.id).is_none(),
        "the entry after the failure was never attempted"
    );
}
