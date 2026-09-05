//! `ScopedOutboxPublisher::publish_batch` (the inherited `reliar_core::Publisher` default): results
//! are **positional**, mixed routes are preserved in call order, staging happens **sequentially**,
//! and a mid-batch stage failure still leaves earlier entries `Ok` — proving §4.1's "a positional
//! `Ok` is not durability" note rather than assuming it (§43.D, R22, ADR 0033 Amendment D).
//!
//! The "not durable after a rollback" half of that note needs a **real** transaction — this
//! crate's fakes commit eagerly and have no rollback to give — so it is proven against real
//! Postgres in `crates/reliar-store-postgres/tests/postgres/routing_enqueue.rs`
//! (`positional_ok_is_not_durability_a_later_stage_failure_aborts_the_whole_transaction`), not
//! here.

#![cfg(feature = "test-support")]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering as AtomicOrdering};

use reliar_core::{Envelope, MessageId, Publisher as _, SerializedEnvelope};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryStoreError, InMemoryTransaction, OutboxPolicy, OutboxPublisher,
    OutboxSettings, OutboxStaging, RecordingPublisher,
};

#[tokio::test]
async fn results_are_positional_and_mixed_routes_are_preserved() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let settings = OutboxSettings::default()
        .allowed_types(
            reliar_outbox::MessageTypeNames::try_from_iter("test", ["a"]).expect("valid"),
        )
        .expect("no overlap");
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);

    let a1 = common::serialize(Envelope::builder(common::TypeA).build());
    let c = common::serialize(Envelope::builder(common::TypeC).build());
    let a2 = common::serialize(Envelope::builder(common::TypeA).build());
    let ids = [a1.id, c.id, a2.id];

    let mut tx = InMemoryTransaction;
    let results = outbox
        .in_transaction(&mut tx)
        .publish_batch(&[a1, c, a2])
        .await;

    assert_eq!(results.len(), 3);
    assert!(results.iter().all(Result::is_ok), "{results:?}");

    // Mixed routes, in the input's own order: a1 (staged), c (published), a2 (staged).
    assert!(store.record(ids[0]).is_some());
    assert_eq!(publisher.published(), vec![ids[1]]);
    assert!(store.record(ids[2]).is_some());
}

/// Wraps [`InMemoryOutboxStore`] to fail exactly one, by-id, chosen `stage` call — precise
/// control the shared `fail_next_enqueue` counter can't give, since that always fires on the
/// *next* call rather than a specific position in a batch.
#[derive(Clone)]
struct FailsOneId {
    inner: InMemoryOutboxStore,
    fails: MessageId,
}

impl OutboxStaging<InMemoryTransaction> for FailsOneId {
    type Error = InMemoryStoreError;

    fn stage(
        &self,
        tx: &mut InMemoryTransaction,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send {
        let fails = envelope.id == self.fails;
        let inner = self.inner.clone();
        let envelope = envelope.clone();
        let mut tx = *tx;
        async move {
            if fails {
                return Err(InMemoryStoreError::Injected);
            }
            inner.stage(&mut tx, &envelope).await
        }
    }
}

#[tokio::test]
async fn a_mid_batch_stage_failure_leaves_earlier_entries_ok() {
    let first = common::serialize(Envelope::builder(common::TypeA).build());
    let second = common::serialize(Envelope::builder(common::TypeB).build());
    let third = common::serialize(Envelope::builder(common::TypeC).build());

    let store = FailsOneId {
        inner: InMemoryOutboxStore::default(),
        fails: second.id,
    };
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher, OutboxPolicy::default());

    let mut tx = InMemoryTransaction;
    let results = outbox
        .in_transaction(&mut tx)
        .publish_batch(&[first.clone(), second.clone(), third.clone()])
        .await;

    assert!(
        results[0].is_ok(),
        "the first entry's statement was accepted"
    );
    assert!(results[1].is_err(), "the chosen entry fails");
    assert!(
        results[2].is_ok(),
        "the default loop keeps going past a failed entry"
    );

    // The positional `Ok`s are statements accepted, not a durability claim: this fake stages
    // eagerly with no surrounding transaction to roll back, so both surviving rows are visible —
    // the real-Postgres companion test is what proves a later failure can undo an earlier `Ok`.
    assert!(store.inner.record(first.id).is_some());
    assert!(store.inner.record(second.id).is_none());
    assert!(store.inner.record(third.id).is_some());
}

/// Wraps [`InMemoryOutboxStore`] to prove `publish_batch` stages **sequentially**: panics if a
/// second `stage` call begins before the first one's future has resolved. A `tokio::task::yield_now`
/// inside gives the executor a real scheduling point to interleave at if some future change ever
/// made this run concurrently.
#[derive(Clone, Default)]
struct SequentialProbe {
    inner: InMemoryOutboxStore,
    in_flight: Arc<AtomicBool>,
    calls: Arc<AtomicUsize>,
}

impl SequentialProbe {
    fn calls(&self) -> usize {
        self.calls.load(AtomicOrdering::SeqCst)
    }
}

impl OutboxStaging<InMemoryTransaction> for SequentialProbe {
    type Error = InMemoryStoreError;

    fn stage(
        &self,
        _tx: &mut InMemoryTransaction,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<MessageId, Self::Error>> + Send {
        assert!(
            !self.in_flight.swap(true, AtomicOrdering::SeqCst),
            "a second stage() call began before the first one finished — publish_batch must be \
             sequential (§4.1)"
        );
        self.calls.fetch_add(1, AtomicOrdering::SeqCst);
        let inner = self.inner.clone();
        let envelope = envelope.clone();
        let in_flight = Arc::clone(&self.in_flight);
        async move {
            tokio::task::yield_now().await;
            let result = inner.stage(&mut InMemoryTransaction, &envelope).await;
            in_flight.store(false, AtomicOrdering::SeqCst);
            result
        }
    }
}

#[tokio::test]
async fn staging_happens_sequentially_not_concurrently() {
    let probe = SequentialProbe::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(probe.clone(), publisher, OutboxPolicy::default());

    let envelopes: Vec<SerializedEnvelope> = (0..5)
        .map(|_| common::serialize(Envelope::builder(common::TypeA).build()))
        .collect();

    let mut tx = InMemoryTransaction;
    let results = outbox
        .in_transaction(&mut tx)
        .publish_batch(&envelopes)
        .await;

    assert!(results.iter().all(Result::is_ok));
    assert_eq!(probe.calls(), 5);
}
