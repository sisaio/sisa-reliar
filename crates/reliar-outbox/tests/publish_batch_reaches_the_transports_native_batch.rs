//! `OutboxPublisher::publish_batch` is overridden to forward to `P::publish_batch` rather than
//! inherited — results stay positional, one per envelope, in order, and the store is never
//! touched (ADR 0036, contract §2.2, E2).

#![cfg(feature = "test-support")]

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use reliar_core::{Envelope, Publisher, SerializedEnvelope};
use reliar_outbox::{
    FakePublishError, InMemoryOutboxStore, OutboxPublisher, PublishStep, ScriptedPublisher,
};

/// Wraps [`ScriptedPublisher`] to count calls to `publish_batch` **specifically** — the plain
/// fake alone can't distinguish "the batch entry point ran" from "the trait default looped over
/// `publish` N times", because both leave the same `published()` trail. `ScriptedPublisher` (not
/// `RecordingPublisher`, which never fails) is the inner fake so one entry in the batch can be
/// scripted to fail — otherwise both results would be `Ok` and a positional swap would go
/// unnoticed (review round 1, minor).
#[derive(Clone)]
struct BatchTrackingPublisher {
    inner: ScriptedPublisher,
    batch_calls: Arc<AtomicUsize>,
}

impl BatchTrackingPublisher {
    fn new(inner: ScriptedPublisher) -> Self {
        Self {
            inner,
            batch_calls: Arc::default(),
        }
    }
}

impl Publisher for BatchTrackingPublisher {
    type Error = FakePublishError;

    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.inner.publish(envelope)
    }

    fn publish_batch(
        &self,
        envelopes: &[SerializedEnvelope],
    ) -> impl Future<Output = Vec<Result<(), Self::Error>>> + Send {
        self.batch_calls.fetch_add(1, AtomicOrdering::SeqCst);
        self.inner.publish_batch(envelopes)
    }
}

#[tokio::test]
async fn publish_batch_reaches_the_publishers_own_batch_entry_point_in_order() {
    let store = InMemoryOutboxStore::default();
    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    let ids = [a.id, b.id];

    // `b` is scripted to fail and `a` is left out of the map (so it publishes `Ok`, per
    // `ScriptedPublisher::keyed`'s documented default) — with both `Ok`, a bug that swapped or
    // reordered the results would go unnoticed; scripting exactly one failure makes the
    // positional pairing (`results[0]` ↔ `a`, `results[1]` ↔ `b`) load-bearing.
    let inner = ScriptedPublisher::keyed([(b.id, PublishStep::Transient)]);
    let publisher = BatchTrackingPublisher::new(inner);
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone());

    let results = outbox.publish_batch(&[a, b]).await;

    assert_eq!(results.len(), 2);
    assert!(
        results[0].is_ok(),
        "index 0 (a) was not scripted to fail: {results:?}"
    );
    assert!(
        matches!(&results[1], Err(FakePublishError::Transient { .. })),
        "index 1 (b) was scripted to fail: {results:?}"
    );
    assert_eq!(
        publisher.batch_calls.load(AtomicOrdering::SeqCst),
        1,
        "publish_batch must forward to P::publish_batch, not loop over P::publish"
    );
    assert_eq!(publisher.inner.published(), ids.to_vec());
    assert_eq!(
        store.enqueue_call_count(),
        0,
        "a batch publish must never call OutboxEnqueue::enqueue"
    );
    assert!(store.records().is_empty());
}
