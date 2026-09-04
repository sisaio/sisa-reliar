//! Throughput of `OutboxDispatcher::run`'s claim → publish → complete path over the
//! `test-support` fakes (no broker, no database — this measures the dispatcher's own overhead,
//! not a transport's or Postgres's).
//!
//! Run with `cargo bench -p reliar-outbox --features test-support`. A baseline is recorded in
//! the task card (`docs/backlog/RELIAR-15-*.md`) — compare a new run's `outbox/*` lines against
//! it before trusting a regression.
//!
//! Seeding the store is `iter_batched` **setup**, excluded from the measured time; only the
//! claim → publish → complete path itself is timed. Completion is detected through a
//! [`tokio::sync::Semaphore`] a bench-local [`OutboxMetrics`] hook feeds one permit per published
//! row, so the loop `acquire_many`s exactly `count` permits instead of re-scanning a growing
//! `Vec` every poll (an O(n²) busy-loop an earlier version of this bench used).

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use reliar_core::{Envelope, Message, MessageId, MessageType};
use reliar_outbox::{
    DispatcherSettings, InMemoryOutboxStore, OutboxDispatcher, OutboxMetrics, RecordingPublisher,
};
use tokio::runtime::Runtime;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct BenchMessage {
    n: u64,
}

impl Message for BenchMessage {
    const TYPE: &'static str = "bench.message";
    const VERSION: u16 = 1;
}

fn seeded_store(count: usize) -> InMemoryOutboxStore {
    let store = InMemoryOutboxStore::default();
    for _ in 0..count {
        let mut envelope = Envelope::builder(BenchMessage { n: 0 })
            .build()
            .map_body(|_| Bytes::from_static(b"{}"));
        envelope.id = MessageId::new();
        store.insert(envelope);
    }
    store
}

/// Feeds one [`Semaphore`] permit per published row, so waiting for "every seeded row published"
/// is a single `acquire_many`, not a poll loop that re-inspects growing state.
struct CompletionMetrics {
    remaining: Arc<Semaphore>,
}

impl OutboxMetrics for CompletionMetrics {
    fn published(&self, n: usize, _message_type: &MessageType) {
        self.remaining.add_permits(n);
    }
}

/// Runs one dispatcher over `store`'s pre-seeded rows to completion (real, unpaused time — a
/// bench measures wall-clock throughput, not deterministic ordering) and returns once every row
/// has been published.
async fn drain_via_dispatcher(store: InMemoryOutboxStore, count: usize) {
    let publisher = RecordingPublisher::default();
    let remaining = Arc::new(Semaphore::new(0));
    let metrics = CompletionMetrics {
        remaining: Arc::clone(&remaining),
    };

    let batch_size = u32::try_from(count).unwrap_or(u32::MAX);
    let settings = DispatcherSettings::default()
        .batch_size(batch_size)
        .max_in_flight(64)
        .poll_interval(Duration::from_micros(100))
        .idle_poll_interval(Duration::from_micros(100));
    let Ok(dispatcher) = OutboxDispatcher::builder(store, publisher)
        .settings(settings)
        .metrics(metrics)
        .build()
    else {
        // Unreachable with the fixed settings above; never panics if it somehow changes.
        return;
    };

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Waits for exactly `count` permits (one per published row) rather than polling a getter.
    let permits_needed = u32::try_from(count).unwrap_or(u32::MAX);
    let _ = remaining.acquire_many(permits_needed).await;

    cancel.cancel();
    let _ = handle.await;
}

fn outbox_throughput(c: &mut Criterion) {
    let Ok(runtime) = Runtime::new() else {
        eprintln!("reliar-outbox bench: failed to create a Tokio runtime; skipping");
        return;
    };
    let mut group = c.benchmark_group("outbox");

    for count in [10_usize, 100, 1_000] {
        group.throughput(criterion::Throughput::Elements(count as u64));
        group.bench_function(format!("claim_publish_complete_{count}"), |b| {
            b.to_async(&runtime).iter_batched(
                || seeded_store(count),
                |store| drain_via_dispatcher(store, count),
                BatchSize::SmallInput,
            );
        });
    }

    group.finish();
}

criterion_group!(benches, outbox_throughput);
criterion_main!(benches);
