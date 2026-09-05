//! Baseline throughput of `NatsPublisher::publish` against a real `JetStream` server.
//!
//! Records a baseline only when `NATS_URL` is set — this crate's own tests apply the same rule
//! (ADR 0031 §4): there is no testcontainers-managed server a criterion run should start and tear
//! down on every invocation, so this skips cleanly when the environment does not provide one.
//!
//! Run: `NATS_URL=nats://127.0.0.1:4222 cargo bench -p reliar-transport-nats`.

use async_nats::jetstream::stream::Config as StreamConfig;
use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use reliar_core::{Envelope, Message, MessageId, Publisher};
use reliar_transport_nats::{NatsPublisher, NatsSettings};
use tokio::runtime::Runtime;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct BenchMessage {
    n: u64,
}

impl Message for BenchMessage {
    const TYPE: &'static str = "bench.message";
    const VERSION: u16 = 1;
}

/// Connects, creates a throwaway stream scoped to this run, and wraps it in a publisher. `None`
/// on any failure — the bench treats "no reachable `JetStream` server" the same as "`NATS_URL`
/// unset": skip, don't fail the run.
async fn build_publisher(url: &str) -> Option<(NatsPublisher, String)> {
    let client = async_nats::connect(url).await.ok()?;
    let context = async_nats::jetstream::new(client);
    let id = uuid::Uuid::now_v7().simple();
    let stream_name = format!("RELIAR_BENCH_{id}");
    let subject_prefix = format!("reliar.bench.{id}");
    context
        .create_stream(StreamConfig {
            name: stream_name.clone(),
            subjects: vec![format!("{subject_prefix}.>")],
            ..StreamConfig::default()
        })
        .await
        .ok()?;
    let publisher = NatsPublisher::new(
        context,
        NatsSettings::default().subject_prefix(subject_prefix),
    )
    .ok()?;
    Some((publisher, stream_name))
}

fn nats_publish(c: &mut Criterion) {
    let Ok(url) = std::env::var("NATS_URL") else {
        eprintln!("reliar-transport-nats bench: NATS_URL not set; skipping");
        return;
    };
    let Ok(runtime) = Runtime::new() else {
        eprintln!("reliar-transport-nats bench: failed to create a Tokio runtime; skipping");
        return;
    };
    let Some((publisher, stream_name)) = runtime.block_on(build_publisher(&url)) else {
        eprintln!("reliar-transport-nats bench: no reachable JetStream server at {url}; skipping");
        return;
    };

    let mut group = c.benchmark_group("nats_publish");
    group.bench_function("publish_one", |b| {
        b.to_async(&runtime).iter_batched(
            || {
                let mut envelope = Envelope::builder(BenchMessage { n: 0 })
                    .build()
                    .map_body(|_| Bytes::from_static(b"{}"));
                envelope.id = MessageId::new();
                envelope
            },
            |envelope| {
                let publisher = publisher.clone();
                async move {
                    let _ = publisher.publish(&envelope).await;
                }
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();

    // Best-effort cleanup: a leftover throwaway stream on a dev server is a minor nuisance, not
    // a correctness issue, so this is never `.expect()`ed.
    runtime.block_on(async {
        if let Ok(client) = async_nats::connect(&url).await {
            let _ = async_nats::jetstream::new(client)
                .delete_stream(&stream_name)
                .await;
        }
    });
}

criterion_group!(benches, nats_publish);
criterion_main!(benches);
