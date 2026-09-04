//! `Envelope<T>` ⇄ `SerializedEnvelope` cost through the default [`JsonSerializer`]: build →
//! serialize → [`Envelope::map_body`], and the reverse via [`Envelope::try_map_body`].
//!
//! Run with `cargo bench -p reliar-core --features json`. A baseline is recorded in the task
//! card (`docs/backlog/RELIAR-19-*.md`) — compare a new run's `serialization/*` lines against it
//! before trusting a regression.

use bytes::Bytes;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use reliar_core::{Envelope, JsonSerializer, Message, SerializedEnvelope, Serializer};
use serde::{Deserialize, Serialize};

/// A modestly-sized body — a handful of scalar fields and a short string list — representative of
/// a typical domain event, not a pathological worst case.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderCreated {
    order_id: u64,
    customer_id: u64,
    total_cents: i64,
    line_items: Vec<String>,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

fn sample_envelope() -> Envelope<OrderCreated> {
    Envelope::builder(OrderCreated {
        order_id: 42,
        customer_id: 7,
        total_cents: 12_345,
        line_items: vec!["widget".to_owned(), "gadget".to_owned(), "gizmo".to_owned()],
    })
    .build()
}

fn sample_serialized(serializer: &JsonSerializer) -> SerializedEnvelope {
    let envelope = sample_envelope();
    let bytes = serializer
        .serialize(&envelope.body)
        .unwrap_or_else(|_| Bytes::new());
    envelope.map_body(|_| bytes)
}

fn serialization(c: &mut Criterion) {
    let serializer = JsonSerializer;
    let mut group = c.benchmark_group("serialization");

    group.bench_function("serialize", |b| {
        b.iter_batched(
            sample_envelope,
            |envelope| serializer.serialize(&envelope.body),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("deserialize", |b| {
        b.iter_batched(
            || sample_serialized(&serializer),
            |serialized| serializer.deserialize::<OrderCreated>(&serialized.body),
            BatchSize::SmallInput,
        );
    });

    group.bench_function("round_trip", |b| {
        b.iter_batched(
            sample_envelope,
            |envelope| {
                let wire = envelope
                    .map_body(|body| serializer.serialize(&body).unwrap_or_else(|_| Bytes::new()));
                wire.try_map_body(|bytes| serializer.deserialize::<OrderCreated>(&bytes))
            },
            BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, serialization);
criterion_main!(benches);
