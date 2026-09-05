//! Baseline throughput of [`NatsEnvelopeMapper::encode`]/[`NatsEnvelopeMapper::decode`] alone —
//! pure CPU work, no server, no `NATS_URL` (S1 review, minor 15). `benches/nats_publish.rs` covers
//! the end-to-end `NatsPublisher::publish` cost against a real `JetStream` server; this bench
//! isolates the mapping step it depends on.
//!
//! `allow-expect-in-tests` (clippy.toml) only recognises `#[test]`-attributed functions; bench
//! functions get the same allowance explicitly instead, same as this crate's own test files.
#![allow(clippy::expect_used)]

use bytes::Bytes;
use criterion::{Criterion, criterion_group, criterion_main};
use reliar_core::{CorrelationId, Envelope, EnvelopeMapper, Message, Metadata};
use reliar_transport_nats::NatsEnvelopeMapper;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct BenchMessage {
    n: u64,
}

impl Message for BenchMessage {
    const TYPE: &'static str = "bench.message";
    const VERSION: u16 = 1;
}

/// A representative envelope: every canonical metadata field set, one custom header, and a small
/// JSON-shaped body — closer to a real publish than the empty envelopes elsewhere in this crate's
/// unit tests.
fn representative_envelope() -> reliar_core::SerializedEnvelope {
    let mut metadata = Metadata::default();
    metadata.correlation.correlation_id =
        Some(CorrelationId::parse("order-12345").expect("legal correlation id"));
    metadata.tenant_id = Some("acme".to_string());
    metadata.trace.traceparent =
        Some("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_string());

    Envelope::builder(BenchMessage { n: 42 })
        .metadata(metadata)
        .header("x-import-batch", "2026-09-04")
        .expect("a plain ASCII key/value is always accepted")
        .build()
        .map_body(|_| Bytes::from_static(br#"{"n":42,"note":"representative payload"}"#))
}

fn nats_encode(c: &mut Criterion) {
    let mapper = NatsEnvelopeMapper::default();
    let envelope = representative_envelope();
    let wire = mapper
        .encode(&envelope)
        .expect("representative_envelope is always encodable");

    let mut group = c.benchmark_group("nats_encode");
    group.bench_function("encode", |b| {
        b.iter(|| mapper.encode(&envelope).expect("always encodable"));
    });
    group.bench_function("decode", |b| {
        b.iter(|| {
            mapper
                .decode(wire.clone())
                .expect("a message this mapper encoded always decodes")
        });
    });
    group.finish();
}

criterion_group!(benches, nats_encode);
criterion_main!(benches);
