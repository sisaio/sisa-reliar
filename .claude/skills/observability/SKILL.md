---
name: observability
description: Observability for a Rust LIBRARY (Reliar) — `tracing` spans with the predictable operation names from SRS §33 (reliar.outbox.claim / publish / retry, reliar.inbox.process), span fields for ids (message.id, correlation.id, worker.id) but never payloads or custom headers, a static-dispatch `OutboxMetrics` hook trait with a `NoopMetrics` default and an optional `metrics`-facade adapter behind a feature, bounded metric labels (message_type ok, ids never), outbox-lag and dead-count gauges, W3C traceparent/tracestate carried in Metadata (not re-derived), and no OTel exporter dependency inside library crates — the host app owns exporters. Use when adding spans, metrics hooks, log lines, or reviewing what a code path emits.
metadata:
  audience: ENGINEER, ARCHITECT, REVIEWER
---

# Observability (library edition)

A library **emits**; the application **exports**. Reliar depends on `tracing` (and optionally the
`metrics` facade) and never on an OpenTelemetry exporter, collector config, or dashboard. SRS §33.

## Span names — stable, predictable, dotted

| Span | Where | Fields |
|---|---|---|
| `reliar.outbox.enqueue` | provider `enqueue` | `message.id`, `message.type`, `conversation.id` |
| `reliar.outbox.claim` | dispatcher → `acquire` | `worker.id`, `batch.requested`, `batch.claimed` |
| `reliar.outbox.publish` | per message | `message.id`, `message.type`, `attempt`, `correlation.id`, `traceparent` |
| `reliar.outbox.retry` | scheduling a retry | `message.id`, `attempt`, `delay_ms`, `error.kind` |
| `reliar.outbox.dead` | dead-lettering | `message.id`, `attempt`, `error.kind` |
| `reliar.outbox.purge` | retention | `deleted` |
| `reliar.inbox.process` | Phase 3 | `message.id`, `handler` |

```rust
#[tracing::instrument(name = "reliar.outbox.publish", skip_all,
    fields(message.id = %rec.envelope.id, message.type = %rec.envelope.message_type, attempt = rec.attempts + 1))]
async fn publish_one<P: Publisher>(p: &P, rec: &OutboxRecord) -> Result<(), P::Error> { p.publish(&rec.envelope).await }
```

`skip_all` is mandatory — an `Envelope`/`OutboxRecord` in a span field would print the payload.
Custom `Debug` on payload-bearing types elides bytes (`payload: <128 bytes>`) as a second guard.

## What is never emitted by default

- Payload bytes, deserialized bodies, custom `Headers` values (SRS §33 — may hold PII).
- Connection strings, broker credentials.
- `last_error` text is stored in the DB for inspection; log it at `warn` **without** the payload.

## Metrics — a hook trait, static dispatch, no-op default

```rust
/// Implement to receive dispatcher counters; every method has a no-op default.
pub trait OutboxMetrics: Send + Sync {
    fn claimed(&self, n: usize) {}
    fn published(&self, n: usize, message_type: &MessageType) {}
    fn retried(&self, n: usize, kind: FailureKind) {}
    fn dead(&self, n: usize) {}
    fn publish_duration(&self, d: Duration, message_type: &MessageType) {}
    fn oldest_pending_age(&self, age: Duration) {}        // "outbox lag" — the alerting signal
}
#[derive(Clone, Copy, Default, Debug)] pub struct NoopMetrics;
impl OutboxMetrics for NoopMetrics {}
```

`OutboxDispatcher<S, P, M = NoopMetrics>` — zero cost when unused. Behind `feature = "metrics"`,
`MetricsFacade` implements the trait on the `metrics` crate with names
`reliar_outbox_claimed_total`, `reliar_outbox_published_total{message_type}`,
`reliar_outbox_retried_total{kind}`, `reliar_outbox_dead_total`,
`reliar_outbox_publish_duration_seconds{message_type}`, `reliar_outbox_oldest_pending_age_seconds`.

**Labels are bounded**: `message_type` and `kind` only. Never `message_id`, `correlation_id`,
`tenant_id`, `worker_id` (SRS §33) — cardinality explosions are the operator's problem, and it's ours
to prevent.

## Trace context — carry, don't invent

`Metadata.trace { traceparent, tracestate }` is set by the **application** at enqueue (from its own
OTel/tracing integration) and carried untouched through the store to the transport mapper, which
writes it as the W3C headers `traceparent`/`tracestate`. The publish span records `traceparent` as a
field so the exporter can link; Reliar does not depend on `tracing-opentelemetry`. Business
correlation (`correlation_id`) and tracing stay separate concepts (SRS §12).

## Logging levels

`error` — the dispatcher loop itself is failing (store unreachable) · `warn` — a message went dead,
or complete/fail affected fewer rows than expected (reclaimed) · `info` — start/stop, config summary
(no secrets) · `debug` — per-batch counts · `trace` — never payloads either.

## Definition of done (observability)

- [ ] New code paths carry the `reliar.<crate>.<op>` span with the fields above; `skip_all` on instrumented fns.
- [ ] No payload/header/credential in any span field, log line, or error `Display`.
- [ ] Counters flow through the `OutboxMetrics` hook; labels limited to `message_type`/`kind`.
- [ ] `oldest_pending_age` is reported so operators can alert on outbox lag.
- [ ] No OTel exporter/collector dependency added to a library crate.
