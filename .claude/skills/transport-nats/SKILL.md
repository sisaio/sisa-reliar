---
name: transport-nats
description: Phase 2 — Reliar's first real transport (reliar-transport-nats) — the NatsEnvelopeMapper that writes canonical Metadata as `reliar-*` NATS headers plus W3C traceparent/tracestate plus pass-through custom headers over a raw body (no JSON re-wrapping), Nats-Msg-Id = message_id for JetStream duplicate suppression, a transport-side SubjectResolver (message_type → subject) kept OUT of reliar-core, the NatsPublisher implementing Publisher with transient/permanent classification of async-nats errors, and end-to-end Outbox→NATS tests against a JetStream server. Use when building or reviewing the NATS mapper, publisher, subject strategy, or the Phase 2 tests.
metadata:
  audience: ENGINEER, ARCHITECT
---

# NATS transport (Phase 2)

Goal (SRS §42 Phase 2): prove the canonical Envelope and the `EnvelopeMapper` model with a real
broker **without touching `reliar-outbox`** (SRS §43.21). Dependencies: `async-nats`,
`reliar-core`, `reliar-outbox` (for `Publisher` + `Classify`). Nothing NATS-specific leaks upstream.

## Mapping — canonical Envelope → NATS message (SRS §15–§16)

| Envelope | NATS header |
|---|---|
| `id` | `reliar-message-id` **and** `Nats-Msg-Id` (JetStream dedup within the stream's duplicate window) |
| `message_type` (`orders.created.v1`) | `reliar-message-type` |
| `metadata.correlation.{correlation,conversation,causation,request}_id` | `reliar-correlation-id`, `reliar-conversation-id`, `reliar-causation-id`, `reliar-request-id` |
| `metadata.trace.{traceparent,tracestate}` | `traceparent`, `tracestate` (W3C names, unprefixed) |
| `metadata.delivery.content_type` | `reliar-content-type` (also `Content-Type` if the ADR wants interop) |
| `metadata.delivery.{sent_at,expires_at,deduplication_id}`, `tenant_id` | `reliar-sent-at`, `reliar-expires-at`, `reliar-dedup-id`, `reliar-tenant-id` |
| `metadata.routing.{source,destination,reply_to}` | `reliar-source`, `reliar-destination`, `reliar-reply-to` (NATS `reply` for request/reply later) |
| `headers` (custom) | passed through verbatim — validation already guarantees no `reliar-` prefix |
| `body` (`Bytes`) | the NATS payload, raw — **no** outer JSON envelope |

```rust
pub struct NatsEnvelopeMapper;
impl EnvelopeMapper<async_nats::Message> for NatsEnvelopeMapper { type Error = NatsMapError;
    fn encode(&self, e: &SerializedEnvelope) -> Result<async_nats::Message, NatsMapError> { … }
    fn decode(&self, m: async_nats::Message) -> Result<SerializedEnvelope, NatsMapError> { … } }
```

`decode` of a message missing `reliar-message-id`/`reliar-message-type` is a **permanent** error
(malformed), never a panic. Round-trip property test: `decode(encode(e)) == e`.

## Subject strategy — transport option, not core metadata (SRS §12)

```rust
pub trait SubjectResolver: Send + Sync { fn subject(&self, e: &SerializedEnvelope) -> String; }
/// Default: `<prefix>.<message_type>` → `reliar.orders.created.v1`
pub struct PrefixSubjects { pub prefix: String }
```

`RoutingMetadata.destination` may be used by a resolver but the NATS-specific shape lives here.

## Publisher

```rust
pub struct NatsPublisher<R = PrefixSubjects> { js: async_nats::jetstream::Context, mapper: NatsEnvelopeMapper, subjects: R }
impl<R: SubjectResolver> Publisher for NatsPublisher<R> { type Error = NatsPublishError;
    fn publish(&self, e: &SerializedEnvelope) -> impl Future<Output = Result<(), NatsPublishError>> + Send {
        async move { let msg = self.mapper.encode(e)?; let ack = self.js.publish_with_headers(self.subjects.subject(e), msg.headers, msg.payload).await?;
                     ack.await?; Ok(()) } } }
impl Classify for NatsPublishError { fn kind(&self) -> FailureKind { match self {
    Self::Timeout | Self::NoResponders | Self::Connection(_) => FailureKind::Transient,
    Self::InvalidSubject | Self::PayloadTooLarge | Self::Map(_) => FailureKind::Permanent, … } } }
```

Await the JetStream **ack** — a Core-NATS fire-and-forget publish would silently break at-least-once.
Stream creation/config is the application's or a documented helper's job, not a side effect of publishing.

## Tests (crate `tests/`, JetStream required)

`deploy/compose/docker-compose.yaml` adds `nats:2-alpine -js`; tests read `NATS_URL` (skip with a clear
message when unset, or start a container via testcontainers' generic image). Scenarios:
`mapper_roundtrip.rs` (proptest), `publisher_publishes_with_headers.rs` (subscribe, assert headers +
raw body), `publisher_dedup_by_msg_id.rs` (publish twice ⇒ one stored message),
`outbox_to_nats_end_to_end.rs` (Postgres outbox → dispatcher → NATS; uses `reliar-store-postgres`
test-support). Classification tests use a stopped server (transient) and an oversized payload (permanent).

## Definition of done (Phase 2)

- [ ] Mapper writes every canonical field once; custom headers pass through; body stays raw.
- [ ] `Nats-Msg-Id` = message id; publisher awaits the JetStream ack.
- [ ] Subject strategy lives in this crate; `reliar-core` untouched (SRS §43.20–21).
- [ ] Errors classified transient/permanent; malformed inbound is permanent, never a panic.
- [ ] End-to-end Outbox→NATS test passes against a real JetStream.
