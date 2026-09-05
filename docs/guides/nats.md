# NATS JetStream transport guide

`reliar-transport-nats` is Reliar's first real transport (Phase 2): `NatsEnvelopeMapper` projects
the canonical `Envelope` onto NATS headers plus a raw payload and back, `SubjectResolver` chooses
the subject, and `NatsPublisher` publishes through `JetStream`, awaiting the server's ack before
returning `Ok`. See `docs/architecture/phase2-contract.md` for the frozen signatures this guide
describes, and `crates/reliar-transport-nats/README.md` for the crate's own quickstart.

## Standalone use — `reliar-core` + `reliar-transport-nats` only

The transport is a complete, self-contained publisher. It depends on `reliar-core` and nothing else
in Reliar — not `reliar-outbox`, not `reliar-store-postgres` — in any dependency kind, and CI's
purity job fails the build if that ever changes (ADR 0032, SRS §18, §43.C C13). You can serialize
an `Envelope` and publish it to JetStream with two crates:

```toml
[dependencies]
reliar-core = { version = "0.2", features = ["json"] }
reliar-transport-nats = "0.1"
async-nats = "0.50"
```

```rust,no_run
use reliar_core::{Envelope, JsonSerializer, Message, Publisher, Serializer};
use reliar_transport_nats::{NatsPublisher, NatsSettings};

#[derive(serde::Serialize, serde::Deserialize)]
struct OrderCreated { order_id: u64 }
impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let client = async_nats::connect(&std::env::var("NATS_URL")?).await?;   // the app owns the connection
let js = async_nats::jetstream::new(client);                              // and the stream (ADR 0029)
let publisher = NatsPublisher::new(js, NatsSettings::default().subject_prefix("app"))?;

let envelope = Envelope::builder(OrderCreated { order_id: 42 }).build();
let bytes = JsonSerializer.serialize(&envelope.body)?;                    // Envelope<T>'s body → Bytes
let mut serialized = envelope.map_body(|_| bytes);                        // Envelope<T> → SerializedEnvelope
serialized.metadata.delivery.content_type = JsonSerializer.content_type().clone();
publisher.publish(&serialized).await?;                                    // returns after the JetStream ack
# Ok(()) }
```

What you get without the outbox: the canonical header projection (`reliar-*`, W3C trace headers,
custom headers verbatim, raw body), `Nats-Msg-Id` for JetStream duplicate suppression, an awaited
ack, and per-variant transient/permanent classification on the error. What you do **not** get is
the transactional guarantee: a direct `publish` happens when you call it, whether or not your
database transaction commits. Add `reliar-outbox` + a store only when you need that (below).

There is a middle ground between the two: `OutboxPublisher` (`reliar-outbox`) takes the same
`NatsPublisher` and a store, and offers **both** operations from one type — `enqueue` for the
durability of the full outbox, `publish` for the low-latency directness above — the call site
picks which one it needs, per SRS §20.2. See `docs/guides/outbox-enqueue-and-publish.md`.

The same is true on the receive side: `NatsEnvelopeMapper::decode` turns a NATS message back into a
`SerializedEnvelope` with only `reliar-core` in scope — the Phase 3 consumer builds on exactly that.

## Stream ownership — always the application's or the operator's

**`NatsPublisher` never connects, never creates a stream, and never inspects one.** It takes an
already-built `async_nats::jetstream::Context` (ADR 0029), exactly as `PostgresOutboxStore` takes
an already-built `PgPool` and runs no migration implicitly (ADR 0018). Wiring one is four lines:

```rust,ignore
let client = async_nats::connect(&std::env::var("NATS_URL")?).await?;   // your own env var
let jetstream = async_nats::jetstream::new(client);
// You (or your operator) create the stream — Reliar never does (ADR 0029):
jetstream
    .get_or_create_stream(async_nats::jetstream::stream::Config {
        name: "ORDERS".to_string(),
        subjects: vec!["app.orders.>".to_string()],
        ..Default::default()
    })
    .await?;

let publisher = NatsPublisher::new(jetstream, NatsSettings::default().subject_prefix("app.orders"))?;
let dispatcher = OutboxDispatcher::builder(store, publisher).build()?;
dispatcher.run(cancel).await?;
```

A stream's `retention`, `max_age`, `max_bytes`, `storage`, `replicas`, `discard` and
`duplicate_window` are exactly as consequential as a schema, so Reliar treats them the same way it
treats migrations: never a silent default chosen on your behalf. If no stream captures the subject
a message resolves to, `NatsPublisher` returns a **transient** `NatsPublishError::StreamNotFound` —
the row backs off and stays inspectable rather than the publisher inventing a stream with defaults
you never chose. `examples/nats-pub-sub` creates its own stream explicitly, exactly like the
snippet above, so it is copy-pasteable.

## The stream's `duplicate_window` vs. retry backoff

JetStream suppresses a duplicate `Nats-Msg-Id` (Reliar's `deduplication_id`, or the message id when
none was set — ADR 0026 §5) republished inside the stream's configured `duplicate_window`. Outside
that window, the duplicate is stored as a second message. This **narrows** the outbox's own
at-least-once duplicate window (SRS §22); it does not close it, and `NatsPublisher` makes no
exactly-once claim.

The two windows are independent, and JetStream's is small if you leave it at the server default
(two minutes). If your `RetryPolicy`'s backoff regularly exceeds `duplicate_window` — a slow
consumer, a long lease, a permanent-then-recovered outage — the crash/slow-batch duplicate window
(`docs/architecture/overview.md`) reaches the wire as two distinct stored messages instead of one
suppressed republish. Both are already-idempotent-consumer territory (Reliar promises at-least-once,
never exactly-once), so this changes only *how often* a duplicate is visible, never *whether* one
can occur. Widen `duplicate_window` on the stream if you want JetStream's suppression to cover more
of your retry backoff; Reliar has no diagnostic for this today (ADR 0029's consequences).

## Subject strategy

`SubjectResolver` is a transport-side concern, kept out of `reliar-core` (ADR 0027) — resolution is
pure and synchronous, so a resolver that cannot route an envelope produces a **permanent**
`NatsPublishError::Subject` (the row dead-letters, it never retries against a subject it can never
resolve differently).

- **`PrefixSubjects`** (the default) resolves `<prefix>.<message_type>` —
  `"reliar.orders.created.v1"` for the default prefix, using `MessageType`'s `Display`. It ignores
  `RoutingMetadata.destination` entirely.
- **`DestinationSubjects`** uses `RoutingMetadata.destination` verbatim when the envelope set one,
  and falls back to a wrapped `PrefixSubjects` otherwise — for applications that already decide
  routing per message:

  ```rust,ignore
  let resolver = DestinationSubjects::new(PrefixSubjects::new("app")?);
  let publisher = NatsPublisher::with_resolver(jetstream, NatsSettings::default(), resolver)?;
  ```

Both resolvers validate the **resolved** subject (never just the configured prefix): empty, an
empty token (`a..b`, `.a`, `a.`), a `*`/`>` wildcard, whitespace or a control character, anything
outside printable ASCII, or over 255 bytes are all rejected as `SubjectError`, permanently.

## `NatsSettings`

`Default` + builder methods + an opt-in `from_env` (ADR 0019) — nothing here reads the environment
until `from_env` is called, and there is deliberately **no server URL or credentials field**: the
application builds the connection and keeps ownership of both (ADR 0029 — see "Stream ownership"
above). TLS, nkeys/credentials, reconnect policy and websockets are all features of *your*
`async-nats`, unified by Cargo with Reliar's (`async-nats = { features = ["jetstream"] }` only).

| Field | Env var | Default | Notes |
|---|---|---|---|
| `subject_prefix` | `RELIAR_NATS_SUBJECT_PREFIX` | `"reliar"` | Ignored when a resolver is supplied through `NatsPublisher::with_resolver`. |
| `publish_timeout` | `RELIAR_NATS_PUBLISH_TIMEOUT_MS` | 10 s | An **upper bound**, not a guarantee: the effective ack deadline is `min(publish_timeout, Context::timeout)`, and async-nats' own `Context::timeout` defaults to 5 s — so with default settings on both sides the *host's* deadline fires first and raising this setting alone has no effect. A host that wants the full window builds its context with `async_nats::jetstream::ContextBuilder::timeout` (RELIAR-38). Exceeded ⇒ transient `Timeout`, reporting the measured elapsed time either way. Bounds one publish's send **and** ack together; in `publish_batch` it bounds one window. |
| `batch_pipeline_depth` | `RELIAR_NATS_BATCH_PIPELINE_DEPTH` | 64 | How many publishes `publish_batch` pipelines before awaiting their acks. Must stay **at or below** the host `Context`'s `max_ack_inflight` (async-nats default 5000). Above that cap, `backpressure_on_inflight` decides the failure mode: **`true`** — async-nats' default — makes each excess send **wait** for a permit, so the whole window stalls until `publish_timeout` and then fails `Timeout`; `false` fails each excess send immediately with a transient `MaxAckPending` instead. Reliar cannot validate the cap itself (`Context` exposes no getter for it), so it is documented as a ceiling to stay under, not a knob to raise. |
| `max_payload` | `RELIAR_NATS_MAX_PAYLOAD_BYTES` | `None` | async-nats already rejects an oversized payload locally, before any I/O, against the `max_payload` the server advertised at connect — so this field is not "a round trip saved", it is a **lower** ceiling than the server's own, with an error type Reliar owns (`PayloadTooLarge`). Unset, Reliar relies on the server's own rejection (`MaxPayloadExceeded`, also permanent). `Some(0)` is rejected at construction; RELIAR-37 tracks deriving the real server limit automatically. |

`batch_pipeline_depth` (and therefore `publish_batch` itself) is **reached only through
[`Publisher::publish_batch`]** — v0.1's `OutboxDispatcher` calls `publish` once per claimed row, so
on the only wiring Reliar ships today this setting is inert for the dispatcher; it takes effect for
a third-party caller that batches directly, and for SRS §36's messaging layer once it lands
(RELIAR-39 tracks giving the dispatcher a batch path).

`from_env("RELIAR_NATS_")` overrides only the variables present, never a silent fallback for a
present-but-unparseable one — same contract as `PostgresOutboxSettings::from_env` and
`OutboxSettings::from_env`. Conventional prefix: `RELIAR_NATS_`.

## Which legal envelopes NATS cannot carry

`reliar-core` validates less than a NATS header name/value allows (tracked as **RELIAR-35**): some
envelopes that are perfectly legal to build and store are **permanently** unrepresentable on this
transport, and `encode` rejects them rather than silently mangling them (ADR 0026 §3):

- **A custom header key containing a space, a colon, or a non-ASCII byte** —
  `async_nats::HeaderName` only accepts ASCII-graphic, colon-free names. Rejected as
  `NatsMapError::UnsupportedHeaderName`.
- **A custom header key starting with `Nats-` (case-insensitive)** — NATS's own reserved
  namespace. Rejected as `NatsMapError::ReservedHeaderName`.
- **A `\r` or `\n` inside any unvalidated core `String`** — `MessageType::name()`, `tenant_id`,
  `traceparent`, `tracestate`, `deduplication_id`, or a custom header value all reach the wire
  verbatim today, and any of them can carry a raw newline. Rejected as
  `NatsMapError::InvalidHeaderValue` (naming the header, never the value — a value is data, a
  key is caller-chosen configuration, ADR 0026 Amendment B).

None of these ever panics: every rejection is a permanent `NatsMapError`, so the affected row
dead-letters with an actionable reason instead of corrupting a header or crashing the publisher.
RELIAR-35 tracks whether `reliar-core` should validate these fields to a transport-portable subset
up front (breaking, pre-1.0) or leave the rejection to each transport, as today.

## `NATS_URL` for tests and examples

Reliar never reads `NATS_URL` (or any environment variable) implicitly — only `NatsSettings::from_env`
does, and only when called. `NATS_URL` is a convention this workspace's own tests and examples
follow, mirroring `DATABASE_URL`:

- **`examples/nats-pub-sub`** reads both `DATABASE_URL` and `NATS_URL` from the environment,
  exactly like `examples/axum-outbox` reads `DATABASE_URL` — never hard-coded, never read by any
  library crate.
- **`reliar-transport-nats`'s own `nats`-suffixed tests** and **`tests/system`'s `e2e` tests** use
  `NATS_URL` when it is set (CI's `docker run -js` step, ADR 0031 §3) and otherwise start one
  ephemeral `nats:2.14-alpine -js -m 8222` container per test binary via testcontainers, torn down
  before the binary exits (ADR 0031 §4/§6, mirroring the Postgres harness's `DATABASE_URL` rule).
- **Locally**, `deploy/compose/docker-compose.yaml`'s `nats` service brings up the same image for
  manual exploration: `docker compose -f deploy/compose/docker-compose.yaml up -d --wait nats`,
  then `NATS_URL=nats://127.0.0.1:4222`.
- **`/jsz?config=1`, not `/healthz`, is the only healthcheck worth writing.** Verified against
  `nats:2.14-alpine`: `/healthz` (and its `js-enabled-only`/`js-server-only` variants) answers
  `{"status":"ok"}` even when the server was started **without** `-js` — `JetStream` disabled.
  `/jsz?config=1` reports `"disabled": true` and omits `store_dir` in that case, so grepping for
  `store_dir` is the probe that actually fails when `JetStream` isn't serving (ADR 0031 — see that
  ADR for why CI starts NATS as a step rather than a `services:` container: GitHub Actions service
  containers cannot override the image's `CMD`, and the official image starts without `-js` by
  default).

## `tests/system`

The Postgres-outbox-to-`JetStream` end-to-end proof lives in its own workspace package,
`tests/system` (ADR 0031 §6), never inside either provider crate: putting it in
`reliar-store-postgres/tests/` or `reliar-transport-nats/tests/` would make one provider
dev-depend on the other, which the crate dependency rule forbids in both directions. It starts one
Postgres and one NATS container (or uses `DATABASE_URL`/`NATS_URL` when set) and covers four
scenarios:

- **E1** — every enqueued row ends up `published_at` **and** present in the stream with a
  matching `reliar-message-id` header and identical raw body, plus a clean drain on cancellation
  (every lease released, nothing dead-lettered) and a separate trial proving that rows past a
  deliberately capped batch are provably never claimed.
- **E2** — a stream deleted while the dispatcher is running leaves its row retryable (`attempts`
  incremented, transient `StreamNotFound`, never dead) until a stream recapturing the same subject
  exists again.
- **E3** — a worker that publishes but crashes before writing its own `complete` has the row's
  lease reclaimed and republished; `Nats-Msg-Id` inside the stream's `duplicate_window` keeps the
  stream at one copy despite two publishes reaching the wire — the duplicate-window narrowing
  above, proven end to end.
- **E4** — an envelope permanently unrepresentable on this transport dead-letters on its first
  attempt, is never retried, and its `last_error` never carries the offending header's value.
