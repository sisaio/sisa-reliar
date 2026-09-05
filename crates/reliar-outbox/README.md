# reliar-outbox

The storage-agnostic transactional outbox: the `OutboxStore`/`OutboxDeadLetters` capability traits
(plus `reliar_core::Publisher`, re-exported here for convenience), the request/result types that
cross their boundary, a pure `RetryPolicy`, the feature's settings (`OutboxSettings`), the
`OutboxMetrics` hook, and the `OutboxDispatcher` worker loop (SRS §19–§26).

Depends only on `reliar-core` — no `sqlx`, no Postgres, no broker client. A provider crate
(`reliar-store-postgres`) implements the traits here; this crate never depends on one.

## Guarantees

**Durable at-least-once publication. Never exactly-once.** Duplicate delivery is expected, and a
consumer built on Reliar must be idempotent. Three windows produce a duplicate, and **all three
are unavoidable in this release**:

1. **The crash window** (§22) — a publish reaches the broker, the worker crashes before `complete`
   persists the outcome, the lease expires, and another worker republishes the same message. A
   crash is not the only way `complete` never lands: `lease` is also the outcome-write retry
   budget (RELIAR-26, M2). A `complete`/`fail` call that keeps failing or timing out is retried on
   every loop iteration, but only for up to `lease` — past that, the row is abandoned to its lease
   (dropped from tracking, no longer renewed) rather than retried forever, so a **perfectly
   healthy** worker with a persistently failing `complete` produces exactly this same duplicate,
   no crash required.
2. **The slow-batch window** (§22.1) — no crash at all. A worker claims a batch under a lease
   shorter than the batch takes to drain; the lease expires while the worker is still healthily
   publishing, a second worker reclaims and republishes the tail, and the first worker's later
   `complete`/`fail` is rejected by the `locked_by` guard. Lease renewal and a per-publish timeout
   make this rare, not impossible — in practice it is the common window.
3. **The drain window** (§26.1) — on cancellation, `run()` drains in-flight publishes for at most
   `DispatcherSettings::drain_timeout`. A publish still unresolved at the timeout is released
   rather than awaited further, and its eventual outcome — success or failure — carries the same
   duplicate risk as the other two, just triggered by shutdown instead of a lease.

**No ordering by default.** `Ordering::Unordered` (the only value this release supports) guarantees
**nothing** about order — not globally, not per `conversation_id`, not per aggregate, not even
approximately. `SKIP LOCKED`, concurrent publishing, per-message backoff and multiple dispatcher
instances each reorder freely (§22.2, ADR 0013). `Ordering::PerKey` is a configuration error before
0.2.

**Pure retry.** `RetryPolicy` is I/O-free and clock-free — it returns a `Duration`, never a
timestamp. The store applies it as `available_at = now() + delay` in SQL, so a worker's clock skew
can never hot-loop a row or park it in the future (ADR 0009).

**The library never reads the environment implicitly.** Only `OutboxSettings::from_env` touches
`std::env`, and only when called (ADR 0019).

See `../../docs/architecture/outbox.md` for the full delivery-path walkthrough and
`../../docs/architecture/phase1-contract.md` §3 for the frozen signatures.

## Quickstart: `enqueue` vs `publish`

`OutboxPublisher` is the application's handle to both guarantees. Which one a call site gets is
chosen by which method it calls — nothing decides it at runtime (ADR 0036):

```rust,no_run
# use reliar_core::{ContentType, Envelope, Message, Publisher as _, Serializer};
# use reliar_outbox::{InMemoryOutboxStore, InMemoryTransaction, OutboxPublisher, RecordingPublisher};
# #[derive(serde::Serialize, serde::Deserialize)]
# struct OrderCreated;
# impl Message for OrderCreated {
#     const TYPE: &'static str = "orders.created";
#     const VERSION: u16 = 1;
# }
# struct RawJson;
# impl Serializer for RawJson {
#     type Error = serde_json::Error;
#     fn content_type(&self) -> &ContentType { &ContentType::JSON }
#     fn serialize<T: Message>(&self, body: &T) -> Result<bytes::Bytes, Self::Error> {
#         serde_json::to_vec(body).map(bytes::Bytes::from)
#     }
#     fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
#         serde_json::from_slice(bytes)
#     }
# }
# #[tokio::main(flavor = "current_thread")]
# async fn main() -> Result<(), Box<dyn std::error::Error>> {
let envelope = Envelope::builder(OrderCreated).build();
let bytes = RawJson.serialize(&envelope.body)?;
let mut serialized = envelope.map_body(|_| bytes);
serialized.metadata.delivery.content_type = RawJson.content_type().clone();

let outbox = OutboxPublisher::new(InMemoryOutboxStore::default(), RecordingPublisher::default());

// enqueue: durable, at-least-once — visible only once the caller's own transaction commits, and
// published later by an OutboxDispatcher.
let mut tx = InMemoryTransaction;
outbox.enqueue(&mut tx, &serialized).await?;

// publish: bypasses the outbox entirely — sends now, one attempt, no Reliar guarantee at all.
outbox.publish(&serialized).await?;
# Ok(())
# }
```

See `../../docs/guides/outbox-enqueue-and-publish.md` for the full guarantee comparison and the
open-transaction warning around `publish`. An app that only ever needs the bypass path — no
durability, no dispatcher — can skip this crate entirely and use a transport publisher (e.g.
`reliar-transport-nats`'s `NatsPublisher`) directly against `reliar-core` alone.

## What this crate ships

- The `OutboxStore`/`OutboxDeadLetters` traits and their request/result types (`AcquireRequest`,
  `AcquiredBatch`, `CompletedMessage`, `FailedMessage`, `FailureOutcome`, `DeadReason`,
  `MessageRef`, `PurgeRequest`/`PurgeReport`, `OutboxStats`, `DeadQuery`/`DeadLetterPage`,
  `PoisonedRow`), plus `OutboxRecord` and its builder.
- `reliar_core::Publisher` (re-exported here) + `Classify`/`FailureKind` — a publisher's error
  carries its own transient/permanent verdict; the dispatcher never guesses.
- `RetryPolicy` and the default `ExponentialBackoff`.
- `OutboxDispatcher`/`OutboxDispatcherBuilder`/`DispatchError` — bounded-concurrency claim →
  publish → batch `complete`/`fail`, half-lease `extend_lease` renewal, a `stats_interval` tick
  feeding `OutboxMetrics`, `tracing` spans (`reliar.outbox.claim`/`publish`/`retry`/`dead`), and
  graceful cancellation via `tokio_util::sync::CancellationToken`.
- `OutboxSettings`/`DispatcherSettings`/`RetentionSettings`, each `Default` + builder, with an
  opt-in `from_env("RELIAR_OUTBOX_")`.
- `OutboxMetrics`/`NoopMetrics` — a static-dispatch hook, no exporter dependency.
- Behind the `test-support` feature: `InMemoryOutboxStore`, `RecordingPublisher`,
  `ScriptedPublisher`, `RecordingMetrics` — reused by provider crates and `examples/`.

## Settings and environment variables

`OutboxSettings::from_env("RELIAR_OUTBOX_")` is opt-in — nothing in this crate reads the
environment implicitly (ADR 0019). It starts from `Default`, overrides only the variables
present, and returns `Err` for a present-but-unparseable or out-of-range value, never a silent
fallback.

`DispatcherSettings` (worker-loop tunables):

| Field | Env var | Default | Meaning |
|---|---|---|---|
| `batch_size` | `RELIAR_OUTBOX_BATCH_SIZE` | `100` | Max rows one `acquire` statement claims. |
| `lease` | `RELIAR_OUTBOX_LEASE_MS` | `30000` (30 s) | How long a claim holds its lease before it may be reclaimed. |
| `max_in_flight` | `RELIAR_OUTBOX_MAX_IN_FLIGHT` | `16` | The claim gate's ceiling on rows this worker actively holds leased — not an absolute one: a row M2 abandons after `lease` stays leased (just no longer counted or renewed by this worker) until that lease elapses. |
| `publish_timeout` | `RELIAR_OUTBOX_PUBLISH_TIMEOUT_MS` | `10000` (10 s) | How long one publish may run before it counts as a timeout (`FailureKind::Transient`). |
| `poll_interval` | `RELIAR_OUTBOX_POLL_INTERVAL_MS` | `500` | Poll cadence after a non-empty claim; also seeds the outcome-write retry's pacing (capped at `lease / 4`) so a persistently *fast*-failing `complete`/`fail` cannot retry at CPU speed. **Must be > 0** (`ConfigError::ZeroPollInterval`). |
| `idle_poll_interval` | `RELIAR_OUTBOX_IDLE_POLL_INTERVAL_MS` | `5000` (5 s) | Poll cadence once a claim comes back empty. **Must be > 0** (`ConfigError::ZeroPollInterval`). |
| `drain_timeout` | `RELIAR_OUTBOX_DRAIN_TIMEOUT_MS` | `30000` (30 s) | Max time `run()` spends draining in-flight publishes after cancellation (§26.1). |
| `store_timeout` | `RELIAR_OUTBOX_STORE_TIMEOUT_MS` | `10000` (10 s) | Client-side bound on **every** `OutboxStore` call `run` makes — without it a hung statement makes `drain_timeout` unenforceable. Must be shorter than half the lease (`store_timeout < lease / 2`, `ConfigError::StoreTimeoutTooLong`) — the outcome-write retry races the lease-renewal tick, so a longer bound risks one hung `complete`/`fail` starving renewal for a whole tick. |
| `stats_interval` | `RELIAR_OUTBOX_STATS_INTERVAL_MS` | `15000` (15 s) | How often `stats()` is polled for the pending/expired-pending/lag gauges. `0` disables the tick. |
| `ordering` | `RELIAR_OUTBOX_ORDERING` | `Unordered` | The publication ordering strategy — see the no-ordering guarantee above. |
| `retry.base` | `RELIAR_OUTBOX_RETRY_BASE_MS` | `1000` (1 s) | `ExponentialBackoff`'s base delay. |
| `retry.max_delay` | `RELIAR_OUTBOX_RETRY_MAX_DELAY_MS` | `300000` (5 min) | `ExponentialBackoff`'s delay cap. |
| `retry.max_attempts` | `RELIAR_OUTBOX_RETRY_MAX_ATTEMPTS` | `10` | Attempts before a row goes dead with `DeadReason::AttemptsExhausted`. |
| `retry.jitter` | `RELIAR_OUTBOX_RETRY_JITTER` | `0.2` | Delay multiplier spread, `U(1 − jitter, 1 + jitter)`; must be in `[0.0, 1.0)`. |
| `worker_id` | `RELIAR_OUTBOX_WORKER_ID` | generated (`pid:uuid7`) | Overrides the generated `WorkerId` — e.g. to embed a pod name. |

`RetentionSettings` (purge tunables):

| Field | Env var | Default | Meaning |
|---|---|---|---|
| `published_retention` | `RELIAR_OUTBOX_PUBLISHED_RETENTION_MS` | `604800000` (7 days) | How long a published row is kept before `purge` deletes it. |
| `dead_retention` | `RELIAR_OUTBOX_DEAD_RETENTION_MS` | unset (`None`) | How long a dead row is kept before `purge` deletes it. **`None` means dead rows are kept until an explicit purge — a default `purge` call deletes zero dead rows** until this is set. |
| `purge_batch_size` | `RELIAR_OUTBOX_PURGE_BATCH_SIZE` | `1000` | Max rows one `purge` pass deletes, per statement, per pass. |

The provider-specific `RELIAR_STORE_POSTGRES_*` variables (`SCHEMA`,
`ENQUEUE_SETS_SEARCH_PATH`, `STATEMENT_TIMEOUT_MS`) are documented in
`../../docs/guides/postgres.md`.

## Features

| Feature | Default | Enables |
|---|---|---|
| `test-support` | no | The in-memory fakes above. |
| `serde` | no | `Serialize`/`Deserialize` on the settings types, `#[serde(default, deny_unknown_fields)]`, durations as integer milliseconds. |
| `metrics` | no | A `metrics`-facade adapter for `OutboxMetrics` (empty until an adapter ships). |

## Testing

`cargo test -p reliar-outbox --all-features`. Every test lives in `tests/` against the public API:
in-memory fakes drive dispatcher behaviour (retry, dead, cancellation, concurrency bounds) under
`#[tokio::test(start_paused = true)]`, with no wall-clock sleeps. `benches/outbox_throughput.rs`
(criterion, `cargo bench -p reliar-outbox --features test-support`) covers the dispatcher's own
claim → publish → complete overhead.
