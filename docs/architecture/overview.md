# Reliar — architecture overview

Reliar is a **library**, not a service. A host application composes it explicitly at startup; there
is no DI container, no ambient configuration, and nothing starts a thread on its own.

- **Baseline:** `../srs.md` v1.1 (approved 2026-09-04).
- **Decisions:** `../decisions/README.md`.
- **Frozen Phase-1 API:** `phase1-contract.md`.
- **Frozen Phase-2 API** (the NATS transport): `phase2-contract.md`. It adds one crate,
  `reliar-transport-nats` (→ `reliar-core`, plus `async-nats`).
- **Frozen routing-publisher API** (v0.2): `routing-publisher-contract.md`, decided by ADR 0033
  (incl. Amendment D). It adds no crate — `reliar-outbox` gains the `OutboxStaging<Tx>` capability
  and `OutboxPublisher`/`ScopedOutboxPublisher` (the latter **is** a `reliar_core::Publisher`),
  `reliar-store-postgres` gains one impl.
- **Amended 2026-09-05 by ADR 0032:** `Publisher`, `Classify`, `FailureKind` and `SettingsError`
  moved from `reliar-outbox` to `reliar-core`, signatures unchanged. Publication is a shared
  capability, not an outbox concept — so a transport (and, in Phase 3, `reliar-messaging`) depends
  on core alone. The map and rules below reflect the move.

## Phase 2 status

`reliar-transport-nats` (S1–S3, RELIAR-32/33/34) is complete: `NatsEnvelopeMapper` + header
projection (§2), `SubjectResolver`/`PrefixSubjects`/`DestinationSubjects` (§3), `NatsPublisher`
(§4) publishing through `JetStream` with an awaited ack (ADR 0028) and never owning a connection or
a stream (ADR 0029). The phase's proof is `tests/system`'s `e2e` scenarios — a migrated Postgres
outbox drained by `OutboxDispatcher<PostgresOutboxStore, NatsPublisher<PrefixSubjects>>` into a real
`JetStream` stream — plus `examples/nats-pub-sub` and `docs/guides/nats.md`. See
`phase2-contract.md` §6 for the slice breakdown.

## Crate map (Phase 1 + Phase 2)

```text
              ┌───────────────────────────────────────────────┐
              │ reliar-core                                   │  pure
              │   Envelope<T> / SerializedEnvelope            │  serde · bytes · uuid · time
              │   Message · MessageType · ContentType         │  (+ serde_json behind `json`)
              │   Metadata · Headers · ids                    │
              │   Serializer · EnvelopeMapper                 │
              │   Publisher · Classify · FailureKind          │  (ADR 0032)
              │   SettingsError                               │
              └───────────────────────────────────────────────┘
                        ▲                        ▲
                        │                        │
  ┌─────────────────────┴─────────┐    ┌─────────┴──────────────────────┐
  │ reliar-outbox                 │    │ reliar-transport-nats          │  async-nats (jetstream feature only)
  │   OutboxStore                 │    │   NatsEnvelopeMapper           │
  │   OutboxDeadLetters           │    │   SubjectResolver              │
  │   RetryPolicy · Ordering      │    │   NatsPublisher (JetStream)    │
  │   OutboxDispatcher<S,P,M,R>   │    └──────────────────────────────┬─┘
  │   Policy · OutboxPublisher    │                                   │
  │   OutboxMetrics · settings    │                                   │
  │   test-support fakes          │                                   │
  └───────────────────────────────┘                                   │
                        ▲                                             │
  ┌─────────────────────┴─────────┐ sqlx · Postgres 18+               │
  │ reliar-store-postgres         │ migrations/ · migrate() · .sqlx/  │
  │   PostgresOutboxStore         │                                   │
  │   enqueue(&mut tx, envelope)  │                                   │
  └─────────────────────┬─────────┘                                   │
                        └────────────────────┬────────────────────────┘
                              ┌──────────────┴────────────────────────┐
                              │ tests/system (publish=false)          │  the only place both providers meet
                              │   e2e: outbox → NATS proof            │  (dev-dependencies only, ADR 0031 §6)
                              └───────────────────────────────────────┘
```

## The dependency rule (inward only)

- **`reliar-core` is pure.** No sqlx, no postgres, no broker client, and **no transport routing
  concepts** — a Kafka partition key, a Rabbit exchange or a NATS subject option belongs to a
  transport crate, never to `Metadata`. Enforced in CI by `cargo tree -p reliar-core -e normal`.
- **Abstraction crates depend only on core.** `reliar-outbox` knows nothing about Postgres or any
  broker; adding a transport in Phase 2 introduced no NATS symbol under `reliar-outbox/src/`.
- **A provider depends on core directly, and on an abstraction crate only when it implements a
  trait that crate owns** (ADR 0032). `reliar-store-postgres` implements `OutboxStore`, so it
  depends on `reliar-outbox`. `reliar-transport-nats` implements only core traits (`Publisher`,
  `EnvelopeMapper`), so it depends on `reliar-core` alone — no `reliar-outbox` edge in any
  dependency kind.
- **Providers never depend on each other.** `reliar-store-postgres` imports no transport;
  `reliar-transport-nats` imports no store. `tests/system` is the one place both meet, and only as **dev**-dependencies of
  a `publish = false` test package — never a normal dependency edge between the two (ADR 0031 §6).
- **The host is the only place a concrete pair is named:**
  `OutboxDispatcher<PostgresOutboxStore, NatsPublisher>`. Everything is monomorphized; no `Box<dyn>`
  appears on a hot path (ADR 0001).

## Two paths through the system

### Write path — the application's transaction

```text
Axum handler
  │
  ├─ pool.begin()                                  ← the application owns the transaction
  ├─ orders::insert(&mut tx, …)                    ← business row
  ├─ store.enqueue(&mut tx, &envelope)             ← Envelope<T> --serialize--> row (plain INSERT)
  └─ tx.commit()                                   ← both rows commit, or neither does
```

`enqueue` is deliberately **not** on `OutboxStore`: taking `&mut sqlx::Transaction` makes the
atomicity requirement visible at every call site (ADR 0008). It returns the `MessageId` it wrote, so
the caller can chain it as the next message's `causation_id` inside the same transaction.

### Delivery path — the dispatcher loop

```text
┌─▶ acquire(AcquireRequest { worker, batch_size, lease, ordering })
│     one statement:  SELECT … FOR UPDATE SKIP LOCKED  →  UPDATE … RETURNING
│     sets locked_by = worker, locked_until = now() + lease      ← DB time, always
│     COMMITTED before the future resolves                        ← no lock held across I/O
│     undecodable rows are excluded, reported, and already dead   ← one bad row never stops the loop
│
├──▶ empty batch?  → idle backoff (poll_interval → idle_poll_interval), loop
│
├──▶ publish, bounded by max_in_flight, each with publish_timeout
│     ┌─ record moved (not cloned) into a spawned task
│     ├─ P::Error: Classify  →  Transient | Permanent
│     └─ lease renewed via extend_lease after half the lease has elapsed
│
├──▶ RetryPolicy::next(attempts, kind)  →  Retry { delay } | Dead { reason }   ← pure, clock-free
│
├──▶ complete(worker, &[CompletedMessage])   ── AND locked_by = $worker ──┐
│    fail(worker, &[FailedMessage])          ── AND locked_by = $worker ──┤ attempts += 1
│      Retry → available_at = now() + delay  (in SQL)                     │ rows-affected shortfall
│      Dead  → dead_at = now(), dead_reason, last_error (truncated)       │   = lease lost = benign
│                                                                          ┘
└─◀ loop, until the CancellationToken fires
      then: stop claiming → drain in-flight (drain_timeout) → persist outcomes
            → release the remainder → Ok(())
```

**The dispatcher polls `stats()`** every `stats_interval` (default 15 s, `Duration::ZERO` disables)
and feeds the pending / expired-pending / outbox-lag gauges. It reads only, so it is safe on the
worker's own timer.

**Retention is the host's**, because it writes: a periodic task calls `OutboxStore::purge` — one
bounded pass plus the expired-to-dead sweep, repeated while `!report.is_complete(batch_size)`. It is
public on the store the host already owns, and `run(self)` consumes the dispatcher, so there is no
maintenance method on it. Reliar starts no timer other than the dispatcher's own loop.

## The guarantees, stated plainly

- **Durable at-least-once publication.** Never exactly-once (ADR 0007). Three duplicate windows are
  documented and each has a test: a crash between publish and `complete`; a lease that expires while
  a healthy worker is still mid-batch; and a publish still in flight when the drain timeout expires.
- **No ordering by default.** `Ordering::Unordered` guarantees nothing — not globally, not per
  conversation, not per aggregate, not approximately. `Ordering::PerKey` lands in 0.2 and is a
  configuration error before then (ADR 0013).
- **Database time is authoritative** for every correctness comparison: leases, due times,
  `expires_at`, and all persisted state timestamps. The app clock owns only `sent_at`, jitter, idle
  sleeps and metric durations. There is no `Clock` trait (ADR 0009).
- **No lock is held across network I/O.** The claim is one statement, so this is true by
  construction rather than by discipline (ADR 0006).
- **Reliar never mutates a host's schema implicitly.** `migrate()` is explicit, idempotent, and
  isolated in its own schema with its own `_migrations` bookkeeping (ADR 0018).
- **Nothing sensitive is emitted.** No payload bytes or custom header values in any span, log or
  error `Display`, at any level (ADR 0020).

## Where things live

| Concern | Home |
|---|---|
| Envelope, metadata, headers, ids, serialization | `reliar-core` |
| The `Publisher` contract, `Classify`/`FailureKind`, `SettingsError` | `reliar-core` (ADR 0032) |
| `OutboxStore`, retry, dispatcher, outbox settings, metrics hook, fakes | `reliar-outbox` |
| The routing rule (`OutboxPolicy`), the `OutboxPublisher`/`ScopedOutboxPublisher` composition | `reliar-outbox` (ADR 0033, Amendment D) |
| Schema, migrations, SQL, `enqueue`, `search_path` verification | `reliar-store-postgres` |
| Transport headers, subject resolution, `JetStream` publish + ack | `reliar-transport-nats` |
| The cross-provider outbox→NATS end-to-end proof | `tests/system` |
| Exporters, config precedence, pool/connection/stream ownership, maintenance scheduling | **the host application** |
