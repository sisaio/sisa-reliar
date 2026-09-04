# Reliar — architecture overview

Reliar is a **library**, not a service. A host application composes it explicitly at startup; there
is no DI container, no ambient configuration, and nothing starts a thread on its own.

- **Baseline:** `../srs.md` v1.1 (approved 2026-09-04).
- **Decisions:** `../decisions/README.md`.
- **Frozen Phase-1 API:** `phase1-contract.md`.

## Crate map (Phase 1)

```text
              ┌───────────────────────────────────────────────┐
              │ reliar-core                                   │  pure
              │   Envelope<T> / SerializedEnvelope            │  serde · bytes · uuid · time
              │   Message · MessageType · ContentType         │  (+ serde_json behind `json`)
              │   Metadata · Headers · ids                    │
              │   Serializer · EnvelopeMapper                 │
              └───────────────────────────────────────────────┘
                        ▲                        ▲
                        │                        │
  ┌─────────────────────┴─────────┐    ┌─────────┴──────────────────────┐
  │ reliar-outbox                 │    │ (Phase 2) reliar-transport-*   │
  │   OutboxStore · Publisher     │◀───┤   Publisher + EnvelopeMapper   │
  │   OutboxDeadLetters           │    └────────────────────────────────┘
  │   RetryPolicy · Ordering      │
  │   OutboxDispatcher<S,P,M,R>   │
  │   OutboxMetrics · settings    │
  │   test-support fakes          │
  └───────────────────────────────┘
                        ▲
  ┌─────────────────────┴─────────┐
  │ reliar-store-postgres         │  sqlx · Postgres 18+
  │   PostgresOutboxStore         │  migrations/ · migrate() · .sqlx/
  │   enqueue(&mut tx, envelope)  │
  └───────────────────────────────┘
```

## The dependency rule (inward only)

- **`reliar-core` is pure.** No sqlx, no postgres, no broker client, and **no transport routing
  concepts** — a Kafka partition key, a Rabbit exchange or a NATS subject option belongs to a
  transport crate, never to `Metadata`. Enforced in CI by `cargo tree -p reliar-core -e normal`.
- **Abstraction crates depend only on core.** `reliar-outbox` knows nothing about Postgres or any
  broker; adding a transport in Phase 2 touches no file under `reliar-outbox/src/`.
- **Providers never depend on each other.** `reliar-store-postgres` implements `reliar-outbox`'s
  traits and imports no other provider.
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
| Store/publisher traits, retry, dispatcher, settings, metrics hook, fakes | `reliar-outbox` |
| Schema, migrations, SQL, `enqueue`, `search_path` verification | `reliar-store-postgres` |
| Transport headers, subject/exchange/partition resolution | a transport crate (Phase 2) |
| Exporters, config precedence, pool ownership, maintenance scheduling | **the host application** |
