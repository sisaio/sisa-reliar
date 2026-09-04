# ADR 0006 — The claim is one statement; publication happens outside any transaction

**Status:** Accepted — 2026-09-04
**SRS:** §21, §21.1, §24.1, §25, §26, §43.A.6–7

## Context

The naive outbox worker opens a transaction, selects due rows `FOR UPDATE`, publishes each one to
the broker, marks them published, and commits. It is correct on a whiteboard and catastrophic in
production: the row locks — and an idle-in-transaction connection — are held for the duration of
network I/O to a broker that may be slow, rate-limiting, or hung. Concurrency collapses to one
worker, vacuum stalls behind the long-lived snapshot, and a hung publish holds locks indefinitely.

The alternative is a lease: claim rows in a short transaction, publish outside it, then record
outcomes in a second short transaction. That trades a lock for a timeout, and the timeout is what
makes the duplicate window real (ADR 0007).

## Decision

- The claim is a **single SQL statement** — a CTE `SELECT … FOR UPDATE SKIP LOCKED` feeding an
  `UPDATE … RETURNING` (§24.1). One statement is one implicit transaction, so the row lock is
  released before `acquire` returns. §21's rule is satisfied **by construction, not by
  discipline** — there is no code path that could hold it open.
- `OutboxStore::acquire` SHALL have committed before its future resolves. Network I/O to a broker
  SHALL NEVER occur while a Reliar transaction is open. A `sqlx::Transaction` SHALL NEVER be held
  across a publish `.await` — it is also a cancellation trap, since a `select!` cancel branch
  would drop and roll it back mid-operation.
- Ownership is a **lease**: `locked_by = $worker`, `locked_until = now() + lease`, both computed
  in DB time (§25, ADR 0009's clock split). Another worker may reclaim an expired lease.
- Outcomes are written in separate short statements, batched (`= ANY` / `UNNEST`) and guarded by
  `locked_by` (ADR 0008).
- Because a lease can expire mid-batch while the worker is healthy, the dispatcher renews it via
  `extend_lease` after **half** the lease has elapsed, and every publish carries a
  `publish_timeout` (default 10 s) classified as transient (§21.1). `build()` warns at startup
  when `lease > batch_size × publish_timeout ÷ max_in_flight` does not hold.
- `enqueue` is the one place a Reliar statement joins someone else's transaction — the
  application's own (§20, ADR 0008) — and it performs no I/O beyond that INSERT.

## Consequences

- Locks are held for microseconds; many workers claim disjoint sets concurrently via
  `SKIP LOCKED` (§43.A.6), and a hung broker cannot block the database.
- A claimed row is invisible to other workers only until `locked_until` passes. Crash recovery is
  therefore free: nothing needs to detect a dead worker, the lease simply expires.
- **This is what creates the duplicate window** — the claim and the publish are no longer atomic
  (ADR 0007). That is the accepted price and it is documented, not hidden.
- A lease that is too short for the batch guarantees duplicates rather than merely risking them
  (§22.1), so lease renewal and the publish timeout are mandatory, not optional tuning.
- Every state-changing statement must tolerate being a no-op (the lease was lost). Row counts are
  returned and a shortfall is logged, never an error (ADR 0008).

## Alternatives considered

- **`SELECT … FOR UPDATE` + publish + `COMMIT`.** Rejected: exactly the forbidden pattern; serial
  throughput, idle-in-transaction connections, vacuum stalls, and no crash recovery story.
- **Advisory locks per message.** Rejected: session-scoped, so they break behind a transaction-mode
  pooler and leak on crash; `SKIP LOCKED` + a durable lease column is strictly better.
- **`LISTEN/NOTIFY` as the source of truth.** Rejected: notifications are not durable. It ships as
  an optional wake-up optimization only; the table and polling remain authoritative (§26).
