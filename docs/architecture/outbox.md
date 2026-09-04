# The transactional outbox

Owner: `reliar-outbox` (contract) + `reliar-store-postgres` (implementation). Frozen signatures:
`phase1-contract.md` §3–§4. Full crate map and both request/delivery paths in one diagram:
`overview.md`.

## Two paths, two owners

**Write path — the application's transaction.** `enqueue` is deliberately **not** on the
`OutboxStore` trait: it takes `&mut sqlx::Transaction` directly, so atomicity is visible in the
call signature (ADR 0008). A business row and an outbox row commit together, or neither does.

**Delivery path — the dispatcher's loop.** `OutboxDispatcher::run` claims due rows with a single
`FOR UPDATE SKIP LOCKED` statement that **commits before the future resolves** — no lock is ever
held across the publish (ADR 0006) — then publishes them with bounded concurrency
(`max_in_flight`), applies the configured `RetryPolicy` to each outcome, and persists
`complete`/`fail` in batches guarded by `locked_by = $worker`.

## Guarantees, stated plainly

- **Durable at-least-once. Never exactly-once** (ADR 0007). A consumer must be idempotent.
- **No ordering by default.** `Ordering::Unordered` (the only supported value in v0.1) guarantees
  **nothing** — not globally, not per `conversation_id`, not per aggregate, not approximately
  (ADR 0013). `SKIP LOCKED`, concurrent publishing and per-message backoff each reorder freely.
- **Database time is authoritative** for every correctness comparison — leases, due times,
  `expires_at`. There is no `Clock` trait (ADR 0009); a worker's own clock only drives its poll
  cadence and jitter.
- **Retry is pure.** `RetryPolicy::next` is I/O-free and clock-free — it returns a `Duration`, and
  the store applies `available_at = now() + delay` **in SQL**, so worker clock skew can never
  hot-loop a row.

## The three duplicate windows

Every one of these is a **real** duplicate, not a bug, and each has a test in
`reliar-store-postgres`'s and `reliar-outbox`'s suites:

1. **Crash** (§22) — the broker accepts a publish, the worker dies before `complete` persists, the
   lease expires, and another worker republishes the same row.
2. **Slow batch** (§22.1) — no crash: a worker holds a batch under a lease shorter than the batch
   takes to drain. The lease expires mid-publish while the worker is perfectly healthy; a second
   worker reclaims and republishes the tail, and the first worker's later `complete`/`fail` is
   rejected by the `locked_by` guard (benign — a rows-affected shortfall, never an error). In
   practice this is the common one; lease renewal (`extend_lease`, fired at half the lease) and a
   per-publish `publish_timeout` make it rare, not impossible.
3. **Drain** (§26.1) — on cancellation, `run` drains in-flight publishes for at most
   `drain_timeout`. A publish still unresolved at the timeout is released rather than awaited
   further; its eventual success or failure carries the same duplicate risk as the other two,
   triggered by shutdown instead of a lease.

## Failure classification and retry

A publisher's error carries its own verdict (`Classify::kind() -> FailureKind::{Transient,
Permanent}`, ADR 0008) — the dispatcher never guesses. `RetryPolicy::next` turns that plus the
attempt count into a `FailureOutcome`: `Retry { delay }` (exponential backoff with jitter, capped)
or `Dead { reason }` (a `Permanent` failure, or `attempts + 1 >= max_attempts`). Dead rows are an
operator surface (`OutboxDeadLetters::list_dead`/`retry_dead`/`purge_dead`) the dispatcher never
touches on its own — see `docs/guides/postgres.md`.

## Shutdown

`run` consumes a `tokio_util::sync::CancellationToken`. On cancellation it stops claiming new
rows, **finishes what already started** (a publish task that has not yet acquired its concurrency
permit is dropped straight into the release set rather than begun), persists whatever resolved
within `drain_timeout`, releases the rest, and returns `Ok(())` — a real drain barrier, not an
abort (ADR 0014).

## See also

- `crates/reliar-outbox/README.md` — the crate's own statement of these guarantees, in full.
- `examples/outbox-basic/src/main.rs` — the loop end-to-end against the in-memory fakes.
- `docs/guides/postgres.md` — the durable path: schema, `migrate()`, retention.
