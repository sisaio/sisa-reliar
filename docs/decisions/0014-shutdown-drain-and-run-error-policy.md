# ADR 0014 — Graceful drain, lease release, and `run()`'s error policy

**Status:** Accepted — 2026-09-04
**SRS:** §5, §26, §26.1, §21.1, §22.1, §43.A.18, §43.A.22
**Extends:** ADR 0007

## Context

Graceful shutdown is required in §5, §26 and §32 and was defined in none of them. Five things were
unspecified and each has a plausible-but-wrong default: whether cancellation stops claiming,
whether in-flight publishes are awaited or aborted, how long to wait, what happens to publishes
still running when that expires, and whether leases are released on a clean exit.

Equally unspecified was **`run()`'s error policy**. The obvious implementation propagates a store
error out of the loop — so a single Postgres failover, or a moment of pool exhaustion, kills the
dispatcher and the host has no worker until someone restarts the pod. A library that dies on a
database blip is unadoptable.

## Decision

On cancellation, `OutboxDispatcher::run(cancel)` SHALL, in order:

1. **Stop claiming immediately.** No `acquire` after the token fires. A claim already in flight is
   awaited — it is one short statement — and its rows join the drain.
2. **Drain in-flight publishes** for at most `DispatcherSettings::drain_timeout` (default 30 s).
   **Only publishes that have actually started.** A spawned task still waiting for a concurrency
   permit is dropped and its row joins step 4's release set. Waiting to *begin* a publish that will
   be released anyway spends the drain budget and widens the duplicate window for no delivery.
   The precise claim is "**has not completed a publish**", not "has not touched the broker": on a
   multi-thread runtime such a task may be polled once before the drop lands, so a released row can
   already have been delivered — the same at-least-once window as the other three, not a new one.
   *(Clarified 2026-09-04 after the S4 reviews.)*
   Publishes are **not aborted**: an aborted publish may already have reached the broker, so
   cancelling it discards the *outcome* without preventing the *delivery* — strictly worse than
   waiting.
3. **Persist every outcome that resolved** during the drain (`complete` / `fail`). An unpersisted
   success is a guaranteed duplicate, so this step matters more than finishing quickly.
4. **`release` the remainder** — rows claimed but never published, and rows whose publish had not
   resolved at the timeout. **One exception:** a row whose publish *succeeded* but whose
   `complete` never landed is **left to its lease** rather than released. It is already delivered,
   so releasing it converts a possible duplicate into an immediate certain one and recovers
   nothing sooner; letting the lease lapse is strictly better. *(Added 2026-09-04, S4 review 2.)* This clears the lease at once so a rolling deploy does not stall a
   batch for a full lease per worker. Rows still publishing at the timeout are a **third duplicate
   window** (ADR 0007) and SHALL be logged at `warn` with their count.
5. Return **`Ok(())`**. Cancellation is a normal outcome, not an error.

`run()`'s error policy:

- **Every store call `run` makes is bounded by `DispatcherSettings::store_timeout`** (default
  30 s). Without it `drain_timeout` is unenforceable — one hung `complete`, against a lost
  connection or a saturated pool, holds shutdown open indefinitely. A timeout counts as a transient
  store error. It is a *client-side* bound on the future, which is what cancellation needs; the
  provider's `statement_timeout` bounds the *server* statement and defaults to inherit, so neither
  substitutes for the other. *(Added 2026-09-04 after the S4 review.)*
- **The permanent-error exit drains as well**, best-effort: `run` persists resolved outcomes and
  releases the remainder before returning `Err`, logging and discarding any error from that drain so
  the original diagnosis is what surfaces. A store broken badly enough to be permanent will often
  fail the release too — but exiting with a full batch still leased leaves it dark for a whole
  lease, which is worth one attempt to avoid. *(Added 2026-09-04 after the S4 review.)*
- **The claim loop applies backpressure**: `run` claims only while `outstanding < max_in_flight`,
  asking for `min(batch_size, max_in_flight - outstanding)`. Without it a publisher slower than the
  poll interval lets one dispatcher hoard leases without bound, and the hoarded tail expires under a
  healthy worker — which is the §22.1 slow-batch duplicate window. The gate therefore shrinks that
  window, not just memory. Consequence: `max_in_flight` is the real ceiling on outstanding rows and
  `batch_size` only caps one claim statement. *(Added 2026-09-04, S4 review 2.)*
- **A failed outcome write keeps its rows outstanding** and is retried next iteration, rather than
  dropped — a dropped outcome leaves the row leased with `attempts` unadvanced, owned by a worker
  that has forgotten it. The `locked_by` guard makes the retry idempotent, and a lost lease makes it
  affect zero rows. This is SRS §23.2's "publish succeeded, completion failed" window, **not a new
  one**. *(Added 2026-09-04, S4 review 2.)*
- **That retry is bounded in both directions** *(added 2026-09-04, S4 review 3)*. Unbounded, it
  wedges the worker: `outstanding` fills to `max_in_flight`, claiming stops, the leases are renewed
  forever and `run` never returns — a silently stalled worker, which is the worst available outcome
  because nothing about it looks like a failure.
  - A **`Permanent`** outcome-write error (schema drift, a `23514` on a row Reliar wrote) **ends the
    loop**: best-effort drain, then `Err(DispatchError::Store(..))` carrying the original error. The
    unwritten published rows are **left to their lease, never released** — they are already
    delivered, so releasing buys an immediate certain duplicate and recovers nothing.
  - A **transient** failure is bounded by **`lease`**: once a row's unwritten outcome has been
    retried for longer than the lease, the row is dropped from `outstanding` **and excluded from
    lease renewal**, so the lease lapses and another worker reclaims it. Both halves are required —
    dropping it while still renewing leaves a row nobody owns and nobody can claim. The lease is the
    right bound because it is already how long an unreachable worker's rows stay dark.

  The gate therefore always frees: the write lands, the row leaves `outstanding` within a lease, or
  `run` returns.
- **A store error is transient by assumption.** `run()` logs it at `error`, backs off (the ADR 0009
  schedule applied to the loop) and continues. A Postgres restart, failover or pool exhaustion
  SHALL NOT end the worker loop.
- `run()` returns `Err` **only** for something no retry can fix: invalid configuration detected at
  startup, or a store error the provider classifies as permanent (for example, the migrations have
  not been run).
- A publish error never ends the loop; it is a per-message outcome (ADR 0009).
- `run()` is **idempotent under repeated cancellation**, and it is safe to run many dispatchers, in
  many processes, against one table.
- One `CancellationToken` drives both the host's graceful shutdown and the dispatcher, so
  `worker.await` in the host is a real drain barrier rather than an abort (§20.1).

## Consequences

- A deploy releases leases promptly instead of leaving a batch dark for up to one lease per pod.
- `drain_timeout` is a real trade-off exposed to the operator: too short and step 4 widens the
  duplicate window; too long and shutdown blocks. Default 30 s, matching the lease.
- Because publishes are awaited rather than aborted, a hung broker call could hold shutdown for
  `drain_timeout`. That is bounded, and `publish_timeout` (10 s, transient) bounds it further.
- The loop can spin quietly through a long outage. It logs at `error` on each failure and backs
  off, so it is visible without being a log flood; `stats`-driven lag metrics are what actually
  alert (§33.1).
- "Never fail on a store error" means a genuinely misconfigured deployment (no migrations) would
  loop forever unless the provider classifies it as permanent. That classification is therefore a
  provider obligation, called out in the store's error enum.

## Alternatives considered

- **Abort in-flight publishes on cancel.** Rejected: discards outcomes for deliveries that already
  happened, converting a clean shutdown into guaranteed duplicates.
- **Wait indefinitely for the drain.** Rejected: a hung broker would block a deploy forever.
- **Let the lease expire instead of calling `release`.** Rejected: stalls a batch for a full lease
  on every rolling deploy, for no benefit.
- **Propagate store errors out of `run()`.** Rejected: makes the worker die on a routine failover.
- **Return `Err` on cancellation.** Rejected: cancellation is the designed exit path; making it an
  error forces every host to special-case it.
