# ADR 0008 — The `OutboxStore` operation set, worker guard, and poison-row policy

**Status:** Accepted — 2026-09-04
**SRS:** §19, §19.1–§19.6, §21.1, §23.2, §26.1, §33.1, §43.A.6–10, §43.A.16, §43.A.19–21
**Supersedes:** the three-method `acquire`/`complete`/`fail` sketch in SRS v1.0 §19

## Context

v1.0's `OutboxStore` had `acquire`, `complete`, `fail` — and none of them took a `WorkerId`, while
§24 had a `locked_by` column and §25 let another worker reclaim an expired lease. That is not a
gap, it is a **correctness bug**: a slow worker's late `complete` would overwrite a row a second
worker had already republished, resetting `attempts`/`available_at` or marking a live message dead.

Several requirements also had no operation to satisfy them: retention (§26), the lag and dead
gauges (§33.1), graceful drain (§26.1), lease renewal for long batches (§21.1), and the dead-state
transition itself (§23). And `AcquireRequest`/`CompletedMessage`/`FailedMessage` were named but
never defined — they *are* the Phase-1 contract that lets the provider and the dispatcher be built
in parallel.

## Decision

- **Operation set:** `acquire`, `complete`, `fail`, `release`, `extend_lease`, `purge`, `stats`.
  Dead-letter inspection (`list_dead` / `retry_dead` / `purge_dead`) is a **separate small trait**,
  `OutboxDeadLetters` — the dispatcher never calls it; an operator surface does (§34: no God trait).
- **Every state-changing operation takes `worker: &WorkerId` and matches it (`AND locked_by =
  $worker`).** A store that ignores the argument does not implement this trait. This is the single
  most important invariant in the product.
- **A row-count shortfall is benign, never an error.** `complete`/`fail`/`release`/`extend_lease`
  return `u64` rows affected. Fewer than expected means the lease was lost; the dispatcher logs
  `debug` (`warn` for a whole lost batch) and continues. At-least-once makes that safe (ADR 0007).
- **`attempts` increments on outcome only**, inside `complete`/`fail` — never on claim. Otherwise a
  DB blip, a lease expiry or a crash before any publish burns the retry budget and kills healthy
  messages. Cost: a claimed-then-crashed row reports `attempts = 0`, which is accurate.
- **The store never re-derives policy.** `FailedMessage.outcome` is
  `Retry { delay } | Dead { reason }`, decided by `RetryPolicy` in `reliar-outbox` (ADR 0009); the
  store only applies `available_at = now() + delay` in SQL.
- **Poison rows (§19.5).** A row that cannot be decoded is excluded from `AcquiredBatch::records`,
  reported in `AcquiredBatch::poisoned`, and moved to dead with `DeadReason::Undecodable` **in the
  same call**. `acquire`'s `Self::Error` is reserved for failures of the *call*, never the content
  of a row, and a decode failure SHALL NEVER panic. Without this, one corrupt row makes every poll
  return `Err` and the table never drains again — a total outage from a single bad row.
- **`publish_batch` is on `Publisher` from day one**, with a looping default impl and **positional**
  per-envelope results, because the trait shape is semver-visible and §32 requires batching.
  Classification lives **with the error type** (`P::Error: Classify`), not as a publisher method:
  the error value is what crosses the `JoinSet` boundary, so it must carry its own verdict.
- **`enqueue` is deliberately not on the trait** (§19.6). It must be atomic with the application's
  own writes, so it takes `&mut sqlx::Transaction<'_, Postgres>` and stays a provider-inherent
  method. A portable `enqueue` would need a transaction abstraction in `reliar-core` that leaks
  provider semantics anyway and hides the atomicity requirement the concrete signature makes
  obvious at every call site.
- **`list_dead` orders by `sequence`, and that ordering is part of the contract.** `after_sequence`
  is a keyset cursor, and a keyset cursor is only correct over the column the query orders by —
  paginating by `sequence` while ordering by `(dead_at, sequence)` silently skips rows. `sequence`
  is unique and monotonic, so it needs no tiebreak; `dead_before`, `message_type` and `tenant_id`
  are filters, never part of the order. The supporting index is therefore
  `ix_outbox_dead ON outbox (sequence) INCLUDE (dead_at) WHERE dead_at IS NOT NULL` — the `INCLUDE`
  lets `dead_before` be evaluated from the index tuple, but **only when `message_type` and
  `tenant_id` are unset**; those are not indexed, and a page filtered by either still visits the
  heap. Acceptable for an operator surface. **A second index, `ix_outbox_dead_at (dead_at) WHERE
  dead_at IS NOT NULL`, exists for `purge`'s dead-retention delete** (`dead_at < now() -
  dead_retention`), which the sequence-leading index cannot serve; without it the bounded `DELETE`
  scans every dead row on every pass. Both are partial, so on a healthy table they index almost
  nothing. On the partitioned variant (ADR 0016) this
  becomes a `Merge Append` over the per-partition indexes, exactly as the claim does. The page
  returns `DeadLetterPage`, whose cursor is the largest `sequence` **scanned**, poisoned rows
  included — deriving it from the last *decoded* record would loop forever on a poisoned tail.
  *(Added 2026-09-04 after review 3 of the Phase-1 contract; SRS §19.3 and §24.1's `ix_outbox_dead`
  need the matching amendment — PO, RELIAR-23.)*
- **`stats` is polled by the dispatcher**, on `stats_interval`, because it only reads; `purge`
  writes and therefore stays the host's to schedule. Both remain `pub` on the store.
  *(Added 2026-09-04 after review 3.)*
- `release` clears the lease with **`attempts` unchanged** — handing rows back is not a failure.
  `retry_dead` is the only operation in the system that resets `attempts`, and it is always an
  explicit operator action.

## Consequences

- Seven methods is a large trait for one provider to implement, and every future provider pays it.
  Accepted: each one exists because a stated requirement has no other source.
- Callers must treat "0 rows affected" as normal control flow. This is unusual and is called out in
  rustdoc on every guarded method.
- `stats` on the hot trait means providers must answer a count query; it is polled on an interval
  (default 15 s), not per batch, and it is the only source for the two signals operators alert on.
- `MessageRef { id, created_at }` rather than a bare `MessageId` on the dead-letter operations
  costs nothing in v0.1 and is what keeps partitioning (ADR 0016) from being an API break.
- `#[non_exhaustive]` on every contract struct means `AcquireRequest` can gain `message_type`
  filtering or a per-type dispatcher later without a break.

## Alternatives considered

- **Unguarded `complete`/`fail` (v1.0's shape).** Rejected: the correctness bug above.
- **Returning `Err` on a row-count shortfall.** Rejected: a lost lease is the *designed* recovery
  path, and turning it into an error would make normal operation look like failure.
- **`Publisher::classify(&self, err)` instead of `P::Error: Classify`.** Rejected: the error, not
  the publisher, is what travels to the dispatcher.
- **`acquire` returning `Err` on an undecodable row.** Rejected: one bad row stops the world.
- **A `ReliabilityStore` God trait** covering outbox + inbox + idempotency. Rejected by §34.
