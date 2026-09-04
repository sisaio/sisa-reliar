# ADR 0009 — Retry policy is pure and lives in `reliar-outbox`; attempts count outcomes

**Status:** Accepted — 2026-09-04
**SRS:** §23, §23.1, §23.2, §12.2, §25, §25.1, §31, §43.A.14–15, §43.A.17

## Context

v1.0 described transient/permanent classification and a dead state but assigned ownership to
nobody. Three questions were open and each has a wrong answer that looks reasonable:

1. **Where does the backoff formula live?** If the store computes `delay` from `attempts`, every
   provider re-implements the policy and they drift.
2. **Where does "now" come from?** If the policy returns a *timestamp*, a worker with a skewed
   clock can either hot-loop a row (clock behind) or park it in the future (clock ahead).
3. **When does `attempts` increment?** On claim is the easy implementation and the wrong one.

§31 additionally listed "Clock where required for testing", which reads as an invitation to build a
`Clock` port and compare leases against it — reintroducing exactly the skew bug §25 exists to
prevent.

## Decision

- **`RetryPolicy` is a pure trait in `reliar-outbox`:**
  `fn next(&self, attempts: u32, kind: FailureKind) -> FailureOutcome`. It is **I/O-free and
  clock-free** and returns a `Duration`, never a timestamp. `ExponentialBackoff` is the default
  (base 1 s, max 5 min, max_attempts 10, jitter 0.2).
- Rules: `FailureKind::Permanent` → `Dead { PermanentError }` immediately, whatever `attempts` is.
  `attempts + 1 >= max_attempts` → `Dead { AttemptsExhausted }`. Otherwise
  `Retry { delay = min(max_delay, base × 2^attempts) × jitter_factor }`.
- **The store applies the delay in SQL:** `available_at = now() + delay`. The delay crosses the
  boundary as milliseconds; the timestamp is computed by the database (§25).
- **`attempts` increments on outcome**, inside `complete`/`fail` (ADR 0008). The budget counts
  observed *publish* failures, not claims.
- **There is no `Clock` trait on any correctness path**, and no correctness decision compares a
  database timestamp with an application timestamp. §31's "Clock where required for testing" is
  superseded. The split is fixed:
  - **SQL `now()`** owns `locked_until`, `available_at`, `expires_at` enforcement, `published_at`,
    `dead_at`, `updated_at`, `created_at`.
  - **App clock** owns `sent_at`, backoff jitter, idle sleeps, publish timeouts, metric durations.
  - Dispatcher timing is tested with `#[tokio::test(start_paused = true)]` + `tokio::time::advance`;
    database timing is tested by **SQL time-travel**, because no Rust fake can move Postgres's clock.
- **Purge is one bounded pass per call, and `batch_size` bounds all three statements.**
  `OutboxStore::purge` deletes at most `batch_size` published rows, deletes at most `batch_size`
  dead rows, and moves at most `batch_size` expired pending rows to dead — the sweep as
  `UPDATE … WHERE id IN (SELECT … LIMIT n)`, **never an unbounded `UPDATE`**. An unbounded `UPDATE`
  on `outbox` is the same hazard as an unbounded `DELETE`, not a lesser one: it holds row locks
  across the entire matched set, blocks concurrent claims for the duration, and writes a WAL record
  per row with no cancellation point. It returns all three counts; **the caller repeats** while
  `!PurgeReport::is_complete(batch_size)`, which is `true` only when **all three** are under the
  bound — so the host's drain loop drains expiry as well. Never one unbounded statement on a hot
  table, and never an unbounded loop inside a trait method — that would have no cancellation point
  and no progress reporting. *(Bound extended to the sweep 2026-09-04 after RELIAR-13 review 2.)*
- **The expiry sweep never transitions a leased row.** Its predicate carries the claim's own lease
  clause, so it moves only rows that are expired **and** unowned. A row whose `expires_at` passed
  *after* it was claimed belongs to its worker: that worker's `complete` still wins if the publish
  succeeded, and the row becomes sweepable once the lease lapses — at most one lease later, for a
  row that is unpublishable either way. The alternative pits an **unguarded** maintenance write
  against a **worker-guarded** one, and `ck_outbox_terminal` (`published_at IS NULL OR dead_at IS
  NULL`) converts that race into a constraint violation on a healthy path: a publish that genuinely
  succeeded, reported by its rightful owner, rejected because maintenance marked the row dead first.
  `purge` is the one Reliar statement that writes without a `WorkerId`, which is precisely why it
  must yield to anything that has one. *(Added 2026-09-04 after RELIAR-14 review 1.)* The dead half is served by `ix_outbox_dead_at (dead_at) WHERE dead_at IS NOT
  NULL`, the published half by `ix_outbox_published`, and the expiry sweep by `ix_outbox_expires` —
  a bounded `DELETE` without a supporting index is a scan wearing a `LIMIT`. *(Clarified 2026-09-04 after review 1 of the Phase-1 contract; SRS §23.2's
  "looped until fewer than n rows are affected" describes the caller's loop.)*
- **Dead is terminal and inspectable.** `DeadReason` is
  `PermanentError | AttemptsExhausted | Expired | Undecodable`. Dead rows keep `dead_at`,
  `attempts`, `last_error` and the full envelope, and are **kept until an explicit purge**
  (`dead_retention` defaults to `None`). Deleting a dead message is always a deliberate act.
- **`expires_at` is enforced, not decorative** (§12.2): it is a promoted column, it is in the claim
  predicate (`expires_at IS NULL OR expires_at > now()`), and an expired pending row is moved to
  dead with `DeadReason::Expired` by the retention pass. Expiry consumes **no** retry attempt and is
  **not** a publish failure. After publication it is advisory only.
  **The sweep runs only in `purge`, never in the claim statement** — the claim is the hottest path
  in the system and must not carry a write to fix bookkeeping. Until the sweep runs, an expired row
  is simply unclaimable; it is excluded from `stats.pending` and the lag gauge and counted as
  `expired_pending` instead, so a host that never purges gets a signal rather than a false backlog.
  *(Clarified 2026-09-04 after review 2 of the Phase-1 contract.)*
- **Publish succeeded, completion failed** is a stated case: the dispatcher retries the outcome
  write with backoff (the `locked_by` guard makes a repeated `complete` idempotent) before
  abandoning the batch. If it never lands, the lease expires and the message is republished — the
  documented at-least-once outcome, bounded by `max_attempts` on the next owner.
- In v0.1 `max_attempts` lives in the dispatcher's policy — not per message, not per
  `message_type`. Per-type policies are an additive later change.

## Consequences

- Backoff bounds are **proptest-able without a database**: monotonic in `attempts`, capped at
  `max_delay × (1 + jitter)`, never zero.
- A clock-skewed worker cannot corrupt scheduling, because it never writes a due timestamp.
- Retry behaviour is identical across providers by construction — there is one implementation.
- Jitter uses the app clock and an RNG. That is a thundering-herd nicety, not a correctness input,
  so it is explicitly exempt from the DB-time rule.
- A crashed-before-publish row shows `attempts = 0`. Operators reading the table must know that
  `attempts` means "outcomes observed", which is rustdoc'd on `OutboxRecord`.
- Dead rows accumulate until purged. `dead_retention` exists for data-minimisation obligations, and
  Reliar says so rather than quietly keeping rows forever.

## Alternatives considered

- **Store-computed backoff.** Rejected: policy duplicated per provider, untestable without a DB.
- **Policy returns a timestamp.** Rejected: reintroduces clock skew as a correctness input.
- **Increment `attempts` on claim.** Rejected: a DB blip or lease expiry would kill healthy
  messages; the retry budget must measure publish attempts.
- **A `Clock` port for testability.** Rejected: paused `tokio::time` and SQL time-travel cover both
  halves, and a port invites comparing app time to DB time.
- **Auto-retrying dead rows after a cool-off.** Rejected: dead is terminal; automatic resurrection
  hides a real failure from the operator.
