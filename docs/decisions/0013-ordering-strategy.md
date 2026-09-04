# ADR 0013 — Ordering is a configured strategy; `Unordered` is the default and the only v0.1 mode

**Status:** Accepted — 2026-09-04
**SRS:** §22.2, §24.1, §7.2, §43.A.13
**Decisions:** human decision 2 (configurable strategy), decision 11 (v0.1/0.2 split)

## Context

Neither v1.0's §21, §22, §26 nor §43 mentioned ordering at all. With `FOR UPDATE SKIP LOCKED`,
bounded concurrent publishing, per-message backoff and multiple workers, Reliar preserves **no**
ordering — not globally, not per conversation, not per aggregate. This is the first question every
adopter asks and, unaddressed, the first bug report: users arriving from MassTransit or Wolverine
assume per-key FIFO.

"No ordering, ever" would be simple and honest but forecloses a guarantee some workloads genuinely
need. The expensive part of an ordered mode is not the enum — it is the schema. Adding a monotonic
sequence column and an ordering key to a large, hot outbox table later is the migration operators
refuse to run.

## Decision

- Ordering is a **configured strategy**, set on the dispatcher builder and passed to the store in
  `AcquireRequest`, because the guarantee needs both the claim query and the publish loop —
  neither can offer it alone:
  `enum Ordering { #[default] Unordered, PerKey }`, `#[non_exhaustive]`.
- **`Ordering::Unordered` guarantees nothing about order** — not globally, not per
  `conversation_id`, not per aggregate, not approximately. `SKIP LOCKED`, concurrent publishing,
  per-message backoff and multiple workers each reorder freely; a retried message can arrive after
  messages enqueued minutes later. Claims are approximately FIFO by `(available_at, sequence)`
  within one worker — a scheduling heuristic, **never** a documented guarantee.
  The disclaimer appears in the README, in `OutboxDispatcher`'s rustdoc, and as an AC (§43.A.13).
- **`Ordering::PerKey`, when implemented, guarantees** for rows sharing a non-null `ordering_key`:
  at most one message per key in flight across **all** workers; publication in `sequence` order;
  and that a failed message **blocks its key** until it succeeds or dies — head-of-line blocking
  per key is the price and is documented as such. Rows with a `NULL` key are unaffected.
  It does **not** guarantee cross-key ordering, that the *broker* preserves publish order (a
  per-key subject/partition is the transport's job, Phase 2), or exactly-once — a duplicate can
  still replay a key's message, so `PerKey` is ordered **at-least-once**.
- **Schema support ships in v0.1 regardless of the mode** (§24.1): `sequence bigint GENERATED
  ALWAYS AS IDENTITY` (the monotonic tiebreak a random UUID PK and a tying `created_at` cannot
  give) and a nullable `ordering_key text` with its partial index, both in `0001_outbox.sql`.
  **An ordered mode SHALL cost no migration.**
- **v0.1 implements `Unordered` only** (decision 11). Constructing a dispatcher with
  `Ordering::PerKey` SHALL return a **configuration error naming 0.2** — never a silent degradation
  to unordered. `#[non_exhaustive]` plus an additive `AcquireRequest` field make shipping `PerKey`
  in 0.2 a non-breaking change.

## Consequences

- v0.1 adopters get an honest, testable statement instead of an implied guarantee, and the enum
  tells them an ordered mode is coming rather than that it does not exist.
- The v0.1 table carries two columns and one partial index it does not yet use. That is the
  deliberate cost — measured in bytes — of never asking an operator to alter a hot queue table.
- `PerKey`'s implementation cost is roughly the dispatcher slice again: the extra `NOT EXISTS`
  claim predicate plus `DISTINCT ON` and its `EXPLAIN`, a per-key in-flight registry, and a
  multi-worker test matrix (a key whose message dies mid-stream, a key containing a poison row,
  starvation behind a hot key). Deferring it buys v0.1 a whole slice for a guarantee no v0.1
  adopter can consume without a real transport (Phase 2).
- A hot key degrades `PerKey` to serial throughput by design. Documented, not engineered around.
- `sequence` is a per-table identity, so it is monotonic but not gap-free; nothing depends on
  gaplessness.

## Alternatives considered

- **Guarantee nothing, no enum, no columns** (the original architect recommendation). Superseded by
  decision 2: it would make an ordered mode a migration on a hot table.
- **Ship `PerKey` in v0.1.** Rejected by decision 11: it roughly doubles the dispatcher slice for a
  guarantee that cannot be exercised end-to-end until Phase 2.
- **Silently fall back to `Unordered` when `PerKey` is selected.** Rejected: a silent downgrade of a
  correctness guarantee is the worst possible failure mode.
- **Order by `conversation_id` implicitly.** Rejected: an implicit guarantee nobody asked for,
  which would then be impossible to remove.
