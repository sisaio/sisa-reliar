# ADR 0016 — Partitioning designed in v0.1, shipped as an opt-in variant in 0.2

**Status:** Accepted — 2026-09-04
**SRS:** §24.3, §19.3, §23.2, §35.1
**Decisions:** human decision 18

## Context

`outbox` is a queue table: rows are inserted, updated a handful of times, then deleted by retention.
At volume the **purge** is the part that hurts — millions of row deletes write WAL, bloat the heap
and leave autovacuum chasing the claim index. Range partitioning turns retention into a catalog
operation (`DROP TABLE <partition>`) instead.

The trap is that partitioning is not a drop-in change. PostgreSQL requires the partition key inside
every unique constraint, so the primary key shape changes — and converting a populated table
requires a full rewrite. If the design is deferred entirely, adopting it later is an API break plus
a migration operators will refuse.

## Decision

- **The design is fixed now; the partitioned table is not v0.1's default.** v0.1 ships the plain
  table. The partitioned DDL ships in **0.2 as an opt-in migration variant** — a separate migration
  file chosen at deploy time, **never an automatic conversion**, because converting a populated
  table requires a rewrite Reliar must not perform behind an operator's back.
- **Shape:** `PARTITION BY RANGE (created_at)`, weekly by default (daily for very high volume,
  monthly for low), plus a `DEFAULT` partition as a safety net that is **expected to stay empty**.
- **v0.1 pays exactly three costs, and pays them now so 0.2 is not a break:**
  1. **`created_at` is immutable.** No Reliar statement SHALL ever update it — changing it would
     move a row between partitions.
  2. **Every by-id operation carries `created_at`.** `complete`, `fail`, `extend_lease` and
     `release` work from rows `acquire` returned, so the pair is already in hand; their `UNNEST`
     joins gain `AND o.created_at = f.created_at` and prune to one partition.
  3. **The dead-letter API takes `MessageRef { id, created_at }`, not a bare `MessageId`** — a
     contract amendment to §19.3 made in v0.1, where it costs nothing (ADR 0008).
- **`pk_outbox` becomes `(created_at, id)`** in the partitioned variant. Two consequences are
  documented rather than engineered around: `id` becomes unique **per partition**, so the
  duplicate-enqueue guarantee weakens to "a reused `MessageId` is rejected only within the same
  period" (this reaches only applications minting deterministic ids — Reliar's own are v7); and
  `ix_outbox_sequence` becomes unenforceable, with uniqueness coming from the sequence object
  itself. A global unique index on a partitioned table is impossible by construction; the honest
  move is to document it.
- **Maintenance is the host's, not a thread Reliar starts.** `ensure_partitions()` is a store
  operation the host calls from the same periodic task that calls `purge`. It is idempotent
  (`CREATE TABLE IF NOT EXISTS … PARTITION OF`) and creates partitions **4 periods ahead** by
  default, so a missed run degrades to the `DEFAULT` partition instead of failing an `INSERT`.
- **Retention becomes `DROP TABLE <partition>`** — but only when the partition holds no pending and
  no dead rows (dead rows are kept until explicitly purged, ADR 0009). The maintenance pass falls
  back to the bounded `DELETE` for a partition that still holds them.

## Consequences

- v0.1's signatures and columns never have to change to adopt partitioning. That is the entire
  point of deciding it now.
- Claims become a `Merge Append` over each partition's `ix_outbox_pending`. Pending rows concentrate
  in the newest partition or two, so older ones contribute an empty index scan: cheap, not free —
  and the real argument for coarse granularity. Past roughly 50 live partitions, planning time on
  every claim starts to show.
- `CREATE INDEX CONCURRENTLY` cannot target a partitioned parent in any PostgreSQL release to date,
  including 18. A later index change on a populated partitioned outbox is therefore an **operator
  procedure**: build concurrently on each partition, then create the parent index non-concurrently
  as a metadata-only step.
- Attaching a new partition while rows sit in `DEFAULT` takes an `ACCESS EXCLUSIVE` lock and scans
  it — precisely the pause partitioning was meant to remove. Hence "expected empty" is normative,
  and `ensure_partitions_ahead` defaults to 4.
- Moving dead rows to an archive table so every partition becomes droppable is deliberately left to
  a later version.

## Alternatives considered

- **Partition in v0.1 by default.** Rejected: it imposes partition maintenance on every adopter,
  including the ones with a thousand rows a day.
- **Defer the design entirely.** Rejected: `MessageRef`, immutable `created_at` and the composite
  PK would all become breaking changes.
- **Automatic conversion on `migrate()`.** Rejected: a full table rewrite under an operator's feet.
- **Time-based `DELETE` only, no partitioning ever.** Rejected: it is the known failure mode of
  queue tables at volume, and the design costs v0.1 almost nothing.
