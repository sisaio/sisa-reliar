# ADR 0015 — PostgreSQL 18 floor; UUIDv7 ids generated client-side

**Status:** Accepted — 2026-09-04
**SRS:** §11, §17.1, §24.1
**Decisions:** human decisions 12, 25

## Context

Two coupled questions. **Which PostgreSQL versions does Reliar support?** A low floor maximises
reach but forces version-conditional DDL — different indexes or defaults per server — which means a
deployed schema that differs between environments. **Where do primary keys come from?** A random
UUIDv4 primary key scatters b-tree inserts across the index and gives no time ordering; a database
`DEFAULT` cannot return the id to the caller inside their own transaction, which `enqueue` must do
so the id can become the `causation_id` of the next message in the same transaction.

The earlier architect recommendation was PG 15 with no column default. The PO overrode it.

## Decision

- **Minimum supported PostgreSQL is 18.** Reliar targets the current major and its successors, not
  the long tail.
- **Every UUID Reliar generates is UUIDv7, produced client-side in the library** — `MessageId`,
  `ConversationId`, `RequestId`. `enqueue` returns the id it wrote. No Reliar code path depends on
  a column default.
- `id` nonetheless carries **`DEFAULT uuidv7()`** (PG 18). It is a **safety net, not the source**:
  it exists so a hand-written `INSERT` — a data fix, a backfill, a psql session during an incident
  — cannot create a row with a random or NULL id.
- Applications may supply any UUID for their own ids; **Reliar SHALL NOT inspect or reject its
  version**.
- **Version-conditional DDL is rejected outright.** One schema, one server version to reason about.
  The `pg` suite runs against the floor (18) and the newest major in the matrix, and asserts the
  DDL is identical on both (§43.A.34).

## Consequences

- The floor buys three things the schema uses directly: `uuidv7()`; identity columns on partitioned
  tables (PG 17+), so ADR 0016's partitioned variant keeps the same `GENERATED ALWAYS AS IDENTITY`
  sequence column rather than a hand-rolled sequence; and a single version to test.
- Everything else Reliar needs is far older — `FOR UPDATE SKIP LOCKED` (9.5), `jsonb` (9.4),
  declarative partitioning (10–11) — so **raising the floor costs nothing technically. It costs
  only reach**, and that is the deliberate trade: teams on PG 13–17 cannot adopt v0.1 without
  changing the `id` default themselves.
- UUIDv7 gives b-tree locality on the primary key and a time-ordered tiebreak, which matters on a
  table where inserts are the hot path.
- Lowering the floor later is additive and easy (drop the `DEFAULT`, or make it conditional in a
  new migration file). Raising it later would be a breaking change. Starting high is reversible;
  starting low is not.
- **Correction to the decision's stated rationale:** `CREATE INDEX CONCURRENTLY` cannot target a
  partitioned parent in **any** PostgreSQL release to date, including 18. That is a standing
  limitation, not a version caveat, and it is why ADR 0016 documents a per-partition operator
  procedure. The half that does hold is identity columns on partitioned tables.

## Alternatives considered

- **PG 15 floor, no `DEFAULT`** (the withdrawn recommendation). Rejected by decision 25: the reach
  is not worth carrying two schema shapes, and `uuidv7()` as a safety net has real value during
  incidents.
- **Version-conditional migrations** (emit `DEFAULT uuidv7()` only on 18+). Rejected: environments
  would diverge and every support conversation would start with "which server version?".
- **UUIDv4 ids.** Rejected: random primary keys fragment the index on the hottest write path and
  give no time ordering, which is exactly what `sequence` then has to compensate for.
- **Database-generated ids (`DEFAULT` as the source).** Rejected: `enqueue` could not return the id
  inside the caller's transaction without a `RETURNING` round-trip the caller cannot use before
  commit for correlation chaining.
