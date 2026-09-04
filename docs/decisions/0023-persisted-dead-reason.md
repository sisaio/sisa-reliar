# ADR 0023 — `dead_reason` is a persisted `text` column with a stable codec

**Status:** Accepted — 2026-09-04
**SRS:** §19.1, §19.5, §23, §23.2, §24.1, §43.A.15, §43.A.20, §43.A.32
**Extends:** ADR 0009. Raised by review 1 of the Phase-1 contract.

## Context

`DeadReason` (`PermanentError | AttemptsExhausted | Expired | Undecodable`) is decided by the
dispatcher, applied by the store, and — per §43.A.20 — **returned by `list_dead`**. SRS §24.1's
schema has `dead_at` and `last_error` but no column to hold it, so as written the reason is
computed, written nowhere, and then asserted on read. The contract's `OutboxRecord.dead_reason`
field has nothing behind it.

Three sub-questions follow: what SQL type, what wire values, and how a reader handles a value it
does not recognise (a row written by a newer Reliar during a rolling deploy, or by a `psql` session).

## Decision

- **`dead_reason text`**, nullable, on `outbox`. **Not** a PostgreSQL `ENUM` type: `DeadReason` is
  `#[non_exhaustive]` and will gain variants, and `ALTER TYPE … ADD VALUE` cannot run inside a
  transaction — which makes it hostile to a migrator that wraps each file in one, and to ADR 0018's
  lock-safety rule. `text` costs a few bytes on rows that are already terminal.
- **The codec is a fixed, stable, lower snake_case string**, and it is a public contract because
  operators query it: `permanent_error`, `attempts_exhausted`, `expired`, `undecodable`. These
  values are **never renamed**; a new variant only ever adds a new string.
- **`ck_outbox_dead_reason CHECK ((dead_at IS NULL) = (dead_reason IS NULL))`** — the two are set
  together or not at all, in the same statement. This is the schema-level statement of the
  invariant, alongside `ck_outbox_lease` and `ck_outbox_terminal` (§24.1).
- **An unrecognised value is a poison row** (ADR 0008): excluded from `list_dead`'s decoded results
  and reported, never a panic and never a silent `None`. The row is already dead, so there is no
  further transition to make — the operator sees it flagged rather than mislabelled.
- `retry_dead` clears `dead_reason` with `dead_at`, keeping `last_error` for audit (ADR 0008).
- **The SRS amendment is the PO's.** §24.1's DDL, §43.A.32's constraint-name assertion and the
  `DeadReason` round-trip criterion are carded as RELIAR-23; this ADR is the decision, the
  frozen contract (`../architecture/phase1-contract.md` §4) is the schema direction the engineer
  builds from, and neither waits on the amendment.

## Consequences

- `list_dead` can answer "why did this die?" in SQL — `WHERE dead_reason = 'expired'` is the query
  an operator actually runs, and it is indexable if it ever needs to be.
- A dead row's reason survives a Reliar upgrade, because the codec is strings rather than ordinals.
  Storing an integer discriminant would have been smaller and would have broken the moment the
  enum's declaration order changed.
- One more column and one more check constraint in `0001_outbox.sql`, and one more field in the
  promoted-column list (ADR 0012). `dead_reason` is **not** part of `MetadataRest`: it is delivery
  state, not message metadata (ADR 0005).
- The check constraint makes a half-written dead transition impossible, so a partial update surfaces
  as a constraint violation at write time rather than as an unexplained row later.
- Adding a variant later is additive in Rust (`#[non_exhaustive]`) and needs no migration — but an
  **older** instance reading a newer variant during a rolling deploy sees a poison row. That is the
  same trade as `metadata_version` (ADR 0012) and is acceptable because the row is already terminal.

## Alternatives considered

- **A PostgreSQL `ENUM` type.** Rejected: `ALTER TYPE … ADD VALUE` outside a transaction fights the
  migrator, and the type becomes a second place the variant list lives.
- **A `smallint` discriminant.** Rejected: couples the on-disk value to the enum's declaration
  order, so reordering variants silently relabels every historical row.
- **Derive the reason from `last_error`'s text.** Rejected: `last_error` is a truncated, redacted
  human string, not a machine field; parsing it would make a log message load-bearing.
- **No column — recompute on read.** Rejected: the information is not recoverable. Nothing on the
  row distinguishes "permanent broker error" from "attempts exhausted" after the fact.
- **Fold it into `metadata` JSONB.** Rejected by ADR 0012: delivery state never goes in the message
  metadata blob, and it would not be filterable without an expression index.
