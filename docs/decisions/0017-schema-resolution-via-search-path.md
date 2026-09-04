# ADR 0017 — Fixed table names in a configurable schema, resolved via `search_path`

**Status:** Accepted — 2026-09-04
**SRS:** §7.2, §24, §35.1, §43.A.31, §43.A.35–36
**Decisions:** human decisions 14, 15, 16, 26, 28

## Context

Reliar's tables live in someone else's database. Two things follow: they must not collide with a
host table called `outbox`, and an operator must be able to grant, inspect and drop them as a unit.

The constraint that decides the design is **`sqlx::query!` checks SQL at compile time, so every
identifier in every statement must be a static literal.** A runtime table prefix or a custom table
name would force the runtime string API and cost the compile-time checking that is the entire
reason to use sqlx.

That leaves one degree of freedom to spend: a namespace. And within it, one sub-question that was
reopened twice — is the schema name **compiled in** (fully qualified `FROM reliar.outbox`) or
**resolved at runtime** via `search_path`?

## Decision

- **All Reliar objects live in one schema, `reliar` by default, with unprefixed table names** —
  `outbox` in v0.1, later `inbox` and `idempotency`, plus the bookkeeping table `_migrations`
  (ADR 0018). The `reliar_` table prefix is gone.
- **Every Reliar statement uses unqualified identifiers** (`FROM outbox`), so the schema name is
  **not compiled into the crate**. `cargo sqlx prepare` runs against a database whose `search_path`
  already carries the schema.
- **The host puts Reliar's schema first on `search_path`** — normally in the connection URL
  (`?options=-c%20search_path%3Dreliar,public`). **First**, so an unqualified name can never be
  shadowed by a host table of the same name in `public`.
- **Pooler fallback is documented, not incidental.** PgBouncer and PgDog in transaction mode may
  not pass startup `options` through, so the URL parameter never reaches the server. The supported
  alternative is a server-side default — `ALTER ROLE <app> SET search_path = reliar, public` —
  which every pooler mode honours because the server applies it at authentication.
- **Reliar sets `search_path` on any pool it owns** (`PgConnectOptions::options`) — the migration
  path, examples, tests — so those never depend on the host's URL.
- **Startup verification, fail fast.** `PostgresOutboxStore::new` verifies **once at construction**
  that the unqualified name `outbox` resolves to the configured schema (`to_regclass` joined to
  `pg_class.relnamespace`). An unresolvable name or a mismatch is a construction **error** naming
  the configured schema, the observed `search_path`, and the `ALTER ROLE` remedy — never a surprise
  failure on the first `acquire`. When a same-named table also exists in another schema on the
  path, Reliar logs a warning naming both.
- **`enqueue` performs no `set_config` by default.** For hosts that can change neither the URL nor
  the role, `PostgresOutboxSettings.enqueue_sets_search_path` (default `false`) opts into wrapping
  `enqueue` in `set_config('search_path', $1, true)` plus a restore that leaves the caller's
  transaction as it was found. It costs a statement per enqueue, which is why it is opt-in.
- **`migrate(&pool, MigrateOptions { schema })` creates the schema** and places both the data
  tables and `_migrations` in it. `PostgresOutboxSettings.schema` and `MigrateOptions.schema`
  default to the same `"reliar"` and SHALL agree or construction fails.
- **Non-goals, documented:** a runtime table prefix, custom table names, and a per-call schema
  override. Revisit only if a runtime-SQL provider variant is ever built.

## Consequences

- `\dt reliar.*` is the complete inventory; a grant or `DROP SCHEMA reliar CASCADE` is one
  operation; nothing of Reliar's lands in the host's `public` schema.
- The cost is a **deployment obligation**: get `search_path` wrong and the store refuses to
  construct. That is the point — a fail-fast construction error beats reading a host's unrelated
  `outbox` table. The error text carries the remedy.
- Because table resolution now depends on `search_path`, **the provider suite must test through a
  pooler** (decision 28): PgBouncer transaction mode and PgDog as testcontainers generic images,
  asserting `ALTER ROLE` resolution, the full enqueue → claim → publish → purge path, concurrent
  `SKIP LOCKED` claims and lease updates, graceful `LISTEN/NOTIFY` degradation, and the fail-fast
  error where the pooler drops URL `options` (§43.A.35, ADR 0021).
- Two schemas cannot be served by one store instance in one process, since the path is a connection
  property. Multi-tenant-by-schema is not supported and is stated as such.
- `migrate` is self-contained and does **not** depend on the caller's `search_path` (ADR 0018), so
  the store constructor is the single place a missing path is caught.

## Alternatives considered

- **Fully qualified SQL with a fixed schema name** (`FROM reliar.outbox`). Withdrawn by decision 26:
  it hard-codes the namespace into the published crate, so a host that cannot use the name `reliar`
  — a shared database, a naming policy — has no path at all.
- **Runtime table prefix / custom table names** (decisions 14, 16). Rejected: requires the runtime
  string API and loses compile-time checking.
- **`set_config` on every `enqueue`.** Rejected as a default: a statement per enqueue on the
  application's hot write path, to fix a deployment problem. Kept as an opt-in escape hatch.
- **Leave Reliar's tables in `public` with a `reliar_` prefix** (v1.0's shape). Rejected: no single
  grant/drop unit, and a prefix collides just as easily as a name.
