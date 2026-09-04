---
name: sqlx-postgres
description: How Reliar's PostgreSQL provider crate (reliar-store-postgres) uses sqlx — compile-time macros only (query!/query_as!/query_scalar!, never the runtime string API, never FromRow), functions over `impl PgExecutor<'_>` plus a transactional `enqueue(&mut Transaction<Postgres>, …)`, the single-statement FOR UPDATE SKIP LOCKED claim with DB-authoritative `now()` leases, locked_by-guarded batch complete/fail via `= ANY`/UNNEST, retention purge, partial indexes derived from the real claim/purge queries, migrations embedded in the crate and run only through an explicit `migrate(&pool)` API (with the shared `_sqlx_migrations` table gotcha), the committed per-crate `.sqlx/` offline cache, and optional LISTEN/NOTIFY wake-up. Use when writing or reviewing any query, schema, migration, transaction boundary, index, or `.sqlx/` change.
metadata:
  audience: ENGINEER, ARCHITECT, REVIEWER
---

# sqlx + PostgreSQL (provider crate)

`reliar-store-postgres` is the **only** place SQLx/Postgres types appear. It implements the small
capability traits from `reliar-outbox`/`reliar-inbox` and owns its schema. Everything here follows
SRS §20–§26 and §35.

## Non-negotiables

- **Macros only**: `sqlx::query!`, `query_as!`, `query_scalar!` — compile-time checked against the
  crate's `.sqlx/` cache. Never `sqlx::query(&string)`, never `FromRow`.
- **DB-authoritative time**: every lease/due/expiry comparison is `now()` in SQL; durations are
  bound as milliseconds and turned into intervals in SQL. `timestamptz` everywhere.
- **Short claim transaction, no network I/O inside it.** The claim is one statement (auto-commit).
- **Guard every status update with `locked_by = $worker`** so a stale worker can't clobber a row
  another worker reclaimed after lease expiry.
- **Explicit migrations**: `reliar_store_postgres::migrate(&pool)`; never at construction time.

## Identifiers: fixed names, configurable schema, deterministic constraint names

- `query!` needs **static SQL**, so identifiers are **unqualified and unprefixed** (`outbox`, later
  `inbox`, bookkeeping `_migrations`) and the **schema is a setting** (`PostgresOutboxSettings.schema`,
  default `reliar`) resolved through **`search_path`**. The **host** puts the schema **first** in its
  DB connection URL — `postgres://…/app?options=-c%20search_path%3Dreliar,public` — so the caller's
  transaction used by `enqueue` resolves `outbox` with no extra statements. **DevOps note for poolers**
  (any transaction-mode pooler that drops startup `options`): `ALTER ROLE app SET search_path =
  reliar, public` instead. Reliar-owned pools set it themselves
  (`PgConnectOptions::options([("search_path", "reliar,public")])`), and `PostgresOutboxStore::new`
  **verifies at startup** that `to_regclass('outbox')` resolves to the configured schema (fail fast;
  warn when a same-named table exists in another schema). `enqueue_sets_search_path` (default
  **false**) opts into a `set_config('search_path', $1, true)` + restore wrap for hosts that can change
  neither. The `cargo sqlx prepare` database has `reliar` on its `search_path`. Table names are not configurable.
- **Every constraint and index is named explicitly** so regeneration is byte-identical and the
  `errors.rs` map can key on the name: `pk_<table>`, `fk_<table>_<ref>`, `ck_<table>_<rule>`, and
  **`ix_<table>_<cols>` for every index, unique or not** (`CREATE UNIQUE INDEX ix_…`). Never rely on
  Postgres auto-names.
- **PostgreSQL 18+** is the minimum: `id uuid NOT NULL DEFAULT uuidv7()`; Reliar still generates v7
  ids client-side so `enqueue` can return the id and callers can set causation.
- **Partitioning** (special tables, outbox first): if the table is range-partitioned by `created_at`,
  the PK becomes `pk_outbox (created_at, id)`, retention becomes `DROP PARTITION`, and an
  `ensure_partitions()` maintenance op creates future partitions. Designed now, shipped as an opt-in
  migration variant when the architect approves (see SRS).
- **Settings**: `PostgresOutboxSettings { schema: "reliar", enqueue_sets_search_path: false, statement_timeout, … }` with
  `Default`, builder methods, `serde` behind a feature, and an opt-in `from_env("RELIAR_STORE_POSTGRES_")`.

## Cargo

```toml
sqlx = { workspace = true, features = ["runtime-tokio", "tls-rustls", "postgres", "uuid", "time", "json", "migrate"] }
```

## Layout

```
crates/reliar-store-postgres/
├── migrations/0001_outbox.sql          # forward-only; one concern per file
├── src/lib.rs                          # PostgresOutboxStore, migrate(), re-exports
├── src/outbox.rs                       # enqueue / acquire / complete / fail / purge
├── src/records.rs                      # OutboxRow ↔ OutboxRecord/SerializedEnvelope mapping
├── src/error.rs                        # PostgresStoreError (hand-rolled, From<sqlx::Error>)
├── .sqlx/                              # committed offline metadata
└── tests/…                             # real-Postgres tests (skill `testcontainers`)
```

## Schema (SRS §24) and the indexes it actually needs

```sql
-- run by migrate() with search_path = <schema>; the schema itself is created first
CREATE TABLE outbox (
    id uuid NOT NULL DEFAULT uuidv7(), sequence bigint GENERATED ALWAYS AS IDENTITY, message_type text NOT NULL, message_version integer NOT NULL,
    correlation_id text, conversation_id uuid NOT NULL, causation_id uuid, request_id uuid,
    content_type text NOT NULL, payload bytea NOT NULL,
    metadata jsonb, headers jsonb,
    created_at timestamptz NOT NULL DEFAULT now(), available_at timestamptz NOT NULL DEFAULT now(),
    attempts integer NOT NULL DEFAULT 0, locked_by text, locked_until timestamptz,
    published_at timestamptz, dead_at timestamptz, last_error text
);
ALTER TABLE outbox ADD CONSTRAINT pk_outbox PRIMARY KEY (id);   -- or declare CONSTRAINT pk_… inline
ALTER TABLE outbox ADD CONSTRAINT ck_outbox_attempts CHECK (attempts >= 0);
-- serves the claim query: pending rows ordered by due time
CREATE INDEX ix_outbox_pending ON outbox (available_at, sequence)
    WHERE published_at IS NULL AND dead_at IS NULL;
-- serves retention purge
CREATE INDEX ix_outbox_published ON outbox (published_at) WHERE published_at IS NOT NULL;
```

Promoted columns are **not** repeated inside `metadata` (SRS §24 rule); `metadata` holds only the
non-promoted parts (trace, routing, delivery minus `content_type`, tenant). Add an index only when a
query needs it and show the `EXPLAIN (ANALYZE, BUFFERS)` on a seeded table in the card.

## Enqueue — inside the caller's transaction (SRS §20)

```rust
pub async fn enqueue<T: Message>(&self, tx: &mut Transaction<'_, Postgres>, envelope: &Envelope<T>)
    -> Result<MessageId, PostgresStoreError> {
    let serialized = self.serializer.serialize(&envelope.body)?;          // Bytes
    sqlx::query!(
        r#"INSERT INTO outbox (id, message_type, message_version, correlation_id, conversation_id,
             causation_id, request_id, content_type, payload, metadata, headers, available_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11, now())"#,
        envelope.id.as_uuid(), T::TYPE, i32::from(T::VERSION), corr.correlation_id.as_deref(), …,
        &serialized[..], metadata_json, headers_json)
    .execute(&mut **tx).await?;
    Ok(envelope.id)
}
```

Taking `&mut Transaction` (not `impl PgExecutor`) makes the atomicity requirement visible in the
signature. Pass `&mut **tx` to sqlx.

## Acquire — one statement, SKIP LOCKED, lease in DB time (SRS §21, §25)

```rust
sqlx::query_as!(OutboxRow, r#"
    WITH claimed AS (
        SELECT id FROM outbox
        WHERE published_at IS NULL AND dead_at IS NULL
          AND available_at <= now()
          AND (locked_until IS NULL OR locked_until < now())
        ORDER BY available_at
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    )
    UPDATE outbox o
       SET locked_by = $2, locked_until = now() + ($3::bigint * interval '1 millisecond')
      FROM claimed WHERE o.id = claimed.id
    RETURNING o.id, o.message_type, o.message_version, o.correlation_id, o.conversation_id, o.causation_id,
              o.request_id, o.content_type, o.payload, o.metadata, o.headers, o.attempts, o.available_at,
              o.locked_by, o.locked_until, o.published_at, o.dead_at, o.last_error"#,
    i64::from(batch_size), worker.as_str(), lease_ms).fetch_all(&self.pool).await?
```

Single statement ⇒ implicit transaction ⇒ the lock is released before the function returns.
`ORDER BY available_at` gives approximate FIFO per worker; **cross-worker ordering is not guaranteed** — say so in rustdoc.

## Complete / fail / dead — batched, guarded (SRS §23)

```rust
// complete
sqlx::query!("UPDATE outbox SET published_at = now(), locked_by = NULL, locked_until = NULL
              WHERE id = ANY($1) AND locked_by = $2", &ids[..], worker.as_str()).execute(&self.pool).await?;
// fail (retry) — per-item backoff computed by the policy, applied relative to DB time
sqlx::query!(r#"UPDATE outbox o
    SET attempts = o.attempts + 1, last_error = f.err, locked_by = NULL, locked_until = NULL,
        available_at = now() + (f.delay_ms * interval '1 millisecond')
    FROM UNNEST($1::uuid[], $2::text[], $3::bigint[]) AS f(id, err, delay_ms)
    WHERE o.id = f.id AND o.locked_by = $4"#, &ids[..], &errs[..], &delays[..], worker.as_str()).execute(&self.pool).await?;
// dead (permanent or attempts exhausted)
sqlx::query!("UPDATE outbox SET attempts = attempts + 1, dead_at = now(), last_error = f.err, locked_by = NULL, locked_until = NULL
              FROM UNNEST($1::uuid[], $2::text[]) AS f(id, err) WHERE outbox.id = f.id AND locked_by = $3", …)
```

Row count < item count means some rows were reclaimed by another worker — log at `debug`, don't error
(at-least-once makes that benign). `attempts` increments **on outcome**, not on claim.

## Purge — retention (SRS §26)

```rust
sqlx::query!("DELETE FROM outbox WHERE id IN (
                SELECT id FROM outbox WHERE published_at < now() - ($1::bigint * interval '1 millisecond') LIMIT $2)",
             retention_ms, i64::from(batch)).execute(&self.pool).await?.rows_affected()
```

Loop until `rows_affected < batch`; never an unbounded `DELETE`. Dead rows are purged separately and
later (they must stay inspectable, SRS §23).

## Records — map at the boundary

`OutboxRow` (sqlx shape: `payload: Vec<u8>`, `metadata: Option<serde_json::Value>`, …) →
`OutboxRecord { envelope: SerializedEnvelope, attempts, available_at, locked_by, … }` in
`records.rs`. Reconstruct `Metadata` from promoted columns **plus** the JSONB remainder; a malformed
row becomes `PostgresStoreError::Corrupt { id }` (→ dead), never a panic.

## Migrations — embedded, explicit, versioned (SRS §35)

```rust
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
/// Applies Reliar's migrations. Never called implicitly.
pub async fn migrate(pool: &PgPool) -> Result<(), sqlx::migrate::MigrateError> { MIGRATOR.run(pool).await }
```

**Shared `_sqlx_migrations` table — solved.** sqlx 0.9's `Migrator` has `dangerous_set_table_name`
and `create_schema`: `migrate()` runs a *clone* of the static `MIGRATOR` with
`create_schema(schema)` + `dangerous_set_table_name(format!("{schema}._migrations"))` +
`set_locking(true)`, so versions/checksums never collide with the host's own sqlx migrations and
concurrent callers serialize.

**Embedded *and* published.** The SQL files inside the crate are the source of truth (they must be
inside the package for `cargo publish` and `sqlx::migrate!`). Each release also uploads the identical
files as `reliar-store-postgres-migrations-<version>.tar.gz` (+ `SHA256SUMS`) for teams that run
migrations through their own pipeline (psql/Flyway); such teams then skip `migrate()` entirely.

Forward-only; never edit an applied file; lock-safe (`CREATE INDEX CONCURRENTLY` can't run inside
the migrator's transaction — put it in its own file with `-- no-transaction` if the migrator supports it,
or document that operators create big indexes out of band).

## Offline cache — `.sqlx/` per crate

```bash
cd crates/reliar-store-postgres
DATABASE_URL=postgres://reliar:reliar@localhost:5432/reliar cargo sqlx prepare -- --all-targets
git add .sqlx   # commit in the same PR as the query change
```

CI: `SQLX_OFFLINE=true` for builds; a job with a fresh migrated Postgres runs `cargo sqlx prepare --check`.

## LISTEN/NOTIFY — optional wake-up only (SRS §26)

Behind a `listen-notify` feature: a trigger (or the `enqueue` path) does `NOTIFY outbox`;
the dispatcher's idle sleep also wakes on `PgListener`. Polling remains the source of truth — a lost
notification only delays delivery by one poll interval.

## Definition of done (a query/schema change)

- [ ] Macros only; `.sqlx/` regenerated and committed; `cargo sqlx prepare --check` passes.
- [ ] Leases/due/expiry use `now()`; complete/fail/dead guarded by `locked_by`; claim is one statement.
- [ ] New query shapes have an index derived from them and an EXPLAIN in the card; every DELETE/claim has `LIMIT`.
- [ ] Migration is a new forward-only file in the crate; runs only via `migrate()`; lock-safe; every PK/FK/index/unique/check is explicitly named (`pk_`/`fk_`/`ck_`; `ix_` for every index).
- [ ] Real-Postgres tests cover the change (skill `testcontainers`).
