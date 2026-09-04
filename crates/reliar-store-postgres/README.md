# reliar-store-postgres

Reliar's PostgreSQL provider for the transactional outbox: the schema, the explicit `migrate()`
API, and `PostgresOutboxStore` — the only crate in the workspace where an `sqlx`/Postgres type
appears (SRS §20–§26, §35).

**MSRV 1.94**, set by `sqlx` 0.9 — six releases above the workspace floor of 1.88. Provider
crates may carry their driver's MSRV; the pure `reliar-core`/`reliar-outbox` crates stay on 1.88
(ADR 0025).

## Guarantees this store honours

`PostgresOutboxStore` implements `reliar-outbox`'s contract, so its two headline guarantees are
`reliar-outbox`'s, not restated differently here — see `../reliar-outbox/README.md` for the full
text. In short:

- **Durable at-least-once publication, never exactly-once.** A consumer built on Reliar must be
  idempotent. Three windows produce a duplicate and all three are unavoidable in this release:
  the **crash window** (a publish reaches the broker, the worker crashes before `complete`
  persists, the lease expires, and another worker republishes), the **slow-batch window** (a
  batch outlives its lease while the worker is still healthily publishing, so a second worker
  reclaims and republishes the tail), and the **drain window** (cancellation drains in-flight
  publishes for at most `drain_timeout`; one still unresolved at the timeout is released rather
  than awaited further, carrying the same duplicate risk, just triggered by shutdown).
- **No ordering by default.** `Ordering::Unordered` (the only value this release supports)
  guarantees nothing about order — not globally, not per `conversation_id`, not per aggregate,
  not even approximately: `acquire`'s `SKIP LOCKED` claim, concurrent publishing, per-message
  backoff and multiple dispatcher instances each reorder freely.

## Features

| Feature | Default | Enables |
|---|---|---|
| `json` | **on** | `PostgresOutboxStore<JsonSerializer>`'s default type parameter and the `new`/`with_settings` convenience constructors (forwards `reliar-core/json`). Not hard-enabled: a deployment supplying its own `Serializer` should not pull in `serde_json`. Under `--no-default-features`, [`PostgresOutboxStore::connect`] is the only constructor. |
| `serde` | off | `serde::Serialize`/`Deserialize` on `PostgresOutboxSettings`, `#[serde(default, deny_unknown_fields)]` so a typo'd config key is a hard error, durations as integer milliseconds (`statement_timeout_ms`). `serde` itself is always a dependency regardless of this feature — it also drives the crate's private `MetadataRest` JSONB contract (ADR 0012), which is not feature-gated. |

Additive, checked with `cargo hack check --feature-powerset`.

## `search_path` setup

Every Reliar object lives in **one configurable schema, `reliar` by default**, with unprefixed
table names (`outbox`). `sqlx::query!` checks SQL at compile time, so every identifier in every
statement is a static, unqualified literal — the schema is resolved at connection time through
`search_path`, never compiled in (ADR 0017).

1. **Put `reliar` first on the connection URL** the host passes to its own pool:

   ```text
   postgres://user:pw@host/app?options=-c%20search_path%3Dreliar,public
   ```

2. **Behind a transaction-mode pooler** — any pooler that drops startup `options` needs a
   server-side default instead, which every pooler mode honours (verify which yours does:
   `PgDog`, the pooler the suite tests against, passes them through):

   ```sql
   ALTER ROLE app SET search_path = reliar, public;
   ```

3. `PostgresOutboxStore::connect`/`new` verify, **once at construction**, that the unqualified
   name `outbox` resolves to the configured schema. An unresolvable name or a mismatch is a
   construction error naming the configured schema, the observed `search_path`, and the `ALTER
   ROLE` remedy — never a surprise failure on the first `acquire`. A same-named table found in
   another schema on the path is logged as a `tracing::warn!`.

4. `migrate()` does **not** depend on the caller's `search_path` — it creates the schema itself
   and sets `search_path` on its own dedicated connection before running the migration files, so
   it works even against a pool whose URL never set one (ADR 0018).

## Usage

```rust,no_run
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let pool = sqlx::PgPool::connect("postgres://...").await?;

reliar_store_postgres::migrate(&pool, reliar_store_postgres::MigrateOptions::default()).await?;

let store = reliar_store_postgres::PostgresOutboxStore::new(pool.clone()).await?;

let mut tx = pool.begin().await?;
// ... write your own business row(s) in the same transaction ...
let envelope = reliar_core::Envelope::builder(/* your Message */ todo!()).build();
store.enqueue(&mut tx, &envelope).await?;
tx.commit().await?;
# Ok(()) }
```

## What this crate ships (S5 + S6)

- `migrations/0001_outbox.sql` — the full v0.1 schema (SRS §24.1), every constraint/index
  explicitly named (`pk_`/`ck_`/`ix_`).
- `migrate(&pool, MigrateOptions)` — isolated bookkeeping in `<schema>._migrations`, never the
  shared `_sqlx_migrations`.
- `PostgresOutboxStore::connect`/`new`/`with_settings` — fail-fast startup `search_path`
  verification.
- `enqueue`/`enqueue_with` — the transactional write path.
- The full `OutboxStore` impl: `acquire` (the single-statement `FOR UPDATE SKIP LOCKED` claim,
  with poisoned-row handling — an undecodable row is moved to dead with
  `DeadReason::Undecodable` and reported, never a panic, the rest of the batch still delivers),
  worker-guarded `complete`/`fail`/`release`/`extend_lease`, bounded `purge` (one pass, three
  `LIMIT`-capped statements), and `stats`.
- The full `OutboxDeadLetters` impl: `list_dead` (keyset-paginated, `ORDER BY sequence`),
  `retry_dead`, `purge_dead`.
- Per-variant `Classify` for `PostgresStoreError`/`EnqueueError`, including SQLSTATE-class-based
  classification of `Database` errors and `42P01` → `NotMigrated` on every operational path.

## Testing

Real-Postgres integration tests live in `tests/` (skill `testcontainers`): one ephemeral
`postgres:18-alpine` container per test binary, or `DATABASE_URL` when set (CI's service
container), with one isolated database per test, migrated via the crate's own public `migrate()`.
Run with Docker available:

```sh
cargo test -p reliar-store-postgres --all-features
```

## `.sqlx/` offline cache

```sh
cd crates/reliar-store-postgres
DATABASE_URL=postgres://user:pw@localhost/db?options=-c%20search_path%3Dreliar,public \
  cargo sqlx prepare -- --all-targets --all-features
git add .sqlx
```

CI builds with `SQLX_OFFLINE=true` and runs `cargo sqlx prepare --check` against a freshly
migrated database.
