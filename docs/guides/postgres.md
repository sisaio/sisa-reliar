# PostgreSQL provider guide

`reliar-store-postgres` is Reliar's PostgreSQL provider: the schema, the explicit `migrate()` API,
and `PostgresOutboxStore`. It is the only crate in the workspace that names an `sqlx`/Postgres
type. Requires **PostgreSQL 18 or later**.

See `crates/reliar-store-postgres/README.md` for the crate's own quickstart, and
`docs/architecture/phase1-contract.md` §4 for the frozen signatures this guide describes.

## `search_path` setup

Every Reliar object lives in **one configurable schema — `reliar` by default** — with unprefixed
table names (`outbox`). `sqlx::query!` checks SQL at compile time, so every identifier in every
statement is a static, unqualified literal; the schema is resolved at *connection* time through
`search_path`, never compiled in (ADR 0017).

**Direct connection — put `reliar` first on the URL:**

```text
postgres://user:pw@host/app?options=-c%20search_path%3Dreliar,public
```

**Behind a transaction-mode pooler (PgBouncer, PgDog)** that drops startup `options`, set a
server-side default instead — every pooler mode honours this:

```sql
ALTER ROLE app SET search_path = reliar, public;
```

**Warning: putting `reliar` first changes where *your own* unqualified DDL/DML lands, on any
connection that shares the same `search_path`.** `CREATE TABLE orders (…)` (or a plain `INSERT
INTO orders …`) executed over a connection whose `search_path` is `reliar, public` creates or
writes to `reliar.orders`, not `public.orders` — Postgres resolves the first schema on the path
that matches, and it does not care which application "owns" the name. Qualify your own tables
explicitly (`public.orders`, or whatever schema they actually live in) on any connection pool that
also talks to Reliar's schema; `examples/axum-outbox` does this throughout. This is not a Reliar
behavior to configure around — it is how `search_path` always works, and the reason to reach for it
carefully rather than as a blanket default across a whole database.

`PostgresOutboxStore::connect`/`::new`/`::with_settings` all verify, **once at construction**, that the unqualified name
`outbox` resolves to the configured schema and fail fast — naming the configured schema, the
observed `search_path`, and the `ALTER ROLE` remedy — instead of surprise-failing on the first
`acquire`. A same-named table found elsewhere on the path is logged as a `tracing::warn!`. This
check runs whichever way you set `search_path`, so it also validates the pooler path.

A host that cannot change either the URL or the role sets
`PostgresOutboxSettings::enqueue_sets_search_path(true)` instead: `enqueue` wraps its `INSERT` in a
transaction-local `set_config('search_path', …, true)`/restore pair. It costs three extra
statements per `enqueue`, which is why it defaults to `false` — prefer the URL or `ALTER ROLE`
first.

## `migrate()` vs. the release SQL artifact

Migrations are **embedded in the crate** (`crates/reliar-store-postgres/migrations/`) and are the
source of truth. They run **only** through the explicit, idempotent API — never implicitly:

```rust,ignore
reliar_store_postgres::migrate(&pool, reliar_store_postgres::MigrateOptions::default()).await?;
```

`MigrateOptions::default().schema` is `"reliar"`, matching `PostgresOutboxSettings::schema`'s
default. `migrate()` creates the schema itself and keeps its own bookkeeping in
`<schema>._migrations` (never the shared `sqlx`-managed `_sqlx_migrations` table, ADR 0018), so it
does not depend on the caller's `search_path` and is safe under concurrent callers.

Internally, `migrate()` opens its own **direct** connection from the pool's connect options
(never a connection borrowed from the pool itself) and serializes concurrent callers with a
**session-level** advisory lock held on that connection for the duration of the migration. Point
it at a direct or session-mode endpoint — never a transaction-mode pooler port (e.g. `PgBouncer`'s
transaction-mode listener) — since a transaction-mode pooler can hand that session's statements to
different server connections mid-migration, breaking both the advisory lock and the connection's
own `search_path`.

There is also a small CLI for the third case — a host that wants neither an in-process
`migrate()` call nor a DBA pipeline:

```bash
DATABASE_URL=postgres://user:pw@host/db cargo run -p reliar-migrate
RELIAR_SCHEMA=tenant_a DATABASE_URL=... cargo run -p reliar-migrate   # non-default schema
```

`tools/reliar-migrate` is a `publish = false` workspace binary that does nothing but call
`migrate()` with `MigrateOptions::default()`. It is the same code path as the snippet above, so it
cannot drift from it, and it is what CI runs to create the schema before the provider's tests and
`cargo sqlx prepare --check`.

Every release also publishes the same `.sql` files as a **standalone artifact**
(`reliar-store-postgres-migrations-<version>.tar.gz`, with a `SHA256SUMS` manifest) for a team that
applies schema changes through its own DBA pipeline rather than calling `migrate()` from the
running application. Both paths apply the identical files — the artifact is a packaging
convenience, not a second migration.

## Settings and environment variables

`PostgresOutboxSettings::from_env("RELIAR_STORE_POSTGRES_")` is opt-in — nothing in this crate
reads the environment implicitly (ADR 0019).

| Field | Env var | Default | Notes |
|---|---|---|---|
| `schema` | `RELIAR_STORE_POSTGRES_SCHEMA` | `reliar` | Must match `MigrateOptions::schema`. |
| `enqueue_sets_search_path` | `RELIAR_STORE_POSTGRES_ENQUEUE_SETS_SEARCH_PATH` | `false` | Transaction-local `SET LOCAL` semantics; three extra statements per `enqueue`. |
| `statement_timeout` | `RELIAR_STORE_POSTGRES_STATEMENT_TIMEOUT_MS` | `0` (inherit) | Applies to every statement Reliar issues on its own pool (`acquire`, `complete`, `fail`, `release`, `extend_lease`, `stats`, `purge`, `list_dead`, `retry_dead`, `purge_dead`), never the caller's `enqueue`. |

The portable dispatcher/retention knobs (`OutboxSettings::from_env("RELIAR_OUTBOX_")`) are
documented in `crates/reliar-outbox/README.md`.

## Dead-letter operations

`PostgresOutboxStore` also implements `OutboxDeadLetters` — an operator surface the dispatcher
never calls:

```rust,ignore
let page = store.list_dead(DeadQuery::default().limit(100)).await?;   // ORDER BY sequence
store.retry_dead(&refs).await?;   // clears dead_at, available_at = now(), attempts reset to 0
store.purge_dead(&refs).await?;   // deletes, no lease guard needed — a dead row holds none
```

`list_dead` paginates by the `sequence` keyset (`DeadQuery::after_sequence`) and filters by
`message_type`/`tenant_id`/`dead_before`. `retry_dead` is the **only** operation in the system that
resets `attempts`, and is always an explicit operator action.

## Retention: the host's own periodic task

Reliar starts no maintenance timer. Retention is the host's, because it writes:

```rust,ignore
loop {
    let report = store.purge(PurgeRequest::default()).await?;
    if report.is_complete(PurgeRequest::default().batch_size) {
        break;   // this pass drained the published delete, the dead delete, and the expiry sweep
    }
    // otherwise at least one of the three statements was cut short at batch_size — loop again
}
```

One call is **one bounded pass**: at most `batch_size` published rows deleted, at most
`batch_size` dead rows deleted, and at most `batch_size` expired-but-unclaimed pending rows moved
to `dead` (`DeadReason::Expired`). Call it on your own periodic task (a cron, a
`tokio::time::interval`) — Reliar never schedules it for you.

**`PurgeRequest::default().dead_retention` is `None`**, meaning "keep dead rows until an explicit
purge" — so `store.purge(PurgeRequest::default())` above deletes **zero** dead rows on its own; it
still deletes published rows past `published_retention` (7 days by default) and sweeps expired
pending rows to dead. Set `dead_retention` (a builder method, or
`RELIAR_OUTBOX_DEAD_RETENTION_MS`) explicitly once you have decided how long a dead row should
stay available for `retry_dead`/operator inspection before it is gone for good.

## `statement_timeout`

`PostgresOutboxSettings::statement_timeout` defaults to `Duration::ZERO` — "issue nothing, inherit
the server/role setting." It is applied as `SET LOCAL statement_timeout` **only** inside the short
transactions Reliar opens for its own `OutboxStore`/`OutboxDeadLetters` calls (`acquire`,
`complete`, `fail`, `release`, `extend_lease`, `stats`, and the dead-letter operations) — never to
the caller's `enqueue` transaction, which Reliar does not own. Most deployments should instead set
a server-side `statement_timeout` on the role; the per-call override exists for hosts that cannot.
A timeout here classifies as a **transient** `PostgresStoreError::Database` (SQLSTATE `57014`), so
the dispatcher retries rather than dead-lettering a row over a slow statement.

## Contributing: running the Postgres test suite

`cargo test -p reliar-store-postgres --all-features` starts **exactly one** Postgres container
for the whole run (`tests/postgres/main.rs`, a `harness = false` binary built on `libtest-mimic`:
`main` owns the container as a local, runs every scenario, then drops it before exiting) — plus,
only inside their own scenario, one `PgBouncer` and one `PgDog` container each. Every container
and its volumes are removed by the time the process exits, on success, on a panicking test, and on
`SIGINT`/`SIGTERM`/`SIGQUIT` (RELIAR-27). Three layers make that true, in order:

1. **`Drop`.** `testcontainers` 0.27 has no reaper (no Ryuk) — `ContainerAsync::Drop` is the
   *only* removal mechanism. This is why the container is a local `main` explicitly holds and
   drops, never a `static` (a `static`'s destructor never runs at process exit, which is exactly
   how this leaked 167 containers / 31 GB of volumes before this fix).
2. **The `watchdog` dev-dependency feature.** Removes registered containers on
   `SIGINT`/`SIGTERM`/`SIGQUIT` — the one case `Drop` alone cannot cover, since a killed process
   runs no destructors either.
3. **`scripts/test.sh`'s label-scoped sweep**, in a `trap … EXIT` so it runs regardless of the
   suite's outcome: `docker ps -aq --filter label=org.testcontainers.managed-by=testcontainers |
   xargs -r docker rm -f -v`. This is the *third* line of defence, never the first — a sweep that
   is relied on hides a harness bug.

**`TESTCONTAINERS_COMMAND=keep` and the `reusable-containers` feature / `.with_reuse(..)` are
forbidden in CI**, and are a local debugging aid only. Both defeat `Drop`-based removal, which is
the entire removal mechanism above `watchdog` and the sweep. CI asserts `TESTCONTAINERS_COMMAND`
is unset or `remove`.

## See also

- `examples/axum-outbox/src/main.rs` — the full reference integration (SRS §20.1): an Axum handler
  writing a business row and an outbox row in one transaction, `migrate()` behind an explicit
  `--migrate` flag, and a dispatcher whose `CancellationToken` is tied to the server's graceful
  shutdown.
- `docs/architecture/outbox.md` — the claim/publish/complete loop and the duplicate windows.
- ADR 0017 (`search_path`), ADR 0018 (migrations), ADR 0025 (provider crate MSRV).
