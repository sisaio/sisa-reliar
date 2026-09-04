---
name: testcontainers
description: >-
  Reliar's real-database integration layer for provider crates - testcontainers-rs tests in
  crates/reliar-store-postgres/tests/ that boot one ephemeral Postgres per test binary (or reuse
  DATABASE_URL in CI), create an isolated database per test from a migrated template, run the
  crate's explicit migrate() API, and prove the DB-truth that fakes cannot - atomic
  enqueue-in-transaction, concurrent FOR UPDATE SKIP LOCKED acquire, lease expiry + recovery via SQL
  time-travel, the crash-after-publish duplicate window, retry/backoff timing on DB now(), dead
  state, purge/retention, and envelope round-trip. Use when writing or reviewing any
  Postgres-backed test or the container harness.
metadata:
  audience: ENGINEER, REVIEWER, ARCHITECT
---

# Testcontainers (real ephemeral Postgres)

Fakes prove dispatcher logic; **only a real Postgres proves the SQL** — `SKIP LOCKED` behavior,
`now()`-based leases, batch updates, indexes, migrations. These tests live in the provider crate's
`tests/` dir (SRS §8) and are the evidence for SRS §43 items 1, 5, 8–15, 17, 24.

## Dev-deps

```toml
[dev-dependencies]
testcontainers = "0.2x"                                   # pin to the workspace lockfile
testcontainers-modules = { version = "0.1x", features = ["postgres"] }
tokio = { workspace = true, features = ["rt-multi-thread", "macros", "time"] }
```

## Harness — `tests/common/postgres.rs`

```rust
#![allow(dead_code)]
use std::sync::OnceLock; use tokio::sync::OnceCell;
use testcontainers::{runners::AsyncRunner, ContainerAsync};
use testcontainers_modules::postgres::Postgres;

static ADMIN_URL: OnceCell<String> = OnceCell::const_new();
static CONTAINER: OnceLock<ContainerAsync<Postgres>> = OnceLock::new();   // kept alive for the binary

/// Admin URL: `DATABASE_URL` when set (CI service container), else one container per test binary.
async fn admin_url() -> &'static str {
    ADMIN_URL.get_or_init(|| async {
        if let Ok(url) = std::env::var("DATABASE_URL") { return url; }
        let c = Postgres::default().with_tag("18-alpine").start().await.expect("postgres container");
        let port = c.get_host_port_ipv4(5432).await.expect("port");
        let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
        let _ = CONTAINER.set(c);
        url
    }).await
}

/// A fresh, migrated database for ONE test (tests in a binary run in parallel).
pub async fn fresh_db() -> sqlx::PgPool {
    let admin = sqlx::PgPool::connect(admin_url().await).await.unwrap();
    let name = format!("t_{}", uuid::Uuid::now_v7().simple());
    sqlx::query(&format!(r#"CREATE DATABASE "{name}""#)).execute(&admin).await.unwrap();
    let url = admin_url().await.rsplit_once('/').map(|(base, _)| format!("{base}/{name}")).unwrap();
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    reliar_store_postgres::migrate(&pool).await.unwrap();          // the explicit API under test (SRS §43.24)
    pool
}
```

`CREATE DATABASE` cannot take bind parameters — the name is generated, never user input, so the
`format!` here is the one sanctioned exception to "macros only" (it's test code, not the crate).
If per-test databases prove slow, migrate once into a template and `CREATE DATABASE … TEMPLATE`.

## SQL time-travel — how to test leases without waiting

Lease decisions use DB `now()`, so expire a lease by moving it into the past:

```rust
sqlx::query("UPDATE outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
    .bind(id).execute(&pool).await?;
```

Likewise `available_at = now() - interval '1 hour'` makes a backoff-delayed row due. Never
`tokio::time::sleep` against a real database.

## The scenario matrix (one file per scenario)

| File | Proves (SRS §43) | Shape |
|---|---|---|
| `outbox_enqueue_atomic.rs` | 1 | enqueue inside a tx that also writes a business row; rollback ⇒ neither exists; commit ⇒ both |
| `outbox_roundtrip.rs` | 5, 6, 7 | enqueue `Envelope<T>` → acquire → `SerializedEnvelope` equals the original; metadata not duplicated in `headers` |
| `outbox_acquire_skip_locked.rs` | 8, 9 | two workers acquire concurrently (`tokio::join!`) ⇒ disjoint id sets, union = all pending; rows carry `locked_by` |
| `outbox_lease_recovery.rs` | 10, 14 | worker A acquires, never completes; time-travel lease; worker B acquires the same row; A's late `complete` affects 0 rows |
| `outbox_duplicate_window.rs` | 11 | acquire → publisher records publish → *no* complete → lease expires → second acquire republishes ⇒ publisher saw the id twice; documented, asserted |
| `outbox_retry_dead.rs` | 12, 13, 15 | `fail` with delay ⇒ `attempts+1`, `available_at` in the future, not claimable until time-travel; permanent ⇒ `dead_at` set, never claimable; completed rows never returned again |
| `outbox_purge.rs` | retention | published rows older than retention removed in bounded batches; dead/pending rows untouched |
| `migrate.rs` | 24 | `migrate()` is idempotent (twice ⇒ Ok); a plain pool without `migrate()` has no `outbox` |

Assert **DB state** (`SELECT attempts, available_at > now(), locked_by …`) and **publisher
observations** (from the in-memory publisher in `reliar-outbox`'s test support), not just `Ok(())`.

## Container hygiene — nothing survives the test run

- **Every container the suite starts is removed (with its volumes) when the run ends.** A
  `ContainerAsync` in a `static`/`OnceLock` is **never dropped** at process exit, so the container
  leaks; with 26 test binaries that is 26 Postgres containers per `cargo test`. Rules:
  1. **One test binary per crate for Postgres scenarios** (`tests/postgres/main.rs` + `mod` files,
     one claim per module) so a run starts **one** container, not one per file.
  2. **testcontainers-rs has no reaper (no Ryuk).** Removal happens only in `ContainerAsync::Drop`,
     so the container must be owned by a **local** that is dropped: a `harness = false` binary
     (`tests/postgres/main.rs`, `libtest-mimic`) where `main` owns the container, runs the trials,
     drops it, then exits with the code — **never `Conclusion::exit()`** (it calls `process::exit`
     and skips destructors). Enable the crate's `watchdog` feature so SIGINT/SIGTERM still remove it.
     `TESTCONTAINERS_COMMAND=keep`, `reusable-containers`/`.with_reuse` are forbidden in CI.
  3. `scripts/test.sh` ends with a sweep as the *third* line of defence only: `docker ps -aq --filter label=org.testcontainers.managed-by=testcontainers | xargs -r docker rm -f -v`.
  4. Child processes (`run_scenario_in_child`-style env tests) receive the parent's `DATABASE_URL`, never start their own container; pooler containers are locals in their trial.
  5. After any local suite run, `docker ps -a` shows no `postgres:18-alpine`/pgbouncer/pgdog leftovers
     and `docker volume ls -q | wc -l` did not grow. Put this check in the card Log when touching the harness.
- Templates/databases live inside the container and vanish with it; never use a long-lived local DB.

## CI

`test.yaml` runs a `postgres:18` service container and exports `DATABASE_URL`; the harness then skips
Docker-in-Docker. Locally with Docker Desktop the same tests spin their own container. Pooler scenarios always start their own PgBouncer/PgDog container (`testcontainers::GenericImage`) pointed at the Postgres under test. Keep the
`sqlx prepare --check` job on the same freshly migrated database.

## Definition of done (real-DB tests)

- [ ] Every row of the matrix that the story touches exists as its own `tests/<scenario>.rs`.
- [ ] Each test uses `fresh_db()` (isolated database) and the crate's public `migrate()`.
- [ ] Lease/backoff tests use SQL time-travel, never sleeps; assertions read DB state.
- [ ] Concurrency tests run real concurrent acquires and assert disjointness + completeness.
- [ ] The duplicate window is asserted explicitly (not just tolerated).
- [ ] One Postgres container per `cargo test -p` run; `docker ps -a` clean after the suite; volumes removed.
