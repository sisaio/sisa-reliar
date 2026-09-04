# ADR 0021 — testcontainers is the only integration substrate, and pooler scenarios are part of it

**Status:** Accepted — 2026-09-04
**SRS:** §8, §8.1, §8.2, §25.1, §41, §43.A.33, §43.A.35
**Decisions:** human decisions 24, 28

## Context

Fakes prove dispatcher logic. They cannot prove the SQL — `SKIP LOCKED` disjointness, `now()`-based
lease expiry, guarded batch updates, index usage, or the migrations themselves. Nor can they move
Postgres's clock, which is what lease and backoff timing tests need.

The usual shortcut is a shared development database. It is also how integration suites start passing
for the wrong reason: a test that can see another run's rows proves nothing about `SKIP LOCKED`,
and cross-test flakiness gets debugged forever instead of designed out.

ADR 0017 adds a second reason to be strict: table resolution now depends on `search_path`, and
`search_path` is exactly what a transaction-mode connection pooler is most likely to break.

## Decision

- **Every provider crate's integration tests run against a real, ephemeral server.**
  `testcontainers-rs` starts **one PostgreSQL container per test binary**, kept alive for the
  binary's lifetime and torn down with it.
- **Each test gets its own database**, created from a migrated **template**, so tests in a binary run
  in parallel without sharing rows, sequences or advisory locks.
- **The schema is created only through the crate's public `migrate()`.** A hand-written
  `CREATE TABLE` in a fixture is **forbidden**: it would let the migrations rot undetected, and it
  would mean the migration path is not under test on every run.
- **CI may substitute a service container**: when `DATABASE_URL` is set the harness uses it as the
  admin connection and starts no container; on a developer's machine the same code spins its own.
  One harness, two environments.
- **Tests SHALL NOT run against a shared or long-lived development database** — not the §41 compose
  Postgres, not a team database, not a "test" schema on staging. The compose stack exists for
  examples, guides and manual exploration.
- **Timing is never `sleep`.** Dispatcher timing uses `#[tokio::test(start_paused = true)]` +
  `tokio::time::advance`; database timing uses **SQL time-travel**
  (`UPDATE … SET locked_until = now() - interval …`). Assertions are on **database state plus
  recorded publisher observations**, not just `Ok`.
- **Pooler scenarios are part of the suite** (decision 28). **PgBouncer** (transaction mode) and
  **PgDog**, each a testcontainers *generic image* in front of the Postgres container, assert:
  `ALTER ROLE … SET search_path` resolves through the pooler; the full enqueue → claim → publish →
  purge path works over it; `FOR UPDATE SKIP LOCKED` claims and lease updates behave (one statement,
  no session state); `LISTEN/NOTIFY` degrades to polling rather than failing when enabled; and where
  the pooler drops the URL `options`, `PostgresOutboxStore::new` **fails fast** with the documented
  error rather than silently reading a table in the wrong schema.
- **Shared fakes ship from `reliar-outbox` behind a `test-support` feature** — `InMemoryOutboxStore`,
  `RecordingPublisher`, `ScriptedPublisher`, a controllable clock — so provider crates, examples and
  `tests/system` reuse one set instead of writing three.
- **No inline `#[cfg(test)]` under any `src/`.** Tests live in each crate's `tests/`, against the
  public API. Pure logic that must be tested is reachable publicly or behind `test-support`.
- Every `tests/common/mod.rs` **SHALL begin with `#![allow(dead_code)]`** — each file in `tests/`
  compiles as its own binary, so shared helpers a given binary does not call would otherwise fail
  `clippy --all-targets -- -D warnings`. A standing convention, not something each crate
  rediscovers.

## Consequences

- Integration tests are slow (container startup) and require Docker locally. That is the price of
  proving the SQL, and per-binary containers plus per-test template databases keep it bounded.
- CI can run without Docker-in-Docker by providing a service container and `DATABASE_URL`, and the
  matrix covers both the PG floor (18) and the newest major (§43.A.34).
- Pooler tests add two more images and real setup complexity. They are justified because ADR 0017
  makes `search_path` a correctness dependency and the pooler is where it breaks.
- Fakes shipped in a public feature become a maintained surface: a change to `OutboxStore` must be
  reflected in `InMemoryOutboxStore`. That is a feature — the fake is a second implementation that
  keeps the trait honest.
- Fakes must be careful not to hold a `std::sync::MutexGuard` across an `.await`, which would
  silently break the `+ Send` bound on the trait's returned future.

## Alternatives considered

- **A shared dev/CI database with per-test schemas.** Rejected: shared sequences and advisory locks
  make concurrency tests meaningless, and cleanup failures poison later runs.
- **`sqlx::test`'s built-in database-per-test.** Not sufficient alone: it does not give an ephemeral
  server, a controlled server version matrix, or a pooler in front. It may be used inside the
  harness where convenient.
- **Mock the database.** Rejected: the entire point is that only a real server proves `SKIP LOCKED`
  and `now()` semantics.
- **Skip pooler tests and document the risk.** Rejected: decision 28, and the failure mode
  (silently reading the wrong schema) is exactly what fail-fast construction exists to prevent.
