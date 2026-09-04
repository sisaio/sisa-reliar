# ADR 0002 — PostgreSQL is the first and only v0.1 provider

**Status:** Accepted — 2026-09-04
**SRS:** §2, §4, §5, §24, §34, §42

## Context

A reliability toolkit is only useful with a durable store. Building several providers at once
would prove portability early, but it would also mean designing the trait against two half-built
implementations and shipping neither well. Building none and abstracting "storage" in the
abstract is worse: an abstraction with one imaginary implementation is a guess.

The outbox pattern also does not need a broker to be correct (§4). Claim, lease, retry, dead
state and the duplicate window are all observable with an in-memory publisher, so the first
release can prove the entire reliability model against a real database and a fake transport.

## Decision

- **v0.1 ships exactly one store provider: `reliar-store-postgres`**, and no transport provider.
  `reliar-inbox`, `-idempotency`, `-cache`, `-store-mysql`, `-transport-*` are not created in
  Phase 1 (§6: crates are created when their implementation begins).
- The portable seam is drawn where it is actually needed: `OutboxStore` (the dispatcher's side)
  is provider-agnostic; `enqueue` is a provider-inherent method on
  `PostgresOutboxStore` taking `&mut sqlx::Transaction<'_, Postgres>` (§19.6, ADR 0008).
- `reliar-core` and `reliar-outbox` SHALL NOT depend on sqlx, postgres, or any broker client;
  CI gates this with `cargo tree` (§43.B).
- Provider-specific capability (schema resolution, partitioning, `LISTEN/NOTIFY`) lives in the
  provider crate and is configured through `PostgresOutboxSettings`, never leaked into the traits.
- PostgreSQL was chosen over MySQL/SQLite because `FOR UPDATE SKIP LOCKED`, `jsonb`, `timestamptz`,
  declarative partitioning and advisory locks are all first-class, and because it is what the
  target integration (Axum + sqlx, §20.1) already runs.

## Consequences

- The `OutboxStore` trait is designed against one real implementation, so its shape is honest
  rather than speculative — and it is validated by the fakes, which must also satisfy it (§8.1).
- Portability claims stay modest: v0.1 says "the dispatcher is provider-agnostic", not "Reliar
  supports any database". A second provider (Phase 6) is the real test, and it may force an
  additive trait change; `#[non_exhaustive]` and default methods keep that non-breaking.
- Postgres semantics can leak into the trait's *documented* semantics without leaking into its
  types — e.g. "the claim must commit before `acquire` returns". That is deliberate: the
  guarantees are the contract, and they are stated in prose (ADR 0008).
- MySQL's weaker `SKIP LOCKED` story and SQLite's single-writer model will be discovered later,
  not designed around now (§31: no abstraction for hypothetical portability).

## Alternatives considered

- **Two providers in v0.1 (Postgres + SQLite).** Rejected: doubles the test matrix and the
  migration surface before the trait has stabilized once.
- **A generic SQL layer over sqlx `Any`.** Rejected: loses `query!` compile-time checking, which
  is the entire reason to use sqlx (§5), and no two engines agree on skip-locked claiming.
- **Ship a broker transport in v0.1 instead of a store.** Rejected: without a store there is no
  outbox, and §4 shows the store half is the part that must be proven first.
