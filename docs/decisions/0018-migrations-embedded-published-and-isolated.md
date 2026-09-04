# ADR 0018 — Migrations embedded in the crate, published as an artifact, with isolated bookkeeping

**Status:** Accepted — 2026-09-04
**SRS:** §35, §35.1, §24, §40, §43.A.24, §43.A.31
**Decisions:** human decisions 19, 26

## Context

Three problems, one file set.

**Bookkeeping collision.** sqlx records applied versions in **one table per database**,
`_sqlx_migrations`. A library that runs `sqlx::migrate!` against a host application's database
therefore writes its versions and checksums into the host's own migration history — colliding
version numbers, foreign checksums, and a host migration tool that now sees rows it did not write.

**Where the SQL lives.** A top-level `migrations/` directory reads better in a repository, and the
human's instinct was that embedding SQL in a crate feels wrong.

**Who runs it.** Many teams route all DDL through psql, Flyway, Liquibase or a DBA review gate and
will not let a library create tables.

## Decision

- **Bookkeeping is isolated.** `migrate` runs a **clone** of the embedded static `Migrator`
  (its fields are `Cow`, so the static is never mutated) configured with:
  `create_schema(options.schema)`, `dangerous_set_table_name("<schema>._migrations")`, and
  `set_locking(true)`. Reliar's versions and checksums live in **its own schema**, in a table named
  `_migrations` — **never** `_sqlx_migrations` — and can never collide with the host's history.
- `set_locking(true)` is required, not optional: five pods booting simultaneously must be safe, and
  every caller after the first observes `Ok(())`. **`migrate()` is idempotent.**
- **`migrate` does not depend on the caller's `search_path`.** `create_schema` plus the qualified
  bookkeeping table name make it self-contained, so it works over a host pool whose URL never set
  `search_path`; the store constructor is where a missing path is caught (ADR 0017).
- **A hand-rolled migration runner is rejected.** `dangerous_set_table_name` makes it dead weight,
  and a bespoke runner is one more thing to get wrong about locking.
- **Embed *and* publish — one source of truth.** `crates/reliar-store-postgres/migrations/*.sql`
  **is** the source of truth. It cannot instead live at the repository root, for three independent
  reasons: `cargo publish` packages only files under the crate's own directory, so a published crate
  would ship with no SQL; `sqlx::migrate!("../../migrations")` resolves relative to
  `CARGO_MANIFEST_DIR` at **compile time**, so it would fail to compile for every downstream user,
  who has a registry copy and no repository root; and the statements must be *in the binary*
  because it is sqlx that opens the transaction, takes the advisory lock and writes the changelog.
- **Every release additionally publishes the identical files** as
  `reliar-store-postgres-migrations-<version>.tar.gz` + `SHA256SUMS` with build provenance. Teams
  running their own pipeline apply that artifact and **never call `migrate`** — fully supported;
  Reliar's bookkeeping table is then never created. `migrate` is the convenience, not the
  requirement. The artifact is a **copy, never a second source**: the release job SHALL fail if it
  differs from the crate's directory.
- **Forward-only.** Migrations are additive; an applied file SHALL NEVER be edited — its checksum is
  recorded and editing it breaks every existing deployment. A companion `.down.sql` MAY ship for
  local development only; Reliar SHALL NOT run `undo()` against a production database and the public
  API exposes **no** `revert`. New checksums are listed in the CHANGELOG.
- **Lock-safe.** No long `ACCESS EXCLUSIVE` lock on a populated `outbox`. An index that must be
  built on a live table goes in its own file, created concurrently outside the migrator's
  transaction, or is documented as an operator step.
- **Never implicit.** No constructor, no `Default`, no first `acquire` runs a migration. A library
  that silently mutates a host's schema is unadoptable.
- **Verification obligation.** If the pinned sqlx version rejects a schema-qualified bookkeeping
  table name, the fallback is a plain `reliar_migrations` in the default schema — which still
  removes the collision. This SHALL be verified in the Phase-1 spike **before** the API is
  published.

## Consequences

- A host application's `_sqlx_migrations` is untouched, so Reliar can be added to a database that
  already has a migration tool without either side noticing the other.
- The repository root has no `migrations/` directory, which reads oddly against most services. The
  reasons are recorded here so the question is not reopened; the published artifact answers the
  ergonomic half.
- CI must diff the artifact against the crate directory on every release, or the two silently drift.
- Forward-only with no `revert` means a bad migration is fixed by a new migration. That is the
  standard production discipline and it is stated rather than implied.
- Teams that skip `migrate` get no version tracking from Reliar; the CHANGELOG's checksum list is
  what they diff against.

## Alternatives considered

- **Top-level `migrations/` + published scripts only** (the human's initial preference). Rejected on
  the three mechanical grounds above — chiefly that `sqlx::migrate!` would not compile downstream.
- **Share the host's `_sqlx_migrations`.** Rejected: guaranteed version and checksum collisions.
- **A hand-rolled runner** with its own table. Rejected: reimplements advisory locking for no gain.
- **Run migrations implicitly at store construction.** Rejected outright (§35).
- **Ship reversible migrations with a public `revert`.** Rejected: an API that can drop a host's
  outbox table is a liability; `.down.sql` stays a local-development convenience.
