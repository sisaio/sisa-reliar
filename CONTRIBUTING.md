# Contributing

Thanks for helping build Reliar. Issues, discussions, and pull requests are all welcome.

## House rules

1. **The SRS is the baseline** (`../sisa-reliar-backlog/docs/srs.md`, the sibling backlog repo); anything it protects changes only through an ADR in
   `docs/decisions/`.
2. **Honest guarantees.** Delivery is at-least-once with a documented duplicate window — never
   claim exactly-once, never promise cross-worker ordering.
3. **No `async-trait`, `thiserror`, `anyhow`, or `chrono`** in library crates: native async fns in
   traits returning `impl Future + Send`, hand-rolled `#[non_exhaustive]` error enums with
   `source()`, and the `time` crate. `deny.toml` enforces this.
4. **Static dispatch by default** — generics and monomorphization, no `Box<dyn _>` on hot paths.
5. **`reliar-core` stays pure**: no sqlx, no database or broker types, no transport routing.
6. **sqlx compile-time macros only**, never the runtime string API, never `FromRow`; commit the
   crate's `.sqlx/` offline cache.
7. **Tests live in `tests/`**, exercise the public API, and no crate ships an inline `#[cfg(test)]`
   module in `src/`. Provider tests run against a real Postgres via testcontainers.
8. **Public items are documented** and state the guarantee they uphold; `cargo doc` runs with
   `-D warnings`.
9. **Conventional commits** (`feat:`, `fix:`, `docs:`, `chore:`, `feat!:` for breaking) — the
   changelog and version bumps are generated from them.
10. **Never commit secrets.** Examples and tests read `DATABASE_URL` from the environment.

## Developer Certificate of Origin

Contributions are accepted under the [DCO](https://developercertificate.org/). Sign off every
commit — `git commit -s` — asserting you wrote the patch or have the right to submit it under the
MIT licence.

## Running the checks

```sh
./scripts/lint.sh                     # fmt, clippy, doc, feature powerset, deny
./scripts/test.sh                     # cargo test --workspace --all-features
./scripts/test.sh -p reliar-outbox    # extra arguments go to cargo test
./scripts/dev-db.sh                   # local Postgres 18 for the examples (`down` to stop)
```

The tests need Docker but no database of your own: provider tests start an ephemeral Postgres
through testcontainers and create an isolated database per test, because SRS §8.2 forbids testing
against a shared or long-lived database. `reliar-store-postgres`'s suite starts exactly **one**
Postgres container for the whole run (plus one `PgBouncer`/`PgDog` each, only inside their own
scenario) and removes every container and volume by the time the process exits — see
`docs/guides/postgres.md`'s "Contributing" section if you touch that harness.
**`TESTCONTAINERS_COMMAND=keep` and container reuse (`.with_reuse(..)`/`reusable-containers`) are
forbidden in CI** — local debugging aids only, since both defeat the `Drop`-based removal that
container hygiene depends on. `deploy/compose` exists for the **examples**; running it needs a
local secret — copy `deploy/compose/secrets/postgres_password.example` to `postgres_password`
first. `cargo xtask <task>` runs the same scripts. CI runs these checks plus MSRV, coverage,
CodeQL, Scorecard, and the dependency audit.

## Pull requests

Keep a PR to one story or fix, include tests that fail without the change, and update
`CHANGELOG.md` under `## [Unreleased]` for anything user-visible. A public API change needs its ADR
in the same PR.

## Branches

Name branches `type/short-description` (`feat/…`, `fix/…`, `chore/…`, `docs/…`); reference the backlog card id in the commit message or PR body.
