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
   module in `src/`. Provider tests run against a real Postgres/NATS via testcontainers;
   `tests/system` proves the two together.
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

The commands below are exactly what CI runs, so what you run
locally is literally what `ci.yaml` runs:

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --all-features
cargo hack check --workspace --feature-powerset --no-dev-deps
cargo deny check && cargo machete
cargo test --workspace --all-features          # add -p <crate> for one crate
```

Prefix the lint commands with `SQLX_OFFLINE=true` if you have no `DATABASE_URL`; the committed
`.sqlx/` cache is what CI builds against.

The tests need Docker but no database (or broker) of your own: provider tests start an ephemeral
Postgres through testcontainers and create an isolated database per test, because SRS §8.2 forbids
testing against a shared or long-lived database. `reliar-store-postgres`'s suite starts exactly
**one** Postgres container for the whole run (plus one `PgDog` pooler, only inside its own
scenario), `reliar-transport-nats`'s suite starts exactly **one** NATS/`JetStream` container
(`nats:2.14-alpine -js -m 8222`), and `tests/system`'s `e2e` suite starts **both** — one Postgres
and one NATS container — for its cross-provider proof. Every one of them removes every container
and volume by the time the process exits — see `docs/guides/postgres.md`'s "Contributing" section
if you touch the Postgres harness, `docs/guides/nats.md` for the NATS one.

**`DATABASE_URL`/`NATS_URL` substitute a shared server for the container**, the same rule for
both: when the variable is set (CI's Postgres service container; CI's `docker run -js` NATS step,
ADR 0031 §3) the harness connects to it and starts no container of its own; when unset, the same
test code boots its own ephemeral container. Set both to point a local run at
`deploy/compose/docker-compose.yaml`'s stack instead of testcontainers, if you prefer:
`DATABASE_URL=postgres://reliar:reliar@localhost:5432/reliar?options=-c%20search_path%3Dreliar,public`
and `NATS_URL=nats://127.0.0.1:4222`.

**`TESTCONTAINERS_COMMAND=keep` and container reuse (`.with_reuse(..)`/`reusable-containers`) are
forbidden in CI** — local debugging aids only, since both defeat the `Drop`-based removal that
container hygiene depends on. If a run is killed hard and leaves something behind, sweep it by
hand:

```sh
docker ps -aq --filter label=org.testcontainers.managed-by=testcontainers --filter name=^reliar- | xargs -r docker rm -f -v
```

`deploy/compose` exists for the **examples**, never for the tests; running it needs a local secret
— copy `deploy/compose/secrets/postgres_password.example` to `postgres_password` first, then
`docker compose -f deploy/compose/docker-compose.yaml up -d --wait postgres nats` (add
`--profile pooler`, with `RELIAR_PG_PASSWORD` exported from that same secret, for the `PgDog`
pooler; drop `nats` if you only need `examples/outbox-basic`/`axum-outbox`). Before opening a
release PR, `release-plz release --dry-run` shows what would publish.
CI runs these checks plus MSRV, coverage, CodeQL, Scorecard, and the dependency audit.

## Pull requests

Keep a PR to one story or fix, include tests that fail without the change, and update
`CHANGELOG.md` under `## [Unreleased]` for anything user-visible. A public API change needs its ADR
in the same PR.

## Branches

Name branches `type/short-description` (`feat/…`, `fix/…`, `chore/…`, `docs/…`); reference the backlog card id in the commit message or PR body.
