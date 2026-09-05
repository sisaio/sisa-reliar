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
9. **Conventional commits** (`feat:`, `fix:`, `docs:`, `chore:`, `feat!:` for breaking).
10. **Bump the version in the change that needs it.** A version on crates.io is frozen: if you edit
    a crate whose current version is already published, bump it in the same PR — minor for a
    public-surface change, patch otherwise — along with `CHANGELOG.md`, plus its `version` pin in
    the root `[workspace.dependencies]` and the crates that depend on it *when the new version
    leaves the requirement they declare* (a patch bump inside `^0.2.0` leaves both alone).
    "Editing a crate" includes editing what it inherits from the root manifest —
    `[workspace.package]`, `[workspace.dependencies]`, `[workspace.lints]` are baked into the
    published `Cargo.toml`, so a root change can freeze-fail a crate whose own directory you never
    touched ([ADR 0034](docs/decisions/0034-versioning-and-release-flow.md)). CI's `versioning` job
    fails the PR otherwise, naming the crate.
11. **Never commit secrets.** Examples and tests read `DATABASE_URL` from the environment.

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
pooler; drop `nats` if you only need `examples/outbox-basic`/`axum-outbox`). The version rules
(house rule 10) are checked by CI against crates.io and the release tags; to reproduce a publish
locally, `cargo package -p <crate>` is the only form that resolves the Reliar dependencies from the
registry the way `cargo publish` will — `cargo package --workspace` resolves them to the local
copies and passes on trees that cannot be published. It fails while a dependency's new version is
not on crates.io yet, which is expected between the merge of a bump and the release that publishes
it.
CI runs these checks plus MSRV, coverage, CodeQL, Scorecard, and the dependency audit.

## Pull requests

Keep a PR to one story or fix, include tests that fail without the change, and update
`CHANGELOG.md` under `## [Unreleased]` for anything user-visible. A public API change needs its ADR
in the same PR, and a change to an already-published crate needs its version bump (house rule 10).
Releases happen from `main`: `release.yaml` runs `release-plz release`, which tags, creates the
GitHub release and publishes in dependency order whatever version on `main` is not yet on
crates.io. There is no release PR to merge.

## Automated review

Every pull request is reviewed automatically by **CodeRabbit**, configured by the committed
[`.coderabbit.yaml`](.coderabbit.yaml): its `path_instructions` restate this project's rules —
`reliar-core` purity, native async fns in traits, hand-rolled `#[non_exhaustive]` errors, sqlx
macros and lease/claim semantics in the Postgres provider, the NATS mapping rules, public-API tests
in `tests/`, and the workflow/YAML policy — so the bot applies the same law a human maintainer
does. Its findings are **advisory input** to the human reviewer and to the team's `reviewer` agent;
they neither approve nor block a merge, and CI plus the
[Definition of Done](team/definition-of-done.md) remain the only gates. The rules themselves live
in [`team/engineering-conventions.md`](team/engineering-conventions.md) and
[`team/definition-of-done.md`](team/definition-of-done.md) — when a rule changes there (or in an
ADR), update `.coderabbit.yaml` in the same PR. Disagree with a finding in the PR thread; comment
`@coderabbitai` to ask it for reasoning or to dismiss it.

## Branches

Name branches `type/short-description` (`feat/…`, `fix/…`, `chore/…`, `docs/…`); reference the backlog card id in the commit message or PR body.
