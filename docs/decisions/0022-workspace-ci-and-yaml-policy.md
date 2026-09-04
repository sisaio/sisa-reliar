# ADR 0022 — Workspace layout, build profiles, semver policy, and the CI/YAML rules

**Status:** Accepted — 2026-09-04; the `rust-version` clause is superseded by [ADR 0024](0024-msrv-1-88-and-msrv-policy.md);
the **action-pinning clause is superseded by human decision #30 (2026-09-04)** — see *Action references* below;
the **`tools/*` workspace member and the `scripts/` helper directory are withdrawn by human
decision #32 (2026-09-04)** — see *No `scripts/`, no `tools/`* below
**SRS:** §6, §7, §7.1, §32, §38, §39, §40, §41, §44, §43.B
**Decisions:** human decisions 6, 8, 20, 21, 22, 23, 30

## Context

Several layout and platform items in v1.0 were not merely unspecified — two of them **do not
build**. `benches/outbox-throughput` and `benches/serialization` were drawn as packages but
`members = ["crates/*", "examples/*", "tools/*"]` does not match `benches/*`, so they would never
be built, linted or run: CI would silently benchmark nothing. A root `tests/system/` cannot compile
at all, because a virtual workspace has no root package and Cargo builds `tests/` only for a package.

There was also no MSRV, no lint policy, no semver policy, and — with every public struct having
public fields — **every future field addition was a breaking change**. That is cheapest to fix
before the first crate exists and most expensive after 0.1 ships.

## Decision

**Workspace.**

- Virtual workspace, `resolver = "3"`, `members = ["crates/*", "examples/*", "tests/*"]`.
  `"tests/*"` is required so `tests/system` is a **real member package** (`Cargo.toml`,
  `publish = false`, tests under `tests/system/tests/`).
  *(Amended 2026-09-04, decision #32: `"tools/*"` is removed — see below.)*
- **Benchmarks live in each crate** (`crates/<name>/benches/`, `[[bench]] harness = false`), using
  Cargo's own convention. This supersedes v1.0's top-level `benches/` tree. Bench targets are not
  compiled into the published library, which satisfies the original intent.
- `[workspace.package]` pins `edition = "2024"`, `license = "MIT"` and an explicit
  `rust-version` — set to `1.85` here, **now 1.88 per [ADR 0024](0024-msrv-1-88-and-msrv-policy.md)**;
  `[workspace.lints.rust]` sets `unsafe_code = "forbid"` and `missing_docs = "warn"`; every crate
  declares `[lints] workspace = true` and takes dependencies from `[workspace.dependencies]` so
  there is one version per dependency.
- **Only create a crate when its implementation begins.** The §6 tree is aspirational; Phase 1 has
  exactly `reliar-core`, `reliar-outbox`, `reliar-store-postgres`.

**Semver and API evolution.**

- **Every public `struct`/`enum` that may gain a field or variant is `#[non_exhaustive]`**, built
  through a builder or a `Default` config rather than positional public fields. A type deliberately
  frozen as a plain record says so in its rustdoc.
- Feature flags are **additive**. `cargo hack --feature-powerset` and `cargo semver-checks` run in
  CI. While 0.x, breaking changes ship only in a **minor** release.
- **MSRV** is `1.85` (edition 2024 and `resolver = "3"` both require it), pinned by a root
  `rust-toolchain.toml`, built in CI on both MSRV and stable. **Raising the MSRV is a minor release
  with a CHANGELOG entry, never a patch.**
  *Superseded:* the floor is **1.88** as of [ADR 0024](0024-msrv-1-88-and-msrv-policy.md)
  (RUSTSEC-2026-0009 has no patch that builds on 1.85), which also records when the MSRV may move.
  The "minor release, never a patch" rule above still stands and is restated there.
- New dependencies require justification (licence, maintenance, size) against `deny.toml`'s
  allow-list. `async-trait`, `thiserror`, `anyhow` and `chrono` are **banned** in library crates
  (`time` is the date/duration crate, decision 7).
  **How that ban is enforced** *(decided 2026-09-04, S5)*: by a per-manifest CI gate over
  `crates/*/Cargo.toml`, **not** by `deny.toml`'s `[bans]`. The rule is "no crate of ours declares
  these", which is a property of our manifests; cargo-deny's bans are evaluated over the whole
  resolved graph, a different question — `sqlx-core` depends on `thiserror`, so a graph-wide ban
  fails the build over a third party's internal choice that is invisible to us and imposes nothing.
  `wrappers = ["sqlx-core", …]` would work but grows a maintenance tail: every sqlx bump that adds
  an internal crate reddens an unrelated PR. The grep is also **stricter** for our code, catching a
  direct dependency in one of our crates even where a third party already pulls the same crate
  transitively. This is the same mechanism, for the same reason, as the `reliar-core` purity gate
  below. `chrono` is the exception and stays a graph-wide `deny`: two datetime stacks in one binary
  is ecosystem hygiene rather than style, and nothing in the graph pulls it today. `anyhow` remains
  allowed in `examples/` *(and, until decision #32 removed the directory, `tools/`)*, which the
  `crates/*`-scoped gate simply does not cover.

**Build profiles.** Declared explicitly in the root `Cargo.toml` rather than inherited: `dev`
(`opt-level = 0`, dependencies at `2`), `test` (`inherits = "dev"`, `opt-level = 1` — sqlx macros
and testcontainers are painfully slow at `O0`), `release` (`opt-level = 3`, thin LTO,
`codegen-units = 1`, `strip = "debuginfo"`, `debug = "line-tables-only"`), `bench`
(`inherits = "release"`, `debug = true`).
**`panic = "unwind"` on `release` is the load-bearing line:** `panic = "abort"` would be inherited
by anything building Reliar from source and would break every host relying on unwinding — Tokio task
boundaries, `catch_unwind`, and the test harness itself. **A library SHALL NOT force `abort` on its
host.**

**Platform.**

- `.github/workflows/` contains **`ci.yaml`, `test.yaml`, `security.yaml`, `codeql.yaml`,
  `scorecard.yaml`, `release.yaml`**, plus `.github/dependabot.yml` (the one `.yml`, see below). `test.yaml` provides a
  PostgreSQL service container and exports `DATABASE_URL`. Every workflow declares
  **least-privilege `permissions`**. Action references: see *Action references* below.
- **Action references — superseded clause** *(amended 2026-09-04, decision #30)*. This ADR
  originally required third-party actions to be **SHA-pinned**, on the Scorecard/supply-chain
  argument that a tag is mutable. **The human reversed that:** every `uses:` now names the action's
  **latest major version tag** (`actions/checkout@v7`, `github/codeql-action/init@v4`,
  `codecov/codecov-action@v7`, …). `dtolnay/rust-toolchain@stable` / `@master` remain branch refs
  **by upstream design — that action publishes no semver tags at all, its branches *are* its
  released interface**, so there is no version reference to prefer over them; and `ossf/scorecard-action@v2.4.4` keeps its exact patch tag because upstream
  publishes no floating `v2` tag. The reasoning accepted: the workflows become readable and
  maintainable and a Dependabot bump is a one-token diff, at the **known cost of OpenSSF
  Scorecard's `Pinned-Dependencies` check**, which only credits immutable references and will
  therefore not score full marks. The compensating control is unchanged and still required —
  `.github/dependabot.yml` keeps the **`github-actions` ecosystem** enabled (weekly, all actions
  grouped), so a new major arrives as a PR rather than as rot.
- **Every YAML file in the repository ends in `.yaml`, never `.yml`** — workflows, compose, configs.
  One spelling, so a glob never misses a file; a glob matching `*.yml` **fails the build**.
  **One exception, and only one: `.github/dependabot.yml`** *(added 2026-09-04, RELIAR-21)*.
  GitHub's documentation names the configuration path `.github/dependabot.yml` on every page that
  names it at all — the options reference, "Configuring Dependabot version updates" ("You enable
  Dependabot version updates by committing a `dependabot.yml` configuration file to your
  repository") and "Keeping your actions up to date with Dependabot" ("Check the `dependabot.yml`
  configuration file in to the `.github` directory of the repository") — and **never** mentions
  `.yaml`. The failure mode decides it: a `.yaml` file in that location is not rejected, it is
  *ignored*, so Dependabot never runs, no error is raised anywhere, and the action version tags and
  the dependency tree quietly stop being updated. Trading a silent security-update failure for one
  spelling is not a trade worth making, and relying on undocumented behaviour for a mechanism whose
  failure is invisible is worse still. The CI gate whitelists the **exact path**, not the
  extension, so a second `.yml` cannot appear beside it. SRS §40's `.yaml`-only sentence needs the
  matching one-line exception (PO).
- **No `scripts/`, no `tools/`** *(added 2026-09-04, human decision #32)*. Both directories are
  removed and neither returns. Three shell wrappers (`test.sh`, `lint.sh`, `release.sh`), a
  `dev-db.sh`, a Python MSRV helper, an `xtask` package whose only job was to `exec` those scripts,
  and a `reliar-migrate` package whose only job was to call one public function were **five
  indirections in front of six commands**, each one a place for the real command and the wrapper to
  drift, and `xtask`/`reliar-migrate` were additionally workspace members with manifests, lint
  configuration and — for `reliar-migrate` — a second `rust-version` the MSRV gate had to be taught
  about (ADR 0025). Instead:
  - **A repeatable command lives in the workflow that runs it**, and is spelled out for humans in
    `CONTRIBUTING.md`. What a contributor types is then literally what CI types.
  - **Logic a workflow needs is inline in the step**, in `bash` + `jq` — the `msrv` job's computed
    ADR-0025 exclusion list is the largest such block at ~30 lines; the `conventions` job's gates
    (the `.yml` whitelist, the inline-`#[cfg(test)]` and banned-crate greps, the compose/test image
    pin equality of decision #31) and the workflows' service probes are the rest. This drops Python as a
    build-time dependency of CI. The one property whose failure was silent (version padding, so
    `1.88` and `1.88.0` compare equal — otherwise the job excludes every member and passes having
    built nothing) keeps its assertion, inline, as the first thing the step does.
  - **A one-off binary is an `examples/` target of the crate that owns the API it calls.**
    `crates/reliar-store-postgres/examples/migrate.rs` replaces `tools/reliar-migrate`;
    `cargo run -p reliar-store-postgres --example migrate` reaches it with no member, no manifest,
    and no second MSRV, and `cargo check --all-targets` builds it like any other target.
  The cost, accepted: a contributor copies a longer command line, and a future cross-cutting tool
  (a release checklist, a codegen step) has to argue for its own home rather than landing in a
  directory that already exists. `examples/*` remains a member glob; `tools/*` does not.
- Dev infrastructure lives under `deploy/docker/` (Dockerfiles) and `deploy/compose/`
  (`docker-compose.yaml` + `configs/`, `secrets/`); only `secrets/*.example` is committed. There is
  no top-level `docker/`. The compose stack is for examples and manual exploration — **no test
  targets it** (ADR 0021). Its optional `pooler` profile runs **PgDog**, the same image and tag the
  provider's pooler scenario starts (decision #31), with `configs/pgdog.toml` committed
  credential-free and the one file that carries the upstream password generated by compose from
  `${RELIAR_PG_PASSWORD}`. Both pins are held equal to the scenario's `const`s by a `conventions`
  gate rather than by a shared file: Rust needs them as `const`s and compose as `image:`, and an
  `include_str!` reaching outside the crate directory would not survive `cargo package`.
  The pgdog service's healthcheck is a **real `select 1` through the pooler**, not `pgdog --version`
  or `pg_isready -p 6432` — both answer "up" for a PgDog that has *disabled its pool* because
  `RELIAR_PG_PASSWORD` was empty, which made `up --wait` report a stack that fails on first use
  (review finding, 2026-09-04). A query is the only probe that covers client auth and the upstream
  connection together; the container's `PGPASSWORD` is the same secret its `users.toml` already holds.
- **Licence: MIT only**, one `LICENSE` file, `license = "MIT"` via `[workspace.package]`. The dual
  `MIT OR Apache-2.0` recommendation is withdrawn (decision 6); there are no `LICENSE-MIT` /
  `LICENSE-APACHE` files. Inbound contributions are MIT under a **DCO sign-off, not a CLA**.
- CI enforces the structural constraints that are not runtime-testable (§43.B): `reliar-core` has no
  sqlx/postgres/broker dependency (`cargo tree` gate);
  **The `cargo tree -p reliar-core -e normal --all-features` gate in `ci.yaml` *substitutes* for the
  crate-scoped `deny.toml` ban SRS §40 v1.0 asked for.** `cargo-deny`'s bans are evaluated over the
  whole workspace graph and cannot be scoped to one crate's dependency tree, so a `deny.toml` entry
  banning sqlx would fail the build for `reliar-store-postgres`, which legitimately needs it. The
  purity rule is inherently per-crate, so it needs a per-crate gate. `deny.toml` keeps the bans that
  *are* workspace-wide (`async-trait`, `thiserror`, `anyhow`, `chrono`) plus the licence allow-list.
  SRS v1.1.1 §40 now says the same. *(Added 2026-09-04, RELIAR-23 AC 2.)* no `#[cfg(test)]` under any `src/`; `fmt`,
  `clippy -D warnings`, `doc -D warnings`, feature powerset, the MSRV job, `cargo deny`,
  `cargo audit`, and `cargo sqlx prepare --check` against a freshly migrated Postgres.
- The **facade crate `reliar` is deferred to 0.2** (decision 8) — a facade's feature-forwarding
  surface is the hardest thing to keep semver-stable and it has nothing to re-export until three
  crates exist. Its crates.io name is reserved now with a `0.0.0` placeholder, along with
  `reliar-core`, `-outbox`, `-inbox`, `-idempotency`, `-store-postgres`,
  `-transport-nats` (all checked free 2026-09-03).

## Consequences

- Everything in the tree is a workspace member, so `cargo build/clippy/test --workspace` genuinely
  covers it — no silently-unbuilt package. Cargo rejects a `members` glob that matches nothing, so
  the globs are enabled one at a time as each directory gains its first package; the target list
  above is what the root `Cargo.toml` carries once Phase 1 lands (`crates/*` and `examples/*`
  today, `tests/*` when the first system-test package exists).
- `#[non_exhaustive]` everywhere makes exhaustive matching impossible for downstream users on
  Reliar's enums, and requires builders for construction. That is the deliberate trade for being
  able to add fields for the life of 0.x.
- A pinned MSRV means CI runs one extra toolchain and a dependency bump can break it. That is the
  signal MSRV exists to give.
- The `.yaml`-only rule is mechanical and enforceable, which is the only reason it is worth having.
- Explicit profiles make CI and local builds reproducible and keep `cargo test` from crawling.

## Alternatives considered

- **Top-level `benches/` and root `tests/`** (v1.0's tree). Rejected: neither builds.
- **No MSRV policy, "latest stable".** Rejected: silently breaks downstream users on a bump with no
  changelog signal.
- **Public fields, no `#[non_exhaustive]`.** Rejected: every field addition becomes a breaking
  change for the life of the crate.
- **Dual MIT/Apache-2.0 licence.** Withdrawn by decision 6: the patent grant and inbound-contribution
  notice are administration this project is not ready for, and the repository already ships MIT.
- **Allow `.yml`.** Rejected: two spellings means a glob eventually misses a file.
- **Keep `scripts/` + `xtask` as the one entry point for every command** (the original layout,
  withdrawn by decision #32). Rejected: a wrapper that only forwards is a second copy of the
  command that can disagree with CI, and `cargo xtask test` was a third name for `cargo test`.
