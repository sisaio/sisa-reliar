---
name: ci-release
description: Reliar's platform for an open-source Rust workspace — GitHub Actions split by responsibility and following GitHub best practice (ci.yaml fmt/clippy/check/doc/feature-powerset/MSRV + reliar-core purity check, test.yaml with a postgres service container + DATABASE_URL + `cargo sqlx prepare --check` + coverage, security.yaml cargo-deny + cargo-audit + dependency-review, codeql.yaml, scorecard.yaml, release.yaml release-plz publish in dependency order + migration-script artifacts with provenance, dependabot.yml — the single GitHub-mandated .yml), `.yaml` extension for every other file, rust-toolchain.toml + MSRV, deny.toml/clippy.toml/rustfmt.toml, the root virtual-workspace Cargo.toml with explicit dev/release/test/bench profiles, deploy/docker + deploy/compose (configs/secrets/volumes) for local Postgres (later NATS), scripts/{test,lint,release}.sh, CHANGELOG (Keep a Changelog), MIT license file, crates.io name reservation, and cargo-semver-checks. Use when creating or changing CI, security policy, release flow, dev infra, or root workspace config.
metadata:
  audience: ARCHITECT
---

# CI, security & release (OSS Rust workspace)

SRS §40–§41, §44. Small, single-purpose workflows beat one giant pipeline. Everything below runs
from the repo root against the **virtual workspace**.

## Root files

```
rust-toolchain.toml      [toolchain] channel = "1.98.0"  components = ["rustfmt", "clippy"]
rustfmt.toml             edition = "2024"  imports_granularity = "Crate"  group_imports = "StdExternalCrate"
clippy.toml              msrv = "1.85"  (matches [workspace.package].rust-version)
deny.toml                see below
Cargo.toml               virtual workspace (skill `rust-library-design`)
LICENSE                  MIT only (`license = "MIT"`); holder name matches the brand
CHANGELOG.md             Keep a Changelog; an `## [Unreleased]` section always present
CONTRIBUTING.md, SECURITY.md (disclosure address + supported versions)
```

## `deny.toml`

```toml
[licenses]
allow = ["MIT", "Apache-2.0", "Apache-2.0 WITH LLVM-exception", "BSD-2-Clause", "BSD-3-Clause", "ISC", "Unicode-3.0", "Zlib", "MPL-2.0"]
[advisories]
yanked = "deny"
[bans]
multiple-versions = "warn"
deny = [{ name = "async-trait" }, { name = "thiserror", wrappers = [] }, { name = "anyhow", wrappers = ["examples", "tools"] }]
[sources]
unknown-registry = "deny"; unknown-git = "deny"
```

The `bans` entries turn house rules into a gate (adjust `wrappers` to the crates allowed to use them).

## File naming and layout rules

- **Every YAML file ends in `.yaml`** — `ci.yaml`, `docker-compose.yaml`. Never `.yml` — with **one GitHub-mandated exception: `.github/dependabot.yml`** (Dependabot silently ignores `.yaml`; the `ci.yaml` gate whitelists exactly that path).
- Infra lives under **`deploy/`**: `deploy/docker/` (Dockerfiles, e.g. for `tests/system` or tools) and
  `deploy/compose/` (`docker-compose.yaml` plus `configs/`, `secrets/` (git-ignored, `*.example` committed),
  `volumes/` when needed). No top-level `docker/`.
- Workflows are split by responsibility: `ci.yaml` · `test.yaml` · `security.yaml` · `codeql.yaml` ·
  `scorecard.yaml` · `release.yaml`, plus `.github/dependabot.yml`. Pin third-party actions to a SHA
  (Dependabot keeps them fresh); set `permissions: read-all` at the top and widen per job.

## Root `Cargo.toml` profiles (explicit, documented)

```toml
[profile.dev]
opt-level = 0
debug = true
[profile.dev.package."*"]            # optimize deps once; keeps the inner loop fast
opt-level = 2

[profile.test]
inherits = "dev"
opt-level = 1                        # sqlx macros + testcontainers are slow at O0

[profile.release]
opt-level = 3
lto = "thin"
codegen-units = 1
strip = "debuginfo"
panic = "unwind"                     # a library must not force abort on its hosts
debug = "line-tables-only"           # keeps backtraces useful in bug reports

[profile.bench]
inherits = "release"
debug = true
```

## `.github/workflows/ci.yaml` — fast, no services

```yaml
name: ci
on: { push: { branches: [main] }, pull_request: {} }
env: { CARGO_TERM_COLOR: always, SQLX_OFFLINE: "true", RUSTFLAGS: "-D warnings", RUSTDOCFLAGS: "-D warnings" }
jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable        # reads rust-toolchain.toml
        with: { components: rustfmt, clippy }
      - uses: Swatinem/rust-cache@v2
      - run: cargo fmt --all --check
      - run: cargo clippy --workspace --all-targets --all-features
      - run: cargo doc --workspace --no-deps --all-features
      - uses: taiki-e/install-action@cargo-hack
      - run: cargo hack check --workspace --feature-powerset --no-dev-deps
      - name: reliar-core stays pure
        run: '! cargo tree -p reliar-core -e normal | grep -Eq "sqlx|postgres|async-nats|rdkafka|lapin|redis"'
      - uses: taiki-e/install-action@cargo-machete
      - run: cargo machete
  msrv:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@master
        with: { toolchain: "1.85" }               # = rust-version
      - run: cargo check --workspace --all-features
```

## `.github/workflows/test.yaml` — with Postgres (+ coverage)

```yaml
name: test
on: { push: { branches: [main] }, pull_request: {} }
jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:18-alpine
        env: { POSTGRES_PASSWORD: postgres }
        ports: ["5432:5432"]
        options: --health-cmd "pg_isready -U postgres" --health-interval 5s --health-timeout 5s --health-retries 10
    env:
      DATABASE_URL: postgres://postgres:postgres@localhost:5432/postgres
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - uses: Swatinem/rust-cache@v2
      - uses: taiki-e/install-action@cargo-llvm-cov
      - run: cargo llvm-cov --workspace --all-features --lcov --output-path lcov.info   # runs the tests; provider tests use DATABASE_URL
      - uses: codecov/codecov-action@v4
        with: { files: lcov.info, fail_ci_if_error: false }
      - uses: taiki-e/install-action@v2
        with: { tool: sqlx-cli }
      - name: offline cache is current
        run: |
          cargo run -p reliar-store-postgres --example migrate --features test-support   # or a tools/ binary that calls migrate()
          cd crates/reliar-store-postgres && cargo sqlx prepare --check -- --all-targets
```

Later phases add a `nats:2-alpine` service (`-js`) and `NATS_URL`.

## `.github/workflows/security.yaml`, `codeql.yaml`, `scorecard.yaml`, `dependabot.yml`

```yaml
name: security
on: { push: { branches: [main] }, pull_request: { paths: ["**/Cargo.toml", "Cargo.lock", "deny.toml"] }, schedule: [{ cron: "0 6 * * 1" }] }
jobs:
  deny:  { runs-on: ubuntu-latest, steps: [ { uses: actions/checkout@v4 }, { uses: EmbarkStudios/cargo-deny-action@v2 } ] }
  audit: { runs-on: ubuntu-latest, steps: [ { uses: actions/checkout@v4 }, { uses: rustsec/audit-check@v2, with: { token: "${{ secrets.GITHUB_TOKEN }}" } } ] }
  dependency-review:
    if: github.event_name == 'pull_request'
    runs-on: ubuntu-latest
    steps: [ { uses: actions/checkout@v4 }, { uses: actions/dependency-review-action@v4, with: { fail-on-severity: high } } ]
```

`codeql.yaml`: `github/codeql-action/init` with `languages: rust` (+ `actions`), build via `cargo build
--workspace --all-features`, then `codeql-action/analyze`; on push to main, PRs, and a weekly cron.
`scorecard.yaml`: `ossf/scorecard-action` publishing SARIF to code scanning (weekly + on main).
`.github/dependabot.yml`: `cargo` and `github-actions` ecosystems, weekly, grouped minor/patch updates.

## `.github/workflows/release.yaml` — release-plz + migration artifacts

`release-plz` opens a release PR that bumps versions from conventional commits, updates
`CHANGELOG.md`, and on merge tags + publishes crates **in dependency order** (`reliar-core` →
`reliar-outbox` → `reliar-store-postgres` → …). Needs `CARGO_REGISTRY_TOKEN` as a repo secret;
`cargo publish` locally is gated to *ask*. Before the first publish: **reserve the crate names on
crates.io** (`reliar`, `reliar-core`, …) with a 0.0.0 placeholder if the names are free — name
squatting is a real risk for a prefix this short.

The release job also uploads **`reliar-store-postgres-migrations-<version>.tar.gz`** (the crate's
`migrations/*.sql` + `SHA256SUMS`) as a GitHub Release asset with build provenance
(`actions/attest-build-provenance`) for teams that run migrations through their own pipeline; the
embedded copy in the crate stays the source of truth.

Add `cargo semver-checks` (`obi1kenobi/cargo-semver-checks-action`) to `ci.yaml` once a version is
published; pre-1.0 breaking changes are allowed but must be in the changelog.

## Local dev infra — `deploy/compose/docker-compose.yaml`

```yaml
services:
  postgres:
    image: postgres:18-alpine
    environment: { POSTGRES_USER: reliar, POSTGRES_PASSWORD: reliar, POSTGRES_DB: reliar }
    ports: ["5432:5432"]
    healthcheck: { test: ["CMD-SHELL", "pg_isready -U reliar"], interval: 5s, retries: 10 }
  # pgbouncer: { image: edoburu/pgbouncer, environment: { DATABASE_URL: postgres://reliar:reliar@postgres/reliar, POOL_MODE: transaction }, ports: ["6432:6432"] }   # optional local pooler run; tests start their own via testcontainers
  # nats: { image: nats:2-alpine, command: ["-js"], ports: ["4222:4222"] }   # Phase 2
```

`scripts/test.sh` (`docker compose -f deploy/compose/docker-compose.yaml up -d --wait && DATABASE_URL=… cargo test --workspace`),
`scripts/lint.sh` (the ci.yaml steps), `scripts/release.sh` (dry-run `release-plz release --dry-run`).

## Definition of done (platform change)

- [ ] Workflows stay split by responsibility; every job runs on PRs; caches enabled; actions SHA-pinned; least-privilege `permissions`.
- [ ] All YAML files use `.yaml`; infra is under `deploy/docker` + `deploy/compose`; root `Cargo.toml` profiles explicit.
- [ ] CodeQL, dependency review, Scorecard, Dependabot and coverage are wired.
- [ ] House rules enforced as gates: `deny.toml` bans, core-purity grep, feature-powerset, MSRV job, doc `-D warnings`.
- [ ] `test.yaml` has a real Postgres and runs `sqlx prepare --check` on a freshly migrated DB.
- [ ] Release publishes in dependency order; changelog updated; `LICENSE` matches `license = "MIT"`.
- [ ] No secrets in workflow files; tokens via repository secrets only.
