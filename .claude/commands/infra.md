---
description: Architect sets up / updates CI, security policy, release flow, or dev infra
argument-hint: <what to set up/change>
---
Use the **architect** subagent (platform hat) to set up or update the project platform for:

$ARGUMENTS

Follow the `ci-release` skill: `.github/workflows/{ci,test,security,codeql,scorecard,release}.yaml` (+ `dependabot.yaml`; always `.yaml`) split by
responsibility, `rust-toolchain.toml` + MSRV, `deny.toml`/`clippy.toml`/`rustfmt.toml`, the root
virtual-workspace `Cargo.toml` (workspace package/deps/lints), `deploy/compose/docker-compose.yaml` for local
Postgres (later NATS; pooler profile = PgDog), `cargo sqlx prepare --check`, and the
release/semver policy. Summarize what changed and how to run it locally.
