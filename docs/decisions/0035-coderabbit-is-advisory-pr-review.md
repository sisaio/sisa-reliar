# ADR 0035 — CodeRabbit reviews every pull request, as advisory input only

**Status:** Accepted — 2026-09-05
**SRS:** §41 (quality gates and CI), §38 (how decisions are recorded)
**Related:** ADR 0022 (CI and YAML policy), ADR 0034 (release flow);
`team/definition-of-done.md`, `team/communication-protocol.md`, skill `ci-release`.
**Supersedes:** nothing.

## Context

Reliar's rules are unusually specific and mostly invisible to a generic linter: `reliar-core` may
not name a storage engine or broker; traits use native async fns, never `async-trait`; errors are
hand-rolled and `#[non_exhaustive]`; the claim transaction may not perform network I/O; leases use
DB `now()`; tests live in `tests/` and exercise the public API. CI enforces the mechanical subset
(`fmt`, `clippy -D warnings`, the feature powerset, `deny.toml`'s bans, the core-purity `cargo tree`
grep, `sqlx prepare --check`). The rest has so far depended on a human maintainer or the team's
`reviewer` agent noticing it on the diff — which scales badly and is exactly the class of rule a
per-path review instruction can restate cheaply.

A third-party review bot also raises two questions worth deciding once rather than per pull request:
whether it may hold the merge gate, and where its rules live.

## Decision

**CodeRabbit reviews every pull request against a committed `.coderabbit.yaml` at the repo root, and
its findings are advisory.**

1. **Configuration is committed and reviewable.** `.coderabbit.yaml` (`.yaml`, per conventions §10a)
   is the whole configuration; nothing behavioural is set in the CodeRabbit web UI, so a change to
   how reviews work arrives as a diff.
2. **The house rules are restated as `path_instructions`**, one entry per area — `reliar-core`
   purity, the crate-wide Rust rules, the Postgres provider's sqlx/lease/migration rules, the NATS
   mapping rules, the test rules, `.github/`, `docs/decisions/`, `deploy/`. The instructions are a
   *projection* of `team/engineering-conventions.md` and `team/definition-of-done.md`, which stay
   the source of truth; when a rule changes there or in an ADR, `.coderabbit.yaml` changes in the
   same pull request.
3. **Advisory, never a gate.** `request_changes_workflow: false`. CodeRabbit does not approve, does
   not request changes, and no branch-protection check depends on it. The merge gate remains CI plus
   the Definition of Done, and the independent verdict remains the human maintainer's and the
   `reviewer` agent's. The bot is a fourth pair of eyes, not a fourth role — it never edits code and
   never opens backlog cards.
4. **Profile `chill`, not `assertive`.** Style is already settled by `rustfmt` and `clippy` in CI, so
   extra nit volume would cost review attention without adding signal; what we want from the bot is
   house-rule and semantics findings.
5. **Only tools that match this repo are enabled** — `actionlint`, `zizmor`, `yamllint`,
   `markdownlint`, `shellcheck`, `hadolint`, `gitleaks`, `squawk` (migration lock-safety) and
   `github-checks`. `clippy` is disabled *in CodeRabbit*: CI runs it on the pinned toolchain across
   the feature powerset with the committed `.sqlx/` cache, and a second run in an environment we do
   not control would produce duplicate or stale findings against an authoritative gate. Scanners
   that duplicate an existing gate (`osvScanner`, `trufflehog`, `trivy`) are off for the same reason.
6. **Generated content is out of scope**: `path_filters` excludes `.sqlx/**`, `Cargo.lock` and
   `target/**`.

## Consequences

- A pull request gets a first review in minutes, and the rules a newcomer would otherwise learn by
  being corrected are stated on the diff, at the line, with the rule named.
- The bot reads the repository. Reliar is public and holds no secrets (tests and examples read
  `DATABASE_URL`/`NATS_URL` from the environment), so the exposure is the source we already publish;
  `gitleaks` stays on as the standing check that this remains true.
- `.coderabbit.yaml` is a second copy of rules that live in `team/` and can drift from them. It is a
  deliberate, bounded duplication: the instructions stay short and name the file that governs, and
  the `docs/decisions/**` instruction makes drift itself reviewable. Rule changes must update both.
- False positives will happen. They are answered in the thread and cost a maintainer a sentence;
  because nothing blocks on them, a wrong finding can never hold a release.
- Vendor dependence is limited to review commentary. Deleting `.coderabbit.yaml` and uninstalling the
  app returns the project to exactly today's gates.

## Alternatives considered

- **No bot.** Keeps the toolchain minimal, but leaves every non-mechanical rule to human attention —
  the failure mode the team already sees on large diffs.
- **Let CodeRabbit request changes (`request_changes_workflow: true`).** Gives the rules teeth, at
  the price of a third party being able to block a merge on a probabilistic finding, and of blurring
  the `reviewer` agent's independent verdict (`team/communication-protocol.md`). Rejected.
- **Profile `assertive`.** More findings, mostly style that `rustfmt`/`clippy` already decide.
  Rejected as noise; revisit if `chill` proves to miss house-rule violations.
- **Encode the rules as `ast-grep` rules instead.** Deterministic and gate-able, and worth doing for
  a few rules later (it is already how `deny.toml` handles `async-trait`/`thiserror`), but it cannot
  express the semantic rules — "no network I/O inside the claim transaction", "this test would not
  catch a mutation" — which is precisely where the review value is.
- **A second CI job running a model over the diff.** Same benefit, but we would own the prompt, the
  cost and the rate limits, and it would need a token with repository write access to comment.
