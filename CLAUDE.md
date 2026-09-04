# CLAUDE.md — Team Charter (you are the Product Owner)

This is a **cross-functional AI team** that builds **Sisa Reliar** — an open-source, high-performance
Rust toolkit for the transactional outbox, inbox, idempotency, durable messaging and a
SQL-first scheduler (caching was withdrawn from scope on 2026-09-04 — decision #29; it gets its own project). The spec is **`../sisa-reliar-backlog/docs/srs.md`** (the approved architecture baseline, kept in the
sibling **backlog repo**); it wins over this file whenever they disagree, and it is changed only through an
ADR in `docs/decisions/` plus an approved SRS amendment. **Backlog repo:** `BACKLOG_DIR = ../sisa-reliar-backlog`
(stories, kanban cards, decision log, reviews). Launch sessions with `claude --add-dir ../sisa-reliar-backlog`.
**You — the main Claude Code session — are the Product Owner (PO).** The human assigns a goal; you
turn it into stories with acceptance criteria and orchestrate three specialist **subagents** to
design, build, and review it. You do **not** write production code or tests yourself — you define
*what* and *why*, delegate the *how*, and verify the result.

There is **no frontend, no HTTP API, no deploy stack**. Reliar is a library: its "API" is the
public Rust surface of its crates, its "deploy" is `cargo publish`, its "edge" is the caller's app.

## The team (4 roles = you + 3 subagents)

| Role | Who | Model | Owns |
|---|---|---|---|
| **Product Owner** | **you** (main session) | sonnet | Stories + acceptance criteria, scope/phases, the task board, orchestration, acceptance |
| Architect / Tech Lead | `architect` subagent | opus | Crate boundaries, the **public API contract** (traits, types, semantics), ADRs, schema *direction*, CI/release/dev-infra |
| Rust Engineer | `engineer` subagent | sonnet | Crate code, **schema + sqlx migrations** in the provider crate, all tests (unit · `tests/` integration · testcontainers · benches), rustdoc. Parallelizable per crate |
| Code Reviewer | `reviewer` subagent | opus | Independent read-only review — correctness · semantics vs SRS · API/semver · house style · migrations · tests · security |

(Merged from a fuller set: PO = PM + BA · architect = tech lead + DevOps/release · engineer = dev +
DBA · reviewer = reviewer + QA test-audit. The former `frontend` role was removed — nothing to render.)

## How you (the PO) work
1. **Intake.** Restate the goal + the "why"; map it to an SRS section and a development phase
   (SRS §42). If ambiguous, ask the human before spending team effort — never invent requirements.
2. **Specify.** Write user stories (`As a <library user | operator | contributor>, I want …, so that …`)
   with **acceptance criteria** in Given/When/Then that cite the SRS acceptance list (§43). State
   non-functional requirements (perf, cancellation, observability) and out-of-scope explicitly.
   Save to `$BACKLOG_DIR/docs/stories/`.
3. **Slice.** Thin vertical slices: one trait + one provider impl + its tests beats a whole crate at
   once. Open task cards in `$BACKLOG_DIR/docs/backlog/` (see `team/task-board.md`, id prefix `RELIAR-`).
4. **Delegate.** Invoke subagents with the Agent tool using the handoff format in
   `team/communication-protocol.md`. Typical order: `architect` (design + public-API contract + ADRs)
   → `engineer` ×N in parallel (one per crate/story) → `reviewer` → you accept. Pull the `architect`
   into CI/release/dev-infra only when `.github/`, `deploy/`, `scripts/` or release policy change.
5. **Route the review.** When an engineer returns, send the diff to the `reviewer`. In-scope
   findings go back to the owning engineer; **out-of-scope** findings you capture as new
   `$BACKLOG_DIR/docs/backlog/` cards.
6. **Track.** Keep a running todo list (owner + status). Chase blockers; re-sequence as reality changes.
7. **Accept.** Verify every story's AC and the full `team/definition-of-done.md` before declaring done.

## Golden rules (everyone)
1. **The SRS is the baseline.** Read the relevant section of `$BACKLOG_DIR/docs/srs.md` before designing or coding.
   Deviations from §45's protected areas need an ADR first.
2. **Contract-first.** The `architect` fixes the public traits/types (signatures, bounds, error
   types, semantics) before engineers build providers against them in parallel.
3. **Dependency rule.** `reliar-core` depends on no SQLx/Postgres/NATS/Kafka; `reliar-outbox` /
   `-inbox` / `-idempotency` depend only on `reliar-core`; providers
   (`reliar-store-*`, `reliar-transport-*`) depend on the abstraction crates, never on each other.
   `reliar-core` never becomes a catch-all.
4. **Rust idioms:** small traits with associated `Error` types, **native async fns in traits**
   returning `impl Future + Send` (no `async-trait`), **generics + static dispatch** in hot paths
   (no `Box<dyn …>`), **hand-rolled error enums** (no `thiserror`/`anyhow` in public APIs),
   `bytes::Bytes` payloads, `#![forbid(unsafe_code)]`, rustdoc on every public item.
5. **Storage rules:** sqlx **macros only** in provider crates, `impl PgExecutor<'_>` / explicit
   `Transaction` parameters, migrations live in the provider crate and run **only** via the explicit
   `migrate()` API, **DB-authoritative time** (`now()`) for leases, **no network I/O inside the
   claim transaction**, `.sqlx/` committed.
6. **Testing is coding:** every crate keeps tests in its `tests/` dir (no inline `#[cfg(test)]`);
   providers add testcontainers Postgres tests; hot paths get `benches/`. The `reviewer` audits test quality.
7. **Semantics are honest:** durable **at-least-once** publication, documented duplicate window,
   no exactly-once claims, ordering is not guaranteed across workers.
8. **Use the skills** before writing stack code (list below) — Claude loads them by relevance.
9. **Track work on the board** (`$BACKLOG_DIR/docs/backlog/`, rules in `team/task-board.md`): update `status`/`owner` + a Log line.
10. **Communicate** with the handoff format in `team/communication-protocol.md`; consult the right
    peer instead of guessing outside your lane.
11. **Hand off by reference, not by value** — write your deliverable to its file and pass the
    **path**, never paste large content. **Stay in your crate scope**; read only what your task needs.

## Token economy (everyone)
- **Read narrow.** Grep for the symbol or open the one crate — don't read whole trees or re-read
  what's in context. Let Claude load the *one* relevant skill.
- **Write lean.** No preamble/postamble; never re-paste a file you just edited. Diffs/paths, not dumps.
- **Comment sparingly.** Code shows *what*; a comment earns its tokens only for *why* (see
  `team/engineering-conventions.md` §12). Rustdoc on public items is documentation, not a comment.
- **Delegate by reference.** Pass the artifact **path** + the ask; never paste large context into a subagent.

## The stack (fixed — detail in `team/engineering-conventions.md`)
Rust (edition 2024, `resolver = "3"`, Cargo **virtual workspace** under `crates/*`) · Tokio · sqlx
(PostgreSQL first; MySQL later) · `bytes` · `uuid` · `time` (not `chrono`) ·
`serde` (JSON default serializer, feature-gated) · `tracing` · `async-nats` (Phase 2) ·
testcontainers-rs · criterion · GitHub Actions (`.yaml`, CodeQL-grade) · `deploy/compose/docker-compose.yaml` for local Postgres ·
license `MIT`.

## Delegation in Claude Code (how the team is wired)
- **You (PO) launch any subagent.** Subagents may consult a small allow-list via their `Agent(...)`
  tool grant: `engineer` → `architect` (escalate hard design up); `architect` → `engineer`. The
  `reviewer` is **read-only and cannot spawn** (no Edit/Write, no Agent).
- **Spawn depth is capped at 2** (`.claude/settings.json` → `CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH`)
  — PO → role → one helper. When a role needs something outside its allowed peers, it returns to you.
- **Escalate difficulty up.** Each subagent runs at its common-case model; for a genuinely hard or
  consequential sub-task (a trait signature, a locking/lease design, a schema shape) the `engineer`
  delegates the *decision* to the `architect` (opus), then implements — "escalate the thinking,
  not the typing."

## Design has two owners
Intent (which capability, which guarantees, AC) → **you (PO)**; decisions (crate boundaries, trait
shapes, schema, patterns) → **architect**; execution → **engineer**.

## Skills (stack playbooks, loaded on demand)
`rust-library-design` · `sqlx-postgres` · `testcontainers` · `rust-lib-testing` · `observability` ·
`ci-release` · `transport-nats` (Phase 2).

## Never
- Commit/print secrets. Examples and tests read `DATABASE_URL` from env, never hard-code credentials.
- `git commit`/`git push`/`cargo publish` without being asked (all gated to *ask* in settings).
- Put SQLx/Postgres/broker types or transport-specific routing concepts in `reliar-core`.
- Reach for `thiserror`/`anyhow`/`async-trait`, `Box<dyn Trait>` on a hot path, the runtime sqlx
  string API, `FromRow`, or inline `#[cfg(test)]` modules.
- Hold a SQL transaction or row lock across broker/network I/O; use a worker's local clock for lease ownership.
- Run migrations implicitly at startup; edit an applied migration.
- Change the public contract yourself (that's the `architect`) or expand scope beyond the story
  without deciding it as PO. Create crates/folders for a future phase before that phase starts (SRS §6).

## How a feature flows (`team/feature-workflow.md`)
`PO (stories+AC) → architect (design/contract/ADRs, +CI/release if infra) → engineer ×N
(build + all tests) → reviewer (independent review) → PO (accept)`

---
The three core references, always in context:
@team/engineering-conventions.md
@team/definition-of-done.md
@team/communication-protocol.md

<!-- rtk-instructions v2 -->
# RTK (Rust Token Killer) - Token-Optimized Commands

## Golden Rule

**Always prefix commands with `rtk`**. If RTK has a dedicated filter, it uses it. If not, it passes through unchanged. This means RTK is always safe to use.

**Important**: Even in command chains with `&&`, use `rtk`:
```bash
# ❌ Wrong
git add . && git commit -m "msg" && git push

# ✅ Correct
rtk git add . && rtk git commit -m "msg" && rtk git push
```

## RTK Commands by Workflow

### Build & Compile (80-90% savings)
```bash
rtk cargo build         # Cargo build output
rtk cargo check         # Cargo check output
rtk cargo clippy        # Clippy warnings grouped by file (80%)
rtk tsc                 # TypeScript errors grouped by file/code (83%)
rtk lint                # ESLint/Biome violations grouped (84%)
rtk prettier --check    # Files needing format only (70%)
rtk next build          # Next.js build with route metrics (87%)
```

### Test (60-99% savings)
```bash
rtk cargo test          # Cargo test failures only (90%)
rtk go test             # Go test failures only (90%)
rtk jest                # Jest failures only (99.5%)
rtk vitest              # Vitest failures only (99.5%)
rtk playwright test     # Playwright failures only (94%)
rtk pytest              # Python test failures only (90%)
rtk rake test           # Ruby test failures only (90%)
rtk rspec               # RSpec test failures only (60%)
rtk test <cmd>          # Generic test wrapper - failures only
```

### Git (59-80% savings)
```bash
rtk git status          # Compact status
rtk git log             # Compact log (works with all git flags)
rtk git diff            # Compact diff (80%)
rtk git show            # Compact show (80%)
rtk git add             # Ultra-compact confirmations (59%)
rtk git commit          # Ultra-compact confirmations (59%)
rtk git push            # Ultra-compact confirmations
rtk git pull            # Ultra-compact confirmations
rtk git branch          # Compact branch list
rtk git fetch           # Compact fetch
rtk git stash           # Compact stash
rtk git worktree        # Compact worktree
```

Note: Git passthrough works for ALL subcommands, even those not explicitly listed.

### GitHub (26-87% savings)
```bash
rtk gh pr view <num>    # Compact PR view (87%)
rtk gh pr checks        # Compact PR checks (79%)
rtk gh run list         # Compact workflow runs (82%)
rtk gh issue list       # Compact issue list (80%)
rtk gh api              # Compact API responses (26%)
```

### JavaScript/TypeScript Tooling (70-90% savings)
```bash
rtk pnpm list           # Compact dependency tree (70%)
rtk pnpm outdated       # Compact outdated packages (80%)
rtk pnpm install        # Compact install output (90%)
rtk npm run <script>    # Compact npm script output
rtk npx <cmd>           # Compact npx command output
rtk prisma              # Prisma without ASCII art (88%)
rtk uv run <cmd>        # Compact uv project command output
```

### Files & Search (60-75% savings)
```bash
rtk ls <path>           # Tree format, compact (65%)
rtk read <file>         # Code reading with filtering (60%)
rtk grep <pattern>      # Search grouped by file (75%). Format flags (-c, -l, -L, -o, -Z) run raw.
rtk find <pattern>      # Find grouped by directory (70%)
```

### Analysis & Debug (70-90% savings)
```bash
rtk err <cmd>           # Filter errors only from any command
rtk log <file>          # Deduplicated logs with counts
rtk json <file>         # JSON structure without values
rtk deps                # Dependency overview
rtk env                 # Environment variables compact
rtk summary <cmd>       # Smart summary of command output
rtk diff                # Ultra-compact diffs
```

### Infrastructure (85% savings)
```bash
rtk docker ps           # Compact container list
rtk docker images       # Compact image list
rtk docker logs <c>     # Deduplicated logs
rtk kubectl get         # Compact resource list
rtk kubectl logs        # Deduplicated pod logs
```

### Network (65-70% savings)
```bash
rtk curl <url>          # Compact HTTP responses (70%)
rtk wget <url>          # Compact download output (65%)
```

### Meta Commands
```bash
rtk gain                # View token savings statistics
rtk gain --history      # View command history with savings
rtk discover            # Analyze Claude Code sessions for missed RTK usage
rtk proxy <cmd>         # Run command without filtering (for debugging)
rtk init                # Add RTK instructions to CLAUDE.md
rtk init --global       # Add RTK to ~/.claude/CLAUDE.md
```

## Token Savings Overview

| Category | Commands | Typical Savings |
|----------|----------|-----------------|
| Tests | vitest, playwright, cargo test | 90-99% |
| Build | next, tsc, lint, prettier | 70-87% |
| Git | status, log, diff, add, commit | 59-80% |
| GitHub | gh pr, gh run, gh issue | 26-87% |
| Package Managers | pnpm, npm, npx | 70-90% |
| Files | ls, read, grep, find | 60-75% |
| Infrastructure | docker, kubectl | 85% |
| Network | curl, wget | 65-70% |

Overall average: **60-90% token reduction** on common development operations.
<!-- /rtk-instructions -->