# Communication Protocol

The roles are separate agents. They collaborate by **delegating** (Agent tool) and by **handing off
structured artifacts**. Clear communication is the difference between a team and four agents
talking past each other.

## Handoff format (use this whenever you pass work to another role)

```
## Handoff → <role>
**Context:** <what this is, link the story id / SRS section / files>
**What I did:** <summary of your output + where it lives (path)>
**What I need from you:** <the concrete ask>
**Contract / assumptions:** <trait signatures, types, schema shape, invariants, edge cases>
**Acceptance:** <how we'll know your part is correct>
**Open questions / risks:** <anything unresolved>
```

Keep it tight. **Hand off by reference, not by value:** write your deliverable to its file (the
artifact) and pass the **path** + the ask — never paste the full content into the message. The
receiving agent reads the file itself, so delegation stays small and token cost stays low.

## Artifacts & repo scope by role

| Role | Writes (hand off by path) | Repo scope (edits) |
|---|---|---|
| PO | `$BACKLOG_DIR/docs/stories/`, `$BACKLOG_DIR/docs/backlog/*.md` cards, the brief, `$BACKLOG_DIR/docs/analysis/`, `$BACKLOG_DIR/docs/srs.md` (only with the human) | `../sisa-reliar-backlog/docs/`, `docs/guides/` |
| Architect | `docs/decisions/` ADRs, `docs/architecture/`, the **public API contract** (trait/type signatures written into the owning crate's `src/` as documented stubs or into the ADR), `.github/`, `deploy/`, root `Cargo.toml`/`deny.toml`/`clippy.toml`/`rustfmt.toml`, `CHANGELOG.md` release notes | `docs/`, `.github/`, `deploy/`, root config (reads all crates) |
| Engineer | crate code, **schema + migrations** in provider crates, `.sqlx/`, tests, benches, examples, crate READMEs, `docs/guides/` | `crates/<assigned>/`, `examples/`, `benches/`, `docs/guides/`, `$BACKLOG_DIR/docs/backlog/` (own card) |
| Reviewer | the review findings | **read-only** — the diff |

Every role may edit **its own card in `$BACKLOG_DIR/docs/backlog/`** (status/log). Read only what your task
needs — don't scan the whole repo.

## Who talks to whom (the collaboration graph)

- **PO ↔ everyone.** PO assigns, sequences, and accepts. The human talks to the PO.
- **PO → architect:** stories + acceptance criteria + the SRS sections in play; **architect → PO:**
  constraints/feasibility that change scope or phase.
- **architect → engineer:** the contract (trait/type signatures + semantics) + schema direction;
  **engineer → architect:** schema/perf implications, contract questions, Rust type-system blockers.
- **engineer ↔ engineer:** align through the contract file, never by private agreement.
- **engineer → reviewer:** submit the diff. **reviewer → engineer:** in-scope findings by severity;
  **reviewer → PO:** blockers **and out-of-scope findings**, which the PO opens as new cards.
  The reviewer is **read-only and independent** — it flags, never edits or cards.

A role should **consult the owner** of an area rather than guessing: schema/data → **engineer**
(direction from the architect), contract/architecture/CI/release → **architect**, scope/AC/phase →
**PO**, the review verdict → **reviewer**.

## Delegation rules

- The **PO** (the main Claude Code session) launches subagents. A subagent may consult only the
  peers named in its own **`Agent(...)` tool grant** (`.claude/agents/<name>.md` frontmatter) —
  `engineer` → `architect`, `architect` → `engineer`.
- Delegation depth is capped at 2 (`CLAUDE_CODE_MAX_SUBAGENT_SPAWN_DEPTH` in `.claude/settings.json`).
- When you need something outside your allowed peers, **route it back through the PO**.
- The **`reviewer`** is read-only and has no `Agent` grant — it cannot spawn anyone.
- **Escalate difficulty up.** The `engineer` runs on `sonnet`; when a sub-task is genuinely hard or
  consequential — a public trait signature, lease/locking semantics, a schema/index shape, a
  cancellation-safety question — delegate the **decision** to the **`architect`** (`opus`) by
  reference, then do the work yourself. Escalate the *thinking*, not the *typing*. (The `reviewer`
  never escalates — delegating to the `architect` who designed the code would break its independence.)

## Decisions & disagreements

- Architecture/contract/CI/release decisions are the **architect's** call, recorded as an **ADR**
  (`docs/decisions/NNNN-title.md`, numbering continues from the SRS's 0001–0007). Scope/phase
  decisions are the **PO's**. Schema detail is the **engineer's** within the architect's direction.
- Disagree with data/evidence (a failing test, an EXPLAIN plan, a bench), not vibes. If unresolved,
  escalate to the PO, who decides or takes it to the human.

## Status & blockers

- Report status as: **done / in-progress / blocked** + one line each.
- Surface blockers **immediately** to the PO with what's needed to unblock.
- Never silently drop scope or invent requirements — ask the PO.
