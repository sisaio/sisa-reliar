# Task Board (Markdown Kanban)

No Jira. Tasks are **markdown files with YAML frontmatter** under `../sisa-reliar-backlog/docs/backlog/` (the sibling backlog repo, added to the session with `--add-dir`) — one file per
work item. Agents `rg` the frontmatter to query the board, edit one field to move a card, and every
change is a reviewable git diff.

## File & id

- Path: `../sisa-reliar-backlog/docs/backlog/<ID>-<slug>.md` (e.g. `RELIAR-12-postgres-acquire-skip-locked.md`).
- **ID** = `RELIAR-` + incrementing integer. The **PO** assigns ids.
- Epics/features use the same format with `type: epic|feature` and children pointing via `parent`.
  Phases (SRS §42) are natural epics: `RELIAR-1 Phase 1 — Core + Postgres + Outbox`.

## Frontmatter schema (the contract agents read/write)

```yaml
---
id: RELIAR-12
title: Postgres acquire with FOR UPDATE SKIP LOCKED
type: story           # epic | feature | story | task | bug | spike | adr
status: todo          # backlog | todo | in-progress | review | blocked | done
owner: engineer       # po | architect | engineer | reviewer | unassigned
crate: reliar-store-postgres   # optional — the crate this touches
priority: p1          # p0 (now) | p1 | p2 | p3
parent: RELIAR-1      # optional — epic/feature this belongs to
depends_on: [RELIAR-8] # optional — blockers (ids)
srs: [§21, §24, §26]  # SRS sections this satisfies
labels: [outbox, postgres]
estimate: M           # XS | S | M | L | XL
created: 2026-09-03
updated: 2026-09-03
---
```

## Body (sections, in order)

```markdown
## Context
Why this exists; links to the SRS sections, ADRs, contract files, or the parent epic.

## Acceptance criteria
- [ ] Given <state>, when <action>, then <outcome>   (cite SRS §43 item where applicable)

## Tasks
- [ ] concrete sub-steps (checked off as done)

## Log
- 2026-09-03 (po) created, assigned to engineer
- 2026-09-03 (engineer) in-progress: migration + acquire query done, tests next
```

## Status flow (kanban columns)

```
backlog → todo → in-progress → review → done
                     └────────→ blocked ─┘   (blocked is a flag; set it + why in Log)
```

## Rules (every agent follows)

1. **Moving a card = edit `status`** + bump `updated` + append a dated **Log** line
   (`YYYY-MM-DD (role) note`). One card is `in-progress` per owner at a time where practical.
2. **`owner`** is who holds it *now*; on handoff, set the new owner and log it.
3. **`blocked`** requires a Log line stating the blocker and what's needed; the PO clears blockers.
4. A story reaches **`done`** only when its acceptance criteria and `team/definition-of-done.md`
   are met (reviewer audits the tests, PO accepts).
5. **`depends_on`** must be `done` before a card leaves `todo` (or the PO re-sequences).
6. The **PO** owns the backlog (ids, priorities, parents) and authors story bodies.
7. **Out-of-scope review findings become new cards** — the `reviewer` flags them (read-only), the
   PO opens a `bug`/`spike` card. In-scope findings stay in the review loop (the story's Log).
8. **ADR-worthy decisions get a `type: adr` card** so the architect's decision work is visible on the board.

## Querying the board

```bash
rg -l "status: in-progress" ../sisa-reliar-backlog/docs/backlog          # what's active
rg -l "owner: engineer"     ../sisa-reliar-backlog/docs/backlog           # a role's queue
rg -l "crate: reliar-core"  ../sisa-reliar-backlog/docs/backlog           # a crate's queue
rg -l "status: blocked"     ../sisa-reliar-backlog/docs/backlog           # blockers
```

## Branches & commits

Branch names are `type/short-description` (`feat/nats-mapper`, `fix/outcome-write-pacing`, `chore/test-lint-allows`,
`docs/guides`; in the backlog repo always `docs/…`). The card id goes in the commit message or PR body, not the
branch name. Commits follow conventional commits; AI co-authors are credited with `Co-Authored-By:` trailers.

Use `/board` to render the columns and `/task` to create a new card.
