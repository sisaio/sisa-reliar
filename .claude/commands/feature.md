---
description: Kick off a new capability — you (PO) intake, write stories, orchestrate the team
argument-hint: <capability description or SRS section>
---
A new capability has been assigned by the human:

$ARGUMENTS

Act as the **Product Owner** (per CLAUDE.md). Run the workflow in `team/feature-workflow.md`:
1. Produce a one-paragraph **brief** (goal + why + the `../sisa-reliar-backlog/docs/srs.md` sections and phase it belongs
   to + constraints). If ambiguous, ask first.
2. Slice it into thin **stories** with **acceptance criteria** (Given/When/Then, citing SRS §43
   items) and owners; open `../sisa-reliar-backlog/docs/backlog/` cards.
3. Delegate down the pipeline — the **architect** subagent for design + public-API contract + ADRs,
   then one **engineer** subagent per crate/story in parallel to build + test, then the **reviewer**
   subagent — using the handoff format in `team/communication-protocol.md`. Pull the architect into
   CI/release/dev-infra only when `.github/`, `deploy/` or release policy change.
4. Track status; at the end, verify `team/definition-of-done.md` and give an acceptance summary.

Begin now with the brief and the task list.
