---
description: Independent review of the current working changes
argument-hint: (optional focus)
---
Use the **reviewer** subagent to review the current working changes against `../sisa-reliar-backlog/docs/srs.md`,
`team/engineering-conventions.md` and `team/definition-of-done.md`.

Optional focus: $ARGUMENTS

It should report findings by severity (**blocker / major / minor**) with `file:line`, checking:
correctness vs the outbox guarantees (leases on DB time, `locked_by` guards, no I/O inside the claim
tx, retry/dead semantics, cancellation), public API/semver quality, the crate dependency rule and
house style, migration safety, security (no payload logging, no `unwrap` on wire/DB data), and
**test quality** (AC coverage, missing scenarios, determinism, right layer). In-scope findings go to
the owning engineer; **out-of-scope** findings it flags for you (the PO) to open as new
`../sisa-reliar-backlog/docs/backlog/` cards. If there are no changes, say so.
