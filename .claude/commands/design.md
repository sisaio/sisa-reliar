---
description: Architect produces the technical design, the public-API contract, and ADRs
argument-hint: <capability/story or SRS section>
---
Use the **architect** subagent to produce the technical design and the **public API contract** for:

$ARGUMENTS

It should design within `../sisa-reliar-backlog/docs/srs.md` and `team/engineering-conventions.md` (crate dependency rule,
small traits, native async fns in traits, static dispatch, hand-rolled errors), fix the contract as
documented stubs in the owning crate (or in the ADR if the crate doesn't exist yet), give the schema
direction and the **test matrix** per crate, list the work each **engineer** will do in parallel, and
record non-obvious decisions as ADRs in `docs/decisions/` (numbering continues after 0007).
Summarize the design, the contract path, and the ADRs.
