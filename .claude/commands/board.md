---
description: Render the markdown task board as a kanban
---
Render the current task board from `../sisa-reliar-backlog/docs/backlog/*.md` as a kanban. Read each card's frontmatter and
group them into columns **backlog · todo · in-progress · review · blocked · done**. Show each card as
`ID [owner · crate] title (priority)`. Call out blocked cards and their blockers, and anything with
unmet `depends_on`. If `../sisa-reliar-backlog/docs/backlog/` is empty or missing, say so.
