# Feature Workflow

How a capability goes from the human's request to accepted, tested library code. The PO runs this
loop; each stage has an **owner**, an **input**, an **output artifact**, and an **exit gate**.
Four agents: **po · architect · engineer · reviewer**.

```
Human ──▶ PO ──▶ Architect ──▶ ┌ Engineer (crate A) ┐ ──▶ Reviewer ──▶ PO ──▶ Human
                                └ Engineer (crate B) ┘
                 (contract-first lets engineers build crates in parallel)
```

## Size lanes (the PO picks one at intake)

- **Patch** (bugfix, doc fix, refactor with no public-API change): PO → one engineer → **reviewer**.
- **Standard** (a new capability inside existing crates): the pipeline below.
- **Full** (a new crate, a public-API or schema change, anything in SRS §45): the pipeline below
  **plus** an ADR, and the architect's CI/release work if `.github/`, `docker/` or release policy change.

## 0. Intake & stories — PO
- **In:** the human's request.
- **Do:** restate goal + "why"; locate the SRS sections and the phase (§42); write stories with
  **acceptance criteria** (Given/When/Then) tied to §43; state non-functional requirements
  (perf, cancellation, observability) and out-of-scope. Pick a size lane. Open cards.
- **Out:** a brief, `$BACKLOG_DIR/docs/stories/<id>-<slug>.md`, `$BACKLOG_DIR/docs/backlog/` cards.
- **Gate:** each story is testable and independently valuable; scope unambiguous (else ask the human).

## 1. Design & contract — Architect
- **In:** stories + AC + SRS sections.
- **Do:** decide crate boundaries and patterns without breaking the dependency rule; fix the
  **public contract** (trait/type signatures, bounds, error types, documented semantics such as
  lease, retry, ordering, duplicate window); give schema direction; set perf budgets; record
  **ADRs**. When CI/release/dev-infra change, do them (skill `ci-release`).
- **Out:** a design note, the contract (documented stubs in the owning crate or in the ADR),
  `docs/decisions/NNNN-*.md`, any `.github/`/`docker/`/`scripts/` changes.
- **Gate:** contract agreed; engineers can build their crates against it independently.

## 2. Build — Engineer(s), in parallel per crate
- **In:** contract + design.
- **Do:** implement per `rust-library-design`; in provider crates design the **schema** and write
  **migrations** + the `migrate()` API (skill `sqlx-postgres`); write the tests — `tests/` against the
  public API with fakes/paused time (skill `rust-lib-testing`), real-Postgres tests
  (skill `testcontainers`), benches for hot paths; rustdoc everything public; keep examples compiling.
- **Out:** crate code, migrations + `.sqlx/`, tests, benches, docs.
- **Gate:** DoD code + storage + tests sections pass.

## 3. Independent review — Reviewer
- **In:** the working diff.
- **Do:** review for **correctness vs the SRS semantics**, **public API/semver**, **house style**,
  **migration safety**, **security**, and **test quality** (AC coverage, assertion quality, missing
  edges, determinism, right layer, gap analysis). Read-only; findings by severity with `file:line`.
  In-scope → owning engineer; **out-of-scope** → PO for a new card.
- **Out:** a severity-ranked review or an explicit "clean — no blockers".
- **Gate:** no **blocker** findings.

## 4. Acceptance — PO
- **In:** the reviewed, tested change.
- **Do:** verify every AC and the full **Definition of Done**; summarize what shipped, decisions
  (ADRs), and follow-ups; update `CHANGELOG.md` *Unreleased* via the engineer if missing.
- **Out:** an acceptance summary for the human.
- **Gate:** DoD complete → deliver. Otherwise loop back to the owning stage.

## Notes
- **Thin slices.** One trait + one provider method + its tests, end to end, beats a whole crate at once.
- **Design has two owners.** Intent/guarantees/AC → **PO**; crate/trait/schema decisions →
  **architect**; execution → **engineer**.
- **Testing is coding.** Engineers write every automated test; the **reviewer** judges whether they
  prove anything; the **PO** verifies against the AC. There is no separate QA role.
- **The contract is law.** If reality forces a change, the `architect` updates it, records the ADR,
  and notifies every engineer building against it.
- **No future-phase scaffolding.** Crates/folders appear only when their phase starts (SRS §6).
