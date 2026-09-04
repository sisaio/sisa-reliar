# Definition of Done

A story is **done** only when every box below is true. The PO checks this before accepting; the
`reviewer` audits the tests. "It compiles" is not done.

## Every story

- [ ] Acceptance criteria in the story are all met (PO confirms) and map to SRS §43 items (`$BACKLOG_DIR/docs/srs.md`) where applicable.
- [ ] Code follows `team/engineering-conventions.md` (crate dependency rule, traits/static dispatch, errors, naming).
- [ ] The relevant **skill** was followed for each area touched.
- [ ] No SQLx/Postgres/broker types or transport-specific routing in `reliar-core`; no provider depends on another provider.
- [ ] Any change to an SRS §45 protected area has an ADR in `docs/decisions/` (context → decision → consequences).

## Code (Rust)

- [ ] Public items have rustdoc; `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` clean; `#![forbid(unsafe_code)]`.
- [ ] `cargo fmt --all --check` · `cargo clippy --workspace --all-targets --all-features -- -D warnings` · `cargo test --workspace` green.
- [ ] Feature flags additive and checked with `cargo hack check --feature-powerset` (crates with features).
- [ ] No `async-trait`, no `thiserror`/`anyhow` in library crates, no `Box<dyn …>` on a hot path, no inline `#[cfg(test)]`.
- [ ] Error enums are hand-rolled, `#[non_exhaustive]`, with `source()`; publish failures classified transient/permanent.
- [ ] Tasks respect cancellation (`CancellationToken`), bounded concurrency, and shut down gracefully.

## Storage (provider crates)

- [ ] Schema changes are new forward-only migrations in the provider crate; run only through the explicit `migrate()` API; lock-safe.
- [ ] sqlx macros only; `.sqlx/` regenerated (`cargo sqlx prepare`) and committed; `cargo sqlx prepare --check` passes.
- [ ] Leases/due-time comparisons use DB `now()`; completion/failure guarded by `locked_by`; no network I/O inside the claim tx.
- [ ] Indexes justified by the actual claim/cleanup query shape (EXPLAIN shown in the PR/card when new).

## Observability

- [ ] Spans use the `reliar.<crate>.<op>` names; payloads and custom headers not logged by default.
- [ ] No high-cardinality ids as metric labels; metric hooks have a no-op default.

## Tests (devs write, reviewer audits)

- [ ] Every crate's tests live in `tests/` and exercise the **public API**; shared helpers in `tests/common/`.
- [ ] Abstraction crates: fakes + paused-time tests cover dispatcher behavior (retry, dead, cancellation, concurrency bounds).
- [ ] Provider crates: real-Postgres tests (testcontainers / `DATABASE_URL`) cover atomic enqueue, `SKIP LOCKED` concurrency,
      lease expiry + recovery, crash-after-publish duplicate window, retry/backoff, dead, purge.
- [ ] Every acceptance criterion has a test that fails if it breaks; assertions are meaningful; tests are deterministic
      (no wall-clock sleeps, isolated data) and run in CI.
- [ ] Hot paths touched have a criterion bench in `benches/` (or an existing one still passes).

## Review

- [ ] **Independent review passed** — the `reviewer` found no **blocker** findings on the diff (code, migrations, tests, docs).
- [ ] Any **out-of-scope** findings the reviewer raised are captured as new cards in `$BACKLOG_DIR/docs/backlog/` (opened by the PO).

## Delivery

- [ ] Docs updated: story status, ADR if a decision was made, crate README/`lib.rs` docs, `docs/architecture/` or `docs/guides/`
      when user-facing behavior changed, `CHANGELOG.md` entry under *Unreleased*.
- [ ] Examples under `examples/` still compile (they are workspace members).
- [ ] Change is behind a branch/PR; commit messages follow conventional commits.
- [ ] PO has confirmed scope matches the story — no silent scope creep, no future-phase crates created early.
