# ADR 0025 — Provider crates may carry their driver's MSRV; pure crates may not

**Status:** Accepted — 2026-09-04
**SRS:** §7.1, §40, §44
**Builds on:** [ADR 0024](0024-msrv-1-88-and-msrv-policy.md) (workspace MSRV 1.88 and the policy behind it)
**Related:** RELIAR-20 (this ADR), RELIAR-16 (the crate that makes it bite)

## Context

ADR 0024 moved the workspace MSRV to **1.88** because that was the lowest floor on which a `time`
release patched for RUSTSEC-2026-0009 will build. It deliberately left one question open, and this
ADR answers it.

`sqlx` **0.9.0** declares `rust-version = "1.94.0"` — six releases above the workspace floor and
well above anything Reliar's own code needs. `reliar-store-postgres` cannot exist without it. The
moment RELIAR-16 lands the crate, the `msrv` CI job (`cargo +1.88 check --workspace --all-features`)
goes red, and it goes red for a reason that has nothing to do with Reliar's source.

The choice is between one MSRV for the workspace and a per-crate MSRV:

| Option | Consequence |
|---|---|
| (a) `reliar-store-postgres` declares `rust-version = "1.94"`; the `msrv` job excludes it | Two floors to explain; `reliar-core` and `reliar-outbox` stay reachable on 1.88 |
| (b) Raise the whole workspace to 1.94 | One number, but `reliar-core` — which depends on `bytes`, `uuid`, `time`, `serde`, `tracing` and nothing else — would demand a toolchain six releases newer than any of its dependencies need |

Option (b) has a specific cost that is easy to miss. `reliar-core` and `reliar-outbox` are the crates
a user takes when they want the envelope model or the dispatcher **with their own store** — an
in-memory one, their own SQL, a provider Reliar has not written. Those users pay sqlx's MSRV for a
dependency they never compile. A library that makes its purest crate its least portable one has the
dependency rule (conventions §2) pointing the right way and the MSRV pointing the wrong way.

## Decision

**Pure crates keep the workspace MSRV. Provider crates may declare the MSRV of their driver.**

1. `[workspace.package].rust-version` stays the floor for every crate that does not wrap a driver:
   `reliar-core`, `reliar-outbox`, and the `-inbox` / `-idempotency` crates when they
   exist. They **may not** raise it locally; a pure crate that needs a newer toolchain is a signal to
   move the workspace floor through an ADR, per 0024.

2. A **provider crate** (`reliar-store-*`, `reliar-transport-*`) may override `rust-version` in its
   own `[package]` to its driver's MSRV. `reliar-store-postgres` declares `rust-version = "1.94"`
   for sqlx 0.9. The override is never lower than the workspace floor and never higher than its
   driver actually requires.

3. **The override is documented where a user looks.** Every crate that overrides states its MSRV and
   the driver forcing it in its README and its `lib.rs` docs — "MSRV 1.94, set by sqlx 0.9". An
   undocumented per-crate MSRV is a build failure with no explanation attached.

4. **CI.** The `msrv` job **computes** its exclusions from `cargo metadata`: every workspace
   package whose declared `rust-version` exceeds the workspace floor. Excluded crates are still
   fully built by the `check` job on the pinned toolchain — the exclusion narrows *which toolchain*
   covers them, never *whether* they are covered.

   *(Amended 2026-09-04, RELIAR-19 review 1.)* The list was originally the hard-coded
   `--exclude reliar-store-postgres`, which broke as soon as `examples/axum-outbox` became a member:
   `cargo --exclude` only skips building a package as a **root**, so the example pulled the provider
   straight back in and the job failed. The job therefore also **fails** when an included member
   depends on an above-floor package without declaring a `rust-version` of its own — such a package
   can neither build on the MSRV toolchain nor be excluded from it, so leaving it undeclared is an
   error rather than something to work around.

   *(Amended 2026-09-04, human decision #32.)* That logic lived in `scripts/msrv-exclusions.py`;
   `scripts/` is gone (ADR 0022) and the logic is now **inline `bash` + `jq` in the `msrv` step**,
   which removes Python as a build-time dependency of CI. Two properties survived the move
   verbatim, because both fail *silently*: versions are padded to exactly three components before
   comparison (so `1.88` and `1.88.0` compare equal — without the padding jq's array comparison
   reads `1.88.0` as above a `1.88` floor, excludes every member, and leaves the job green having
   built nothing), and that equality is **asserted in the step** before the exclusion list is
   computed. The list itself is still computed from `cargo metadata --no-deps`, never hard-coded.

   Alongside it, the package that made this bite twice is gone: `tools/reliar-migrate` declared
   `rust-version = "1.94"` purely to satisfy the dependent check above, and is now
   `crates/reliar-store-postgres/examples/migrate.rs` — a target of the crate whose MSRV it
   already shares, so there is one fewer floor to declare and keep in step.

5. **Raising a provider's MSRV** follows the same rule as the workspace floor (0024): a minor
   release with a `CHANGELOG.md` entry, never a patch. A provider crate's MSRV tracking its driver's
   is expected and is not on its own a reason to hold back a dependency upgrade.

## Consequences

- Reliar publishes more than one MSRV. That is a documentation burden, discharged by (3) and by a
  table in the workspace README once the provider crate exists.
- `reliar-core` and `reliar-outbox` stay usable on 1.88 by anyone bringing their own store, which is
  a supported way to use Reliar rather than an edge case.
- The `msrv` job's exclusion list is a thing that can rot. It is guarded on crate existence and the
  guard is the reviewer's checkpoint whenever a provider crate is added.
- If a *second* provider lands with a different driver MSRV, the exclusion list grows rather than the
  policy changing — the rule already covers it.
- `cargo hack --feature-powerset` and the `check` job run on the pinned toolchain (1.98) and are
  unaffected.

## Alternatives considered

- **Raise the workspace to 1.94** (option b). Rejected for the reason above: it taxes the crates that
  do not use sqlx. Worth revisiting if 1.94 ever becomes unremarkable *and* the per-crate split has
  cost more in confusion than it saved in reach — a judgement to make with real user reports, not now.
- **Hold sqlx at 0.8.** 0.8 does not support PostgreSQL 18's `uuidv7()` path the schema direction
  assumes (ADR 0015) and would trade a toolchain floor for a database-feature floor, which is worse.
- **Drop the `msrv` job and state the MSRV without enforcing it.** An unenforced MSRV is a claim, not
  a guarantee; it drifts within one release. The whole point of 0024's policy is that the number is
  checked.
- **A per-crate `msrv` job matrix**, pinning each crate's declared floor. Correct in principle and
  more CI than one provider crate justifies. Reconsider at the third distinct MSRV.
