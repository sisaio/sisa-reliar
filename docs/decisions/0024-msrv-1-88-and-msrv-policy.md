# ADR 0024 — Workspace MSRV rises to 1.88, and the policy that governs it

**Status:** Accepted — 2026-09-04
**SRS:** §7, §40, §41, §44
**Supersedes:** the `rust-version = "1.85"` clause of ADR 0022 (that ADR stands otherwise)
**Related:** RELIAR-11 (S0 platform); the provider-crate question §4 leaves open is now
answered by [ADR 0025](0025-provider-crate-msrv.md)

## Context

`cargo deny check` fails the workspace on **RUSTSEC-2026-0009** (published 2026-02-05,
CVE-2026-25727): `time` can be driven into stack exhaustion when parsing attacker-supplied input
with the RFC 2822 format. The advisory's `patched` range is `>= 0.3.47`.

Reliar cannot simply take the patch. Every `time` release from 0.3.47 onward declares
`rust-version = "1.88.0"`, as do the `time-core >= 0.1.8` and `time-macros >= 0.2.27` it pulls in.
The workspace declared `rust-version = "1.85"` (ADR 0022) and CI enforces it with a
`cargo +1.85 check --workspace --all-features` job. The two constraints are mutually exclusive:

| Option | Consequence |
|---|---|
| Hold MSRV at 1.85 | `time` stays on the vulnerable 0.3.45; `cargo deny check` and `cargo audit` stay red; the library ships a known DoS in its timestamp dependency |
| Pin an unaffected `time < 0.3.6` | Predates `UtcDateTime`, `PrimitiveDateTime` ergonomics and the sqlx 0.9 `time` feature; not viable |
| Drop `time` for `chrono` | Reverses the 2026-09-03 decision recorded in the conventions log, and buys nothing — the choice is orthogonal to the advisory |
| Raise the workspace MSRV to 1.88 | Costs compatibility with Rust 1.85–1.87 |

Reliar has **published nothing**. There is no downstream user whose build this breaks, and the
pinned developer toolchain is already 1.98. The cost of the bump is currently zero and rises with
every release after the first.

The exposure is real but not acute for Reliar's own code: Reliar parses no RFC 2822 input. It
matters because Reliar is a library — the vulnerable `time` lands in *its users'* dependency graphs,
where the RFC 2822 parser may well be reachable from untrusted input. A library does not get to
decide that its consumers are safe.

## Decision

**1. `[workspace.package] rust-version = "1.88"`.** `clippy.toml`'s `msrv` and the `msrv` job in
`ci.yaml` move with it, in lockstep — those three values are one value written three times, and a
change that does not touch all three is a bug.

**2. `time` is floored at `0.3.47`** in `[workspace.dependencies]`, not merely updated in
`Cargo.lock`. A floor is what protects a consumer who resolves the graph themselves; a lock file
only protects Reliar's own CI.

**3. MSRV policy, from here on.**

- The MSRV is a **published fact**, declared in `[workspace.package].rust-version` and enforced by a
  dedicated CI job that pins that exact toolchain. It is not "whatever compiles".
- Raising it is a **minor** version bump pre-1.0 and a **minor** bump post-1.0 (Reliar follows the
  common Rust-ecosystem practice that an MSRV increase is not major), and it is always recorded in
  `CHANGELOG.md`.
- The MSRV rises for exactly two reasons: **a security advisory whose only patch requires it**, or a
  dependency the project has decided it needs. It does not rise for convenience or for a language
  feature that has a stable-on-MSRV workaround.
- A **security advisory always wins over the MSRV floor.** When the two conflict, the floor moves.
  This is the rule that made the present decision, and it is the rule that will make the next one.

**4. Provider crates are explicitly out of scope here.** `sqlx` 0.9 declares `rust-version = 1.94`,
far above 1.88, so `reliar-store-postgres` will still need either its own `rust-version` or a
further workspace bump. That choice — one MSRV for the whole workspace versus a per-crate MSRV with
the abstraction crates held lower — was RELIAR-20 and is deliberately *not* settled here; this ADR
moves the floor the advisory forces and no further. It is settled by
[ADR 0025](0025-provider-crate-msrv.md), which chose the per-crate MSRV.

## Consequences

- Rust 1.85, 1.86 and 1.87 no longer build Reliar. Nothing is published, so nobody is broken.
- `cargo deny check` and `cargo audit` go green; the advisories gate becomes meaningful again rather
  than a permanently-failing check the team learns to ignore.
- The `msrv` CI job now pins `1.88`, and `clippy.toml` lints against 1.88 — which means clippy will
  begin *suggesting* 1.86–1.88 APIs it previously suppressed. That is intended.
- The gap between the MSRV (1.88) and the pinned toolchain (1.98) stays wide enough that the MSRV
  job earns its runtime by catching real regressions.
- RELIAR-20 inherited a simpler question — only sqlx/1.94, against a policy (§3 above) that already
  existed — and answered it in [ADR 0025](0025-provider-crate-msrv.md).

## Alternatives considered

- **Vendor or patch `time` 0.3.45.** Carrying a security patch for a dependency is a maintenance
  burden an early-stage library cannot service, and `[patch]` does not propagate to consumers.
- **`cargo deny` advisory ignore with an expiry.** Silences the signal without removing the
  vulnerability, and an ignore added in week one is an ignore that is still there in year two. The
  team gets one such escape hatch and this is not the occasion to spend it.
- **Per-crate MSRV now**, holding `reliar-core` at 1.85 and letting only the crates that need `time`
  rise. `time` is a `reliar-core` dependency (timestamps are in the envelope), so this would not
  even help — and it front-runs RELIAR-20's decision with no benefit.
