# ADR 0034 — Independent per-crate versions, bumped in the change that breaks them

**Status:** Accepted — 2026-09-05
**SRS:** §7 (public API and SemVer), §40 (release), §44 (versioning), §45 ("release and versioning
policy")
**Related:** ADR 0021/0022 (CI and action pinning), ADR 0024/0025 (MSRV), ADR 0032 (the relocation
that triggered this); `team/engineering-conventions.md` §11/§13, skill `ci-release`.
**Supersedes:** nothing. It fixes the release procedure the platform already used.

## Context

Phase 1 published `reliar-core`, `reliar-outbox` and `reliar-store-postgres` at **0.1.0**
(tags `reliar-<crate>-v0.1.0`, 2026-09-04). Phase 2 then merged to `main` with two properties that
cannot both hold:

- ADR 0032 **moved** `Publisher`, `Classify`, `FailureKind` and `SettingsError` into
  `reliar-core` — the published 0.1.0 tarball has none of them;
- every crate manifest still said `version = "0.1.0"`, and `[workspace.dependencies]` still pinned
  `reliar-core = { path = …, version = "0.1.0" }`.

`release-plz release` therefore skipped the three crates whose manifest version was already on the
registry and went straight to the one new crate, `reliar-transport-nats`. `cargo publish` strips
`path` from a published manifest and resolves the sibling **from crates.io**, so the verification
build compiled the Phase-2 transport against `reliar-core` **0.1.0** and failed:

```
error[E0432]: unresolved imports `reliar_core::Classify`, `reliar_core::FailureKind`
error[E0432]: unresolved import `reliar_core::Publisher`
error[E0432]: unresolved import `reliar_core::SettingsError`
error: failed to verify package tarball
```

The bug is not the pin. The pin (`0.1.0`) matched the local manifest (`0.1.0`) exactly — a
consistency check between the two would have passed. The bug is that **a published crate's public
surface changed while its version number did not**, so `0.1.0` came to mean two different APIs: the
one on crates.io and the one in the working tree. Every internal `path` dependency hides that
divergence at workspace build time and exposes it only at `cargo publish`.

`cargo package --workspace` does not expose it either. Since Cargo 1.83 the workspace form packages
members *together* and resolves siblings to the freshly packaged local copies; on cargo 1.98 this
repository's `cargo package --workspace` passes on the exact tree that failed to publish. Only
per-crate `cargo package -p <crate>` reaches the registry — and that form cannot run as a
pull-request gate, because during any release the dependency's new version is by definition not yet
published (`failed to select a version for the requirement reliar-core = "^0.2.0"`).

Two things had to be decided: how versions are numbered across the crates, and **who** assigns
them — the humans in the change, or `release-plz` in a follow-up release PR.

## Decision

### 1. Versions are independent per crate, not lockstep

Each published crate carries its own SemVer line. A new crate starts at `0.1.0` whatever the others
are at (`reliar-transport-nats` is `0.1.0` alongside `reliar-core` `0.2.0`). Lockstep was rejected:
it forces a publish of untouched crates, it fights `release-plz`'s per-package model, and it lies to
users about what changed.

### 2. In the 0.x line, any public-surface change bumps the **minor**

Patch is reserved for changes with no effect on the public API — a bug fix in a body, docs, an
internal dependency's patch bump. Anything a user can observe in the API — added item, removed item,
changed signature, changed item identity (a type that becomes a re-export of another crate's type) —
bumps the minor. Cargo treats `0.x.y` as breaking at the minor, so this is the only bump that gives
users a truthful compatibility signal before 1.0, and it removes the "is this additive enough for a
patch?" argument from every release.

### 3. A dependent is released when the new dependency version falls outside its requirement

`reliar-store-postgres` 0.1.0 requires `reliar-outbox ^0.1.0`, which does not admit `0.2.0`. A user
who wants `reliar-outbox` 0.2.0 therefore cannot have *any* published `reliar-store-postgres` unless
a new one ships. So: when a crate's version changes, every workspace crate **whose requirement on it
no longer admits the new version** is released in the same release, at least a minor bump under §2.

The narrowing matters, because §2 makes the common bump a minor one and Cargo reads `0.x` minors as
breaking, so most bumps do leave their dependents' ranges. An **admitted** bump does not: a
dependency going `0.2.0 → 0.2.1` under a dependent's `^0.2.0` obliges no dependent release. Cargo
resolves the already-published dependent onto the new patch on its own, so republishing it would
change nothing a user can observe, and the pin in `[workspace.dependencies]` does not move either
(`version = "0.2.0"` still matches). A dependent is still released when its *own* surface or
behaviour changes (§2), or when anything else it inherits from the root manifest changes (§6).

### 4. The version bump lands in the change that requires it — not in a release PR

**A version that is on crates.io is frozen.** The pull request that changes a crate whose current
version is already published also changes:

- that crate's `[package] version`,
- the `[workspace.dependencies]` `version` pin for it, whenever the new version leaves the pinned
  requirement (Cargo enforces that half — a `path` dependency whose `version` requirement no longer
  matches the local manifest is a hard resolution error, so a forgotten pin cannot compile; a patch
  bump inside `^0.2.0` leaves the pin alone),
- the versions of the dependents whose requirement the new version leaves (§3),
- `CHANGELOG.md`.

`main` is therefore always publishable, and CI checks the bump on the PR (§6) instead of the release
job discovering it on `main`.

### 5. `release-plz` runs `release` only

`.github/workflows/release.yaml` keeps `release-plz`'s `release` command — tag, GitHub release,
`cargo publish` in dependency order, waiting for the index between crates — and **drops the
`release-pr` command**, because §4 gives version numbers a single owner. Two owners is how a bump
gets forgotten. `release-plz.toml` records this (`changelog_update = false`: the root
`CHANGELOG.md` is hand-written Keep a Changelog, and per-crate changelog files are not generated;
`release_always = true`; `semver_check = true`; `dependencies_update = false`).

The ordering bug in the old workflow is fixed by the same removal: `release` and `release-pr` were
two steps of one job with `release` first, so the failed publish also cancelled the release PR that
would have proposed the missing bump.

### 6. CI proves it on every pull request — freeze first, `cargo semver-checks` second

`ci.yaml`'s `versioning` job runs two checks, in this order.

**The freeze check** asks crates.io for each publishable member's published versions. If the
manifest version is among them, **what that crate would publish** must be identical to the tag that
published it — and that is two comparisons, because the crate directory is only half of the tarball.

- *The directory*: `git diff --quiet <name>-v<version> HEAD -- crates/<name>`.
- *The fields inherited from the root manifest*: `[workspace.package]`, `[workspace.dependencies]`
  and `[workspace.lints]` are resolved into the generated `Cargo.toml` at publish time, so a root
  edit changes a published crate's manifest — a dependency requirement, `rust-version`, `edition`, a
  lint level — while its directory stays byte-identical to the tag. The job therefore adds the tag
  as a second `git worktree`, runs `cargo metadata --no-deps --format-version 1` at both refs,
  reduces each to that one package's record (version, edition, `rust_version`, license, features,
  and every dependency's `req`/`kind`/`optional`/`features`/`uses_default_features`/`target`, plus
  `publish`, `links` and the crates.io metadata fields) with absolute paths, ids and `manifest_path`
  normalised away, and diffs the two. `[workspace.lints]` is the one part invisible to
  `cargo metadata`, so it is compared textually — and only for a crate that writes
  `[lints] workspace = true`.

Diffing the root manifest wholesale was rejected as the fix: it would force a bump of every
published crate on any root edit, including one touching an examples-only dependency or a build
profile. The effective-inputs comparison fires only for the crates that actually inherit what
changed — moving `[workspace.package] rust-version` fails `reliar-core`, which inherits it, and
leaves `reliar-store-postgres` alone, because that crate declares its own (ADR 0025).

`cargo metadata --no-deps` is the only form of "resolved manifest" that can be a pull-request gate:
it reads manifests, runs no resolver and contacts no registry, so it evaluates a tag whose sibling
crates' *current* versions are unpublished. Reading the generated `Cargo.toml` out of a
`cargo package -p <crate>` tarball exposes the same resolved fields, but it resolves siblings from
crates.io and therefore fails during exactly the releases this gate exists to survive — the same
reason `cargo package` is not a gate at all (below).

A crate that is not on crates.io yet, and a version that is not published yet, are both skipped — so
a release candidate is never blocked, and a crate enrols itself at its first publish with no
workflow edit. Verified both ways on this repository: the check exits 1 on `2265e6e` (the tree that
failed to publish), naming all three crates, and exits 0 once they are bumped. The inherited half
was verified on a scratch clone checked out at the Phase-1 release tags, where all three crate
directories are clean and the check passes: changing `[workspace.dependencies] sqlx` from `0.9` to
`0.9.1` then fails `reliar-store-postgres` alone (`req` `^0.9` → `^0.9.1`); adding a lint to
`[workspace.lints.clippy]` fails all three; and changing `[workspace.dependencies] axum`, which only
`examples/` uses, still passes.

**`cargo semver-checks check-release -p <crate> --baseline-version <highest published>`** then runs
for every published crate. It is the second half, not the first: semver-checks finds *breaking*
differences, and `reliar-core`'s change was purely additive — on the broken tree it reported "no
semver update required" for `reliar-core` while `reliar-outbox` correctly failed with
`semver requires new major version` (four items removed from its root). An additive-only change with
a forgotten bump would sail past semver-checks and still break the next dependent's publish. The
freeze check is what catches that class; semver-checks is what sizes the bump and catches the
breaking half.

`cargo package`, `cargo publish --dry-run` and `release-plz release --dry-run` are **not** gates,
for the reason in the Context: the workspace form does not reach the registry, and the per-crate
form cannot succeed while a dependency's new version is unpublished — `cargo package -p
reliar-transport-nats` on the fixed tree fails with `failed to select a version for the requirement
reliar-core = "^0.2.0"`, and will keep failing until `reliar-core` 0.2.0 is on crates.io.
Publication stays verified where it is verifiable: serially, at publish time, by `release-plz`, each
crate's real tarball built against the sibling it has just published.

## Consequences

- The version number is part of a change's diff and part of review. "Bump forgotten" becomes a red
  PR check instead of a red release job.
- The first pull request to touch a crate after that crate's release must bump it — including for a
  test-only or docs-only edit, since the published tarball carries those files too, and including a
  root-manifest edit that reaches it through `[workspace.package]`, `[workspace.dependencies]` or
  `[workspace.lints]`. That bump is a patch under §2. Every later pull request in the same cycle
  sees an unpublished version and is not asked again.
- A root-manifest edit can therefore make several published crates need a patch bump at once. That
  is the honest cost of workspace inheritance: those crates really would publish a different
  manifest. The blast radius stays proportional — the check compares each crate's own resolved
  record, so an edit no published crate inherits costs nothing.
- `main` is publishable at every commit; the release job is a publish, never a decision.
- Users of `reliar-outbox` 0.1.0 who upgrade to 0.2.0 must also move to `reliar-core` 0.2.0 and
  `reliar-store-postgres` 0.2.0. That is stated in `CHANGELOG.md` and is what the 0.x minor is for.
- A crate published today can never be republished at the same number with a different API, which is
  what `--baseline-version <highest published>` asserts.
- `cargo-semver-checks` builds rustdoc JSON for each published crate on every PR. It is a slower job
  than `check`; it runs in parallel with the others and only touches published crates.
- Pre-1.0 breaking changes stay allowed (`team/engineering-conventions.md` §11); they are now
  *numbered* honestly rather than absorbed.

## Alternatives considered

**Lockstep versions across all Reliar crates.** One number for the whole toolkit, easy to document,
and it makes the mismatch structurally impossible. Rejected: it republishes untouched crates on
every release, `release-plz` has no fixed-version mode so it would have to be fought with config,
and a user reading `reliar-transport-nats 0.4.0` learns nothing about that crate.

**Let `release-plz release-pr` own the bumps (its intended flow).** It computes bumps from
conventional commits and `cargo-semver-checks`, and it would have proposed the right numbers here.
Rejected for this team: it adds a second merge to every release, it wants to generate per-crate
changelog files that duplicate the hand-written root one, and — decisively — it leaves a window in
which `main` is *known unpublishable*, which is exactly the state that produced this ADR. The
release PR is a good fit for a repository where humans do not touch versions at all; this one edits
manifests in every phase branch anyway.

**Pin internal dependencies to the exact local version and check pin == version in CI.** Cheap, but
it would not have caught this failure: pin and version already agreed. The divergence was between
the local `0.1.0` and the registry's `0.1.0`, and only a comparison against what was published sees
that.

**`cargo semver-checks` alone, without the freeze check.** The obvious single gate, and it does
catch the `reliar-outbox` half of this incident. Rejected as sufficient: it reports breaking
changes, and `reliar-core`'s — the one that actually broke the publish — was additive, which it
passes. The two checks answer different questions ("did this version change at all?" and "is the
bump big enough?") and the repository needs both.

**`cargo package -p <crate>` for every crate on each PR.** The check that actually reproduces the
failure — proven locally on this tree. Rejected as a gate: it is red for the whole of any release
that bumps a dependency, since the dependency's new version is not on crates.io until it publishes.
It stays a useful *local* diagnostic and is documented as such in `CONTRIBUTING.md`.
