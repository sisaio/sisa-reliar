# Architecture Decision Records

Foundational decisions for Sisa Reliar. the SRS (`../sisa-reliar-backlog/docs/srs.md`) **v1.1** (approved 2026-09-04) is the
architecture baseline; these records carry the *reasoning* and the *rejected alternatives* behind
it, especially for the areas SRS §45 protects.

**Rules**

- Anything in **SRS §45's protected areas** changes only via a new ADR — canonical Envelope
  structure · message contract identity/versioning · metadata/header ownership · transport mapping ·
  outbox transaction boundaries · delivery guarantees · retry/dead-state semantics · database
  provider boundaries · static vs dynamic dispatch policy · migration behaviour.
- An ADR is **never rewritten to change its decision in place.** There are two mechanisms, and
  which one applies depends on whether the decision has **shipped** — it ships with the first
  crates.io release of the crate that carries it (for a platform decision, the first `main` commit
  that runs it in CI):
  - **Before it ships**, the decision is still being built and may be **amended**: append a dated
    `## Amendment X — YYYY-MM-DD — <what changed>` section at the end of the record. The original
    text is never edited away — every section the amendment overrides keeps its words and gains a
    banner pointing at the amendment — and the index marks the record **Amended** with the dates
    and a one-line summary of each amendment. An amendment is part of the same unreleased decision,
    so no user of a released crate ever finds a rule that changed under them.
  - **After it ships**, a reversal or any material change is a **new ADR**. The old record's status
    becomes `Superseded by NNNN` (or `Superseded in part by NNNN`, naming the sections), the new
    record states what it replaces and why, and the index is updated on both rows.
  - A **clarification** that changes no behaviour — a wrong dependency listing, a rationale that was
    never true, a stale cross-reference — may still be appended after shipping, dated and labelled a
    correction, precisely because there is nothing a reader could have relied on.
- Format: `# ADR NNNN — Title` · **Status** (with date) · **Context** · **Decision** ·
  **Consequences** · **Alternatives considered**.
- The **frozen public contracts** live at `../architecture/phase1-contract.md` (core, outbox,
  Postgres) and `../architecture/phase2-contract.md` (the NATS transport); the crate map at
  `../architecture/overview.md`.

## Index

| # | Title | Status | SRS sections |
|---|---|---|---|
| [0001](0001-static-dispatch.md) | Static dispatch by default | Accepted 2026-09-04 | §3, §19, §30, §32, §34 |
| [0002](0002-postgres-first.md) | PostgreSQL is the first and only v0.1 provider | Accepted 2026-09-04 | §2, §4, §5, §24, §34, §42 |
| [0003](0003-canonical-envelope.md) | One canonical `Envelope<T>` for typed and serialized messages | Accepted 2026-09-04 | §9, §9.1, §11, §12, §17 |
| [0004](0004-no-metadata-header-duplication.md) | Framework metadata has exactly one source of truth | Accepted 2026-09-04 | §13, §13.1, §14, §15, §16 |
| [0005](0005-envelope-vs-outbox-record.md) | `Envelope`, `OutboxRecord` and `InboxRecord` are distinct types | Accepted 2026-09-04 | §17, §17.1, §24, §24.2 |
| [0006](0006-short-claim-transactions.md) | The claim is one statement; publication happens outside any transaction | Accepted 2026-09-04 | §21, §21.1, §24.1, §25, §26 |
| [0007](0007-at-least-once-publication.md) | At-least-once publication, with both duplicate windows documented | Accepted 2026-09-04 | §22, §22.1, §23.2, §26.1 |
| [0008](0008-outbox-store-contract.md) | The `OutboxStore` operation set, worker guard, and poison-row policy | Accepted 2026-09-04 | §19, §19.1–§19.6, §21.1, §26.1 |
| [0009](0009-retry-and-dead-state.md) | Retry policy is pure and lives in `reliar-outbox`; attempts count outcomes | Accepted 2026-09-04 | §12.2, §23, §23.1, §23.2, §25.1, §31 |
| [0010](0010-serializer-and-message-identity.md) | The `Serializer` contract and stable `MessageType` identity | Accepted 2026-09-04 | §9, §10, §10.1, §12.1, §24.2, §31 |
| [0011](0011-headers-and-envelope-construction.md) | `Headers` is a validating newtype; `Envelope` is built through a builder | Accepted 2026-09-04 | §9.1, §11, §13, §13.1, §14, §17.1 |
| [0012](0012-metadata-persistence-contract.md) | Promoted columns and the `MetadataRest` JSONB contract | Accepted 2026-09-04 | §12, §12.2, §24, §24.1, §24.2 |
| [0013](0013-ordering-strategy.md) | Ordering is a configured strategy; `Unordered` is the default and the only v0.1 mode | Accepted 2026-09-04 | §7.2, §22.2, §24.1 |
| [0014](0014-shutdown-drain-and-run-error-policy.md) | Graceful drain, lease release, and `run()`'s error policy | Accepted 2026-09-04 | §5, §21.1, §22.1, §26, §26.1 |
| [0015](0015-minimum-postgres-and-id-generation.md) | PostgreSQL 18 floor; UUIDv7 ids generated client-side | Accepted 2026-09-04 | §11, §17.1, §24.1 |
| [0016](0016-outbox-partitioning.md) | Partitioning designed in v0.1, shipped as an opt-in variant in 0.2 | Accepted 2026-09-04 | §19.3, §23.2, §24.3, §35.1 |
| [0017](0017-schema-resolution-via-search-path.md) | Fixed table names in a configurable schema, resolved via `search_path` | Accepted 2026-09-04 | §7.2, §24, §35.1 |
| [0018](0018-migrations-embedded-published-and-isolated.md) | Migrations embedded in the crate, published as an artifact, with isolated bookkeeping | Accepted 2026-09-04 | §24, §35, §35.1, §40 |
| [0019](0019-settings-pattern-and-opt-in-from-env.md) | One `*Settings` struct per feature, with an opt-in `from_env` | Accepted 2026-09-04 | §7.1, §7.2, §21.1, §22.2, §23.1 |
| [0020](0020-observability-hook-mechanism.md) | Metrics are a static-dispatch hook trait; Reliar ships no exporter | Accepted 2026-09-04 | §5, §19.3, §26, §33, §33.1 |
| [0021](0021-testcontainers-and-pooler-test-substrate.md) | testcontainers is the only integration substrate, and pooler scenarios are part of it | Accepted 2026-09-04 | §8, §8.1, §8.2, §25.1, §41 |
| [0022](0022-workspace-ci-and-yaml-policy.md) | Workspace layout, build profiles, semver policy, and the CI/YAML rules | Accepted 2026-09-04 | §6, §7, §7.1, §32, §38–§41, §44 |
| [0023](0023-persisted-dead-reason.md) | `dead_reason` is a persisted `text` column with a stable codec | Accepted 2026-09-04 | §19.1, §23, §24.1, §43.A.20 |
| [0024](0024-msrv-1-88-and-msrv-policy.md) | Workspace MSRV rises to 1.88, and the policy that governs it | Accepted 2026-09-04 | §7, §40, §41, §44 |
| [0025](0025-provider-crate-msrv.md) | Provider crates may carry their driver's MSRV; pure crates may not | Accepted 2026-09-04 | §7.1, §40, §44 |
| [0026](0026-nats-header-projection.md) | The NATS header projection and the decode policy | **Amended** — accepted 2026-09-04, amended 2026-09-04 (**A** case-insensitive framework/custom collision · **B** `InvalidHeaderValue` names the header · **C** an empty required header value is `MalformedHeader`) | §12, §12.3, §14–§16, §17.1 |
| [0027](0027-subject-resolution-is-a-transport-strategy.md) | Subject resolution is a transport-side strategy, not envelope metadata | Accepted 2026-09-04 | §12, §16, §18 |
| [0028](0028-jetstream-ack-is-the-only-publish-path.md) | JetStream with an awaited ack is the only publish path | **Amended** — accepted 2026-09-04, amended 2026-09-04 (**A** the ack deadline is `min(publish_timeout, Context::timeout)`; the pipeline depth stays ≤ the host's `max_ack_inflight`) and 2026-09-05 (**A corrected** `backpressure_on_inflight` defaults to `true` · **B** `max_in_flight` → `batch_pipeline_depth`) | §19.4, §21, §22, §22.1, §32 |
| [0029](0029-stream-and-connection-ownership.md) | The application owns the NATS connection and the stream | Accepted 2026-09-04 | §3.12, §7.2, §30, §31, §35 |
| [0030](0030-nats-publish-error-classification.md) | `NatsPublishError` classifies per variant and leaks nothing | **Amended** — accepted 2026-09-04, amended 2026-09-04 (**A** `max_payload = Some(0)` is a construction error · **B** the `Broker` `warn` logs the kind name, never the `async-nats` `Display`), corrected 2026-09-05 (the size guard saves no round-trip) | §17.1, §19.4, §23, §33 |
| [0031](0031-nats-dependency-and-test-substrate.md) | The `async-nats` pin, its minimal features, and the NATS test substrate | **Amended** — accepted 2026-09-04, amended 2026-09-04 (**A** `tokio` is a runtime dependency; readiness is the JetStream retry loop) and 2026-09-05 (**B** the pin gate covers `tests/system`; §6 becomes a CI gate) | §7.1, §8.2, §40–§43 |
| [0032](0032-publisher-and-shared-primitives-in-core.md) | `Publisher`, `Classify`, `FailureKind` and `SettingsError` belong to `reliar-core` | **Amended** — accepted 2026-09-05, amended 2026-09-05 (**A** a `tokio` dev-dependency in `reliar-core` is inside this ADR) | §18, §19.4, §23, §36, §43.B |
| [0033](0033-outbox-routing-publisher.md) | The outbox routing publisher: `OutboxPolicy` (the rule) + `OutboxPublisher`/`ScopedOutboxPublisher` (composition, and a `reliar_core::Publisher`) + the `OutboxStaging<Tx>` capability | **Amended** — accepted 2026-09-05, amended 2026-09-05 (**A** settings are top-level `OutboxSettings` fields, no `RoutingSettings` · **B** `allowed_types` + `disallowed_types`, disallow wins, an overlap is a `SettingsError` at construction · **C** the rule is its own type `OutboxPolicy` in module `policy`; the composition owns one and delegates · **D** the routing publisher **is** a `Publisher`: `OutboxRouter` → `OutboxPublisher` handing out a transaction-scoped `ScopedOutboxPublisher` that implements `reliar_core::Publisher`; the `OutboxEnqueue`/`OutboxEnqueueIn<Cx>` pair collapses into one `OutboxStaging<Tx>`; the caller serializes, so no `Serializer` here) | §7, §12, §18, §19.6, §20, §20.2, §22, §23, §31, §33, §36 |
| [0034](0034-versioning-and-release-flow.md) | Independent per-crate versions, bumped in the change that breaks them; `release-plz` publishes rather than decides | Accepted 2026-09-05 | §7, §40, §44, §45 |
| [0035](0035-coderabbit-is-advisory-pr-review.md) | CodeRabbit reviews every ready pull request into `main` against a committed `.coderabbit.yaml`, as advisory input only | Accepted 2026-09-05 | §38, §41 |

## Numbering note

0001–0007 are the records SRS §38 names. 0008–0022 continue from the candidate list in
`../analysis/architect-review.md` §5–§7, with two deviations from that file's predicted numbering:

- §7 predicted `0008 migration-bookkeeping-isolation` as its own record. It is **merged into 0018**
  with the embed-and-publish decision — they are one migration-packaging story and were separating
  the same file set.
- §7 predicted a single `0011 envelope-serialization-and-headers`. It is **split into 0010**
  (serializer + `MessageType`/`Message` bounds) and **0011** (`Headers` newtype + `Envelope`
  builder + identity types), because they are independently revisitable.

**0023** was added after review 1 of the Phase-1 contract (2026-09-04): the `dead_reason` column had
been put into the schema direction with no record behind it.

**0024** was added after review 2 of RELIAR-11 (2026-09-04): RUSTSEC-2026-0009 has no patch that
builds on Rust 1.85, so the workspace MSRV moved to 1.88 and the policy for future conflicts between
an advisory and the MSRV floor was written down. It supersedes ADR 0022's `rust-version` clause only.

**0025** answers the question 0024 left open (2026-09-04): sqlx 0.9 needs Rust 1.94, so
`reliar-store-postgres` declares its own `rust-version` and the `msrv` job excludes it — pure crates
keep the workspace floor so that using `reliar-core`/`reliar-outbox` with your own store does not
cost you sqlx's toolchain.

**0026–0031** open **Phase 2** (2026-09-04, story RELIAR-2): the five decisions SRS §45 requires
before a transport ships — header projection, subject-strategy placement, the JetStream-ack
requirement, per-variant error classification, stream/connection ownership — plus 0031 for the
platform side (the `async-nats` pin and feature set, the JetStream test substrate, and where the
end-to-end test package lives). The frozen surface they define is
`../architecture/phase2-contract.md`; none of them changes `reliar-core` or `reliar-outbox`.

**0032** (2026-09-05, story RELIAR-2) relocates `Publisher`, `Classify`, `FailureKind` and
`SettingsError` from `reliar-outbox` to `reliar-core` and states the kind test that keeps core from
becoming the catch-all SRS §18 forbids. It changes no signature and adds nothing to core's
dependency graph; it lets `reliar-transport-nats` drop `reliar-outbox` entirely, and it amends both
`../architecture/phase1-contract.md` (§3.5, §3.7) and the frozen
`../architecture/phase2-contract.md` (§1, §4).

**0033** (2026-09-05, story RELIAR-43) adds the routing publisher: one call that either stages a
message in the outbox — inside the caller's transaction — or publishes it straight to the transport,
decided by three top-level `OutboxSettings` fields — `enabled`, `allowed_types` (empty = all) and
`disallowed_types` (**Amendment A**, 2026-09-05: the human chose top-level fields and
`RELIAR_OUTBOX_ENABLED` over a `RoutingSettings` sub-section; `enabled = false` stops *entry*, never
draining. **Amendment B**, same day: the routed list is `allowed_types`, a `disallowed_types` list
sits beside it, **disallow wins** — so "everything except `c`" is an empty allow list plus one name
— and a type in both lists is a `SettingsError` on every construction path, never a silent
tie-break. **Amendment C**, same day: that whole rule is its own type, `OutboxPolicy` in
`reliar-outbox/src/policy.rs` — built and validated once by `OutboxPolicy::from_settings`, answering
`decide(&MessageType) -> RouteKind`, testable and previewable without a store or a transport, while
the composition merely owns one and delegates; `OutboxSettings::route_for`/`validate_routing` are
removed and the constructor is infallible again. **Amendment D**, same day: the human's "the routing
publisher *is* a `Publisher`" — `OutboxRouter` becomes **`OutboxPublisher`**, and because
`Publisher::publish` carries no transaction it hands out a transaction-scoped
**`ScopedOutboxPublisher`** that implements `reliar_core::Publisher` for the life of the borrow;
that scoped type borrows a transaction, so it is neither `'static` nor `Clone` and
`OutboxDispatcher`'s bound rejects it — the feedback cycle 0033 §4 feared is now a **compile error**
rather than a rustdoc warning, and the un-scoped type still implements no `Publisher`. D also
collapses the capability pair into one `OutboxStaging<Tx>` taking `&mut Tx` — which deletes the
`Transaction<'c, _>` invariance trap — and moves serialization to the caller, so `reliar-outbox`
holds no `Serializer` and both routes carry the same buffer). It lives in
`reliar-outbox` because it is outbox mechanics under 0032's kind test; the caller's transaction
reaches it as an opaque type parameter (`OutboxStaging<Tx>`), so no SQLx type enters the crate. Its
frozen surface is `../architecture/routing-publisher-contract.md`.

**0034** (2026-09-05) settles versioning and the release flow after the first Phase-2 release
attempt failed: `reliar-core`'s public surface had changed (0032) while its version stayed at the
0.1.0 that was already on crates.io, so `cargo publish` built the new `reliar-transport-nats`
against the registry's `reliar-core` 0.1.0 and could not resolve `Publisher`, `Classify`,
`FailureKind` or `SettingsError`. Crates version independently; in the 0.x line a public-surface
change is a minor bump and everything else a patch; a dependent is released when the new version of
a crate it depends on falls outside its declared requirement (an admitted patch bump obliges
nothing, and moves no `[workspace.dependencies]` pin); and the bump lands in the pull request that requires it rather than in a `release-plz`
release PR — so `main` is publishable at every commit and `release.yaml` runs `release-plz`'s
`release` command only (`release-plz.toml`). `ci.yaml`'s `versioning` job enforces it: a version that
is on crates.io is frozen against its release tag — both the crate directory and the fields it
inherits from the root manifest, compared as `cargo metadata`'s resolved record for that one package
so a root edit fails only the crates that actually inherit it — and `cargo semver-checks` then runs
against the published baseline (RELIAR-22).

**0035** (2026-09-05) puts an automated reviewer on every pull request that is ready for review and
targets `main` — a draft is reviewed once it is marked ready, and `main` is the only base branch a
change merges into here. `.coderabbit.yaml` at the
repo root is the whole configuration — nothing lives in the vendor's UI — and its `path_instructions`
restate the house rules per area (`reliar-core` purity, the crate-wide Rust rules, the Postgres
provider's sqlx/lease/migration rules, the NATS mapping, the test rules, `.github/`, `deploy/`), as a
projection of `team/engineering-conventions.md` and `team/definition-of-done.md`, which remain the
source of truth. The findings are advisory: `request_changes_workflow` is off, no branch protection
depends on the bot, and the merge gate is still CI plus the Definition of Done plus the independent
`reviewer` verdict. `clippy` is disabled inside CodeRabbit because CI already runs it pinned across
the feature powerset — a rule change in `team/` or an ADR updates `.coderabbit.yaml` in the same
pull request.

**Amended records, and the ship test applied to them.** Six records carry amendments: **0026**
(A–C), **0028** (A, A corrected, B), **0030** (A, B, plus a correction), **0031** (A, B), **0032**
(A) and **0033** (A–D). Every one of those amendments predates the first release of the crate whose
behaviour it changes, so all of them are legal under the rule above and none needs reclassifying as
a supersession. 0026 and 0028–0032 were written — decision *and* amendments — in commit `f7b029b`
(2026-09-05 10:06), while `reliar-transport-nats` 0.1.0 and `reliar-core` / `reliar-outbox` /
`reliar-store-postgres` 0.2.0 were tagged at `f08f7cc` two hours later; the amendments therefore
describe the code as first published, not a change to it. 0032's Amendment A concerns a
`[dev-dependencies]` entry, which is not in the published graph at all. 0033's A–D describe
`reliar-outbox` 0.3.0 and `reliar-store-postgres` 0.3.0, neither of which is released. From the next
release onwards the freeze in ADR 0034 and the rule above line up: a version that is frozen on
crates.io has a frozen ADR behind it.

Everything the review queued is recorded; nothing was dropped. Decisions the SRS resolved without
needing an ADR (promoting `tenant_id`/`expires_at`, constraint/index naming, the `deploy/` layout,
the clock split, facade timing) are specified in the SRS text and referenced from the ADRs above.

## Where a decision lives

| If you are changing… | Read first |
|---|---|
| a trait signature or bound | `../architecture/phase1-contract.md`, then 0001, 0008 |
| whether an item belongs in `reliar-core` or a feature crate | 0032, then 0027 |
| whether a message goes through the outbox or straight to the transport | 0033 (the rule is `OutboxPolicy` — Amendment C), then `../architecture/routing-publisher-contract.md` §2.5 |
| which type implements `Publisher`, and why the un-scoped one must not | 0033 Amendment D §4, then `../architecture/routing-publisher-contract.md` §4, §4.1 |
| the NATS mapping, subjects, or the publisher | `../architecture/phase2-contract.md`, then 0026–0030 |
| the `async-nats` pin or the NATS test substrate | 0031, then 0021, 0022 |
| the envelope, metadata, or headers | 0003, 0004, 0010, 0011, 0012 |
| retry, dead state, leases, or timing | 0006, 0007, 0009, 0014 |
| the schema, indexes, or migrations | 0012, 0015, 0016, 0017, 0018, 0023 |
| settings, metrics, spans, or logs | 0019, 0020 |
| tests, CI, profiles, or the workspace | 0021, 0022, 0024, 0025 |
| the MSRV, or a security advisory that conflicts with it | 0024, 0025 |
| a version number, a crate's release, or `release-plz` | 0034, then 0021, 0022 |
| the automated PR review, or `.coderabbit.yaml` | 0035, then `../../team/engineering-conventions.md` |
