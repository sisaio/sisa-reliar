# Architecture Decision Records

Foundational decisions for Sisa Reliar. the SRS (`../sisa-reliar-backlog/docs/srs.md`) **v1.1** (approved 2026-09-04) is the
architecture baseline; these records carry the *reasoning* and the *rejected alternatives* behind
it, especially for the areas SRS §45 protects.

**Rules**

- Anything in **SRS §45's protected areas** changes only via a new ADR — canonical Envelope
  structure · message contract identity/versioning · metadata/header ownership · transport mapping ·
  outbox transaction boundaries · delivery guarantees · retry/dead-state semantics · database
  provider boundaries · static vs dynamic dispatch policy · migration behaviour.
- An ADR is **never edited to change its decision**. Write a new one and mark the old
  `Superseded by NNNN`.
- Format: `# ADR NNNN — Title` · **Status** (with date) · **Context** · **Decision** ·
  **Consequences** · **Alternatives considered**.
- The **frozen Phase-1 public contract** lives at `../architecture/phase1-contract.md`; the crate map
  at `../architecture/overview.md`.

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

Everything the review queued is recorded; nothing was dropped. Decisions the SRS resolved without
needing an ADR (promoting `tenant_id`/`expires_at`, constraint/index naming, the `deploy/` layout,
the clock split, facade timing) are specified in the SRS text and referenced from the ADRs above.

## Where a decision lives

| If you are changing… | Read first |
|---|---|
| a trait signature or bound | `../architecture/phase1-contract.md`, then 0001, 0008 |
| the envelope, metadata, or headers | 0003, 0004, 0010, 0011, 0012 |
| retry, dead state, leases, or timing | 0006, 0007, 0009, 0014 |
| the schema, indexes, or migrations | 0012, 0015, 0016, 0017, 0018, 0023 |
| settings, metrics, spans, or logs | 0019, 0020 |
| tests, CI, profiles, or the workspace | 0021, 0022, 0024, 0025 |
| the MSRV, or a security advisory that conflicts with it | 0024, 0025 |
