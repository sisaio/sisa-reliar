# ADR 0032 — `Publisher`, `Classify`, `FailureKind` and `SettingsError` belong to `reliar-core`

**Status:** Accepted — 2026-09-05
**SRS:** §18 (crate ownership — `reliar-core` owns "shared primitives"), §19.4 (`Publisher`,
`Classify`, `FailureKind`), §23, §36, §43.B, §45 ("database provider boundaries", "static vs
dynamic dispatch policy")
**Related:** ADR 0008 (classification lives with the error type), ADR 0019 (settings + `from_env`),
ADR 0027 (routing stays in the transport), ADR 0031 §6 (providers never depend on each other);
contracts `../architecture/phase1-contract.md` §3.5/§3.7, `../architecture/phase2-contract.md` §1/§4;
story RELIAR-2.
**Supersedes:** nothing. It relocates surface frozen by the Phase-1 contract; the contract files are
updated in the same change.

## Context

`Publisher`, `Classify` and `FailureKind` were written into `reliar-outbox`
(`crates/reliar-outbox/src/publisher.rs`) because the outbox dispatcher was their first — and, in
Phase 1, only — consumer. `SettingsError` was written into `crates/reliar-outbox/src/settings.rs`
for the same reason: `OutboxSettings::from_env` was the first `from_env`.

Phase 2 showed the cost. `reliar-transport-nats` is a **transport**: it implements
`reliar_core::EnvelopeMapper` and `Publisher`, resolves subjects, and talks to JetStream. It has no
outbox concept anywhere in it — no store, no record, no lease, no retry policy, no dispatcher. Yet
its manifest carries `reliar-outbox = { workspace = true }`, and a grep of the crate shows that
dependency exists for exactly four items:

| Item | Used at |
|---|---|
| `Publisher` | `src/publisher.rs:10`, plus 8 test files and 1 bench |
| `Classify`, `FailureKind` | `src/publisher.rs:10`, plus 4 test files |
| `SettingsError` | `src/settings.rs:10`, re-exported at `src/lib.rs:22` |

Nothing else. A transport depends on the transactional-outbox crate to learn what a publish failure
is and what a bad environment variable is. That is an accident of authoring order, and it points the
wrong way: it says publication is an outbox concept, when the outbox is one of several things that
publish.

Three further facts make this more than an aesthetic complaint.

1. **`Classify` is already not publisher-only.** SRS §19 puts `+ Classify` on `OutboxStore::Error`
   too, so a *store* author must implement it — `crates/reliar-store-postgres/src/error.rs:12`
   imports `Classify`/`FailureKind` and never mentions `Publisher`. The trait is a shared
   error-classification vocabulary that two unrelated capability traits happen to bound.
2. **`SettingsError` is already contractually shared.** Phase-1 contract §7 I3 records the ruling
   that it is *the* error every provider's `from_env` returns, and public constructors exist on it
   solely so crates other than `reliar-outbox` can build a variant through the `#[non_exhaustive]`
   wall. It is a shared primitive whose definition sits in a crate that has no claim to it.
3. **Phase 3 makes it structural, not cosmetic.** SRS §36 lists `reliar-messaging` publishing
   events, sending commands, and doing request/reply. Some of those go through the outbox; some
   are direct sends that never touch a store. `reliar-messaging → reliar-outbox` only to name
   `Publisher` would drag the entire outbox — records, leases, retry, the dispatcher — into the
   dependency graph of a bus that may not use it. The same is true of an inbox-driven republish and
   of `reliar-scheduler`.

The obvious objection is SRS §18's closing sentence: *"`reliar-core` SHALL NOT gradually become a
catch-all crate."* That rule is real and it is the reason ADR 0027 kept `SubjectResolver` out of
core. But "catch-all" is not a headcount — it is a **kind** test.

## Decision

**`Publisher`, `Classify`, `FailureKind` and `SettingsError` move to `reliar-core`.**

### 1. The line that keeps core from becoming a catch-all

An item belongs in `reliar-core` when **both** hold:

- it names no storage engine, no broker, and no transport routing concept (no subject, exchange,
  topic, partition key, connection, pool, or SQL notion); **and**
- it is a vocabulary more than one Reliar capability needs in order to talk to another.

An item stays out of core when it encodes *how* a capability works: `OutboxStore`, `OutboxRecord`,
`RetryPolicy`, `AcquireRequest`, `Ordering`, the dispatcher and its settings are outbox mechanics
and remain in `reliar-outbox`. `SubjectResolver`, `NatsEnvelopeMapper` and every wire detail remain
in the transport (ADR 0027 is unchanged and remains the worked example of the second half of this
rule).

Applying the test:

- `Publisher` names one type, `reliar_core::SerializedEnvelope`, plus `std::error::Error`. It says
  "given a serialized envelope, put it on the wire, and tell me positionally what happened." That is
  the same *kind* of statement as `Serializer` and `EnvelopeMapper`, which already live in core.
- `Classify`/`FailureKind` are a two-variant verdict on an error. They are bound by an outbox trait
  and by a publisher trait today, and by whatever Phase 3 adds tomorrow.
- `SettingsError` is a `#[non_exhaustive]` enum of two `String`/`&'static str` variants describing a
  malformed environment variable. It reads no environment itself.

**`reliar-core`'s dependency graph gains nothing.** No new crate, no new feature, no new `use`
beyond `core`/`std`. The CI purity gate's banned list — `sqlx*`, `postgres`, `tokio-postgres`,
`async-nats`, `nats`, `rdkafka`, `lapin`, `redis` — is unaffected, and `async-nats` remains banned
from `reliar-core` **and** `reliar-outbox` exactly as today.

### 2. Module placement in `reliar-core`

Two modules, not one:

```text
crates/reliar-core/src/failure.rs    →  pub trait Classify, pub enum FailureKind
crates/reliar-core/src/publisher.rs  →  pub trait Publisher
crates/reliar-core/src/settings.rs   →  pub enum SettingsError + its constructors
```

`Classify` does **not** live in `publisher.rs`. Its other consumer is `OutboxStore::Error`, and a
store author reading `use reliar_core::publisher::Classify` would reasonably conclude they had
imported the wrong thing. Core's modules are already one-concept-per-file (`headers`, `ids`,
`metadata`, `serializer`); this follows that.

`settings.rs` in core holds **only `SettingsError`**. `OutboxSettings`, `DispatcherSettings`,
`RetentionSettings` and every private `env_*` parser stay in `reliar-outbox`;
`PostgresOutboxSettings` and `NatsSettings` stay in their crates. Promoting the shared `env_u32` /
`env_duration_ms` parsing helpers into a public core module is **explicitly not decided here** —
that would grow core's public surface for an ergonomics reason rather than a contract reason, and
it is a separate proposal if the duplication ever bites.

### 3. Signatures are unchanged

Every signature, bound, default method body and documented semantic is preserved verbatim. This is a
relocation, not a redesign: `Publisher: Send + Sync` with `type Error: std::error::Error + Send +
Sync + 'static + Classify`, `publish` / `publish_batch` returning `impl Future<..> + Send`, the
positional-results guarantee, the looping default `publish_batch`, the "v0.1's dispatcher calls
`publish`" note, `Classify::kind`, `FailureKind::{Transient, Permanent}`, and
`SettingsError::{parse, out_of_range, key}` with `Clone + Debug + PartialEq + #[non_exhaustive]`.

Two rustdoc links must change, because a doc link may only point **inward**:

- `Publisher::publish`'s "retry is the dispatcher's and [`crate::RetryPolicy`]'s job" — becomes a
  plain code span naming `reliar-outbox`'s `RetryPolicy`, not an intra-doc link.
- `Classify`'s reference to [`crate::OutboxStore::Error`] — becomes a plain code span.

Prose that names `reliar-outbox` from core is fine and wanted (it explains who the first consumer
is); a resolvable *link* upward is not possible and must not be faked.

### 4. `reliar-outbox` keeps re-exports

```rust
pub use reliar_core::{Classify, FailureKind, Publisher, SettingsError};
```

They stay, as **convenience aliases with no second definition**, for three reasons: `Classify` is a
bound `OutboxStore` imposes, so a store author must find it from the outbox docs; the existing
`use reliar_outbox::{AcquireRequest, Classify, FailureKind, OutboxStore, WorkerId};` style imports
in provider tests stay one line; and it costs nothing — same type, one extra path.

The rule attached to them: **new code and all normative prose name the canonical `reliar_core::`
path.** Contract files, ADRs, rustdoc and every crate's `src/` use `reliar_core::`. Test files may
keep an aggregate `reliar_outbox::{…}` import where the same line also imports genuine outbox items.

### 5. `reliar-transport-nats` drops `reliar-outbox` entirely

Verified by grep over the whole crate (`src/`, `tests/`, `benches/`): the four items above are the
complete set of what it uses. With them in core, the dependency edge has no remaining
justification, so it is removed from `[dependencies]` and **is not** added to `[dev-dependencies]`.
The crate's dependency line becomes:

```text
reliar-transport-nats ──▶ reliar-core   (EnvelopeMapper, SerializedEnvelope, Metadata,
                     └──▶ async-nats     Publisher, Classify, FailureKind, SettingsError)
```

`reliar-store-postgres` **keeps** its `reliar-outbox` dependency: it implements `OutboxStore` and
`OutboxDeadLetters` and names `OutboxRecord`, `AcquireRequest`, `DeadReason`, `WorkerId`,
`PurgeRequest` and more. Only its imports of the four relocated items re-point to `reliar_core::`.

### 6. The dependency rule, restated

> Providers (`reliar-store-*`, `reliar-transport-*`) depend on `reliar-core` **directly**, and on an
> abstraction crate (`reliar-outbox`, `-inbox`, `-idempotency`) **only when they implement a trait
> that crate owns**. Providers never depend on each other, in any dependency kind.

`reliar-store-postgres` implements `OutboxStore` → it depends on `reliar-outbox`.
`reliar-transport-nats` implements only core traits → it depends on core alone. This is a
sharpening of conventions §2, not a reversal: the arrows still point inward only.

## Consequences

- **Nothing is published, so nothing breaks.** `CHANGELOG.md` has only *Unreleased*; there is no
  semver event and `cargo semver-checks` has no published baseline to compare against. This is the
  last cheap moment to make this move — after 0.1.0 it would be a breaking change to four crates.
- **A transport crate no longer builds the outbox.** `reliar-transport-nats`'s compile graph loses
  `reliar-outbox` and, transitively, `tokio-util` and the dispatcher's code. Marginal build-time
  win; the real win is that the graph now states the truth.
- **Phase 3 is unblocked without a further move.** `reliar-messaging` can publish against
  `reliar_core::Publisher` without depending on the outbox, and `reliar-inbox` /
  `reliar-idempotency` get `Classify` and `SettingsError` for free.
- **`reliar-core` grows by three small modules and four public items.** Judged against §1's test,
  not against a count. The next candidate for core must pass the same two-part test in its own ADR;
  "it is small and two crates want it" is not sufficient if it names a storage or transport concept.
- **Both contract files change**, and `phase2-contract.md` was frozen. Per its own preamble the
  change route is ADR-first — this is that ADR. The moved signatures are byte-identical, so no
  engineer has to rewrite logic, only imports.
- **SRS §43.C's draft item C4 needs a wording fix.** C4 currently asserts that `git diff --stat`
  shows no file under `crates/reliar-outbox/src/` or `crates/reliar-core/src/` changed. This
  refactor changes files under both while introducing no NATS symbol anywhere. The binding clause is
  and should be the `cargo tree` gate plus "contains no NATS symbol"; the diff clause must be scoped
  to NATS-specific symbols or dropped. Draft text is in
  `../../sisa-reliar-backlog/docs/analysis/srs-amendment-publisher-in-core.md`. §43.B's purity item
  is unaffected — core still has no infrastructure dependency.
- **A new CI gate becomes possible and is deferred to the same change.** `reliar-transport-nats`
  must not reacquire a `reliar-outbox` edge, in any dependency kind — a regression there would mean
  an outbox concept had leaked into a transport. The assertion is architect-owned
  (`.github/workflows/ci.yaml`, `purity` job) and is committed together with the move, not before:
  a gate asserting the absence of a dependency that still exists would fail, and one written after
  the fact tends never to be written.

## Alternatives considered

**Leave everything in `reliar-outbox`.** Rejected. It makes `reliar-messaging` and every future
transport depend on the transactional outbox to name a publish failure, and it keeps a transport
crate compiling a dispatcher it will never call. The dependency edge would be permanent and
load-bearing for the wrong reason.

**Move `Publisher` only; leave `SettingsError` in `reliar-outbox`.** Rejected, and this is the pivot
of the decision. `SettingsError` is one of exactly two reasons `reliar-transport-nats` depends on
`reliar-outbox`; leaving it behind keeps the whole edge alive and buys nothing. Its own contract
ruling (§7 I3) already declares it the shared type, and its public constructors exist only so
foreign crates can build it — it has been a core primitive in everything but address since S5.

**A new `reliar-publish` crate holding just these traits.** Rejected under SRS §6 (crates are
created when their implementation begins) and §31 (no abstraction for hypothetical portability). A
crate with one trait, one marker trait and two enums, that every other crate depends on, is
`reliar-core` with extra manifest.

**Keep a duplicate definition in `reliar-outbox` and convert.** Rejected outright. Two `FailureKind`
enums that must be mapped at every boundary is strictly worse than either single home, and it makes
`P::Error: Classify` ambiguous at the dispatcher.

**Put `Classify` in `publisher.rs` inside core.** Rejected — see §2. It would mis-name a trait whose
other implementor is a database store.

**Move the `test-support` publisher fakes (`RecordingPublisher`, `ScriptedPublisher`,
`FakePublishError`) to core as well.** Rejected for now. They are dispatcher-driving fakes, they
would force a `test-support` feature onto `reliar-core`, and no crate outside `reliar-outbox` and
its provider imports them today (`reliar-transport-nats` deliberately has its own). Revisit when
Phase 3 needs a publisher fake with no outbox in sight; nothing here blocks that.

---

## Amendments

### Amendment A — 2026-09-05 — a `tokio` **dev**-dependency in `reliar-core` is inside this ADR

Prompted by the RELIAR-40 review
(`../../../sisa-reliar-backlog/docs/analysis/reviews/phase2-publisher-move-review-1.md`, nit 9).
Work-list item 5 said "no change expected" to `crates/reliar-core/Cargo.toml`, and the engineer
added `tokio = { workspace = true, features = ["rt-multi-thread", "macros"] }` to
**`[dev-dependencies]`**, because the test that moved with the trait
(`crates/reliar-core/tests/publisher_batch_default_is_positional.rs`) is a `#[tokio::test]`:
`Publisher::publish` is a native async fn in a trait (conventions §3), so the only way to exercise
the default `publish_batch` loop is to drive a future on some runtime, even though every future in
that test resolves immediately.

**Ruling — ratified, and the rule is stated rather than left to judgement.**

1. **§1's kind test is about the crate's *published* graph, not its test harness.** A
   dev-dependency is compiled only when `reliar-core`'s own tests, benches or examples are built.
   It is absent from the graph a host resolves when it depends on `reliar-core`, it cannot appear
   in a signature, a bound, a re-export or a doc link, and it cannot make a public item name a
   runtime. The two questions §1 asks — does the item name a storage engine, broker or routing
   concept, and is it vocabulary more than one capability needs — are questions about the public
   surface; a test's executor answers neither. `reliar-core` stays runtime-agnostic in the only
   sense the contract can be held to.
2. **`-e normal` is the right edge kind for the CI purity gate, and it stays.** The gate asserts
   what a *host* pulls in, so it must read normal edges; widening it to `-e all` would fail on a
   test executor while catching no leak that matters, and §F item 25's
   `cargo tree -p reliar-core -e normal --all-features` is unchanged by this addition — that is the
   evidence, not an argument. The ban list (`sqlx*`, `postgres`, `tokio-postgres`, `async-nats`,
   `nats`, `rdkafka`, `lapin`, `redis`) is likewise a normal-edge assertion and is untouched;
   note that `tokio-postgres` remains banned in every kind while plain `tokio` never was.
3. **No new crate enters the workspace.** `tokio` is already in `[workspace.dependencies]` and is a
   normal dependency of `reliar-outbox` and `reliar-store-postgres`. The lockfile, the
   `cargo deny` license/advisory surface and the audit surface are unchanged; `workspace = true`
   keeps the version single-sourced.
4. **The standing rule for `reliar-core`'s manifest.** A `[dev-dependencies]` addition needs no
   escalation when all three hold: the crate is already in `[workspace.dependencies]`; it is named
   by no file under `crates/reliar-core/src/`; and `cargo tree -p reliar-core -e normal
   --all-features` is unchanged. Any change to `[dependencies]` or `[features]` still stops and
   escalates — including adding `tokio` there. A test-only runtime edge is **not** licence to reach
   for `tokio::time`, `tokio::sync` or a spawn in core's `src/`: core defines vocabulary, it runs
   nothing.

**Alternative considered.** *Drive the future without a runtime* — `futures::executor::block_on` or
`pollster`. Rejected: it adds a crate to the workspace to avoid a dev edge that costs nothing, and
it would make this one test read differently from every other async test in the repo for no
contract benefit. The comment already in the manifest explaining *why* the dep exists is the right
weight of documentation for it.

**Consequences.** Work-list item 5 is amended above. Nothing in §1–§6 changes; no contract file,
no CI workflow and no `CHANGELOG` entry follows from this — a dev-dependency is not a published
fact.

## Engineer work list (one engineer, one atomic change — card id assigned by the PO;
next free is RELIAR-39, since RELIAR-33 is Phase 2's S2 slice)

Do it in one commit/PR: the workspace must not be half-moved at any point that CI observes.

### A. `reliar-core`

1. New `crates/reliar-core/src/failure.rs` — move `Classify` + `FailureKind` verbatim from
   `crates/reliar-outbox/src/publisher.rs`. Rewrite the `crate::OutboxStore::Error` intra-doc link
   as a plain code span; keep the ADR 0008 rationale sentence.
2. New `crates/reliar-core/src/publisher.rs` — move `Publisher` verbatim. Rewrite the
   `crate::RetryPolicy` intra-doc link as a plain code span. `use crate::SerializedEnvelope;`.
3. New `crates/reliar-core/src/settings.rs` — move `SettingsError`, its `impl` block
   (`parse`, `out_of_range`, `key`), `Display` and `Error` impls verbatim from
   `crates/reliar-outbox/src/settings.rs`. Reword the doc that says "a crate other than
   `reliar-outbox`" to "a crate other than the one defining a given `Settings` type". Reword
   "Why `OutboxSettings::from_env` failed" to the general "Why a `*Settings::from_env` failed",
   naming `OutboxSettings::from_env` as an example in prose.
4. `crates/reliar-core/src/lib.rs` — declare the three modules and
   `pub use failure::{Classify, FailureKind}; pub use publisher::Publisher;
   pub use settings::SettingsError;`. Add one bullet to the crate-doc `# Guarantees` list stating
   the §1 kind test: core owns vocabulary every capability shares, and names no storage engine,
   broker or routing concept.
5. `crates/reliar-core/Cargo.toml` — **no change expected** in `[dependencies]` or `[features]`.
   If anything new is needed there, stop and escalate: that would mean the move is not the pure
   relocation this ADR authorises. *(Amended 2026-09-05 — a `[dev-dependencies]` entry is outside
   this rule and the `tokio` one added by RELIAR-40 is ratified; see [Amendment A](#amendments).)*

### B. `reliar-outbox`

6. Delete `crates/reliar-outbox/src/publisher.rs` and the `mod publisher;` line.
7. Delete the `SettingsError` definition + `impl`s from `crates/reliar-outbox/src/settings.rs`;
   add `use reliar_core::SettingsError;`. The `env_*` private helpers stay.
8. `crates/reliar-outbox/src/lib.rs` — replace `pub use publisher::{Classify, FailureKind, Publisher};`
   and the `SettingsError` item in the `settings::{…}` re-export with
   `pub use reliar_core::{Classify, FailureKind, Publisher, SettingsError};`. Keep the crate-doc's
   `[`Publisher`]` link (it resolves through the re-export); add a one-line note that these four are
   re-exported from `reliar-core` (ADR 0032).
9. Re-point in-crate `use`s: `store.rs`, `dispatcher.rs`, `retry.rs`, `error.rs`, `metrics.rs`,
   `test_support.rs` — anything naming `crate::publisher::…` or the local `SettingsError` now uses
   `reliar_core::`.

### C. `reliar-store-postgres`

10. `src/error.rs:12` → `use reliar_core::{Classify, FailureKind};`
11. `src/settings.rs:10` → `use reliar_core::SettingsError;`
12. `src/lib.rs:70` → `pub use reliar_core::SettingsError;`
13. Leave `Cargo.toml` alone — `reliar-outbox` stays a real dependency (`OutboxStore`,
    `OutboxRecord`, `AcquireRequest`, `DeadReason`, `WorkerId`, `PurgeRequest`, …).
14. Tests: `tests/postgres/outbox_error_classification.rs` and
    `tests/postgres/outbox_enqueue_serialize_error.rs` import `Classify`/`FailureKind` — split the
    import so those two come from `reliar_core`. Other test files need no change.

### D. `reliar-transport-nats`

15. `src/publisher.rs:10` → `use reliar_core::{Classify, FailureKind, Publisher};`
16. `src/settings.rs:10` → `use reliar_core::SettingsError;`
17. `src/lib.rs:22` → `pub use reliar_core::SettingsError;`, and rewrite the comment above it (it
    currently says "this crate already depends on `reliar-outbox`" — no longer true).
18. `src/publisher.rs:502` — the doc naming `reliar_outbox::ConfigError` as an analogy: keep the
    analogy, make it a plain code span, not a link (nothing resolves it once the dep is gone).
19. **`Cargo.toml`: delete `reliar-outbox = { workspace = true }`** and its two comment lines. Do
    not add it to `[dev-dependencies]`.
20. Tests + bench: re-point every `use reliar_outbox::{…}` to `reliar_core::` —
    `tests/publish_error_classification.rs`, `tests/nats/{n1,n3,n4,n5,n8,n9,n10,n11}*.rs`,
    `benches/nats_publish.rs`. Prose mentions of `reliar-outbox`'s test helpers in
    `tests/common/mod.rs` and `tests/nats/common/mod.rs` are comparisons, not imports — leave them.
21. `crates/reliar-transport-nats/README.md:7` — the dependency sentence is now "depends on
    `reliar-core` only (plus `async-nats`)".

### E. `tests/system` and `examples/`

22. `tests/system` legitimately dev-depends on both providers and on `reliar-outbox` (it drives
    `OutboxDispatcher`). Re-point only the four relocated items; the outbox dev-dep stays.
23. `examples/outbox-basic`, `examples/axum-outbox`, `examples/nats-pub-sub` — re-point imports of
    the four items; they are workspace members and must still compile.

### F. Verification (all must pass before review)

24. `cargo tree -p reliar-transport-nats -e normal | grep reliar-outbox` → **empty**.
25. `cargo tree -p reliar-core -e normal --all-features` → unchanged from before the move.
26. `cargo hack check --feature-powerset` for `reliar-core` and `reliar-outbox` (`reliar-core` now
    has items that no feature gates — confirm the `json`/`serde` powerset still compiles).
27. `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --workspace` — this is the gate that catches a
    doc link left pointing upward out of core.
28. `cargo fmt --all --check` · `cargo clippy --workspace --all-targets --all-features -- -D warnings`
    · `cargo test --workspace` · `cargo machete` (it will flag the removed NATS dep if step 19 is
    missed).
29. No `.sqlx/` regeneration is needed — no SQL changed.
30. `CHANGELOG.md` *Unreleased*: one line under **Changed** naming the four relocated items and
    the retained `reliar-outbox` re-exports.

### G. Not the engineer's

31. `.github/workflows/ci.yaml` — the new "a transport does not depend on an abstraction crate it
    implements nothing from" assertion in the `purity` job is **architect-owned** and lands with
    this change. Flag to the PO when the branch is ready rather than editing it.
32. The SRS amendment (§18, §19.4, §43.C C4) is the PO's, from the draft at
    `../../sisa-reliar-backlog/docs/analysis/srs-amendment-publisher-in-core.md`.
