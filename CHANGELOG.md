# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). Before 1.0 a minor bump may contain
breaking changes; every one of them is listed under **Changed** or **Removed**.

This file covers the workspace as a whole. Reliar's crates version **independently** (ADR 0034), so
a release section is dated and names the crate versions it shipped rather than carrying a single
workspace version; per-crate changelog files are not generated.

## [Unreleased]

### Removed

- **`reliar-outbox` 0.4.0** withdraws the routing rule shipped in 0.3.0 ([ADR
  0036](docs/decisions/0036-outbox-enqueue-and-publisher-passthrough.md), superseding [ADR
  0033](docs/decisions/0033-outbox-routing-publisher.md)): `OutboxPolicy` (module `policy`),
  `RouteKind`, `ScopedOutboxPublisher`, `OutboxPublisher::in_transaction`/`publish_direct`/`policy`,
  `RouteError`, `DirectPublishError`, `OutboxSettings::enabled`/`allowed_types`/`disallowed_types`
  (fields + builder setters), `MessageTypeNames`, `OutboxMetrics::routed` and
  `RecordingMetrics::routed()`, and the `RELIAR_OUTBOX_ENABLED`/`_ALLOWED_TYPES`/`_DISALLOWED_TYPES`
  environment keys — none of it has a replacement, because the rule itself is withdrawn.
- **`reliar-outbox` 0.4.0** also removes `OutboxStaging` (the trait, in module `staging`) and
  `OutboxStaging::stage`, renamed to `OutboxEnqueue` (module `enqueue`) and `OutboxEnqueue::enqueue`
  (decision #34, 2026-09-06) — see **Changed** below; shape, bounds and the abort-on-error
  invariant carry over unchanged, only the names differ.

### Changed

- **`reliar-outbox` 0.4.0**: `OutboxPublisher` is now a `reliar_core::Publisher` in its own right
  — its `publish`/`publish_batch` bypass the outbox entirely and forward to the transport
  byte-identical, with no Reliar-side guarantee at all. `new`/`with_metrics` lose their policy
  argument. `OutboxSettings` deserialization keeps `deny_unknown_fields`, so a document still
  carrying `enabled`/`allowed_types`/`disallowed_types` now fails to load, naming the field.
  The staging capability is renamed: `OutboxStaging` → `OutboxEnqueue` (module `staging` →
  `enqueue`), and its method `stage` → `enqueue` (decision #34, 2026-09-06). Shape, bounds and the
  abort-on-error invariant are unchanged.
- **`reliar-store-postgres` 0.4.0** — follows `reliar-outbox` 0.4.0 (its `reliar-outbox`
  requirement moves to `^0.4`). No schema, migration or `.sqlx/` change: the `OutboxEnqueue` impl
  is unchanged behaviour, reached now through `OutboxPublisher::enqueue` rather than the withdrawn
  `ScopedOutboxPublisher`; rustdoc retouched accordingly, including the `OutboxStaging` → `OutboxEnqueue` rename.

### Added

- **`reliar-outbox` 0.4.0** — `OutboxPublisher::enqueue`/`enqueue_batch`: enqueue a
  `SerializedEnvelope` in the caller's own transaction through `OutboxEnqueue`, the durable,
  at-least-once path published later by an `OutboxDispatcher`. `enqueue_batch` fails fast with
  `EnqueueBatchError { index, source }` naming the first envelope that failed to enqueue. New
  `OutboxMetrics::enqueued` hook and `reliar.outbox.enqueue`/`enqueue_batch` spans. **The call site
  now names the guarantee**: `enqueue` for durability, `publish` to bypass the outbox — nothing
  decides between them at runtime, and no setting can.
  - **Migration (0.3 → 0.4):** `outbox.in_transaction(&mut tx).publish(&e).await?` →
    `outbox.enqueue(&mut tx, &e).await?`; `outbox.publish_direct(&e).await?` →
    `outbox.publish(&e).await?`; delete `RELIAR_OUTBOX_ENABLED`/`_ALLOWED_TYPES`/
    `_DISALLOWED_TYPES` from every deployment and every config document (a document that keeps
    them now fails to load); a message type you had disallowed is now a plain `publish` call site,
    changed in code, not in configuration. A custom `OutboxStaging` implementation renames to
    `OutboxEnqueue` (module `staging` → `enqueue`) and its `stage` method to `enqueue`.

## 2026-09-05 — `reliar-outbox` 0.3.0 · `reliar-store-postgres` 0.3.0 · `reliar-transport-nats` 0.1.1

The outbox routing rule (SRS §20.2, ADR 0033) — later withdrawn and replaced by the enqueue/publish
rule recorded in *Unreleased* above (ADR 0036).

### Added

- **`reliar-outbox` 0.3.0** — the outbox routing publisher (SRS §20.2, ADR 0033 incl. Amendment
  D): one publish call that either stages a message in the outbox or sends it straight to the
  transport, decided by configuration rather than by the call site. The application-facing object
  **is** a `reliar_core::Publisher`.
  - `OutboxSettings` gains three top-level fields — `enabled` (default `true`), `allowed_types`,
    `disallowed_types` (both a new `MessageTypeNames` validated-list newtype) — with fallible
    builder setters, `{prefix}ENABLED`/`{prefix}ALLOWED_TYPES`/`{prefix}DISALLOWED_TYPES`
    `from_env` keys, and a `serde` repr that keeps validation from being bypassable through a
    config document. An overlapping allow/disallow pair is a `SettingsError::OutOfRange` at
    construction, on every path, never a silent tie-break.
  - `OutboxPolicy` (module `policy`) is the routing rule as a value: `from_settings` validates
    once, `decide(&MessageType) -> RouteKind` evaluates the rule's truth table — disallow wins, an
    empty allow list means every type is durable, a non-empty allow list is exhaustive. Pure,
    allocation-free, emits no tracing; previewable without a store or a transport.
  - `OutboxStaging<Tx>` (module `staging`) is the storage-agnostic staging capability a provider
    implements per transaction-handle type: `stage(&self, &mut Tx, &SerializedEnvelope)`.
  - `OutboxPublisher<S, P, M = NoopMetrics>` (module `publisher`) composes a staging capability, a
    `reliar_core::Publisher` and an `OutboxPolicy`. `in_transaction(&mut tx)` returns a
    `ScopedOutboxPublisher`, which **implements `reliar_core::Publisher`** for the life of the
    borrow — routed types staged in `tx`, direct types forwarded to the transport immediately and
    outside it. `publish_direct(&SerializedEnvelope)` reaches only the direct path and returns
    `DirectPublishError::TransactionRequired` for a routed type rather than silently downgrading.
    `OutboxPublisher` itself deliberately does **not** implement `Publisher` — a `'static`,
    `Clone`-able publisher here could be wired into an `OutboxDispatcher`, draining the outbox back
    into itself; the guard is enforced by the compiler (`ScopedOutboxPublisher` is neither
    `'static` nor `Clone`). Never retries. Nothing here serializes — the caller does, exactly as
    for a bare `NatsPublisher`, so both routes carry the same `SerializedEnvelope` value and the
    wire bytes never depend on the route taken. Emits one `reliar.outbox.route` span per call and a
    new `OutboxMetrics::routed` hook.
  - `test-support` gains `InMemoryTransaction`, an `OutboxStaging` impl on `InMemoryOutboxStore`,
    `fail_next_enqueue`/`enqueue_call_count`, and a `RecordingMetrics::routed()` getter for
    `OutboxMetrics::routed`.
  - `tests/system`'s `e2e` suite proves the rule against a real Postgres and a real `JetStream`
    stream: a routed type stages in `outbox` and only reaches the stream once a running
    `OutboxDispatcher` drains it, a non-routed type reaches the stream immediately and never
    appears as an `outbox` row, the "everything except" rollout shape and `enabled = false` both
    behave as documented, and a direct publish survives a rollback of the caller's transaction —
    the honest, non-transactional guarantee, asserted rather than merely documented
    (`e5_routing_stages_and_streams_together.rs`, `e6_disallow_wins_and_the_switch.rs`).
  - New guide `docs/guides/outbox-routing.md`: the rule's truth table, settings and env keys, both
    rollout shapes, what the direct path costs you, previewing `OutboxPolicy` standalone, and why
    `enabled = false` never stops the dispatcher draining. Linked from `README.md`,
    `docs/guides/getting-started.md` and `docs/guides/nats.md`.
  - `examples/nats-pub-sub` now publishes both its messages through an `OutboxPublisher` built from
    `OutboxSettings::from_env("RELIAR_OUTBOX_")`, printing which route each took (read from
    `outbox.policy().decide(..)`) — the same call site works unmodified whether `RELIAR_OUTBOX_*`
    routes both messages through the outbox or sends one of them direct.
  - `examples/axum-outbox`'s handler now publishes through `OutboxPublisher::in_transaction(&mut
    tx).publish(&serialized)` rather than the store's own inherent `enqueue`, so the §20.1
    reference integration doubles as the routing publisher's worked example.
- **`reliar-store-postgres` 0.3.0** — implements `reliar_outbox::OutboxStaging<sqlx::Transaction<'_,
  Postgres>>` for `PostgresOutboxStore<Ser>` (routing-publisher contract §6, ADR 0033 Amendment D):
  the provider side of `ScopedOutboxPublisher::publish`'s routed path. No new migration, no SQL
  text change, no `.sqlx/` change — `stage` reuses the same `search_path` handling and `insert_row`
  helper as `enqueue`/`enqueue_with` (factored into one shared `insert_staged` function so the two
  callers cannot drift), and persists `envelope.metadata.delivery.content_type` **verbatim** (the
  caller's own content type) rather than the store's configured serializer — the one semantic
  difference from the inherent `enqueue`/`enqueue_with`. `insert_row` is now generic over any
  envelope body (not only `T: Message`), so it also persists the `SerializedEnvelope`'s own
  `message_type`/`message_version` rather than deriving them from a Rust `Message` impl's
  `TYPE`/`VERSION` constants — an observable behaviour change on the already-published `enqueue`
  path, covered by a test that builds its envelope with `Envelope::from_parts` (naming a type no
  `Message` impl in the test binary declares) rather than through any `Message`. The `OutboxStaging`
  impl carries no `Ser: 'static` bound (it issues no statement through `self.serializer`) and no
  `where 'c: 'a` bound on its transaction-handle lifetime — see the impl's rustdoc — a regression
  covered by a real-Postgres test that spawns a scoped publish's future through `tokio::spawn`.

### Changed

- **`reliar-transport-nats` 0.1.1** — doc/dev-dependency-only patch: README gains a "Standalone
  use" section showing the crate used with `reliar-core` alone, no `reliar-outbox`/
  `reliar-store-postgres` in the graph; its doctest pulls in `reliar-core/json` as a
  dev-dependency. No production code, public API, or SQL changed.

## 2026-09-05 — `reliar-core` 0.2.0 · `reliar-outbox` 0.2.0 · `reliar-store-postgres` 0.2.0 · `reliar-transport-nats` 0.1.0

Phase 2: the NATS transport, and the relocation of the shared publish vocabulary into
`reliar-core` that made a transport possible without an outbox dependency.

### Added

- `reliar-core` gains the shared `Publisher` capability trait, `Classify`/`FailureKind` (a publish
  error's transient/permanent verdict) and `SettingsError` — the one error every
  `*Settings::from_env` returns. They moved here from `reliar-outbox` so a transport can implement
  them without depending on outbox mechanics ([ADR 0032](docs/decisions/0032-publisher-and-shared-primitives-in-core.md)).

- **`reliar-transport-nats`** (Phase 2, SRS §12, §12.3, §14–§16, §18, §19.4, §22–§23, §32–§33,
  ADRs 0026–0031): Reliar's first real transport, depending on `reliar-core` alone (plus
  `async-nats`) — never `reliar-outbox` — since `Publisher`/`Classify`/`FailureKind`/
  `SettingsError` all live in `reliar-core` (ADR 0032). `NatsEnvelopeMapper` projects the canonical
  envelope onto NATS headers plus a raw payload and back (`NatsWireMessage`, the `headers` module
  of framework constants, `NatsMapError`), never through `async_nats`'s panicking `&str` header
  conversions; `SubjectResolver`/`PrefixSubjects`/`DestinationSubjects` for subject selection, kept
  a transport-side concern out of `reliar-core` (ADR 0027); `NatsPublisher<R>` — an at-least-once
  `Publisher` over `JetStream` that awaits the server's ack before returning `Ok` (ADR 0028), with
  `NatsSettings` (`Default` + builder + opt-in `from_env`, no server URL/credentials — the
  application owns the connection and the stream, ADR 0029) and a `NatsPublishError`/`Classify`
  table fixed by ADR 0030. `async-nats` pinned `default-features = false, features = ["jetstream"]`
  (ADR 0031 §1); local/CI JetStream substrate (`deploy/compose`'s `nats` service, `test.yaml`'s
  `docker run -js` step, the crate's own one-binary `nats`-suffixed test harness) per ADR 0031 §2–§4.
- **`tests/system`** (new workspace package, `publish = false`, ADR 0031 §6): the only place
  `reliar-store-postgres` and `reliar-transport-nats` meet, both as dev-dependencies so neither
  provider dev-depends on the other. Its `e2e` suite (one Postgres **and** one NATS container,
  or `DATABASE_URL`/`NATS_URL`, mirroring the provider harnesses' RELIAR-27 shape) proves the
  phase end to end across four scenarios: **E1** — `OutboxDispatcher<PostgresOutboxStore,
  NatsPublisher<PrefixSubjects>>` drains every enqueued row into a real `JetStream` stream, each
  ending up `published_at` with a matching `reliar-message-id` header and byte-identical raw body,
  plus a clean cancellation (every lease released, nothing dead-lettered) and a separate,
  deterministic claim-stop trial proving rows past a capped batch are never touched; **E2** — a
  stream deleted while the dispatcher keeps running leaves its row retryable (`attempts`
  incremented, transient `StreamNotFound`) until a stream recapturing the same subject exists
  again; **E3** — a worker that publishes but crashes before its own `complete` has its lease
  reclaimed and the row republished, and `Nats-Msg-Id` inside the stream's `duplicate_window`
  keeps the stream at one copy despite two publishes reaching the wire (SRS §22's duplicate
  window, proven end to end); **E4** — an envelope permanently unrepresentable on this transport
  (ADR 0026 §3) dead-letters on its first attempt, is never retried, and its `last_error` never
  carries the offending header's value. Root `members` gains `"tests/*"`.
- `examples/nats-pub-sub` (workspace member, `publish = false`): pool → `PostgresOutboxStore` →
  `OutboxDispatcher` → `NatsPublisher` wiring, an explicitly created example `JetStream` stream
  (never created by Reliar itself, ADR 0029), and a Core NATS subscriber task decoding what
  arrives with `NatsEnvelopeMapper` — ids and message types only, never payload bytes (SRS §33).
- `docs/guides/nats.md`: stream ownership and the copy-pasteable `create_stream` snippet, the
  stream's `duplicate_window` versus retry backoff, subject strategy, the `NatsSettings`/env
  table, which legal envelopes NATS cannot carry (tracked as RELIAR-35), the `NATS_URL` convention
  for tests/examples, and the `/jsz?config=1` healthcheck note (ADR 0031). `docs/architecture/overview.md`'s
  crate map and dependency rule now include `reliar-transport-nats` and `tests/system`.

### Changed

- **Breaking (pre-1.0): `reliar_outbox::{Publisher, Classify, FailureKind, SettingsError}` are now
  re-exports of `reliar_core`'s items, not definitions** ([ADR 0032](docs/decisions/0032-publisher-and-shared-primitives-in-core.md)).
  The paths still resolve, so most code compiles unchanged, but the items are different types from
  the ones `reliar-outbox` 0.1.0 defined: anything compiled against 0.1.0 must be recompiled, and
  `reliar-core` 0.2.0 is now required alongside. The canonical path in every signature and doc is
  `reliar_core::`. `cargo semver-checks` reports the 0.1.0 → 0.2.0 difference as four removed
  items, which is why this is a minor bump and not a patch.
- **`reliar-store-postgres` requires `reliar-outbox` 0.2.0 and `reliar-core` 0.2.0.** Its own API is
  unchanged; the release exists so that a user on `reliar-outbox` 0.2.0 has a Postgres store that
  admits it — `reliar-store-postgres` 0.1.0 requires `reliar-outbox ^0.1.0`, which does not admit
  `reliar-outbox` 0.2.0, so no published `reliar-store-postgres` could be used alongside it
  (ADR 0034 §3).
- **Versions are owned by the change that needs them, and `release-plz` publishes rather than
  decides** ([ADR 0034](docs/decisions/0034-versioning-and-release-flow.md)). `release-plz.toml`
  configures the `release` command only, the `release-pr` step is gone, and `ci.yaml`'s
  `versioning` job fails a pull request that edits a crate whose version is already on crates.io
  without bumping it, then runs `cargo semver-checks` against the published baseline. This is the
  gate the first Phase-2 release attempt lacked: it packaged `reliar-transport-nats` against the
  registry's `reliar-core` 0.1.0, which has none of the items ADR 0032 moved, and failed to verify
  the tarball.
- **`NatsSettings::max_in_flight` renamed to `batch_pipeline_depth`** ([ADR 0028 Amendment
  B](docs/decisions/0028-jetstream-ack-is-the-only-publish-path.md#amendment-b--2026-09-05--the-pipeline-depth-setting-is-batch_pipeline_depth-not-max_in_flight),
  contract §4.1): the field, the builder method, the env key (`{prefix}MAX_IN_FLIGHT` →
  `{prefix}BATCH_PIPELINE_DEPTH`) and `NatsConfigError::ZeroInFlight` →
  `NatsConfigError::ZeroBatchPipelineDepth`. `from_env` simply never looks up the old
  `{prefix}MAX_IN_FLIGHT` key any more — a value set under it is silently **ignored**, not
  rejected. The old name collided in meaning with `DispatcherSettings::max_in_flight`
  (`reliar-outbox`), which bounds the dispatcher's concurrent publish tasks — a different knob
  entirely. `reliar-transport-nats` had not been published when this landed, so no released
  version ever carried the old name.

## 2026-09-04 — `reliar-core` 0.1.0 · `reliar-outbox` 0.1.0 · `reliar-store-postgres` 0.1.0

Phase 1: the outbox contract, its dispatcher, the PostgreSQL provider, and the platform.

### Added

- Cargo virtual workspace, pinned toolchain, workspace lints and explicit build profiles.
- GitHub Actions: `ci`, `test`, `security`, `codeql`, `scorecard`, `release`, plus Dependabot.
- `deny.toml` licence allow-list and dependency bans; `deploy/compose` local Postgres 18 stack.
- `reliar-core`: identity newtypes (`MessageId`/`ConversationId`/`RequestId`/`CorrelationId`),
  the `Message` contract, `MessageType`, `ContentType`, `Serializer` + `JsonSerializer` (feature
  `json`), typed `Metadata` (`CorrelationMetadata`/`TraceContext`/`RoutingMetadata`/
  `DeliveryMetadata`/`EndpointAddress`), validating `Headers`, `Envelope<T>`/`SerializedEnvelope`
  with its builder, and the `EnvelopeMapper` trait (SRS §9–§17).
- `reliar-outbox`: the storage-agnostic outbox contract and settings — `OutboxStore`/
  `OutboxDeadLetters`/`Publisher` (+ `Classify`/`FailureKind`), their request/result types
  (`AcquireRequest`, `AcquiredBatch`, `CompletedMessage`, `FailedMessage`, `FailureOutcome`,
  `DeadReason`, `MessageRef`, `PurgeRequest`/`PurgeReport`, `OutboxStats`, `DeadQuery`/
  `DeadLetterPage`, `PoisonedRow`), `OutboxRecord` + its builder, the pure `RetryPolicy`/
  `ExponentialBackoff`, `Ordering`, `OutboxSettings`/`DispatcherSettings`/`RetentionSettings`
  with an opt-in `from_env`, and the `OutboxMetrics`/`NoopMetrics` hook (SRS §19–§26, §33.1).
- `reliar-outbox`'s `OutboxDispatcher`/`OutboxDispatcherBuilder`/`DispatchError`: the worker
  loop — bounded-concurrency claim → publish → batch `complete`/`fail` through the configured
  `RetryPolicy`, half-lease `extend_lease` renewal for long batches, a `stats_interval` tick
  feeding `OutboxMetrics`, `tracing` spans/events (`reliar.outbox.claim`/`publish`/`retry`/
  `dead`), and graceful cancellation that drains in-flight publishes for `drain_timeout`,
  persists what resolved, and releases the rest (SRS §21, §21.1, §22, §22.1, §23, §26, §26.1,
  §33.1; ADR 0006, 0007, 0009, 0013, 0014). Every `OutboxStore` call `run` makes is bounded by
  the new `DispatcherSettings::store_timeout` (default 10 s, validated shorter than half the
  lease-renewal period — `ConfigError::StoreTimeoutTooLong` otherwise, RELIAR-26). The
  outcome-write retry is raced against the lease-renewal tick so a hung `complete`/`fail` can
  never starve renewal; a fresh outcome is written eagerly, and only a write that itself fails
  is paced — by a fixed `outcome_retry_interval` derived from `poll_interval` (capped at a
  quarter of `lease`) — so neither a healthy store is throttled nor a fast-failing one spins at
  CPU speed (RELIAR-26); `poll_interval`/`idle_poll_interval` must both be greater than zero
  (`ConfigError::ZeroPollInterval` otherwise, RELIAR-26) since a zero `poll_interval` would
  re-enable that same spin via `outcome_retry_interval`'s basis. The retry policy is
  `DispatcherSettings::retry` by default (`DefaultRetry` marker type) or a fully custom
  `RetryPolicy` via `.retry_policy(..)`, never both silently. The claim loop is bounded by
  `max_in_flight` (never re-claims more than the free capacity it actually has, so a slow
  publisher cannot make one worker hoard the whole backlog's leases); a `complete`/`fail` write
  that fails or times out keeps its rows outstanding and is retried on a later iteration instead
  of being forgotten, unless it has been retried longer than `lease`, in which case the row is
  dropped from the claim gate's accounting and lease renewal so another worker can reclaim it
  (the gate always frees); a write classified `Permanent` ends `run()` with
  `Err(DispatchError::Store)` after a best-effort drain instead of retrying forever. A row whose
  publish succeeded but whose `complete` never landed is left to its lease at drain rather than
  released. `Semaphore`-bounded concurrent `Publisher::publish` calls remain a second,
  independent bound alongside the claim gate — kept deliberately, since `OutboxStore` is a
  public extension point whose conformance to the requested `batch_size` cannot be assumed. New
  `ConfigError::ZeroBatchSize`. `tokio`/`tokio-util` are now unconditional dependencies of
  `reliar-outbox`.
- `reliar-outbox`'s `test-support` feature: `InMemoryOutboxStore` (a full `OutboxStore` +
  `OutboxDeadLetters` fake with worker-guarded state changes, an injectable clock for lease
  expiry/retry timing, and bounded `purge`), `RecordingPublisher`/`ScriptedPublisher` (+
  `PublishStep`/`FakePublishError`, plus `InMemoryOutboxStore::hang_next` for simulating a slow
  store call) and `RecordingMetrics`, reused by provider crates and examples (SRS §8.1, §43.A.27).
- `reliar-store-postgres` (S5 slice): `migrations/0001_outbox.sql` — the full v0.1 schema with
  every constraint/index explicitly named (`pk_outbox`, `ck_outbox_*`, `ix_outbox_*`, including
  the two dead-row indexes and the `dead_reason` codec, ADR 0023); `migrate(&pool,
  MigrateOptions)` with isolated `<schema>._migrations` bookkeeping (never `_sqlx_migrations`,
  ADR 0018); `PostgresOutboxSettings` with `from_env`; `PostgresOutboxStore::connect`/`new`/
  `with_settings` with fail-fast startup `search_path` verification (ADR 0017); `enqueue`/
  `enqueue_with` (the transactional write path, ADR 0008); the `OutboxStore::acquire` impl — the
  single-statement `FOR UPDATE SKIP LOCKED` claim (ADR 0006) with poisoned-row handling (an
  undecodable row is moved to dead with `DeadReason::Undecodable` and reported, batch continues,
  §19.5); the promoted-column + `MetadataRest` JSONB reconstruction (ADR 0012); `.sqlx/`
  committed. MSRV 1.94, set by `sqlx` 0.9 (ADR 0025).
- `reliar-store-postgres` (S6 slice): the outcome path — worker-guarded `complete`/`fail`
  (`fail` splits retry vs. dead outcomes into two `UNNEST`-batched updates), `release`/
  `extend_lease` on `&[MessageRef]`; bounded `purge` (three `LIMIT`-capped statements: published
  delete, dead delete, and the expired→dead sweep, the last guarded by the claim's lease clause
  so a live worker's row is never swept from under it); `stats`; the `OutboxDeadLetters`
  `list_dead`/`retry_dead`/`purge_dead` impls (`list_dead` orders by `sequence`, keyset-paginated,
  filters optional). Row-count shortfalls logged at `debug`, never an error (ADR 0008).
  Per-variant `Classify` for `PostgresStoreError`/`EnqueueError` (SQLSTATE-class-based for
  `Database`, contract §7 J1); `42P01` maps to `NotMigrated` on every operational path, not just
  startup (§7 J2); schema-identifier validation (`[A-Za-z_][A-Za-z0-9_$]*`, ≤ 63 bytes) before
  it reaches `dangerous_set_table_name`/`SET search_path` (§7 J4); `migrate` now returns a
  provider-owned `MigrateError { InvalidSchema, Sqlx }` (§7 J3); `MetadataRest.sent_at` is
  epoch-milliseconds, not RFC 3339, so it can never fail to serialize (§7 J5); `statement_timeout`
  wraps the `acquire` claim when non-zero — and, after review, every other Reliar-owned op
  (`complete`/`fail`/`release`/`extend_lease`/`stats`) the same way; `complete` increments
  `attempts`; `list_dead`'s `DeadQuery.limit` is capped at 1000.
- `reliar-store-postgres` (S7 slice): end-to-end `reliar-outbox::OutboxDispatcher` tests against
  a real Postgres (publish-drain-releases-leases, permanent-failure-to-dead); real transaction-
  mode pooler tests for both `PgBouncer` and `PgDog` (decision #28, §43.A.35) — `PgBouncer`
  rejects the URL-`options` `search_path` parameter outright (`08P01`), while `PgDog`
  (`ghcr.io/pgdogdev/pgdog:v0.1.46`) passes it through instead, so only `PgBouncer` needs the
  `ALTER ROLE … SET search_path` server-side default to make the full enqueue → `SKIP LOCKED`
  claim → complete/fail/purge path behave as it does direct (both are proven; the difference is
  documented in `lib.rs`'s `search_path` section); a real "held lock" `SKIP LOCKED` proof (a
  second connection holds `FOR UPDATE` open on a known subset; `acquire` still returns promptly
  with exactly the complement); the test harness clones a migrated template database
  (`CREATE DATABASE … TEMPLATE`) instead of re-running `migrate()` per test; `test.yaml`'s
  Postgres service is now a `strategy.matrix` (currently one entry — 18 is still the newest
  published major); `sent_at`/`expires_at`/`deduplication_id`/`ordering_key` round-trip proptest
  coverage (incl. the full `time::OffsetDateTime` representable date range for `sent_at`) and a
  dedicated `sent_at_ms` epoch-milliseconds codec test (`i64::MAX`/`MIN` poison cleanly rather
  than panic). `stats()` now issues one `count(*) FILTER (...)` statement instead of four —
  measured cheaper by `EXPLAIN (ANALYZE, BUFFERS)` on a realistic 20k-row seed (fewer buffer
  touches, one round trip instead of four) since two of the four original statements fell back to
  a `Seq Scan` anyway. `purge`'s three statements each repeat their subselect's own predicate in
  the outer `WHERE`/`SET` clause, so a row `retry_dead` or a lapsed-lease worker's `complete`
  concurrently transitions between the subselect's snapshot and the statement's lock acquisition
  is never silently deleted or double-transitioned (`EvalPlanQual` re-check, review 3 B1/M1);
  `statement_timeout` now bounds every statement Reliar issues on its own pool, not just
  `acquire`/`complete`/`fail`/`release`/`extend_lease`/`stats` — `purge`, `list_dead`,
  `retry_dead`, `purge_dead` too. `PostgresOutboxSettings.listen_notify` (never implemented)
  removed rather than merely documented as inert.

- `examples/outbox-basic` and `examples/axum-outbox` (workspace members, `publish = false`): the
  in-memory quickstart and the §20.1 reference Axum integration (handler-owned transaction +
  `enqueue`, `PostgresOutboxStore::with_settings`/`migrate()` behind an explicit `--migrate` flag,
  an `OutboxDispatcher` whose `CancellationToken` is tied to SIGINT graceful shutdown).
- `docs/guides/getting-started.md` and `docs/guides/postgres.md` (search_path setup incl.
  pooler `ALTER ROLE`, `migrate()` vs. the release SQL artifact, settings/env tables, dead-letter
  ops, the retention/purge loop, `statement_timeout`); `docs/architecture/envelope.md` and
  `docs/architecture/outbox.md`; `crates/reliar-core/README.md` and
  `crates/reliar-outbox/README.md` (the latter states the three duplicate windows and the
  no-ordering default, §43.A.11/13).
- `crates/reliar-core/benches/serialization.rs` (criterion): `Envelope<T>` ⇄ `SerializedEnvelope`
  cost through `JsonSerializer`.

### Fixed

- `reliar-store-postgres`'s Postgres-backed test suite (RELIAR-27): consolidated every
  Postgres-touching scenario file into one `harness = false`/`libtest-mimic` binary
  (`tests/postgres/main.rs`) whose `main` owns the shared container as a local and drops it
  explicitly before exiting, plus the `testcontainers` `watchdog` feature and a label-scoped sweep
  in `scripts/test.sh` — fixes a leak (167 containers / 31 GB of volumes observed) caused by every
  scenario file being its own binary, each parking its container in a `static` that Rust never
  drops at process exit.

### Changed

- **Minimum supported Rust version is now 1.88** (was 1.85). RUSTSEC-2026-0009 (`time` RFC 2822
  parsing, stack exhaustion) has no patched release that builds on 1.85, and a security advisory
  outranks the MSRV floor. See [ADR 0024](docs/decisions/0024-msrv-1-88-and-msrv-policy.md), which
  also records the policy for future advisory-versus-MSRV conflicts.

### Removed

- **`scripts/` and `tools/` are gone** (human decision #32,
  [ADR 0022](docs/decisions/0022-workspace-ci-and-yaml-policy.md)). `scripts/{test,lint,release,dev-db}.sh`,
  `scripts/msrv-exclusions.py`, `tools/xtask` (and its `cargo xtask` alias) and
  `tools/reliar-migrate` were five indirections in front of six commands. The commands are now
  spelled out in `CONTRIBUTING.md` and are literally what CI runs; the `msrv` job's ADR-0025
  exclusion list is inline `bash` + `jq` (Python is no longer a CI dependency); and the migration
  CLI is now **`cargo run -p reliar-store-postgres --example migrate`**
  (`crates/reliar-store-postgres/examples/migrate.rs`), which needs neither a manifest nor a
  second MSRV. Root `members` is `["crates/*", "examples/*"]`.
- The second pooler integration scenario (`tests/postgres/outbox_pgbouncer.rs`). **PgDog is the
  single transaction-mode pooler substrate** (human decision #31, amending decision #28 and
  [ADR 0021](docs/decisions/0021-testcontainers-and-pooler-test-substrate.md)): two poolers proved
  the same property at twice the container cost, and the one behaviour the removed scenario
  covered — a pooler that drops the URL `options` must make construction fail fast — needs no
  pooler at all and is asserted by
  `outbox_schema_verification::construction_fails_fast_without_search_path` and by a bare
  no-`options` connection inside the PgDog scenario. `deploy/compose`'s `pooler` profile now runs
  the same PgDog image and tag, so a by-hand reproduction matches CI. The docs name no pooler
  brand as "the one that drops startup options" — whether yours does is a property of its build
  and configuration, so verify it.

### Security

- `time` floored at `0.3.47` in `[workspace.dependencies]`, patching **RUSTSEC-2026-0009** /
  CVE-2026-25727. The floor is in the manifest, not only the lock file, so consumers resolving the
  graph themselves also get the patched version.
