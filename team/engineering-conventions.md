# Engineering Conventions

The team's law for **Sisa Reliar**. The SRS (`../sisa-reliar-backlog/docs/srs.md`) is the architecture baseline; this file turns it
into day-to-day decisions. The **skills** carry the detailed code patterns and load on demand.
Change a decision via an ADR in `docs/decisions/` (SRS §38, §45).

> **Fastest way to be correct: copy an existing crate/trait/test.** Mirror the structure, names,
> and file boundaries of a known-good slice rather than inventing.

---

## 1. Stack (fixed)

**Language & runtime** — Rust edition **2024**, toolchain pinned in `rust-toolchain.toml`, an
explicit **MSRV** in `[workspace.package] rust-version`. `#![forbid(unsafe_code)]` in every crate.
**Tokio** (multi-thread runtime, `tokio-util` for `CancellationToken`).

**Storage** — **sqlx** with PostgreSQL first (`reliar-store-postgres`), macros only; MySQL later
as a separate provider crate. **Transport** — `async-nats` first (Phase 2), then RabbitMQ/Kafka.

**Core deps** — `bytes` (payloads) · `uuid` (v7 ids) · `time` for timestamps (decided 2026-09-03; never `chrono`;
see decisions log) · `serde` + `serde_json` behind a `json` feature for the default serializer ·
`tracing` (+ optional `metrics` feature). `reliar-core` has **no** sqlx/postgres/nats/kafka/redis deps.

**Tooling** — `cargo fmt` (`rustfmt.toml`) · `cargo clippy -D warnings` (`clippy.toml`) ·
`cargo deny` (`deny.toml`) · `cargo audit` · `cargo hack --feature-powerset` · `cargo semver-checks`
· `cargo machete` · `criterion` benches · GitHub Actions · `deploy/compose/docker-compose.yaml` (Postgres, later NATS).

Don't introduce alternatives (`async-trait`, `thiserror`, `anyhow`, Diesel/SeaORM, an
in-house DI container, runtime plugin loading…) without an ADR.

---

## 2. Workspace & crate layout (SRS §6–§8)

The root is a **Cargo virtual workspace** — no root `src/`. Members: `crates/*`, `examples/*`
(plus `tests/*` when the first system-test package lands). A one-off binary is an `examples/` target of the crate that
owns the API it calls, and a repeatable command lives in the workflow that runs it, spelled out in
`CONTRIBUTING.md` for humans. Shared version/edition/license/MSRV via
`[workspace.package]`; shared dependency versions via `[workspace.dependencies]`; shared lints via
`[workspace.lints]`. Examples set `publish = false`.

```
crates/<crate>/
├── Cargo.toml
├── src/            # production code only — no #[cfg(test)] modules
│   ├── lib.rs      # crate docs + re-exports; small modules by concept (dispatcher.rs, config.rs, error.rs)
│   └── …
├── migrations/     # provider crates only (0001_outbox.sql …)
├── tests/          # integration tests against the PUBLIC API
│   ├── <scenario>.rs
│   └── common/mod.rs
└── benches/        # optional criterion benches (or a package under /benches)
```

**Crate dependency rule (inward only):**

```
reliar-store-postgres ──┐
reliar-transport-nats ──┼──▶ reliar-outbox / -inbox / -idempotency ──▶ reliar-core
reliar-messaging ───────┘                                                           ▲
reliar-scheduler ──────────────────────────────────────────────────────────────────┘
```

Providers never depend on each other; abstraction crates depend only on `reliar-core`;
`reliar-core` depends on nothing Reliar-specific and nothing storage/transport-specific.
Create a crate **only when its implementation begins** (SRS §6).

## 3. Traits, dispatch & async (SRS §3, §19, §30, §34)

- **Small capability traits** (`OutboxStore`, `Publisher`, `Serializer`, `EnvelopeMapper`,
  `InboxStore`, …) with an associated `type Error`. Never a God trait.
- **Native async fns in traits** — declared as `fn f(&self, …) -> impl Future<Output = Result<…,
  Self::Error>> + Send;` so callers can spawn. **No `async-trait` crate.**
- **Static dispatch by default**: `OutboxDispatcher<S: OutboxStore, P: Publisher>`. `Arc<T>` is fine.
  `dyn` only at deliberate non-hot boundaries (diagnostics, plugin registries) and justified in an ADR.
- Bounds are explicit and minimal: `Send + Sync + 'static` where the type is spawned, `Clone` only
  when needed. Prefer `impl Trait` params over generics in leaf fns.
- Public structs that may grow are `#[non_exhaustive]` or built via a builder/`Config` with
  `Default`; construction never requires positional field lists.

## 4. Errors (SRS §23)

- **Hand-rolled, transport-free enums** per crate/module with manual `Display`,
  `std::error::Error` (`source()`), and targeted `From` impls. **No `thiserror`/`anyhow`** in public
  APIs (`anyhow` is acceptable in `examples/` only).
- Publication failures carry a **classification**: the dispatcher requires `P::Error` to expose
  `Transient | Permanent` (a small `FailureKind`/`Classify` trait in `reliar-outbox`); transient →
  retry with backoff, permanent or attempts exhausted → **dead** (`dead_at`, `last_error`).
- Error enums are `#[non_exhaustive]`; variants carry enough context to act, never payload bytes.

## 5. Envelope, metadata & headers (SRS §9–§17)

- `Envelope<T>` is the typed application object; `SerializedEnvelope = Envelope<Bytes>` is what
  storage/transport see. `Message` gives each type a **stable `TYPE` + `VERSION`** — never
  `type_name::<T>()`.
- **Typed `Metadata`** (correlation · trace · routing · delivery · tenant) is canonical; **`Headers`**
  is a validated newtype (not a bare `HashMap`) that **rejects the reserved `reliar-` prefix** and is
  lazily allocated (`Option<Headers>`).
- Never duplicate a metadata value into headers. Transport headers are produced only by an
  `EnvelopeMapper` at the wire boundary.
- `Envelope`, `OutboxRecord`, `InboxRecord` stay distinct types; execution state never lives on the Envelope.

## 6. Persistence — provider crates (SRS §20–§26, §35)

- **sqlx compile-time macros only** (`query!`, `query_as!`, `query_scalar!`); never the runtime string
  API, never `FromRow`. Commit the crate's **`.sqlx/`** (`cargo sqlx prepare`); CI runs `prepare --check`.
- Functions take `executor: impl PgExecutor<'_>`; the transactional write side (`enqueue`) takes the
  caller's `&mut Transaction<'_, Postgres>` (or `impl PgExecutor` — architect decides once).
- **Claim flow:** short tx `SELECT … FOR UPDATE SKIP LOCKED` + `UPDATE locked_by/locked_until` →
  commit → publish → new tx complete/fail. **Never** network I/O inside the claim tx.
- **DB-authoritative time:** `now()` for `locked_until`, `available_at`, expiry comparisons;
  `timestamptz` everywhere; app-side values are UTC. Completion/failure updates are guarded by
  `locked_by = $worker` so a stale worker cannot clobber a reclaimed row.
- **Batch** status updates (`= ANY($1)` / `UNNEST`). Indexes are designed from the actual claim and
  cleanup queries (a partial index on pending rows), never speculatively.
- **Migrations** live in `crates/reliar-store-<db>/migrations/` (source of truth, embedded) and are
  also published per release as a SQL artifact; they run only via the explicit `migrate(&pool)` API or
  the application's own pipeline. Forward-only; never edit an applied migration; lock-safe on large tables.
- **Identifiers:** unprefixed, unqualified names (`outbox`) in a **configurable schema via
  `search_path`, default `reliar`** — sqlx macros need static SQL, so the schema is resolved by the
  connection: the host puts `reliar` first in its DB URL `options` (or `ALTER ROLE … SET search_path`
  behind a pooler); Reliar sets it on its own pool and verifies at startup. The pooler scenario
  (**PgDog**, decision #31) is part of the integration suite; any transaction-mode pooler that
  drops startup options needs the `ALTER ROLE` path instead of URL `options`.
  Table names are not configurable. Every PK/FK/check is named `pk_`/`fk_`/`ck_`; **every index is `ix_`**.
  **PostgreSQL 18+**; ids are UUID v7 (`DEFAULT uuidv7()`, also generated client-side).
- **Settings:** every feature exposes a `*Settings` struct — `Default` + builder, `serde` behind a
  feature, opt-in `from_env(prefix)`; the library never reads env implicitly.

## 7. Dispatcher & worker model (SRS §21, §26, §32)

Configurable batch size · bounded Tokio concurrency (`Semaphore`/`JoinSet`) · exponential backoff
with jitter · lease duration · max attempts · retention · poll interval with idle backoff · optional
`LISTEN/NOTIFY` wake-up (polling remains the source of truth) · **graceful cancellation** via
`CancellationToken` (finish in-flight publishes, persist their outcome, then exit within a drain
timeout). `WorkerId` is unique per dispatcher instance.

## 8. Naming

Crates `reliar-<concept>` / `reliar-store-<db>` / `reliar-transport-<broker>`. Traits are
capabilities (`OutboxStore`, `Publisher`); implementors are `<Provider><Capability>`
(`PostgresOutboxStore`, `NatsPublisher`, `InMemoryPublisher`). Records are `*Record`; requests
`*Request`; configs `*Config`; errors `*Error`. Operation/span names `reliar.<crate>.<op>`
(`reliar.outbox.claim`). `find` = one row, `acquire` = claim a batch, `complete`/`fail` = outcomes,
`purge` = retention. Files snake_case; modules are nouns, fns are verbs.

## 9. Observability (SRS §33)

`tracing` spans with the predictable names above; **payloads and custom headers are never logged
by default**; high-cardinality ids go on spans, **never on metric labels**. Metrics through a small
hook trait with a no-op default (optional `metrics`-facade adapter behind a feature). No OTel
exporter dependency inside library crates — the application wires exporters.

## 10. Testing (SRS §8, §43)

- **No inline `#[cfg(test)]` in `src/`.** Every crate tests its **public API** from `tests/`;
  shared helpers in `tests/common/`. Doctests on public items are encouraged.
- **Abstraction crates** test with in-memory fakes (`InMemoryOutboxStore`, `InMemoryPublisher`,
  a failing publisher, a controllable clock) and `#[tokio::test(start_paused = true)]` for time.
- **Provider crates** test against a **real ephemeral Postgres** (testcontainers, or `DATABASE_URL`
  when set in CI) — migrate fresh, seed deterministically, tear down. Cover: insert-in-tx atomicity,
  concurrent acquire with `SKIP LOCKED`, lease expiry + recovery, crash-after-publish duplicate
  window, retry/backoff, dead state, retention purge.
- **Benches** live in `benches/` (criterion), never in production crates.
- Tests are deterministic: no wall-clock `sleep` waits, no shared mutable state between tests.
- **Containers never leak:** one testcontainer per test binary run, removed with its volumes when the
  run ends (Ryuk + explicit teardown; never a `static` that is never dropped); `docker ps -a` is
  clean after a local suite run (skill `testcontainers`).

## 10a. Layout hygiene

Infra under `deploy/docker/` and `deploy/compose/` (+ `configs/`, `secrets/`, `volumes/`). **All YAML
files use `.yaml`** — never `.yml`, with the single GitHub-mandated exception `.github/dependabot.yml`.
Root `Cargo.toml` declares explicit `dev`/`test`/`release`/`bench` profiles (see skill `ci-release`).
**GitHub Actions are referenced by their latest major version tag** (`actions/checkout@v7`), never by
commit SHA — decision #30 (2026-09-04) reversing ADR 0022; `dtolnay/rust-toolchain@stable`/`@master`
stay branch refs and `ossf/scorecard-action` takes its exact patch tag (no floating major exists).
Dependabot's `github-actions` ecosystem keeps the majors current; new majors must run on **Node 24**.

## 11. Public API & releases (SRS §7, §40, §44)

Every public item has rustdoc (`cargo doc -D warnings`, `#![warn(missing_docs)]`). Feature flags
are additive and documented in the crate README/`lib.rs`. SemVer is enforced with
`cargo semver-checks` from the first published version; pre-1.0 breaking changes are allowed but
recorded in `CHANGELOG.md` (Keep a Changelog). License `MIT` (single `LICENSE` file, `license = "MIT"`; the human chose MIT-only over the SRS v1.0 dual-license text). Release
via `release.yaml` only after `ci` + `test` + `security` pass.

## 12. Comments & code density

Comments cost tokens and rot. **Code shows *what*; a comment justifies *why*** (an invariant, a
constraint, a `PERF:`/`SAFETY:` note). `///` on public items is documentation and is required;
`//` inside a fn only for non-obvious *why*. No line-restating comments, no commented-out code, no
ownerless `// TODO`. Let names and types carry the meaning.

## 13. Quality gates (Definition of Done in `definition-of-done.md`)

`cargo fmt --all --check` · `cargo clippy --workspace --all-targets --all-features -- -D warnings` ·
`cargo test --workspace` (with Postgres available for provider crates) · `cargo hack check
--feature-powerset` (per crate with features) · `cargo sqlx prepare --check` · `cargo deny check` ·
`cargo audit` · `cargo machete` · `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps` ·
`cargo semver-checks` (once published).
