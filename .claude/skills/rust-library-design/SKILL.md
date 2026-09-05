---
name: rust-library-design
description: >-
  The house pattern for Reliar's Rust library crates - a Cargo virtual workspace (crates/*,
  examples/*) with workspace-level package/deps/lints, the inward crate dependency rule
  (reliar-core pure; abstraction crates depend only on core; providers never on each other), small
  capability traits with associated Error types and native async fns in traits returning
  `impl Future + Send` (no async-trait), generics + static dispatch (OutboxDispatcher<S, P>, no
  Box<dyn> on hot paths), hand-rolled non_exhaustive error enums with source() (no thiserror/anyhow),
  the Envelope<T>/SerializedEnvelope/Message TYPE+VERSION/typed Metadata/validated Headers model,
  Config+builder construction, bounded concurrency + backoff + CancellationToken worker loops,
  rustdoc/feature-flag/semver hygiene. Use when creating or reviewing any crate, trait, type,
  dispatcher/worker loop, error enum, or public API.
metadata:
  audience: ARCHITECT, ENGINEER, REVIEWER
---

# Rust library design (Reliar house pattern)

Reliar is a **library**: its API is its public Rust surface, consumed by applications that compose
it explicitly at startup (SRS §3.12 — no DI container). Every decision below serves three goals:
**honest guarantees**, **zero-cost abstraction** (monomorphized), and **an API we can keep stable**.

## Workspace (root `Cargo.toml`, virtual — no root `src/`)

```toml
[workspace]
resolver = "3"
members = ["crates/*", "examples/*"]

[workspace.package]
edition = "2024"
rust-version = "1.85"              # MSRV — bump deliberately, note in CHANGELOG
license = "MIT"
repository = "https://github.com/sisaio/sisa-reliar"

[workspace.dependencies]           # one version per dep, crates use `dep.workspace = true`
tokio = { version = "1", default-features = false, features = ["rt", "sync", "time", "macros"] }
tokio-util = { version = "0.7", default-features = false }
bytes = "1"
uuid = { version = "1", features = ["v7"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tracing = "0.1"
reliar-core = { path = "crates/reliar-core", version = "0.1.0" }

[workspace.lints.rust]
unsafe_code = "forbid"
missing_docs = "warn"
[workspace.lints.clippy]
all = "warn"
pedantic = "warn"          # allow individual pedantic lints per crate with a reason
```

Each crate: `[lints] workspace = true`, `[package] … .workspace = true` for shared fields; examples
and tools add `publish = false`. **Create a crate only when its implementation begins** (SRS §6).

## Crate dependency rule

```
reliar-core        ← nothing Reliar-specific; no sqlx/postgres/nats/kafka/redis; no transport routing concepts
                     owns the shared vocabulary: Envelope, Metadata, Headers, Serializer,
                     EnvelopeMapper, Publisher, Classify/FailureKind, SettingsError (ADR 0032)
reliar-outbox      ← reliar-core            (OutboxStore, dispatcher, retry policy, outbox settings)
reliar-inbox       ← reliar-core   (reliar-idempotency likewise; caching is out of scope — decision #29)
reliar-store-postgres ← reliar-outbox, reliar-core       (implements OutboxStore; never another provider)
reliar-transport-nats ← reliar-core ONLY                 (implements Publisher + EnvelopeMapper)
```

A provider depends on `reliar-core` directly, and on an abstraction crate **only when it implements
a trait that crate owns** (ADR 0032). Core is not a catch-all: an item goes there when it names no
storage engine, broker or routing concept **and** more than one capability needs it to talk to
another — never when it encodes *how* a capability works (`OutboxStore`, `RetryPolicy`,
`SubjectResolver` all stay out).

Enforce it in CI: `cargo tree -p reliar-core -e normal | grep -E 'sqlx|postgres|async-nats'` must be empty.

## Traits — small capabilities, associated Error, native async

```rust
/// Storage side of the outbox. One provider implements this per database.
pub trait OutboxStore: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Claim up to `request.batch_size` due, unlocked records for `request.worker`.
    /// Must commit before returning; the caller publishes outside any transaction.
    fn acquire(&self, request: AcquireRequest)
        -> impl Future<Output = Result<Vec<OutboxRecord>, Self::Error>> + Send;

    fn complete(&self, worker: &WorkerId, items: &[CompletedMessage])
        -> impl Future<Output = Result<(), Self::Error>> + Send;

    fn fail(&self, worker: &WorkerId, items: &[FailedMessage])
        -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

- Declare `-> impl Future<Output = …> + Send` in the **trait** (not `async fn`) so callers can spawn;
  implementors may write plain `async fn` — the compiler checks the `Send` bound.
- No `#[async_trait]`, ever. No God trait (SRS §34); a provider implements several small traits.
- Bounds: put `Send + Sync` on the trait only if every use needs it; `'static` only on spawned types.
- `type Error` is the provider's own hand-rolled enum. Cross-provider code needs only `std::error::Error`.

## Static dispatch & composition

```rust
pub struct OutboxDispatcher<S, P, M = NoopMetrics> { store: S, publisher: P, metrics: M, config: DispatcherConfig }

impl<S, P, M> OutboxDispatcher<S, P, M>
where S: OutboxStore + Clone + 'static, P: Publisher + Clone + 'static, M: OutboxMetrics + Clone + 'static,
      P::Error: Classify,                     // transient vs permanent (SRS §23)
{
    pub fn new(store: S, publisher: P, config: DispatcherConfig) -> Self { … }
    pub async fn run(self, cancel: CancellationToken) -> Result<(), DispatchError<S::Error>> { … }
}
```

Application code composes concretely: `OutboxDispatcher::new(PostgresOutboxStore::new(pool), NatsPublisher::new(js), cfg)`.
`Arc<T>` is fine (`Arc<PostgresOutboxStore>` implements the trait via a blanket `impl<T: OutboxStore> OutboxStore for Arc<T>`).
`Box<dyn …>` only for deliberate cold boundaries, with an ADR.

## Errors — hand-rolled, transport-free, classified

```rust
#[derive(Debug)]
#[non_exhaustive]
pub enum PublishError {
    Transient { source: Box<dyn std::error::Error + Send + Sync> },   // boxing a *source* is fine
    Permanent { reason: String },
}
impl std::fmt::Display for PublishError { … }                            // never prints payload bytes
impl std::error::Error for PublishError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> { match self { Self::Transient { source } => Some(source.as_ref()), _ => None } }
}

/// In `reliar-core` (ADR 0032). Implemented by every `Publisher::Error` **and every
/// `OutboxStore::Error`**, so the dispatcher can decide retry vs dead without a downcast.
pub trait Classify { fn kind(&self) -> FailureKind; }
#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum FailureKind { Transient, Permanent }
```

No `thiserror`/`anyhow` in library crates (`anyhow` ok in `examples/`). Errors carry context,
never payloads. `#[non_exhaustive]` on every public enum/struct that may grow.

## The envelope model (SRS §9–§17)

```rust
pub trait Message: serde::Serialize + serde::de::DeserializeOwned { const TYPE: &'static str; const VERSION: u16; }

pub struct Envelope<T> { pub id: MessageId, pub message_type: MessageType, pub body: T, pub metadata: Metadata, headers: Option<Headers> }
pub type SerializedEnvelope = Envelope<bytes::Bytes>;

/// Validated custom headers: rejects the reserved `reliar-` prefix (case-insensitive), lazily allocated.
pub struct Headers(HashMap<String, String>);
impl Headers { pub fn insert(&mut self, k: impl Into<String>, v: impl Into<String>) -> Result<(), HeaderError> { … } }

pub trait Serializer { type Error; fn content_type(&self) -> &ContentType;
    fn serialize<T: Message>(&self, body: &T) -> Result<bytes::Bytes, Self::Error>;
    fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error>; }
```

`MessageType` renders `orders.created.v1` from `TYPE` + `VERSION` — never `type_name::<T>()`.
Metadata is typed and canonical; nothing is duplicated into `Headers`; `EnvelopeMapper` produces
transport headers only at the wire (SRS §15–§16). `Envelope != OutboxRecord != InboxRecord`.

## Worker loop — bounded, cancellable, DB time

```rust
loop {
    let batch = tokio::select! { biased;
        _ = cancel.cancelled() => break,
        r = store.acquire(AcquireRequest { worker: id.clone(), batch_size, lease }) => r?,
    };
    if batch.is_empty() { idle.sleep_with_backoff(&cancel).await; continue; }   // low idle DB load
    let mut set = tokio::task::JoinSet::new();
    for rec in batch { let permit = sem.clone().acquire_owned().await?; let p = publisher.clone();
        set.spawn(async move { let r = p.publish(&rec.envelope).await; drop(permit); (rec, r) }); }
    let (done, failed) = partition(set.join_all().await, &policy);         // classify + backoff per item
    store.complete(&id, &done).await?; store.fail(&id, &failed).await?;    // batches, in a fresh tx
}
```

Rules: publish **after** `acquire` returned (claim tx already committed); on cancel, finish in-flight
publishes and persist their outcomes within a drain timeout; backoff = `min(max, base·2^attempts)`
with jitter; **lease/due comparisons happen in SQL with `now()`** — the app clock is only for
`sent_at`, jitter, and idle sleeps.

## Public-API hygiene

- `#![forbid(unsafe_code)]`, `#![warn(missing_docs)]`; `///` on every public item with the
  guarantee it upholds (e.g. "at-least-once; duplicates possible after a crash between publish and complete").
- Construction via `*Settings`/`*Config` structs with `Default` + builder methods, `serde` derive behind
  a `serde` feature, and an **opt-in** `from_env(prefix)` (e.g. `OutboxSettings::from_env("RELIAR_OUTBOX_")`)
  — the library never reads the environment implicitly. No positional public fields on types that may
  grow (`#[non_exhaustive]`).
- Feature flags are additive (`json` default serializer, `metrics`, `listen-notify`); `cargo hack
  --feature-powerset` keeps every combination compiling.
- Re-export the public surface from `lib.rs`; keep internals `pub(crate)`. The `reliar` facade
  crate (later) re-exports commonly used items and forwards features.
- `#[must_use]` on builders and on futures-returning constructors; `Debug` on everything public
  (custom `Debug` for payload-bearing types that elides bytes).

## Definition of done (a crate/trait/API change)

- [ ] Dependency rule holds (`cargo tree` check); `reliar-core` unchanged unless the ADR says so.
- [ ] Traits: associated `Error`, `impl Future + Send`, no `async-trait`, no `dyn` on hot paths.
- [ ] Errors hand-rolled, `#[non_exhaustive]`, `source()` wired, classification available where the dispatcher needs it.
- [ ] Rustdoc states the guarantee; `cargo doc -D warnings` clean; features additive and powerset-checked.
- [ ] Worker code bounded, cancellable, DB-time-based; no I/O inside a claim transaction.
