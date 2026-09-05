//! `reliar-outbox` is the storage-agnostic transactional outbox: the [`OutboxStore`]/
//! [`OutboxDeadLetters`] capability traits (plus `reliar_core::Publisher`, re-exported here for
//! convenience), the request and result types that cross their boundary, a pure [`RetryPolicy`],
//! the feature's [`OutboxSettings`], and the [`OutboxMetrics`] hook (SRS §19–§26).
//!
//! [`OutboxPolicy`] and [`OutboxPublisher`] add the routing rule of SRS §20.2: one publish call
//! that either stages a message in the outbox or sends it straight to the transport, decided by
//! [`OutboxSettings::enabled`]/`allowed_types`/`disallowed_types` (ADR 0033, Amendment D). The
//! application-facing object **is** a [`Publisher`]: [`OutboxPublisher::in_transaction`] hands out
//! a [`ScopedOutboxPublisher`] that implements it for the life of a borrowed transaction.
//!
//! This slice ships the traits and types a provider builds against, a host configures, the
//! [`OutboxDispatcher`] worker loop, and the `test-support` fakes a test drives without a
//! database.
//!
//! Enable the `test-support` feature for `InMemoryOutboxStore`, `RecordingPublisher`,
//! `ScriptedPublisher` and `RecordingMetrics` — one shared set of fakes reused by provider
//! crates, examples and `tests/system` (SRS §8.1, §43.A.27).
// Plain code spans above, not intra-doc links: those types only exist when `test-support` is
// enabled, and `cargo doc` on default features must not break trying to resolve a link to an
// item that is not compiled in.
//!
//! # Guarantees
//!
//! - **Durable at-least-once publication. Never exactly-once.** Duplicate delivery is expected
//!   and must be handled by an idempotent consumer (SRS §22). Three distinct windows produce a
//!   duplicate, and all three are unavoidable:
//!   1. **The crash window** (§22): a publish reaches the broker, the worker crashes before
//!      `complete` persists, the lease expires, and another worker republishes the same message.
//!   2. **The slow-batch window** (§22.1): no crash at all — a worker claims a large batch under
//!      a lease shorter than the batch takes to drain, the lease expires while the worker is
//!      still healthily publishing, a second worker reclaims and republishes the tail, and the
//!      first worker's later `complete`/`fail` is rejected by the `locked_by` guard.
//!   3. **The drain window** (§26.1): on cancellation, `run()` drains in-flight publishes for at
//!      most `DispatcherSettings::drain_timeout`; a publish still unresolved at the timeout is
//!      released rather than awaited further, and its outcome — success or failure — is the same
//!      duplicate risk as the other two windows, just triggered by shutdown instead of a lease.
//! - **No ordering by default.** [`Ordering::Unordered`] (the default) guarantees **nothing**
//!   about order — not globally, not per `conversation_id`, not per aggregate, not
//!   approximately. `SKIP LOCKED`, concurrent publishing, per-message backoff and multiple
//!   workers each reorder freely (§22.2, ADR 0013). [`Ordering::PerKey`] is a configuration
//!   error in this release — see [`Ordering::validate`].
//! - **Pure retry.** [`RetryPolicy`] is I/O-free and clock-free: it returns a [`core::time::Duration`],
//!   never a timestamp. The store applies it as `available_at = now() + delay` in SQL, so a
//!   worker's clock skew can never hot-loop a row or park it in the future (ADR 0009).
//! - **The library never reads the environment implicitly.** Only [`OutboxSettings::from_env`]
//!   touches `std::env`, and only when called (ADR 0019).
//! - **The routing rule's direct path carries none of the above.** A message
//!   [`OutboxPolicy::decide`] routes [`RouteKind::Direct`] publishes with **no** Reliar-side
//!   guarantee at all: one attempt, no retry, no backoff, no dead state, no duplicate window —
//!   only as much retry as the transport publisher itself performs. Called inside an open
//!   transaction, it publishes even if that transaction later rolls back (SRS §20.2, ADR 0033).
//!   Documented in full on [`OutboxPublisher`].

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod dispatcher;
mod duration_serde;
mod error;
mod metrics;
mod ordering;
mod policy;
mod publisher;
mod record;
mod retry;
mod settings;
mod staging;
mod store;
#[cfg(feature = "test-support")]
mod test_support;
mod worker;

pub use dispatcher::{DefaultRetry, DispatchError, OutboxDispatcher, OutboxDispatcherBuilder};
pub use error::ConfigError;
pub use metrics::{NoopMetrics, OutboxMetrics};
pub use ordering::Ordering;
pub use policy::{OutboxPolicy, RouteKind};
pub use publisher::{DirectPublishError, OutboxPublisher, RouteError, ScopedOutboxPublisher};
pub use record::{OutboxRecord, OutboxRecordBuilder};
/// Re-exported from `reliar-core` (ADR 0032): a store author's or a publisher's `Classify`
/// bound, a publish/store failure's `FailureKind`, the `Publisher` capability trait, and the
/// shared `SettingsError` all live in core now. New code should name `reliar_core::` directly;
/// this re-export keeps existing `use reliar_outbox::{…}` imports one line.
pub use reliar_core::{Classify, FailureKind, Publisher, SettingsError};
pub use retry::{ExponentialBackoff, RetryPolicy};
pub use settings::{DispatcherSettings, MessageTypeNames, OutboxSettings, RetentionSettings};
pub use staging::OutboxStaging;
pub use store::{
    AcquireRequest, AcquiredBatch, CompletedMessage, DeadLetterPage, DeadQuery, DeadReason,
    FailedMessage, FailureOutcome, MessageRef, OutboxDeadLetters, OutboxStats, OutboxStore,
    PoisonedRow, PurgeReport, PurgeRequest,
};
#[cfg(feature = "test-support")]
pub use test_support::{
    FakePublishError, InMemoryOutboxStore, InMemoryStoreError, InMemoryTransaction, PublishStep,
    RecordingMetrics, RecordingPublisher, ScriptedPublisher,
};
pub use worker::WorkerId;
