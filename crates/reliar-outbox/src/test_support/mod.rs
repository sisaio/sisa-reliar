//! In-memory fakes for `reliar-outbox`'s public API, shipped behind the `test-support` feature
//! so provider crates, examples and `tests/system` reuse one set instead of each hand-rolling its
//! own (SRS §8.1, §43.A.27).
//!
//! [`InMemoryOutboxStore`] keeps the same lease/attempt/dead-letter semantics
//! `reliar-store-postgres` implements in SQL, driven by an injectable clock
//! ([`InMemoryOutboxStore::advance`]) rather than wall-clock sleeps. [`RecordingPublisher`] and
//! [`ScriptedPublisher`] stand in for a transport; [`RecordingMetrics`] stands in for an
//! [`crate::OutboxMetrics`] exporter.
//!
//! **None of these fakes ever holds a [`std::sync::MutexGuard`] across an `.await`**: every
//! method locks, mutates or copies out what it needs, drops the guard, and only *then* returns a
//! future (or awaits inside one), so the futures stay `Send` and safe to `tokio::spawn`.

mod metrics;
mod publisher;
mod store;

pub use metrics::RecordingMetrics;
pub use publisher::{FakePublishError, PublishStep, RecordingPublisher, ScriptedPublisher};
pub use store::{InMemoryOutboxStore, InMemoryStoreError, InMemoryTransaction};
