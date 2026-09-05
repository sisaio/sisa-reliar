//! Static-dispatch metrics hook (SRS §33.1, ADR 0020).

use std::time::Duration;

use reliar_core::{FailureKind, MessageType};

use crate::store::DeadReason;

/// A metrics hook with no-op defaults, so it costs nothing unused and adding an instrument
/// later is not a breaking change. No library crate depends on a metrics exporter — the host
/// wires one behind an implementation of this trait.
///
/// Labels are bounded to `message_type`, `kind`, `reason` and similar low-cardinality values.
/// `message_id`, `correlation_id`, `tenant_id`, `worker_id` and `last_error` **SHALL NEVER** be
/// metric labels (ADR 0020).
pub trait OutboxMetrics: Send + Sync {
    /// Called once per `acquire` with the number of rows claimed (which may be zero).
    fn claimed(&self, _n: usize) {}
    /// Called once per publish outcome that succeeded.
    fn published(&self, _n: usize, _message_type: &MessageType) {}
    /// Called once per publish outcome resolved as [`crate::FailureOutcome::Retry`].
    fn retried(&self, _n: usize, _kind: FailureKind) {}
    /// Called once per row moved to dead.
    fn dead(&self, _n: usize, _reason: DeadReason) {}
    /// Called once per publish attempt with its wall-clock duration.
    fn publish_duration(&self, _d: Duration, _message_type: &MessageType) {}
    /// The claimable backlog, from the last [`crate::OutboxStore::stats`] poll.
    fn pending(&self, _n: u64) {}
    /// Pending rows past `expires_at` awaiting the next purge — counted separately so they can
    /// be alerted on without pinning [`Self::oldest_pending_age`] (§43.B).
    fn expired_pending(&self, _n: u64) {}
    /// The outbox-lag alerting signal: age of the oldest claimable row, from
    /// [`crate::OutboxStats::lag`]. **Not called when the backlog is empty** — there is no
    /// oldest pending row to report an age for, and calling this with a made-up zero would read
    /// as "no lag" rather than "no data."
    fn oldest_pending_age(&self, _age: Duration) {}
    /// Rows deleted by the last purge pass.
    fn purged(&self, _published: u64, _dead: u64) {}
}

/// The default [`OutboxMetrics`]: every hook is a no-op.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoopMetrics;

impl OutboxMetrics for NoopMetrics {}
