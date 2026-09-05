//! [`RecordingMetrics`]: an [`OutboxMetrics`] fake that remembers every call (§43.A.25).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Duration;

use reliar_core::{FailureKind, MessageType};

use crate::metrics::OutboxMetrics;
use crate::policy::RouteKind;
use crate::store::DeadReason;

/// Records every [`OutboxMetrics`] call for assertion. Each getter below has the same name as
/// the hook it observes but a different arity — an inherent method always shadows a trait method
/// of the same name for a concrete `RecordingMetrics` value, so `metrics.claimed()` (the getter)
/// and `OutboxMetrics::claimed(&metrics, n)` (the hook, reached through the trait bound a
/// generic dispatcher holds it behind) never collide in practice.
#[derive(Clone, Debug, Default)]
pub struct RecordingMetrics {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug, Default)]
struct Inner {
    claimed: usize,
    published: Vec<MessageType>,
    retried: Vec<FailureKind>,
    dead: Vec<DeadReason>,
    pending: Option<u64>,
    expired_pending: Option<u64>,
    oldest_pending_age: Option<Duration>,
    purged: Option<(u64, u64)>,
    publish_duration: Option<(Duration, MessageType)>,
    routed: Vec<(RouteKind, MessageType)>,
}

impl RecordingMetrics {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The total claimed across every [`OutboxMetrics::claimed`] call.
    #[must_use]
    pub fn claimed(&self) -> usize {
        self.lock().claimed
    }

    /// One entry per published message, in call order.
    #[must_use]
    pub fn published(&self) -> Vec<MessageType> {
        self.lock().published.clone()
    }

    /// One entry per retried message, in call order.
    #[must_use]
    pub fn retried(&self) -> Vec<FailureKind> {
        self.lock().retried.clone()
    }

    /// One entry per message moved to dead, in call order.
    #[must_use]
    pub fn dead(&self) -> Vec<DeadReason> {
        self.lock().dead.clone()
    }

    /// The last observed pending count, if [`OutboxMetrics::pending`] was ever called.
    #[must_use]
    pub fn pending(&self) -> Option<u64> {
        self.lock().pending
    }

    /// The last observed expired-pending count, if [`OutboxMetrics::expired_pending`] was ever
    /// called.
    #[must_use]
    pub fn expired_pending(&self) -> Option<u64> {
        self.lock().expired_pending
    }

    /// The last observed outbox lag, if [`OutboxMetrics::oldest_pending_age`] was ever called.
    /// `None` also when the dispatcher correctly skipped the call on an empty backlog.
    #[must_use]
    pub fn oldest_pending_age(&self) -> Option<Duration> {
        self.lock().oldest_pending_age
    }

    /// The last `(published, dead)` pair observed via [`OutboxMetrics::purged`], if it was ever
    /// called.
    #[must_use]
    pub fn purged(&self) -> Option<(u64, u64)> {
        self.lock().purged
    }

    /// The last `(duration, message_type)` pair observed via
    /// [`OutboxMetrics::publish_duration`], if it was ever called.
    #[must_use]
    pub fn publish_duration(&self) -> Option<(Duration, MessageType)> {
        self.lock().publish_duration.clone()
    }

    /// One `(route, message_type)` entry per [`OutboxMetrics::routed`] call, in call order.
    #[must_use]
    pub fn routed(&self) -> Vec<(RouteKind, MessageType)> {
        self.lock().routed.clone()
    }
}

impl OutboxMetrics for RecordingMetrics {
    fn claimed(&self, n: usize) {
        self.lock().claimed += n;
    }

    fn published(&self, n: usize, message_type: &MessageType) {
        let mut guard = self.lock();
        guard
            .published
            .extend(std::iter::repeat_n(message_type.clone(), n));
    }

    fn retried(&self, n: usize, kind: FailureKind) {
        let mut guard = self.lock();
        guard.retried.extend(std::iter::repeat_n(kind, n));
    }

    fn dead(&self, n: usize, reason: DeadReason) {
        let mut guard = self.lock();
        guard.dead.extend(std::iter::repeat_n(reason, n));
    }

    fn publish_duration(&self, d: Duration, message_type: &MessageType) {
        self.lock().publish_duration = Some((d, message_type.clone()));
    }

    fn pending(&self, n: u64) {
        self.lock().pending = Some(n);
    }

    fn expired_pending(&self, n: u64) {
        self.lock().expired_pending = Some(n);
    }

    fn oldest_pending_age(&self, age: Duration) {
        self.lock().oldest_pending_age = Some(age);
    }

    fn purged(&self, published: u64, dead: u64) {
        self.lock().purged = Some((published, dead));
    }

    fn routed(&self, route: RouteKind, message_type: &MessageType) {
        self.lock().routed.push((route, message_type.clone()));
    }
}
