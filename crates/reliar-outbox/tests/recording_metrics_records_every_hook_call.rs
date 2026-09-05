//! [`RecordingMetrics`] remembers every [`OutboxMetrics`] call for assertion (§43.A.25). Every
//! hook is invoked **through the trait** — exactly how a generic dispatcher holds it — because
//! `RecordingMetrics`'s own inherent getters intentionally share a name with the hook they
//! observe.
#![cfg(feature = "test-support")]

use std::time::Duration;

use reliar_core::MessageType;
use reliar_outbox::{DeadReason, FailureKind, OutboxMetrics, RecordingMetrics, RouteKind};

/// Drives every hook generically, exactly as `OutboxDispatcher` (or a
/// [`reliar_outbox::ScopedOutboxPublisher`]) will.
fn drive<M: OutboxMetrics>(metrics: &M, message_type: &MessageType) {
    metrics.claimed(3);
    metrics.published(2, message_type);
    metrics.retried(1, FailureKind::Transient);
    metrics.dead(1, DeadReason::AttemptsExhausted);
    metrics.publish_duration(Duration::from_millis(5), message_type);
    metrics.pending(7);
    metrics.expired_pending(2);
    metrics.oldest_pending_age(Duration::from_secs(9));
    metrics.purged(4, 1);
    metrics.routed(RouteKind::Outbox, message_type);
}

#[test]
fn every_hook_call_is_observable_through_its_getter() {
    let metrics = RecordingMetrics::default();
    let message_type = MessageType::new("orders.created", 1);

    drive(&metrics, &message_type);

    assert_eq!(metrics.claimed(), 3);
    assert_eq!(
        metrics.published(),
        vec![message_type.clone(), message_type.clone()]
    );
    assert_eq!(metrics.retried(), vec![FailureKind::Transient]);
    assert_eq!(metrics.dead(), vec![DeadReason::AttemptsExhausted]);
    assert_eq!(
        metrics.publish_duration(),
        Some((Duration::from_millis(5), message_type.clone()))
    );
    assert_eq!(metrics.pending(), Some(7));
    assert_eq!(metrics.expired_pending(), Some(2));
    assert_eq!(metrics.oldest_pending_age(), Some(Duration::from_secs(9)));
    assert_eq!(metrics.purged(), Some((4, 1)));
    assert_eq!(metrics.routed(), vec![(RouteKind::Outbox, message_type)]);
}

#[test]
fn getters_start_empty_before_any_call() {
    let metrics = RecordingMetrics::default();

    assert_eq!(metrics.claimed(), 0);
    assert!(metrics.published().is_empty());
    assert!(metrics.pending().is_none());
    assert!(
        metrics.oldest_pending_age().is_none(),
        "no data yet is distinct from a reported zero"
    );
    assert!(metrics.purged().is_none());
    assert!(metrics.publish_duration().is_none());
    assert!(metrics.routed().is_empty());
}
