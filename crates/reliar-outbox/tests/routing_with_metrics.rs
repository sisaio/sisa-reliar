//! `OutboxPublisher::with_metrics` calls [`reliar_outbox::OutboxMetrics::routed`] exactly once per
//! successful publish, with the route actually taken — on both the outbox and the direct path
//! (§43.D, ADR 0033 Amendment D; review round 1, M-3).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, MessageType, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, OutboxSettings,
    RecordingMetrics, RecordingPublisher, RouteKind,
};

#[tokio::test]
async fn scoped_publish_calls_the_hook_once_with_route_outbox() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let metrics = RecordingMetrics::default();
    let outbox =
        OutboxPublisher::with_metrics(store, publisher, OutboxPolicy::default(), metrics.clone());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let expected_type = envelope.message_type.clone();
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("publish succeeds");

    assert_eq!(metrics.routed(), vec![(RouteKind::Outbox, expected_type)]);
}

#[tokio::test]
async fn publish_direct_calls_the_hook_once_with_route_direct() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let metrics = RecordingMetrics::default();
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let outbox = OutboxPublisher::with_metrics(store, publisher, policy, metrics.clone());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let expected_type = envelope.message_type.clone();
    let serialized = common::serialize(envelope);
    outbox
        .publish_direct(&serialized)
        .await
        .expect("publish succeeds");

    assert_eq!(metrics.routed(), vec![(RouteKind::Direct, expected_type)]);
}

/// A failed publish never calls the hook — `routed` is recorded "only on success", so a publisher
/// scripted to fail must leave `RecordingMetrics::routed` empty.
#[tokio::test]
async fn a_failed_direct_publish_never_calls_the_hook() {
    let store = InMemoryOutboxStore::default();
    let publisher = reliar_outbox::ScriptedPublisher::always(reliar_outbox::PublishStep::Permanent);
    let metrics = RecordingMetrics::default();
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let outbox = OutboxPublisher::with_metrics(store, publisher, policy, metrics.clone());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);
    let err = outbox
        .publish_direct(&serialized)
        .await
        .expect_err("the transport was scripted to reject the publish");
    assert!(matches!(err, reliar_outbox::DirectPublishError::Publish(_)));

    assert!(metrics.routed().is_empty());
}

/// Sanity: the recorded message type is the envelope's own type, not a fixed placeholder — with
/// two distinct types both routed, each publish's hook call names the type that was actually
/// published.
#[tokio::test]
async fn the_recorded_message_type_matches_the_envelope_actually_published() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let metrics = RecordingMetrics::default();
    let outbox =
        OutboxPublisher::with_metrics(store, publisher, OutboxPolicy::default(), metrics.clone());

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    let mut tx = InMemoryTransaction;
    let scoped = outbox.in_transaction(&mut tx);
    scoped.publish(&a).await.expect("publish succeeds");
    scoped.publish(&b).await.expect("publish succeeds");

    assert_eq!(
        metrics.routed(),
        vec![
            (RouteKind::Outbox, MessageType::new("a", 1)),
            (RouteKind::Outbox, MessageType::new("b", 1)),
        ]
    );
}
