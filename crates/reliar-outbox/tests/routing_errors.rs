//! `ScopedOutboxPublisher`/`OutboxPublisher::publish_direct` map each collaborator's failure to
//! the matching error variant, and never retry a failed publish (§43.D, ADR 0033 Amendment D).
//! Nothing here serializes any more — there is no `RouteError::Serialize` case (Amendment D §3).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    Classify, DirectPublishError, FailureKind, InMemoryOutboxStore, InMemoryTransaction,
    OutboxPolicy, OutboxPublisher, OutboxSettings, PublishStep, RecordingPublisher, RouteError,
    ScriptedPublisher,
};

#[tokio::test]
async fn transaction_required_display_names_the_type_and_never_leaks_the_payload() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store, publisher, OutboxPolicy::default());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);
    let err = outbox
        .publish_direct(&serialized)
        .await
        .expect_err("a routed type has no transaction at this call site");

    let text = err.to_string();
    assert!(
        text.contains("orders.created"),
        "the message must name the type: {text}"
    );
    assert!(!text.contains("order_id"), "must never mention the payload");
}

#[tokio::test]
async fn stage_failure_surfaces_as_route_error_stage_with_source_wired() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_enqueue(1);
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), OutboxPolicy::default());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    let err = outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect_err("the store was armed to fail this stage");

    assert!(matches!(&err, RouteError::Stage(_)), "got {err:?}");
    assert!(
        std::error::Error::source(&err).is_some(),
        "RouteError::Stage must wire source()"
    );
    // Non-tautological: `InMemoryStoreError::Injected` (what `fail_next_enqueue` produces)
    // classifies `Transient`, so this fails if `Classify for RouteError` ever collapses to a
    // constant verdict instead of forwarding to the collaborator that actually failed.
    assert_eq!(err.kind(), FailureKind::Transient, "got {err:?}");
    let text = err.to_string();
    assert!(!text.contains("order_id"), "must never mention the payload");
    assert!(publisher.published().is_empty());
}

#[tokio::test]
async fn direct_publish_failure_surfaces_as_route_error_publish() {
    let store = InMemoryOutboxStore::default();
    let publisher = ScriptedPublisher::always(PublishStep::Permanent);
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let outbox = OutboxPublisher::new(store.clone(), publisher, policy);

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);
    let mut tx = InMemoryTransaction;
    let err = outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect_err("the transport was scripted to reject the publish");

    assert!(matches!(&err, RouteError::Publish(_)), "got {err:?}");
    // Non-tautological: `FakePublishError::Permanent` (what `ScriptedPublisher::always(Permanent)`
    // produces) classifies `Permanent`, so this fails if `Classify for RouteError` ever collapses
    // to a constant verdict instead of forwarding to the collaborator that actually failed.
    assert_eq!(err.kind(), FailureKind::Permanent, "got {err:?}");
    assert!(store.records().is_empty());
}

/// `publish_direct`'s direct failure surfaces through `DirectPublishError::Publish`.
#[tokio::test]
async fn publish_direct_failure_surfaces_as_direct_publish_error_publish() {
    let store = InMemoryOutboxStore::default();
    let publisher = ScriptedPublisher::always(PublishStep::Permanent);
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let outbox = OutboxPublisher::new(store, publisher, policy);

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);
    let err = outbox
        .publish_direct(&serialized)
        .await
        .expect_err("the transport was scripted to reject the publish");

    assert!(
        matches!(&err, DirectPublishError::Publish(_)),
        "got {err:?}"
    );
}

/// §43.D / R12: never retries — a publisher scripted to fail once then succeed is called exactly
/// once, and that call's failure is what the caller sees.
#[tokio::test]
async fn never_retries_a_failed_direct_publish() {
    let store = InMemoryOutboxStore::default();
    let publisher = ScriptedPublisher::new([PublishStep::Transient, PublishStep::Ok]);
    let policy = OutboxPolicy::from_settings(&OutboxSettings::default().enabled(false))
        .expect("valid settings");
    let outbox = OutboxPublisher::new(store, publisher.clone(), policy);

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);
    let result = outbox.publish_direct(&serialized).await;

    assert!(result.is_err(), "the first scripted outcome is a failure");
    assert_eq!(
        publisher.published().len(),
        1,
        "no retry — exactly one publish call"
    );
}
