//! `OutboxPublisher::publish` returns the transport's error unwrapped, with its `Classify`
//! verdict preserved, and never retries a failed publish (ADR 0036 §3, contract §2.2, E9).
//! The proof is `publisher.published().len() == 1`: the script's second step is `Ok`, so if
//! `publish` retried even once, the call count would be 2 and the returned `Err` would never
//! surface at all (`start_paused` just keeps the test cheap; a paused clock proves nothing about
//! retries on its own — a synchronous retry needs no clock advance to run).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Classify, FailureKind, Publisher as _};
use reliar_outbox::{
    FakePublishError, InMemoryOutboxStore, OutboxPublisher, PublishStep, ScriptedPublisher,
};

#[tokio::test(start_paused = true)]
async fn publish_forwards_the_transports_error_unwrapped_and_never_retries() {
    let store = InMemoryOutboxStore::default();
    let publisher = ScriptedPublisher::new([PublishStep::Transient, PublishStep::Ok]);
    let outbox = OutboxPublisher::new(store, publisher.clone());

    let serialized = common::serialized_envelope();
    let err = outbox
        .publish(&serialized)
        .await
        .expect_err("the first scripted outcome is a failure");

    // Transparent: exactly the transport's own error type.
    assert!(matches!(err, FakePublishError::Transient { .. }));
    assert_eq!(err.kind(), FailureKind::Transient);
    assert_eq!(
        publisher.published().len(),
        1,
        "no retry — exactly one publish call, even though the script's second outcome is Ok"
    );
}
