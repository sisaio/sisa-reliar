//! [`ScriptedPublisher`] replays a fixed script of outcomes: positional (cycling the last entry
//! once exhausted), keyed (order-independent, safe at any concurrency), or the same outcome for
//! every call. `Hang` resolves `Ok` only after the given duration — driving a `publish_timeout`
//! test elsewhere.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_core::MessageId;
use reliar_outbox::{Classify, FailureKind, PublishStep, Publisher, ScriptedPublisher};

#[test]
fn never_records_a_call_whose_future_is_never_polled() {
    let publisher = ScriptedPublisher::always(PublishStep::Ok);
    let envelope = common::serialized_envelope();

    let future = publisher.publish(&envelope);
    drop(future);

    assert!(
        publisher.published().is_empty(),
        "recorded on first poll, not when publish() was called"
    );
}

#[tokio::test]
async fn positional_script_cycles_its_last_entry_once_exhausted() {
    let publisher = ScriptedPublisher::new([
        PublishStep::Ok,
        PublishStep::Transient,
        PublishStep::Permanent,
    ]);
    let envelope = common::serialized_envelope();

    publisher
        .publish(&envelope)
        .await
        .expect("first step is Ok");

    let second = publisher
        .publish(&envelope)
        .await
        .expect_err("second step fails");
    assert_eq!(second.kind(), FailureKind::Transient);

    let third = publisher
        .publish(&envelope)
        .await
        .expect_err("third step fails");
    assert_eq!(third.kind(), FailureKind::Permanent);

    // Exhausted: the last entry (`Permanent`) repeats.
    let fourth = publisher
        .publish(&envelope)
        .await
        .expect_err("cycles the last step");
    assert_eq!(fourth.kind(), FailureKind::Permanent);

    assert_eq!(publisher.published(), vec![envelope.id; 4]);
}

#[tokio::test]
async fn keyed_script_is_order_independent_with_an_ok_fallback() {
    let mut ok_envelope = common::serialized_envelope();
    ok_envelope.id = MessageId::new();
    let mut permanent_envelope = common::serialized_envelope();
    permanent_envelope.id = MessageId::new();
    let unlisted_envelope = common::serialized_envelope();

    let publisher = ScriptedPublisher::keyed([
        (ok_envelope.id, PublishStep::Ok),
        (permanent_envelope.id, PublishStep::Permanent),
    ]);

    // Deliberately out of "declared" order.
    publisher
        .publish(&permanent_envelope)
        .await
        .expect_err("scripted permanent");
    publisher.publish(&ok_envelope).await.expect("scripted ok");
    publisher
        .publish(&unlisted_envelope)
        .await
        .expect("an unlisted id falls back to Ok");
}

#[tokio::test]
async fn always_applies_the_same_step_to_every_call() {
    let publisher = ScriptedPublisher::always(PublishStep::Transient);
    let mut a = common::serialized_envelope();
    a.id = MessageId::new();
    let mut b = common::serialized_envelope();
    b.id = MessageId::new();

    assert_eq!(
        publisher.publish(&a).await.expect_err("transient").kind(),
        FailureKind::Transient
    );
    assert_eq!(
        publisher.publish(&b).await.expect_err("transient").kind(),
        FailureKind::Transient
    );
}

#[tokio::test(start_paused = true)]
async fn hang_resolves_ok_only_after_its_duration() {
    let publisher = ScriptedPublisher::always(PublishStep::Hang(Duration::from_millis(200)));
    let envelope = common::serialized_envelope();

    let handle = tokio::spawn({
        let publisher = publisher.clone();
        let envelope = envelope.clone();
        async move { publisher.publish(&envelope).await }
    });

    tokio::time::advance(Duration::from_millis(199)).await;
    assert!(!handle.is_finished(), "not yet resolved");

    tokio::time::advance(Duration::from_millis(1)).await;
    handle
        .await
        .expect("task joins")
        .expect("resolves Ok once the hang elapses");
}
