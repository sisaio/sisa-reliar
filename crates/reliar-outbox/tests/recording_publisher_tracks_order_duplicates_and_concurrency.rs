//! [`RecordingPublisher`] never fails, records every publish in call order (duplicates
//! included — that is the assertion a crash-after-publish or a reclaimed lease needs, SRS §22),
//! and its `in_flight_peak` proves how many publishes genuinely overlapped.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_core::MessageId;
use reliar_outbox::{Publisher, RecordingPublisher};

#[test]
fn default_publisher_never_touches_a_timer() {
    // A bare current-thread runtime with **no time driver** — if `RecordingPublisher::default`
    // ever awaited `tokio::time::sleep` unconditionally (M3), this would panic ("there is no
    // timer running") instead of completing.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a minimal runtime builds");
    let publisher = RecordingPublisher::default();
    let envelope = common::serialized_envelope();

    runtime.block_on(async {
        publisher.publish(&envelope).await.expect("never fails");
    });

    assert_eq!(publisher.published(), vec![envelope.id]);
}

#[test]
fn never_records_a_call_whose_future_is_never_polled() {
    let publisher = RecordingPublisher::default();
    let envelope = common::serialized_envelope();

    // Constructing the future is not calling it: `async move` bodies are lazy, so nothing runs
    // until something polls this future — which nothing here ever does.
    let future = publisher.publish(&envelope);
    drop(future);

    assert!(
        publisher.published().is_empty(),
        "recorded on first poll, not when publish() was called"
    );
    assert!(publisher.envelopes().is_empty());
}

#[tokio::test(start_paused = true)]
async fn sequential_publishes_are_recorded_in_call_order() {
    let publisher = RecordingPublisher::default();
    let envelope = common::serialized_envelope();

    publisher.publish(&envelope).await.expect("never fails");
    publisher.publish(&envelope).await.expect("never fails");

    assert_eq!(publisher.published(), vec![envelope.id, envelope.id]);
    assert_eq!(
        publisher.count(envelope.id),
        2,
        "proves the duplicate window"
    );
    assert_eq!(publisher.envelopes().len(), 2);
    assert_eq!(publisher.in_flight_peak(), 1, "never overlapped");
}

#[tokio::test(start_paused = true)]
async fn concurrent_publishes_raise_the_in_flight_peak() {
    let publisher = RecordingPublisher::with_concurrency_probe(Duration::from_millis(1));
    let envelopes: Vec<_> = (0..4)
        .map(|_| {
            let mut envelope = common::serialized_envelope();
            envelope.id = MessageId::new();
            envelope
        })
        .collect();

    let mut set = tokio::task::JoinSet::new();
    for envelope in envelopes.clone() {
        let publisher = publisher.clone();
        set.spawn(async move { publisher.publish(&envelope).await });
    }
    while let Some(result) = set.join_next().await {
        result.expect("task joins").expect("publish never fails");
    }

    assert_eq!(publisher.in_flight_peak(), 4, "all four ran concurrently");
    assert_eq!(publisher.published().len(), 4);
    for envelope in &envelopes {
        assert_eq!(publisher.count(envelope.id), 1);
    }
}
