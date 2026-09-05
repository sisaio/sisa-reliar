//! `ScopedOutboxPublisher`/`OutboxPublisher::publish_direct` emit exactly one
//! `reliar.outbox.route` span per call, carrying `route`; no header value ever appears in a span
//! field, event, or `Debug` output (§43.D, SRS §33 — mirrors
//! `dispatcher_never_logs_payload_or_header_values.rs`, §43.A.26).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPolicy, OutboxPublisher, RecordingPublisher,
};

const SECRET_HEADER_VALUE: &str = "RELIAR_OUTBOX_HEADER_VALUE_MUST_NEVER_APPEAR_IN_A_LOG";

#[tokio::test]
async fn one_route_span_carries_route_and_never_a_header_value() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store, publisher, OutboxPolicy::default());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 })
        .header("x-secret", SECRET_HEADER_VALUE)
        .expect("a non-reserved header key is accepted")
        .build();
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("publish succeeds");

    let text = recorder.text();
    // Positive assertions first: an empty or wrongly-wired transcript would make the negative
    // assertion below pass vacuously (mirrors S4 review, major 9).
    assert!(
        text.contains("reliar.outbox.route"),
        "sanity: the route span should appear:\n{text}"
    );
    assert_eq!(
        text.matches("reliar.outbox.route").count(),
        1,
        "exactly one route span per call:\n{text}"
    );
    assert!(
        text.contains("route=\"outbox\""),
        "the route field should record route=\"outbox\":\n{text}"
    );

    assert!(
        !text.contains(SECRET_HEADER_VALUE),
        "a header value leaked into a span field or log line:\n{text}"
    );
}

/// The direct path records `route="direct"` on the same span — proves the field is actually
/// written by `record("route", …)`, not merely satisfied by the span's own name (which never
/// changes) or by a substring match against the wrong route (mutation M-1, review round 1).
#[tokio::test]
async fn direct_route_records_route_equals_direct() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let policy =
        OutboxPolicy::from_settings(&reliar_outbox::OutboxSettings::default().enabled(false))
            .expect("valid settings");
    let outbox = OutboxPublisher::new(store, publisher, policy);

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 }).build();
    let serialized = common::serialize(envelope);
    outbox
        .publish_direct(&serialized)
        .await
        .expect("publish succeeds");

    let text = recorder.text();
    assert!(
        text.contains("route=\"direct\""),
        "the route field should record route=\"direct\":\n{text}"
    );
    assert!(
        !text.contains("route=\"outbox\""),
        "a direct publish must not also record route=\"outbox\":\n{text}"
    );
}

/// `publish_batch` (the inherited default) emits one span per envelope — a per-message outcome
/// needs a per-message span.
#[tokio::test]
async fn publish_batch_emits_one_span_per_envelope() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store, publisher, OutboxPolicy::default());

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());

    let mut tx = InMemoryTransaction;
    let results = outbox.in_transaction(&mut tx).publish_batch(&[a, b]).await;
    assert!(results.iter().all(Result::is_ok));

    let text = recorder.text();
    assert_eq!(
        text.matches("reliar.outbox.route").count(),
        2,
        "one span per envelope in the batch:\n{text}"
    );
}
