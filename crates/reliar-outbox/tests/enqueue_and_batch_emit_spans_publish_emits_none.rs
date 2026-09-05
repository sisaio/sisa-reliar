//! `enqueue` runs under `reliar.outbox.enqueue` (`message.id`, `message.type`); `enqueue_batch`
//! runs under `reliar.outbox.enqueue_batch` (`batch.size`) wrapping one nested `enqueue` span per
//! envelope; `publish`/`publish_batch` open **no** span at all — and none of it ever contains a
//! payload byte or a header value (ADR 0036 §6, contract §6, E14).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryTransaction, OutboxPublisher, RecordingPublisher,
};

const SECRET_PAYLOAD_MARKER: &str = "sk_live_RELIAR_PAYLOAD_MUST_NEVER_APPEAR_IN_A_LOG";
const SECRET_HEADER_VALUE: &str = "RELIAR_HEADER_VALUE_MUST_NEVER_APPEAR_IN_A_LOG";

#[tokio::test]
async fn enqueue_and_enqueue_batch_emit_the_documented_spans_with_no_leak() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store, publisher);

    let secret_body = bytes::Bytes::from(format!("{{\"secret\":\"{SECRET_PAYLOAD_MARKER}\"}}"));

    let single = Envelope::builder(common::OrderCreated { order_id: 1 })
        .header("x-secret", SECRET_HEADER_VALUE)
        .expect("a non-reserved header key is accepted")
        .build()
        .map_body(|_| secret_body);
    let single_id = single.id;
    let mut tx = InMemoryTransaction;
    outbox
        .enqueue(&mut tx, &single)
        .await
        .expect("enqueue succeeds");

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    let (a_id, b_id) = (a.id, b.id);
    outbox
        .enqueue_batch(&mut tx, &[a, b])
        .await
        .expect("batch enqueues");

    let text = recorder.text();
    // Not just "the string `reliar.outbox.enqueue` appears somewhere" — that would also be
    // satisfied by the `enqueue_batch` span alone, since `tracing_subscriber`'s fmt layer renders
    // `reliar.outbox.enqueue_batch{..}:reliar.outbox.enqueue{..}` for the nested span, and
    // `"reliar.outbox.enqueue"` is a substring of that. Count the span-open marker
    // `reliar.outbox.enqueue{` directly: one for the single call, two more nested inside the
    // batch (review round 1, M2).
    let enqueue_span_count = text.matches("reliar.outbox.enqueue{").count();
    assert_eq!(
        enqueue_span_count, 3,
        "sanity: one enqueue span for the single call, one per batch envelope (3 total):\n{text}"
    );
    assert!(
        text.contains("reliar.outbox.enqueue_batch"),
        "sanity: the batch span must appear:\n{text}"
    );
    assert!(
        text.contains("batch.size=2"),
        "sanity: the batch span must record batch.size=2:\n{text}"
    );
    assert!(
        text.contains(&single_id.to_string()),
        "sanity: message.id must be recorded on the enqueue span:\n{text}"
    );
    assert!(
        text.contains(&a_id.to_string()) && text.contains(&b_id.to_string()),
        "sanity: both batch envelope ids must be recorded on their own nested enqueue span:\n{text}"
    );
    assert!(
        text.contains("orders.created"),
        "sanity: message.type must be recorded on the enqueue span:\n{text}"
    );
    assert!(
        !text.contains(SECRET_PAYLOAD_MARKER),
        "payload leaked:\n{text}"
    );
    assert!(
        !text.contains(SECRET_HEADER_VALUE),
        "header value leaked:\n{text}"
    );
}

#[tokio::test]
async fn publish_and_publish_batch_emit_no_outbox_span_at_all() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store, publisher);

    let a = common::serialize(Envelope::builder(common::TypeA).build());
    let b = common::serialize(Envelope::builder(common::TypeB).build());
    outbox.publish(&a).await.expect("publish succeeds");
    outbox.publish_batch(&[b]).await;

    let text = recorder.text();
    assert!(
        !text.contains("reliar.outbox"),
        "publish/publish_batch must open no reliar.outbox span at all:\n{text}"
    );
}
