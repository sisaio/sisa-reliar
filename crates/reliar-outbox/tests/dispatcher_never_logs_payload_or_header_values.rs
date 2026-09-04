//! No span field, log line, or error `Display` on the claim/publish/fail path contains payload
//! bytes or header values (§43.A.26, SRS §33).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DeadReason, InMemoryOutboxStore, OutboxDispatcher, PublishStep, ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

/// A payload string and a header value distinctive enough that any leak would be obvious in the
/// captured transcript, and unlikely to appear by coincidence in framework text.
const SECRET_PAYLOAD_MARKER: &str = "sk_live_RELIAR_PAYLOAD_MUST_NEVER_APPEAR_IN_A_LOG";
const SECRET_HEADER_VALUE: &str = "RELIAR_HEADER_VALUE_MUST_NEVER_APPEAR_IN_A_LOG";

#[tokio::test(start_paused = true)]
async fn no_span_or_log_line_contains_the_payload_or_a_header_value() {
    let (recorder, _guard) = common::RecordingSubscriber::install();

    let store = InMemoryOutboxStore::default();

    let mut ok_envelope = common::serialized_envelope();
    ok_envelope.body = bytes::Bytes::from(format!("{{\"secret\":\"{SECRET_PAYLOAD_MARKER}\"}}"));
    ok_envelope
        .headers_mut()
        .insert("x-secret", SECRET_HEADER_VALUE)
        .expect("a non-reserved header key is accepted");
    let ok_row = store.insert(ok_envelope);

    let mut dead_envelope = common::distinct_envelope();
    dead_envelope.body = bytes::Bytes::from(format!("{{\"secret\":\"{SECRET_PAYLOAD_MARKER}\"}}"));
    let dead_row = store.insert(dead_envelope);

    let publisher = ScriptedPublisher::keyed([
        (ok_row.id, PublishStep::Ok),
        (dead_row.id, PublishStep::Permanent),
    ]);

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));
    common::advance_and_settle(Duration::from_millis(30)).await;
    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");

    // Sanity: the scenario actually exercised the dead path this assertion depends on.
    assert_eq!(
        store.record(dead_row.id).and_then(|r| r.dead_reason),
        Some(DeadReason::PermanentError)
    );

    let text = recorder.text();
    // Positive assertions first: an empty or wrongly-wired transcript would make the negative
    // assertions below pass vacuously (S4 review, major 9).
    assert!(
        text.contains("reliar.outbox"),
        "sanity: the transcript should contain reliar.outbox spans/events:\n{text}"
    );
    assert!(
        text.contains("reliar.outbox.dead"),
        "sanity: the dead event should have been logged:\n{text}"
    );

    assert!(
        !text.contains(SECRET_PAYLOAD_MARKER),
        "payload bytes leaked into a span field or log line:\n{text}"
    );
    assert!(
        !text.contains(SECRET_HEADER_VALUE),
        "a header value leaked into a span field or log line:\n{text}"
    );
}
