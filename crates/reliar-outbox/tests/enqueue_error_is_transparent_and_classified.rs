//! `OutboxPublisher::enqueue` returns the enqueue error unwrapped — `source()` wired, `Classify`
//! preserved, `Display` never leaking a payload or header value (ADR 0036 §3, contract §2.1, E8).

#![cfg(feature = "test-support")]

mod common;

use reliar_core::{Classify, Envelope, FailureKind};
use reliar_outbox::{
    InMemoryOutboxStore, InMemoryStoreError, InMemoryTransaction, OutboxPublisher,
    RecordingPublisher,
};

#[tokio::test]
async fn enqueue_forwards_the_enqueue_error_unwrapped() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_enqueue(1);
    let publisher = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store, publisher.clone());

    let envelope = Envelope::builder(common::OrderCreated { order_id: 1 })
        .header("x-secret", "SUPER_SECRET_HEADER_VALUE")
        .expect("a non-reserved header key is accepted")
        .build();
    let serialized = common::serialize(envelope);

    let mut tx = InMemoryTransaction;
    let err = outbox
        .enqueue(&mut tx, &serialized)
        .await
        .expect_err("the store was armed to fail this enqueue");

    // Transparent: exactly the provider's own error type, not a Reliar-side wrapper.
    assert!(matches!(err, InMemoryStoreError::Injected));
    assert!(
        std::error::Error::source(&err).is_none(),
        "InMemoryStoreError::Injected itself has no further source, but the type must still \
         implement std::error::Error"
    );
    // Non-tautological: `InMemoryStoreError::Injected` (what `fail_next_enqueue` produces)
    // classifies `Transient`, so this fails if the error path ever collapsed to a constant verdict.
    assert_eq!(err.kind(), FailureKind::Transient);

    let text = err.to_string();
    assert!(!text.contains("order_id"), "must never mention the payload");
    assert!(
        !text.contains("SUPER_SECRET_HEADER_VALUE"),
        "must never mention a header value"
    );
    assert!(publisher.published().is_empty());
}
