//! B1/U16: every one of `NatsPublishError`'s ten variants, constructed directly with no server and
//! no `Context`, asserted against contract §4.3's fixed `Classify` table. The round-2 review found
//! only 4 of 10 variants covered by the real-NATS `n5_error_classification.rs` scenarios — flipping
//! any of the other six verdicts passed every test that existed before this file. Each assertion
//! here fails if that one variant's verdict is ever flipped (review B1).
//!
//! **What this file does not cover:** the `PublishErrorKind → NatsPublishError` mapping itself
//! (`classify_publish_error` in `src/publisher.rs`). `async_nats::jetstream::context::PublishError`
//! is not constructible outside `async-nats`, so that mapping can only be exercised against a real
//! server, and only 3 of its 8 `PublishErrorKind` arms are reliably reachable that way; in
//! particular `MaxAckPending` never fires against a default `Context` — reaching it at all
//! requires deliberately shrinking `max_ack_inflight` and opting out of `backpressure_on_inflight`
//! (`n5_error_classification.rs`, review M5).

use async_nats::Subject;
use reliar_core::{Classify, FailureKind};
use reliar_transport_nats::{NatsMapError, NatsPublishError, SubjectError};

fn subject() -> Subject {
    Subject::from("reliar.test.classification".to_string())
}

#[test]
fn map_is_permanent() {
    let err = NatsPublishError::Map(NatsMapError::MissingHeader {
        header: "reliar-message-id",
    });
    assert_eq!(err.kind(), FailureKind::Permanent);
}

#[test]
fn subject_is_permanent() {
    let err = NatsPublishError::Subject {
        source: Box::new(SubjectError::Empty),
    };
    assert_eq!(err.kind(), FailureKind::Permanent);
}

#[test]
fn payload_too_large_is_permanent() {
    let err = NatsPublishError::PayloadTooLarge { len: 100, limit: 1 };
    assert_eq!(err.kind(), FailureKind::Permanent);
}

#[test]
fn max_payload_exceeded_is_permanent() {
    let err = NatsPublishError::MaxPayloadExceeded { subject: subject() };
    assert_eq!(err.kind(), FailureKind::Permanent);
}

#[test]
fn wrong_last_message_is_permanent() {
    let err = NatsPublishError::WrongLastMessage { subject: subject() };
    assert_eq!(err.kind(), FailureKind::Permanent);
}

#[test]
fn timeout_is_transient() {
    let err = NatsPublishError::Timeout {
        subject: subject(),
        after_ms: 10_000,
    };
    assert_eq!(err.kind(), FailureKind::Transient);
}

#[test]
fn connection_is_transient() {
    let err = NatsPublishError::Connection { subject: subject() };
    assert_eq!(err.kind(), FailureKind::Transient);
}

#[test]
fn stream_not_found_is_transient() {
    let err = NatsPublishError::StreamNotFound { subject: subject() };
    assert_eq!(err.kind(), FailureKind::Transient);
}

#[test]
fn max_ack_pending_is_transient() {
    let err = NatsPublishError::MaxAckPending { subject: subject() };
    assert_eq!(err.kind(), FailureKind::Transient);
}

#[test]
fn broker_is_transient() {
    let err = NatsPublishError::Broker { subject: subject() };
    assert_eq!(err.kind(), FailureKind::Transient);
}
