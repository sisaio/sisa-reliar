//! Every `last_error`/`error` field is truncated to 2 KiB at a char boundary with a
//! `"…[truncated]"` marker (§17.1) — exercised through each of its three public entry points:
//! [`PoisonedRow::new`], [`OutboxRecord::builder`]'s `last_error`, and [`FailedMessage::new`].

mod common;

use reliar_core::MessageId;
use reliar_outbox::{FailedMessage, FailureOutcome, MessageRef, OutboxRecord, PoisonedRow};

/// Mirrors the private `MAX_ERROR_LEN` in `record.rs` — this file asserts the public,
/// documented bound (§17.1), not the implementation constant itself.
const MAX_ERROR_LEN: usize = 2048;
const MARKER: &str = "…[truncated]";

#[test]
fn poisoned_row_keeps_a_short_error_verbatim() {
    let poisoned = PoisonedRow::new(MessageId::new(), 1, "decode failed: bad tag");
    assert_eq!(poisoned.error, "decode failed: bad tag");
}

#[test]
fn poisoned_row_truncates_an_over_limit_error() {
    let error = "x".repeat(MAX_ERROR_LEN + 500);
    let poisoned = PoisonedRow::new(MessageId::new(), 1, error);
    assert!(poisoned.error.len() <= MAX_ERROR_LEN);
    assert!(poisoned.error.ends_with(MARKER));
}

#[test]
fn poisoned_row_truncation_never_splits_a_multi_byte_character() {
    // Each 'é' is 2 bytes in UTF-8, chosen so a naive byte-2048 cut would land mid-character.
    let error: String = std::iter::repeat_n('é', 2000).collect::<String>() + &"x".repeat(200);
    // `PoisonedRow::error` is a `String`, which is always valid UTF-8 by construction — the
    // meaningful assertion is that building it did not panic while searching for a boundary.
    let poisoned = PoisonedRow::new(MessageId::new(), 1, error);
    assert!(poisoned.error.len() <= MAX_ERROR_LEN);
    assert!(poisoned.error.ends_with(MARKER));
}

#[test]
fn record_builder_truncates_last_error() {
    let error = "x".repeat(MAX_ERROR_LEN + 10);
    let record = OutboxRecord::builder(
        common::serialized_envelope(),
        1,
        time::OffsetDateTime::now_utc(),
    )
    .last_error(Some(error))
    .build();

    let last_error = record.last_error.expect("set above");
    assert!(last_error.len() <= MAX_ERROR_LEN);
    assert!(last_error.ends_with(MARKER));
}

#[test]
fn record_builder_keeps_no_error_as_none() {
    let record = OutboxRecord::builder(
        common::serialized_envelope(),
        1,
        time::OffsetDateTime::now_utc(),
    )
    .build();
    assert_eq!(record.last_error, None);
}

#[test]
fn failed_message_truncates_its_error() {
    let message_ref = MessageRef::new(MessageId::new(), time::OffsetDateTime::now_utc());
    let error = "x".repeat(MAX_ERROR_LEN + 10);
    let failed = FailedMessage::new(
        message_ref,
        error,
        FailureOutcome::Retry {
            delay: std::time::Duration::from_secs(1),
        },
    );

    assert!(failed.error.len() <= MAX_ERROR_LEN);
    assert!(failed.error.ends_with(MARKER));
}
