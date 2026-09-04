//! A permanent failure, or attempts exceeding `max_attempts`, sets `dead_at`/`DeadReason` and a
//! truncated, payload-free `last_error`; dead rows are never claimed again (§43.A.15, SRS §23,
//! §17.1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DeadReason, ExponentialBackoff, InMemoryOutboxStore, OutboxDispatcher, PublishStep,
    ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn permanent_failure_dead_letters_immediately() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::keyed([(seeded.id, PublishStep::Permanent)]);

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    common::advance_and_settle(Duration::from_millis(20)).await;

    let record = store.record(seeded.id).expect("row still exists");
    assert_eq!(record.dead_reason, Some(DeadReason::PermanentError));
    assert!(record.dead_at.is_some());
    assert!(record.locked_by.is_none());
    assert_eq!(
        record.attempts, 1,
        "a permanent failure dead-letters on its first attempt"
    );
    let last_error = record.last_error.expect("fail() records the error");
    assert!(!last_error.is_empty());

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}

#[tokio::test(start_paused = true)]
async fn attempts_exhausted_dead_letters_after_max_attempts() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::always(PublishStep::Transient);

    // `max_attempts(2)`: the first failure (attempts_before = 0) retries, the second
    // (attempts_before = 1, `1 + 1 >= 2`) dead-letters.
    let retry = ExponentialBackoff::default()
        .base(Duration::from_millis(1))
        .max_delay(Duration::from_millis(1))
        .max_attempts(2)
        .jitter(0.0);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(common::fast_dispatcher_settings())
        .retry_policy(retry)
        .build()
        .expect("valid settings");
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // First failure, then let its 1ms retry delay elapse (in lockstep with the store clock) so
    // the dispatcher's next poll reclaims it for the second, exhausting attempt.
    common::advance_both(&store, Duration::from_millis(15)).await;
    common::advance_both(&store, Duration::from_millis(15)).await;

    let record = store.record(seeded.id).expect("row still exists");
    assert_eq!(record.dead_reason, Some(DeadReason::AttemptsExhausted));
    assert_eq!(record.attempts, 2);
    assert!(record.locked_by.is_none());

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
