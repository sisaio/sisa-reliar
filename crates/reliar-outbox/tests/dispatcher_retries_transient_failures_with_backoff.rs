//! A transient publish failure yields `FailureOutcome::Retry`: `attempts + 1`, `available_at =
//! now() + delay` from the configured `RetryPolicy`, and the row stays unclaimable before then
//! (§43.A.14, SRS §23.1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, ExponentialBackoff, InMemoryOutboxStore, OutboxDispatcher, OutboxStore,
    PublishStep, ScriptedPublisher, WorkerId,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn transient_failure_schedules_a_deterministic_retry() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::keyed([(seeded.id, PublishStep::Transient)]);

    // `jitter(0.0)` makes the delay exact, so the test asserts an equality instead of a range.
    let retry = ExponentialBackoff::default()
        .base(Duration::from_secs(1))
        .jitter(0.0);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(common::fast_dispatcher_settings())
        .retry_policy(retry)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Tokio-time only: the store's own clock must still read `seeded.created_at` (t=0) when the
    // first claim and failure happen, so `available_at` lands exactly `delay` after it.
    common::advance_and_settle(Duration::from_millis(20)).await;

    let record = store.record(seeded.id).expect("row still exists");
    assert_eq!(record.attempts, 1);
    assert!(
        record.dead_at.is_none(),
        "a transient failure never dead-letters"
    );
    assert!(record.locked_by.is_none(), "fail() clears the lease");
    assert_eq!(
        (record.available_at - seeded.created_at).unsigned_abs(),
        Duration::from_secs(1),
        "available_at = now() + delay, exactly, with jitter disabled"
    );

    // Not due yet: a fresh worker's claim right after the failure sees nothing.
    let probe = OutboxStore::acquire(&store, AcquireRequest::new(WorkerId::generate()))
        .await
        .expect("acquire never fails for this fake");
    assert!(
        probe.is_empty(),
        "the row is not due until its retry delay elapses"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
