//! On cancellation, `run` stops claiming, drains in-flight publishes for at most
//! `drain_timeout`, persists every outcome that resolved, and releases the remainder — no row
//! stays locked by the exited worker (§43.A.22, SRS §26.1, ADR 0014).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DispatcherSettings, InMemoryOutboxStore, OutboxDispatcher, PublishStep, ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn drain_persists_the_fast_row_and_releases_the_slow_one() {
    let store = InMemoryOutboxStore::default();
    let fast = store.insert(common::distinct_envelope());
    let slow = store.insert(common::distinct_envelope());

    // `fast` resolves on its first poll; `slow` is still hanging when `drain_timeout` expires.
    let publisher = ScriptedPublisher::keyed([
        (fast.id, PublishStep::Ok),
        (slow.id, PublishStep::Hang(Duration::from_secs(10))),
    ]);

    let settings = common::fast_dispatcher_settings()
        .max_in_flight(4)
        .drain_timeout(Duration::from_millis(50));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Let both rows be claimed and `fast` resolve and persist before cancelling.
    common::advance_and_settle(Duration::from_millis(20)).await;
    cancel.cancel();

    // The drain has at most `drain_timeout` (50 ms) to wait on `slow`, which never resolves in
    // time.
    common::advance_and_settle(Duration::from_millis(80)).await;

    let outcome = handle.await.expect("dispatcher task joins");
    assert!(
        outcome.is_ok(),
        "cancellation is a normal outcome, never an error"
    );

    let fast_record = store.record(fast.id).expect("row still exists");
    assert!(
        fast_record.published_at.is_some(),
        "the resolved outcome was persisted"
    );
    assert!(fast_record.locked_by.is_none());

    let slow_record = store.record(slow.id).expect("row still exists");
    assert!(
        slow_record.published_at.is_none(),
        "the still-hanging publish was never completed"
    );
    assert!(
        slow_record.locked_by.is_none(),
        "no row stays locked by the exited worker — it was released, not abandoned"
    );
    assert_eq!(
        slow_record.attempts, 0,
        "release() never increments attempts — a released row is not a failed attempt (§19.1)"
    );
}

#[tokio::test(start_paused = true)]
async fn drain_is_a_no_op_when_nothing_is_in_flight() {
    let store = InMemoryOutboxStore::default();
    let dispatcher = OutboxDispatcher::builder(store, ScriptedPublisher::always(PublishStep::Ok))
        .settings(DispatcherSettings::default().drain_timeout(Duration::from_millis(50)))
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    cancel.cancel();
    let outcome = dispatcher.run(cancel).await;
    assert!(
        outcome.is_ok(),
        "an already-cancelled dispatcher exits cleanly with nothing to drain"
    );
}
