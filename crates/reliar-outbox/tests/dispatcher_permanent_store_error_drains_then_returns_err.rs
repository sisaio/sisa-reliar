//! A store error the provider classifies `Permanent` ends `run()` with
//! `Err(DispatchError::Store(_))` — but only after draining best-effort: outcomes already
//! resolved are persisted, the rest are released, and any error from that drain is logged and
//! discarded so the original diagnosis surfaces (S4 review, major 6; ADR-lite K5).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DispatchError, InMemoryOutboxStore, OutboxDispatcher, PublishStep, RecordingPublisher,
    ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_permanent_store_error_ends_run_with_err_after_draining() {
    let store = InMemoryOutboxStore::default();
    let published = store.insert(common::distinct_envelope());
    let never_claimed = store.insert(common::distinct_envelope());

    // The first `acquire` succeeds and claims both rows; the *second* call (the next poll, or
    // the drain-phase `release`) is permanently broken. `published` gets a chance to complete
    // before the permanent failure is ever observed by `run`'s own claim loop, because the
    // failure is injected to strike the *next* store call after the first successful claim.
    let script = ScriptedPublisher::keyed([(published.id, PublishStep::Ok)]);

    // `batch_size(1)`: only `published` (the smaller sequence) is claimed by the first
    // `acquire`; `never_claimed` is left for a second `acquire` that never gets to run.
    let dispatcher = OutboxDispatcher::builder(store.clone(), script)
        .settings(common::fast_dispatcher_settings().batch_size(1))
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel));

    // Let the first claim + publish + complete happen, *then* arm the permanent failure so the
    // second `acquire` (claiming `never_claimed`) is what actually trips it.
    common::advance_and_settle(Duration::from_millis(5)).await;
    store.fail_next_permanent(1);
    common::advance_and_settle(Duration::from_millis(30)).await;

    let outcome = handle.await.expect("dispatcher task joins");

    let err = outcome.expect_err("a permanent store error must return Err, never Ok");
    match &err {
        DispatchError::Store(store_err) => {
            assert!(
                store_err.to_string().contains("permanent"),
                "the underlying store error should describe itself: {store_err}"
            );
        }
        other => panic!("expected DispatchError::Store, got a differently-shaped error: {other:?}"),
    }
    // `Display`/`source()` on `DispatchError` itself.
    assert!(err.to_string().contains("permanent store error"));
    assert!(
        std::error::Error::source(&err).is_some(),
        "DispatchError::Store wires source() to the underlying store error"
    );

    // The row published before the permanent failure was persisted; the row never claimed is
    // untouched (never leased, nothing to release).
    let published_record = store.record(published.id).expect("row still exists");
    assert!(published_record.published_at.is_some());
    let never_claimed_record = store.record(never_claimed.id).expect("row still exists");
    assert!(never_claimed_record.locked_by.is_none());
}

/// A second, independent case: the permanent failure strikes on the very first `acquire`, before
/// anything has ever been claimed — `run` still returns `Err`, and drains trivially (nothing
/// outstanding to release).
#[tokio::test(start_paused = true)]
async fn a_permanent_failure_on_the_first_claim_still_returns_err() {
    let store = InMemoryOutboxStore::default();
    store.fail_next_permanent(1);
    let publisher = RecordingPublisher::default();

    let dispatcher = OutboxDispatcher::builder(store, publisher)
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel));
    common::advance_and_settle(Duration::from_millis(10)).await;

    let outcome = handle.await.expect("dispatcher task joins");
    assert!(matches!(outcome, Err(DispatchError::Store(_))));
}

/// A third case, matching K5's request directly: a row is **genuinely in flight** (its publish
/// is blocking) when the permanent failure hits, so the drain this exit path performs actually
/// has something to do — a resolved outcome to persist and an unresolved one to release.
#[tokio::test(start_paused = true)]
async fn a_permanent_failure_with_a_genuinely_in_flight_row_drains_it() {
    let store = InMemoryOutboxStore::default();
    let fast = store.insert(common::distinct_envelope());
    let slow = store.insert(common::distinct_envelope());
    let script = ScriptedPublisher::keyed([
        (fast.id, PublishStep::Ok),
        (slow.id, PublishStep::Hang(Duration::from_secs(3600))),
    ]);

    // `batch_size(1)`, `max_in_flight(2)`: `fast` is claimed and persisted first, freeing
    // capacity for `slow` to be claimed next while still genuinely blocking.
    let settings = common::fast_dispatcher_settings()
        .batch_size(1)
        .max_in_flight(2)
        .drain_timeout(Duration::from_millis(50));
    let dispatcher = OutboxDispatcher::builder(store.clone(), script)
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel));

    // Let both rows be claimed (`fast` completes, `slow` starts hanging), *then* arm the
    // permanent failure so the next `acquire` (finding nothing left anyway) is what trips it.
    common::advance_and_settle(Duration::from_millis(10)).await;
    let fast_record = store.record(fast.id).expect("row still exists");
    assert!(
        fast_record.published_at.is_some(),
        "fast completed before the permanent failure"
    );
    let slow_record = store.record(slow.id).expect("row still exists");
    assert!(
        slow_record.locked_by.is_some(),
        "slow is genuinely claimed and in flight"
    );

    store.fail_next_permanent(1);
    // Past the permanent failure and the 50 ms drain_timeout that follows it.
    common::advance_and_settle(Duration::from_millis(100)).await;

    let outcome = handle.await.expect("dispatcher task joins");
    assert!(matches!(outcome, Err(DispatchError::Store(_))));

    let slow_record = store.record(slow.id).expect("row still exists");
    assert!(
        slow_record.locked_by.is_none(),
        "the still-hanging row was released once drain_timeout expired"
    );
    assert!(slow_record.published_at.is_none());
}
