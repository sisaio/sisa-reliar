//! A `complete`/`fail` that fails *fast* (returns `Err` immediately — a connection pool
//! exhausted, a reset connection — no hang at all) must not retry at CPU speed for the whole
//! `lease` window: the outcome-write retry branch is gated behind a due-time
//! (`outcome_retry_interval`), so even a persistently, instantly failing store call is paced
//! rather than spun (S4 review 5, blocker).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DispatcherSettings, InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_fast_failing_complete_retries_on_a_schedule_not_at_cpu_speed() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // Every `complete` attempt fails immediately (transient, not hung) until the store
    // "recovers". A large but *finite* budget, not `usize::MAX` (S4 review 7): with the
    // due-time gate genuinely broken, an unbounded budget never lets the store recover, so
    // `has_unwritten` never goes false and the paused clock never has an idle moment to advance
    // past — `tokio::time::advance` then hangs the whole test binary forever rather than
    // failing (confirmed by hand: killed after 40+ s pinned at 100% CPU with zero progress).
    // 100_000 is comfortably larger than any bound this test asserts on, so a healthy dispatcher
    // never gets close to it, but a regressed one exhausts it and lets the loop terminate.
    store.fail_next_complete(100_000);

    // `outcome_retry_interval` is derived from `poll_interval` (S4 review 6, minor — not
    // `idle_poll_interval`, which only paces the claim path), capped at `lease / 4`; the default
    // `lease` (30 s) keeps the cap well out of the way here.
    let poll_interval = Duration::from_millis(10);
    let settings = DispatcherSettings::default()
        .poll_interval(poll_interval)
        .max_in_flight(1);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // 5 s of virtual time while every attempt fails fast. Many small steps, not one big jump —
    // each retry's due-time timer is only registered once the previous attempt resolves.
    // Wrapped in a timeout (S4 review 7): the finite budget above is what actually terminates a
    // regression, but a *different* regression shaped so the budget alone would not resolve it
    // (e.g. the loop needing far more than 500 steps) still fails this test cleanly instead of
    // hanging the suite.
    tokio::time::timeout(Duration::from_secs(60), async {
        for _ in 0..500 {
            common::advance_and_settle(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect(
        "the advance/settle loop did not finish inside 60 s of virtual time — the dispatcher is \
         likely spinning",
    );

    // Bounded, not CPU-speed. This is checked before the assertions below it so that a
    // regression severe enough to reach this point at all (i.e. one the timeout above did not
    // already catch) fails on the assertion that names the actual defect, rather than on one of
    // the downstream symptom assertions.
    // At most one attempt per `outcome_retry_interval`, plus a small margin for the very first
    // (immediately-due) attempt and rounding at the seams of the 10 ms steps above.
    let max_expected_calls =
        (Duration::from_secs(5).as_millis() / poll_interval.as_millis()) as usize + 2;
    let calls = store.complete_call_count();
    assert!(
        calls <= max_expected_calls,
        "complete() was called {calls} times in 5 s of virtual time — expected at most \
         {max_expected_calls} (paced by outcome_retry_interval), not a CPU-speed spin"
    );
    assert!(calls >= 1, "at least the first attempt must have happened");

    assert_eq!(
        publisher.count(seeded.id),
        1,
        "the publish itself only ever ran once"
    );
    assert!(
        store
            .record(seeded.id)
            .expect("row still exists")
            .published_at
            .is_none(),
        "every attempt so far failed fast — none has landed"
    );

    // Let the store "recover" and confirm the outcome is still eventually written once it does.
    store.fail_next_complete(0);
    tokio::time::timeout(Duration::from_secs(60), async {
        for _ in 0..30 {
            common::advance_and_settle(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the recovery loop did not finish — the outcome never landed after recovery");
    let record = store.record(seeded.id).expect("row still exists");
    assert!(
        record.published_at.is_some(),
        "once the store recovers, the paced retry still eventually persists the outcome"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
