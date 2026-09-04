//! A healthy store persists every outcome promptly, even with **every** [`DispatcherSettings`]
//! field left at its default (S4 review 6, blocker): before the fix, the outcome-retry branch's
//! due time was re-armed to `now + outcome_retry_interval` after **every** attempt, including a
//! success — so once the first outcome of a run was written, every later one inherited that
//! stale due time and waited out the whole interval (with the defaults,
//! `poll_interval.min(lease / 4)` = 500 ms) before even being attempted, capping a perfectly
//! healthy store to one outcome batch per interval. The fix leaves the due time at "now" after a
//! success, so a fresh outcome is written on the very next loop iteration.
//!
//! Three rows publish at staggered times (0, 150, 300 ms) so each produces its own
//! `retry_unwritten_outcomes` round rather than all landing in the first, unthrottled attempt —
//! the second and third rounds are exactly where the bug bit.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{OutboxDispatcher, PublishStep, ScriptedPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn three_staggered_publishes_are_each_persisted_within_a_poll_interval() {
    let store = reliar_outbox::InMemoryOutboxStore::default();
    let first = store.insert(common::distinct_envelope());
    let second = store.insert(common::distinct_envelope());
    let third = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::keyed([
        (first.id, PublishStep::Ok),
        (second.id, PublishStep::Hang(Duration::from_millis(150))),
        (third.id, PublishStep::Hang(Duration::from_millis(300))),
    ]);

    // No settings overrides at all — only `worker_id` defaults to a generated value, exactly
    // like every other field (S4 review 6: the bug must reproduce with the library's own
    // defaults, not a test-tuned configuration).
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .build()
        .expect("default settings are valid");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Claim (immediate — `next_poll_at` starts at "now") and let `first`'s immediate publish
    // land.
    common::advance_both(&store, Duration::from_millis(20)).await;
    assert!(
        store
            .record(first.id)
            .expect("row still exists")
            .published_at
            .is_some(),
        "the first outcome, written before any pacing could apply, must land almost immediately"
    );

    // `second` resolves at 150 ms; check shortly after — well inside the 500 ms default
    // `poll_interval` — that it has already been persisted, not still waiting out a stale
    // 500 ms due time armed by `first`'s success.
    common::advance_both(&store, Duration::from_millis(130)).await; // t ≈ 150 ms
    common::advance_both(&store, Duration::from_millis(50)).await; // t ≈ 200 ms
    assert!(
        store
            .record(second.id)
            .expect("row still exists")
            .published_at
            .is_some(),
        "the second outcome (published at ~150 ms) was not persisted by ~200 ms — the \
         outcome-retry due time was re-armed after the first success instead of being left due \
         immediately"
    );

    // `third` resolves at 300 ms (t ≈ 150 ms + 150 ms from here); check shortly after.
    common::advance_both(&store, Duration::from_millis(150)).await; // t ≈ 350 ms
    common::advance_both(&store, Duration::from_millis(50)).await; // t ≈ 400 ms
    assert!(
        store
            .record(third.id)
            .expect("row still exists")
            .published_at
            .is_some(),
        "the third outcome (published at ~300 ms) was not persisted by ~400 ms — a healthy \
         store is being paced as if it had just failed"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
