//! A row with a resolved publish but an unwritten `complete`/`fail` outcome still has its lease
//! renewed by the lease ticker — dropping it from renewal while its outcome write keeps retrying
//! would let another worker reclaim and republish it while this worker still believes it owns it
//! (S4 review 3, major 4).
//!
//! Checked by directly inspecting `locked_until` rather than by probing reclaimability: M2 (a
//! later ruling in this same story) bounds outcome-write retries by the *same* `lease` value, so
//! a probe checked at or after one full lease cannot tell "never renewed" apart from "renewed,
//! then M2 gave up right on schedule" — both look identical from outside once the row is old
//! enough for M2 to have dropped it. Checking that `locked_until` has already moved **past**
//! what a single, unrenewed claim would have produced, part-way through the lease, has no such
//! ambiguity.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_pending_complete_outcome_has_its_lease_renewed_before_it_would_naturally_expire() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // Every `complete` attempt hangs far longer than `store_timeout`, so the row's outcome never
    // leaves `unwritten_complete` within this test's window.
    store.hang_next(1_000, Duration::from_secs(3600));

    let lease = Duration::from_millis(200);
    // `max_in_flight(1)`: with only one row ever seeded, this also keeps the claim gate closed
    // once the row moves to `unwritten_complete`, so the dispatcher's own *next* claim cannot
    // re-claim the same row itself (a fresh claim would also push `locked_until` forward, which
    // would be indistinguishable from renewal).
    let settings = common::fast_dispatcher_settings()
        .lease(lease)
        .max_in_flight(1)
        .publish_timeout(Duration::from_millis(50))
        .store_timeout(Duration::from_millis(10));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Tokio-time only first, so the claim happens while the store's own clock is still at t=0.
    common::advance_and_settle(Duration::from_millis(5)).await;
    assert_eq!(
        publisher.count(seeded.id),
        1,
        "the publish itself already resolved"
    );
    let original_locked_until = store
        .record(seeded.id)
        .expect("row still exists")
        .locked_until
        .expect("still leased right after being claimed");

    // Past the lease ticker's first tick (lease / 2 = 100 ms) but well *before* the full lease
    // (200 ms) — M2's own age-based drop cannot have fired yet at this point.
    common::advance_both(&store, Duration::from_millis(120)).await;

    let renewed_locked_until = store
        .record(seeded.id)
        .expect("row still exists")
        .locked_until
        .expect("still leased — the row was never released or completed");
    assert!(
        renewed_locked_until > original_locked_until,
        "the lease ticker must have pushed locked_until forward for this row even though its \
         publish already resolved and only its complete() outcome is still unwritten; \
         locked_until never moving is exactly what dropping unwritten rows from lease renewal \
         would produce"
    );

    cancel.cancel();
    let _ = handle.await;
}
