//! A row with a resolved publish but an unwritten `complete`/`fail` outcome still counts toward
//! the `max_in_flight` claim gate — it still holds this worker's lease, even though its publish
//! task has already finished (L1, L2, S4 review 2/3, major 3).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_pending_complete_outcome_blocks_a_new_claim_at_max_in_flight_one() {
    let store = InMemoryOutboxStore::default();
    let claimed = store.insert(common::distinct_envelope());
    let never_claimed = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // Every `complete` attempt hangs far longer than `store_timeout`, so `claimed`'s outcome
    // never leaves `unwritten_complete` within this test's window.
    store.hang_next(1_000, Duration::from_secs(3600));

    let settings = common::fast_dispatcher_settings()
        .max_in_flight(1)
        .store_timeout(Duration::from_millis(20));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Long enough for `claimed` to be claimed, published, and for its stuck `complete` to be
    // retried several times — never enough to also claim `never_claimed`, if the gate is honest.
    common::advance_and_settle(Duration::from_millis(100)).await;

    assert_eq!(
        publisher.count(claimed.id),
        1,
        "claimed was published exactly once"
    );
    let never_claimed_record = store.record(never_claimed.id).expect("row still exists");
    assert!(
        never_claimed_record.locked_by.is_none(),
        "max_in_flight(1) is already spent on claimed's unwritten outcome — a store call that \
         only counted `outstanding.len()` (ignoring unwritten_complete/unwritten_fail) would let \
         this row be claimed too, which is exactly the mutation this assertion catches"
    );

    cancel.cancel();
    let _ = handle.await;
}
