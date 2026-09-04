//! A `complete`/`fail` write error the provider classifies `Permanent` ends `run()` with
//! `Err(DispatchError::Store(_))` after a best-effort drain; the row it was trying to persist
//! (already published) is left to its lease rather than released — it is already delivered, so
//! releasing it would buy an immediate, certain duplicate and recover nothing (RELIAR-26, M1;
//! contract §7 M1, ADR 0014).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{DispatchError, InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_permanent_complete_error_ends_run_and_leaves_the_published_row_leased() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // Hang every `complete` attempt from the very start (a huge budget — this is *reset*, not
    // exhausted, below): `hang_next` only ever affects `complete`, never `acquire`/
    // `extend_lease`, so this cannot be "stolen" by the initial claim or a lease-renewal tick,
    // and it guarantees the row cannot land while we still need it unwritten.
    store.hang_next(1_000_000, Duration::from_secs(3600));

    // `max_in_flight(1)` with only one row ever seeded: once claimed, the gate stays closed for
    // the rest of the test, so `acquire` never runs again either.
    let settings = common::fast_dispatcher_settings()
        .max_in_flight(1)
        .store_timeout(Duration::from_millis(20));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel));

    // Claim + publish, then let a few (hung) `complete` attempts time out at 20 ms each — every
    // one is guaranteed to hang, so none can land regardless of how many rounds this takes.
    common::advance_and_settle(Duration::from_millis(60)).await;
    assert_eq!(
        publisher.count(seeded.id),
        1,
        "the publish itself already succeeded"
    );
    assert!(
        store
            .record(seeded.id)
            .expect("row still exists")
            .published_at
            .is_none(),
        "every attempt so far hung and timed out — none has landed"
    );

    // Switch deterministically: cut the hang off (no attempt in flight right now can be affected
    // — `complete`'s synchronous prefix already ran for it, so it is already committed to either
    // its `Eager` or `Deferred` path) and arm a permanent failure for every attempt from here on.
    store.hang_next(0, Duration::ZERO);
    store.fail_next_permanent(20);
    common::advance_and_settle(Duration::from_millis(60)).await;

    let outcome = handle.await.expect("dispatcher task joins");
    assert!(
        matches!(outcome, Err(DispatchError::Store(_))),
        "a Permanent-classified outcome-write error must end run() with Err, never wedge the \
         worker silently (M1)"
    );

    let record = store.record(seeded.id).expect("row still exists");
    assert!(
        record.published_at.is_none(),
        "the write never actually landed — try_complete returns before mutating when the \
         injected failure fires"
    );
    assert!(
        record.locked_by.is_some(),
        "an already-delivered row is left to its lease, never released — releasing it would be \
         a certain duplicate for a message that already went out (L3, M1)"
    );
}
