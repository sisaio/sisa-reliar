//! A `complete`/`fail` write that keeps failing or timing out (transient) is not retried
//! forever: once it has been retried for longer than `lease`, the row is dropped from
//! `outstanding` and excluded from further lease renewal, so the lease lapses, another worker
//! can reclaim it, and — the property this test checks directly — the claim gate always frees
//! eventually rather than being held shut by one permanently stuck row (RELIAR-26, M2; contract
//! §7 M2, SRS §22.1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_permanently_stuck_row_does_not_hold_the_gate_shut_for_other_rows() {
    let store = InMemoryOutboxStore::default();
    let stuck = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // Every `complete` attempt for *every* row hangs, and hangs longer than `store_timeout`
    // every single time — `stuck` never gets marked complete, no matter how many times it is
    // retried.
    store.hang_next(10_000, Duration::from_secs(3600));

    // `max_in_flight(1)`: with `stuck` permanently occupying the one slot, `other` (seeded
    // below, *after* `stuck` is already claimed) can only ever be claimed if M2 eventually drops
    // `stuck` from the gate's accounting — otherwise the gate stays shut forever.
    let lease = Duration::from_millis(100);
    let settings = common::fast_dispatcher_settings()
        .max_in_flight(1)
        .lease(lease)
        .publish_timeout(Duration::from_millis(30))
        .store_timeout(Duration::from_millis(20));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Tokio-time only first, so the claim lands while the store's clock is still at t=0.
    common::advance_and_settle(Duration::from_millis(5)).await;
    assert_eq!(
        publisher.count(stuck.id),
        1,
        "stuck was published once, then got stuck completing"
    );

    // Seed the second row only now — before this point there was nothing else *to* claim, which
    // would make "other was never claimed" trivially true regardless of M2.
    let other = store.insert(common::distinct_envelope());

    // Small, repeated hops (not one big jump) so each subsequent `complete` retry / lease-ticker
    // renewal / re-claim attempt only gets *registered* once the previous one resolves.
    for _ in 0..30 {
        common::advance_both(&store, Duration::from_millis(10)).await;
    }

    assert_eq!(
        publisher.count(other.id),
        1,
        "the gate must have freed at some point — `stuck` alone, at max_in_flight(1), can never \
         let a second row be claimed unless M2 eventually stops counting it"
    );

    cancel.cancel();
    let _ = handle.await;
}
