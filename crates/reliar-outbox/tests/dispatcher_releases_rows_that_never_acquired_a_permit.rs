//! On cancellation, a spawned publish task that has **not yet** acquired its concurrency permit
//! is dropped and its row released immediately, rather than started or awaited (K3, S4 review;
//! "drain finishes what started, it never starts anything new").
//!
//! Under the `max_in_flight` claim gate (L1), a *conforming* store never hands back more rows
//! than there is concurrency budget for, so this window is normally only reachable through a
//! genuine scheduling race. An [`common::OverDeliveringStore`] makes it deterministic instead
//! (architect ruling, RELIAR-15 review 3, blocker 1/2): it claims far more rows than
//! `max_in_flight` permits exist, so most of them are genuinely parked on the semaphore —
//! `started == false` — when cancellation arrives.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, PublishStep, ScriptedPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn rows_that_never_started_are_released_not_awaited() {
    let store = InMemoryOutboxStore::default();
    let rows: Vec<_> = (0..20)
        .map(|_| store.insert(common::distinct_envelope()))
        .collect();
    // If any of these ever actually started publishing, this would record it.
    let publisher = ScriptedPublisher::always(PublishStep::Hang(Duration::from_secs(3600)));

    // Claims all 20 rows at once against only 4 permits: 16 of them are genuinely un-permitted,
    // not a transient scheduling artifact.
    let over_delivering = common::OverDeliveringStore::new(store.clone(), 20);
    let settings = common::fast_dispatcher_settings()
        .max_in_flight(4)
        .drain_timeout(Duration::from_millis(50));
    let dispatcher = OutboxDispatcher::builder(over_delivering, publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Let the claim happen and the 4 permitted tasks start hanging, then cancel.
    common::advance_and_settle(Duration::from_millis(10)).await;
    cancel.cancel();
    // Past drain_timeout, so the 4 genuinely in-flight (permitted) tasks are also released.
    common::advance_and_settle(Duration::from_millis(100)).await;

    let outcome = handle.await.expect("dispatcher task joins");
    assert!(
        outcome.is_ok(),
        "cancellation is a normal outcome, never an error"
    );

    assert_eq!(
        publisher.published().len(),
        4,
        "exactly the 4 rows that genuinely acquired a permit ever touched the publisher — a \
         hung publish records itself at first poll, so a count above 4 would mean a \
         not-yet-permitted task was started anyway"
    );
    for row in &rows {
        let record = store.record(row.id).expect("row still exists");
        assert!(
            record.locked_by.is_none(),
            "every claimed row is released — the 4 that were genuinely in flight release at \
             the drain timeout, the 16 that never acquired a permit release immediately; this \
             assertion is what mutating `release_immediately` to an empty `Vec` breaks"
        );
    }
}
