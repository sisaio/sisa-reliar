//! A `complete`/`fail` write that fails or times out keeps its rows outstanding rather than
//! forgetting them: the write is retried on a later loop iteration instead of being dropped (L2,
//! S4 review 2; ADR-lite L2, SRS §23.2 — "publish succeeded, completion failed").
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_timed_out_complete_is_retried_and_eventually_persists() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // The *first* `complete` call hangs far longer than `store_timeout`, so `bounded()` gives up
    // on it; the row must stay outstanding and get a second attempt, which this time is not
    // armed to hang and so succeeds.
    store.hang_next(1, Duration::from_secs(60));

    let settings = common::fast_dispatcher_settings().store_timeout(Duration::from_millis(20));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Long enough to cover: publish, the first `complete` timing out at 20 ms, the
    // `outcome_retry_interval` due-time gate before the retry (S4 review 5, blocker), and a
    // later loop iteration's successful retry. Many small steps, not one big jump — a timer only
    // registered partway through an earlier jump (the due-time sleep, created fresh once the
    // first attempt resolves) does not reliably fire within that same jump.
    for _ in 0..10 {
        common::advance_and_settle(Duration::from_millis(10)).await;
    }

    assert_eq!(
        publisher.count(seeded.id),
        1,
        "the publish itself only ever ran once"
    );
    let record = store.record(seeded.id).expect("row still exists");
    assert!(
        record.published_at.is_some(),
        "the outcome was eventually persisted by a retried complete()"
    );
    assert!(
        record.locked_by.is_none(),
        "the successful retry cleared the lease"
    );
    assert_eq!(
        record.attempts, 1,
        "the row was never double-counted by the retry"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
