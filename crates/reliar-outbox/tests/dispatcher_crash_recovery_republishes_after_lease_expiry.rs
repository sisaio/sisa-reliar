//! The crash window (SRS §22, §43.A.11): a publish reaches the broker, the worker crashes
//! before `complete` persists, the lease expires, and another worker republishes the same row —
//! the recording publisher observes the id twice.
//!
//! Drives a **real** `OutboxDispatcher`: [`RecordingPublisher::with_concurrency_probe`] holds
//! the publish open on a paused timer just long enough to observe the id recorded (at first
//! poll, S4 review 7) before the whole dispatcher task is `abort()`-ed — a hard kill, standing in
//! for the process crashing outright, with no drain and no `complete` ever reached. A second
//! dispatcher, sharing the same store and publisher, then reclaims the row once its lease has
//! expired.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_lease_expiring_before_complete_republishes_the_row() {
    let store = InMemoryOutboxStore::default();
    // Short enough to resolve well inside the default 10 s `publish_timeout` (worker B needs it
    // to actually complete), long enough that 20 ms of advance can never race past it (worker A
    // needs it to still be genuinely in flight when aborted).
    let publisher = RecordingPublisher::with_concurrency_probe(Duration::from_secs(1));
    let seeded = store.insert(common::distinct_envelope());

    // The default lease (30 s) comfortably exceeds the default `publish_timeout` (10 s), which
    // `OutboxDispatcherBuilder::build` requires.
    let lease = Duration::from_secs(30);
    let settings = common::fast_dispatcher_settings();

    // Worker A claims and publishes — the probe holds it "in flight" long enough to be certain
    // the id was recorded before we kill the task.
    let dispatcher_a = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings.clone())
        .build()
        .expect("valid settings");
    let cancel_a = CancellationToken::new();
    let handle_a = tokio::spawn(dispatcher_a.run(cancel_a));

    common::advance_and_settle(Duration::from_millis(20)).await;
    assert_eq!(
        publisher.count(seeded.id),
        1,
        "worker A's publish was recorded"
    );

    // Worker A "crashes": the task is killed outright, never draining, never calling `complete`.
    handle_a.abort();
    let _ = handle_a.await;

    // The lease expires (SQL time-travel's fake counterpart).
    store.advance(lease + Duration::from_secs(1));

    // Worker B, sharing the same store and publisher, reclaims and republishes the row.
    let dispatcher_b = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");
    let cancel_b = CancellationToken::new();
    let handle_b = tokio::spawn(dispatcher_b.run(cancel_b.clone()));

    // Past the 1 s probe, well inside the 10 s `publish_timeout`.
    common::advance_and_settle(Duration::from_millis(1_100)).await;
    assert_eq!(
        publisher.count(seeded.id),
        2,
        "the recording publisher observes the id twice — this is the duplicate, not a bug"
    );

    let record = store.record(seeded.id).expect("row still exists");
    assert!(
        record.published_at.is_some(),
        "worker B's complete() persisted"
    );

    cancel_b.cancel();
    handle_b
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
