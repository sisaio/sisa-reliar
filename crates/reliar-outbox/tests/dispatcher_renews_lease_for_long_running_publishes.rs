//! `extend_lease` renews `locked_until` for rows still outstanding once a publish has been
//! running for roughly half the lease, repeating until the batch finishes (§21.1).
//!
//! A **single** publish can never outlast a lease on its own — `OutboxDispatcherBuilder::build`
//! requires `lease > publish_timeout`, so one publish attempt is always bounded well inside one
//! lease. The renewal this proves is the **batch** case §21.1 actually describes: with
//! `max_in_flight = 1`, a second row only starts publishing once the first finishes, so the pair
//! together can run past the lease the whole batch was claimed under, even though neither
//! publish alone comes close to `publish_timeout`.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, DispatcherSettings, InMemoryOutboxStore, OutboxDispatcher, OutboxStore,
    PublishStep, ScriptedPublisher, WorkerId,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn the_second_row_of_a_serialized_batch_keeps_its_lease_renewed() {
    let store = InMemoryOutboxStore::default();
    let first = store.insert(common::distinct_envelope());
    let second = store.insert(common::distinct_envelope());

    let lease = Duration::from_millis(100);
    let publish_timeout = Duration::from_millis(80);
    let hang = Duration::from_millis(60);
    let publisher = ScriptedPublisher::keyed([
        (first.id, PublishStep::Hang(hang)),
        (second.id, PublishStep::Hang(hang)),
    ]);

    let settings = DispatcherSettings::default()
        .poll_interval(Duration::from_millis(5))
        .idle_poll_interval(Duration::from_millis(5))
        .lease(lease)
        .publish_timeout(publish_timeout)
        .store_timeout(Duration::from_millis(5))
        .max_in_flight(1);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Both rows are claimed together at t=0 with `locked_until = 100ms`. Serialized by
    // `max_in_flight = 1`: `first` publishes for 0..60ms and completes; `second` only starts at
    // ~60ms and is still hanging at t=110ms — past the *original* lease, which a lease-renewal
    // tick (every 50ms) must have already pushed forward, or `second` would be a stale claim by
    // now.
    for _ in 0..22 {
        common::advance_both(&store, Duration::from_millis(5)).await;
    }

    let first_record = store.record(first.id).expect("row still exists");
    assert!(
        first_record.published_at.is_some(),
        "the first row already completed"
    );

    let other_worker = WorkerId::generate();
    let probe = OutboxStore::acquire(&store, AcquireRequest::new(other_worker).lease(lease))
        .await
        .expect("acquire never fails for this fake");
    assert!(
        probe.is_empty(),
        "extend_lease kept the second row's lease renewed past its original 100 ms budget"
    );

    // Let `second` finish publishing.
    for _ in 0..10 {
        common::advance_both(&store, Duration::from_millis(5)).await;
    }
    let second_record = store.record(second.id).expect("row still exists");
    assert!(second_record.published_at.is_some());

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
