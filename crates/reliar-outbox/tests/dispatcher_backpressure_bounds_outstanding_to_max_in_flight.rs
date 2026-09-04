//! `run` claims only while `outstanding < max_in_flight` — a publisher slower than the poll
//! interval does not make one dispatcher hoard every lease in the backlog (L1, S4 review 2;
//! ADR-lite L1). A second dispatcher can still claim and publish whatever the first never got to.
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, InMemoryOutboxStore, OutboxDispatcher, OutboxStore, PublishStep,
    RecordingPublisher, ScriptedPublisher, WorkerId,
};
use tokio_util::sync::CancellationToken;

const TOTAL_ROWS: usize = 20;
const MAX_IN_FLIGHT: usize = 4;

#[tokio::test(start_paused = true)]
async fn a_slow_publisher_never_hoards_more_than_max_in_flight_leases() {
    let store = InMemoryOutboxStore::default();
    for _ in 0..TOTAL_ROWS {
        store.insert(common::distinct_envelope());
    }
    // Never resolves within this test's window — every claimed row stays outstanding.
    let publisher = ScriptedPublisher::always(PublishStep::Hang(Duration::from_secs(3600)));

    let settings = common::fast_dispatcher_settings()
        .batch_size(100)
        .max_in_flight(MAX_IN_FLIGHT);
    let dispatcher_a = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel_a = CancellationToken::new();
    let handle_a = tokio::spawn(dispatcher_a.run(cancel_a.clone()));

    // Give worker A several poll cycles' worth of time — if the claim loop lacked backpressure
    // it would have re-claimed the whole backlog by now.
    for _ in 0..10 {
        common::advance_and_settle(Duration::from_millis(10)).await;
    }

    // Probe with a second worker at a huge batch size: whatever is still claimable is exactly
    // `TOTAL_ROWS - MAX_IN_FLIGHT`, proving worker A holds no more than its `max_in_flight` ceiling.
    let probe = OutboxStore::acquire(
        &store,
        AcquireRequest::new(WorkerId::generate()).batch_size(1000),
    )
    .await
    .expect("acquire never fails for this fake");
    assert_eq!(
        probe.records.len(),
        TOTAL_ROWS - MAX_IN_FLIGHT,
        "worker A must hold exactly max_in_flight rows leased, no matter how slow its publisher is"
    );
    // Hand the probed rows back — a second, real dispatcher claims them below instead.
    let refs: Vec<_> = probe
        .records
        .iter()
        .map(reliar_outbox::OutboxRecord::message_ref)
        .collect();
    store
        .release(
            &probe.records[0].locked_by.clone().expect("just claimed"),
            &refs,
        )
        .await
        .expect("release never fails for this fake");

    // A second dispatcher, with a normal publisher, claims and publishes the rest.
    let publisher_b = RecordingPublisher::default();
    let dispatcher_b = OutboxDispatcher::builder(store.clone(), publisher_b.clone())
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");
    let cancel_b = CancellationToken::new();
    let handle_b = tokio::spawn(dispatcher_b.run(cancel_b.clone()));
    common::advance_and_settle(Duration::from_millis(50)).await;

    assert_eq!(
        publisher_b.published().len(),
        TOTAL_ROWS - MAX_IN_FLIGHT,
        "the second dispatcher publishes everything the first never touched"
    );

    cancel_a.cancel();
    let _ = handle_a.await;
    cancel_b.cancel();
    handle_b
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}
