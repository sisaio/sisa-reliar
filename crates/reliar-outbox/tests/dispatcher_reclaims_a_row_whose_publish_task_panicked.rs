//! A publish task that panics must not leave its row's lease renewed forever: `outstanding` is
//! keyed by the task's own id, so a `JoinError` removes the row from lease-renewal bookkeeping
//! just like a clean resolution does, and the row becomes reclaimable once its lease expires
//! (S4 review, blocker 2).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, InMemoryOutboxStore, OutboxDispatcher, OutboxStore, PublishStep,
    ScriptedPublisher, WorkerId,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_panicking_publish_task_does_not_renew_its_row_forever() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::keyed([(seeded.id, PublishStep::Panic)]);

    let lease = Duration::from_secs(30);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // The row is claimed and its publish task panics; the lease-renewal ticker (every 15 s for
    // the default 30 s lease) gets at least one chance to fire before we check.
    common::advance_and_settle(Duration::from_secs(16)).await;

    // The lease expires: if the panicked task's row were still being renewed, this probe by a
    // different worker would find nothing.
    store.advance(lease + Duration::from_secs(1));
    let probe = OutboxStore::acquire(
        &store,
        AcquireRequest::new(WorkerId::generate()).lease(lease),
    )
    .await
    .expect("acquire never fails for this fake");
    assert_eq!(
        probe.records.len(),
        1,
        "the row is reclaimable — its lease was never renewed after the panic"
    );
    assert_eq!(probe.records[0].envelope.id, seeded.id);

    cancel.cancel();
    let outcome = handle
        .await
        .expect("dispatcher task joins (the panic was in a spawned subtask, not run() itself)");
    assert!(
        outcome.is_ok(),
        "a panicking publish task does not end run() itself"
    );
}
