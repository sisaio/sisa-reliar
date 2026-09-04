//! The whole claim → publish → complete path, driven only by the `test-support` fakes — no
//! broker, no database (§43.A.27).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn claimed_rows_are_published_and_marked_complete() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let seeded = store.insert(common::distinct_envelope());

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    common::advance_and_settle(Duration::from_millis(20)).await;

    assert_eq!(publisher.count(seeded.id), 1);
    let record = store.record(seeded.id).expect("row still exists");
    assert!(record.published_at.is_some(), "complete() should have run");
    assert!(record.locked_by.is_none(), "complete() clears the lease");
    assert_eq!(record.attempts, 1);

    cancel.cancel();
    let outcome = handle.await.expect("dispatcher task joins");
    assert!(outcome.is_ok(), "run() returns Ok(()) after cancellation");
}
