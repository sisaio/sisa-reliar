//! An [`OutboxPublisher`] is accepted as an [`OutboxDispatcher`]'s own publisher — legal because
//! its `Publisher::publish` has no path back to `S`, so the outbox cannot drain into itself
//! (ADR 0036 §2, contract §2.2, E7).

#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, OutboxPublisher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn wiring_an_outbox_publisher_as_the_dispatchers_publisher_still_drains_and_never_grows_the_store()
 {
    // One shared store plays both roles: the dispatcher's claim/complete side, and the
    // `OutboxPublisher`'s enqueue capability. If a publish ever reached `enqueue`, this store
    // would see its own dispatcher's output grow its own input — the cycle ADR 0036 §2 makes
    // unrepresentable.
    let store = InMemoryOutboxStore::default();
    let transport = RecordingPublisher::default();
    let outbox = OutboxPublisher::new(store.clone(), transport.clone());

    let seeded: Vec<_> = (0..3)
        .map(|_| store.insert(common::distinct_envelope()))
        .collect();
    let seeded_row_count = store.records().len();

    let dispatcher = OutboxDispatcher::builder(store.clone(), outbox)
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));
    common::advance_and_settle(Duration::from_millis(30)).await;
    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");

    for row in &seeded {
        assert_eq!(
            transport.count(row.id),
            1,
            "every seeded row reached the transport"
        );
    }
    assert_eq!(
        store.records().len(),
        seeded_row_count,
        "the store's row count never grew — nothing looped back through enqueue"
    );
    assert_eq!(
        store.enqueue_call_count(),
        0,
        "OutboxEnqueue::enqueue was never called on the publish path"
    );
}
