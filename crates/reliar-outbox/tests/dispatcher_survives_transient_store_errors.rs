//! `run` survives a transient store error: it logs, backs off by `idle_poll_interval`, and
//! keeps going, ending only on cancellation (§43.A.18, SRS §26.1, ADR 0014).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_transient_store_error_is_logged_and_the_loop_keeps_going() {
    let store = InMemoryOutboxStore::default();
    let publisher = RecordingPublisher::default();
    let seeded = store.insert(common::distinct_envelope());

    // The very first `acquire` call fails; `InMemoryStoreError` always self-classifies
    // `Transient` (test-support), so `run` must log it, back off, and try again rather than
    // returning `Err`.
    store.fail_next(1);

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(common::fast_dispatcher_settings())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // One injected failure, then the idle backoff, then a successful claim + publish.
    common::advance_and_settle(Duration::from_millis(50)).await;

    assert_eq!(
        publisher.count(seeded.id),
        1,
        "the loop recovered from the injected store error and published the row"
    );

    cancel.cancel();
    let outcome = handle.await.expect("dispatcher task joins");
    assert!(
        outcome.is_ok(),
        "run() only ever ends via cancellation for a transient store error"
    );
}
