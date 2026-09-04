//! Two closely related rulings, proven together because they share one setup: a `complete` that
//! hangs forever.
//!
//! - **K4**: `store_timeout` bounds every `OutboxStore` call, so a permanently hung `complete`
//!   does not make `drain_timeout` unenforceable — `run` still returns within a bounded time.
//! - **L3**: a row whose publish **succeeded** but whose `complete` never lands is **left to its
//!   lease** at drain, not released — releasing it would turn a possible duplicate into a
//!   certain one for a message already delivered (SRS §23.2, ADR-lite L3).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn a_permanently_hung_complete_still_lets_drain_finish_and_leaves_the_row_leased() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = RecordingPublisher::default();

    // Every `complete` call for the foreseeable future hangs far longer than either timeout.
    store.hang_next(1_000, Duration::from_secs(3600));

    let settings = common::fast_dispatcher_settings()
        .store_timeout(Duration::from_millis(20))
        .drain_timeout(Duration::from_millis(50));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Let the publish succeed and the first (doomed) `complete` attempt begin.
    common::advance_and_settle(Duration::from_millis(10)).await;
    assert_eq!(publisher.count(seeded.id), 1);
    cancel.cancel();

    // Generously past `drain_timeout` (50 ms) plus one more `store_timeout` (20 ms) for the
    // drain's own final flush attempt — if `store_timeout` did not bound `complete`, this would
    // not be enough and `handle.await` below would hang.
    common::advance_and_settle(Duration::from_millis(200)).await;

    let outcome = handle.await.expect("dispatcher task joins");
    assert!(
        outcome.is_ok(),
        "a hung complete() does not turn shutdown into an error — it is a transient store failure"
    );

    let record = store.record(seeded.id).expect("row still exists");
    assert!(
        record.published_at.is_none(),
        "the write never actually landed — the mutation lives inside the hung future, which \
         `bounded()` dropped before it ever ran"
    );
    assert!(
        record.locked_by.is_some(),
        "a succeeded-but-uncompleted row is left to its lease, not released — releasing it would \
         be a certain duplicate for a message already delivered (L3)"
    );
}
