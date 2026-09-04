//! `DispatcherSettings::stats_interval = Duration::ZERO` disables the `stats()` tick entirely —
//! a host that wants no gauges (or polls `stats()` itself) pays nothing for it (SRS §33.1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{InMemoryOutboxStore, OutboxDispatcher, RecordingMetrics, RecordingPublisher};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn zero_stats_interval_never_ticks() {
    let store = InMemoryOutboxStore::default();
    store.insert(common::distinct_envelope());
    let metrics = RecordingMetrics::default();

    let settings = common::fast_dispatcher_settings().stats_interval(Duration::ZERO);
    let dispatcher = OutboxDispatcher::builder(store, RecordingPublisher::default())
        .settings(settings)
        .metrics(metrics.clone())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));
    common::advance_and_settle(Duration::from_millis(200)).await;
    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");

    assert_eq!(metrics.pending(), None, "stats() was never called");
    assert_eq!(metrics.expired_pending(), None);
    assert_eq!(metrics.oldest_pending_age(), None);
}
