//! Every dispatcher counter/gauge flows through the `OutboxMetrics` hook, driven by the
//! dispatcher's own `stats_interval` tick; with no claimable rows the dispatcher **skips**
//! `oldest_pending_age` entirely rather than reporting `Duration::ZERO`, and reports it once a
//! row genuinely is pending (§43.A.25, SRS §33.1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DeadReason, FailureKind, InMemoryOutboxStore, OutboxDispatcher, PublishStep, RecordingMetrics,
    ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn every_hook_is_observed_across_a_mixed_batch() {
    let store = InMemoryOutboxStore::default();
    let ok_row = store.insert(common::distinct_envelope());
    let retried_row = store.insert(common::distinct_envelope());
    let dead_row = store.insert(common::distinct_envelope());

    let publisher = ScriptedPublisher::keyed([
        (ok_row.id, PublishStep::Ok),
        (retried_row.id, PublishStep::Transient),
        (dead_row.id, PublishStep::Permanent),
    ]);
    let metrics = RecordingMetrics::default();

    let settings = common::fast_dispatcher_settings().stats_interval(Duration::from_millis(5));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .metrics(metrics.clone())
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

    assert_eq!(
        metrics.claimed(),
        3,
        "one acquire claimed all three seeded rows"
    );
    assert_eq!(metrics.published().len(), 1);
    assert_eq!(metrics.retried(), vec![FailureKind::Transient]);
    assert_eq!(metrics.dead(), vec![DeadReason::PermanentError]);
    assert!(
        metrics.pending().is_some(),
        "the stats tick fed the pending gauge"
    );
    assert!(metrics.expired_pending().is_some());
    assert!(
        metrics.publish_duration().is_some(),
        "every publish attempt reports its wall-clock duration"
    );
}

#[tokio::test(start_paused = true)]
async fn oldest_pending_age_is_skipped_when_nothing_is_pending() {
    let store = InMemoryOutboxStore::default();
    let publisher = ScriptedPublisher::always(PublishStep::Ok);
    let metrics = RecordingMetrics::default();

    let settings = common::fast_dispatcher_settings().stats_interval(Duration::from_millis(5));
    let dispatcher = OutboxDispatcher::builder(store, publisher)
        .settings(settings)
        .metrics(metrics.clone())
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

    assert_eq!(
        metrics.pending(),
        Some(0),
        "the gauge still reports zero, not silence"
    );
    assert_eq!(metrics.expired_pending(), Some(0));
    assert_eq!(
        metrics.oldest_pending_age(),
        None,
        "an empty backlog is skipped entirely, never reported as Duration::ZERO"
    );
}

#[tokio::test(start_paused = true)]
async fn oldest_pending_age_reports_a_real_pending_row() {
    let store = InMemoryOutboxStore::default();
    // `batch_size(1)`: the first poll claims only `claimed_first`, leaving `still_pending`
    // genuinely claimable-but-unclaimed when the stats tick fires shortly after.
    let claimed_first = store.insert(common::distinct_envelope());
    let still_pending = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::always(PublishStep::Ok);
    let metrics = RecordingMetrics::default();

    let settings = common::fast_dispatcher_settings()
        .batch_size(1)
        .poll_interval(Duration::from_millis(50))
        .stats_interval(Duration::from_millis(5));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .metrics(metrics.clone())
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Past the first stats tick (5 ms) but before the second poll (50 ms) would claim
    // `still_pending` too.
    common::advance_and_settle(Duration::from_millis(10)).await;
    assert_eq!(
        metrics.pending(),
        Some(1),
        "still_pending is claimable but has not been claimed yet"
    );
    assert_eq!(
        metrics.oldest_pending_age(),
        Some(Duration::ZERO),
        "a genuinely pending row reports an age, never skipped"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
    assert!(
        store
            .record(claimed_first.id)
            .and_then(|r| r.published_at)
            .is_some(),
        "sanity: claimed_first was processed before the probe"
    );
    let _ = still_pending;
}
