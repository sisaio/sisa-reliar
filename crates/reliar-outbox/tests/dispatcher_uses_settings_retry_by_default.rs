//! A dispatcher built without ever calling `.retry_policy()` uses `DispatcherSettings::retry` as
//! its actual backoff — the `DefaultRetry` path (K1, S4 review 2/3; ADR-lite K1).
#![cfg(feature = "test-support")]

mod common;

use std::time::Duration;

use reliar_outbox::{
    DispatcherSettings, ExponentialBackoff, InMemoryOutboxStore, OutboxDispatcher, PublishStep,
    ScriptedPublisher,
};
use tokio_util::sync::CancellationToken;

#[tokio::test(start_paused = true)]
async fn settings_retry_drives_the_default_backoff_with_no_retry_policy_call() {
    let store = InMemoryOutboxStore::default();
    let seeded = store.insert(common::distinct_envelope());
    let publisher = ScriptedPublisher::keyed([(seeded.id, PublishStep::Transient)]);

    // A distinctive, non-default base delay — if this were ignored (the bug K1 fixes), the row
    // would instead follow `ExponentialBackoff::default()`'s 1 s base.
    let custom_base = Duration::from_secs(7);
    let settings = common::fast_dispatcher_settings()
        .retry(ExponentialBackoff::default().base(custom_base).jitter(0.0));

    // No `.retry_policy(..)` call anywhere — the builder stays on `DefaultRetry`.
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .build()
        .expect("valid settings");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));
    common::advance_and_settle(Duration::from_millis(20)).await;

    let record = store.record(seeded.id).expect("row still exists");
    assert_eq!(record.attempts, 1);
    assert_eq!(
        (record.available_at - seeded.created_at).unsigned_abs(),
        custom_base,
        "settings.retry's base delay drove the schedule, not the ExponentialBackoff default"
    );

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task joins")
        .expect("run() returns Ok(()) after cancellation");
}

#[tokio::test(start_paused = true)]
async fn a_default_dispatcher_still_builds_with_no_retry_policy_call() {
    // Sanity: `OutboxDispatcher::builder(..).settings(..).build()` with the untouched
    // `DispatcherSettings::default()` — the everyday case — must still succeed.
    let store = InMemoryOutboxStore::default();
    let publisher = ScriptedPublisher::always(PublishStep::Ok);
    let dispatcher = OutboxDispatcher::builder(store, publisher)
        .settings(DispatcherSettings::default())
        .build();
    assert!(dispatcher.is_ok());
}
