//! End-to-end `OutboxDispatcher` (`reliar-outbox`) driving `PostgresOutboxStore` against a real
//! Postgres: publish success drains the queue and releases every lease, a permanent failure
//! reaches dead, and (§43.A.11 `pg` half, review 3 M5) a crash after an observed publish but
//! before its outcome is persisted produces the documented duplicate window. Bounded polling
//! assertions, never a blind sleep — the dispatcher's own loop runs on real wall-clock time (it
//! drives a real socket), so a test waits for its effect to appear rather than guessing how
//! long that takes.

use crate::common;

use std::future::Future;
use std::time::Duration;

use reliar_outbox::{
    DeadQuery, ExponentialBackoff, OutboxDeadLetters, OutboxDispatcher, PublishStep,
    RecordingPublisher, ScriptedPublisher,
};
use reliar_store_postgres::PostgresOutboxStore;
use tokio_util::sync::CancellationToken;

fn fast_settings(batch_size: u32) -> reliar_outbox::DispatcherSettings {
    reliar_outbox::DispatcherSettings::default()
        .batch_size(batch_size)
        .poll_interval(Duration::from_millis(20))
        .idle_poll_interval(Duration::from_millis(20))
        .lease(Duration::from_secs(30))
        .drain_timeout(Duration::from_secs(5))
}

/// Polls `f` every 20ms until it resolves `true` or `timeout` elapses, panicking on timeout —
/// the bounded, non-flaky alternative to a blind sleep for a condition driven by a real
/// background task.
async fn wait_until<F, Fut>(timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn dispatcher_drains_the_queue_and_releases_every_lease() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 5).await;

    let publisher = RecordingPublisher::default();
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(fast_settings(10))
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    wait_until(Duration::from_secs(5), || async {
        publisher.published().len() == 5
    })
    .await;

    cancel.cancel();
    handle.await.unwrap().unwrap();

    for envelope in &envelopes {
        assert_eq!(publisher.count(envelope.id), 1);
    }
    let locked: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE locked_by IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(locked, 0, "run() must release every lease before returning");
}

async fn a_permanently_failing_publisher_moves_every_row_to_dead() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    common::seed(&store, &pool, 3).await;

    let publisher = ScriptedPublisher::always(PublishStep::Permanent);
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(fast_settings(10))
        .retry_policy(ExponentialBackoff::default().max_attempts(1))
        .build()
        .unwrap();

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    wait_until(Duration::from_secs(5), || {
        let store = &store;
        async move {
            store
                .list_dead(DeadQuery::default())
                .await
                .unwrap()
                .records
                .len()
                == 3
        }
    })
    .await;

    cancel.cancel();
    handle.await.unwrap().unwrap();

    let page = store.list_dead(DeadQuery::default()).await.unwrap();
    assert_eq!(page.records.len(), 3);
    assert!(
        page.records
            .iter()
            .all(|r| r.dead_reason == Some(reliar_outbox::DeadReason::PermanentError))
    );
}

async fn crash_after_observed_publish_produces_the_documented_duplicate_window() {
    // §43.A.11 (`pg` half), review 3 M5 — a dispatcher that publishes a message and is then
    // killed *before* it can write `complete` back must not be mistaken for having lost the
    // message: at-least-once means the same message is published again once its lease lapses
    // and a second dispatcher claims it (SRS §22 — this is the documented duplicate window, not
    // a bug).
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 1).await;
    let id = envelopes[0].id;

    // The probe's delay holds `publish`'s future in flight (parked on a timer) after it has
    // already recorded the publish (on first poll) but before it resolves — the exact window
    // "observed but not yet persisted" needs. 200ms real time is ample: `wait_until` below
    // returns the instant `count(id) == 1` becomes true, well inside that window, and the abort
    // happens immediately after with no further await in between — `complete` cannot have run.
    let publisher = RecordingPublisher::with_concurrency_probe(Duration::from_millis(200));
    let dispatcher_a = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(fast_settings(1))
        .build()
        .unwrap();

    let cancel_a = CancellationToken::new();
    let handle_a = tokio::spawn(dispatcher_a.run(cancel_a.clone()));

    wait_until(Duration::from_secs(5), || async {
        publisher.count(id) == 1
    })
    .await;

    // The crash: abort dispatcher A's task while its one publish is still in flight, so its
    // `complete` never runs and the row's lease is never cleared by it.
    handle_a.abort();
    let _ = handle_a.await;

    // SQL time-travel, not a wall-clock wait: makes the still-leased row claimable again,
    // standing in for the lease simply lapsing on its own.
    common::expire_lease(&pool, id.as_uuid()).await;

    let dispatcher_b = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(fast_settings(1))
        .build()
        .unwrap();
    let cancel_b = CancellationToken::new();
    let handle_b = tokio::spawn(dispatcher_b.run(cancel_b.clone()));

    wait_until(Duration::from_secs(5), || async {
        publisher.count(id) == 2
    })
    .await;

    cancel_b.cancel();
    handle_b.await.unwrap().unwrap();

    assert_eq!(
        publisher.count(id),
        2,
        "at-least-once over real Postgres: the crashed-then-reclaimed message is published twice"
    );
    let published_at_is_set: bool =
        sqlx::query_scalar("SELECT published_at IS NOT NULL FROM outbox WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        published_at_is_set,
        "dispatcher B's successful publish must still have completed the row"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_dispatcher_end_to_end::dispatcher_drains_the_queue_and_releases_every_lease",
            move || {
                rt.block_on(dispatcher_drains_the_queue_and_releases_every_lease());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_dispatcher_end_to_end::a_permanently_failing_publisher_moves_every_row_to_dead",
            move || {
                rt.block_on(a_permanently_failing_publisher_moves_every_row_to_dead());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_dispatcher_end_to_end::crash_after_observed_publish_produces_the_documented_duplicate_window",
            move || {
                rt.block_on(
                    crash_after_observed_publish_produces_the_documented_duplicate_window(),
                );
                Ok(())
            },
        ),
    ]
}
