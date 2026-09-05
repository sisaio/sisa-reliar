//! E2 (story C9, C8, contract §7, review round 1 minor 8): a stream deleted while the dispatcher
//! keeps running leaves the affected row retryable — `attempts` incremented, `published_at` still
//! `NULL`, `dead_at` still `NULL` — and its `last_error` identifies the transient failure without
//! ever printing the message body or the NATS connection URL. The row publishes once a stream
//! recapturing the same subject exists again. Proves `NatsPublisher`'s `StreamNotFound` transient
//! classification (ADR 0030) actually round-trips through a live dispatcher loop, not just a
//! single `publish` call.

use std::time::Duration;

use reliar_outbox::{DispatcherSettings, ExponentialBackoff, OutboxDispatcher};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::NatsPublisher;
use reliar_transport_nats::NatsSettings;
use tokio_util::sync::CancellationToken;

use crate::common;

fn fast_settings() -> DispatcherSettings {
    DispatcherSettings::default()
        .batch_size(10)
        .poll_interval(Duration::from_millis(20))
        .idle_poll_interval(Duration::from_millis(20))
        .lease(Duration::from_secs(30))
        .drain_timeout(Duration::from_secs(5))
        // Fast, deterministic, jitter-free backoff: the assertions only need "attempts >= 1
        // while unpublished" to become true quickly and stay true until the stream returns.
        .retry(
            ExponentialBackoff::default()
                .base(Duration::from_millis(50))
                .max_delay(Duration::from_millis(200))
                .jitter(0.0)
                .max_attempts(1000),
        )
}

async fn publish_recovers_after_the_stream_is_deleted_and_recreated_mid_run() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e2.{id}");
    let subject = format!("{prefix}.>");
    let stream_name = format!("RELIAR_SYSTEST_E2_{id}");
    common::create_stream(&context, &stream_name, &subject).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(fast_settings())
        .build()
        .expect("dispatcher config");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Prove the pipe works first, with the dispatcher already running: one envelope publishes
    // while the stream is present.
    let first = common::seed(&store, &pool, 1).await;
    let first_id = first[0].id;
    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::is_published(&pool, first_id).await }
    })
    .await;

    // The crash: the stream disappears while the dispatcher keeps running, mid-loop.
    common::delete_stream(&context, &stream_name).await;

    let second = common::seed(&store, &pool, 1).await;
    let second_id = second[0].id;

    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move {
            !common::is_published(&pool, second_id).await
                && common::attempts_for(&pool, second_id).await >= 1
        }
    })
    .await;

    assert!(
        !common::is_dead(&pool, second_id).await,
        "a transient StreamNotFound must never dead-letter the row"
    );
    let last_error = common::last_error_for(&pool, second_id)
        .await
        .expect("a failed attempt must record a last_error");
    assert!(
        last_error.contains("no stream is bound to subject"),
        "last_error must identify the StreamNotFound failure: {last_error:?}"
    );
    assert!(
        !last_error.contains("order_id"),
        "last_error must never carry the message body: {last_error:?}"
    );
    assert!(
        !last_error.contains("nats://"),
        "last_error must never carry the NATS connection URL: {last_error:?}"
    );

    // The stream returns, recapturing the exact same subject space — `NatsPublisher` never
    // learned the stream's identity, only the subject it resolves to (ADR 0029), so a
    // *recreated* stream is exactly as good as the original one from its point of view.
    common::create_stream(&context, &stream_name, &subject).await;

    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::is_published(&pool, second_id).await }
    })
    .await;

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task did not panic")
        .expect("dispatcher run returned Ok");

    assert_eq!(
        common::locked_row_count(&pool).await,
        0,
        "run() must release every lease before returning"
    );
    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        1,
        "only the recovered publish reaches the recreated stream — the failed attempts never did"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "e2_publish_recovers_after_stream_returns::publish_recovers_after_the_stream_is_deleted_and_recreated_mid_run",
        move || {
            rt.block_on(publish_recovers_after_the_stream_is_deleted_and_recreated_mid_run());
            Ok(())
        },
    )]
}
