//! E3 (RELIAR-34, PO addendum 2026-09-05, SRS §22's headline proven end to end): a worker whose
//! publish reaches `JetStream` and is acked, but which never writes its own `complete` — modelling
//! a crash in exactly the window between the two — has its lease reclaimed by another worker,
//! which republishes the same envelope. `Nats-Msg-Id` (the message id, ADR 0026 §5) makes the
//! second publish a JetStream-recognized duplicate of the first inside the stream's
//! `duplicate_window` — set explicitly here (120s) rather than left at the server default, since
//! that window is exactly the mechanic under test — so the stream still holds **one** copy even
//! though two distinct publishes reached the wire — narrowing, not closing, the outbox's own
//! at-least-once duplicate window (`docs/guides/nats.md`).
//!
//! Unlike `reliar-store-postgres`'s own `crash_after_observed_publish_produces_the_documented_
//! duplicate_window` (which has no broker and asserts two publishes, since nothing there
//! deduplicates), this scenario is the one place both the crash *and* the broker-side dedup are
//! exercised together. Worker A's claim, publish and (deliberately omitted) `complete` are driven
//! directly against the public `OutboxStore`/`Publisher` APIs rather than through a dispatcher
//! instance, so there is no timing race to win — a real dispatcher only enters the picture to
//! reclaim and republish after the lease has already expired.

use std::time::Duration;

use reliar_core::Publisher;
use reliar_outbox::{AcquireRequest, OutboxDispatcher, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings};
use tokio_util::sync::CancellationToken;

use crate::common;

async fn crash_after_publish_reclaims_and_republishes_with_one_stream_copy() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");
    let envelopes = common::seed(&store, &pool, 1).await;
    let id = envelopes[0].id;

    let context = common::jetstream_context().await;
    let uid = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e3.{uid}");
    let stream_name = format!("RELIAR_SYSTEST_E3_{uid}");
    // The window under test, set explicitly rather than left at the server default (currently
    // two minutes): the reclaim below must land well inside it for the dedup assertion to mean
    // anything, and this scenario should not depend on a default it never reads back.
    common::create_stream_with_duplicate_window(
        &context,
        &stream_name,
        &format!("{prefix}.>"),
        Duration::from_secs(120),
    )
    .await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    // Worker A: claims the row directly, publishes it for real — the ack lands and the stream
    // holds one copy — then deliberately never writes `complete`. This *is* the crash SRS §22
    // documents: an outcome observed at the broker but never persisted.
    let worker_a = WorkerId::generate();
    let claimed = store
        .acquire(AcquireRequest::new(worker_a.clone()).lease(Duration::from_secs(30)))
        .await
        .expect("acquire the seeded row")
        .records;
    assert_eq!(claimed.len(), 1, "the one seeded row must be claimable");
    publisher
        .publish(&claimed[0].envelope)
        .await
        .expect("worker A's publish is acked by the stream");

    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        1,
        "worker A's publish must have reached the stream before the simulated crash"
    );
    assert!(
        !common::is_published(&pool, id).await,
        "worker A must never have written its own complete — that is the crash being modelled"
    );

    // The crash: worker A's lease expires without it ever completing the row (SQL time-travel,
    // not a wall-clock wait — mirrors reliar-store-postgres's own crash-after-publish test).
    common::expire_lease(&pool, id.as_uuid()).await;

    // A real dispatcher now reclaims the row — the lease is gone, so `acquire` returns it again —
    // and republishes it through the same `NatsPublisher`: same message id, same `Nats-Msg-Id`.
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(common::fast_settings())
        .build()
        .expect("dispatcher config");
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::is_published(&pool, id).await }
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
        "JetStream's Nats-Msg-Id dedup inside duplicate_window must suppress the reclaiming \
         worker's republish — the stream holds one copy despite two publishes reaching the wire"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "e3_crash_after_publish_dedupes_on_the_stream::crash_after_publish_reclaims_and_republishes_with_one_stream_copy",
        move || {
            rt.block_on(crash_after_publish_reclaims_and_republishes_with_one_stream_copy());
            Ok(())
        },
    )]
}
