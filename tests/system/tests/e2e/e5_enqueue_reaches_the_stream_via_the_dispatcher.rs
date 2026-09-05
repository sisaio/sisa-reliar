//! E20 (outbox-publisher contract `docs/architecture/outbox-publisher-contract.md` §9, §10;
//! SRS §43.D D7): an envelope enqueued through [`reliar_outbox::OutboxPublisher::enqueue`] lands in
//! `outbox` and only reaches a real `JetStream` stream once a running
//! [`reliar_outbox::OutboxDispatcher`] drains it — proved end to end against a real Postgres and a
//! real `JetStream` stream, both provider crates in play.
//!
//! Replaces `e5_routing_stages_and_streams_together.rs` (withdrawn with ADR 0036).

use std::time::Duration;

use reliar_core::Envelope;
use reliar_outbox::{OutboxDispatcher, OutboxPublisher};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings, headers};
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::OrderCreated;

async fn enqueued_lands_in_outbox_and_reaches_the_stream_via_the_dispatcher() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e5.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E5_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    let outbox = OutboxPublisher::new(store.clone(), publisher.clone());

    let envelope = common::serialize(Envelope::builder(OrderCreated { order_id: 1 }).build());
    let mut tx = pool.begin().await.expect("begin tx");
    outbox
        .enqueue(&mut tx, &envelope)
        .await
        .expect("enqueue the envelope");
    tx.commit().await.expect("commit tx");

    assert_eq!(
        common::row_count_for(&pool, envelope.id).await,
        1,
        "enqueue must write exactly one row"
    );
    assert!(
        !common::is_published(&pool, envelope.id).await,
        "nothing publishes an enqueued row before a dispatcher runs"
    );

    // Only now does a dispatcher exist to drain it.
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(common::fast_settings())
        .build()
        .expect("dispatcher config");
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::is_published(&pool, envelope.id).await }
    })
    .await;
    cancel.cancel();
    handle
        .await
        .expect("dispatcher task did not panic")
        .expect("dispatcher run returned Ok");

    common::wait_until(Duration::from_secs(10), || {
        let context = context.clone();
        let stream_name = stream_name.clone();
        async move { common::stream_message_count(&context, &stream_name).await >= 1 }
    })
    .await;

    let message = common::stream_raw_message(&context, &stream_name, 1).await;
    assert_eq!(
        common::header_value(&message.headers, headers::MESSAGE_ID),
        envelope.id.to_string()
    );
    assert_eq!(message.payload, envelope.body);

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "e5_enqueue_reaches_the_stream_via_the_dispatcher::enqueued_lands_in_outbox_and_reaches_the_stream_via_the_dispatcher",
        move || {
            rt.block_on(enqueued_lands_in_outbox_and_reaches_the_stream_via_the_dispatcher());
            Ok(())
        },
    )]
}
