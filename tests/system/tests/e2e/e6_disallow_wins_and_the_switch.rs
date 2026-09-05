//! E6 (RELIAR-45, contract `docs/architecture/routing-publisher-contract.md` §10 R16; SRS §43.D
//! D8's second half): the "everything except" rollout shape, the `enabled = false` switch, and
//! the two honesty guarantees around a caller's transaction — proved end to end against a real
//! Postgres and a real `JetStream` stream, with [`reliar_outbox::OutboxPublisher`] /
//! [`reliar_outbox::ScopedOutboxPublisher`] as the call site.
//!
//! `everything_except_c_routes_a_and_the_direct_publish_survives_a_rollback` is the primary
//! rollout shape (`allowed_types = []`, `disallowed_types = [audit.logged]`) **and** the honesty
//! test the contract names explicitly so nobody "fixes" it: a directly published envelope is not
//! part of the caller's transaction, so rolling that transaction back still leaves the message on
//! the stream.
//!
//! `disabling_routing_sends_even_an_otherwise_routed_type_direct` proves `enabled = false`
//! overrides both lists (SRS §43.D D1): a type that would otherwise be staged publishes directly
//! and never touches `outbox`.
//!
//! `a_rolled_back_routed_publish_never_reaches_the_stream` is the ordinary, expected half of the
//! same transaction boundary: a routed type whose transaction never commits leaves no row and,
//! having never been staged, nothing for any dispatcher to ever publish.

use std::time::Duration;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::{
    MessageTypeNames, OutboxDispatcher, OutboxPolicy, OutboxPublisher, OutboxSettings,
};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings, headers};
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::{AuditLogged, OrderCreated};

async fn everything_except_c_routes_a_and_the_direct_publish_survives_a_rollback() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e6.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E6_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    // "Everything except `audit.logged`" — the primary rollout shape (an empty allow list plus a
    // one-name disallow list).
    let settings = OutboxSettings::default()
        .disallowed_types(
            MessageTypeNames::try_from_iter("disallowed_types", ["audit.logged"])
                .expect("valid name"),
        )
        .expect("disjoint from the empty allow list");
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);

    // `a` is still routed under this shape.
    let envelope_a = common::serialize(Envelope::builder(OrderCreated { order_id: 10 }).build());
    assert!(
        outbox.policy().decide(&envelope_a.message_type).is_outbox(),
        "a must still route through the outbox"
    );
    let mut tx_a = pool.begin().await.expect("begin tx for a");
    outbox
        .in_transaction(&mut tx_a)
        .publish(&envelope_a)
        .await
        .expect("publish a");
    tx_a.commit().await.expect("commit tx for a");
    assert_eq!(common::row_count_for(&pool, envelope_a.id).await, 1);

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher.clone())
        .settings(common::fast_settings())
        .build()
        .expect("dispatcher config");
    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));
    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::is_published(&pool, envelope_a.id).await }
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

    // `c` — `audit.logged`, disallowed — publishes directly, and the scoped view touches no
    // statement on `tx_c` at all: a rollback of the caller's transaction must **not** un-publish
    // it. This is the direct path's non-transactional guarantee, asserted rather than merely
    // documented.
    let envelope_c = common::serialize(
        Envelope::builder(AuditLogged {
            event: "password_reset".to_string(),
        })
        .build(),
    );
    assert!(
        !outbox.policy().decide(&envelope_c.message_type).is_outbox(),
        "c must be published directly"
    );
    let mut tx_c = pool.begin().await.expect("begin tx for c");
    outbox
        .in_transaction(&mut tx_c)
        .publish(&envelope_c)
        .await
        .expect("publish c");

    common::wait_until(Duration::from_secs(10), || {
        let context = context.clone();
        let stream_name = stream_name.clone();
        async move { common::stream_message_count(&context, &stream_name).await >= 2 }
    })
    .await;

    // The rollback: `c`'s message must still be on the stream afterward.
    tx_c.rollback().await.expect("roll back tx for c");

    assert_eq!(
        common::row_count_for(&pool, envelope_c.id).await,
        0,
        "a direct publish never touches outbox regardless of the transaction's outcome"
    );
    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        2,
        "rolling back the caller's transaction must not un-publish an already-direct message"
    );
    let seq2 = common::stream_raw_message(&context, &stream_name, 2).await;
    assert_eq!(
        common::header_value(&seq2.headers, headers::MESSAGE_ID),
        envelope_c.id.to_string(),
        "the rolled-back transaction's message is still the one on the stream"
    );

    common::delete_stream(&context, &stream_name).await;
}

async fn disabling_routing_sends_even_an_otherwise_routed_type_direct() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e6b.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E6B_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    // `enabled = false` overrides even a non-empty `allowed_types` naming this exact type — both
    // lists are ignored while disabled (SRS §43.D D1).
    let settings = OutboxSettings::default()
        .enabled(false)
        .allowed_types(
            MessageTypeNames::try_from_iter("allowed_types", ["orders.created"])
                .expect("valid name"),
        )
        .expect("disjoint from the empty disallow list");
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);

    let envelope_a = common::serialize(Envelope::builder(OrderCreated { order_id: 11 }).build());
    assert!(
        !outbox.policy().decide(&envelope_a.message_type).is_outbox(),
        "an otherwise-routed type must publish directly while routing is disabled"
    );
    let mut tx = pool.begin().await.expect("begin tx");
    outbox
        .in_transaction(&mut tx)
        .publish(&envelope_a)
        .await
        .expect("publish a");
    tx.commit().await.expect("commit tx");

    assert_eq!(
        common::row_count_for(&pool, envelope_a.id).await,
        0,
        "disabled routing must never stage a row, regardless of allowed_types"
    );
    common::wait_until(Duration::from_secs(10), || {
        let context = context.clone();
        let stream_name = stream_name.clone();
        async move { common::stream_message_count(&context, &stream_name).await >= 1 }
    })
    .await;
    let seq1 = common::stream_raw_message(&context, &stream_name, 1).await;
    assert_eq!(
        common::header_value(&seq1.headers, headers::MESSAGE_ID),
        envelope_a.id.to_string()
    );

    common::delete_stream(&context, &stream_name).await;
}

async fn a_rolled_back_routed_publish_never_reaches_the_stream() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e6c.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E6C_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    // The durable default: every type routes.
    let outbox = OutboxPublisher::new(store.clone(), publisher, OutboxPolicy::default());

    let envelope_a = common::serialize(Envelope::builder(OrderCreated { order_id: 12 }).build());
    assert!(outbox.policy().decide(&envelope_a.message_type).is_outbox());
    let mut tx = pool.begin().await.expect("begin tx");
    outbox
        .in_transaction(&mut tx)
        .publish(&envelope_a)
        .await
        .expect("publish a");
    tx.rollback().await.expect("roll back tx");

    assert_eq!(
        common::row_count_for(&pool, envelope_a.id).await,
        0,
        "a rolled-back routed publish must leave no row"
    );
    // Never staged, so no dispatcher — not even a future one — could ever have published it; a
    // bounded wait for a positive condition would be the wrong tool for a negative proof, and one
    // was never needed here: the row provably never existed.
    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        0,
        "nothing was ever staged, so the stream must stay empty"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "e6_disallow_wins_and_the_switch::everything_except_c_routes_a_and_the_direct_publish_survives_a_rollback",
            move || {
                rt.block_on(
                    everything_except_c_routes_a_and_the_direct_publish_survives_a_rollback(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "e6_disallow_wins_and_the_switch::disabling_routing_sends_even_an_otherwise_routed_type_direct",
            move || {
                rt.block_on(disabling_routing_sends_even_an_otherwise_routed_type_direct());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "e6_disallow_wins_and_the_switch::a_rolled_back_routed_publish_never_reaches_the_stream",
            move || {
                rt.block_on(a_rolled_back_routed_publish_never_reaches_the_stream());
                Ok(())
            },
        ),
    ]
}
