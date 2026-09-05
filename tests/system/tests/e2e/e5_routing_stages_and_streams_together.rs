//! E5 (RELIAR-45, contract `docs/architecture/routing-publisher-contract.md` §10 R15; SRS §43.D
//! D8's first half): the "only these" rollout shape — `allowed_types = [orders.created]`,
//! `disallowed_types = []` — proved end to end against a real Postgres and a real `JetStream`
//! stream, with both provider crates in play through [`reliar_outbox::OutboxPublisher`] /
//! [`reliar_outbox::ScopedOutboxPublisher`].
//!
//! `a_is_staged_then_drained_while_c_never_touches_the_outbox` publishes a routed type (`a`,
//! `orders.created`) inside a committed host transaction — it lands in `outbox` and only reaches
//! the stream once a running [`reliar_outbox::OutboxDispatcher`] drains it — and a non-routed type
//! (`c`, `audit.logged`) — it reaches the stream immediately and **never** appears as a row in
//! `outbox`, asserted by an exact `count(*)`, not merely "still pending".
//!
//! `route_independent_bytes_a_direct_duplicate_matches_the_routed_wire_bytes` is the
//! route-independent-bytes guarantee the contract's §4.2 documents: the caller serializes once
//! regardless of path, so a second envelope with the identical body, published **directly**
//! through a publisher whose policy sends `orders.created` straight to the transport, must produce
//! the exact same raw payload bytes on the wire as the routed envelope's — proving the route
//! taken never changes what a subscriber receives.

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

#[allow(
    clippy::too_many_lines,
    reason = "one ordered narrative — c direct then a staged and drained, both asserted against \
              the stream and against outbox — splitting it would scatter the ordering the test \
              depends on across helper functions with no reuse"
)]
async fn a_is_staged_then_drained_while_c_never_touches_the_outbox() {
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

    // The "only these" rollout shape: only `orders.created` is routed; everything else,
    // including `audit.logged`, publishes directly.
    let settings = OutboxSettings::default()
        .allowed_types(
            MessageTypeNames::try_from_iter("allowed_types", ["orders.created"])
                .expect("valid name"),
        )
        .expect("disjoint from the empty disallow list");
    let policy = OutboxPolicy::from_settings(&settings).expect("valid settings");
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);

    // `c` — not in `allowed_types` — publishes directly, reaching the stream before anything else
    // in this test runs a dispatcher.
    let envelope_c = common::serialize(
        Envelope::builder(AuditLogged {
            event: "signed_in".to_string(),
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
    tx_c.commit().await.expect("commit tx for c");

    assert_eq!(
        common::row_count_for(&pool, envelope_c.id).await,
        0,
        "a directly published type must never be staged in outbox"
    );
    common::wait_until(Duration::from_secs(10), || {
        let context = context.clone();
        let stream_name = stream_name.clone();
        async move { common::stream_message_count(&context, &stream_name).await >= 1 }
    })
    .await;

    // `a` — in `allowed_types` — is staged inside the caller's transaction.
    let envelope_a = common::serialize(Envelope::builder(OrderCreated { order_id: 1 }).build());
    assert!(
        outbox.policy().decide(&envelope_a.message_type).is_outbox(),
        "a must be routed through the outbox"
    );
    let mut tx_a = pool.begin().await.expect("begin tx for a");
    outbox
        .in_transaction(&mut tx_a)
        .publish(&envelope_a)
        .await
        .expect("publish a");
    tx_a.commit().await.expect("commit tx for a");

    assert_eq!(
        common::row_count_for(&pool, envelope_a.id).await,
        1,
        "a routed type must be staged as exactly one row"
    );
    assert!(
        !common::is_published(&pool, envelope_a.id).await,
        "nothing publishes a staged row before a dispatcher runs"
    );

    // Only now does a dispatcher exist to drain `a`.
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
        async move { common::stream_message_count(&context, &stream_name).await >= 2 }
    })
    .await;

    // Sequence 1 is `c` (published directly before the dispatcher existed); sequence 2 is `a`
    // (only reachable once the dispatcher claimed and published its row) — a deterministic order
    // by construction, not a race.
    let seq1 = common::stream_raw_message(&context, &stream_name, 1).await;
    assert_eq!(
        common::header_value(&seq1.headers, headers::MESSAGE_ID),
        envelope_c.id.to_string()
    );
    assert_eq!(
        common::header_value(&seq1.headers, headers::MESSAGE_TYPE),
        "audit.logged"
    );

    let seq2 = common::stream_raw_message(&context, &stream_name, 2).await;
    assert_eq!(
        common::header_value(&seq2.headers, headers::MESSAGE_ID),
        envelope_a.id.to_string()
    );
    assert_eq!(
        common::header_value(&seq2.headers, headers::MESSAGE_TYPE),
        "orders.created"
    );
    assert_eq!(
        seq2.payload, envelope_a.body,
        "the routed message's wire body must match the enqueued envelope byte for byte"
    );

    common::delete_stream(&context, &stream_name).await;
}

#[allow(
    clippy::too_many_lines,
    reason = "one ordered narrative — a routed and drained, then a2 published directly through a \
              second policy, then both wire messages compared — splitting it would scatter the \
              ordering the byte-identical assertion depends on across helper functions with no \
              reuse"
)]
async fn route_independent_bytes_a_direct_duplicate_matches_the_routed_wire_bytes() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e5b.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E5B_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix.clone()),
    )
    .expect("valid settings");

    // Outbox A: `orders.created` is routed (the durable default — an empty allow list).
    let outbox_routed =
        OutboxPublisher::new(store.clone(), publisher.clone(), OutboxPolicy::default());
    let envelope_a = common::serialize(Envelope::builder(OrderCreated { order_id: 42 }).build());
    let mut tx = pool.begin().await.expect("begin tx");
    assert!(
        outbox_routed
            .policy()
            .decide(&envelope_a.message_type)
            .is_outbox()
    );
    outbox_routed
        .in_transaction(&mut tx)
        .publish(&envelope_a)
        .await
        .expect("publish a");
    tx.commit().await.expect("commit tx");

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

    // Outbox B: same store, same publisher — but a policy that sends `orders.created`
    // **directly**. The "hypothetical direct `a`" the contract's byte-identical guarantee is
    // about. No transaction is involved on this path, so `publish_direct` is the right call.
    let direct_settings = OutboxSettings::default()
        .disallowed_types(
            MessageTypeNames::try_from_iter("disallowed_types", ["orders.created"])
                .expect("valid name"),
        )
        .expect("disjoint from the empty allow list");
    let direct_policy = OutboxPolicy::from_settings(&direct_settings).expect("valid settings");
    let outbox_direct = OutboxPublisher::new(store.clone(), publisher.clone(), direct_policy);
    let envelope_a2 = common::serialize(Envelope::builder(OrderCreated { order_id: 42 }).build());
    assert!(
        !outbox_direct
            .policy()
            .decide(&envelope_a2.message_type)
            .is_outbox(),
        "a2 must publish directly"
    );
    outbox_direct
        .publish_direct(&envelope_a2)
        .await
        .expect("publish a2 directly");

    common::wait_until(Duration::from_secs(10), || {
        let context = context.clone();
        let stream_name = stream_name.clone();
        async move { common::stream_message_count(&context, &stream_name).await >= 2 }
    })
    .await;

    let routed_message = common::stream_raw_message(&context, &stream_name, 1).await;
    let direct_message = common::stream_raw_message(&context, &stream_name, 2).await;

    assert_ne!(
        common::header_value(&routed_message.headers, headers::MESSAGE_ID),
        common::header_value(&direct_message.headers, headers::MESSAGE_ID),
        "two distinct envelopes, so two distinct ids"
    );
    // The header projection carried by both routes agrees on everything but the id.
    assert_eq!(
        common::header_value(&routed_message.headers, headers::MESSAGE_TYPE),
        common::header_value(&direct_message.headers, headers::MESSAGE_TYPE)
    );
    assert_eq!(
        common::header_value(&routed_message.headers, headers::MESSAGE_VERSION),
        common::header_value(&direct_message.headers, headers::MESSAGE_VERSION)
    );
    assert_eq!(
        common::header_value(&routed_message.headers, headers::CONTENT_TYPE),
        common::header_value(&direct_message.headers, headers::CONTENT_TYPE)
    );
    assert_eq!(
        routed_message.payload, direct_message.payload,
        "the same body serialized on either route must be byte-identical on the wire"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "e5_routing_stages_and_streams_together::a_is_staged_then_drained_while_c_never_touches_the_outbox",
            move || {
                rt.block_on(a_is_staged_then_drained_while_c_never_touches_the_outbox());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "e5_routing_stages_and_streams_together::route_independent_bytes_a_direct_duplicate_matches_the_routed_wire_bytes",
            move || {
                rt.block_on(
                    route_independent_bytes_a_direct_duplicate_matches_the_routed_wire_bytes(),
                );
                Ok(())
            },
        ),
    ]
}
