//! E21/E22 (outbox-publisher contract `docs/architecture/outbox-publisher-contract.md` §9, §10;
//! SRS §43.D D7): [`reliar_core::Publisher::publish`] on an [`reliar_outbox::OutboxPublisher`]
//! bypasses the outbox entirely — it reaches the stream immediately and never appears as a row in
//! `outbox` (E21), and it is **not** part of any transaction the caller has open: rolling that
//! transaction back does not un-publish it (E22) — proved end to end against a real Postgres and a
//! real `JetStream` stream.
//!
//! Replaces `e6_disallow_wins_and_the_switch.rs` (withdrawn with ADR 0036).

use std::time::Duration;

use reliar_core::{Envelope, Publisher as _};
use reliar_outbox::OutboxPublisher;
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings, headers};

use crate::common;
use crate::common::AuditLogged;

/// E21 — a published envelope reaches the stream immediately and never appears in `outbox`,
/// asserted by an exact `count(*)`, not merely "still pending".
async fn published_reaches_the_stream_immediately_and_never_appears_in_outbox() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e6a.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E6A_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");
    let outbox = OutboxPublisher::new(store, publisher);

    let envelope = common::serialize(
        Envelope::builder(AuditLogged {
            event: "signed_in".to_string(),
        })
        .build(),
    );
    outbox.publish(&envelope).await.expect("publish");

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
    assert_eq!(
        common::row_count_for(&pool, envelope.id).await,
        0,
        "publish must never enqueue a row in outbox"
    );

    common::delete_stream(&context, &stream_name).await;
}

/// E22 — `publish` has no relationship to any transaction the caller has open: it is called while
/// the caller's own business transaction is still open, and that transaction is then rolled back.
/// The message must still be on the stream — the pass-through's non-transactional nature asserted,
/// not merely documented.
async fn publish_survives_a_rollback_of_the_surrounding_business_transaction() {
    let pool = common::fresh_postgres_db().await;
    sqlx::query("CREATE TABLE widgets (id bigserial PRIMARY KEY)")
        .execute(&pool)
        .await
        .expect("create business table");
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
    let outbox = OutboxPublisher::new(store, publisher);

    let envelope = common::serialize(
        Envelope::builder(AuditLogged {
            event: "password_reset".to_string(),
        })
        .build(),
    );

    // The caller's own business transaction is open when `publish` is called — `publish` takes no
    // transaction parameter and issues its own, unrelated network call.
    let mut tx = pool.begin().await.expect("begin business tx");
    sqlx::query("INSERT INTO widgets DEFAULT VALUES")
        .execute(&mut *tx)
        .await
        .expect("business write");
    outbox.publish(&envelope).await.expect("publish");

    common::wait_until(Duration::from_secs(10), || {
        let context = context.clone();
        let stream_name = stream_name.clone();
        async move { common::stream_message_count(&context, &stream_name).await >= 1 }
    })
    .await;

    tx.rollback().await.expect("roll back the business tx");

    let widget_count: i64 = sqlx::query_scalar("SELECT count(*) FROM widgets")
        .fetch_one(&pool)
        .await
        .expect("count widgets");
    assert_eq!(widget_count, 0, "the business write did roll back");
    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        1,
        "rolling back the caller's business transaction must not un-publish an already-sent message"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "e6_publish_bypasses_the_outbox::published_reaches_the_stream_immediately_and_never_appears_in_outbox",
            move || {
                rt.block_on(published_reaches_the_stream_immediately_and_never_appears_in_outbox());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "e6_publish_bypasses_the_outbox::publish_survives_a_rollback_of_the_surrounding_business_transaction",
            move || {
                rt.block_on(publish_survives_a_rollback_of_the_surrounding_business_transaction());
                Ok(())
            },
        ),
    ]
}
