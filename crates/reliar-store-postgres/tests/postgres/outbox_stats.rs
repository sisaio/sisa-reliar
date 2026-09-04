//! §43.A.21 — `stats` reports the claimable pending count, the oldest pending row's
//! `available_at` (by DB time), and the dead count; expired-but-unswept rows are counted
//! separately so they never pin the lag gauge.

use crate::common;

use reliar_outbox::OutboxStore;
use reliar_store_postgres::PostgresOutboxStore;

async fn stats_reports_pending_dead_and_expired_pending() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let empty = store.stats().await.unwrap();
    assert_eq!(empty.pending, 0);
    assert_eq!(empty.dead, 0);
    assert_eq!(empty.expired_pending, 0);
    assert!(empty.oldest_pending_available_at.is_none());
    assert!(empty.lag().is_none());

    common::seed(&store, &pool, 3).await;
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at, dead_at, dead_reason) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), now(), 'expired')",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at, expires_at) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), \
                 now() - interval '1 hour')",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();

    let stats = store.stats().await.unwrap();
    assert_eq!(
        stats.pending, 3,
        "the expired-pending row is excluded from the claim predicate"
    );
    assert_eq!(stats.dead, 1);
    assert_eq!(stats.expired_pending, 1);
    assert!(stats.oldest_pending_available_at.is_some());
    assert!(stats.lag().is_some());
    assert!(stats.as_of <= time::OffsetDateTime::now_utc() + time::Duration::seconds(5));
}

async fn stats_pending_excludes_a_leased_row_and_a_row_not_yet_available() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    // Currently leased by a live worker — not claimable, so not "pending".
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at, locked_by, locked_until) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), 'w1', \
                 now() + interval '1 hour')",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();

    // `available_at` in the future — a retry-delayed row, not yet due.
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', \
                 now() + interval '1 hour')",
    )
    .bind(uuid::Uuid::now_v7())
    .execute(&pool)
    .await
    .unwrap();

    let stats = store.stats().await.unwrap();
    assert_eq!(
        stats.pending, 0,
        "a leased row and a not-yet-due row must both be excluded from pending"
    );
    assert!(stats.oldest_pending_available_at.is_none());
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_stats::stats_reports_pending_dead_and_expired_pending",
            move || {
                rt.block_on(stats_reports_pending_dead_and_expired_pending());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_stats::stats_pending_excludes_a_leased_row_and_a_row_not_yet_available",
            move || {
                rt.block_on(stats_pending_excludes_a_leased_row_and_a_row_not_yet_available());
                Ok(())
            },
        ),
    ]
}
