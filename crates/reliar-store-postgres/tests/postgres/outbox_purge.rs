//! §43.A.17 — a pending row whose `expires_at` has passed is marked dead with
//! `DeadReason::Expired`, never published. §43.A.19 — `purge(PurgeRequest)` removes published
//! rows older than the retention in bounded batches, reports counts, and leaves pending rows
//! untouched; dead rows are removed only through `purge_dead`. Contract §7 G1 — all three
//! statements are bounded by `batch_size`, and `is_complete` is `false` while any of them
//! hit the cap. G2 — the expired sweep never transitions a row still leased by a live worker.

use crate::common;

use std::time::Duration;

use reliar_core::Envelope;
use reliar_outbox::{
    AcquireRequest, CompletedMessage, MessageRef, OutboxDeadLetters, OutboxStore, PurgeRequest,
    WorkerId,
};
use reliar_store_postgres::PostgresOutboxStore;

async fn seed_expired(pool: &sqlx::PgPool, n: u64) {
    for _ in 0..n {
        sqlx::query(
            "INSERT INTO outbox (id, message_type, message_version, conversation_id, \
                                  content_type, payload, available_at, expires_at) \
             VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), \
                     now() - interval '1 hour')",
        )
        .bind(uuid::Uuid::now_v7())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_published(pool: &sqlx::PgPool, n: u64, age: &str) {
    for _ in 0..n {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO outbox (id, message_type, message_version, conversation_id, \
                                  content_type, payload, available_at, published_at) \
             VALUES ($1, 'orders.created', 1, $1, 'application/json', '{{}}', now(), now() - interval '{age}')"
        )))
        .bind(uuid::Uuid::now_v7())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn seed_dead(pool: &sqlx::PgPool, n: u64, age: &str) {
    for _ in 0..n {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO outbox (id, message_type, message_version, conversation_id, \
                                  content_type, payload, available_at, dead_at, dead_reason) \
             VALUES ($1, 'orders.created', 1, $1, 'application/json', '{{}}', now(), \
                     now() - interval '{age}', 'permanent_error')"
        )))
        .bind(uuid::Uuid::now_v7())
        .execute(pool)
        .await
        .unwrap();
    }
}

async fn expired_pending_rows_are_swept_to_dead_and_never_published() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    seed_expired(&pool, 1).await;

    let report = store
        .purge(PurgeRequest::default().published_retention(None))
        .await
        .unwrap();
    assert_eq!(report.expired_to_dead, 1);

    let (dead_reason, published_at): (Option<String>, Option<time::OffsetDateTime>) =
        sqlx::query_as("SELECT dead_reason, published_at FROM outbox LIMIT 1")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(dead_reason.as_deref(), Some("expired"));
    assert!(published_at.is_none());

    // Never claimable.
    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert!(batch.records.is_empty());
}

async fn purge_removes_only_published_rows_older_than_retention() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    seed_published(&pool, 1, "10 days").await;
    seed_published(&pool, 1, "1 hour").await;
    let envelope = Envelope::builder(common::OrderCreated { order_id: 0 }).build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let report = store
        .purge(
            PurgeRequest::default()
                .published_retention(Some(Duration::from_hours(168)))
                .dead_retention(None),
        )
        .await
        .unwrap();
    assert_eq!(report.published_deleted, 1);
    assert_eq!(report.dead_deleted, 0);

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        remaining, 2,
        "the recent published row and the pending row survive"
    );
}

async fn dead_rows_survive_purge_without_dead_retention_and_are_removed_by_purge_dead() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    seed_dead(&pool, 1, "30 days").await;

    let report = store
        .purge(PurgeRequest::default().published_retention(None))
        .await
        .unwrap();
    assert_eq!(
        report.dead_deleted, 0,
        "dead rows are kept until an explicit purge"
    );

    let id: uuid::Uuid = sqlx::query_scalar("SELECT id FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    let created_at: time::OffsetDateTime = sqlx::query_scalar("SELECT created_at FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    let affected = store
        .purge_dead(&[MessageRef::new(
            reliar_core::MessageId::from_uuid(id),
            created_at,
        )])
        .await
        .unwrap();
    assert_eq!(affected, 1);
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}

/// Contract §7 G1 — seeding more of each kind than `batch_size`, one pass caps each count and
/// `is_complete` is `false`.
async fn purge_caps_every_pass_at_batch_size() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let batch_size = 5u32;
    seed_published(&pool, 8, "10 days").await;
    seed_dead(&pool, 8, "10 days").await;
    seed_expired(&pool, 8).await;

    let request = PurgeRequest::default()
        .published_retention(Some(Duration::from_secs(60)))
        .dead_retention(Some(Duration::from_secs(60)))
        .batch_size(batch_size);
    let report = store.purge(request.clone()).await.unwrap();

    assert_eq!(report.published_deleted, u64::from(batch_size));
    assert_eq!(report.dead_deleted, u64::from(batch_size));
    assert_eq!(report.expired_to_dead, u64::from(batch_size));
    assert!(
        !report.is_complete(batch_size),
        "a full pass on every kind must report incomplete"
    );

    // A second pass drains the remainder.
    let report2 = store.purge(request).await.unwrap();
    assert_eq!(report2.published_deleted, 3);
    assert_eq!(report2.dead_deleted, 3);
    assert_eq!(report2.expired_to_dead, 3);
    assert!(report2.is_complete(batch_size));
}

/// Contract §7 G2 — a row that expired while still leased by a live worker is not swept; the
/// worker's own `complete` still wins once it reports success.
async fn expired_sweep_never_touches_a_row_leased_by_a_live_worker() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelope = Envelope::builder(common::OrderCreated { order_id: 0 })
        .expires_at(time::OffsetDateTime::now_utc() + time::Duration::hours(1))
        .build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()).lease(Duration::from_secs(3600)))
        .await
        .unwrap();
    assert_eq!(
        batch.records.len(),
        1,
        "a not-yet-expired row is still claimable"
    );
    let record = &batch.records[0];

    // Let expires_at fall behind now, while the lease is still held (not expired).
    sqlx::query("UPDATE outbox SET expires_at = now() - interval '1 hour' WHERE id = $1")
        .bind(envelope.id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();

    let report = store
        .purge(PurgeRequest::default().published_retention(None))
        .await
        .unwrap();
    assert_eq!(
        report.expired_to_dead, 0,
        "a leased, live row must not be swept"
    );

    let affected = store
        .complete(&worker, &[CompletedMessage::new(record.message_ref())])
        .await
        .unwrap();
    assert_eq!(affected, 1, "the owning worker's complete must still win");
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_purge::expired_pending_rows_are_swept_to_dead_and_never_published",
            move || {
                rt.block_on(expired_pending_rows_are_swept_to_dead_and_never_published());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_purge::purge_removes_only_published_rows_older_than_retention",
            move || {
                rt.block_on(purge_removes_only_published_rows_older_than_retention());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_purge::dead_rows_survive_purge_without_dead_retention_and_are_removed_by_purge_dead",
            move || {
                rt.block_on(
                    dead_rows_survive_purge_without_dead_retention_and_are_removed_by_purge_dead(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_purge::purge_caps_every_pass_at_batch_size",
            move || {
                rt.block_on(purge_caps_every_pass_at_batch_size());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_purge::expired_sweep_never_touches_a_row_leased_by_a_live_worker",
            move || {
                rt.block_on(expired_sweep_never_touches_a_row_leased_by_a_live_worker());
                Ok(())
            },
        ),
    ]
}
