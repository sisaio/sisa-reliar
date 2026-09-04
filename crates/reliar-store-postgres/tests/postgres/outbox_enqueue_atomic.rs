//! §43.A.1 — business data and an outbox row written in one `sqlx` transaction are both present
//! after commit and both absent after rollback.

use crate::common;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::OutboxStore;
use reliar_store_postgres::PostgresOutboxStore;

async fn count_business_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM widgets")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn count_outbox_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn create_widgets_table(pool: &sqlx::PgPool) {
    sqlx::query("CREATE TABLE widgets (id bigserial PRIMARY KEY)")
        .execute(pool)
        .await
        .unwrap();
}

async fn commit_makes_both_rows_visible() {
    let pool = common::fresh_db().await;
    create_widgets_table(&pool).await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO widgets DEFAULT VALUES")
        .execute(&mut *tx)
        .await
        .unwrap();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(count_business_rows(&pool).await, 1);
    assert_eq!(count_outbox_rows(&pool).await, 1);
}

async fn rollback_makes_neither_row_visible() {
    let pool = common::fresh_db().await;
    create_widgets_table(&pool).await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO widgets DEFAULT VALUES")
        .execute(&mut *tx)
        .await
        .unwrap();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(count_business_rows(&pool).await, 0);
    assert_eq!(count_outbox_rows(&pool).await, 0);
}

async fn duplicate_message_id_aborts_the_transaction() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();

    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let result = store.enqueue(&mut tx, &envelope).await;
    assert!(matches!(
        result,
        Err(reliar_store_postgres::EnqueueError::Duplicate { id }) if id == envelope.id
    ));
}

/// Review 2, major 7 — `enqueue_with(.., EnqueueOptions { ordering_key })` round-trips end to
/// end: the column is written, and the acquired record carries it back (contract §4 #9, §22.2).
async fn enqueue_with_persists_and_returns_the_ordering_key() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();

    let mut tx = pool.begin().await.unwrap();
    store
        .enqueue_with(
            &mut tx,
            &envelope,
            reliar_store_postgres::EnqueueOptions::default().ordering_key("customer-42"),
        )
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let batch = store
        .acquire(reliar_outbox::AcquireRequest::new(
            reliar_outbox::WorkerId::generate(),
        ))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.records[0].ordering_key.as_deref(),
        Some("customer-42")
    );

    // Plain `enqueue` (no options) writes no ordering key — the "None = unordered" default.
    let envelope2 = Envelope::builder(OrderCreated { order_id: 2 }).build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope2).await.unwrap();
    tx.commit().await.unwrap();
    let ordering_key: Option<String> =
        sqlx::query_scalar("SELECT ordering_key FROM outbox WHERE id = $1")
            .bind(envelope2.id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(ordering_key.is_none());
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_enqueue_atomic::commit_makes_both_rows_visible",
            move || {
                rt.block_on(commit_makes_both_rows_visible());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_enqueue_atomic::rollback_makes_neither_row_visible",
            move || {
                rt.block_on(rollback_makes_neither_row_visible());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_enqueue_atomic::duplicate_message_id_aborts_the_transaction",
            move || {
                rt.block_on(duplicate_message_id_aborts_the_transaction());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_enqueue_atomic::enqueue_with_persists_and_returns_the_ordering_key",
            move || {
                rt.block_on(enqueue_with_persists_and_returns_the_ordering_key());
                Ok(())
            },
        ),
    ]
}
