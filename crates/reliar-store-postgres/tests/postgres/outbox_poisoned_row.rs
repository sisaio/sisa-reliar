//! §19.5 / task card AC — a row `acquire` cannot decode is excluded from `records`, reported in
//! `poisoned`, and moved to dead with `DeadReason::Undecodable` by the same call; the rest of
//! the batch is still delivered.

use crate::common;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;

async fn undecodable_row_is_poisoned_and_marked_dead_while_the_batch_continues() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let good = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &good).await.unwrap();
    tx.commit().await.unwrap();

    // A second row, valid enough to pass the check constraints, but carrying an
    // `metadata_version` this build does not know how to read (ADR 0012) — the only realistic
    // way to poison a row without a raw SQL insert bypassing every Rust-level validation.
    let poisoned_id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, metadata_version, available_at) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', 999, now())",
    )
    .bind(poisoned_id)
    .execute(&pool)
    .await
    .unwrap();

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(10))
        .await
        .unwrap();

    assert_eq!(batch.records.len(), 1, "the good row is still delivered");
    assert_eq!(batch.records[0].envelope.id, good.id);

    assert_eq!(batch.poisoned.len(), 1);
    assert_eq!(batch.poisoned[0].id.as_uuid(), poisoned_id);

    let (dead_at_is_set, dead_reason): (bool, Option<String>) =
        sqlx::query_as("SELECT dead_at IS NOT NULL, dead_reason FROM outbox WHERE id = $1")
            .bind(poisoned_id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(dead_at_is_set, "the poisoned row must be moved to dead");
    assert_eq!(dead_reason.as_deref(), Some("undecodable"));
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_poisoned_row::undecodable_row_is_poisoned_and_marked_dead_while_the_batch_continues",
        move || {
            rt.block_on(undecodable_row_is_poisoned_and_marked_dead_while_the_batch_continues());
            Ok(())
        },
    )]
}
