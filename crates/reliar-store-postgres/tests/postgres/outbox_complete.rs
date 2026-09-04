//! Review 3 M2 — `complete` increments `attempts` (it counts an observed outcome, the same as
//! `fail` does, ADR 0009); no existing test asserted this directly.

use crate::common;

use reliar_outbox::{AcquireRequest, CompletedMessage, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;

async fn complete_increments_attempts() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 1).await;
    let id = envelopes[0].id;

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()))
        .await
        .unwrap();
    let record = &batch.records[0];
    assert_eq!(
        record.attempts, 0,
        "claim itself never counts as an attempt"
    );

    let affected = store
        .complete(&worker, &[CompletedMessage::new(record.message_ref())])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        attempts, 1,
        "complete must count as one observed publish attempt"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_complete::complete_increments_attempts",
        move || {
            rt.block_on(complete_increments_attempts());
            Ok(())
        },
    )]
}
