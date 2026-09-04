//! §43.A.7 — the claim is a single statement: while a publish is in flight, no transaction or
//! row lock is held for the row (ADR 0006).
//!
//! Proven directly: immediately after `acquire()` returns, a second connection can take its own
//! `FOR UPDATE NOWAIT` lock on the very same row. If the claim's implicit transaction were still
//! open — the forbidden "hold the lock across a publish" shape — that would raise a
//! `55P03 lock_not_available` error instead.

use crate::common;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;
use sqlx::Acquire;

async fn no_row_lock_survives_the_claim_statement() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);

    // Simulate the dispatcher now being "in flight" on a slow publish for this row: acquire()
    // has already returned, so no lock should remain.
    let mut second = pool.acquire().await.unwrap();
    let mut probe_tx = second.begin().await.unwrap();
    let locked: Result<uuid::Uuid, sqlx::Error> =
        sqlx::query_scalar("SELECT id FROM outbox WHERE id = $1 FOR UPDATE NOWAIT")
            .bind(envelope.id.as_uuid())
            .fetch_one(&mut *probe_tx)
            .await;

    assert!(
        locked.is_ok(),
        "a second session must be able to lock the just-claimed row immediately \
         (got {locked:?}) — the claim transaction must already be committed"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_claim_no_lock_during_publish::no_row_lock_survives_the_claim_statement",
        move || {
            rt.block_on(no_row_lock_survives_the_claim_statement());
            Ok(())
        },
    )]
}
