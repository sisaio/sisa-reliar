//! §43.A.6 — `tests/outbox_acquire_skip_locked.rs` proves two concurrent `acquire`s partition a
//! seed disjointly and exhaustively, but that alone doesn't prove `SKIP LOCKED` is actually
//! skipping a *held* row lock rather than, say, both calls happening to race the same rows
//! apart by luck. This file holds a real row lock open on a known subset from a second
//! connection (`SELECT … FOR UPDATE`, uncommitted) and proves `acquire` still returns promptly
//! (bounded by a `tokio::time::timeout`, so a regression that makes `acquire` block on the lock
//! instead of skipping it fails the test rather than hanging the suite) with exactly the
//! complement — then, once the lock is released, the previously locked rows are claimable.

use crate::common;

use std::collections::HashSet;
use std::time::Duration;

use crate::common::OrderCreated;
use reliar_core::{Envelope, MessageId};
use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;

async fn seed(store: &PostgresOutboxStore, pool: &sqlx::PgPool, n: u64) -> Vec<MessageId> {
    let mut ids = Vec::with_capacity(n as usize);
    for i in 0..n {
        let envelope = Envelope::builder(OrderCreated { order_id: i }).build();
        let mut tx = pool.begin().await.unwrap();
        store.enqueue(&mut tx, &envelope).await.unwrap();
        tx.commit().await.unwrap();
        ids.push(envelope.id);
    }
    ids
}

async fn acquire_skips_a_row_a_second_connection_holds_locked_and_claims_it_after_release() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let seeded: Vec<MessageId> = seed(&store, &pool, 10).await;
    let seeded_set: HashSet<MessageId> = seeded.iter().copied().collect();

    let held: Vec<uuid::Uuid> = seeded[..4].iter().map(MessageId::as_uuid).collect();
    let held_set: HashSet<MessageId> = seeded[..4].iter().copied().collect();

    // A genuinely separate connection — not the pool `acquire` uses internally — holds a real
    // row lock open via an uncommitted `FOR UPDATE`, exactly what `SKIP LOCKED` must skip.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM outbox WHERE id = ANY($1) FOR UPDATE")
        .bind(&held)
        .fetch_all(&mut *holder)
        .await
        .unwrap();

    let batch = tokio::time::timeout(
        Duration::from_secs(5),
        store.acquire(AcquireRequest::new(WorkerId::generate()).batch_size(20)),
    )
    .await
    .expect("acquire must return promptly instead of blocking on the held lock")
    .unwrap();

    let claimed: HashSet<MessageId> = batch.records.iter().map(|r| r.envelope.id).collect();
    assert_eq!(
        claimed,
        &seeded_set - &held_set,
        "acquire must claim exactly the rows that are not held locked"
    );
    assert!(
        claimed.is_disjoint(&held_set),
        "none of the held-locked rows may be claimed while the lock is open"
    );

    // Release the held lock.
    holder.rollback().await.unwrap();

    let batch_after = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(20))
        .await
        .unwrap();
    let claimed_after: HashSet<MessageId> =
        batch_after.records.iter().map(|r| r.envelope.id).collect();
    assert_eq!(
        claimed_after, held_set,
        "once released, exactly the previously held-locked rows are claimable"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_acquire_skip_locked_held_lock::acquire_skips_a_row_a_second_connection_holds_locked_and_claims_it_after_release",
        move || {
            rt.block_on(
                acquire_skips_a_row_a_second_connection_holds_locked_and_claims_it_after_release(),
            );
            Ok(())
        },
    )]
}
