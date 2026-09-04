//! §43.A.6 — two workers acquiring concurrently from one table receive disjoint sets whose
//! union is every due row; each claimed row carries the claiming `WorkerId` and a
//! `locked_until` in the future by DB time.

use crate::common;

use std::collections::HashSet;

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

async fn concurrent_acquires_are_disjoint_and_exhaustive() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let seeded: HashSet<MessageId> = seed(&store, &pool, 20).await.into_iter().collect();

    let worker_a = WorkerId::generate();
    let worker_b = WorkerId::generate();
    let (batch_a, batch_b) = tokio::join!(
        store.acquire(AcquireRequest::new(worker_a.clone()).batch_size(15)),
        store.acquire(AcquireRequest::new(worker_b.clone()).batch_size(15)),
    );
    let batch_a = batch_a.unwrap();
    let batch_b = batch_b.unwrap();

    let ids_a: HashSet<MessageId> = batch_a.records.iter().map(|r| r.envelope.id).collect();
    let ids_b: HashSet<MessageId> = batch_b.records.iter().map(|r| r.envelope.id).collect();

    assert!(
        ids_a.is_disjoint(&ids_b),
        "the same row must never be claimed by two concurrent acquires"
    );
    assert_eq!(
        &(&ids_a | &ids_b),
        &seeded,
        "the union of both claims must be every seeded row"
    );

    for record in batch_a.records.iter().chain(batch_b.records.iter()) {
        assert!(record.locked_by.is_some());
        assert!(record.locked_until.is_some());
    }

    let now: time::OffsetDateTime = sqlx::query_scalar("SELECT now()")
        .fetch_one(&pool)
        .await
        .unwrap();
    for record in &batch_a.records {
        assert_eq!(record.locked_by.as_ref().unwrap(), &worker_a);
        assert!(record.locked_until.unwrap() > now);
    }
    for record in &batch_b.records {
        assert_eq!(record.locked_by.as_ref().unwrap(), &worker_b);
        assert!(record.locked_until.unwrap() > now);
    }
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_acquire_skip_locked::concurrent_acquires_are_disjoint_and_exhaustive",
        move || {
            rt.block_on(concurrent_acquires_are_disjoint_and_exhaustive());
            Ok(())
        },
    )]
}
