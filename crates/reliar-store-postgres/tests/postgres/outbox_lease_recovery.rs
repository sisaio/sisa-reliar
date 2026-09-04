//! §43.A.8 — a row whose lease has expired (moved into the past via SQL time-travel) is
//! acquirable by another worker; the original worker's late `complete`/`fail` affects zero
//! rows. §43.A.12 — completed rows are never returned by a later `acquire`.

use crate::common;

use reliar_outbox::{
    AcquireRequest, CompletedMessage, FailedMessage, FailureOutcome, MessageRef, OutboxStore,
    WorkerId,
};
use reliar_store_postgres::PostgresOutboxStore;

async fn expired_lease_is_reclaimed_and_the_original_workers_outcome_is_a_no_op() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 1).await;
    let id = envelopes[0].id;

    let worker_a = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker_a.clone()).lease(std::time::Duration::from_secs(30)))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    let created_at = batch.records[0].created_at;

    common::expire_lease(&pool, id.as_uuid()).await;

    let worker_b = WorkerId::generate();
    let batch_b = store
        .acquire(AcquireRequest::new(worker_b.clone()))
        .await
        .unwrap();
    assert_eq!(
        batch_b.records.len(),
        1,
        "worker B must reclaim the row once the lease expired"
    );
    assert_eq!(batch_b.records[0].envelope.id, id);
    assert_eq!(batch_b.records[0].locked_by.as_ref().unwrap(), &worker_b);

    // Worker A's late outcome affects zero rows — it no longer owns the lease.
    let complete_count = store
        .complete(
            &worker_a,
            &[CompletedMessage::new(MessageRef::new(id, created_at))],
        )
        .await
        .unwrap();
    assert_eq!(complete_count, 0);

    let fail_count = store
        .fail(
            &worker_a,
            &[FailedMessage::new(
                MessageRef::new(id, created_at),
                "too late",
                FailureOutcome::Retry {
                    delay: std::time::Duration::from_secs(1),
                },
            )],
        )
        .await
        .unwrap();
    assert_eq!(fail_count, 0);
}

async fn completed_rows_are_never_reacquired() {
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
    let affected = store
        .complete(&worker, &[CompletedMessage::new(record.message_ref())])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let batch_again = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert!(
        batch_again.records.iter().all(|r| r.envelope.id != id),
        "a completed row must never be returned by acquire again"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_lease_recovery::expired_lease_is_reclaimed_and_the_original_workers_outcome_is_a_no_op",
            move || {
                rt.block_on(
                    expired_lease_is_reclaimed_and_the_original_workers_outcome_is_a_no_op(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_lease_recovery::completed_rows_are_never_reacquired",
            move || {
                rt.block_on(completed_rows_are_never_reacquired());
                Ok(())
            },
        ),
    ]
}
