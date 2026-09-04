//! §43.A.9 — `extend_lease` by the owning worker moves `locked_until` forward; by a
//! non-owner it affects zero rows. §43.A.10 — `release` by the owning worker clears the lease
//! so the rows are immediately claimable.

use crate::common;

use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;

async fn extend_lease_moves_locked_until_forward_for_the_owner_only() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 1).await;

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()).lease(std::time::Duration::from_secs(5)))
        .await
        .unwrap();
    let record = &batch.records[0];
    let original_until = record.locked_until.unwrap();

    let other = WorkerId::generate();
    let affected = store
        .extend_lease(
            &other,
            &[record.message_ref()],
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(
        affected, 0,
        "a non-owner must not be able to extend the lease"
    );

    let affected = store
        .extend_lease(
            &worker,
            &[record.message_ref()],
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let new_until: time::OffsetDateTime =
        sqlx::query_scalar("SELECT locked_until FROM outbox WHERE id = $1")
            .bind(envelopes[0].id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(new_until > original_until);
}

async fn release_clears_the_lease_and_the_row_is_immediately_claimable() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    common::seed(&store, &pool, 1).await;

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()))
        .await
        .unwrap();
    let record = &batch.records[0];
    let attempts_before = record.attempts;
    let available_before = record.available_at;

    let affected = store
        .release(&worker, &[record.message_ref()])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let batch_again = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert_eq!(batch_again.records.len(), 1);
    assert_eq!(batch_again.records[0].envelope.id, record.envelope.id);
    // `release` touches neither attempts nor available_at (SRS §26.1).
    assert_eq!(batch_again.records[0].attempts, attempts_before);
    assert_eq!(batch_again.records[0].available_at, available_before);
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_lease_management::extend_lease_moves_locked_until_forward_for_the_owner_only",
            move || {
                rt.block_on(extend_lease_moves_locked_until_forward_for_the_owner_only());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_lease_management::release_clears_the_lease_and_the_row_is_immediately_claimable",
            move || {
                rt.block_on(release_clears_the_lease_and_the_row_is_immediately_claimable());
                Ok(())
            },
        ),
    ]
}
