//! §43.A.14 (pg half) — a transient failure yields `FailureOutcome::Retry`: `attempts + 1`,
//! `available_at = now() + delay`, not claimable before then. §43.A.15 (pg half) — a permanent
//! failure sets `dead_at`, a `DeadReason`, and a truncated `last_error`; dead rows are never
//! claimed again.

use crate::common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, DeadReason, FailedMessage, FailureOutcome, OutboxStore, WorkerId,
};
use reliar_store_postgres::PostgresOutboxStore;

async fn retry_outcome_delays_availability_and_increments_attempts() {
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
    assert_eq!(record.attempts, 0);

    let affected = store
        .fail(
            &worker,
            &[FailedMessage::new(
                record.message_ref(),
                "connection reset",
                FailureOutcome::Retry {
                    delay: Duration::from_secs(3600),
                },
            )],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // Not claimable before the delay elapses.
    let batch_too_soon = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert!(batch_too_soon.records.iter().all(|r| r.envelope.id != id));

    let (attempts, available_at, locked_by): (i32, time::OffsetDateTime, Option<String>) =
        sqlx::query_as("SELECT attempts, available_at, locked_by FROM outbox WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(attempts, 1);
    assert!(locked_by.is_none(), "a retried row's lease is cleared");
    let now: time::OffsetDateTime = sqlx::query_scalar("SELECT now()")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(available_at > now + time::Duration::seconds(3000));

    // SQL time-travel makes it due, and it is then claimable again.
    common::make_available_now(&pool, id.as_uuid()).await;
    let batch_after = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert_eq!(batch_after.records.len(), 1);
    assert_eq!(batch_after.records[0].attempts, 1);
}

async fn dead_outcome_sets_dead_at_reason_and_truncated_error_and_is_never_reclaimed() {
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
        .fail(
            &worker,
            &[FailedMessage::new(
                record.message_ref(),
                "broker rejected: payload too large",
                FailureOutcome::Dead {
                    reason: DeadReason::PermanentError,
                },
            )],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);

    let (dead_at_set, dead_reason, last_error, locked_by): (
        bool,
        Option<String>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(
        "SELECT dead_at IS NOT NULL, dead_reason, last_error, locked_by FROM outbox WHERE id = $1",
    )
    .bind(id.as_uuid())
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(dead_at_set);
    assert_eq!(dead_reason.as_deref(), Some("permanent_error"));
    assert_eq!(
        last_error.as_deref(),
        Some("broker rejected: payload too large")
    );
    assert!(locked_by.is_none());

    let batch_again = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert!(
        batch_again.records.iter().all(|r| r.envelope.id != id),
        "a dead row must never be claimed again"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_fail_retry_dead::retry_outcome_delays_availability_and_increments_attempts",
            move || {
                rt.block_on(retry_outcome_delays_availability_and_increments_attempts());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_fail_retry_dead::dead_outcome_sets_dead_at_reason_and_truncated_error_and_is_never_reclaimed",
            move || {
                rt.block_on(
                    dead_outcome_sets_dead_at_reason_and_truncated_error_and_is_never_reclaimed(),
                );
                Ok(())
            },
        ),
    ]
}
