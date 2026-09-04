//! Review 3 B1 (blocker) / M1 (major) — `purge`'s three statements each take an MVCC snapshot in
//! their subselect, then acquire a row lock in the outer `DELETE`/`UPDATE`. Under READ COMMITTED,
//! if another transaction commits a change to that row between the snapshot and the lock
//! acquisition, PostgreSQL's `EvalPlanQual` machinery re-evaluates **only the outer statement's own
//! WHERE clause** against the row's current version before proceeding — it does not re-run the
//! subselect. An outer clause of bare `id IN (...)` (as the dead/published deletes originally
//! had) or `id IN (...)` plus only the *immutable* part of the sweep's predicate (as the expired
//! sweep originally had) therefore acts on stale information: a row a concurrent transaction just
//! resurrected (`retry_dead`) or completed (`complete`) is still deleted/swept, either losing a
//! message outright (B1) or racing a live worker's own terminal-state write into a
//! `ck_outbox_terminal` (23514) constraint violation that fails the whole `purge` call (M1).
//!
//! Both scenarios below force the race deterministically with a genuine held row lock (not a
//! timing guess): a second connection opens a transaction and takes `SELECT … FOR UPDATE` on the
//! target row, a concurrent `purge` call is confirmed (via `pg_stat_activity`, polled — never a
//! blind sleep) to be blocked waiting on that exact lock, the second connection performs the
//! resurrection/completion **in the same transaction** (so it's still holding the lock) and
//! commits, and only then is the blocked `purge` allowed to proceed — the point at which
//! `EvalPlanQual`'s re-check either does or doesn't save the row.

use crate::common;

use std::future::Future;
use std::time::Duration;

use reliar_outbox::{OutboxStore, PurgeRequest};
use reliar_store_postgres::PostgresOutboxStore;

/// Polls `f` every 20ms until it resolves `true` or `timeout` elapses, panicking on timeout —
/// used here to wait for a concurrent `purge` call to actually be blocked on the held row lock,
/// never a blind sleep (mirrors `outbox_dispatcher_end_to_end.rs`'s helper of the same name).
async fn wait_until<F, Fut>(timeout: Duration, mut f: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if f().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "condition not met within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// True once some *other* backend on this database is waiting on a lock — the signal that the
/// concurrent `purge` call is blocked behind the row lock this test's own connection holds.
async fn some_other_backend_is_waiting_on_a_lock(pool: &sqlx::PgPool) -> bool {
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_stat_activity \
          WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid() \
            AND datname = current_database()",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    count > 0
}

async fn dead_retention_purge_does_not_delete_a_row_concurrently_resurrected_by_retry_dead() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at, dead_at, dead_reason) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), \
                 now() - interval '1 hour', 'permanent_error')",
    )
    .bind(id)
    .execute(&pool)
    .await
    .unwrap();

    // Holds the row lock open — simulates `retry_dead` being "in flight" concurrently with the
    // purge statement's own attempt to lock the same row.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM outbox WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_all(&mut *holder)
        .await
        .unwrap();

    let purge_task = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .purge(PurgeRequest::default().dead_retention(Some(Duration::ZERO)))
                .await
        }
    });

    wait_until(Duration::from_secs(5), || {
        some_other_backend_is_waiting_on_a_lock(&pool)
    })
    .await;

    // The resurrection, in the same transaction that still holds the row lock — mirrors
    // `retry_dead`'s own SQL exactly.
    sqlx::query(
        "UPDATE outbox \
            SET dead_at = NULL, dead_reason = NULL, available_at = now(), attempts = 0, \
                locked_by = NULL, locked_until = NULL, updated_at = now() \
          WHERE id = $1 AND dead_at IS NOT NULL",
    )
    .bind(id)
    .execute(&mut *holder)
    .await
    .unwrap();
    holder.commit().await.unwrap();

    let report = purge_task
        .await
        .expect("purge task did not panic")
        .expect("purge itself did not error");

    assert_eq!(
        report.dead_deleted, 0,
        "the resurrected row must not be counted as deleted"
    );

    let (still_present, dead_at_is_null): (i64, bool) =
        sqlx::query_as("SELECT count(*), bool_and(dead_at IS NULL) FROM outbox WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(still_present, 1, "the row must survive the purge");
    assert!(dead_at_is_null, "the resurrection must not be undone");
}

async fn expired_sweep_does_not_clobber_a_row_concurrently_completed_by_a_lapsed_lease_worker() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let id = uuid::Uuid::now_v7();
    let worker = "lapsed-worker";
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at, expires_at, locked_by, locked_until) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), \
                 now() - interval '1 minute', $2, now() - interval '1 second')",
    )
    .bind(id)
    .bind(worker)
    .execute(&pool)
    .await
    .unwrap();

    // Holds the row lock open — simulates the lapsed-lease worker's `complete` call being "in
    // flight" concurrently with the expired sweep's own attempt to lock the same row.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM outbox WHERE id = $1 FOR UPDATE")
        .bind(id)
        .fetch_all(&mut *holder)
        .await
        .unwrap();

    let purge_task = tokio::spawn({
        let store = store.clone();
        async move { store.purge(PurgeRequest::default()).await }
    });

    wait_until(Duration::from_secs(5), || {
        some_other_backend_is_waiting_on_a_lock(&pool)
    })
    .await;

    // The completion, in the same transaction that still holds the row lock — mirrors
    // `complete`'s own SQL exactly (worker-guarded on `locked_by`, matching what a worker whose
    // lease lapsed only very recently, but which still owns `locked_by`, would issue).
    sqlx::query(
        "UPDATE outbox \
            SET published_at = now(), attempts = attempts + 1, locked_by = NULL, \
                locked_until = NULL, updated_at = now() \
          WHERE id = $1 AND locked_by = $2",
    )
    .bind(id)
    .bind(worker)
    .execute(&mut *holder)
    .await
    .unwrap();
    holder.commit().await.unwrap();

    let report = purge_task
        .await
        .expect("purge task did not panic")
        .expect("purge must not fail with a ck_outbox_terminal violation");

    assert_eq!(
        report.expired_to_dead, 0,
        "the concurrently completed row must not be counted as swept"
    );

    let (published_at_is_set, dead_at_is_null): (bool, bool) = sqlx::query_as(
        "SELECT published_at IS NOT NULL, dead_at IS NULL FROM outbox WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(
        published_at_is_set,
        "the worker's own completion must stand"
    );
    assert!(
        dead_at_is_null,
        "the sweep must not have also marked the now-published row dead"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_purge_concurrent_resurrection::dead_retention_purge_does_not_delete_a_row_concurrently_resurrected_by_retry_dead",
            move || {
                rt.block_on(dead_retention_purge_does_not_delete_a_row_concurrently_resurrected_by_retry_dead());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_purge_concurrent_resurrection::expired_sweep_does_not_clobber_a_row_concurrently_completed_by_a_lapsed_lease_worker",
            move || {
                rt.block_on(expired_sweep_does_not_clobber_a_row_concurrently_completed_by_a_lapsed_lease_worker());
                Ok(())
            },
        ),
    ]
}
