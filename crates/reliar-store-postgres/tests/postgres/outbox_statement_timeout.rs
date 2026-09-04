//! Review 3 M3 — the non-zero `statement_timeout` branch (`BEGIN`/`SET LOCAL statement_timeout`/
//! statement/`COMMIT` instead of the plain implicit-transaction statement) had no test at all
//! across roughly 80 lines spanning `acquire`/`complete`/`fail`/`release`/`extend_lease`/`stats`/
//! `purge`/`list_dead`/`retry_dead`/`purge_dead`. Two things need proving: (1) a generous
//! non-zero timeout changes nothing observable — same successful outcomes as the zero-timeout
//! path; (2) a timeout genuinely too short for the statement to complete yields a clean, typed,
//! `FailureKind::Transient` error (SQLSTATE `57014 query_canceled`) — never a hang, and never an
//! untyped/opaque failure.

use crate::common;

use std::time::Duration;

use reliar_outbox::{
    AcquireRequest, Classify, CompletedMessage, FailedMessage, FailureKind, FailureOutcome,
    OutboxStore, WorkerId,
};
use reliar_store_postgres::{PostgresOutboxSettings, PostgresOutboxStore, PostgresStoreError};

async fn a_generous_non_zero_timeout_behaves_identically_to_zero_for_acquire_complete_and_fail() {
    let pool = common::fresh_db().await;
    let settings = PostgresOutboxSettings::default().statement_timeout(Duration::from_secs(30));
    let store = PostgresOutboxStore::with_settings(pool.clone(), settings)
        .await
        .unwrap();
    let envelopes = common::seed(&store, &pool, 2).await;

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()).batch_size(10))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 2, "acquire behaves the same wrapped");
    let ids: Vec<_> = batch.records.iter().map(|r| r.envelope.id).collect();
    assert!(ids.contains(&envelopes[0].id));
    assert!(ids.contains(&envelopes[1].id));

    let affected = store
        .complete(
            &worker,
            &[CompletedMessage::new(batch.records[0].message_ref())],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1, "complete behaves the same wrapped");

    let affected = store
        .fail(
            &worker,
            &[FailedMessage::new(
                batch.records[1].message_ref(),
                "transient, retry shortly",
                FailureOutcome::Retry {
                    delay: Duration::from_millis(1),
                },
            )],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1, "fail behaves the same wrapped");
}

async fn a_too_short_timeout_yields_a_clean_transient_error_instead_of_hanging() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 1).await;

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker.clone()))
        .await
        .unwrap();
    let record_ref = batch.records[0].message_ref();
    assert_eq!(record_ref.id, envelopes[0].id);

    // Holds the row's lock open from a second connection, so `complete`'s `UPDATE ... WHERE id =
    // ANY($1)` — a plain row lock wait, not `SKIP LOCKED` — has no choice but to block until the
    // `statement_timeout` cancels it.
    let mut holder = pool.begin().await.unwrap();
    sqlx::query("SELECT id FROM outbox WHERE id = $1 FOR UPDATE")
        .bind(record_ref.id.as_uuid())
        .fetch_all(&mut *holder)
        .await
        .unwrap();

    let settings = PostgresOutboxSettings::default().statement_timeout(Duration::from_millis(50));
    let timeout_store = PostgresOutboxStore::with_settings(pool.clone(), settings)
        .await
        .unwrap();

    // The outer bound proves this is a *clean, timely* error, not a hang the test would
    // otherwise wait on indefinitely; the `statement_timeout` itself is what actually resolves
    // it, well inside this bound.
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        timeout_store.complete(&worker, &[CompletedMessage::new(record_ref)]),
    )
    .await
    .expect("must return well within the outer bound, not hang");

    let err = result.expect_err("a canceled statement must surface as a typed error");
    assert!(
        matches!(err, PostgresStoreError::Database { .. }),
        "expected Database, got {err:?}"
    );
    assert_eq!(
        err.kind(),
        FailureKind::Transient,
        "a canceled statement is safe to retry"
    );

    holder.rollback().await.unwrap();
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_statement_timeout::a_generous_non_zero_timeout_behaves_identically_to_zero_for_acquire_complete_and_fail",
            move || {
                rt.block_on(a_generous_non_zero_timeout_behaves_identically_to_zero_for_acquire_complete_and_fail());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_statement_timeout::a_too_short_timeout_yields_a_clean_transient_error_instead_of_hanging",
            move || {
                rt.block_on(
                    a_too_short_timeout_yields_a_clean_transient_error_instead_of_hanging(),
                );
                Ok(())
            },
        ),
    ]
}
