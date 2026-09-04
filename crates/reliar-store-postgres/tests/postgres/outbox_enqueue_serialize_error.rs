//! Review 3 gap — `EnqueueError::Serialize` had no test against real Postgres: a serializer
//! that rejects the body must fail `enqueue` before any SQL runs (no partial/aborted `INSERT`),
//! classified `Permanent` (the same body serializes the same way every time).

use crate::common;

use crate::common::{AlwaysFailingSerializer, OrderCreated};
use reliar_core::Envelope;
use reliar_outbox::{Classify, FailureKind};
use reliar_store_postgres::{EnqueueError, PostgresOutboxSettings, PostgresOutboxStore};

async fn a_failing_serializer_yields_enqueue_error_serialize_before_touching_the_database() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::connect(
        pool.clone(),
        PostgresOutboxSettings::default(),
        AlwaysFailingSerializer,
    )
    .await
    .unwrap();

    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let mut tx = pool.begin().await.unwrap();
    let err = store.enqueue(&mut tx, &envelope).await.unwrap_err();
    assert!(matches!(err, EnqueueError::Serialize { .. }));
    assert_eq!(err.kind(), FailureKind::Permanent);
    // The transaction is still usable — nothing was sent to the database for this row, so it
    // was never aborted by a bad statement.
    tx.rollback().await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0, "the rejected body must never reach the table");
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_enqueue_serialize_error::a_failing_serializer_yields_enqueue_error_serialize_before_touching_the_database",
        move || {
            rt.block_on(
                a_failing_serializer_yields_enqueue_error_serialize_before_touching_the_database(),
            );
            Ok(())
        },
    )]
}
