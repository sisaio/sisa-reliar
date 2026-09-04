//! §43.A.32 — after `migrate()`, `pg_constraint` and `pg_indexes` show every constraint and
//! index under exactly the names specified in §24.1 — `pk_`/`ck_` for constraints and `ix_` for
//! every index, unique or not, with no `uq_` name present; a duplicate `MessageId` insert
//! surfaces the `pk_outbox` violation mapped to the documented typed error.

use crate::common;

use std::collections::HashSet;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_store_postgres::{EnqueueError, PostgresOutboxStore};

const EXPECTED_CONSTRAINTS: &[&str] = &[
    "pk_outbox",
    "ck_outbox_attempts",
    "ck_outbox_message_version",
    "ck_outbox_metadata_version",
    "ck_outbox_lease",
    "ck_outbox_terminal",
    "ck_outbox_dead_reason",
];

const EXPECTED_INDEXES: &[&str] = &[
    "ix_outbox_sequence",
    "ix_outbox_pending",
    "ix_outbox_published",
    "ix_outbox_dead",
    "ix_outbox_dead_at",
    "ix_outbox_ordering_key",
    "ix_outbox_expires",
];

async fn every_constraint_and_index_is_named_as_specified() {
    let pool = common::fresh_db().await;

    let constraints: Vec<String> = sqlx::query_scalar(
        "SELECT conname FROM pg_constraint c \
           JOIN pg_class t ON t.oid = c.conrelid \
           JOIN pg_namespace n ON n.oid = t.relnamespace \
          WHERE t.relname = 'outbox' AND n.nspname = 'reliar'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let constraints: HashSet<&str> = constraints.iter().map(String::as_str).collect();
    for expected in EXPECTED_CONSTRAINTS {
        assert!(
            constraints.contains(expected),
            "missing constraint {expected}; found {constraints:?}"
        );
    }

    let indexes: Vec<String> = sqlx::query_scalar(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'outbox' AND schemaname = 'reliar'",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    let index_set: HashSet<&str> = indexes.iter().map(String::as_str).collect();
    for expected in EXPECTED_INDEXES {
        assert!(
            index_set.contains(expected),
            "missing index {expected}; found {index_set:?}"
        );
    }
    assert!(
        indexes.iter().all(|name| !name.starts_with("uq_")),
        "no index may use the retired uq_ prefix (decision 27): {indexes:?}"
    );
    // pk_outbox is a PRIMARY KEY constraint, backed by an index of the same name — the only
    // constraint-derived index name, and it is `pk_`, not `ix_`.
    assert!(index_set.contains("pk_outbox"));
}

async fn duplicate_message_id_maps_to_pk_outbox_violation() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();

    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    match store.enqueue(&mut tx, &envelope).await {
        Err(EnqueueError::Duplicate { id }) => assert_eq!(id, envelope.id),
        other => panic!("expected EnqueueError::Duplicate, got {other:?}"),
    }
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_constraint_names::every_constraint_and_index_is_named_as_specified",
            move || {
                rt.block_on(every_constraint_and_index_is_named_as_specified());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_constraint_names::duplicate_message_id_maps_to_pk_outbox_violation",
            move || {
                rt.block_on(duplicate_message_id_maps_to_pk_outbox_violation());
                Ok(())
            },
        ),
    ]
}
