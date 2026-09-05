//! Contract §7 J1/J2/J4 — per-variant `Classify`, SQLSTATE-class-based classification of
//! `Database` errors on **every** path (not just startup verification), and schema-identifier
//! validation before it ever reaches `dangerous_set_table_name`/`SET search_path`.

use crate::common;

use reliar_core::{Classify, FailureKind};
use reliar_outbox::{OutboxStore, WorkerId};
use reliar_store_postgres::{
    MigrateError, MigrateOptions, PostgresOutboxSettings, PostgresOutboxStore, PostgresStoreError,
};

async fn undefined_table_maps_to_not_migrated_on_the_operational_path_and_is_permanent() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    // `outbox` existed at construction; drop it out from under an already-constructed store, so
    // the *next* SQLSTATE 42P01 is seen by an operational call, not `connect`'s own check
    // (contract §7 J2).
    sqlx::query("DROP TABLE outbox")
        .execute(&pool)
        .await
        .unwrap();

    let err = store.stats().await.unwrap_err();
    match &err {
        PostgresStoreError::NotMigrated { schema } => assert_eq!(schema, "reliar"),
        other => panic!("expected NotMigrated, got {other:?}"),
    }
    assert_eq!(err.kind(), FailureKind::Permanent);
}

async fn a_closed_pool_classifies_transient() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    pool.close().await;

    let err = store.stats().await.unwrap_err();
    assert!(matches!(err, PostgresStoreError::Database { .. }));
    assert_eq!(err.kind(), FailureKind::Transient);
}

async fn a_data_exception_sqlstate_classifies_permanent() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let envelopes = common::seed(&store, &pool, 1).await;

    let worker = WorkerId::generate();
    let batch = store
        .acquire(reliar_outbox::AcquireRequest::new(worker.clone()))
        .await
        .unwrap();
    let record = &batch.records[0];

    // `now() + (i64::MAX milliseconds)` overflows `timestamptz`'s representable range —
    // PostgreSQL raises SQLSTATE 22008 (datetime_field_overflow, class 22 = data exception),
    // which the table classifies **permanent**.
    let err = store
        .extend_lease(&worker, &[record.message_ref()], std::time::Duration::MAX)
        .await
        .unwrap_err();
    assert!(matches!(err, PostgresStoreError::Database { .. }));
    assert_eq!(err.kind(), FailureKind::Permanent);
    let _ = envelopes;
}

async fn connect_rejects_an_invalid_schema_name() {
    let pool = common::fresh_db().await;
    // `connect`/`with_settings` validate the schema **before** ever querying the database, so
    // a pool that is merely migrated for the default "reliar" schema is fine to reuse here.
    for invalid in ["1leading_digit", "has-a-dash", "", "has space"] {
        let settings = PostgresOutboxSettings::default().schema(invalid);
        let result = PostgresOutboxStore::with_settings(pool.clone(), settings).await;
        match result {
            Err(PostgresStoreError::InvalidSchema { schema }) => assert_eq!(schema, invalid),
            other => panic!("expected InvalidSchema for {invalid:?}, got {other:?}"),
        }
    }
}

async fn connect_rejects_a_schema_name_over_the_length_cap() {
    let pool = common::fresh_db().await;
    let too_long = "a".repeat(64);
    let settings = PostgresOutboxSettings::default().schema(too_long.clone());
    let result = PostgresOutboxStore::with_settings(pool, settings).await;
    match result {
        Err(PostgresStoreError::InvalidSchema { schema }) => assert_eq!(schema, too_long),
        other => panic!("expected InvalidSchema, got {other:?}"),
    }
}

async fn migrate_rejects_an_invalid_schema_name_before_touching_the_database() {
    let pool = common::fresh_unmigrated_db().await;
    for invalid in ["1leading_digit", "has-a-dash", "", "has space"] {
        let result =
            reliar_store_postgres::migrate(&pool, MigrateOptions::default().schema(invalid)).await;
        match result {
            Err(MigrateError::InvalidSchema { schema }) => assert_eq!(schema, invalid),
            other => panic!("expected InvalidSchema for {invalid:?}, got {other:?}"),
        }
    }

    // Nothing was created by the rejected attempts.
    let outbox_exists: bool = sqlx::query_scalar("SELECT to_regclass('reliar.outbox') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(!outbox_exists);
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_error_classification::undefined_table_maps_to_not_migrated_on_the_operational_path_and_is_permanent",
            move || {
                rt.block_on(
                    undefined_table_maps_to_not_migrated_on_the_operational_path_and_is_permanent(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_error_classification::a_closed_pool_classifies_transient",
            move || {
                rt.block_on(a_closed_pool_classifies_transient());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_error_classification::a_data_exception_sqlstate_classifies_permanent",
            move || {
                rt.block_on(a_data_exception_sqlstate_classifies_permanent());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_error_classification::connect_rejects_an_invalid_schema_name",
            move || {
                rt.block_on(connect_rejects_an_invalid_schema_name());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_error_classification::connect_rejects_a_schema_name_over_the_length_cap",
            move || {
                rt.block_on(connect_rejects_a_schema_name_over_the_length_cap());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_error_classification::migrate_rejects_an_invalid_schema_name_before_touching_the_database",
            move || {
                rt.block_on(migrate_rejects_an_invalid_schema_name_before_touching_the_database());
                Ok(())
            },
        ),
    ]
}
