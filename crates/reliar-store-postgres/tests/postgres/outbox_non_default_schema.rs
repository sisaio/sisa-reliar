//! §43.A.31 — `migrate(&pool, MigrateOptions { schema })` into a **non-default** schema puts
//! both `outbox` and `_migrations` in it (nothing Reliar owns lands in `public`); a pool whose
//! URL carries that schema first on `search_path` then runs enqueue → claim with no extra
//! statement, while `PostgresOutboxStore::new` over a pool *without* that `search_path` fails at
//! construction.

use crate::common;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::{MigrateOptions, PostgresOutboxSettings, PostgresOutboxStore, migrate};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;

const CUSTOM_SCHEMA: &str = "acme_reliar";

async fn non_default_schema_end_to_end() {
    let base = common::fresh_unmigrated_db().await;
    migrate(&base, MigrateOptions::default().schema(CUSTOM_SCHEMA))
        .await
        .expect("migrate into a non-default schema");

    let outbox_in_custom_schema: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
           WHERE table_schema = $1 AND table_name = 'outbox')",
    )
    .bind(CUSTOM_SCHEMA)
    .fetch_one(&base)
    .await
    .unwrap();
    assert!(outbox_in_custom_schema);

    let migrations_in_custom_schema: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.tables \
           WHERE table_schema = $1 AND table_name = '_migrations')",
    )
    .bind(CUSTOM_SCHEMA)
    .fetch_one(&base)
    .await
    .unwrap();
    assert!(migrations_in_custom_schema);

    let anything_in_public: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables \
           WHERE table_schema = 'public' AND table_name IN ('outbox', '_migrations')",
    )
    .fetch_one(&base)
    .await
    .unwrap();
    assert_eq!(
        anything_in_public, 0,
        "nothing Reliar owns may land in public"
    );

    // A pool whose connection carries the custom schema first on search_path runs the full
    // path with no extra statement (enqueue_sets_search_path stays false).
    let base_options: PgConnectOptions = base.connect_options().as_ref().clone();
    let scoped_pool = PgPool::connect_with(
        base_options
            .clone()
            .options([("search_path", &format!("{CUSTOM_SCHEMA},public"))]),
    )
    .await
    .unwrap();

    let settings = PostgresOutboxSettings::default().schema(CUSTOM_SCHEMA);
    let store = PostgresOutboxStore::with_settings(scoped_pool.clone(), settings)
        .await
        .expect("construction succeeds when search_path matches the configured schema");

    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let mut tx = scoped_pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].envelope.id, envelope.id);

    // A pool without the custom schema on its search_path fails fast at construction.
    let unscoped_pool = PgPool::connect_with(base_options).await.unwrap();
    let settings = PostgresOutboxSettings::default().schema(CUSTOM_SCHEMA);
    let result = PostgresOutboxStore::with_settings(unscoped_pool, settings).await;
    assert!(
        result.is_err(),
        "construction must fail fast when the configured schema is not on search_path"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_non_default_schema::non_default_schema_end_to_end",
        move || {
            rt.block_on(non_default_schema_end_to_end());
            Ok(())
        },
    )]
}
