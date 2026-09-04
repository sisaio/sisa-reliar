//! §43.A.24 — `migrate()` creates schema `reliar`, is idempotent, serializes concurrent
//! callers, records versions in Reliar's own `_migrations` bookkeeping table (never
//! `_sqlx_migrations`), and is never invoked implicitly; an unmigrated pool has no `outbox`.

use crate::common;

use reliar_store_postgres::{MigrateOptions, migrate};

async fn migrate_creates_schema_and_bookkeeping_table() {
    let pool = common::fresh_unmigrated_db().await;

    migrate(&pool, MigrateOptions::default())
        .await
        .expect("first migrate succeeds");

    let outbox_exists: bool = sqlx::query_scalar("SELECT to_regclass('reliar.outbox') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(outbox_exists, "migrate() must create reliar.outbox");

    let bookkeeping_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('reliar._migrations') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        bookkeeping_exists,
        "migrate() must record versions in reliar._migrations, never _sqlx_migrations"
    );

    let host_migrations_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('public._sqlx_migrations') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(
        !host_migrations_exists,
        "migrate() must never write to the shared, one-per-database _sqlx_migrations table"
    );
}

async fn migrate_is_idempotent() {
    let pool = common::fresh_unmigrated_db().await;
    let options = MigrateOptions::default();

    migrate(&pool, options).await.expect("first call");
    migrate(&pool, options)
        .await
        .expect("second call observes Ok(())");
}

async fn migrate_serializes_concurrent_callers() {
    let pool = common::fresh_unmigrated_db().await;
    let options = MigrateOptions::default();

    let (first, second) = tokio::join!(migrate(&pool, options), migrate(&pool, options));
    first.expect("first concurrent caller succeeds");
    second.expect("second concurrent caller succeeds, serialized by the advisory lock");

    let outbox_exists: bool = sqlx::query_scalar("SELECT to_regclass('reliar.outbox') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(outbox_exists);
}

/// Review 4 major 1 — `migrate()` used to run its `SET search_path` on a connection borrowed
/// from the *caller's* pool via `pool.acquire()`; sqlx 0.9 does not reset session-level GUCs
/// when a connection is released back to a pool, so that connection would silently keep
/// resolving unqualified names against the migrated schema for the rest of its life, for any
/// unrelated query the host later happened to run on it. Proven here by holding several
/// connections from the same pool open *simultaneously* (forcing the pool to open that many
/// distinct physical connections, not just reuse one) both before and after `migrate()`, and
/// asserting every one of them still reports the pool's original `search_path`.
async fn migrate_does_not_leak_search_path_into_the_callers_pool_connections() {
    let pool = common::fresh_unmigrated_db().await;

    let original: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(&pool)
        .await
        .unwrap();

    migrate(&pool, MigrateOptions::default().schema("tenant_a"))
        .await
        .expect("migrate into a non-default schema");

    let mut held = Vec::new();
    for _ in 0..5 {
        held.push(pool.acquire().await.unwrap());
    }
    for mut conn in held {
        let observed: String = sqlx::query_scalar("SELECT current_setting('search_path')")
            .fetch_one(&mut *conn)
            .await
            .unwrap();
        assert_eq!(
            observed, original,
            "a pool connection's search_path must be untouched by migrate()"
        );
    }
}

async fn unmigrated_pool_has_no_outbox_table() {
    let pool = common::fresh_unmigrated_db().await;

    let outbox_exists: bool = sqlx::query_scalar("SELECT to_regclass('reliar.outbox') IS NOT NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(
        !outbox_exists,
        "a pool nobody has called migrate() on must have no outbox table"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "migrate::migrate_creates_schema_and_bookkeeping_table",
            move || {
                rt.block_on(migrate_creates_schema_and_bookkeeping_table());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test("migrate::migrate_is_idempotent", move || {
            rt.block_on(migrate_is_idempotent());
            Ok(())
        }),
        libtest_mimic::Trial::test(
            "migrate::migrate_serializes_concurrent_callers",
            move || {
                rt.block_on(migrate_serializes_concurrent_callers());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test("migrate::unmigrated_pool_has_no_outbox_table", move || {
            rt.block_on(unmigrated_pool_has_no_outbox_table());
            Ok(())
        }),
        libtest_mimic::Trial::test(
            "migrate::migrate_does_not_leak_search_path_into_the_callers_pool_connections",
            move || {
                rt.block_on(migrate_does_not_leak_search_path_into_the_callers_pool_connections());
                Ok(())
            },
        ),
    ]
}
