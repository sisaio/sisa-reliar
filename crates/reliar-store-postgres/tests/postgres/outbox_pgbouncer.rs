//! §43.A.35 (`PgBouncer` half), decision #28 — a transaction-mode pooler drops startup `options`,
//! so `search_path` must come from a server-side default (`ALTER ROLE … SET search_path`).
//! Proves: a store behind the pooler **without** that default fails fast at `connect` (URL
//! `options` were silently dropped); with it, the full enqueue → claim → complete path, and
//! concurrent `SKIP LOCKED` claims, behave exactly as they do direct.
//!
//! One dedicated Postgres + two `PgBouncer` instances sharing its network: `PgBouncer`'s own
//! transaction-mode connection pooling would otherwise let a *first* server connection
//! (authenticated before `ALTER ROLE` ran) linger with the old `search_path` and get reused for
//! the "after" half, which is exactly the ambiguity a second, fresh `PgBouncer` instance avoids.

use std::time::Duration;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::{AcquireRequest, CompletedMessage, OutboxStore, PurgeRequest, WorkerId};
use reliar_store_postgres::{PostgresOutboxSettings, PostgresOutboxStore, PostgresStoreError};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use testcontainers::core::ContainerRequest;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

const PGBOUNCER_IMAGE: &str = "edoburu/pgbouncer";
const PGBOUNCER_TAG: &str = "v1.25.2-p0";

fn pgbouncer(network: &str, pg_host: &str) -> ContainerRequest<GenericImage> {
    // `reliar-` name prefix + `reliar.test=true` label (review 4 major 3, RELIAR-27): the same
    // reasoning as the shared container in `tests/postgres/common/mod.rs` — the sweep in
    // `scripts/test.sh` keys on both so it only ever touches this crate's own leftovers.
    GenericImage::new(PGBOUNCER_IMAGE, PGBOUNCER_TAG)
        .with_exposed_port(5432.tcp())
        .with_wait_for(WaitFor::millis(1500))
        .with_env_var("DB_HOST", pg_host)
        .with_env_var("DB_PORT", "5432")
        .with_env_var("DB_USER", "postgres")
        .with_env_var("DB_NAME", "postgres")
        .with_env_var("DB_PASSWORD", "postgres")
        .with_env_var("POOL_MODE", "transaction")
        .with_env_var("AUTH_TYPE", "scram-sha-256")
        .with_env_var("MAX_CLIENT_CONN", "50")
        .with_env_var("DEFAULT_POOL_SIZE", "10")
        .with_network(network)
        .with_container_name(format!(
            "reliar-pgbouncer-{}",
            uuid::Uuid::now_v7().simple()
        ))
        .with_label("reliar.test", "true")
}

#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end pooler scenario: two container topologies (before/after ALTER \
              ROLE) plus the full enqueue/claim/complete path; splitting it would scatter one \
              ordered narrative across helper functions with no reuse"
)]
async fn transaction_mode_pooler_needs_alter_role_and_then_behaves_like_direct() {
    let network = format!("reliar-pooler-{}", uuid::Uuid::now_v7().simple());
    let pg_name = format!("reliar-pg-{}", uuid::Uuid::now_v7().simple());

    let pg = Postgres::default()
        .with_tag("18-alpine")
        .with_container_name(&pg_name)
        .with_network(&network)
        .with_label("reliar.test", "true")
        .start()
        .await
        .expect("start postgres");
    let pg_direct_port = pg.get_host_port_ipv4(5432).await.expect("postgres port");
    let direct_url = format!("postgres://postgres:postgres@127.0.0.1:{pg_direct_port}/postgres");

    // DDL runs direct — `migrate()` opens and manages its own connection, and a transaction-mode
    // pooler is not the place to run a migrator's advisory lock anyway.
    let direct_pool = PgPool::connect(&direct_url).await.expect("connect direct");
    reliar_store_postgres::migrate(
        &direct_pool,
        reliar_store_postgres::MigrateOptions::default(),
    )
    .await
    .expect("migrate direct");

    // --- Before `ALTER ROLE`: a dedicated pooler instance whose URL `options` claim a
    // search_path the pooler will silently drop, so construction must fail fast. ---
    let bouncer_before = pgbouncer(&network, &pg_name)
        .start()
        .await
        .expect("start pgbouncer (before)");
    let before_port = bouncer_before
        .get_host_port_ipv4(5432)
        .await
        .expect("pgbouncer (before) port");
    let before_options: PgConnectOptions =
        format!("postgres://postgres:postgres@127.0.0.1:{before_port}/postgres")
            .parse()
            .unwrap();
    let before_options = before_options.options([("search_path", "reliar,public")]);
    // This transaction-mode `PgBouncer` build rejects an unrecognised startup `options` parameter
    // outright (`08P01 unsupported startup parameter`) rather than silently dropping it — an
    // even earlier, stronger form of the documented "URL options do not survive a
    // transaction-mode pooler" failure (decision #28): the connection itself never completes.
    let connect_err = PgPool::connect_with(before_options).await.unwrap_err();
    assert!(
        connect_err.to_string().contains("startup parameter")
            || connect_err.to_string().contains("options"),
        "expected the pooler to reject the search_path startup option, got {connect_err:?}"
    );

    // A bare connection (no `options` at all) still fails fast at construction, since the role
    // default has not been set yet — `search_path` falls back to `"$user", public`, which does
    // not contain `outbox`.
    let bare_options: PgConnectOptions =
        format!("postgres://postgres:postgres@127.0.0.1:{before_port}/postgres")
            .parse()
            .unwrap();
    let bare_pool = PgPool::connect_with(bare_options)
        .await
        .expect("connect through pgbouncer without options");
    let err = PostgresOutboxStore::new(bare_pool).await.unwrap_err();
    assert!(
        matches!(
            err,
            PostgresStoreError::SchemaResolution { .. } | PostgresStoreError::NotMigrated { .. }
        ),
        "expected a fail-fast schema error before the role default is set, got {err:?}"
    );

    // --- Server-side default: the pooler-portable alternative (decision #28, ADR 0017). ---
    sqlx::query("ALTER ROLE postgres SET search_path = reliar, public")
        .execute(&direct_pool)
        .await
        .expect("alter role");

    // A fresh `PgBouncer` instance so every server connection it ever opens authenticates *after*
    // the new role default is in place — the pooled connection reuse this sidesteps.
    let bouncer_after = pgbouncer(&network, &pg_name)
        .start()
        .await
        .expect("start pgbouncer (after)");
    let after_port = bouncer_after
        .get_host_port_ipv4(5432)
        .await
        .expect("pgbouncer (after) port");
    // No URL `options` at all this time — the point is that none are needed.
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{after_port}/postgres"
    ))
    .await
    .expect("connect through pgbouncer (after)");

    let store = PostgresOutboxStore::with_settings(pool.clone(), PostgresOutboxSettings::default())
        .await
        .expect("construction succeeds once the role default is in place");

    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let worker_a = WorkerId::generate();
    let worker_b = WorkerId::generate();
    let (batch_a, batch_b) = tokio::join!(
        store.acquire(AcquireRequest::new(worker_a.clone()).lease(Duration::from_secs(30))),
        store.acquire(AcquireRequest::new(worker_b.clone()).lease(Duration::from_secs(30))),
    );
    let batch_a = batch_a.unwrap();
    let batch_b = batch_b.unwrap();
    let claimed = batch_a.records.len() + batch_b.records.len();
    assert_eq!(
        claimed, 1,
        "SKIP LOCKED still gives exactly one worker the row through a pooler"
    );

    let (record, worker) = if batch_a.records.len() == 1 {
        (&batch_a.records[0], &worker_a)
    } else {
        (&batch_b.records[0], &worker_b)
    };
    assert_eq!(record.envelope.id, envelope.id);
    assert!(record.locked_until.is_some());

    let affected = store
        .complete(worker, &[CompletedMessage::new(record.message_ref())])
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // Review 4 minor — `purge` itself was never exercised through this pooler (only
    // enqueue/acquire/complete were); backdate the just-completed row and confirm it's swept.
    sqlx::query("UPDATE outbox SET published_at = now() - interval '1 hour' WHERE id = $1")
        .bind(record.envelope.id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let report = store
        .purge(PurgeRequest::default().published_retention(Some(Duration::ZERO)))
        .await
        .unwrap();
    assert_eq!(
        report.published_deleted, 1,
        "purge works through the pooler too"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_pgbouncer::transaction_mode_pooler_needs_alter_role_and_then_behaves_like_direct",
        move || {
            rt.block_on(transaction_mode_pooler_needs_alter_role_and_then_behaves_like_direct());
            Ok(())
        },
    )]
}
