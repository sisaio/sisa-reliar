//! §43.A.35, decision #28 as amended by **decision #31** — `PgDog` is *the* transaction-mode
//! pooler this contract runs behind. (A second pooler image ran the same assertions until
//! decision #31 retired it: it proved the same property at twice the container cost, and its one
//! unique case — "the pooler dropped the URL `options`, so construction must fail fast" — needs
//! no pooler at all to assert, and is covered below and by
//! `outbox_schema_verification::construction_fails_fast_without_search_path`.)
//!
//! This `PgDog` build was found empirically — against a live container, since its config schema
//! is not documented anywhere reachable from this crate — to **pass the startup `options`
//! parameter through to the upstream server** rather than rejecting or silently dropping it, so
//! the URL-`options` `search_path` path Reliar's docs recommend as the primary mechanism works
//! unmodified. The `ALTER ROLE` server-side default is exercised too, since decision #28 requires
//! it as the portable fallback for any pooler that drops those options.
//!
//! What the scenario asserts, in order: URL-`options` pass-through; fail-fast with neither
//! options nor a role default; the `ALTER ROLE` fallback; then the full store path through the
//! pooler — transactional `enqueue`, concurrent `SKIP LOCKED` `acquire`, `complete`, `fail`
//! (retry) and reclaim, `extend_lease`/`release`, and `purge`.
//!
//! `PgDog` is configured by two mounted TOML files, written below in the same credential-free
//! `[[databases]]` shape as `deploy/compose/configs/pgdog.toml` — the upstream password lives
//! only in `[[users]]`. The image bundles no example config, so the schema was recovered by
//! round-tripping deliberately invalid fields through `pgdog configcheck`, which echoes every
//! field serde expects.

use std::io::Write as _;
use std::time::Duration;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::{
    AcquireRequest, CompletedMessage, FailedMessage, FailureOutcome, OutboxStore, PurgeRequest,
    WorkerId,
};
use reliar_store_postgres::{PostgresOutboxSettings, PostgresOutboxStore, PostgresStoreError};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use testcontainers::core::{IntoContainerPort, Mount, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;

// Equal to `deploy/compose/docker-compose.yaml`'s pin by decision #31, enforced by ci.yaml's
// "compose and the tests pin the same images" step — bump both together.
const PGDOG_IMAGE: &str = "ghcr.io/pgdogdev/pgdog";
const PGDOG_TAG: &str = "v0.1.46";

/// Writes `pgdog.toml` + `users.toml` into a fresh directory under `std::env::temp_dir()` and
/// returns the directory. `pg_host` is the Postgres container's network alias — `PgDog` dials it
/// directly by container name over the shared Docker network.
fn write_pgdog_config(pg_host: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("reliar-pgdog-{}", uuid::Uuid::now_v7().simple()));
    std::fs::create_dir_all(&dir).expect("create pgdog config dir");

    let pgdog_toml = format!(
        r#"[general]
host = "0.0.0.0"
port = 6432
pooler_mode = "transaction"

[[databases]]
name = "postgres"
host = "{pg_host}"
port = 5432
database_name = "postgres"
user = "postgres"
"#
    );
    let users_toml = r#"[[users]]
name = "postgres"
database = "postgres"
password = "postgres"
"#;

    let mut f = std::fs::File::create(dir.join("pgdog.toml")).unwrap();
    f.write_all(pgdog_toml.as_bytes()).unwrap();
    let mut f = std::fs::File::create(dir.join("users.toml")).unwrap();
    f.write_all(users_toml.as_bytes()).unwrap();

    dir
}

#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end pooler scenario: URL-options pass-through, the ALTER ROLE fallback, \
              and the full enqueue/acquire/complete/fail/purge path — splitting it would scatter \
              one ordered narrative across helper functions with no reuse"
)]
async fn pgdog_pooler_passes_options_through_and_behaves_like_direct() {
    let network = format!("reliar-pgdog-{}", uuid::Uuid::now_v7().simple());
    let pg_name = format!("reliar-pg-{}", uuid::Uuid::now_v7().simple());

    // `reliar-` name prefix + `reliar.test=true` label on every container this scenario starts
    // (review 4 major 3, RELIAR-27): lets the manual sweep documented in `CONTRIBUTING.md` key on
    // both, so it only ever touches this crate's own leftovers.
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

    // DDL runs direct: `migrate()` manages its own connection and holds a session-level advisory
    // lock on it, and a transaction-mode pooler can hand that session's statements to different
    // server connections mid-migration (ADR 0018).
    let direct_pool = PgPool::connect(&direct_url).await.expect("connect direct");
    reliar_store_postgres::migrate(
        &direct_pool,
        reliar_store_postgres::MigrateOptions::default(),
    )
    .await
    .expect("migrate direct");

    let config_dir = write_pgdog_config(&pg_name);

    // --- Pass 1: URL `options` alone, no `ALTER ROLE` yet. ---
    let pgdog_before = GenericImage::new(PGDOG_IMAGE, PGDOG_TAG)
        .with_exposed_port(6432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("PgDog listening on"))
        .with_network(&network)
        .with_mount(Mount::bind_mount(
            config_dir.join("pgdog.toml").to_string_lossy().into_owned(),
            "/pgdog/pgdog.toml",
        ))
        .with_mount(Mount::bind_mount(
            config_dir.join("users.toml").to_string_lossy().into_owned(),
            "/pgdog/users.toml",
        ))
        .with_container_name(format!(
            "reliar-pgdog-before-{}",
            uuid::Uuid::now_v7().simple()
        ))
        .with_label("reliar.test", "true")
        .start()
        .await
        .expect("start pgdog (before ALTER ROLE)");
    let before_port = pgdog_before
        .get_host_port_ipv4(6432)
        .await
        .expect("pgdog (before) port");

    let options_url: PgConnectOptions =
        format!("postgres://postgres:postgres@127.0.0.1:{before_port}/postgres")
            .parse()
            .unwrap();
    let options_url = options_url.options([("search_path", "reliar,public")]);
    let options_pool = PgPool::connect_with(options_url)
        .await
        .expect("connect through pgdog with URL options");
    // The documented finding: this `PgDog` build passes the startup `options` parameter through
    // to the upstream server rather than rejecting it (a pooler that refuses an unrecognised
    // startup parameter answers `08P01`), so construction succeeds without `ALTER ROLE` at all.
    PostgresOutboxStore::new(options_pool)
        .await
        .expect("PgDog passes URL options through, so search_path resolves without ALTER ROLE");

    // A bare connection through the same instance, with no `options` and no role default yet,
    // still fails fast — proving the pass-through is really carrying `search_path`, not just
    // coincidentally finding `outbox` some other way.
    let bare_url: PgConnectOptions =
        format!("postgres://postgres:postgres@127.0.0.1:{before_port}/postgres")
            .parse()
            .unwrap();
    let bare_pool = PgPool::connect_with(bare_url)
        .await
        .expect("connect through pgdog without options");
    let err = PostgresOutboxStore::new(bare_pool).await.unwrap_err();
    assert!(
        matches!(
            err,
            PostgresStoreError::SchemaResolution { .. } | PostgresStoreError::NotMigrated { .. }
        ),
        "expected a fail-fast schema error with neither options nor a role default, got {err:?}"
    );

    // --- Server-side default: the portable fallback decision #28 requires for every pooler. ---
    sqlx::query("ALTER ROLE postgres SET search_path = reliar, public")
        .execute(&direct_pool)
        .await
        .expect("alter role");

    // A fresh `PgDog` instance so every server connection it opens authenticates *after* the
    // role default is set. A reused server connection authenticated *before* the `ALTER ROLE`
    // would still carry the old `search_path`, which is the ambiguity a fresh instance removes.
    let pgdog_after = GenericImage::new(PGDOG_IMAGE, PGDOG_TAG)
        .with_exposed_port(6432.tcp())
        .with_wait_for(WaitFor::message_on_stderr("PgDog listening on"))
        .with_network(&network)
        .with_mount(Mount::bind_mount(
            config_dir.join("pgdog.toml").to_string_lossy().into_owned(),
            "/pgdog/pgdog.toml",
        ))
        .with_mount(Mount::bind_mount(
            config_dir.join("users.toml").to_string_lossy().into_owned(),
            "/pgdog/users.toml",
        ))
        .with_container_name(format!(
            "reliar-pgdog-after-{}",
            uuid::Uuid::now_v7().simple()
        ))
        .with_label("reliar.test", "true")
        .start()
        .await
        .expect("start pgdog (after ALTER ROLE)");
    let after_port = pgdog_after
        .get_host_port_ipv4(6432)
        .await
        .expect("pgdog (after) port");

    // No URL `options` this time — the point of `ALTER ROLE` is that none are needed.
    let pool = PgPool::connect(&format!(
        "postgres://postgres:postgres@127.0.0.1:{after_port}/postgres"
    ))
    .await
    .expect("connect through pgdog (after)");
    let store = PostgresOutboxStore::with_settings(pool.clone(), PostgresOutboxSettings::default())
        .await
        .expect("construction succeeds once the role default is in place");

    // --- Full path through the pooler: enqueue, concurrent SKIP LOCKED acquire, complete, ---
    // --- fail/retry, lease extend/release, purge. ---
    let envelope_a = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let envelope_b = Envelope::builder(OrderCreated { order_id: 2 }).build();
    for envelope in [&envelope_a, &envelope_b] {
        let mut tx = pool.begin().await.unwrap();
        store.enqueue(&mut tx, envelope).await.unwrap();
        tx.commit().await.unwrap();
    }

    let worker_a = WorkerId::generate();
    let worker_b = WorkerId::generate();
    let (batch_a, batch_b) = tokio::join!(
        store.acquire(AcquireRequest::new(worker_a.clone()).lease(Duration::from_secs(30))),
        store.acquire(AcquireRequest::new(worker_b.clone()).lease(Duration::from_secs(30))),
    );
    let batch_a = batch_a.unwrap();
    let batch_b = batch_b.unwrap();
    assert_eq!(
        batch_a.records.len() + batch_b.records.len(),
        2,
        "SKIP LOCKED still partitions the seed disjointly and exhaustively through PgDog"
    );

    let mut all_records: Vec<_> = batch_a.records.into_iter().chain(batch_b.records).collect();
    all_records.sort_by_key(|r| r.envelope.id);
    let (record_complete, record_fail) = (all_records[0].clone(), all_records[1].clone());

    // `complete` on one row.
    let owner_a = if record_complete.locked_by.as_ref() == Some(&worker_a) {
        &worker_a
    } else {
        &worker_b
    };
    let affected = store
        .complete(
            owner_a,
            &[CompletedMessage::new(record_complete.message_ref())],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // `fail` (retry) on the other, then extend/release its next lease.
    let owner_b = if record_fail.locked_by.as_ref() == Some(&worker_a) {
        &worker_a
    } else {
        &worker_b
    };
    let affected = store
        .fail(
            owner_b,
            &[FailedMessage::new(
                record_fail.message_ref(),
                "transient failure through pgdog",
                FailureOutcome::Retry {
                    delay: Duration::from_millis(1),
                },
            )],
        )
        .await
        .unwrap();
    assert_eq!(affected, 1);

    // Make it due (SQL time-travel, no wall-clock sleep) and reclaim it.
    sqlx::query("UPDATE outbox SET available_at = now() - interval '1 second' WHERE id = $1")
        .bind(record_fail.envelope.id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let batch_retry = store
        .acquire(AcquireRequest::new(WorkerId::generate()).lease(Duration::from_secs(30)))
        .await
        .unwrap();
    let retried = batch_retry
        .records
        .iter()
        .find(|r| r.envelope.id == record_fail.envelope.id)
        .expect("the retried row is claimable again through pgdog");
    assert_eq!(retried.attempts, 1);
    let retry_worker = retried.locked_by.clone().unwrap();

    // Lease extend/release still behave through the pooler.
    let extended = store
        .extend_lease(
            &retry_worker,
            &[retried.message_ref()],
            Duration::from_secs(120),
        )
        .await
        .unwrap();
    assert_eq!(extended, 1);
    let released = store
        .release(&retry_worker, &[retried.message_ref()])
        .await
        .unwrap();
    assert_eq!(released, 1);

    // `purge`: backdate the completed row's `published_at` and confirm it is swept.
    sqlx::query("UPDATE outbox SET published_at = now() - interval '1 hour' WHERE id = $1")
        .bind(record_complete.envelope.id.as_uuid())
        .execute(&pool)
        .await
        .unwrap();
    let report = store
        .purge(PurgeRequest::default().published_retention(Some(Duration::ZERO)))
        .await
        .unwrap();
    assert_eq!(
        report.published_deleted, 1,
        "purge works through the pooler"
    );

    // Review 3 minor: the mounted config directory under `std::env::temp_dir()` was never
    // cleaned up. Both containers have their config loaded at process start (not re-read live),
    // so it's safe to drop them — releasing the bind mount — before removing the directory;
    // best-effort (`let _ =`), since a leaked temp dir is a cleanliness issue, not a test
    // correctness one, and is never worth failing the test over.
    drop(pgdog_before);
    drop(pgdog_after);
    let _ = std::fs::remove_dir_all(&config_dir);
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "outbox_pgdog::pgdog_pooler_passes_options_through_and_behaves_like_direct",
        move || {
            rt.block_on(pgdog_pooler_passes_options_through_and_behaves_like_direct());
            Ok(())
        },
    )]
}
