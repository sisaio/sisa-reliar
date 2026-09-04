#![allow(dead_code)]
//! Shared test fixtures for the single-binary Postgres harness (RELIAR-27): a minimal `Message`
//! body, two `Serializer` fixtures, and the real-Postgres harness itself (§8.2).
//!
//! **The one thing that changed for RELIAR-27**: the shared `ContainerAsync` is never stored in
//! a `static`. A `static` is never dropped at process exit, so `Drop` — the *only* container
//! removal path in `testcontainers` 0.27 (there is no Ryuk/reaper) — never ran; combined with
//! one container-starting binary per scenario file, that leaked one Postgres container (with
//! its volumes) per file, per run. [`start_shared_container`] instead returns the container to
//! its caller (`tests/postgres/main.rs`), which keeps it as a **local** for the process's whole
//! lifetime and drops it explicitly before exiting — see that file's module docs.

use std::sync::LazyLock;

use reliar_core::{ContentType, Message, Serializer};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use testcontainers::ContainerAsync;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

/// A minimal message body used across scenario files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrderCreated {
    pub order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// A second `Serializer` with a **non-JSON** `ContentType`, so §43.A.4's round-trip equality
/// (`acquired.content_type == store.content_type()`) is proven for a store that is not `JSON`,
/// not just by coincidence with the default. Encodes as JSON under the hood — only the declared
/// `ContentType` differs — so the test body stays a plain `Message`.
#[derive(Clone, Debug, Default)]
pub(crate) struct TestVndSerializer;

#[derive(Debug)]
pub(crate) struct TestVndSerializerError(String);

impl std::fmt::Display for TestVndSerializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "test serializer error: {}", self.0)
    }
}

impl std::error::Error for TestVndSerializerError {}

impl Serializer for TestVndSerializer {
    type Error = TestVndSerializerError;

    fn content_type(&self) -> &ContentType {
        static CONTENT_TYPE: LazyLock<ContentType> =
            LazyLock::new(|| ContentType::parse("application/vnd.reliar-test+json").unwrap());
        &CONTENT_TYPE
    }

    fn serialize<T: Message>(&self, body: &T) -> Result<bytes::Bytes, Self::Error> {
        serde_json::to_vec(body)
            .map(bytes::Bytes::from)
            .map_err(|err| TestVndSerializerError(err.to_string()))
    }

    fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        serde_json::from_slice(bytes).map_err(|err| TestVndSerializerError(err.to_string()))
    }
}

/// A `Serializer` whose `serialize` always fails — proves `enqueue`'s
/// `EnqueueError::Serialize` path never reaches the database at all (no partial `INSERT`, no
/// SQL round trip for a body the serializer itself rejected).
#[derive(Clone, Debug, Default)]
pub(crate) struct AlwaysFailingSerializer;

#[derive(Debug)]
pub(crate) struct AlwaysFailingSerializerError;

impl std::fmt::Display for AlwaysFailingSerializerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "this serializer always fails, by design")
    }
}

impl std::error::Error for AlwaysFailingSerializerError {}

impl Serializer for AlwaysFailingSerializer {
    type Error = AlwaysFailingSerializerError;

    fn content_type(&self) -> &ContentType {
        &ContentType::JSON
    }

    fn serialize<T: Message>(&self, _body: &T) -> Result<bytes::Bytes, Self::Error> {
        Err(AlwaysFailingSerializerError)
    }

    fn deserialize<T: Message>(&self, _bytes: &[u8]) -> Result<T, Self::Error> {
        Err(AlwaysFailingSerializerError)
    }
}

static ADMIN_URL: OnceCell<String> = OnceCell::const_new();

/// Starts the shared Postgres container this whole binary's scenarios use (unless `DATABASE_URL`
/// is set, e.g. CI's service container), and records its admin connection URL for every
/// [`fresh_db`]/[`fresh_unmigrated_db`] call. Returns the container itself — **the caller
/// (`main`) owns it as a local for the rest of the process's life and must drop it explicitly
/// before exiting** (RELIAR-27); this function never stashes it anywhere longer-lived than that.
///
/// Must run to completion exactly once, before any trial touches Postgres.
pub(crate) async fn start_shared_container() -> Option<ContainerAsync<Postgres>> {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        ADMIN_URL
            .set(url)
            .expect("start_shared_container must run exactly once");
        return None;
    }
    // `reliar-` name prefix + `reliar.test=true` label (review 4 major 3, RELIAR-27): the
    // built-in `org.testcontainers.managed-by=testcontainers` label alone is too broad for
    // the manual sweep in `CONTRIBUTING.md` to key on — it would also match a different project's
    // testcontainers-managed containers on the same Docker host. Both together are what let the
    // sweep (and a human skimming `docker ps`) tell "this crate's leftovers" apart from
    // anything else testcontainers-rs is managing.
    let container = Postgres::default()
        .with_tag("18-alpine")
        .with_container_name(format!("reliar-pg-{}", uuid::Uuid::now_v7().simple()))
        .with_label("reliar.test", "true")
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    ADMIN_URL
        .set(url)
        .expect("start_shared_container must run exactly once");
    Some(container)
}

fn admin_url() -> &'static str {
    ADMIN_URL
        .get()
        .expect("start_shared_container must run before any scenario touches Postgres")
}

/// Creates a fresh, empty, uniquely named database on the admin connection and returns
/// [`PgConnectOptions`] pointing at it (no `search_path` set yet).
async fn create_fresh_database() -> PgConnectOptions {
    let admin = PgPool::connect(admin_url())
        .await
        .expect("connect to admin database");
    let name = format!("t_{}", uuid::Uuid::now_v7().simple());
    // `CREATE DATABASE` cannot take a bind parameter; `name` is a freshly generated UUIDv7, never
    // user input, so this is the one sanctioned exception to "macros only" (test code, not the
    // crate) — asserted safe explicitly via `AssertSqlSafe` (sqlx 0.9's SQL-injection audit gate).
    sqlx::query(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
        .execute(&admin)
        .await
        .expect("create test database");

    let options: PgConnectOptions = admin_url()
        .parse()
        .expect("admin url parses as PgConnectOptions");
    options.database(&name)
}

/// A fresh, empty database — **not yet migrated**. For tests that exercise
/// construction/migration failure paths before `migrate()` has run.
pub(crate) async fn fresh_unmigrated_db() -> PgPool {
    PgPool::connect_with(create_fresh_database().await)
        .await
        .expect("connect to fresh database")
}

/// Migrated once per process; every [`fresh_db`] call clones it via `CREATE DATABASE …
/// TEMPLATE …` instead of re-running `migrate()` — a storage-level file copy is materially
/// faster than replaying DDL for every test (§8.2, reviewer note on RELIAR-16).
static TEMPLATE_NAME: OnceCell<String> = OnceCell::const_new();

async fn template_name() -> &'static str {
    TEMPLATE_NAME
        .get_or_init(|| async {
            let options = create_fresh_database().await;
            let name = options.get_database().unwrap().to_owned();
            let pool = PgPool::connect_with(options)
                .await
                .expect("connect to template database");
            reliar_store_postgres::migrate(&pool, reliar_store_postgres::MigrateOptions::default())
                .await
                .expect("migrate the template database");
            // `CREATE DATABASE … TEMPLATE` refuses a source with open connections; closing the
            // pool here is what makes every later clone safe.
            pool.close().await;
            name
        })
        .await
}

/// A fresh database, cloned from the migrated [`template_name`], with `search_path` set on the
/// returned pool itself (§24) so the store's startup verification and every query resolve
/// `outbox` in the default `reliar` schema with **no** URL/role configuration — the equivalent
/// of a host putting `reliar` first on its connection URL.
pub(crate) async fn fresh_db() -> PgPool {
    let admin = PgPool::connect(admin_url())
        .await
        .expect("connect to admin database");
    let name = format!("t_{}", uuid::Uuid::now_v7().simple());
    let template = template_name().await;
    // Same sanctioned `AssertSqlSafe` exception as `create_fresh_database`: both names are
    // freshly generated UUIDv7s, never user input.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        r#"CREATE DATABASE "{name}" TEMPLATE "{template}""#
    )))
    .execute(&admin)
    .await
    .expect("clone the migrated template database");

    let options: PgConnectOptions = admin_url()
        .parse()
        .expect("admin url parses as PgConnectOptions");
    PgPool::connect_with(
        options
            .database(&name)
            .options([("search_path", "reliar,public")]),
    )
    .await
    .expect("connect with search_path set")
}

/// Enqueues `n` plain [`OrderCreated`] envelopes and returns their [`reliar_core::MessageId`]s
/// in enqueue order.
pub(crate) async fn seed<Ser: reliar_core::Serializer + Send + Sync + 'static>(
    store: &reliar_store_postgres::PostgresOutboxStore<Ser>,
    pool: &PgPool,
    n: u64,
) -> Vec<reliar_core::Envelope<OrderCreated>> {
    let mut envelopes = Vec::with_capacity(n as usize);
    for i in 0..n {
        let envelope = reliar_core::Envelope::builder(OrderCreated { order_id: i }).build();
        let mut tx = pool.begin().await.unwrap();
        store.enqueue(&mut tx, &envelope).await.unwrap();
        tx.commit().await.unwrap();
        envelopes.push(envelope);
    }
    envelopes
}

/// Moves `id`'s `locked_until` into the past — SQL time-travel for lease-expiry tests, never a
/// wall-clock sleep (§8.2).
pub(crate) async fn expire_lease(pool: &PgPool, id: uuid::Uuid) {
    sqlx::query("UPDATE outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}

/// Moves `id`'s `available_at` into the past — makes a retry-delayed row due without waiting.
pub(crate) async fn make_available_now(pool: &PgPool, id: uuid::Uuid) {
    sqlx::query("UPDATE outbox SET available_at = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
}
