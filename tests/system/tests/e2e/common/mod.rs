#![allow(dead_code)]
//! Shared fixtures for the one Postgres-and-NATS-touching test binary (ADR 0031 §6): container
//! startup for both substrates, a fresh migrated database, a `JetStream` context, minimal
//! stream helpers, a seed helper for the outbox, and a bounded-poll `wait_until` — the same
//! shapes `reliar-store-postgres`'s and `reliar-transport-nats`'s own harnesses use, duplicated
//! here because this binary cannot see either crate's private `tests/*/common`.

use std::future::Future;
use std::time::Duration;

use reliar_core::{Envelope, JsonSerializer, Message, SerializedEnvelope, Serializer as _};
use reliar_outbox::DispatcherSettings;
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;

/// Dispatcher tuning shared by every scenario that just needs a fast, deterministic drain: a
/// 20ms poll — quick enough that `wait_until`'s own 20ms bounded polls do not become the long
/// pole — a lease comfortably longer than any scenario's run, and a short drain timeout. E2
/// layers its own backoff on top via `.retry(..)`; the claim-stop trial in `e1` builds its own
/// settings from scratch (`batch_size(1)` and a poll interval far longer than the trial's own
/// real-time budget), since reusing this shape would defeat the point.
pub(crate) fn fast_settings() -> DispatcherSettings {
    DispatcherSettings::default()
        .batch_size(10)
        .poll_interval(Duration::from_millis(20))
        .idle_poll_interval(Duration::from_millis(20))
        .lease(Duration::from_secs(30))
        .drain_timeout(Duration::from_secs(5))
}

/// A minimal message body shared by every scenario.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct OrderCreated {
    pub order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// A second, distinct message type — used by the routing scenarios (E5/E6, RELIAR-45) to prove a
/// disallowed/non-routed type publishes directly while `OrderCreated` stays durable.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct AuditLogged {
    pub event: String,
}

impl Message for AuditLogged {
    const TYPE: &'static str = "audit.logged";
    const VERSION: u16 = 1;
}

// ---------------------------------------------------------------------------------------------
// Postgres
// ---------------------------------------------------------------------------------------------

static ADMIN_URL_PG: OnceCell<String> = OnceCell::const_new();

/// Starts the shared Postgres container this whole binary's scenarios use (unless `DATABASE_URL`
/// is set, e.g. CI's service container — SRS §8.2), and records its admin connection URL for
/// every [`fresh_postgres_db`] call. Returns the container itself — **the caller (`main`) owns it
/// as a local for the rest of the process's life and must drop it explicitly before exiting**
/// (RELIAR-27).
///
/// Must run to completion exactly once, before any trial touches Postgres.
pub(crate) async fn start_shared_postgres(tag: &str) -> Option<ContainerAsync<Postgres>> {
    if let Ok(url) = std::env::var("DATABASE_URL") {
        ADMIN_URL_PG
            .set(url)
            .expect("start_shared_postgres must run exactly once");
        return None;
    }
    let container = Postgres::default()
        .with_tag(tag)
        .with_container_name(format!(
            "reliar-systest-pg-{}",
            uuid::Uuid::now_v7().simple()
        ))
        .with_label("reliar.test", "true")
        .start()
        .await
        .expect("start postgres container");
    let port = container
        .get_host_port_ipv4(5432)
        .await
        .expect("mapped port");
    let url = format!("postgres://postgres:postgres@127.0.0.1:{port}/postgres");
    ADMIN_URL_PG
        .set(url)
        .expect("start_shared_postgres must run exactly once");
    Some(container)
}

fn admin_url_pg() -> &'static str {
    ADMIN_URL_PG
        .get()
        .expect("start_shared_postgres must run before any scenario touches Postgres")
}

/// A fresh, uniquely named, migrated database with `search_path` set on the returned pool (§24),
/// so the store's startup verification and every query resolve `outbox` in the default `reliar`
/// schema with no URL/role configuration.
pub(crate) async fn fresh_postgres_db() -> PgPool {
    let admin = PgPool::connect(admin_url_pg())
        .await
        .expect("connect to admin database");
    let name = format!("t_{}", uuid::Uuid::now_v7().simple());
    // `CREATE DATABASE` cannot take a bind parameter; `name` is a freshly generated UUIDv7, never
    // user input — the one sanctioned exception to "macros only" (test code, not a crate),
    // asserted safe explicitly via `AssertSqlSafe` (sqlx 0.9's SQL-injection audit gate).
    sqlx::query(sqlx::AssertSqlSafe(format!(r#"CREATE DATABASE "{name}""#)))
        .execute(&admin)
        .await
        .expect("create test database");

    let options: PgConnectOptions = admin_url_pg()
        .parse()
        .expect("admin url parses as PgConnectOptions");
    let pool = PgPool::connect_with(
        options
            .database(&name)
            .options([("search_path", "reliar,public")]),
    )
    .await
    .expect("connect with search_path set");

    reliar_store_postgres::migrate(&pool, reliar_store_postgres::MigrateOptions::default())
        .await
        .expect("migrate the fresh database");

    pool
}

/// Enqueues `n` plain [`OrderCreated`] envelopes, each inside its own host transaction that
/// commits before the next one starts — the atomicity contract `enqueue` makes visible in its
/// signature (ADR 0008), exercised exactly as a real caller would. Returns the envelopes in
/// enqueue order.
pub(crate) async fn seed(
    store: &reliar_store_postgres::PostgresOutboxStore<JsonSerializer>,
    pool: &PgPool,
    n: u64,
) -> Vec<Envelope<OrderCreated>> {
    let mut envelopes = Vec::with_capacity(n as usize);
    for i in 0..n {
        let envelope = Envelope::builder(OrderCreated { order_id: i }).build();
        let mut tx = pool.begin().await.expect("begin tx");
        store.enqueue(&mut tx, &envelope).await.expect("enqueue");
        tx.commit().await.expect("commit tx");
        envelopes.push(envelope);
    }
    envelopes
}

/// The caller's own three-line serialization block (contract §4.2, Amendment D §3): nothing in
/// `reliar-outbox`/`reliar-store-postgres` serializes on the routing publisher's path, so E5/E6
/// build the [`SerializedEnvelope`] value themselves, exactly as a real host would before calling
/// `OutboxPublisher::in_transaction(..).publish(..)` or `publish_direct(..)`.
pub(crate) fn serialize<T: Message>(envelope: Envelope<T>) -> SerializedEnvelope {
    let ser = JsonSerializer;
    let bytes = ser.serialize(&envelope.body).expect("serialize body");
    let mut serialized = envelope.map_body(|_| bytes);
    serialized.metadata.delivery.content_type = ser.content_type().clone();
    serialized
}

/// Whether `id`'s row has been published.
pub(crate) async fn is_published(pool: &PgPool, id: reliar_core::MessageId) -> bool {
    sqlx::query_scalar("SELECT published_at IS NOT NULL FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("query published state")
}

/// `id`'s current `attempts` count.
pub(crate) async fn attempts_for(pool: &PgPool, id: reliar_core::MessageId) -> i32 {
    sqlx::query_scalar("SELECT attempts FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("query attempts")
}

/// `id`'s current `last_error`, if any.
pub(crate) async fn last_error_for(pool: &PgPool, id: reliar_core::MessageId) -> Option<String> {
    sqlx::query_scalar("SELECT last_error FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("query last_error")
}

/// Whether `id`'s row has been marked dead.
pub(crate) async fn is_dead(pool: &PgPool, id: reliar_core::MessageId) -> bool {
    sqlx::query_scalar("SELECT dead_at IS NOT NULL FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("query dead state")
}

/// How many rows in `outbox` are currently leased — used to assert `run()` released every lease
/// before returning.
pub(crate) async fn locked_row_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox WHERE locked_by IS NOT NULL")
        .fetch_one(pool)
        .await
        .expect("query locked count")
}

/// How many rows in `outbox` have been published.
pub(crate) async fn published_row_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox WHERE published_at IS NOT NULL")
        .fetch_one(pool)
        .await
        .expect("query published count")
}

/// How many rows in `outbox` are dead — used to assert a graceful cancellation never dead-letters
/// an in-flight row.
pub(crate) async fn dead_row_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox WHERE dead_at IS NOT NULL")
        .fetch_one(pool)
        .await
        .expect("query dead count")
}

/// How many rows in `outbox` carry `id` — `0` or `1`. Used by the routing scenarios (E5/E6) to
/// prove a directly published envelope was **never** staged, and that a rolled-back routed
/// envelope never became visible.
pub(crate) async fn row_count_for(pool: &PgPool, id: reliar_core::MessageId) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(pool)
        .await
        .expect("query row count")
}

/// How many rows in `outbox` have never been claimed. `attempts == 0` implies `acquire` never
/// touched the row — every claim increments it in the same statement that sets `locked_by` — so
/// `attempts == 0` also implies never locked, never published and never dead. Used by the
/// claim-stop trial to prove rows a capped-to-one-poll dispatcher never reached are provably
/// untouched, without querying each of those columns separately.
pub(crate) async fn never_claimed_row_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox WHERE attempts = 0")
        .fetch_one(pool)
        .await
        .expect("query never-claimed count")
}

/// Moves `id`'s `locked_until` into the past — SQL time-travel for lease-expiry tests, never a
/// wall-clock sleep (§8.2). The same technique `reliar-store-postgres`'s own crash-after-publish
/// test uses (E3).
pub(crate) async fn expire_lease(pool: &PgPool, id: uuid::Uuid) {
    sqlx::query("UPDATE outbox SET locked_until = now() - interval '1 second' WHERE id = $1")
        .bind(id)
        .execute(pool)
        .await
        .expect("expire the lease");
}

// ---------------------------------------------------------------------------------------------
// NATS / JetStream
// ---------------------------------------------------------------------------------------------

static ADMIN_URL_NATS: OnceCell<String> = OnceCell::const_new();

/// Starts the shared NATS/`JetStream` container this whole binary's scenarios use (unless
/// `NATS_URL` is set, e.g. CI's `docker run` step — ADR 0031 §3), and records its client URL for
/// every [`jetstream_context`] call. Returns the container itself — **the caller (`main`) owns it
/// as a local for the rest of the process's life and must drop it explicitly before exiting**
/// (RELIAR-27's lesson, ADR 0031 §4/§6).
///
/// Must run to completion exactly once, before any trial touches NATS.
pub(crate) async fn start_shared_nats(
    image: &str,
    tag: &str,
) -> Option<ContainerAsync<GenericImage>> {
    if let Ok(url) = std::env::var("NATS_URL") {
        ADMIN_URL_NATS
            .set(url)
            .expect("start_shared_nats must run exactly once");
        return None;
    }
    // No `WaitFor::message_on_stdout`: `nats-server` prints its ready line within a few
    // milliseconds, often before testcontainers' log-follow subscription attaches — the
    // connect-and-retry loop in `jetstream_context` is the readiness check instead (same lesson
    // `reliar-transport-nats`'s own harness records).
    let container = GenericImage::new(image, tag)
        .with_exposed_port(4222.tcp())
        .with_exposed_port(8222.tcp())
        .with_container_name(format!(
            "reliar-systest-nats-{}",
            uuid::Uuid::now_v7().simple()
        ))
        .with_label("reliar.test", "true")
        .with_cmd(["-js", "-m", "8222"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("mapped client port");
    let url = format!("nats://127.0.0.1:{port}");
    ADMIN_URL_NATS
        .set(url)
        .expect("start_shared_nats must run exactly once");
    Some(container)
}

/// Connects to the shared server and returns a `JetStream` context, retrying the connect itself
/// (the port may not be listening yet right after container start) and then `query_account` (a
/// `JetStream` API call, answered only once `-js` has finished initialising) until both succeed
/// or attempts are exhausted.
pub(crate) async fn jetstream_context() -> async_nats::jetstream::Context {
    let url = ADMIN_URL_NATS
        .get()
        .expect("start_shared_nats must run before any scenario touches NATS");
    let mut attempts = 0u32;
    loop {
        if let Ok(client) = async_nats::connect(url).await {
            let context = async_nats::jetstream::new(client);
            if context.query_account().await.is_ok() {
                return context;
            }
        }
        attempts += 1;
        assert!(attempts < 100, "JetStream did not become ready at {url}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Creates a stream named `name`, capturing exactly `subject` — the caller chooses both so a
/// scenario can delete and recreate a stream over the same subject space (E2).
pub(crate) async fn create_stream(
    context: &async_nats::jetstream::Context,
    name: &str,
    subject: &str,
) {
    context
        .create_stream(async_nats::jetstream::stream::Config {
            name: name.to_string(),
            subjects: vec![subject.to_string()],
            ..Default::default()
        })
        .await
        .expect("create the scenario's stream");
}

/// Deletes `name`. Best-effort: a scenario that already failed its own assertions still leaves
/// the shared server clean.
pub(crate) async fn delete_stream(context: &async_nats::jetstream::Context, name: &str) {
    let _ = context.delete_stream(name).await;
}

/// Creates a stream exactly like [`create_stream`], but with `duplicate_window` set explicitly
/// rather than left at the server default — for E3, where the window is the mechanic under test,
/// not an incidental default this scenario merely outlives.
pub(crate) async fn create_stream_with_duplicate_window(
    context: &async_nats::jetstream::Context,
    name: &str,
    subject: &str,
    duplicate_window: Duration,
) {
    context
        .create_stream(async_nats::jetstream::stream::Config {
            name: name.to_string(),
            subjects: vec![subject.to_string()],
            duplicate_window,
            ..Default::default()
        })
        .await
        .expect("create the scenario's stream");
}

/// The current stored-message count for `name`, fetched fresh from the server.
pub(crate) async fn stream_message_count(
    context: &async_nats::jetstream::Context,
    name: &str,
) -> u64 {
    let mut stream = context.get_stream(name).await.expect("fetch the stream");
    stream
        .info()
        .await
        .expect("fetch stream info")
        .state
        .messages
}

/// The raw stored message at `sequence` in `name`, headers and payload as the server holds them.
pub(crate) async fn stream_raw_message(
    context: &async_nats::jetstream::Context,
    name: &str,
    sequence: u64,
) -> async_nats::jetstream::message::StreamMessage {
    let stream = context.get_stream(name).await.expect("fetch the stream");
    stream
        .get_raw_message(sequence)
        .await
        .expect("fetch the raw stored message")
}

/// Reads a decoded header's single value as a string, panicking (test-only) if it is absent —
/// every header this crate writes is single-valued.
pub(crate) fn header_value(headers: &async_nats::HeaderMap, name: &str) -> String {
    headers.get(name).expect("header present").to_string()
}

// ---------------------------------------------------------------------------------------------
// Bounded polling — never a blind sleep
// ---------------------------------------------------------------------------------------------

/// Polls `f` every 20ms until it resolves `true` or `timeout` elapses, panicking on timeout — the
/// bounded, non-flaky alternative to a blind sleep for a condition driven by a real background
/// task talking to real Postgres/NATS sockets.
pub(crate) async fn wait_until<F, Fut>(timeout: Duration, mut f: F)
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
