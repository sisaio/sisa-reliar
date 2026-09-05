//! Reference integration (SRS §20.1, §20.2): an Axum handler that writes a business row and an
//! outbox row in one `sqlx` transaction, a `PostgresOutboxStore` built once at startup, an
//! `OutboxDispatcher` whose `CancellationToken` is tied to the same graceful shutdown as the HTTP
//! server, and `OutboxPublisher` as the handler's call site: `outbox.enqueue(&mut tx,
//! &serialized)` — the durable path — rather than the store's own inherent `enqueue`. See
//! `docs/guides/outbox-enqueue-and-publish.md` for when a call site instead wants
//! `Publisher::publish`, the pass-through that bypasses the outbox entirely.
//!
//! ```sh
//! export DATABASE_URL='postgres://user:pw@localhost/app?options=-c%20search_path%3Dreliar,public'
//! cargo run -p axum-outbox -- --migrate   # first run only — applies Reliar's migrations
//! curl -X POST localhost:3000/orders -H 'content-type: application/json' -d '{"order_id":1}'
//! ```
//!
//! See `docs/guides/postgres.md` for `search_path` setup behind a transaction-mode pooler: any
//! pooler that drops the `?options=` query parameter needs an
//! `ALTER ROLE <app> SET search_path = reliar, public;` instead.

use std::net::SocketAddr;

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::{Json, Router};
use reliar_core::{
    Classify, Envelope, FailureKind, JsonSerializer, Message, Publisher, SerializedEnvelope,
    Serializer as _,
};
use reliar_outbox::{OutboxDispatcher, OutboxPublisher};
use reliar_store_postgres::{MigrateOptions, PostgresOutboxSettings, PostgresOutboxStore};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

/// The business event. `TYPE`/`VERSION` are the stable wire identity a downstream consumer
/// matches on (ADR 0010).
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderCreated {
    order_id: i64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

#[derive(Clone)]
struct AppState {
    pool: sqlx::PgPool,
    // Cheap to clone into `AppState`: `OutboxPublisher` wraps a `PostgresOutboxStore` (a `PgPool`
    // plus an `Arc<Ser>`) and a `StdoutPublisher` — no outer `Arc` required (outbox-publisher
    // contract §2).
    outbox: OutboxPublisher<PostgresOutboxStore<JsonSerializer>, StdoutPublisher>,
}

#[derive(Deserialize)]
struct CreateOrderRequest {
    order_id: i64,
}

#[derive(Serialize)]
struct CreateOrderResponse {
    order_id: i64,
    message_id: String,
}

/// A minimal `Publisher` for a Phase-1 example: no broker ships until Phase 2
/// (`reliar-transport-nats`), so this prints what would have gone out. `RecordingPublisher`
/// (`reliar-outbox`'s `test-support` fakes) is the equivalent for a test, not a running process.
#[derive(Clone, Debug, Default)]
struct StdoutPublisher;

/// Never actually constructed — `StdoutPublisher::publish` never fails — but the type still has
/// to exist and classify, because `Publisher::Error` is a real associated type on the trait.
#[derive(Debug)]
struct Unreachable;

impl std::fmt::Display for Unreachable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("unreachable: StdoutPublisher never fails")
    }
}
impl std::error::Error for Unreachable {}
impl Classify for Unreachable {
    fn kind(&self) -> FailureKind {
        FailureKind::Transient
    }
}

impl Publisher for StdoutPublisher {
    type Error = Unreachable;

    fn publish(
        &self,
        envelope: &SerializedEnvelope,
    ) -> impl std::future::Future<Output = Result<(), Self::Error>> + Send {
        // Never a payload: `envelope.id`/`message_type` only (SRS §33 — payloads and headers are
        // never logged by default).
        println!("published {} ({})", envelope.id, envelope.message_type);
        std::future::ready(Ok(()))
    }
}

/// The write path: the application owns the transaction, so the business row and the outbox row
/// commit together or neither does (ADR 0008).
///
/// This example uses `sqlx`'s runtime string API (`sqlx::query`), not the compile-time `query!`
/// macro — that "macros only" rule is a house style for `reliar-store-postgres`'s **own**
/// queries against its **own**, known-at-compile-time schema (`sqlx-postgres` skill). The
/// `orders` table belongs to this example's host application, which is free to choose its own
/// tooling; a real service would likely use `query!` too, backed by its own `.sqlx/` cache.
async fn create_order(
    State(state): State<AppState>,
    Json(request): Json<CreateOrderRequest>,
) -> Result<Json<CreateOrderResponse>, AppError> {
    let mut tx = state.pool.begin().await?;

    // Qualified `public.orders`, not bare `orders`: this connection's `search_path` puts
    // `reliar` first (see `main`'s `DATABASE_URL` comment), so an *unqualified* name here would
    // resolve into Reliar's own schema instead of the host's — see
    // `docs/guides/postgres.md`'s "search_path changes where your own DDL lands" warning.
    sqlx::query("INSERT INTO public.orders (id) VALUES ($1)")
        .bind(request.order_id)
        .execute(&mut *tx)
        .await?;

    let envelope = Envelope::builder(OrderCreated {
        order_id: request.order_id,
    })
    .build();

    // The caller serializes once, exactly as it would for a bare `Publisher` with no outbox in
    // play (outbox-publisher contract §2.1) — `OutboxPublisher` holds no `Serializer` of its own.
    let ser = JsonSerializer;
    let bytes = ser.serialize(&envelope.body)?;
    let mut serialized = envelope.map_body(|_| bytes);
    serialized.metadata.delivery.content_type = ser.content_type().clone();
    let message_id = serialized.id;

    // The durable path: enqueued in this handler's own transaction, atomic with the `orders`
    // insert above. A running `OutboxDispatcher` publishes it afterwards. A call site that wants
    // to send immediately instead, with no Reliar durability at all, calls
    // `Publisher::publish(&serialized)` — see `docs/guides/outbox-enqueue-and-publish.md`.
    state.outbox.enqueue(&mut tx, &serialized).await?;

    tx.commit().await?;

    Ok(Json(CreateOrderResponse {
        order_id: request.order_id,
        message_id: message_id.to_string(),
    }))
}

/// Wraps any error as a `500` — acceptable in an example; a real service classifies its own
/// domain errors.
struct AppError(anyhow::Error);

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

impl<E: Into<anyhow::Error>> From<E> for AppError {
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

/// The host's own business schema — out of scope for Reliar, which never touches a table it did
/// not create. A real application manages this with its own migration tool. Qualified
/// `public.orders`: with `reliar` first on `search_path`, an unqualified `CREATE TABLE orders`
/// would land in Reliar's own schema instead of the host's `public` one.
async fn ensure_orders_table(pool: &sqlx::PgPool) -> Result<()> {
    sqlx::query("CREATE TABLE IF NOT EXISTS public.orders (id bigint PRIMARY KEY)")
        .execute(pool)
        .await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber_init();

    // Reliar never owns or reads `DATABASE_URL` — the host's own pool, the host's own env var.
    // `?options=-c%20search_path%3Dreliar,public` puts the `reliar` schema first on the search
    // path. Behind a transaction-mode pooler that drops startup `options`, set a server-side
    // default instead: `ALTER ROLE <app> SET search_path = reliar, public;` (docs/guides/postgres.md).
    let database_url = std::env::var("DATABASE_URL")
        .context("DATABASE_URL must be set, e.g. postgres://user:pw@localhost/app?options=-c%20search_path%3Dreliar,public")?;
    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .context("connecting to Postgres")?;

    // Migrations run only through this explicit call, never implicitly (SRS §35) — and only when
    // the operator asks for it, via `--migrate` or `RELIAR_MIGRATE=1`.
    let should_migrate = std::env::args().any(|arg| arg == "--migrate")
        || std::env::var("RELIAR_MIGRATE")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if should_migrate {
        reliar_store_postgres::migrate(&pool, MigrateOptions::default())
            .await
            .context("running Reliar's migrations")?;
        println!("migrations applied");
    }

    ensure_orders_table(&pool).await?;

    let settings = PostgresOutboxSettings::from_env("RELIAR_STORE_POSTGRES_")
        .context("RELIAR_STORE_POSTGRES_* environment variables")?;
    // Fails fast if `outbox` does not resolve to the configured schema — a wrong `search_path`,
    // a pooler dropping `options`, or missing migrations are all caught here, not on the first
    // `acquire` (ADR 0017).
    let store = PostgresOutboxStore::with_settings(pool.clone(), settings)
        .await
        .context("connecting the outbox store (check search_path / migrate())")?;

    // `OutboxSettings` feeds the dispatcher's tuning, entirely from the environment (SRS §7.2).
    let outbox_settings = reliar_outbox::OutboxSettings::from_env("RELIAR_OUTBOX_")
        .context("RELIAR_OUTBOX_* environment variables")?;
    let dispatcher = OutboxDispatcher::builder(store.clone(), StdoutPublisher)
        .settings(outbox_settings.dispatcher)
        .build()?;
    let outbox = OutboxPublisher::new(store.clone(), StdoutPublisher);

    // One `CancellationToken` drives both Axum's graceful shutdown and the dispatcher's drain.
    let cancel = CancellationToken::new();
    let dispatcher_handle = tokio::spawn(dispatcher.run(cancel.clone()));

    let app = Router::new()
        .route("/orders", post(create_order))
        .with_state(AppState { pool, outbox });

    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    let listener = TcpListener::bind(addr).await?;
    println!("listening on http://{addr}");

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(cancel.clone()))
        .await;

    // Join the dispatcher **even when `serve` itself failed** — `cancel` is idempotent, so this
    // is safe whether or not `shutdown_signal` already fired it. A real drain barrier, not an
    // abort: waits for `run()` to finish releasing/persisting in-flight work within
    // `drain_timeout`, so an unexpected server crash still leaves no row leased and dark.
    cancel.cancel();
    let dispatcher_result = dispatcher_handle.await;

    serve_result.context("axum::serve")?;
    dispatcher_result.context("dispatcher task panicked")??;
    Ok(())
}

/// Waits for SIGINT (Ctrl+C) and cancels the shared token, which unblocks both Axum's
/// `with_graceful_shutdown` and the dispatcher's `run` loop at the same moment.
async fn shutdown_signal(cancel: CancellationToken) {
    let _ = tokio::signal::ctrl_c().await;
    println!("shutting down (draining in-flight publishes)...");
    cancel.cancel();
}

fn tracing_subscriber_init() {
    // A host wires its own exporter; this example only needs `run`'s spans to be visible on
    // stdout when `RUST_LOG` is set.
    let _ = tracing_subscriber::fmt::try_init();
}
