//! A minimal outbox → NATS `JetStream` pipeline: two typed messages, one enqueued through
//! [`reliar_outbox::OutboxPublisher::enqueue`] and drained by `OutboxDispatcher`/`NatsPublisher`,
//! one sent through [`reliar_core::Publisher::publish`] (the pass-through that bypasses the
//! outbox entirely, ADR 0036) — and a Core NATS subscriber task that decodes what arrives with
//! `NatsEnvelopeMapper` — the exact composition `docs/architecture/phase2-contract.md` §5
//! describes, plus the enqueue/publish rule of SRS §20.2.
//!
//! ```sh
//! export DATABASE_URL='postgres://user:pw@localhost/app?options=-c%20search_path%3Dreliar,public'
//! export NATS_URL='nats://127.0.0.1:4222'
//! cargo run -p nats-pub-sub -- --migrate   # first run only — applies Reliar's migrations
//! ```
//!
//! See `docs/guides/outbox-enqueue-and-publish.md` for the guarantee each call carries, and
//! `docs/guides/nats.md` for stream ownership, the `duplicate_window`, and subject strategy. This
//! example creates its own stream explicitly, on every run: Reliar's `NatsPublisher` never
//! connects and never creates one (ADR 0029) — that is always the application's or the operator's
//! job, and here it is inline so `cargo run` stays self-contained.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use reliar_core::{
    Envelope, EnvelopeMapper, Message, Publisher as _, SerializedEnvelope, Serializer as _,
};
use reliar_outbox::{DispatcherSettings, OutboxDispatcher, OutboxPublisher};
use reliar_store_postgres::{MigrateOptions, PostgresOutboxSettings, PostgresOutboxStore};
use reliar_transport_nats::{NatsEnvelopeMapper, NatsPublisher, NatsSettings, NatsWireMessage};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

/// Subject prefix for this example's messages: `<prefix>.orders.created.v1` (the default
/// `PrefixSubjects` resolver, ADR 0027).
const SUBJECT_PREFIX: &str = "nats-pub-sub-example";
/// The stream this example owns end to end — created on every run via `get_or_create_stream` so
/// re-running the example never fails on "stream already exists".
const STREAM_NAME: &str = "NATS_PUB_SUB_EXAMPLE";

/// The business event. `TYPE`/`VERSION` are the stable wire identity a downstream consumer
/// matches on (ADR 0010) — never `type_name::<T>()`.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct OrderCreated {
    order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// A second, distinct message type — this example's stand-in for events a call site sends
/// **directly**, with `Publisher::publish`, rather than enqueuing them durably.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuditLogged {
    event: String,
}

impl Message for AuditLogged {
    const TYPE: &'static str = "audit.logged";
    const VERSION: u16 = 1;
}

/// The caller's own serialization block (outbox-publisher contract §2.1): nothing in
/// `reliar-outbox` serializes on either the `enqueue` or the `publish` path, so this example
/// serializes once, exactly as it would for a bare `NatsPublisher`.
///
/// # Errors
///
/// Whatever the serializer's own `serialize` returns.
fn serialize<T: Message>(envelope: Envelope<T>) -> Result<SerializedEnvelope> {
    let ser = reliar_core::JsonSerializer;
    let bytes = ser
        .serialize(&envelope.body)
        .context("serializing the envelope body")?;
    let mut serialized = envelope.map_body(|_| bytes);
    serialized.metadata.delivery.content_type = ser.content_type().clone();
    Ok(serialized)
}

#[tokio::main]
#[allow(
    clippy::too_many_lines,
    reason = "one ordered narrative — wiring, both call sites, and the graceful shutdown — \
              splitting it would scatter the ordering the example depends on across helper \
              functions with no reuse"
)]
async fn main() -> Result<()> {
    let _ = tracing_subscriber::fmt::try_init();

    // Reliar never owns or reads `DATABASE_URL`/`NATS_URL` — both are this example's own env
    // vars, exactly as `examples/axum-outbox` reads `DATABASE_URL` (SRS §7.2, ADR 0029 §1).
    let database_url = std::env::var("DATABASE_URL").context(
        "DATABASE_URL must be set, e.g. postgres://user:pw@localhost/app?options=-c%20search_path%3Dreliar,public",
    )?;
    let nats_url =
        std::env::var("NATS_URL").context("NATS_URL must be set, e.g. nats://127.0.0.1:4222")?;

    let pool = sqlx::PgPool::connect(&database_url)
        .await
        .context("connecting to Postgres")?;

    // Migrations run only through this explicit call, never implicitly (SRS §35), and only when
    // the operator asks for it — mirrors `examples/axum-outbox`.
    let should_migrate = std::env::args().any(|arg| arg == "--migrate")
        || std::env::var("RELIAR_MIGRATE")
            .is_ok_and(|v| v == "1" || v.eq_ignore_ascii_case("true"));
    if should_migrate {
        reliar_store_postgres::migrate(&pool, MigrateOptions::default())
            .await
            .context("running Reliar's migrations")?;
        println!("migrations applied");
    }

    let store = PostgresOutboxStore::with_settings(pool.clone(), PostgresOutboxSettings::default())
        .await
        .context("connecting the outbox store (check search_path / migrate())")?;

    // The application owns the connection and the JetStream context (ADR 0029): Reliar's
    // publisher only ever receives an already-built `Context`.
    let client = async_nats::connect(&nats_url)
        .await
        .context("connecting to NATS")?;
    let jetstream = async_nats::jetstream::new(client.clone());

    // Stream provisioning is the application's job, not Reliar's (ADR 0029) — a real deployment
    // does this once, out of band, with the retention/duplicate_window/replicas it actually
    // wants (see docs/guides/nats.md). `get_or_create_stream` keeps this example idempotent
    // across repeated `cargo run`s.
    jetstream
        .get_or_create_stream(async_nats::jetstream::stream::Config {
            name: STREAM_NAME.to_string(),
            subjects: vec![format!("{SUBJECT_PREFIX}.>")],
            ..Default::default()
        })
        .await
        .context("creating the example's JetStream stream")?;

    // A plain Core NATS subscription over the same subject space, decoding with
    // `NatsEnvelopeMapper` — Reliar ships no subscriber/consumer API in Phase 2 (contract §8),
    // so this loop is host code, exactly as a real consumer would write it.
    let mut subscription = client
        .subscribe(format!("{SUBJECT_PREFIX}.>"))
        .await
        .context("subscribing")?;
    let received = Arc::new(AtomicUsize::new(0));
    let received_by_subscriber = Arc::clone(&received);
    let subscriber = tokio::spawn(async move {
        let mapper = NatsEnvelopeMapper::default();
        while let Some(message) = subscription.next().await {
            let wire = NatsWireMessage::from(message);
            match mapper.decode(wire) {
                // Ids and the message type only — never a payload byte or a header value by
                // default (SRS §33).
                Ok(envelope) => {
                    println!(
                        "received {} ({}.v{})",
                        envelope.id,
                        envelope.message_type.name(),
                        envelope.message_type.version()
                    );
                    received_by_subscriber.fetch_add(1, AtomicOrdering::SeqCst);
                }
                Err(err) => eprintln!("could not decode a received message: {err}"),
            }
        }
    });

    let publisher = NatsPublisher::new(
        jetstream,
        NatsSettings::default().subject_prefix(SUBJECT_PREFIX),
    )
    .context("building the publisher")?;

    let outbox = OutboxPublisher::new(store.clone(), publisher.clone());

    let dispatcher_settings = DispatcherSettings::default()
        .poll_interval(Duration::from_millis(50))
        .idle_poll_interval(Duration::from_millis(50));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(dispatcher_settings)
        .build()
        .context("building the dispatcher")?;

    let cancel = CancellationToken::new();
    let dispatcher_handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // The durable path: enqueued in this example's own transaction, atomic with whatever else it
    // writes there. A running `OutboxDispatcher` (spawned above) publishes it afterwards, with
    // at-least-once delivery.
    let order = serialize(Envelope::builder(OrderCreated { order_id: 1 }).build())
        .context("serializing the order")?;
    let mut tx = pool.begin().await.context("begin transaction")?;
    outbox
        .enqueue(&mut tx, &order)
        .await
        .context("enqueue the order")?;
    tx.commit().await.context("commit transaction")?;
    println!("enqueued {} (order 1) — durable, at-least-once", order.id);

    // The bypass path: sent now, through the transport publisher, one attempt — no retry, no
    // backoff, no dead state, no duplicate window, and no relationship to any transaction. This
    // call needs no `tx` at all: `publish` bypasses the outbox entirely (ADR 0036).
    let audit = serialize(
        Envelope::builder(AuditLogged {
            event: "signed_in".to_string(),
        })
        .build(),
    )
    .context("serializing the audit event")?;
    outbox
        .publish(&audit)
        .await
        .context("publish the audit event")?;
    println!(
        "published {} (audit.logged) — one attempt, no Reliar guarantee",
        audit.id
    );

    // Poll for "the subscriber received both messages" instead of guessing a fixed sleep,
    // bounded by an overall deadline so a broken pipeline fails this example loudly.
    let deadline = Duration::from_secs(10);
    let poll_every = Duration::from_millis(50);
    let awaited = tokio::time::timeout(deadline, async {
        while received.load(AtomicOrdering::SeqCst) < 2 {
            tokio::time::sleep(poll_every).await;
        }
    })
    .await;
    if awaited.is_err() {
        anyhow::bail!(
            "timed out after {deadline:?} waiting for the subscriber to receive both messages"
        );
    }

    // Shut down gracefully — `run` drains in-flight publishes before returning `Ok(())`.
    cancel.cancel();
    dispatcher_handle
        .await
        .context("dispatcher task panicked")??;
    subscriber.abort();

    println!("done");
    Ok(())
}
