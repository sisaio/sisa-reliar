//! A minimal outbox → NATS `JetStream` pipeline: two typed messages, published through
//! [`reliar_outbox::OutboxPublisher`], drained (the routed one) by `OutboxDispatcher`/`NatsPublisher`,
//! and a Core NATS subscriber task that decodes what arrives with `NatsEnvelopeMapper` — the exact
//! composition `docs/architecture/phase2-contract.md` §5 describes, plus the routing rule of SRS
//! §20.2 (ADR 0033 Amendment D).
//!
//! ```sh
//! export DATABASE_URL='postgres://user:pw@localhost/app?options=-c%20search_path%3Dreliar,public'
//! export NATS_URL='nats://127.0.0.1:4222'
//! cargo run -p nats-pub-sub -- --migrate   # first run only — applies Reliar's migrations
//! ```
//!
//! By default (no `RELIAR_OUTBOX_*` set) both messages route through the outbox — the durable
//! default. Set `RELIAR_OUTBOX_DISALLOWED_TYPES=audit.logged` to see the second message publish
//! **directly** instead, and print `route = direct`:
//!
//! ```sh
//! RELIAR_OUTBOX_DISALLOWED_TYPES=audit.logged cargo run -p nats-pub-sub
//! ```
//!
//! See `docs/guides/outbox-routing.md` for the rule, both rollout shapes and the direct path's
//! guarantees, and `docs/guides/nats.md` for stream ownership, the `duplicate_window`, and subject
//! strategy. This example creates its own stream explicitly, on every run: Reliar's
//! `NatsPublisher` never connects and never creates one (ADR 0029) — that is always the
//! application's or the operator's job, and here it is inline so `cargo run` stays self-contained.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::time::Duration;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use reliar_core::{
    Envelope, EnvelopeMapper, Message, Publisher as _, SerializedEnvelope, Serializer as _,
};
use reliar_outbox::{
    DispatcherSettings, OutboxDispatcher, OutboxPolicy, OutboxPublisher, OutboxSettings,
};
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

/// A second, distinct message type — this example's stand-in for the audit-style events a real
/// deployment often routes **directly** (`RELIAR_OUTBOX_DISALLOWED_TYPES=audit.logged`) rather
/// than staging them durably.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct AuditLogged {
    event: String,
}

impl Message for AuditLogged {
    const TYPE: &'static str = "audit.logged";
    const VERSION: u16 = 1;
}

/// The caller's own serialization block (contract §4.2, ADR 0033 Amendment D §3): nothing in
/// `reliar-outbox` serializes on the [`OutboxPublisher`] path, so this example serializes once,
/// exactly as it would for a bare `NatsPublisher`.
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
    reason = "one ordered narrative — wiring, routing policy, both publishes, and the graceful \
              shutdown — splitting it would scatter the ordering the example depends on across \
              helper functions with no reuse"
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

    // The routing rule, entirely from the environment (SRS §7.2, §20.2; `docs/guides/outbox-routing.md`):
    // nothing here is hard-coded, so `RELIAR_OUTBOX_ENABLED`/`_ALLOWED_TYPES`/`_DISALLOWED_TYPES`
    // are the only way to change which messages stage durably and which publish directly.
    let outbox_settings =
        OutboxSettings::from_env("RELIAR_OUTBOX_").context("reading RELIAR_OUTBOX_* settings")?;
    let policy = OutboxPolicy::from_settings(&outbox_settings)
        .context("building the routing policy from RELIAR_OUTBOX_* settings")?;
    println!(
        "routing policy: enabled={} allowed_types={:?} disallowed_types={:?}",
        policy.enabled(),
        policy.allowed_types().names(),
        policy.disallowed_types().names(),
    );
    let outbox = OutboxPublisher::new(store.clone(), publisher.clone(), policy);

    let dispatcher_settings = DispatcherSettings::default()
        .poll_interval(Duration::from_millis(50))
        .idle_poll_interval(Duration::from_millis(50));
    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(dispatcher_settings)
        .build()
        .context("building the dispatcher")?;

    let cancel = CancellationToken::new();
    let dispatcher_handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // One call each through the outbox — `in_transaction(&mut tx).publish(..)` reaches whichever
    // path the policy above decided, so this same call site keeps working no matter how
    // `RELIAR_OUTBOX_*` is set. The route is read from the policy directly (there is nothing else
    // to preview it with, ADR 0033 Amendment C) before the publish that acts on it.
    let order = serialize(Envelope::builder(OrderCreated { order_id: 1 }).build())
        .context("serializing the order")?;
    let order_route = outbox.policy().decide(&order.message_type);
    let mut tx = pool.begin().await.context("begin transaction")?;
    outbox
        .in_transaction(&mut tx)
        .publish(&order)
        .await
        .context("publish the order")?;
    tx.commit().await.context("commit transaction")?;
    println!(
        "published {} (order 1) via route = {}",
        order.id,
        order_route.as_str()
    );

    let audit = serialize(
        Envelope::builder(AuditLogged {
            event: "signed_in".to_string(),
        })
        .build(),
    )
    .context("serializing the audit event")?;
    let audit_route = outbox.policy().decide(&audit.message_type);
    let mut tx = pool.begin().await.context("begin transaction")?;
    outbox
        .in_transaction(&mut tx)
        .publish(&audit)
        .await
        .context("publish the audit event")?;
    tx.commit().await.context("commit transaction")?;
    println!(
        "published {} (audit.logged) via route = {}",
        audit.id,
        audit_route.as_str()
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
