//! §43.A.36 — `PostgresOutboxStore::new` resolves `outbox` against the configured schema
//! exactly once at construction: succeeds when they match, fails naming the configured schema
//! and the observed `search_path` when unresolvable, and warns (asserted with a recording
//! `tracing` subscriber) when a same-named table also exists elsewhere on the path. With
//! `enqueue_sets_search_path = true`, `enqueue` sets and restores the path inside the caller's
//! transaction and leaves it unchanged afterward.

use crate::common;

use std::sync::{Arc, Mutex};

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_store_postgres::{PostgresOutboxSettings, PostgresOutboxStore, PostgresStoreError};
use sqlx::PgPool;
use sqlx::postgres::PgConnectOptions;
use tracing::field::{Field, Visit};
use tracing_subscriber::layer::{Context, SubscriberExt};

#[derive(Default, Clone)]
struct Recorded(Arc<Mutex<Vec<String>>>);

struct Recorder(Recorded);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Recorder {
    fn on_event(&self, event: &tracing::Event<'_>, _ctx: Context<'_, S>) {
        struct MessageVisitor(String);
        impl Visit for MessageVisitor {
            fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                }
            }
        }
        let mut visitor = MessageVisitor(String::new());
        event.record(&mut visitor);
        self.0.0.lock().unwrap().push(visitor.0);
    }
}

/// Async-friendly: `set_default` returns a guard that stays active across `.await` points on
/// the *current* OS thread, which is exactly what a default (single-threaded) `#[tokio::test]`
/// runs on — unlike `tracing::subscriber::with_default`, this needs no nested runtime.
async fn with_recording_subscriber<Fut: std::future::Future<Output = ()>>(f: Fut) -> Vec<String> {
    let recorded = Recorded::default();
    let subscriber = tracing_subscriber::registry().with(Recorder(recorded.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    f.await;
    recorded.0.lock().unwrap().clone()
}

async fn pool_without_search_path() -> PgPool {
    let base = common::fresh_unmigrated_db().await;
    reliar_store_postgres::migrate(&base, reliar_store_postgres::MigrateOptions::default())
        .await
        .unwrap();
    // A pool whose `search_path` explicitly excludes `reliar` — deliberately set to just
    // `public` rather than left at the server default, since a local role happening to share
    // the schema's name would otherwise resolve it via Postgres's own `"$user", public` default.
    let options: PgConnectOptions = base
        .connect_options()
        .as_ref()
        .clone()
        .options([("search_path", "public")]);
    PgPool::connect_with(options).await.unwrap()
}

async fn construction_fails_fast_without_search_path() {
    let pool = pool_without_search_path().await;
    let err = PostgresOutboxStore::new(pool).await.unwrap_err();
    match err {
        PostgresStoreError::SchemaResolution { configured, .. } => {
            assert_eq!(configured, "reliar");
        }
        other => panic!("expected SchemaResolution, got {other:?}"),
    }
}

async fn construction_succeeds_with_search_path_set() {
    let pool = common::fresh_db().await;
    PostgresOutboxStore::new(pool)
        .await
        .expect("construction succeeds once outbox resolves to the configured schema");
}

async fn warns_when_a_same_named_table_exists_elsewhere() {
    let base = common::fresh_unmigrated_db().await;
    reliar_store_postgres::migrate(&base, reliar_store_postgres::MigrateOptions::default())
        .await
        .unwrap();
    sqlx::query("CREATE TABLE public.outbox (id uuid)")
        .execute(&base)
        .await
        .unwrap();

    let options: PgConnectOptions = base
        .connect_options()
        .as_ref()
        .clone()
        .options([("search_path", "reliar,public")]);
    let pool = PgPool::connect_with(options).await.unwrap();

    let messages = with_recording_subscriber(async {
        PostgresOutboxStore::new(pool).await.unwrap();
    })
    .await;

    assert!(
        messages.iter().any(|m| m.contains("outbox")),
        "expected a warning naming the duplicate `outbox` table; got {messages:?}"
    );
}

async fn enqueue_sets_search_path_restores_the_callers_value() {
    let pool = common::fresh_db().await;
    let settings = PostgresOutboxSettings::default().enqueue_sets_search_path(true);
    let store = PostgresOutboxStore::with_settings(pool.clone(), settings)
        .await
        .unwrap();

    let mut tx = pool.begin().await.unwrap();
    let before: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(&mut *tx)
        .await
        .unwrap();

    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    store.enqueue(&mut tx, &envelope).await.unwrap();

    let after: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "enqueue must restore the caller's search_path"
    );
    tx.commit().await.unwrap();
}

/// Review 2, major 4 — `enqueue_sets_search_path(true)`'s `set_config`/restore wrap must not
/// interfere with the ordinary `pk_outbox`-violation mapping: a duplicate id still surfaces as
/// `EnqueueError::Duplicate`, not masked by the (correctly skipped-on-failure) restore.
async fn duplicate_id_surfaces_correctly_with_enqueue_sets_search_path() {
    let pool = common::fresh_db().await;
    let settings = PostgresOutboxSettings::default().enqueue_sets_search_path(true);
    let store = PostgresOutboxStore::with_settings(pool.clone(), settings)
        .await
        .unwrap();
    let envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();

    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    match store.enqueue(&mut tx, &envelope).await {
        Err(reliar_store_postgres::EnqueueError::Duplicate { id }) => assert_eq!(id, envelope.id),
        other => panic!("expected EnqueueError::Duplicate, got {other:?}"),
    }
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_schema_verification::construction_fails_fast_without_search_path",
            move || {
                rt.block_on(construction_fails_fast_without_search_path());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_schema_verification::construction_succeeds_with_search_path_set",
            move || {
                rt.block_on(construction_succeeds_with_search_path_set());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_schema_verification::warns_when_a_same_named_table_exists_elsewhere",
            move || {
                rt.block_on(warns_when_a_same_named_table_exists_elsewhere());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_schema_verification::enqueue_sets_search_path_restores_the_callers_value",
            move || {
                rt.block_on(enqueue_sets_search_path_restores_the_callers_value());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_schema_verification::duplicate_id_surfaces_correctly_with_enqueue_sets_search_path",
            move || {
                rt.block_on(duplicate_id_surfaces_correctly_with_enqueue_sets_search_path());
                Ok(())
            },
        ),
    ]
}
