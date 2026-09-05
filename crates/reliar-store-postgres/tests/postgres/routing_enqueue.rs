//! R14, R22, R23 (routing-publisher contract `docs/architecture/routing-publisher-contract.md`
//! §10, §6; ADR 0033 Amendment D) — [`PostgresOutboxStore`]'s `OutboxStaging` impl, exercised
//! through [`reliar_outbox::OutboxPublisher`]/[`reliar_outbox::ScopedOutboxPublisher`] against a
//! real Postgres.

use crate::common;
use crate::common::OrderCreated;

use reliar_core::{Envelope, MessageId, MessageType, Metadata, Publisher as _, Serializer as _};
use reliar_outbox::{OutboxPolicy, OutboxPublisher, RecordingPublisher};
use reliar_store_postgres::{PostgresOutboxSettings, PostgresOutboxStore};

/// [`common::serialize_with`]-equivalent for this crate: the caller's own three-line block
/// (contract §4.2) — nothing in `reliar-outbox`/`reliar-store-postgres` serializes on this path
/// any more (Amendment D §3).
fn serialize(envelope: Envelope<OrderCreated>) -> reliar_core::SerializedEnvelope {
    let ser = reliar_core::JsonSerializer;
    let bytes = ser.serialize(&envelope.body).unwrap();
    let mut out = envelope.map_body(|_| bytes);
    out.metadata.delivery.content_type = ser.content_type().clone();
    out
}

fn build_outbox(
    store: PostgresOutboxStore,
) -> OutboxPublisher<PostgresOutboxStore, RecordingPublisher> {
    OutboxPublisher::new(
        store,
        RecordingPublisher::default(),
        OutboxPolicy::default(),
    )
}

/// R23 — the scoped view **is** a `Publisher`, and its `publish` future is `Send` even though it
/// borrows a non-`'static` `Transaction<'_, Postgres>` scope: `tokio::spawn` requires exactly
/// that, and proving it requires the compiler to check `PostgresOutboxStore`'s `OutboxStaging`
/// impl for genericity over the transaction's lifetime. This is what R14a — the regression the
/// pre-Amendment-D `where 'c: 'a` trap caused — becomes now that `&mut Tx` lives in the trait's
/// own method signature and there is no second lifetime left to accidentally bound (contract §6).
async fn stage_is_a_publisher_and_its_future_is_send_through_tokio_spawn() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 7 }).build());
    let id = serialized.id;

    // The realistic host shape: acquire a transaction, hand the whole call to a spawned task.
    // `tx` is `Transaction<'static, Postgres>` (owned from the pool), but the reference
    // `&mut tx` the scoped view borrows lives only inside this async block's own scope, not
    // `'static` — the non-`'static` transaction scope R23 needs.
    let mut tx = pool.begin().await.unwrap();
    tokio::spawn(async move {
        outbox
            .in_transaction(&mut tx)
            .publish(&serialized)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    })
    .await
    .unwrap();

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 1);
}

/// R14 — atomicity: the row is invisible before commit and present after, exactly like the
/// store's own `enqueue` (`outbox_enqueue_atomic.rs`), but reached through
/// `OutboxStaging::stage` (the scoped publisher's path) instead.
async fn commit_makes_the_row_visible() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 1 }).build());
    let id = serialized.id;

    let mut tx = pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .unwrap();

    let count_before: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count_before, 0,
        "not visible to another connection before commit"
    );

    tx.commit().await.unwrap();

    let count_after: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count_after, 1);
}

/// R14 — rollback leaves nothing, same guarantee as the store's own `enqueue`.
async fn rollback_leaves_nothing() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 2 }).build());

    let mut tx = pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .unwrap();
    tx.rollback().await.unwrap();

    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

/// R14 — `stage` persists `envelope.metadata.delivery.content_type` **verbatim** — the caller's
/// own content type, never [`PostgresOutboxStore::content_type`] (contract §3, §6). Proven by
/// staging through a serializer whose `ContentType` differs from the store's own default.
async fn content_type_is_the_envelopes_not_the_stores() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let envelope = Envelope::builder(OrderCreated { order_id: 3 }).build();
    let vnd = common::TestVndSerializer;
    let bytes = vnd.serialize(&envelope.body).unwrap();
    let mut serialized = envelope.map_body(|_| bytes);
    serialized.metadata.delivery.content_type = vnd.content_type().clone();
    let id = serialized.id;

    let mut tx = pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let content_type: String = sqlx::query_scalar("SELECT content_type FROM outbox WHERE id = $1")
        .bind(id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        content_type,
        vnd.content_type().as_str(),
        "must be the caller's own content type, not the store's default JSON"
    );
    assert_ne!(content_type, reliar_core::ContentType::JSON.as_str());
}

/// R14 — a reused `MessageId` maps to [`reliar_store_postgres::EnqueueError::Duplicate`], same
/// as the store's inherent `enqueue` (`outbox_enqueue_atomic::duplicate_message_id_aborts_the_transaction`).
async fn duplicate_message_id_is_rejected() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 4 }).build());

    let mut tx = pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let result = outbox.in_transaction(&mut tx).publish(&serialized).await;
    let err = result.expect_err("a reused MessageId must be rejected");
    match err {
        reliar_outbox::RouteError::Stage(reliar_store_postgres::EnqueueError::Duplicate { id }) => {
            assert_eq!(id, serialized.id);
        }
        other => panic!("expected RouteError::Stage(EnqueueError::Duplicate), got {other:?}"),
    }
}

/// m1 (review round 2): `insert_row`'s generalization persists `envelope.message_type` **as
/// carried on the `SerializedEnvelope`**, not a name/version derived from any Rust type. Proven
/// with `Envelope::from_parts`, whose `message_type` names a type no `Message` impl in this
/// binary declares.
async fn stage_persists_the_envelopes_own_message_type_not_a_rust_types_constants() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);

    let message_type = MessageType::new("a.type.no.rust.struct.declares", 7);
    let serialized = reliar_core::SerializedEnvelope::from_parts(
        MessageId::new(),
        message_type.clone(),
        bytes::Bytes::from_static(b"{}"),
        Metadata::default(),
        None,
    );

    let mut tx = pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    let row: (String, i32) =
        sqlx::query_as("SELECT message_type, message_version FROM outbox WHERE id = $1")
            .bind(serialized.id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.0, message_type.name());
    assert_eq!(row.1, i32::from(message_type.version()));
}

/// m3 (review round 2): the staging path was never exercised against a **non-default** schema —
/// only `enqueue`/`enqueue_with` were (`outbox_non_default_schema.rs`). Same shape, through
/// `stage()` instead: migrate into a custom schema, connect with it first on the pool's own
/// `search_path` (construction's fail-fast check needs that regardless of
/// `enqueue_sets_search_path` — that setting only changes what one `stage()`/`enqueue` call does
/// transaction-locally, never the startup verification), and confirm the row lands there.
async fn stage_honours_a_non_default_schema() {
    const CUSTOM_SCHEMA: &str = "acme_reliar_routing";

    let base = common::fresh_unmigrated_db().await;
    reliar_store_postgres::migrate(
        &base,
        reliar_store_postgres::MigrateOptions::default().schema(CUSTOM_SCHEMA),
    )
    .await
    .expect("migrate into a non-default schema");

    let base_options = base.connect_options().as_ref().clone();
    let scoped_pool = sqlx::PgPool::connect_with(
        base_options.options([("search_path", &format!("{CUSTOM_SCHEMA},public"))]),
    )
    .await
    .unwrap();
    let settings = PostgresOutboxSettings::default().schema(CUSTOM_SCHEMA);
    let store = PostgresOutboxStore::with_settings(scoped_pool.clone(), settings)
        .await
        .expect("construction succeeds when search_path matches the configured schema");
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 5 }).build());
    let id = serialized.id;

    let mut tx = scoped_pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .expect("stage succeeds in the custom schema");
    tx.commit().await.unwrap();

    // `CUSTOM_SCHEMA` is a compile-time constant, never user input — the same sanctioned
    // `AssertSqlSafe` exception `tests/postgres/common/mod.rs` uses for its own dynamic
    // `CREATE DATABASE` statements (test code, not the crate's own "macros only" queries).
    let count: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT count(*) FROM {CUSTOM_SCHEMA}.outbox WHERE id = $1"
    )))
    .bind(id.as_uuid())
    .fetch_one(&scoped_pool)
    .await
    .unwrap();
    assert_eq!(
        count, 1,
        "the row must land in the configured custom schema"
    );
}

/// m3 (review round 2): `OutboxStaging::stage` runs the shared `enqueue_sets_search_path` dance
/// too (set transaction-locally, restore only on success) — proven the same way
/// `outbox_schema_verification::enqueue_sets_search_path_restores_the_callers_value` proves it for
/// `enqueue`, but through `stage()`.
async fn stage_with_enqueue_sets_search_path_restores_the_callers_value() {
    let pool = common::fresh_db().await;
    let settings = PostgresOutboxSettings::default().enqueue_sets_search_path(true);
    let store = PostgresOutboxStore::with_settings(pool.clone(), settings)
        .await
        .unwrap();
    let outbox = build_outbox(store);

    let mut tx = pool.begin().await.unwrap();
    let before: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(&mut *tx)
        .await
        .unwrap();

    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 6 }).build());
    outbox
        .in_transaction(&mut tx)
        .publish(&serialized)
        .await
        .unwrap();

    let after: String = sqlx::query_scalar("SELECT current_setting('search_path')")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        before, after,
        "stage() must restore the caller's search_path"
    );
    tx.commit().await.unwrap();
}

/// R22 — the pg half of "a positional `Ok` is not durability": within **one** transaction, two
/// entries stage successfully and a third — reusing an id already committed by an earlier,
/// separate transaction — aborts. The whole transaction then rolls back, discarding the first
/// two entries' `Ok` results along with it, exactly the way real Postgres transactions are
/// all-or-nothing regardless of what any one statement returned along the way. The fake-driven
/// half of this proof (positional results, sequential staging) lives in
/// `crates/reliar-outbox/tests/routing_batch.rs`, which cannot model this transactional half.
async fn positional_ok_is_not_durability_a_later_stage_failure_aborts_the_whole_transaction() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);

    // Committed ahead of time, in its own transaction — its id is what the batch's third entry
    // reuses, forcing that `INSERT` (and therefore the whole batch's transaction) to abort.
    let already_committed = serialize(Envelope::builder(OrderCreated { order_id: 90 }).build());
    let mut seed_tx = pool.begin().await.unwrap();
    outbox
        .in_transaction(&mut seed_tx)
        .publish(&already_committed)
        .await
        .unwrap();
    seed_tx.commit().await.unwrap();

    let first = serialize(Envelope::builder(OrderCreated { order_id: 91 }).build());
    let second = serialize(Envelope::builder(OrderCreated { order_id: 92 }).build());
    let third_reused_id = already_committed.clone();

    let mut tx = pool.begin().await.unwrap();
    let results = outbox
        .in_transaction(&mut tx)
        .publish_batch(&[first.clone(), second.clone(), third_reused_id])
        .await;

    assert!(results[0].is_ok(), "the first statement was accepted");
    assert!(results[1].is_ok(), "the second statement was accepted");
    assert!(
        results[2].is_err(),
        "the third reuses a committed id and aborts the transaction"
    );

    // The transaction is already aborted server-side; roll it back explicitly to release the
    // connection cleanly (a plain `COMMIT` here would itself error).
    tx.rollback().await.unwrap();

    // Neither `first` nor `second` is durable: the whole transaction rolled back, discarding both
    // earlier `Ok`s along with the failing third entry. This is the point of the note — a
    // positional `Ok` means "the statement was accepted," never "the message is durable."
    for id in [first.id, second.id] {
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE id = $1")
            .bind(id.as_uuid())
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "an earlier positional Ok must not survive the transaction's abort"
        );
    }
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "routing_enqueue::stage_is_a_publisher_and_its_future_is_send_through_tokio_spawn",
            move || {
                rt.block_on(stage_is_a_publisher_and_its_future_is_send_through_tokio_spawn());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test("routing_enqueue::commit_makes_the_row_visible", move || {
            rt.block_on(commit_makes_the_row_visible());
            Ok(())
        }),
        libtest_mimic::Trial::test("routing_enqueue::rollback_leaves_nothing", move || {
            rt.block_on(rollback_leaves_nothing());
            Ok(())
        }),
        libtest_mimic::Trial::test(
            "routing_enqueue::content_type_is_the_envelopes_not_the_stores",
            move || {
                rt.block_on(content_type_is_the_envelopes_not_the_stores());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "routing_enqueue::duplicate_message_id_is_rejected",
            move || {
                rt.block_on(duplicate_message_id_is_rejected());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "routing_enqueue::stage_persists_the_envelopes_own_message_type_not_a_rust_types_constants",
            move || {
                rt.block_on(
                    stage_persists_the_envelopes_own_message_type_not_a_rust_types_constants(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "routing_enqueue::stage_honours_a_non_default_schema",
            move || {
                rt.block_on(stage_honours_a_non_default_schema());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "routing_enqueue::stage_with_enqueue_sets_search_path_restores_the_callers_value",
            move || {
                rt.block_on(stage_with_enqueue_sets_search_path_restores_the_callers_value());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "routing_enqueue::positional_ok_is_not_durability_a_later_stage_failure_aborts_the_whole_transaction",
            move || {
                rt.block_on(
                    positional_ok_is_not_durability_a_later_stage_failure_aborts_the_whole_transaction(),
                );
                Ok(())
            },
        ),
    ]
}
