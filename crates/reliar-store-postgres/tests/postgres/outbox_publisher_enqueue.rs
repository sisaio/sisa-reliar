//! E17–E19 (outbox-publisher contract `docs/architecture/outbox-publisher-contract.md` §9, §5;
//! ADR 0036) — [`PostgresOutboxStore`]'s `OutboxEnqueue` impl, exercised through
//! [`reliar_outbox::OutboxPublisher::enqueue`]/`enqueue_batch` against a real Postgres.

use crate::common;
use crate::common::OrderCreated;

use reliar_core::{Envelope, Serializer as _};
use reliar_outbox::{OutboxPublisher, RecordingPublisher};
use reliar_store_postgres::PostgresOutboxStore;

/// [`common::serialize_with`]-equivalent for this crate: the caller's own three-line block
/// (contract §2.1) — nothing in `reliar-outbox`/`reliar-store-postgres` serializes on this path
/// any more (ADR 0036 §4).
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
    OutboxPublisher::new(store, RecordingPublisher::default())
}

async fn count_outbox_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn count_widget_rows(pool: &sqlx::PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM widgets")
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn create_widgets_table(pool: &sqlx::PgPool) {
    sqlx::query("CREATE TABLE widgets (id bigserial PRIMARY KEY)")
        .execute(pool)
        .await
        .unwrap();
}

/// E19 — the future `OutboxPublisher::enqueue` returns through `PostgresOutboxStore`'s
/// `OutboxEnqueue` impl is `Send` even though it borrows a non-`'static` `Transaction<'_,
/// Postgres>` scope: `tokio::spawn` requires exactly that, and proving it requires the compiler
/// to check the impl for genericity over the transaction's lifetime (keeps 0.3.0's R23
/// regression guard for the `where 'c: 'a` trap an earlier lifetime shape had).
async fn enqueue_is_a_publisher_and_its_future_is_send_through_tokio_spawn() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 7 }).build());
    let id = serialized.id;

    // The realistic host shape: acquire a transaction, hand the whole call to a spawned task.
    // `tx` is `Transaction<'static, Postgres>` (owned from the pool), but the reference `&mut
    // tx` `enqueue` borrows lives only inside this async block's own scope, not `'static` — the
    // non-`'static` transaction scope this test needs.
    let mut tx = pool.begin().await.unwrap();
    tokio::spawn(async move {
        outbox.enqueue(&mut tx, &serialized).await.unwrap();
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

/// E17 — atomicity: the row is invisible before commit and present after, exactly like the
/// store's own `enqueue` (`outbox_enqueue_atomic.rs`), but reached through
/// `OutboxPublisher::enqueue` (the enqueue path) instead.
async fn commit_makes_the_row_visible() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 1 }).build());
    let id = serialized.id;

    let mut tx = pool.begin().await.unwrap();
    outbox.enqueue(&mut tx, &serialized).await.unwrap();

    let count_before = count_outbox_rows(&pool).await;
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

/// E17 — rollback leaves nothing, same guarantee as the store's own `enqueue`.
async fn rollback_leaves_nothing() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 2 }).build());

    let mut tx = pool.begin().await.unwrap();
    outbox.enqueue(&mut tx, &serialized).await.unwrap();
    tx.rollback().await.unwrap();

    assert_eq!(count_outbox_rows(&pool).await, 0);
}

/// E17 — `enqueue` persists `envelope.metadata.delivery.content_type` **verbatim** — the
/// caller's own content type, never [`PostgresOutboxStore::content_type`] (contract §5). Proven
/// by enqueuing through a serializer whose `ContentType` differs from the store's own default.
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
    outbox.enqueue(&mut tx, &serialized).await.unwrap();
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

/// E17 — a reused `MessageId` maps to [`reliar_store_postgres::EnqueueError::Duplicate`], same
/// as the store's inherent `enqueue`
/// (`outbox_enqueue_atomic::duplicate_message_id_aborts_the_transaction`).
async fn duplicate_message_id_is_rejected() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 4 }).build());

    let mut tx = pool.begin().await.unwrap();
    outbox.enqueue(&mut tx, &serialized).await.unwrap();
    tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    let result = outbox.enqueue(&mut tx, &serialized).await;
    match result.expect_err("a reused MessageId must be rejected") {
        reliar_store_postgres::EnqueueError::Duplicate { id } => {
            assert_eq!(id, serialized.id);
        }
        other => panic!("expected EnqueueError::Duplicate, got {other:?}"),
    }
}

/// E18 — an enqueue failure leaves the transaction aborted: the next statement issued on `tx`
/// fails, and an earlier business write made in the same transaction is gone once the caller
/// gives up on it (rolls back, or a `commit` attempt itself errors) — the "treat any enqueue
/// error as *abort this transaction*" rule the contract's `enqueue` rustdoc states.
async fn an_enqueue_failure_aborts_the_transaction_and_discards_the_earlier_business_write() {
    let pool = common::fresh_db().await;
    create_widgets_table(&pool).await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let outbox = build_outbox(store);
    let serialized = serialize(Envelope::builder(OrderCreated { order_id: 5 }).build());

    // Committed ahead of time, in its own transaction — its id is what the second `enqueue`
    // below reuses, forcing that `INSERT` (and therefore the whole transaction) to abort.
    let mut seed_tx = pool.begin().await.unwrap();
    outbox.enqueue(&mut seed_tx, &serialized).await.unwrap();
    seed_tx.commit().await.unwrap();

    let mut tx = pool.begin().await.unwrap();
    sqlx::query("INSERT INTO widgets DEFAULT VALUES")
        .execute(&mut *tx)
        .await
        .unwrap();

    let result = outbox.enqueue(&mut tx, &serialized).await;
    assert!(
        result.is_err(),
        "reusing an already-committed MessageId must fail"
    );

    // The transaction is already aborted server-side: the next statement on it fails too.
    let next_statement = sqlx::query("INSERT INTO widgets DEFAULT VALUES")
        .execute(&mut *tx)
        .await;
    assert!(
        next_statement.is_err(),
        "a statement on an aborted transaction must fail"
    );

    tx.rollback().await.unwrap();

    // The earlier business write never survives — the whole transaction was all-or-nothing.
    assert_eq!(
        count_widget_rows(&pool).await,
        0,
        "an earlier write in the aborted transaction must not be durable"
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_publisher_enqueue::enqueue_is_a_publisher_and_its_future_is_send_through_tokio_spawn",
            move || {
                rt.block_on(enqueue_is_a_publisher_and_its_future_is_send_through_tokio_spawn());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_publisher_enqueue::commit_makes_the_row_visible",
            move || {
                rt.block_on(commit_makes_the_row_visible());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_publisher_enqueue::rollback_leaves_nothing",
            move || {
                rt.block_on(rollback_leaves_nothing());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_publisher_enqueue::content_type_is_the_envelopes_not_the_stores",
            move || {
                rt.block_on(content_type_is_the_envelopes_not_the_stores());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_publisher_enqueue::duplicate_message_id_is_rejected",
            move || {
                rt.block_on(duplicate_message_id_is_rejected());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_publisher_enqueue::an_enqueue_failure_aborts_the_transaction_and_discards_the_earlier_business_write",
            move || {
                rt.block_on(
                    an_enqueue_failure_aborts_the_transaction_and_discards_the_earlier_business_write(),
                );
                Ok(())
            },
        ),
    ]
}
