//! Contract §7 J5 — `sent_at` is not a promoted column; it round-trips through
//! `MetadataRest::delivery.sent_at_ms` as epoch milliseconds (`records::encode_epoch_millis`/
//! `decode_epoch_millis`), specifically so that `sent_at` arithmetic can never panic on the
//! enqueue path the way an RFC 3339 formatter would at the edges of the representable range
//! (`records.rs` module docs). Both functions are `pub(crate)`, so this file proves them through
//! the crate's public surface: `tests/outbox_roundtrip.rs` already proves the happy path
//! (`Date::MIN`/`Date::MAX`/epoch round-trip); this file proves the two failure/extreme shapes
//! that codec can't reach through a valid `OffsetDateTime` input at all:
//!
//! 1. a `sent_at_ms` value written directly into the JSONB column (bypassing every Rust-level
//!    `OffsetDateTime` constructor) at `i64::MIN`/`i64::MAX` — outside what
//!    `OffsetDateTime::from_unix_timestamp_nanos` can represent — must poison the row cleanly
//!    (§19.5), never panic `acquire`;
//! 2. `encode_epoch_millis`'s saturating arithmetic, proven indirectly by round-tripping the
//!    exact instants nearest the boundary at millisecond resolution (`Date::MAX` at
//!    `23:59:59.999`), where a non-saturating implementation would be most likely to overflow.

use crate::common;

use crate::common::OrderCreated;
use reliar_core::Envelope;
use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;
use time::OffsetDateTime;

/// Inserts a row whose `metadata` JSONB carries `delivery.sent_at_ms = ms` directly — the only
/// way to reach a `sent_at_ms` value no `OffsetDateTime` builder could ever produce, since every
/// public constructor rejects a year outside `-9999..=9999` before `encode_epoch_millis` runs.
async fn seed_with_raw_sent_at_ms(pool: &sqlx::PgPool, ms: i64) -> uuid::Uuid {
    let id = uuid::Uuid::now_v7();
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, metadata, metadata_version, available_at) \
         VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', \
                 jsonb_build_object('delivery', jsonb_build_object('sent_at_ms', $2::bigint)), \
                 1, now())",
    )
    .bind(id)
    .bind(ms)
    .execute(pool)
    .await
    .unwrap();
    id
}

async fn sent_at_ms_at_i64_extremes_poisons_the_row_instead_of_panicking() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let max_id = seed_with_raw_sent_at_ms(&pool, i64::MAX).await;
    let min_id = seed_with_raw_sent_at_ms(&pool, i64::MIN).await;
    let good = Envelope::builder(OrderCreated { order_id: 1 }).build();
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &good).await.unwrap();
    tx.commit().await.unwrap();

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(10))
        .await
        .unwrap();

    assert_eq!(
        batch.records.len(),
        1,
        "only the well-formed row is delivered"
    );
    assert_eq!(batch.records[0].envelope.id, good.id);

    let poisoned_ids: std::collections::HashSet<uuid::Uuid> =
        batch.poisoned.iter().map(|p| p.id.as_uuid()).collect();
    assert_eq!(
        poisoned_ids,
        std::collections::HashSet::from([max_id, min_id]),
        "both out-of-range sent_at_ms rows are reported poisoned"
    );

    for id in [max_id, min_id] {
        let (dead_at_is_set, dead_reason): (bool, Option<String>) =
            sqlx::query_as("SELECT dead_at IS NOT NULL, dead_reason FROM outbox WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(dead_at_is_set);
        assert_eq!(dead_reason.as_deref(), Some("undecodable"));
    }
}

async fn sent_at_round_trips_at_millisecond_resolution_near_the_max_representable_instant() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    // `Date::MAX` (`9999-12-31`) at the last representable millisecond of the day — the value
    // closest to overflowing `encode_epoch_millis`'s `unix_timestamp() * 1000 + millis_of_second`
    // that a real `OffsetDateTime` can still produce.
    let sent_at = OffsetDateTime::from_unix_timestamp(253_402_300_799)
        .unwrap()
        .replace_millisecond(999)
        .unwrap();

    let mut envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
    envelope.metadata.delivery.sent_at = Some(sent_at);
    let mut tx = pool.begin().await.unwrap();
    store.enqueue(&mut tx, &envelope).await.unwrap();
    tx.commit().await.unwrap();

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(
        batch.records[0].envelope.metadata.delivery.sent_at,
        Some(sent_at)
    );
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_epoch_millis_codec::sent_at_ms_at_i64_extremes_poisons_the_row_instead_of_panicking",
            move || {
                rt.block_on(sent_at_ms_at_i64_extremes_poisons_the_row_instead_of_panicking());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_epoch_millis_codec::sent_at_round_trips_at_millisecond_resolution_near_the_max_representable_instant",
            move || {
                rt.block_on(sent_at_round_trips_at_millisecond_resolution_near_the_max_representable_instant());
                Ok(())
            },
        ),
    ]
}
