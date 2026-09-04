//! §43.A.20 — `list_dead(DeadQuery)` returns dead rows with `attempts`, `last_error`,
//! `dead_at`, `DeadReason`; `retry_dead(refs)` — over `MessageRef`s from `list_dead` — clears
//! `dead_at`, resets attempts/lease and makes them claimable. `ORDER BY sequence ASC` is
//! normative: pagination is proven with rows whose `dead_at` order differs from `sequence`
//! order.

use crate::common;

use reliar_outbox::{AcquireRequest, DeadQuery, OutboxDeadLetters, OutboxStore, WorkerId};
use reliar_store_postgres::PostgresOutboxStore;

const MAX_LIST_DEAD_LIMIT: u32 = 1000;

async fn seed_dead_in_order(pool: &sqlx::PgPool, count: u32) -> Vec<uuid::Uuid> {
    let mut ids = Vec::with_capacity(count as usize);
    for i in 0..count {
        let id = uuid::Uuid::now_v7();
        // `dead_at` deliberately runs in the *opposite* order of `sequence` (insertion order),
        // so a cursor over `dead_at` would return these out of order while `sequence` would not.
        sqlx::query(
            "INSERT INTO outbox (id, message_type, message_version, conversation_id, \
                                  content_type, payload, available_at, dead_at, dead_reason, \
                                  attempts, last_error) \
             VALUES ($1, 'orders.created', 1, $1, 'application/json', '{}', now(), \
                     now() - ($2 || ' minutes')::interval, 'permanent_error', 3, 'boom')",
        )
        .bind(id)
        .bind((count - i).to_string())
        .execute(pool)
        .await
        .unwrap();
        ids.push(id);
    }
    ids
}

async fn list_dead_returns_attempts_error_dead_at_and_reason() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let ids = seed_dead_in_order(&pool, 1).await;

    let page = store.list_dead(DeadQuery::default()).await.unwrap();
    assert_eq!(page.records.len(), 1);
    let record = &page.records[0];
    assert_eq!(record.envelope.id.as_uuid(), ids[0]);
    assert_eq!(record.attempts, 3);
    assert_eq!(record.last_error.as_deref(), Some("boom"));
    assert!(record.dead_at.is_some());
    assert_eq!(
        record.dead_reason,
        Some(reliar_outbox::DeadReason::PermanentError)
    );
}

async fn list_dead_paginates_by_sequence_even_when_dead_at_order_differs() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let ids = seed_dead_in_order(&pool, 5).await; // ids[0] has the *latest* dead_at

    let mut seen = Vec::new();
    let mut cursor = None;
    loop {
        let mut query = DeadQuery::default().limit(2);
        if let Some(after) = cursor {
            query = query.after_sequence(after);
        }
        let page = store.list_dead(query).await.unwrap();
        seen.extend(page.records.iter().map(|r| r.envelope.id.as_uuid()));
        match page.next_after_sequence {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }

    assert_eq!(
        seen, ids,
        "pagination must follow insertion (sequence) order, not dead_at order"
    );
}

async fn retry_dead_clears_dead_state_and_makes_the_row_claimable() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    let ids = seed_dead_in_order(&pool, 1).await;
    let page = store.list_dead(DeadQuery::default()).await.unwrap();
    let refs: Vec<_> = page
        .records
        .iter()
        .map(reliar_outbox::OutboxRecord::message_ref)
        .collect();

    let affected = store.retry_dead(&refs).await.unwrap();
    assert_eq!(affected, 1);

    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    assert_eq!(batch.records.len(), 1);
    assert_eq!(batch.records[0].envelope.id.as_uuid(), ids[0]);
    assert_eq!(
        batch.records[0].attempts, 0,
        "retry_dead is the only op that resets attempts"
    );
    assert!(batch.records[0].dead_at.is_none());
}

async fn list_dead_caps_a_caller_supplied_limit_above_the_provider_maximum() {
    // Review 3 M4 / review 4 major 2 — `DeadQuery::limit` must never reach the database
    // uncapped. `len() <= MAX` alone proves nothing (it's vacuously true for a store that
    // returns nothing, or one that doesn't cap at all as long as fewer rows happen to exist) —
    // seeding one row more than the cap and asserting the count is *exactly* the cap, with a
    // cursor for the remainder, is what actually proves the limit was applied.
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    let row_count = MAX_LIST_DEAD_LIMIT + 5;
    sqlx::query(
        "INSERT INTO outbox (id, message_type, message_version, conversation_id, content_type, \
                              payload, available_at, dead_at, dead_reason, attempts, last_error) \
         SELECT uuidv7(), 'orders.created', 1, uuidv7(), 'application/json', '{}', now(), \
                now() - interval '1 hour', 'permanent_error', 1, 'boom' \
         FROM generate_series(1, $1)",
    )
    .bind(i64::from(row_count))
    .execute(&pool)
    .await
    .unwrap();

    let page = store
        .list_dead(DeadQuery::default().limit(5000))
        .await
        .unwrap();
    assert_eq!(
        page.records.len(),
        MAX_LIST_DEAD_LIMIT as usize,
        "a limit(5000) request over {row_count} seeded rows must be capped to exactly {MAX_LIST_DEAD_LIMIT}"
    );
    assert!(
        page.next_after_sequence.is_some(),
        "capped at the limit with rows left over must still hand back a cursor for them"
    );
}

async fn purge_dead_on_a_never_dead_ref_affects_zero_rows_and_leaves_it_intact() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
    common::seed(&store, &pool, 1).await;

    // Acquired but never failed/dead — `purge_dead` on it must be a safe no-op, not an error
    // and not a deletion of a row that was never dead.
    let batch = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .unwrap();
    let message_ref = batch.records[0].message_ref();

    let affected = store.purge_dead(&[message_ref]).await.unwrap();
    assert_eq!(affected, 0);

    let still_present: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox WHERE id = $1")
        .bind(message_ref.id.as_uuid())
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(still_present, 1);
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "outbox_dead_letters::list_dead_returns_attempts_error_dead_at_and_reason",
            move || {
                rt.block_on(list_dead_returns_attempts_error_dead_at_and_reason());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_dead_letters::list_dead_paginates_by_sequence_even_when_dead_at_order_differs",
            move || {
                rt.block_on(list_dead_paginates_by_sequence_even_when_dead_at_order_differs());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_dead_letters::retry_dead_clears_dead_state_and_makes_the_row_claimable",
            move || {
                rt.block_on(retry_dead_clears_dead_state_and_makes_the_row_claimable());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_dead_letters::list_dead_caps_a_caller_supplied_limit_above_the_provider_maximum",
            move || {
                rt.block_on(list_dead_caps_a_caller_supplied_limit_above_the_provider_maximum());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "outbox_dead_letters::purge_dead_on_a_never_dead_ref_affects_zero_rows_and_leaves_it_intact",
            move || {
                rt.block_on(
                    purge_dead_on_a_never_dead_ref_affects_zero_rows_and_leaves_it_intact(),
                );
                Ok(())
            },
        ),
    ]
}
