//! E1 (story C9, contract §7): two trials against a real Postgres and a real `JetStream` stream.
//!
//! `dispatcher_drains_the_outbox_into_jetstream_with_matching_id_and_body` proves C9's actual
//! claim — every one of `N` enqueued rows ends `published_at` **and** appears in the stream with
//! a matching `reliar-message-id` header and byte-identical raw body — by waiting for all `N`
//! rows before cancelling, then asserting a clean drain: every lease released, no row
//! dead-lettered. A dispatcher that stalls after the first row (or any row short of `N`) fails
//! this trial, because it waits for every row rather than merely the first.
//!
//! `cancelling_after_the_first_claim_leaves_the_rest_provably_unclaimed` is the separate,
//! deterministic in-flight-cancel property review round 2 asked for: `batch_size(1)` plus a poll
//! interval far longer than the trial's own real-time budget guarantees exactly one row is ever
//! claimed before cancellation lands on the dispatcher's very next loop iteration (`select!` is
//! `biased`, cancellation checked first) — no timing race, no assumption about scheduler speed.
//! With `N` seeded rows far larger than `batch_size`, the rest are provably never claimed:
//! `attempts == 0` for every one of them.

use std::collections::HashMap;
use std::time::Duration;

use bytes::Bytes;
use reliar_outbox::{DispatcherSettings, OutboxDispatcher};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings, headers};
use tokio_util::sync::CancellationToken;

use crate::common;

const N: u64 = 5;

async fn dispatcher_drains_the_outbox_into_jetstream_with_matching_id_and_body() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");
    let envelopes = common::seed(&store, &pool, N).await;

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e1.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E1_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(common::fast_settings())
        .build()
        .expect("dispatcher config");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    // Wait for every one of the N rows to publish before cancelling — C9's claim is about the
    // whole batch, not the first row to land.
    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::published_row_count(&pool).await >= i64::try_from(N).unwrap() }
    })
    .await;
    cancel.cancel();

    handle
        .await
        .expect("dispatcher task did not panic")
        .expect("dispatcher run returned Ok — every publish drained cleanly");

    assert_eq!(
        common::locked_row_count(&pool).await,
        0,
        "run() must release every lease before returning"
    );
    assert_eq!(
        common::dead_row_count(&pool).await,
        0,
        "a clean drain must never dead-letter a row"
    );

    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        N,
        "the stream must hold exactly one message per enqueued row"
    );

    let mut expected: HashMap<String, (&'static str, Bytes)> = envelopes
        .iter()
        .map(|envelope| {
            let body = Bytes::from(serde_json::to_vec(&envelope.body).expect("serialize body"));
            (envelope.id.to_string(), ("orders.created", body))
        })
        .collect();

    for sequence in 1..=N {
        let stored = common::stream_raw_message(&context, &stream_name, sequence).await;
        let id = common::header_value(&stored.headers, headers::MESSAGE_ID);
        let (expected_type, expected_body) = expected
            .remove(&id)
            .unwrap_or_else(|| panic!("unexpected message id {id} in the stream"));
        assert_eq!(
            common::header_value(&stored.headers, headers::MESSAGE_TYPE),
            expected_type
        );
        assert_eq!(
            stored.payload, expected_body,
            "raw body must match the enqueued envelope byte for byte — no outer JSON wrapper"
        );
    }
    assert!(
        expected.is_empty(),
        "every enqueued envelope must appear exactly once in the stream"
    );

    common::delete_stream(&context, &stream_name).await;
}

/// Far more rows than `batch_size(1)` ever lets a single claim take, so the rows this trial
/// leaves behind are provably untouched, not merely "probably still pending".
const CLAIM_STOP_N: u64 = 50;

async fn cancelling_after_the_first_claim_leaves_the_rest_provably_unclaimed() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");
    common::seed(&store, &pool, CLAIM_STOP_N).await;

    let context = common::jetstream_context().await;
    let id = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e1b.{id}");
    let stream_name = format!("RELIAR_SYSTEST_E1B_{id}");
    common::create_stream(&context, &stream_name, &format!("{prefix}.>")).await;

    let publisher = NatsPublisher::new(
        context.clone(),
        NatsSettings::default().subject_prefix(prefix),
    )
    .expect("valid settings");

    // `batch_size(1)` caps every single claim statement to one row regardless of
    // `max_in_flight`; a ten-minute poll interval guarantees the *second* poll's due time never
    // arrives before this trial has long since cancelled and returned — the claim loop is
    // `sleep_until(next_poll_at)`-gated (`dispatcher.rs`), so no second claim can happen without
    // that sleep resolving first.
    let settings = DispatcherSettings::default()
        .batch_size(1)
        .poll_interval(Duration::from_secs(600))
        .idle_poll_interval(Duration::from_secs(600))
        .lease(Duration::from_secs(30))
        .drain_timeout(Duration::from_secs(5));

    let dispatcher = OutboxDispatcher::builder(store.clone(), publisher)
        .settings(settings)
        .build()
        .expect("dispatcher config");

    let cancel = CancellationToken::new();
    let handle = tokio::spawn(dispatcher.run(cancel.clone()));

    common::wait_until(Duration::from_secs(15), || {
        let pool = pool.clone();
        async move { common::published_row_count(&pool).await >= 1 }
    })
    .await;
    cancel.cancel();

    handle
        .await
        .expect("dispatcher task did not panic")
        .expect("dispatcher run returned Ok");

    assert_eq!(
        common::published_row_count(&pool).await,
        1,
        "batch_size(1) plus a ten-minute poll interval must cap this run to exactly one claim"
    );
    assert_eq!(
        common::locked_row_count(&pool).await,
        0,
        "the one claimed row completed and released its lease before cancellation"
    );
    assert_eq!(common::dead_row_count(&pool).await, 0);
    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        1,
        "only the one claimed row can have reached the stream"
    );
    assert_eq!(
        common::never_claimed_row_count(&pool).await,
        i64::try_from(CLAIM_STOP_N - 1).unwrap(),
        "every row but the one claimed must show attempts == 0 — never acquired, hence never \
         locked, published or dead"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "e1_outbox_drains_into_jetstream::dispatcher_drains_the_outbox_into_jetstream_with_matching_id_and_body",
            move || {
                rt.block_on(
                    dispatcher_drains_the_outbox_into_jetstream_with_matching_id_and_body(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "e1_outbox_drains_into_jetstream::cancelling_after_the_first_claim_leaves_the_rest_provably_unclaimed",
            move || {
                rt.block_on(cancelling_after_the_first_claim_leaves_the_rest_provably_unclaimed());
                Ok(())
            },
        ),
    ]
}
