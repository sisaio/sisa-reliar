//! E4 (RELIAR-34, PO addendum 2026-09-05, ADR 0026 §3): an envelope that is perfectly legal to
//! build and store — `reliar-core`'s `Headers::insert` only rejects the reserved `reliar-` prefix,
//! control characters and length/count caps — can still be **permanently** unrepresentable on this
//! transport. A custom header key containing a space is exactly such a case: `async_nats::HeaderName`
//! only accepts ASCII-graphic, colon-free names, so `NatsEnvelopeMapper::encode` rejects it as
//! `NatsMapError::UnsupportedHeaderName`, which `NatsPublishError::Map` classifies `Permanent`
//! (`Classify`). The row must dead-letter on its very first attempt, never be retried, and its
//! `last_error` must identify the failure without ever printing the header's value.

use std::time::Duration;

use reliar_core::{Envelope, MessageId};
use reliar_outbox::{
    AcquireRequest, DeadQuery, DeadReason, OutboxDeadLetters, OutboxDispatcher, OutboxStore,
    WorkerId,
};
use reliar_store_postgres::PostgresOutboxStore;
use reliar_transport_nats::{NatsPublisher, NatsSettings};
use tokio_util::sync::CancellationToken;

use crate::common;
use crate::common::OrderCreated;

/// A header value distinctive enough that its accidental presence in `last_error` could not be
/// mistaken for something else.
const SECRET_HEADER_VALUE: &str = "e4-header-value-must-never-be-logged";

/// Fetches `id`'s dead-letter record, panicking if it is not (yet) dead.
async fn dead_record(store: &PostgresOutboxStore, id: MessageId) -> reliar_outbox::OutboxRecord {
    store
        .list_dead(DeadQuery::default())
        .await
        .expect("list dead rows")
        .records
        .into_iter()
        .find(|r| r.envelope.id == id)
        .expect("the row must be dead")
}

async fn permanent_mapping_failure_dead_letters_without_retry() {
    let pool = common::fresh_postgres_db().await;
    let store = PostgresOutboxStore::new(pool.clone())
        .await
        .expect("connect store");

    // Legal to build and enqueue — `reliar-core` validates less than a NATS header name allows
    // (ADR 0026 §3) — but permanently unrepresentable on this transport.
    let envelope = Envelope::builder(OrderCreated { order_id: 0 })
        .header("bad header", SECRET_HEADER_VALUE)
        .expect("reliar-core accepts a header key containing a space")
        .build();
    let id: MessageId = envelope.id;
    let mut tx = pool.begin().await.expect("begin tx");
    store.enqueue(&mut tx, &envelope).await.expect("enqueue");
    tx.commit().await.expect("commit tx");

    let context = common::jetstream_context().await;
    let uid = uuid::Uuid::now_v7().simple();
    let prefix = format!("reliar.systest.e4.{uid}");
    let stream_name = format!("RELIAR_SYSTEST_E4_{uid}");
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

    common::wait_until(Duration::from_secs(15), || {
        let store = &store;
        async move {
            store
                .list_dead(DeadQuery::default())
                .await
                .expect("list dead rows")
                .records
                .iter()
                .any(|r| r.envelope.id == id)
        }
    })
    .await;

    cancel.cancel();
    handle
        .await
        .expect("dispatcher task did not panic")
        .expect("dispatcher run returned Ok");

    assert_eq!(
        common::stream_message_count(&context, &stream_name).await,
        0,
        "an envelope that never encodes must never reach the wire"
    );

    let record = dead_record(&store, id).await;
    assert_eq!(record.dead_reason, Some(DeadReason::PermanentError));
    assert_eq!(
        record.attempts, 1,
        "a permanent failure dead-letters on its first attempt — never retried"
    );
    let last_error = record
        .last_error
        .as_deref()
        .expect("a dead row must carry a last_error");
    assert!(
        last_error.contains("not a legal NATS header name"),
        "last_error must identify the mapping failure: {last_error:?}"
    );
    assert!(
        !last_error.contains(SECRET_HEADER_VALUE),
        "last_error must never contain the header's value: {last_error:?}"
    );

    // A later poll must not retry a dead row. `acquire` is exactly what every dispatcher poll
    // calls to claim work, so proving it excludes this row proves no future poll would retry it —
    // deterministic, no wall-clock wait needed.
    let claimed_again = store
        .acquire(AcquireRequest::new(WorkerId::generate()))
        .await
        .expect("acquire");
    assert!(
        claimed_again.records.iter().all(|r| r.envelope.id != id),
        "a dead row must never be reclaimed by acquire"
    );
    assert_eq!(
        dead_record(&store, id).await.attempts,
        1,
        "attempts must stay frozen — a dead row is never reclaimed"
    );

    common::delete_stream(&context, &stream_name).await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![libtest_mimic::Trial::test(
        "e4_unrepresentable_envelope_dead_letters::permanent_mapping_failure_dead_letters_without_retry",
        move || {
            rt.block_on(permanent_mapping_failure_dead_letters_without_retry());
            Ok(())
        },
    )]
}
