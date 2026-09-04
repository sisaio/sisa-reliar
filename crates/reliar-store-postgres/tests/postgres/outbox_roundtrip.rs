//! §43.A.4 — an enqueued envelope, acquired back from the store, equals the original
//! `SerializedEnvelope` (with `content_type` filled in from the store's serializer, per contract
//! §4); the promoted-column + `MetadataRest` merge is property-tested for round-trip, including
//! against a **non-JSON** serializer so the content-type rule is observed rather than assumed.
//!
//! `sent_at` and `expires_at` are exercised across the whole date range `time::OffsetDateTime`
//! can represent without the `large-dates` feature (`Date::MIN` = `-9999-01-01`, `Date::MAX` =
//! `9999-12-31`, unix seconds `-377_705_116_800..=253_402_300_799`) — `sent_at` is not a
//! promoted column, it round-trips through `MetadataRest::delivery.sent_at_ms` as epoch
//! milliseconds (contract §7 J5), so this is the property test that would catch a saturation or
//! precision bug at either end of the representable range. A true "year 10000" cannot be
//! constructed at all without `large-dates` (`OffsetDateTime` itself rejects it), so `Date::MAX`
//! is the closest real edge and is exercised deliberately, not just left to chance.

use crate::common;

use crate::common::{OrderCreated, TestVndSerializer};
use proptest::prelude::*;
use reliar_core::{
    ConversationId, CorrelationId, EndpointAddress, Envelope, JsonSerializer, RequestId, Serializer,
};
use reliar_outbox::{AcquireRequest, OutboxStore, WorkerId};
use reliar_store_postgres::{EnqueueOptions, PostgresOutboxStore};
use sqlx::PgPool;
use time::OffsetDateTime;
use uuid::Uuid;

/// `time::Date::MIN` (`-9999-01-01T00:00:00Z`) as unix seconds.
const MIN_UNIX_SECS: i64 = -377_705_116_800;
/// `time::Date::MAX` (`9999-12-31T23:59:59Z`) as unix seconds.
const MAX_UNIX_SECS: i64 = 253_402_300_799;

/// One arbitrary combination of every field §24.2 carves out of the JSONB remainder plus a
/// handful of custom headers, restricted to values each newtype's own validation accepts (no
/// control characters, within length caps) so a proptest failure is about the round-trip, never
/// about input validity.
// Split into two nested tuples of at most 7 elements each: `std::fmt::Debug` is only
// implemented for tuples up to arity 12, and proptest's macro requires `Debug` on the whole
// generated value — a flat 14-tuple does not qualify, but a 2-tuple of two 7-tuples does (each
// inner tuple is `Debug`, so the outer pair trivially is too).
#[allow(clippy::type_complexity)]
fn arb_fields() -> impl Strategy<
    Value = (
        (
            Option<String>,
            Option<u128>,
            Option<u128>,
            Option<String>,
            Option<String>,
            Option<String>,
            Option<String>,
        ),
        (
            Option<String>,
            Option<String>,
            Vec<(String, String)>,
            Option<i64>,
            Option<i64>,
            Option<String>,
            Option<String>,
        ),
    ),
> {
    let safe_string = "[a-zA-Z0-9_-]{1,32}";
    // `expires_at` is enforced by `acquire`'s claim predicate (`expires_at IS NULL OR
    // expires_at > now()`) — a past value would make the row unclaimable, which is the expiry
    // sweep's own scenario (`tests/outbox_purge.rs`), not this round-trip's. Bounded to the
    // future (from "well past this test process's lifetime" up to `Date::MAX`) so every
    // generated value is guaranteed claimable.
    let future_floor = OffsetDateTime::now_utc().unix_timestamp() + 86_400;
    (
        (
            proptest::option::of(safe_string),
            proptest::option::of(any::<u128>()),
            proptest::option::of(any::<u128>()),
            proptest::option::of(safe_string),
            proptest::option::of(safe_string),
            proptest::option::of("[0-9a-f]{32}"),
            proptest::option::of("[0-9a-f]{16}"),
        ),
        (
            proptest::option::of(safe_string),
            proptest::option::of(safe_string),
            proptest::collection::vec((safe_string, safe_string), 0..4),
            proptest::option::of(MIN_UNIX_SECS..=MAX_UNIX_SECS),
            proptest::option::of(future_floor..=MAX_UNIX_SECS),
            proptest::option::of(safe_string),
            proptest::option::of(safe_string),
        ),
    )
}

#[allow(clippy::too_many_arguments)]
async fn run_roundtrip<Ser: Serializer + Send + Sync + 'static>(
    pool: PgPool,
    store: &PostgresOutboxStore<Ser>,
    serializer: &Ser,
    order_id: u64,
    correlation_id: Option<String>,
    causation: Option<u128>,
    request: Option<u128>,
    tenant_id: Option<String>,
    source: Option<String>,
    traceparent: Option<String>,
    tracestate: Option<String>,
    destination: Option<String>,
    reply_to: Option<String>,
    headers: Vec<(String, String)>,
    sent_at_secs: Option<i64>,
    expires_at_secs: Option<i64>,
    deduplication_id: Option<String>,
    ordering_key: Option<String>,
) {
    let mut builder = Envelope::builder(OrderCreated { order_id });
    if let Some(c) = &correlation_id {
        builder = builder.correlation_id(CorrelationId::parse(c.clone()).unwrap());
    }
    if let Some(c) = causation {
        builder = builder.causation(reliar_core::MessageId::from_uuid(Uuid::from_u128(c)));
    }
    if let Some(tenant) = &tenant_id {
        builder = builder.tenant(tenant.clone());
    }
    if let Some(tp) = &traceparent {
        builder = builder.trace(tp.clone(), tracestate.clone());
    }
    // Conversation rooting is decided by value (ADR 0011); exercise the non-default path too.
    builder = builder.conversation(ConversationId::new());
    for (k, v) in &headers {
        builder = builder.header(k.clone(), v.clone()).unwrap();
    }
    let mut envelope = builder.build();
    envelope.metadata.routing.source = source.map(|s| EndpointAddress::parse(s).unwrap());
    envelope.metadata.routing.destination = destination.map(|s| EndpointAddress::parse(s).unwrap());
    envelope.metadata.routing.reply_to = reply_to.map(|s| EndpointAddress::parse(s).unwrap());
    if let Some(r) = request {
        envelope.metadata.correlation.request_id = Some(RequestId::from_uuid(Uuid::from_u128(r)));
    }
    envelope.metadata.delivery.sent_at =
        sent_at_secs.map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap());
    envelope.metadata.delivery.expires_at =
        expires_at_secs.map(|secs| OffsetDateTime::from_unix_timestamp(secs).unwrap());
    envelope.metadata.delivery.deduplication_id = deduplication_id;

    let body_bytes = serializer.serialize(&envelope.body).ok().unwrap();
    let mut expected = envelope.clone().map_body(|_| body_bytes);
    expected.metadata.delivery.content_type = store.content_type().clone();

    let mut tx = pool.begin().await.unwrap();
    match &ordering_key {
        Some(key) => {
            store
                .enqueue_with(
                    &mut tx,
                    &envelope,
                    EnqueueOptions::default().ordering_key(key),
                )
                .await
                .unwrap();
        }
        None => {
            store.enqueue(&mut tx, &envelope).await.unwrap();
        }
    }
    tx.commit().await.unwrap();

    let worker = WorkerId::generate();
    let batch = store
        .acquire(AcquireRequest::new(worker).batch_size(10))
        .await
        .unwrap();
    let acquired = batch
        .records
        .into_iter()
        .find(|r| r.envelope.id == envelope.id)
        .expect("the enqueued row was claimed");

    assert_eq!(acquired.envelope, expected);
    assert_eq!(acquired.ordering_key, ordering_key);
}

// Hand-expanded `proptest! { #[test] fn ... }` sugar (RELIAR-27): that macro forwards whatever
// attributes the caller writes onto the generated `fn`, including `#[test]` — and `#[test]`-
// annotated items are stripped from compilation entirely unless the crate is built with
// `rustc --test`, which a `harness = false` binary (this one) explicitly is **not** (it supplies
// its own `main`, which `--test` mode's auto-generated `main` would collide with). Under the old
// per-file `#[[test]]` harness these two functions were real `#[test]` items and ran; folded into
// this `harness = false` binary, they silently vanished — the exact expansion below (mirroring
// `proptest`'s own `sugar.rs` `@_BODY` arm) reproduces identical behavior (12 cases, shrinking,
// the same panic-with-`runner`-display message on failure) without depending on `#[test]` at all.
#[allow(clippy::too_many_arguments)]
fn json_roundtrip() {
    let mut config = ProptestConfig::with_cases(12);
    config.source_file = Some(file!());
    let mut runner = proptest::test_runner::TestRunner::new(config);
    let strategy = (any::<u64>(), arb_fields());
    let outcome = runner.run(&strategy, |(order_id, fields)| {
        let (
            (correlation_id, causation, request, tenant_id, source, traceparent, tracestate),
            (
                destination,
                reply_to,
                headers,
                sent_at_secs,
                expires_at_secs,
                deduplication_id,
                ordering_key,
            ),
        ) = fields;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pool = common::fresh_db().await;
            let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();
            run_roundtrip(
                pool,
                &store,
                &JsonSerializer,
                order_id,
                correlation_id,
                causation,
                request,
                tenant_id,
                source,
                traceparent,
                tracestate,
                destination,
                reply_to,
                headers,
                sent_at_secs,
                expires_at_secs,
                deduplication_id,
                ordering_key,
            )
            .await;
        });
        Ok(())
    });
    if let Err(e) = outcome {
        panic!("{e}\n{runner}");
    }
}

#[allow(clippy::too_many_arguments)]
fn non_json_content_type_roundtrip() {
    let mut config = ProptestConfig::with_cases(12);
    config.source_file = Some(file!());
    let mut runner = proptest::test_runner::TestRunner::new(config);
    let strategy = (any::<u64>(), arb_fields());
    let outcome = runner.run(&strategy, |(order_id, fields)| {
        let (
            (correlation_id, causation, request, tenant_id, source, traceparent, tracestate),
            (
                destination,
                reply_to,
                headers,
                sent_at_secs,
                expires_at_secs,
                deduplication_id,
                ordering_key,
            ),
        ) = fields;
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let pool = common::fresh_db().await;
            let store = PostgresOutboxStore::connect(
                pool.clone(),
                reliar_store_postgres::PostgresOutboxSettings::default(),
                TestVndSerializer,
            )
            .await
            .unwrap();
            run_roundtrip(
                pool,
                &store,
                &TestVndSerializer,
                order_id,
                correlation_id,
                causation,
                request,
                tenant_id,
                source,
                traceparent,
                tracestate,
                destination,
                reply_to,
                headers,
                sent_at_secs,
                expires_at_secs,
                deduplication_id,
                ordering_key,
            )
            .await;
        });
        Ok(())
    });
    if let Err(e) = outcome {
        panic!("{e}\n{runner}");
    }
}

/// A fixed (non-proptest) case at the exact edges of the representable date range — proptest's
/// random sampling is very unlikely to ever land exactly on `Date::MIN`/`Date::MAX`, and this is
/// precisely the boundary the epoch-millis codec (contract §7 J5) needs to prove it handles
/// without panicking or losing precision.
async fn sent_at_round_trips_at_the_exact_min_and_max_representable_instant() {
    let pool = common::fresh_db().await;
    let store = PostgresOutboxStore::new(pool.clone()).await.unwrap();

    for secs in [MIN_UNIX_SECS, MAX_UNIX_SECS, 0] {
        let mut envelope = Envelope::builder(OrderCreated { order_id: 1 }).build();
        envelope.metadata.delivery.sent_at =
            Some(OffsetDateTime::from_unix_timestamp(secs).unwrap());

        let mut tx = pool.begin().await.unwrap();
        store.enqueue(&mut tx, &envelope).await.unwrap();
        tx.commit().await.unwrap();

        let batch = store
            .acquire(AcquireRequest::new(WorkerId::generate()).batch_size(1))
            .await
            .unwrap();
        assert_eq!(batch.records.len(), 1, "secs={secs}");
        assert_eq!(
            batch.records[0].envelope.metadata.delivery.sent_at, envelope.metadata.delivery.sent_at,
            "secs={secs}"
        );
    }
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        // Both proptest-generated fns build their own internal `tokio::runtime::Runtime` per
        // case (see their bodies) rather than using the shared `rt` — they were already
        // self-contained plain sync fns before RELIAR-27, since `proptest!`'s macro expansion
        // produces a zero-argument `fn`, not something `#[tokio::test]` ever touched.
        libtest_mimic::Trial::test("outbox_roundtrip::json_roundtrip", || {
            json_roundtrip();
            Ok(())
        }),
        libtest_mimic::Trial::test("outbox_roundtrip::non_json_content_type_roundtrip", || {
            non_json_content_type_roundtrip();
            Ok(())
        }),
        libtest_mimic::Trial::test(
            "outbox_roundtrip::sent_at_round_trips_at_the_exact_min_and_max_representable_instant",
            move || {
                rt.block_on(sent_at_round_trips_at_the_exact_min_and_max_representable_instant());
                Ok(())
            },
        ),
    ]
}
