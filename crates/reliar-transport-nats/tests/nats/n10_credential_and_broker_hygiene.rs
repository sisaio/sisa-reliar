//! N10 (was U13, extended for the round-2 review, story C8, §43.A.26): no span field, `Debug`,
//! or `Display` on any `NatsPublisher` path contains a payload byte, a header value, or a
//! credentialed server URL — proven with the credentialed URL genuinely in play (the shared
//! server does not enforce auth, so a client presenting one still connects, exactly the scenario
//! §17.1 worries about) — **and** the `Broker`/unrecognised `warn` path (review m7) carries only
//! the subject and a bounded kind name, never the `async-nats` error's own `Display` (ADR 0030
//! Amendment B).

use bytes::Bytes;
use reliar_core::{Classify, Envelope, FailureKind, Publisher};
use reliar_transport_nats::{NatsPublishError, NatsPublisher, NatsSettings, PrefixSubjects};

use crate::common::{self, OrderCreated, RecordingSubscriber, TestStream};

const SECRET_HEADER_VALUE: &str = "SECRET-HEADER-VALUE-DO-NOT-LEAK";
const SECRET_BODY_MARKER: &str = "SECRET-BODY-CONTENT-DO-NOT-LEAK";
const LEAK_CREDENTIAL: &str = "leaksecret";

/// Rebuilds the shared server's URL with a fake, distinctive credential embedded — the server
/// has no auth configured, so it accepts the connection anyway (ignoring the unused
/// credentials), which is what lets this test observe whether that URL ever resurfaces.
fn credentialed_url(plain_url: &str) -> String {
    let rest = plain_url
        .strip_prefix("nats://")
        .expect("NATS URLs are always nats://host:port");
    format!("nats://leakuser:{LEAK_CREDENTIAL}@{rest}")
}

fn secret_envelope() -> reliar_core::SerializedEnvelope {
    Envelope::builder(OrderCreated { order_id: 1 })
        .header("x-secret", SECRET_HEADER_VALUE)
        .expect("a plain ASCII key/value is always accepted")
        .build()
        .map_body(|_| Bytes::from(SECRET_BODY_MARKER.as_bytes().to_vec()))
}

fn assert_no_leak(haystack: &str, label: &str) {
    assert!(
        !haystack.contains(SECRET_HEADER_VALUE),
        "{label} leaked the custom header value: {haystack}"
    );
    assert!(
        !haystack.contains(SECRET_BODY_MARKER),
        "{label} leaked the payload: {haystack}"
    );
    assert!(
        !haystack.contains(LEAK_CREDENTIAL),
        "{label} leaked the credentialed server URL: {haystack}"
    );
}

async fn a_successful_publish_never_leaks_the_payload_header_or_credentials() {
    // The shared server enforces no auth, so a client presenting a credentialed URL still
    // connects — exactly the scenario §17.1 worries about (a URL like this reaching a log or a
    // persisted error).
    let context = common::jetstream_context_at(&credentialed_url(common::admin_url())).await;
    let stream = TestStream::create(context).await;

    let (subscriber, _guard) = RecordingSubscriber::install();
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default().subject_prefix(stream.subject_prefix.clone()),
    )
    .expect("valid settings");

    publisher
        .publish(&secret_envelope())
        .await
        .expect("publish acks");

    assert_no_leak(&subscriber.text(), "a successful publish's tracing output");

    stream.delete().await;
}

async fn a_stream_not_found_failure_never_leaks_the_payload_header_or_credentials() {
    let context = common::jetstream_context_at(&credentialed_url(common::admin_url())).await;
    let prefix = format!("reliar.test.unbound.{}", uuid::Uuid::now_v7().simple());

    let (subscriber, _guard) = RecordingSubscriber::install();
    let publisher = NatsPublisher::new(context, NatsSettings::default().subject_prefix(prefix))
        .expect("valid settings");

    let err = publisher
        .publish(&secret_envelope())
        .await
        .expect_err("no stream captures this subject");

    assert_no_leak(&format!("{err}"), "StreamNotFound Display");
    assert_no_leak(&format!("{err:?}"), "StreamNotFound Debug");
    assert_no_leak(
        &subscriber.text(),
        "a StreamNotFound failure's tracing output",
    );
}

async fn a_preflight_payload_too_large_failure_never_leaks_the_payload_or_header() {
    let context = common::jetstream_context_at(&credentialed_url(common::admin_url())).await;
    let stream = TestStream::create(context).await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default()
            .subject_prefix(stream.subject_prefix.clone())
            .max_payload(Some(8)),
    )
    .expect("valid settings");

    let (subscriber, _guard) = RecordingSubscriber::install();
    let err = publisher
        .publish(&secret_envelope())
        .await
        .expect_err("the pre-flight guard rejects this locally");

    assert_no_leak(&format!("{err}"), "PayloadTooLarge Display");
    assert_no_leak(&format!("{err:?}"), "PayloadTooLarge Debug");
    assert_no_leak(
        &subscriber.text(),
        "a PayloadTooLarge failure's tracing output",
    );

    stream.delete().await;
}

/// Review M2: `NatsPublisher`'s own manual `Debug` never prints the `async_nats::jetstream::Context`
/// it wraps — proven with a credentialed URL genuinely in play, the same way the payload/header
/// scenarios above prove it for the publish path. `async-nats` 0.50's `Client`/`Context` happen not
/// to retain the connect URL in any field their own `Debug` would print (verified: its `ServerInfo`
/// carries the server's own advertised `host`, never the client's connect string or credentials),
/// so the credential/address substrings alone would not catch a regression that reintroduces
/// `self.context` into this `Debug` impl. The structural assertions below do: they fail the moment
/// `context`/`Client` internals appear at all, which is the actual invariant this type promises —
/// not merely "no credential happened to be in there today".
async fn a_credentialed_publishers_debug_never_leaks_the_address_or_credentials() {
    let context = common::jetstream_context_at(&credentialed_url(common::admin_url())).await;
    let publisher = NatsPublisher::new(context, NatsSettings::default()).expect("valid settings");

    let debug = format!("{publisher:?}");

    assert!(
        !debug.contains(LEAK_CREDENTIAL),
        "NatsPublisher::Debug leaked the credential: {debug}"
    );
    assert!(
        !debug.contains("nats://"),
        "NatsPublisher::Debug leaked a server address: {debug}"
    );
    assert!(
        !debug.contains("Context {") && !debug.contains("Client {"),
        "NatsPublisher::Debug must never print the wrapped Context/Client at all: {debug}"
    );
    assert_eq!(
        debug,
        format!(
            "NatsPublisher {{ settings: {:?}, resolver: {:?}, .. }}",
            publisher.settings(),
            PrefixSubjects::default(),
        ),
        "NatsPublisher::Debug's exact shape is settings + resolver + a non-exhaustive marker, nothing else"
    );
}

/// Review m7/N10 (ADR 0030 Amendment B): a stream that rejects a publish server-side —
/// `max_messages: 1, discard: DiscardPolicy::New` published to twice — maps to
/// `PublishErrorKind::Other` and hence `NatsPublishError::Broker`. The `warn` this crate emits
/// carries only the subject and a bounded kind name; it is compared against the *actual*
/// `async-nats` error `Display` for the very same failure (captured by publishing a third time
/// directly through the raw `Context`, bypassing `NatsPublisher`), which must **not** appear
/// anywhere in the recorded transcript or in `NatsPublishError`'s own `Display`/`Debug`.
async fn a_broker_rejection_warns_with_only_the_subject_and_kind_name() {
    use async_nats::jetstream::stream::{Config as StreamConfig, DiscardPolicy};

    let context = common::jetstream_context_at(&credentialed_url(common::admin_url())).await;
    let stream = TestStream::create_with(
        context,
        StreamConfig {
            max_messages: 1,
            discard: DiscardPolicy::New,
            ..StreamConfig::default()
        },
    )
    .await;
    let publisher = NatsPublisher::new(
        stream.context.clone(),
        NatsSettings::default().subject_prefix(stream.subject_prefix.clone()),
    )
    .expect("valid settings");

    publisher
        .publish(&common::serialized_envelope())
        .await
        .expect("the first message fits inside max_messages: 1");

    let (subscriber, _guard) = RecordingSubscriber::install();
    let err = publisher
        .publish(&common::distinct_envelope())
        .await
        .expect_err("discard: New rejects the second message instead of evicting the first");

    assert!(
        matches!(err, NatsPublishError::Broker { .. }),
        "expected Broker, got {err:?}"
    );
    assert_eq!(err.kind(), FailureKind::Transient);

    // The raw `async-nats` `Display` for the *same* server rejection, captured independently of
    // `NatsPublisher` — this is the text that must never reach the transcript or this crate's own
    // error `Display`/`Debug`.
    let raw = stream
        .context
        .publish(stream.subject("broker-raw-probe"), Bytes::new())
        .await
        .expect("send accepted")
        .await
        .expect_err("the stream rejects this one too");
    let raw_display = format!("{raw}");

    let transcript = subscriber.text();
    assert!(
        transcript.contains(stream.subject_prefix.as_str()),
        "the warn is expected to carry the subject:\n{transcript}"
    );
    assert!(
        !transcript.contains(&raw_display),
        "the warn leaked the async-nats error's own Display:\n{transcript}"
    );
    assert!(
        !format!("{err}").contains(&raw_display),
        "NatsPublishError::Display leaked the async-nats error's own Display"
    );
    assert!(
        !format!("{err:?}").contains(&raw_display),
        "NatsPublishError::Debug leaked the async-nats error's own Display"
    );

    stream.delete().await;
}

pub(crate) fn trials(rt: &'static tokio::runtime::Runtime) -> Vec<libtest_mimic::Trial> {
    vec![
        libtest_mimic::Trial::test(
            "n10_credential_and_broker_hygiene::a_successful_publish_never_leaks_the_payload_header_or_credentials",
            move || {
                rt.block_on(a_successful_publish_never_leaks_the_payload_header_or_credentials());
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n10_credential_and_broker_hygiene::a_stream_not_found_failure_never_leaks_the_payload_header_or_credentials",
            move || {
                rt.block_on(
                    a_stream_not_found_failure_never_leaks_the_payload_header_or_credentials(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n10_credential_and_broker_hygiene::a_preflight_payload_too_large_failure_never_leaks_the_payload_or_header",
            move || {
                rt.block_on(
                    a_preflight_payload_too_large_failure_never_leaks_the_payload_or_header(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n10_credential_and_broker_hygiene::a_credentialed_publishers_debug_never_leaks_the_address_or_credentials",
            move || {
                rt.block_on(
                    a_credentialed_publishers_debug_never_leaks_the_address_or_credentials(),
                );
                Ok(())
            },
        ),
        libtest_mimic::Trial::test(
            "n10_credential_and_broker_hygiene::a_broker_rejection_warns_with_only_the_subject_and_kind_name",
            move || {
                rt.block_on(a_broker_rejection_warns_with_only_the_subject_and_kind_name());
                Ok(())
            },
        ),
    ]
}
