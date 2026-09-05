#![allow(dead_code)]
//! Shared fixtures for the one NATS-touching test binary (ADR 0031 §4, mirrors
//! `reliar-store-postgres`'s `tests/postgres/common/mod.rs`): container/`NATS_URL` startup, a
//! per-scenario `JetStream` stream, a minimal message body, and the recording `tracing`
//! subscriber for the no-leak assertions (U13).

use std::sync::Arc;

use async_nats::HeaderMap;
use async_nats::jetstream::stream::Config as StreamConfig;
use bytes::Bytes;
use reliar_core::{Envelope, Message, MessageId, SerializedEnvelope};
use testcontainers::core::IntoContainerPort;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::OnceCell;

/// A minimal message body used across scenario files. No real serializer runs in this binary —
/// every scenario only inspects the header/payload projection on the wire, never a deserialized
/// shape. `Message` requires `Serialize + DeserializeOwned` regardless, so both are derived.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub(crate) struct OrderCreated {
    pub order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// Builds a [`SerializedEnvelope`] with a fixed, inspectable JSON body.
pub(crate) fn serialized_envelope() -> SerializedEnvelope {
    Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(br#"{"order_id":1}"#))
}

/// A fresh envelope with its own [`MessageId`] — scenarios that publish several distinct
/// envelopes need to tell them apart.
pub(crate) fn distinct_envelope() -> SerializedEnvelope {
    let mut envelope = serialized_envelope();
    envelope.id = MessageId::new();
    envelope
}

static ADMIN_URL: OnceCell<String> = OnceCell::const_new();

/// Starts the shared NATS/`JetStream` container this whole binary's scenarios use (unless
/// `NATS_URL` is set, e.g. CI's `docker run` step — ADR 0031 §3), and records its client URL for
/// every [`jetstream_context`] call. Returns the container itself — **the caller (`main`) owns
/// it as a local for the rest of the process's life and must drop it explicitly before exiting**
/// (RELIAR-27's lesson, ADR 0031 §4).
///
/// Must run to completion exactly once, before any trial touches NATS.
pub(crate) async fn start_shared_container(
    image: &str,
    tag: &str,
) -> Option<ContainerAsync<GenericImage>> {
    if let Ok(url) = std::env::var("NATS_URL") {
        ADMIN_URL
            .set(url)
            .expect("start_shared_container must run exactly once");
        return None;
    }

    let (container, url) = start_isolated_container(image, tag).await;
    ADMIN_URL
        .set(url)
        .expect("start_shared_container must run exactly once");
    Some(container)
}

/// Starts a **dedicated** `JetStream` server, independent of the shared one every other trial
/// uses — for the one scenario (N5's "server stopped mid-run") that needs to actually stop a
/// server without disturbing every other trial that may be running against the shared one in
/// parallel. Returns the container and its client URL; the caller owns the container as a local
/// and is responsible for its eventual drop.
pub(crate) async fn start_isolated_container(
    image: &str,
    tag: &str,
) -> (ContainerAsync<GenericImage>, String) {
    // No `WaitFor::message_on_stdout` here: `nats-server` prints "Server is ready" within a few
    // milliseconds of starting — often before testcontainers' log-follow subscription has
    // attached, which would then wait for a line that already scrolled past and hang for the
    // full `DEFAULT_STARTUP_TIMEOUT` (verified against this exact image in this environment).
    // [`jetstream_context_at`]'s own connect-and-retry loop is the readiness check instead — the
    // same "poll until it answers" idea CI's own startup script uses, just over `async-nats`
    // rather than an HTTP probe.
    let container = GenericImage::new(image, tag)
        .with_exposed_port(4222.tcp())
        .with_exposed_port(8222.tcp())
        .with_container_name(format!("reliar-nats-{}", uuid::Uuid::now_v7().simple()))
        .with_label("reliar.test", "true")
        .with_cmd(["-js", "-m", "8222"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("mapped client port");
    (container, format!("nats://127.0.0.1:{port}"))
}

pub(crate) fn admin_url() -> &'static str {
    ADMIN_URL
        .get()
        .expect("start_shared_container must run before any scenario touches NATS")
}

/// Connects to the shared server and returns a `JetStream` context. See
/// [`jetstream_context_at`].
pub(crate) async fn jetstream_context() -> async_nats::jetstream::Context {
    jetstream_context_at(admin_url()).await
}

/// Connects to `url` and returns a `JetStream` context, retrying the connect itself (the port may
/// not be listening yet right after container start) and then `query_account` (a `JetStream` API
/// call, answered only once `-js` has finished initialising) until both succeed or attempts are
/// exhausted — the `async-nats`-native equivalent of the `/jsz?config=1` probe CI's own startup
/// script uses (ADR 0031 §4).
pub(crate) async fn jetstream_context_at(url: &str) -> async_nats::jetstream::Context {
    let mut attempts = 0u32;
    loop {
        if let Ok(client) = async_nats::connect(url).await {
            let context = async_nats::jetstream::new(client);
            if context.query_account().await.is_ok() {
                return context;
            }
        }
        attempts += 1;
        assert!(attempts < 100, "JetStream did not become ready at {url}");
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// Connects to `url`, retrying on a transient handshake hiccup (observed under this binary's own
/// concurrent trial load against the shared server — an occasional `"expected INFO, got
/// nothing"` on an otherwise-ready server) until it succeeds or attempts are exhausted. Scenarios
/// that need a raw `Client` to build a custom `ContextBuilder` (rather than the plain
/// [`jetstream_context_at`]) use this instead of a single bare `async_nats::connect`.
pub(crate) async fn connect_retrying(url: &str) -> async_nats::Client {
    let mut attempts = 0u32;
    loop {
        if let Ok(client) = async_nats::connect(url).await {
            return client;
        }
        attempts += 1;
        assert!(attempts < 100, "could not connect to {url}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

/// A `JetStream` stream scoped to one scenario: a unique name and subject prefix
/// (`reliar.test.<uuid>.>`), so scenarios sharing a server (CI's `NATS_URL`) never collide
/// (contract §7). Deleted explicitly via [`TestStream::delete`] at the end of the owning
/// scenario — there is no `Drop`-based cleanup, because deletion is an async server call.
pub(crate) struct TestStream {
    pub(crate) context: async_nats::jetstream::Context,
    pub(crate) name: String,
    pub(crate) subject_prefix: String,
}

impl TestStream {
    /// Creates a stream over its own unique subject space with `config` applied on top of the
    /// name/subjects this helper always sets.
    pub(crate) async fn create_with(
        context: async_nats::jetstream::Context,
        config: StreamConfig,
    ) -> Self {
        let id = uuid::Uuid::now_v7().simple().to_string();
        let name = format!("RELIAR_TEST_{id}");
        let subject_prefix = format!("reliar.test.{id}");
        // Only the name is ever overridden — a caller-supplied `subjects` (e.g. a scenario
        // proving a wildcard capture, N6) must win; the default (empty) `subjects` falls back to
        // this stream's own `{subject_prefix}.>`, which is what every other caller wants.
        let subjects = if config.subjects.is_empty() {
            vec![format!("{subject_prefix}.>")]
        } else {
            config.subjects.clone()
        };
        context
            .create_stream(StreamConfig {
                name: name.clone(),
                subjects,
                ..config
            })
            .await
            .expect("create a test-scoped stream");
        Self {
            context,
            name,
            subject_prefix,
        }
    }

    /// Creates a stream with every default except name/subjects.
    pub(crate) async fn create(context: async_nats::jetstream::Context) -> Self {
        Self::create_with(context, StreamConfig::default()).await
    }

    /// A subject inside this stream's captured space: `reliar.test.<uuid>.<suffix>`.
    pub(crate) fn subject(&self, suffix: &str) -> async_nats::Subject {
        async_nats::Subject::from(format!("{}.{suffix}", self.subject_prefix))
    }

    /// The current stored-message count, fetched fresh from the server (never cached).
    pub(crate) async fn message_count(&self) -> u64 {
        let mut stream = self
            .context
            .get_stream(&self.name)
            .await
            .expect("fetch the test stream");
        stream
            .info()
            .await
            .expect("fetch stream info")
            .state
            .messages
    }

    /// The raw stored message at `sequence`, headers and payload as the server holds them.
    pub(crate) async fn raw_message(
        &self,
        sequence: u64,
    ) -> async_nats::jetstream::message::StreamMessage {
        let stream = self
            .context
            .get_stream(&self.name)
            .await
            .expect("fetch the test stream");
        stream
            .get_raw_message(sequence)
            .await
            .expect("fetch the raw stored message")
    }

    /// Deletes the stream. Best-effort: a scenario that already failed its own assertions still
    /// leaves the shared server clean.
    pub(crate) async fn delete(self) {
        let _ = self.context.delete_stream(&self.name).await;
    }
}

/// Reads a decoded header's single value as a string, panicking (test-only) if it is absent or
/// has more than one value — every header this crate writes is single-valued.
pub(crate) fn header_value(headers: &HeaderMap, name: &str) -> String {
    headers.get(name).expect("header present").to_string()
}

/// A `tracing` subscriber that records every span field and event into an in-memory buffer, for
/// §43.A.26: no span field, log line, `Debug`, or error `Display` on this crate's paths may
/// contain payload bytes, header values, or a credentialed server URL. Mirrors
/// `reliar-outbox`'s and this crate's own `tests/common::RecordingSubscriber`, duplicated here
/// per this binary's self-containment (this test binary cannot see `tests/common/mod.rs`, which
/// belongs to the crate's standard-harness test targets).
pub(crate) struct RecordingSubscriber {
    buffer: Arc<std::sync::Mutex<Vec<u8>>>,
}

#[derive(Clone, Default)]
struct BufferWriter(Arc<std::sync::Mutex<Vec<u8>>>);

impl std::io::Write for BufferWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for BufferWriter {
    type Writer = Self;

    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

impl RecordingSubscriber {
    /// Installs the recording subscriber as the default for the current thread until the
    /// returned guard drops. Span **close** events are enabled so a recorded field
    /// (`windows`, `jetstream.sequence`, `jetstream.duplicate`) reaches the transcript even when
    /// the span itself never logs an event of its own (review m6) — every field this crate
    /// records is an id, a count, or a subject, never a payload/header value/credential, so this
    /// adds no new leakage surface for the negative assertions elsewhere in this binary.
    pub(crate) fn install() -> (Self, tracing::subscriber::DefaultGuard) {
        let buffer = Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = BufferWriter(buffer.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            .with_span_events(tracing_subscriber::fmt::format::FmtSpan::CLOSE)
            .finish();
        let guard = tracing::subscriber::set_default(subscriber);
        (Self { buffer }, guard)
    }

    /// The full captured transcript so far.
    pub(crate) fn text(&self) -> String {
        let bytes = self
            .buffer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        String::from_utf8_lossy(&bytes).into_owned()
    }
}
