//! Shared test fixtures for `reliar-outbox`'s public-API tests.
#![allow(dead_code)]

use std::time::Duration;

use bytes::Bytes;
use reliar_core::{ContentType, Envelope, Message, MessageId, SerializedEnvelope, Serializer};
use serde::{Deserialize, Serialize};

/// A minimal message body used across scenario files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrderCreated {
    pub order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// Three distinct message types for scenarios that need more than one `MessageType` name in the
/// same test (e.g. `enqueue_batch` order preservation).
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TypeA;
impl Message for TypeA {
    const TYPE: &'static str = "a";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TypeB;
impl Message for TypeB {
    const TYPE: &'static str = "b";
    const VERSION: u16 = 1;
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct TypeC;
impl Message for TypeC {
    const TYPE: &'static str = "c";
    const VERSION: u16 = 1;
}

/// A minimal [`Serializer`] fixture: `reliar-outbox` names no wire format of its own (ADR 0033
/// §13), so router tests bring their own rather than depending on `reliar-core`'s `json` feature.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RawJson;

impl Serializer for RawJson {
    type Error = serde_json::Error;

    fn content_type(&self) -> &ContentType {
        &ContentType::JSON
    }

    fn serialize<T: Message>(&self, body: &T) -> Result<Bytes, Self::Error> {
        serde_json::to_vec(body).map(Bytes::from)
    }

    fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A [`Serializer`] fixture whose `ContentType` is deliberately **not** JSON — proves
/// [`reliar_outbox::OutboxPublisher::enqueue`]/`publish` persist/forward
/// `metadata.delivery.content_type` **verbatim** rather than merely happening to agree with a
/// JSON default that every other fixture in this crate also produces.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct VndSerializer;

impl Serializer for VndSerializer {
    type Error = serde_json::Error;

    #[allow(clippy::expect_used, reason = "a fixture, not a #[test] body")]
    fn content_type(&self) -> &ContentType {
        static CONTENT_TYPE: std::sync::LazyLock<ContentType> = std::sync::LazyLock::new(|| {
            ContentType::parse("application/vnd.reliar-test+json").expect("valid content type")
        });
        &CONTENT_TYPE
    }

    fn serialize<T: Message>(&self, body: &T) -> Result<Bytes, Self::Error> {
        serde_json::to_vec(body).map(Bytes::from)
    }

    fn deserialize<T: Message>(&self, bytes: &[u8]) -> Result<T, Self::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Serializes `envelope` exactly the way a caller of [`reliar_outbox::OutboxPublisher`] must —
/// the three-line block contract §4.2 describes and the crate's own doctest shows: serialize the
/// body, replace it, then set `metadata.delivery.content_type` to the serializer's own type.
/// Nothing in `reliar-outbox` performs this step any more (Amendment D §3) — every routing test
/// in this crate does it itself, through this one helper.
///
/// # Errors
///
/// Whatever `serializer.serialize` returns.
pub(crate) fn serialize_with<T: Message, Ser: Serializer>(
    envelope: Envelope<T>,
    ser: &Ser,
) -> Result<SerializedEnvelope, Ser::Error> {
    let bytes = ser.serialize(&envelope.body)?;
    let mut out = envelope.map_body(|_| bytes);
    out.metadata.delivery.content_type = ser.content_type().clone();
    Ok(out)
}

/// [`serialize_with`] over [`RawJson`], for the many tests that don't care which serializer was
/// used.
#[allow(clippy::expect_used, reason = "a fixture, not a #[test] body")]
pub(crate) fn serialize(envelope: Envelope<impl Message>) -> SerializedEnvelope {
    serialize_with(envelope, &RawJson).expect("RawJson never fails")
}

/// Builds a [`SerializedEnvelope`] without a real serializer — the body bytes are irrelevant to
/// every test in this crate, which only ever inspects delivery state, not payloads.
pub(crate) fn serialized_envelope() -> SerializedEnvelope {
    Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"))
}

/// A fresh envelope with its own [`MessageId`] — dispatcher tests seed several distinct rows and
/// need to tell their outcomes apart.
pub(crate) fn distinct_envelope() -> SerializedEnvelope {
    let mut envelope = serialized_envelope();
    envelope.id = MessageId::new();
    envelope
}

/// Dispatcher settings tuned for a fast, deterministic `#[tokio::test(start_paused = true)]`:
/// short polling intervals so `tokio::time::advance` moves the loop through many iterations
/// without needing a real wall-clock wait, while every other field keeps the library default.
pub(crate) fn fast_dispatcher_settings() -> reliar_outbox::DispatcherSettings {
    reliar_outbox::DispatcherSettings::default()
        .poll_interval(Duration::from_millis(10))
        .idle_poll_interval(Duration::from_millis(10))
}

/// Advances the paused clock by `by` and then yields the executor a bounded number of times so
/// every task woken by that jump (a spawned dispatcher, its publish tasks) gets a chance to make
/// progress before the test's own assertions run. Never a wall-clock sleep — `yield_now` just
/// reschedules within the single-threaded test runtime.
pub(crate) async fn advance_and_settle(by: Duration) {
    // Give any freshly `tokio::spawn`ed task (the dispatcher) its first poll — and so its first
    // chance to read `Instant::now()` and register its timers — *before* the clock moves, so a
    // deadline computed inside `run()` is relative to t=0, not to whatever this jump already
    // advanced past.
    tokio::task::yield_now().await;
    tokio::time::advance(by).await;
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }
}

/// [`InMemoryOutboxStore`](reliar_outbox::InMemoryOutboxStore) keeps its **own** clock
/// ([`InMemoryOutboxStore::advance`]), entirely independent of Tokio's paused clock — exactly
/// like a real database's `now()` is independent of a worker's poll-timer clock. A test that
/// cares about both the dispatcher's own timers (poll cadence, lease-renewal cadence,
/// `publish_timeout`) *and* store-authoritative time (`available_at`, `locked_until`) must
/// advance both together, by the same amount, or the two drift apart. This is that single step.
#[cfg(feature = "test-support")]
pub(crate) async fn advance_both(store: &reliar_outbox::InMemoryOutboxStore, by: Duration) {
    store.advance(by);
    advance_and_settle(by).await;
}

/// A `tracing` subscriber that records every span field and event into an in-memory buffer, for
/// §43.A.26: no span field, log line, `Debug`, or error `Display` on the dispatcher's paths may
/// contain payload bytes, header values, or a connection string. [`RecordingSubscriber::text`]
/// is the whole captured transcript; the test greps it for forbidden substrings instead of
/// asserting on any one field, so it also catches a leak in a field this crate did not think to
/// name.
pub(crate) struct RecordingSubscriber {
    buffer: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
}

#[derive(Clone, Default)]
struct BufferWriter(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

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
    /// returned guard drops (mirroring [`tracing::subscriber::set_default`]'s RAII scoping).
    pub(crate) fn install() -> (Self, tracing::subscriber::DefaultGuard) {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = BufferWriter(buffer.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
            // `CLOSE` prints each span's name and recorded fields once it ends, even for a span
            // that never logs an event of its own (`reliar.outbox.enqueue`/`enqueue_batch`,
            // §43.D) — without this, a leak-free span with no event would leave nothing in the
            // transcript to assert against.
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

/// The marker `env`-var scenarios use to tell a re-executed child apart from the parent test.
pub(crate) const CHILD_MARKER: &str = "RELIAR_OUTBOX_TEST_CHILD";

/// Re-executes this same test binary, filtered to exactly `test_name`, with `envs` set only for
/// the child process, and returns whether the child's assertions passed.
///
/// `OutboxSettings::from_env` reads real process environment variables, and mutating *this*
/// process's environment safely requires `std::env::set_var` — `unsafe` since edition 2024, and
/// `unsafe_code = "forbid"` is a workspace lint applied to every target in this crate, `tests/`
/// included (verified: a local `unsafe` block here fails to compile with `-F unsafe-code`, even
/// inside a `#[test]`). Each `env`-touching scenario instead spawns a **child** copy of this
/// binary scoped to one test name, with the environment set via `Command::env` — safe, because
/// it only ever affects the child's environment, never this process's.
///
/// Returns an `Err` describing what went wrong spawning the child (never a panic) — this helper
/// is not itself a `#[test]` function, so `clippy::expect_used` still applies to it; the caller's
/// `#[test]` body is the right place to `.expect()`.
///
/// **Requires the child to report exactly one passing test**, not just a zero exit code — a
/// typo'd or stale `test_name` matches nothing under `--exact`, the harness still exits `0`, and
/// a bare `status().success()` check would pass vacuously without ever running the scenario's
/// assertions.
///
/// # Errors
///
/// Returns the underlying [`std::io::Error`] if this binary's own path cannot be resolved or the
/// child process cannot be spawned or awaited.
pub(crate) fn run_scenario_in_child(
    test_name: &str,
    envs: &[(&str, &str)],
) -> std::io::Result<bool> {
    let exe = std::env::current_exe()?;
    let mut command = std::process::Command::new(exe);
    command.arg("--exact").arg(test_name).env(CHILD_MARKER, "1");
    for (key, value) in envs {
        command.env(key, value);
    }
    let output = command.output()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    let ran_exactly_the_one_scenario = stdout.contains("1 passed; 0 failed");
    if !output.status.success() || !ran_exactly_the_one_scenario {
        eprintln!(
            "child scenario `{test_name}` did not cleanly report `1 passed; 0 failed`:\n{stdout}"
        );
    }
    Ok(output.status.success() && ran_exactly_the_one_scenario)
}

/// `true` inside the child process spawned by [`run_scenario_in_child`].
pub(crate) fn is_child() -> bool {
    std::env::var_os(CHILD_MARKER).is_some()
}

/// A test-only `OutboxStore` that ignores the caller's requested `batch_size` and always claims
/// up to `over_claim_batch_size` instead — standing in for a third-party store that does not
/// honor the request. Used to prove the dispatcher's `Semaphore` (not just the `max_in_flight`
/// claim gate) genuinely bounds concurrent `Publisher::publish` calls: with a *conforming* store
/// the gate alone never lets `outstanding` exceed `max_in_flight`, so nothing ever queues behind
/// the semaphore for a meaningfully long time. An over-delivering store can make `outstanding`
/// exceed the permit count, at which point the semaphore is the only thing left bounding
/// concurrency (architect ruling, RELIAR-15 review 3, blocker 2).
#[cfg(feature = "test-support")]
#[derive(Clone)]
pub(crate) struct OverDeliveringStore<S> {
    inner: S,
    over_claim_batch_size: u32,
}

#[cfg(feature = "test-support")]
impl<S> OverDeliveringStore<S> {
    pub(crate) fn new(inner: S, over_claim_batch_size: u32) -> Self {
        Self {
            inner,
            over_claim_batch_size,
        }
    }
}

#[cfg(feature = "test-support")]
impl<S: reliar_outbox::OutboxStore> reliar_outbox::OutboxStore for OverDeliveringStore<S> {
    type Error = S::Error;

    fn acquire(
        &self,
        request: reliar_outbox::AcquireRequest,
    ) -> impl Future<Output = Result<reliar_outbox::AcquiredBatch, Self::Error>> + Send {
        let request = request.batch_size(self.over_claim_batch_size);
        self.inner.acquire(request)
    }

    fn complete(
        &self,
        worker: &reliar_outbox::WorkerId,
        items: &[reliar_outbox::CompletedMessage],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        self.inner.complete(worker, items)
    }

    fn fail(
        &self,
        worker: &reliar_outbox::WorkerId,
        items: &[reliar_outbox::FailedMessage],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        self.inner.fail(worker, items)
    }

    fn release(
        &self,
        worker: &reliar_outbox::WorkerId,
        items: &[reliar_outbox::MessageRef],
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        self.inner.release(worker, items)
    }

    fn extend_lease(
        &self,
        worker: &reliar_outbox::WorkerId,
        items: &[reliar_outbox::MessageRef],
        lease: Duration,
    ) -> impl Future<Output = Result<u64, Self::Error>> + Send {
        self.inner.extend_lease(worker, items, lease)
    }

    fn purge(
        &self,
        request: reliar_outbox::PurgeRequest,
    ) -> impl Future<Output = Result<reliar_outbox::PurgeReport, Self::Error>> + Send {
        self.inner.purge(request)
    }

    fn stats(
        &self,
    ) -> impl Future<Output = Result<reliar_outbox::OutboxStats, Self::Error>> + Send {
        self.inner.stats()
    }
}
