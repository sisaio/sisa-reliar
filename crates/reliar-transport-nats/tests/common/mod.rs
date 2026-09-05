//! Shared test fixtures for `reliar-transport-nats`'s public-API tests.
#![allow(dead_code)]

use bytes::Bytes;
use reliar_core::{Envelope, Message, MessageId, SerializedEnvelope};
use serde::{Deserialize, Serialize};

/// A non-panicking `&str -> HeaderName` conversion for test fixtures — this crate's own house
/// rule ("no `&str` into an `async-nats` header", contract §1) applies to test code too, since a
/// bare literal handed to `HeaderMap::insert` still goes through the panicking `IntoHeaderName`
/// conversion. `allow-expect-in-tests` (clippy.toml) only recognises `#[test]`-attributed
/// functions, so the same allowance is granted here explicitly instead.
#[allow(clippy::expect_used)]
pub(crate) fn header_name(s: &str) -> async_nats::HeaderName {
    s.parse()
        .expect("test fixture header names are always legal")
}

/// The value-side equivalent of [`header_name`]: a non-panicking `&str -> HeaderValue`
/// conversion, so a raw literal handed to `HeaderMap::insert`/`append` never goes through the
/// panicking `IntoHeaderValue for &str` conversion, even in test fixtures (review n4).
#[allow(clippy::expect_used)]
pub(crate) fn header_value(s: &str) -> async_nats::HeaderValue {
    use std::str::FromStr;
    async_nats::HeaderValue::from_str(s).expect("test fixture header values are always legal")
}

/// A minimal message body used across scenario files.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct OrderCreated {
    pub order_id: u64,
}

impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

/// Builds a [`SerializedEnvelope`] without a real serializer — every scenario in this crate only
/// inspects the header/metadata projection, never a real payload shape.
pub(crate) fn serialized_envelope() -> SerializedEnvelope {
    Envelope::builder(OrderCreated { order_id: 1 })
        .build()
        .map_body(|_| Bytes::from_static(b"{}"))
}

/// A fresh envelope with its own [`MessageId`] — scenarios that seed several distinct rows need
/// to tell them apart.
pub(crate) fn distinct_envelope() -> SerializedEnvelope {
    let mut envelope = serialized_envelope();
    envelope.id = MessageId::new();
    envelope
}

/// A `tracing` subscriber that records every span field and event into an in-memory buffer, for
/// §43.A.26: no span field, log line, `Debug`, or error `Display` on this crate's paths may
/// contain payload bytes or header values. Mirrors `reliar-outbox`'s own test helper of the same
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
    /// returned guard drops.
    pub(crate) fn install() -> (Self, tracing::subscriber::DefaultGuard) {
        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = BufferWriter(buffer.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(writer)
            .with_ansi(false)
            .with_max_level(tracing::Level::TRACE)
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
pub(crate) const CHILD_MARKER: &str = "RELIAR_NATS_TEST_CHILD";

/// Re-executes this same test binary, filtered to exactly `test_name`, with `envs` set only for
/// the child process, and returns whether the child's assertions passed. See
/// `reliar-outbox/tests/common::run_scenario_in_child` for the full rationale (mutating this
/// process's own environment needs `unsafe`, forbidden workspace-wide).
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
