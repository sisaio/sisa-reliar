//! Publisher settings, with an opt-in environment loader (SRS §7.2, ADR 0019, ADR 0029, contract
//! §4.1).
//!
//! **The library never reads the environment implicitly.** No constructor, `Default`, or builder
//! method touches [`std::env`] — only [`NatsSettings::from_env`] does, and only when called.

use std::env::VarError;
use std::time::Duration;

use reliar_core::SettingsError;

use crate::subject::{PrefixSubjects, validate_subject};

/// Publisher settings. `Default` + `const` builder methods + an **opt-in** `from_env`
/// (ADR 0019) — the library never reads the environment on its own (SRS §7.2).
///
/// There is no server URL and no credentials here **by design**: the application builds the
/// connection and the `JetStream` context and keeps ownership of both (ADR 0029).
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct NatsSettings {
    /// Prefix for the default [`PrefixSubjects`](crate::PrefixSubjects). Default `"reliar"`.
    /// Ignored when a resolver is supplied through `NatsPublisher::with_resolver` (S2).
    pub subject_prefix: String,
    /// **Upper** bound on one publish — send **and** ack together. Default 10 s. Exceeded ⇒ a
    /// transient `Timeout`. In `publish_batch` it bounds one window (ADR 0028 §3).
    ///
    /// The effective ack deadline is `min(publish_timeout, Context::timeout)`. `Context::timeout`
    /// (`async-nats` default **5 s**) is applied by `async-nats` to every `JetStream` ack await
    /// and belongs to the application, not to Reliar — so with default settings on both sides the
    /// deadline that fires is the host's, and raising this setting above it has no effect. A host
    /// that wants the full window builds its context with
    /// `async_nats::jetstream::ContextBuilder::timeout`. Whichever bound fires, the resulting
    /// `NatsPublishError::Timeout`'s `after_ms` reports the **measured** elapsed time, not this
    /// setting (ADR 0028 Amendment A; RELIAR-38 owns whether Reliar should validate or derive the
    /// pair).
    pub publish_timeout: Duration,
    /// How many publishes `publish_batch` pipelines before awaiting their acks. Default 64.
    ///
    /// **Only `publish_batch` reads this** — v0.1's `OutboxDispatcher` calls
    /// [`Publisher::publish`](reliar_core::Publisher::publish) exclusively (SRS §19.4), so this
    /// setting has no effect on the shipped wiring; it matters only to a third-party caller that
    /// invokes `publish_batch` directly. It is unrelated to
    /// `DispatcherSettings::max_in_flight` (`reliar-outbox`), which bounds the dispatcher's own
    /// concurrent publish tasks — the two are different knobs, no longer sharing a name
    /// (ADR 0028 Amendment B).
    ///
    /// Keep it **at or below the host context's `max_ack_inflight`** (`async-nats` default 5000),
    /// which caps the acks one `Context` may have outstanding. Above that cap the host's
    /// `backpressure_on_inflight` decides the failure mode: **`true`** (`async-nats`'s own
    /// default) makes each excess send **wait** for a permit that only an awaited or dropped ack
    /// releases — and a `publish_batch` window issues every send before awaiting any ack, so the
    /// window then stalls until `publish_timeout` and its whole remainder fails `Timeout`;
    /// `false` fails each excess send immediately instead, with a transient
    /// `NatsPublishError::MaxAckPending`, which the dispatcher retries. Reliar cannot validate
    /// this: `Context` exposes no getter for the cap (verified against `async-nats` 0.50), so the
    /// constraint is documented and the default 64 sits far below the default cap (ADR 0028
    /// Amendment A).
    pub batch_pipeline_depth: usize,
    /// The server's `max_payload`, when the host chooses to declare it: an oversized message is
    /// then rejected locally as a permanent [`NatsPublishError::PayloadTooLarge`]. Default `None`
    /// — Reliar does not guess a server limit (ADR 0030).
    ///
    /// **This does not add a check `async-nats` lacks.** `async-nats` already rejects a payload
    /// exceeding the connected server's advertised limit locally, before any I/O — this setting
    /// only lets the host declare a limit *below* the server's, so an oversized message is
    /// rejected — and classified — by Reliar's own [`NatsPublishError::PayloadTooLarge`] rather
    /// than surfacing however `async-nats` reports its own rejection. RELIAR-37 removes the need
    /// to declare it by deriving the effective limit from the connected server.
    ///
    /// `Some(0)` is rejected at construction as [`NatsConfigError::ZeroMaxPayload`]. A merely
    /// *small* limit is **documented, not validated**: any value below the framework header
    /// block (the `NATS/1.0` line, the four required `reliar-*` headers and the terminator — on
    /// the order of 150 bytes before a single payload byte) also dead-letters everything, but the
    /// exact floor depends on the message-type name and the metadata present, and pinning a
    /// numeric floor into this field would pin `encode`'s byte formatting into its semver
    /// contract. RELIAR-37 removes the guesswork by deriving the limit from the connected server
    /// (ADR 0030 Amendment A).
    ///
    /// [`NatsConfigError::ZeroMaxPayload`]: crate::NatsConfigError::ZeroMaxPayload
    /// [`NatsPublishError::PayloadTooLarge`]: crate::NatsPublishError::PayloadTooLarge
    pub max_payload: Option<usize>,
}

impl Default for NatsSettings {
    fn default() -> Self {
        Self {
            subject_prefix: PrefixSubjects::DEFAULT_PREFIX.to_string(),
            publish_timeout: Duration::from_secs(10),
            batch_pipeline_depth: 64,
            max_payload: None,
        }
    }
}

impl NatsSettings {
    /// Sets [`Self::subject_prefix`].
    #[must_use]
    pub fn subject_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.subject_prefix = prefix.into();
        self
    }

    /// Sets [`Self::publish_timeout`].
    #[must_use]
    pub const fn publish_timeout(mut self, publish_timeout: Duration) -> Self {
        self.publish_timeout = publish_timeout;
        self
    }

    /// Sets [`Self::batch_pipeline_depth`].
    #[must_use]
    pub const fn batch_pipeline_depth(mut self, batch_pipeline_depth: usize) -> Self {
        self.batch_pipeline_depth = batch_pipeline_depth;
        self
    }

    /// Sets [`Self::max_payload`].
    #[must_use]
    pub const fn max_payload(mut self, max_payload: Option<usize>) -> Self {
        self.max_payload = max_payload;
        self
    }

    /// Opt-in. Starts from [`Self::default`], overrides **only** the variables present under
    /// `prefix`, and returns `Err` for a present-but-unparseable or out-of-range value — never a
    /// silent fallback. Keys: `{prefix}SUBJECT_PREFIX`, `{prefix}PUBLISH_TIMEOUT_MS`,
    /// `{prefix}BATCH_PIPELINE_DEPTH`, `{prefix}MAX_PAYLOAD_BYTES`. Conventional prefix:
    /// `"RELIAR_NATS_"`. **No `URL` key exists** (ADR 0029 §1).
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Parse`] for a present variable that cannot be parsed as its
    /// declared type, or [`SettingsError::OutOfRange`] for a zero `BATCH_PIPELINE_DEPTH`/
    /// `PUBLISH_TIMEOUT_MS`/`MAX_PAYLOAD_BYTES`, or a `SUBJECT_PREFIX` that is not a legal
    /// subject prefix.
    pub fn from_env(prefix: &str) -> Result<Self, SettingsError> {
        let mut settings = Self::default();

        if let Some(v) = env_raw(prefix, "SUBJECT_PREFIX")? {
            validate_subject(&v).map_err(|_| {
                SettingsError::out_of_range(
                    format!("{prefix}SUBJECT_PREFIX"),
                    "must be a legal NATS subject prefix",
                )
            })?;
            settings.subject_prefix = v;
        }
        if let Some(v) = env_duration_ms(prefix, "PUBLISH_TIMEOUT_MS")? {
            if v.is_zero() {
                return Err(SettingsError::out_of_range(
                    format!("{prefix}PUBLISH_TIMEOUT_MS"),
                    "must be greater than zero",
                ));
            }
            settings.publish_timeout = v;
        }
        if let Some(v) = env_usize(prefix, "BATCH_PIPELINE_DEPTH")? {
            if v == 0 {
                return Err(SettingsError::out_of_range(
                    format!("{prefix}BATCH_PIPELINE_DEPTH"),
                    "must be greater than zero",
                ));
            }
            settings.batch_pipeline_depth = v;
        }
        if let Some(v) = env_usize(prefix, "MAX_PAYLOAD_BYTES")? {
            if v == 0 {
                return Err(SettingsError::out_of_range(
                    format!("{prefix}MAX_PAYLOAD_BYTES"),
                    "must be greater than zero — a zero max_payload would reject every message",
                ));
            }
            settings.max_payload = Some(v);
        }

        Ok(settings)
    }
}

/// Reads one raw environment variable under `prefix`. `Ok(None)` when absent; a present but
/// non-UTF-8 value is treated as unparseable rather than panicking or silently skipping it.
fn env_raw(prefix: &str, suffix: &str) -> Result<Option<String>, SettingsError> {
    let key = format!("{prefix}{suffix}");
    match std::env::var(&key) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(SettingsError::parse(key, "a UTF-8 string")),
    }
}

fn env_usize(prefix: &str, suffix: &str) -> Result<Option<usize>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    raw.trim()
        .parse::<usize>()
        .map(Some)
        .map_err(|_| SettingsError::parse(format!("{prefix}{suffix}"), "usize"))
}

fn env_duration_ms(prefix: &str, suffix: &str) -> Result<Option<Duration>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    let ms = raw
        .trim()
        .parse::<u64>()
        .map_err(|_| SettingsError::parse(format!("{prefix}{suffix}"), "milliseconds"))?;
    Ok(Some(Duration::from_millis(ms)))
}
