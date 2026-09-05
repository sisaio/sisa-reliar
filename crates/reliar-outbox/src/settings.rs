//! The outbox feature's settings, with an opt-in environment loader (SRS §7.2, §23.1, ADR 0019).
//!
//! **The library never reads the environment implicitly.** No constructor, `Default` or builder
//! method touches [`std::env`] — only [`OutboxSettings::from_env`] does, and only when called.

use std::env::VarError;
use std::time::Duration;

use reliar_core::SettingsError;

use crate::ordering::Ordering;
use crate::retry::ExponentialBackoff;
use crate::worker::WorkerId;

/// The one settings struct for the outbox feature. Env prefix `RELIAR_OUTBOX_` by convention;
/// [`OutboxSettings::from_env`] takes whatever prefix the caller passes.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(try_from = "OutboxSettingsRepr"))]
#[non_exhaustive]
pub struct OutboxSettings {
    /// Worker-loop tunables.
    pub dispatcher: DispatcherSettings,
    /// Purge tunables.
    pub retention: RetentionSettings,
    /// `true` (the default): the routing rule applies to messages published through
    /// [`crate::OutboxPublisher`]. `false`: **every** message publishes directly and the store is
    /// never touched — [`Self::allowed_types`] and [`Self::disallowed_types`] are both ignored.
    ///
    /// **This stops new messages entering the outbox; it never stops draining.** Rows already
    /// staged are still claimed and published by [`crate::OutboxDispatcher`], so a deployment
    /// that flips this to `false` keeps its dispatcher running until the backlog is empty. This
    /// sentence is the whole reason the field is called `enabled` and not something longer — the
    /// rustdoc carries the nuance a name cannot (ADR 0033 Amendment A).
    pub enabled: bool,
    /// The message-type names that route through the outbox. **Empty (the default) means every
    /// type is routed** — the durable default. Ignored when [`Self::enabled`] is `false`, and
    /// overridden per type by [`Self::disallowed_types`].
    pub allowed_types: MessageTypeNames,
    /// The message-type names that publish **directly** even while routing is enabled.
    /// **Disallow wins over allow**, so "everything except `c`" is an empty
    /// [`Self::allowed_types`] plus `disallowed_types = [c]` — the primary rollout shape.
    ///
    /// A name present in **both** lists is a configuration error at construction, never a silent
    /// tie-break ([`crate::OutboxPolicy::from_settings`]).
    pub disallowed_types: MessageTypeNames,
}

/// **Hand-written, never derived**: a derived `Default` would give `enabled = false` (`bool`'s
/// own default), silently disabling the durable default the whole point of `enabled` is to
/// preserve.
impl Default for OutboxSettings {
    fn default() -> Self {
        Self {
            dispatcher: DispatcherSettings::default(),
            retention: RetentionSettings::default(),
            enabled: true,
            allowed_types: MessageTypeNames::empty(),
            disallowed_types: MessageTypeNames::empty(),
        }
    }
}

impl OutboxSettings {
    /// Sets [`Self::dispatcher`].
    #[must_use]
    pub fn dispatcher(mut self, dispatcher: DispatcherSettings) -> Self {
        self.dispatcher = dispatcher;
        self
    }

    /// Sets [`Self::retention`].
    #[must_use]
    pub fn retention(mut self, retention: RetentionSettings) -> Self {
        self.retention = retention;
        self
    }

    /// Sets [`Self::enabled`].
    #[must_use]
    pub const fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Sets [`Self::allowed_types`].
    ///
    /// # Errors
    ///
    /// [`SettingsError::OutOfRange`] with `key = "allowed_types"` when `allowed` names a type
    /// that the already-configured [`Self::disallowed_types`] also names.
    pub fn allowed_types(mut self, allowed: MessageTypeNames) -> Result<Self, SettingsError> {
        crate::policy::check_disjoint("allowed_types", &allowed, &self.disallowed_types)?;
        self.allowed_types = allowed;
        Ok(self)
    }

    /// Sets [`Self::disallowed_types`].
    ///
    /// # Errors
    ///
    /// As above, with `key = "disallowed_types"`.
    pub fn disallowed_types(mut self, disallowed: MessageTypeNames) -> Result<Self, SettingsError> {
        crate::policy::check_disjoint("disallowed_types", &self.allowed_types, &disallowed)?;
        self.disallowed_types = disallowed;
        Ok(self)
    }
}

/// A validated list of message-type **names** (`"orders.created"`), never `Display` forms
/// (`"orders.created.v1"`). Order is irrelevant; duplicates are tolerated.
///
/// One type serves both [`OutboxSettings::allowed_types`] and [`OutboxSettings::disallowed_types`]:
/// the validation, the matching and the accessors are identical, and the two fields are set by two
/// separately named methods, so a distinct newtype per field would guard against an argument swap
/// that no signature makes possible (ADR 0033 Amendment B).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[cfg_attr(feature = "serde", serde(into = "Vec<String>"))]
pub struct MessageTypeNames(Vec<String>);

impl MessageTypeNames {
    /// The empty list. On [`OutboxSettings::allowed_types`] that means *every* type routes; on
    /// [`OutboxSettings::disallowed_types`] it means *no* type is excluded. The neutral name is
    /// deliberate — "all" is a property of the field, not of the list.
    #[must_use]
    pub const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Parses a comma-separated list. Entries are trimmed; empty entries are dropped, so `""`
    /// yields [`Self::empty`] and `"a,,b"` yields `[a, b]`.
    ///
    /// `field` is the field or environment-variable name reported in the error —
    /// `"allowed_types"` or `"RELIAR_OUTBOX_ALLOWED_TYPES"`. It exists so the error names the
    /// thing the operator has to edit (SRS §43.D13).
    ///
    /// # Errors
    ///
    /// [`SettingsError::Parse`] with `value_kind = "message type names without a version suffix"`
    /// for an entry ending in `.v<digits>` — that is
    /// [`MessageType`](reliar_core::MessageType)'s `Display` form, and matching is on the name
    /// alone, so accepting it would silently match nothing (ADR 0033 §5). The offending value is
    /// never echoed.
    pub fn parse(field: &str, list: &str) -> Result<Self, SettingsError> {
        let mut names = Vec::new();
        for raw in list.split(',') {
            let name = raw.trim();
            if name.is_empty() {
                continue;
            }
            names.push(name.to_string());
        }
        Self::validated(field, names)
    }

    /// Same validation, from any iterator of names. An entry that is empty after trimming is
    /// [`SettingsError::Parse`] with `value_kind = "non-empty message type names"` — unlike
    /// [`Self::parse`], which drops empties, an explicit empty name is a mistake worth reporting.
    ///
    /// # Errors
    ///
    /// As [`Self::parse`], plus the empty-name case.
    pub fn try_from_iter<I, S>(field: &str, names: I) -> Result<Self, SettingsError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        let mut out = Vec::new();
        for raw in names {
            let name = raw.as_ref().trim();
            if name.is_empty() {
                return Err(SettingsError::parse(
                    field.to_string(),
                    "non-empty message type names",
                ));
            }
            out.push(name.to_string());
        }
        Self::validated(field, out)
    }

    fn validated(field: &str, names: Vec<String>) -> Result<Self, SettingsError> {
        for name in &names {
            if is_versioned_message_type_name(name) {
                return Err(SettingsError::parse(
                    field.to_string(),
                    "message type names without a version suffix",
                ));
            }
        }
        Ok(Self(names))
    }

    /// `true` when the list holds no names.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Exact, case-sensitive. `O(n)` over a list expected to hold a handful of names; allocates
    /// nothing.
    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.0.iter().any(|n| n == name)
    }

    /// The configured names, for diagnostics.
    #[must_use]
    pub fn names(&self) -> &[String] {
        &self.0
    }
}

impl From<MessageTypeNames> for Vec<String> {
    fn from(names: MessageTypeNames) -> Self {
        names.0
    }
}

/// `true` for a name ending in a [`MessageType`](reliar_core::MessageType) `Display` version
/// suffix (`.v<digits>`) — matching happens by name alone, so accepting one would silently match
/// nothing (ADR 0033 §5).
fn is_versioned_message_type_name(name: &str) -> bool {
    match name.rsplit_once(".v") {
        Some((_, suffix)) => !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()),
        None => false,
    }
}

/// Deserialize-side shape for [`OutboxSettings`] (feature `serde`): validation is not bypassable
/// through a config file, so deserializing goes through this repr and [`OutboxSettings`]'s
/// [`TryFrom`] impl rather than a direct derive.
#[cfg(feature = "serde")]
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct OutboxSettingsRepr {
    #[serde(default)]
    dispatcher: DispatcherSettings,
    #[serde(default)]
    retention: RetentionSettings,
    #[serde(default = "default_enabled_true")]
    enabled: bool,
    #[serde(default)]
    allowed_types: Vec<String>,
    #[serde(default)]
    disallowed_types: Vec<String>,
}

#[cfg(feature = "serde")]
const fn default_enabled_true() -> bool {
    true
}

#[cfg(feature = "serde")]
impl TryFrom<OutboxSettingsRepr> for OutboxSettings {
    type Error = SettingsError;

    fn try_from(repr: OutboxSettingsRepr) -> Result<Self, Self::Error> {
        let allowed_types = MessageTypeNames::try_from_iter("allowed_types", repr.allowed_types)?;
        let disallowed_types =
            MessageTypeNames::try_from_iter("disallowed_types", repr.disallowed_types)?;
        crate::policy::check_disjoint("disallowed_types", &allowed_types, &disallowed_types)?;
        Ok(Self {
            dispatcher: repr.dispatcher,
            retention: repr.retention,
            enabled: repr.enabled,
            allowed_types,
            disallowed_types,
        })
    }
}

/// Worker-loop tunables — the struct the §23.1 defaults table and the §26.1 drain rule refer
/// to.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[non_exhaustive]
pub struct DispatcherSettings {
    /// The maximum number of rows one `acquire` **statement** claims. `BATCH_SIZE`. Default
    /// 100.
    ///
    /// **`max_in_flight` is the real ceiling on rows this worker holds leased at once —
    /// `batch_size` only caps a single claim.** The dispatcher claims only while
    /// `outstanding < max_in_flight`, asking for `min(batch_size, max_in_flight - outstanding)`
    /// (S4 review 2); with the accepted defaults (100 / 16) the claim never asks for more than
    /// 16, so `batch_size` does not bind. Raise it only alongside `max_in_flight` if a single
    /// worker is meant to hold more rows leased at once.
    pub batch_size: u32,
    /// How long a claim holds the lease before it may be reclaimed. `LEASE_MS`. Default 30 s.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "lease_ms", with = "crate::duration_serde::millis")
    )]
    pub lease: Duration,
    /// The maximum number of publishes running concurrently. `MAX_IN_FLIGHT`. Default 16.
    pub max_in_flight: usize,
    /// How long one publish is allowed to run before it counts as a timeout (classified
    /// [`crate::FailureKind::Transient`]). `PUBLISH_TIMEOUT_MS`. Default 10 s.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "publish_timeout_ms", with = "crate::duration_serde::millis")
    )]
    pub publish_timeout: Duration,
    /// How often the loop polls for work when the previous claim was non-empty. `POLL_INTERVAL_MS`.
    /// Default 500 ms. **Must be greater than zero**
    /// ([`crate::ConfigError::ZeroPollInterval`], S4 review 7) — it also seeds the outcome-write
    /// retry pacing (`outcome_retry_interval`), so a zero value would re-enable a CPU-speed spin
    /// on both paths.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "poll_interval_ms", with = "crate::duration_serde::millis")
    )]
    pub poll_interval: Duration,
    /// How often the loop polls once it has seen an empty claim. `IDLE_POLL_INTERVAL_MS`.
    /// Default 5 s. **Must be greater than zero**
    /// ([`crate::ConfigError::ZeroPollInterval`], S4 review 7) — a zero value would poll an idle
    /// store at CPU speed instead of backing off.
    #[cfg_attr(
        feature = "serde",
        serde(
            rename = "idle_poll_interval_ms",
            with = "crate::duration_serde::millis"
        )
    )]
    pub idle_poll_interval: Duration,
    /// The maximum time `run()` spends draining in-flight publishes after cancellation (§26.1).
    /// `DRAIN_TIMEOUT_MS`. Default 30 s.
    ///
    /// With the defaults, worst-case shutdown is roughly **`drain_timeout + store_timeout`**,
    /// not just `drain_timeout`: the drain loop itself is bounded by `drain_timeout`, and the one
    /// best-effort outcome-write attempt made right after it is separately bounded by
    /// `store_timeout` — the two budgets are not nested, they are sequential (S4 review 3,
    /// minor).
    #[cfg_attr(
        feature = "serde",
        serde(rename = "drain_timeout_ms", with = "crate::duration_serde::millis")
    )]
    pub drain_timeout: Duration,
    /// A client-side bound on **every** `OutboxStore` call `run` makes — without it a hung
    /// statement (a lost connection with no server-side `statement_timeout`, a saturated pool)
    /// makes `drain_timeout` unenforceable (S4 review). A timeout is treated as a transient
    /// store error.
    ///
    /// **Must be shorter than half the lease** (`store_timeout < lease / 2`,
    /// [`crate::ConfigError::StoreTimeoutTooLong`], S4 review 4): `run`'s outcome-write retry
    /// races the lease-renewal tick inside the same `select!`, so a `store_timeout` any longer
    /// could let one hung `complete`/`fail` attempt occupy an entire tick gap and starve
    /// renewal. `STORE_TIMEOUT_MS`. Default 10 s (comfortably under half the default 30 s
    /// lease's 15 s).
    #[cfg_attr(
        feature = "serde",
        serde(rename = "store_timeout_ms", with = "crate::duration_serde::millis")
    )]
    pub store_timeout: Duration,
    /// How often `stats()` is polled for the lag/dead-count gauges. `STATS_INTERVAL_MS`.
    /// Default 15 s.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "stats_interval_ms", with = "crate::duration_serde::millis")
    )]
    pub stats_interval: Duration,
    /// The publication ordering strategy. `ORDERING`. Default [`Ordering::Unordered`].
    pub ordering: Ordering,
    /// The retry/backoff policy. `RETRY_BASE_MS`, `RETRY_MAX_DELAY_MS`,
    /// `RETRY_MAX_ATTEMPTS`, `RETRY_JITTER`.
    pub retry: ExponentialBackoff,
    /// Overrides the generated [`WorkerId`]. `WORKER_ID`. Default: generated.
    pub worker_id: Option<WorkerId>,
}

impl Default for DispatcherSettings {
    fn default() -> Self {
        Self {
            batch_size: 100,
            lease: Duration::from_secs(30),
            max_in_flight: 16,
            publish_timeout: Duration::from_secs(10),
            poll_interval: Duration::from_millis(500),
            idle_poll_interval: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(30),
            store_timeout: Duration::from_secs(10),
            stats_interval: Duration::from_secs(15),
            ordering: Ordering::default(),
            retry: ExponentialBackoff::default(),
            worker_id: None,
        }
    }
}

impl DispatcherSettings {
    /// Sets [`Self::batch_size`].
    #[must_use]
    pub const fn batch_size(mut self, batch_size: u32) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Sets [`Self::lease`].
    #[must_use]
    pub const fn lease(mut self, lease: Duration) -> Self {
        self.lease = lease;
        self
    }

    /// Sets [`Self::max_in_flight`].
    #[must_use]
    pub const fn max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = max_in_flight;
        self
    }

    /// Sets [`Self::publish_timeout`].
    #[must_use]
    pub const fn publish_timeout(mut self, publish_timeout: Duration) -> Self {
        self.publish_timeout = publish_timeout;
        self
    }

    /// Sets [`Self::poll_interval`].
    #[must_use]
    pub const fn poll_interval(mut self, poll_interval: Duration) -> Self {
        self.poll_interval = poll_interval;
        self
    }

    /// Sets [`Self::idle_poll_interval`].
    #[must_use]
    pub const fn idle_poll_interval(mut self, idle_poll_interval: Duration) -> Self {
        self.idle_poll_interval = idle_poll_interval;
        self
    }

    /// Sets [`Self::drain_timeout`].
    #[must_use]
    pub const fn drain_timeout(mut self, drain_timeout: Duration) -> Self {
        self.drain_timeout = drain_timeout;
        self
    }

    /// Sets [`Self::store_timeout`].
    #[must_use]
    pub const fn store_timeout(mut self, store_timeout: Duration) -> Self {
        self.store_timeout = store_timeout;
        self
    }

    /// Sets [`Self::stats_interval`].
    #[must_use]
    pub const fn stats_interval(mut self, stats_interval: Duration) -> Self {
        self.stats_interval = stats_interval;
        self
    }

    /// Sets [`Self::ordering`].
    #[must_use]
    pub const fn ordering(mut self, ordering: Ordering) -> Self {
        self.ordering = ordering;
        self
    }

    /// Sets [`Self::retry`].
    #[must_use]
    pub const fn retry(mut self, retry: ExponentialBackoff) -> Self {
        self.retry = retry;
        self
    }

    /// Sets [`Self::worker_id`].
    #[must_use]
    pub fn worker_id(mut self, worker_id: WorkerId) -> Self {
        self.worker_id = Some(worker_id);
        self
    }
}

/// Purge tunables.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[non_exhaustive]
pub struct RetentionSettings {
    /// How long a published row is kept before `purge` deletes it. `PUBLISHED_RETENTION_MS`.
    /// Default 7 days.
    #[cfg_attr(
        feature = "serde",
        serde(
            rename = "published_retention_ms",
            with = "crate::duration_serde::millis"
        )
    )]
    pub published_retention: Duration,
    /// How long a dead row is kept before `purge` deletes it. `None` keeps dead rows until an
    /// explicit purge. `DEAD_RETENTION_MS`. Default `None`.
    #[cfg_attr(
        feature = "serde",
        serde(
            rename = "dead_retention_ms",
            with = "crate::duration_serde::optional_millis"
        )
    )]
    pub dead_retention: Option<Duration>,
    /// The maximum number of rows one purge pass deletes, per pass. `PURGE_BATCH_SIZE`.
    /// Default 1000.
    pub purge_batch_size: u32,
}

impl Default for RetentionSettings {
    fn default() -> Self {
        Self {
            published_retention: Duration::from_secs(7 * 24 * 60 * 60),
            dead_retention: None,
            purge_batch_size: 1_000,
        }
    }
}

impl RetentionSettings {
    /// Sets [`Self::published_retention`].
    #[must_use]
    pub const fn published_retention(mut self, retention: Duration) -> Self {
        self.published_retention = retention;
        self
    }

    /// Sets [`Self::dead_retention`].
    #[must_use]
    pub const fn dead_retention(mut self, retention: Option<Duration>) -> Self {
        self.dead_retention = retention;
        self
    }

    /// Sets [`Self::purge_batch_size`].
    #[must_use]
    pub const fn purge_batch_size(mut self, purge_batch_size: u32) -> Self {
        self.purge_batch_size = purge_batch_size;
        self
    }
}

impl OutboxSettings {
    /// Opt-in. Starts from [`Self::default`], overrides **only** the variables present under
    /// `prefix`, and returns `Err` for a present-but-unparseable or out-of-range value — never
    /// a silent fallback to the default. Env variable names are flat under `prefix` (e.g.
    /// `{prefix}LEASE_MS`), regardless of which nested settings struct they populate.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Parse`] for a present variable that cannot be parsed as its
    /// declared type (a bad UTF-8 value counts as unparseable), or
    /// [`SettingsError::OutOfRange`] for one that parses but violates a documented bound (a
    /// `RETRY_JITTER` outside `[0.0, 1.0)`, a `WORKER_ID` over its maximum length).
    pub fn from_env(prefix: &str) -> Result<Self, SettingsError> {
        let mut dispatcher = DispatcherSettings::default();
        let mut retention = RetentionSettings::default();
        let mut enabled = true;
        let mut allowed_types = MessageTypeNames::empty();
        let mut disallowed_types = MessageTypeNames::empty();

        if let Some(v) = env_u32(prefix, "BATCH_SIZE")? {
            dispatcher.batch_size = v;
        }
        if let Some(v) = env_duration_ms(prefix, "LEASE_MS")? {
            dispatcher.lease = v;
        }
        if let Some(v) = env_usize(prefix, "MAX_IN_FLIGHT")? {
            dispatcher.max_in_flight = v;
        }
        if let Some(v) = env_duration_ms(prefix, "PUBLISH_TIMEOUT_MS")? {
            dispatcher.publish_timeout = v;
        }
        if let Some(v) = env_duration_ms(prefix, "POLL_INTERVAL_MS")? {
            dispatcher.poll_interval = v;
        }
        if let Some(v) = env_duration_ms(prefix, "IDLE_POLL_INTERVAL_MS")? {
            dispatcher.idle_poll_interval = v;
        }
        if let Some(v) = env_duration_ms(prefix, "DRAIN_TIMEOUT_MS")? {
            dispatcher.drain_timeout = v;
        }
        if let Some(v) = env_duration_ms(prefix, "STORE_TIMEOUT_MS")? {
            dispatcher.store_timeout = v;
        }
        if let Some(v) = env_duration_ms(prefix, "STATS_INTERVAL_MS")? {
            dispatcher.stats_interval = v;
        }
        if let Some(v) = env_ordering(prefix, "ORDERING")? {
            dispatcher.ordering = v;
        }
        if let Some(v) = env_duration_ms(prefix, "RETRY_BASE_MS")? {
            dispatcher.retry.base = v;
        }
        if let Some(v) = env_duration_ms(prefix, "RETRY_MAX_DELAY_MS")? {
            dispatcher.retry.max_delay = v;
        }
        if let Some(v) = env_u32(prefix, "RETRY_MAX_ATTEMPTS")? {
            dispatcher.retry.max_attempts = v;
        }
        if let Some(v) = env_jitter(prefix, "RETRY_JITTER")? {
            dispatcher.retry.jitter = v;
        }
        if let Some(v) = env_worker_id(prefix, "WORKER_ID")? {
            dispatcher.worker_id = Some(v);
        }

        if let Some(v) = env_duration_ms(prefix, "PUBLISHED_RETENTION_MS")? {
            retention.published_retention = v;
        }
        if let Some(v) = env_duration_ms(prefix, "DEAD_RETENTION_MS")? {
            retention.dead_retention = Some(v);
        }
        if let Some(v) = env_u32(prefix, "PURGE_BATCH_SIZE")? {
            retention.purge_batch_size = v;
        }

        if let Some(v) = env_bool(prefix, "ENABLED")? {
            enabled = v;
        }
        if let Some(raw) = env_raw(prefix, "ALLOWED_TYPES")? {
            allowed_types = MessageTypeNames::parse(&format!("{prefix}ALLOWED_TYPES"), &raw)?;
        }
        if let Some(raw) = env_raw(prefix, "DISALLOWED_TYPES")? {
            disallowed_types = MessageTypeNames::parse(&format!("{prefix}DISALLOWED_TYPES"), &raw)?;
        }
        crate::policy::check_disjoint(
            &format!("{prefix}DISALLOWED_TYPES"),
            &allowed_types,
            &disallowed_types,
        )?;

        Ok(Self {
            dispatcher,
            retention,
            enabled,
            allowed_types,
            disallowed_types,
        })
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

fn env_u32(prefix: &str, suffix: &str) -> Result<Option<u32>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    raw.trim()
        .parse::<u32>()
        .map(Some)
        .map_err(|_| SettingsError::parse(format!("{prefix}{suffix}"), "u32"))
}

fn env_bool(prefix: &str, suffix: &str) -> Result<Option<bool>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "1" => Ok(Some(true)),
        "false" | "0" => Ok(Some(false)),
        _ => Err(SettingsError::parse(
            format!("{prefix}{suffix}"),
            "a boolean (\"true\" or \"false\")",
        )),
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

fn env_jitter(prefix: &str, suffix: &str) -> Result<Option<f64>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    let key = format!("{prefix}{suffix}");
    let value = raw
        .trim()
        .parse::<f64>()
        .map_err(|_| SettingsError::parse(key.clone(), "f64"))?;
    if !(0.0..1.0).contains(&value) {
        return Err(SettingsError::out_of_range(
            key,
            "jitter must be in the range [0.0, 1.0)",
        ));
    }
    Ok(Some(value))
}

fn env_ordering(prefix: &str, suffix: &str) -> Result<Option<Ordering>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    match raw.trim().to_ascii_lowercase().as_str() {
        "unordered" => Ok(Some(Ordering::Unordered)),
        "per_key" | "perkey" | "per-key" => Ok(Some(Ordering::PerKey)),
        _ => Err(SettingsError::parse(
            format!("{prefix}{suffix}"),
            "ordering (\"unordered\" or \"per_key\")",
        )),
    }
}

fn env_worker_id(prefix: &str, suffix: &str) -> Result<Option<WorkerId>, SettingsError> {
    let Some(raw) = env_raw(prefix, suffix)? else {
        return Ok(None);
    };
    let key = format!("{prefix}{suffix}");
    WorkerId::parse(raw).map(Some).map_err(|err| match err {
        reliar_core::IdError::TooLong { .. } => {
            SettingsError::out_of_range(key, "worker id exceeds the maximum length")
        }
        _ => SettingsError::parse(key, "worker id"),
    })
}
