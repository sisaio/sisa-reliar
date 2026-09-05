//! `PostgresOutboxSettings`, with an opt-in environment loader (SRS §7.2, §24, ADR 0017).
//!
//! **The library never reads the environment implicitly.** No constructor, `Default` or
//! builder method touches [`std::env`] — only [`PostgresOutboxSettings::from_env`] does, and
//! only when called (ADR 0019).

use std::env::VarError;
use std::time::Duration;

use reliar_core::SettingsError;

/// What is provider-specific about the outbox (contract §4). Everything portable lives in
/// `reliar_outbox::OutboxSettings`.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(default, deny_unknown_fields))]
#[non_exhaustive]
pub struct PostgresOutboxSettings {
    /// The schema `PostgresOutboxStore::new`/`connect` verifies `outbox` resolves to, and the
    /// same default [`crate::MigrateOptions::schema`] uses. The two SHALL agree — if `migrate()`
    /// used a different schema, `outbox` is absent here and construction fails with
    /// [`crate::PostgresStoreError::NotMigrated`] or, if a same-named table exists elsewhere on
    /// the path, [`crate::PostgresStoreError::SchemaResolution`]. `SCHEMA`. Default `"reliar"`.
    pub schema: String,
    /// When `true`, `enqueue` wraps its `INSERT` in a transaction-local
    /// `set_config('search_path', …, true)` and restores the caller's previous value
    /// afterward — for hosts that can change neither the connection URL nor the role. Costs
    /// three extra statements per `enqueue`, which is why it defaults to `false`.
    /// `ENQUEUE_SETS_SEARCH_PATH`.
    pub enqueue_sets_search_path: bool,
    /// Applied as `SET LOCAL statement_timeout` inside the short transaction Reliar opens for
    /// **every** statement it issues on its own pool — `acquire`, `complete`, `fail`, `release`,
    /// `extend_lease`, `stats`, `purge` (each of its three statements), `list_dead`,
    /// `retry_dead`, `purge_dead` — **never** the caller's `enqueue` transaction, which is the
    /// caller's own to bound. `Duration::ZERO` (the default) issues nothing and inherits the
    /// server/role setting; a non-zero value costs a `BEGIN`/`SET LOCAL`/statement(s)/`COMMIT`
    /// round trip on every call. `STATEMENT_TIMEOUT_MS`.
    #[cfg_attr(
        feature = "serde",
        serde(rename = "statement_timeout_ms", with = "crate::duration_serde")
    )]
    pub statement_timeout: Duration,
}

impl Default for PostgresOutboxSettings {
    fn default() -> Self {
        Self {
            schema: "reliar".to_owned(),
            enqueue_sets_search_path: false,
            statement_timeout: Duration::ZERO,
        }
    }
}

impl PostgresOutboxSettings {
    /// Sets [`Self::schema`].
    #[must_use]
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = schema.into();
        self
    }

    /// Sets [`Self::enqueue_sets_search_path`].
    #[must_use]
    pub const fn enqueue_sets_search_path(mut self, enabled: bool) -> Self {
        self.enqueue_sets_search_path = enabled;
        self
    }

    /// Sets [`Self::statement_timeout`].
    #[must_use]
    pub const fn statement_timeout(mut self, timeout: Duration) -> Self {
        self.statement_timeout = timeout;
        self
    }

    /// Opt-in. Starts from [`Self::default`], overrides **only** the variables present under
    /// `prefix`, and returns `Err` for a present-but-unparseable or out-of-range value — never
    /// a silent fallback to the default.
    ///
    /// # Errors
    ///
    /// Returns [`SettingsError::Parse`] for a present variable that cannot be parsed as its
    /// declared type.
    pub fn from_env(prefix: &str) -> Result<Self, SettingsError> {
        let mut settings = Self::default();

        if let Some(v) = env_raw(prefix, "SCHEMA")? {
            settings.schema = v;
        }
        if let Some(v) = env_bool(prefix, "ENQUEUE_SETS_SEARCH_PATH")? {
            settings.enqueue_sets_search_path = v;
        }
        if let Some(v) = env_duration_ms(prefix, "STATEMENT_TIMEOUT_MS")? {
            settings.statement_timeout = v;
        }

        Ok(settings)
    }
}

fn env_raw(prefix: &str, suffix: &str) -> Result<Option<String>, SettingsError> {
    let key = format!("{prefix}{suffix}");
    match std::env::var(&key) {
        Ok(value) => Ok(Some(value)),
        Err(VarError::NotPresent) => Ok(None),
        Err(VarError::NotUnicode(_)) => Err(SettingsError::parse(key, "a UTF-8 string")),
    }
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
            "bool (\"true\"/\"false\"/\"1\"/\"0\")",
        )),
    }
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
