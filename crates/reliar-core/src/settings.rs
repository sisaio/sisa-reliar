//! The shared error every `*Settings::from_env` returns (SRS §7.2, §23.1, ADR 0019, ADR 0032).

use core::fmt;

/// Why a `*Settings::from_env` call failed. `OutboxSettings::from_env` (`reliar-outbox`) was the
/// first caller; every provider's own `from_env` returns this same type (contract §7 I3).
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum SettingsError {
    /// A present variable could not be parsed as its declared type. The value is **never
    /// echoed** — it may carry an operator's typo of something sensitive.
    Parse {
        /// The full environment variable name, including the prefix.
        key: String,
        /// The type or shape that was expected, e.g. `"u32"`, `"milliseconds"`.
        value_kind: &'static str,
    },
    /// A present variable parsed but violated a documented bound.
    OutOfRange {
        /// The full environment variable name, including the prefix.
        key: String,
        /// The bound that was violated.
        message: &'static str,
    },
}

/// **Public constructors, because every provider's `from_env` returns this type** (contract §7
/// I3). `SettingsError` is `#[non_exhaustive]`, so a crate other than the one defining a given
/// `Settings` type — e.g. `reliar-store-postgres` — cannot build a variant with struct-literal
/// syntax; without these a provider is forced into a parallel, unrelated error type, and a host
/// wiring two `from_env` calls ends up handling two different errors for the same class of
/// failure (ADR 0019).
impl SettingsError {
    /// The variable was present but did not parse. `value_kind` names the expected shape
    /// (`"u32"`, `"milliseconds"`); the offending **value is never carried**.
    #[must_use]
    pub fn parse(key: impl Into<String>, value_kind: &'static str) -> Self {
        Self::Parse {
            key: key.into(),
            value_kind,
        }
    }

    /// The variable parsed but is outside the range the setting accepts.
    #[must_use]
    pub fn out_of_range(key: impl Into<String>, message: &'static str) -> Self {
        Self::OutOfRange {
            key: key.into(),
            message,
        }
    }

    /// The full environment-variable name, prefix included — what an operator has to go fix.
    #[must_use]
    pub fn key(&self) -> &str {
        match self {
            Self::Parse { key, .. } | Self::OutOfRange { key, .. } => key,
        }
    }
}

impl fmt::Display for SettingsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse { key, value_kind } => {
                write!(f, "{key} could not be parsed as {value_kind}")
            }
            Self::OutOfRange { key, message } => {
                write!(f, "{key} is out of range: {message}")
            }
        }
    }
}

impl std::error::Error for SettingsError {}
