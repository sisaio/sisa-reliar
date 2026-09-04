//! Lease-ownership identity (SRS §17.1, §19.2, ADR 0011).

use core::fmt;

use reliar_core::IdError;
use uuid::Uuid;

/// The lease-ownership guard key. Generated **once per dispatcher instance** — not per batch,
/// not per claim.
///
/// Every state-changing [`crate::OutboxStore`] method matches rows on `locked_by = worker`, so a
/// `WorkerId` **SHALL** be unique per running dispatcher and **SHALL NOT** be stable across
/// restarts: a restarted worker must not be able to complete or fail rows its predecessor
/// claimed.
///
/// **Never reads the environment** (ADR 0019, decision #13): [`Self::generate`] is `pid:uuid7`,
/// with no host segment — a `HOSTNAME` (or any other) lookup would be exactly the implicit env
/// read this crate forbids everywhere else. A host that wants its own hostname (or any other
/// identifying string) in the id sets one explicitly via [`Self::parse`] or
/// [`crate::OutboxSettings::from_env`]'s `WORKER_ID`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct WorkerId(String);

impl WorkerId {
    /// Maximum length in bytes.
    pub const MAX_LEN: usize = 128;

    /// Generates a fresh id: `pid:uuid7`. Touches no environment variable and never fails.
    #[must_use]
    pub fn generate() -> Self {
        let pid = std::process::id();
        let id = Uuid::now_v7();
        Self(format!("{pid}:{id}"))
    }

    /// Validates and wraps an application-supplied id. Returns `Err` for an empty string or one
    /// over [`Self::MAX_LEN`] bytes.
    ///
    /// # Errors
    ///
    /// Returns [`IdError::Empty`] or [`IdError::TooLong`].
    pub fn parse(s: impl Into<String>) -> Result<Self, IdError> {
        let s = s.into();
        if s.is_empty() {
            return Err(IdError::Empty);
        }
        if s.len() > Self::MAX_LEN {
            return Err(IdError::TooLong {
                len: s.len(),
                max: Self::MAX_LEN,
            });
        }
        Ok(Self(s))
    }

    /// Returns the worker id as a string slice.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for WorkerId {
    fn default() -> Self {
        Self::generate()
    }
}

impl fmt::Display for WorkerId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(feature = "serde")]
mod serde_impl {
    use serde::{Deserialize, Deserializer, Serialize, Serializer, de::Error as _};

    use super::WorkerId;

    impl Serialize for WorkerId {
        fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
            s.collect_str(&self.0)
        }
    }

    impl<'de> Deserialize<'de> for WorkerId {
        fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
            let raw = String::deserialize(d)?;
            WorkerId::parse(raw).map_err(D::Error::custom)
        }
    }
}
