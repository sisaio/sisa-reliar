//! Stable message contract identity (SRS §10, §10.1, ADR 0010).

use core::fmt;
use std::borrow::Cow;

/// A type that can be built into an [`Envelope`](crate::Envelope) and persisted or published.
///
/// `TYPE`/`VERSION` are stable **application contracts**: they identify a message across
/// serialization, storage and the wire, and are never derived from
/// `std::any::type_name::<T>()` or a module path — renaming or moving the Rust type SHALL NOT
/// orphan a pending row or a message already in flight (ADR 0010).
pub trait Message: serde::Serialize + serde::de::DeserializeOwned {
    /// The message's name, e.g. `"orders.created"`. Stable once anything has published it.
    const TYPE: &'static str;
    /// The message's version. Bump when the wire shape changes incompatibly.
    const VERSION: u16;
}

/// A message's name and version, carried separately so a query can filter a name across every
/// version (§24). Renders as `"{name}.v{version}"` via its [`Display`](fmt::Display) impl.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageType {
    name: Cow<'static, str>,
    version: u16,
}

impl MessageType {
    /// Builds a `MessageType` from a `'static` name and a version.
    #[must_use]
    pub const fn new(name: &'static str, version: u16) -> Self {
        Self {
            name: Cow::Borrowed(name),
            version,
        }
    }

    /// Rehydration path: a provider reads `message_type`/`message_version` columns back into a
    /// `MessageType` for which it has no Rust type.
    pub fn from_parts(name: impl Into<Cow<'static, str>>, version: u16) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }

    /// Builds the `MessageType` a `T: Message` declares: `T::TYPE` + `T::VERSION`. Never derived
    /// from `std::any::type_name::<T>()`.
    #[must_use]
    pub fn of<T: Message>() -> Self {
        Self::new(T::TYPE, T::VERSION)
    }

    /// The message name, e.g. `"orders.created"`.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The message version.
    #[must_use]
    pub const fn version(&self) -> u16 {
        self.version
    }
}

/// Renders `"{name}.v{version}"`, e.g. `orders.created.v1`. **A stable public contract**:
/// clients parse this string. Two distinct Rust types sharing `TYPE`/`VERSION` render
/// identically — that is intended, not a bug (ADR 0010, §43.A.3).
impl fmt::Display for MessageType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.v{}", self.name, self.version)
    }
}
