//! Publication ordering strategy (SRS §22.2, ADR 0013).

use crate::error::ConfigError;

/// A **configured strategy**, not a fixed property of the system. The default guarantees
/// nothing about order.
///
/// Set on `OutboxDispatcherBuilder` (S4) and passed to the store in
/// [`crate::AcquireRequest`], because the guarantee needs both the claim query and the publish
/// loop — neither can offer it alone.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
#[non_exhaustive]
pub enum Ordering {
    /// Default. Maximum throughput; **no ordering guarantee of any kind** — not globally, not
    /// per `conversation_id`, not per aggregate, not approximately. `SKIP LOCKED`, concurrent
    /// publishing, per-message backoff and multiple workers each reorder freely; a retried
    /// message can arrive after messages enqueued minutes later (ADR 0013).
    #[default]
    Unordered,
    /// At most one in-flight message per `ordering_key`, FIFO within a key. **Not implemented
    /// until 0.2** — selecting it in v0.1 is a configuration error (see [`Self::validate`]).
    PerKey,
}

impl Ordering {
    /// Rejects [`Ordering::PerKey`], which is not implemented until 0.2 (ADR 0013). The v0.1
    /// dispatcher builder calls this before construction; exposed here so the rejection is
    /// independently testable without building a dispatcher.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::UnsupportedOrdering`] for [`Ordering::PerKey`].
    pub const fn validate(self) -> Result<(), ConfigError> {
        match self {
            Self::Unordered => Ok(()),
            Self::PerKey => Err(ConfigError::UnsupportedOrdering {
                ordering: self,
                available_in: "0.2",
            }),
        }
    }
}
