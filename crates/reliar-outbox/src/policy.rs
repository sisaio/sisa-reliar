//! The routing rule of SRS §20.2: outbox or direct? (ADR 0033, Amendment C).

use reliar_core::{MessageType, SettingsError};

use crate::settings::{MessageTypeNames, OutboxSettings};

/// The routing rule of SRS §20.2: given a [`MessageType`], does this message go through the
/// outbox or straight to the transport?
///
/// It is a **validated, immutable value**, not behaviour bolted onto a publisher. Built once from
/// an [`OutboxSettings`], it answers [`Self::decide`] for the rest of the process's life.
/// [`crate::OutboxPublisher`] owns one and delegates every decision to it; the publisher itself
/// holds no flag, no list and no branch of the table.
///
/// Because it needs neither a store nor a transport, a host can build one to **preview** the rule
/// at startup (log which types are durable), and the rule's tests are ordinary unit tests over a
/// pure function.
///
/// # Guarantee
///
/// A policy that exists is unambiguous: [`Self::from_settings`] rejects an overlapping
/// allow/disallow pair, so [`Self::decide`] is total and needs no tie-break.
///
/// # Examples
///
/// ```
/// # use reliar_outbox::{MessageTypeNames, OutboxPolicy, OutboxSettings, RouteKind};
/// # use reliar_core::MessageType;
/// let settings = OutboxSettings::default()
///     .disallowed_types(MessageTypeNames::parse("disallowed_types", "audit.logged")?)?;
/// let policy = OutboxPolicy::from_settings(&settings)?;
/// assert_eq!(policy.decide(&MessageType::new("orders.created", 1)), RouteKind::Outbox);
/// assert_eq!(policy.decide(&MessageType::new("audit.logged", 1)), RouteKind::Direct);
/// # Ok::<_, reliar_core::SettingsError>(())
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxPolicy {
    enabled: bool,
    allowed: MessageTypeNames,
    disallowed: MessageTypeNames,
}

/// **Hand-written, never derived**: a derived `Default` would give `enabled = false`, the
/// opposite of "everything durable" (SRS §20.2's durable default).
impl Default for OutboxPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            allowed: MessageTypeNames::empty(),
            disallowed: MessageTypeNames::empty(),
        }
    }
}

impl OutboxPolicy {
    /// Reads [`OutboxSettings::enabled`], [`OutboxSettings::allowed_types`] and
    /// [`OutboxSettings::disallowed_types`] and validates the pair. The dispatcher and retention
    /// sections are ignored — they belong to the worker, not to the rule — so a host passes the
    /// one `OutboxSettings` it already built.
    ///
    /// This is the **only** constructor, so the rule can never drift from the settings shape
    /// that documents it. `OutboxSettings::default()` is the cheap way to name a rule in a test.
    ///
    /// # Errors
    ///
    /// [`SettingsError::OutOfRange`] with `key = "disallowed_types"` and
    /// `message = "a message type may not appear in both allowed_types and disallowed_types"`
    /// when the two lists intersect. The offending name is **not** echoed. This is the backstop
    /// for the one path the setters cannot cover — a host assigning the public fields directly; a
    /// value from `default()`, the setters, `from_env` or serde always passes.
    pub fn from_settings(settings: &OutboxSettings) -> Result<Self, SettingsError> {
        check_disjoint(
            "disallowed_types",
            &settings.allowed_types,
            &settings.disallowed_types,
        )?;
        Ok(Self {
            enabled: settings.enabled,
            allowed: settings.allowed_types.clone(),
            disallowed: settings.disallowed_types.clone(),
        })
    }

    /// The routing decision for one message type — §2.1's table, evaluated in that order:
    ///
    /// ```text
    /// 1. !enabled                     -> Direct
    /// 2. disallowed_types.contains(n) -> Direct      // disallow wins
    /// 3. allowed_types.is_empty()     -> Outbox      // empty allow list = route everything
    /// 4. allowed_types.contains(n)    -> Outbox
    /// 5. otherwise                    -> Direct      // a non-empty allow list is exhaustive
    /// ```
    ///
    /// Total, infallible, allocation-free, and the single implementation of the rule.
    ///
    /// No `OutboxPolicy` is ever built from an overlapping `allowed_types`/`disallowed_types`
    /// pair ([`Self::from_settings`] refuses it), so step 2 above never actually races step 4 in
    /// practice — but the body is total regardless: were such a pair to exist, step 2 would fire
    /// first and the message would go [`RouteKind::Direct`], needing no tie-break.
    ///
    /// Matching is on [`MessageType::name`]: exact, case-sensitive, version-agnostic.
    #[must_use]
    pub fn decide(&self, message_type: &MessageType) -> RouteKind {
        let name = message_type.name();
        if !self.enabled {
            return RouteKind::Direct;
        }
        if self.disallowed.contains(name) {
            return RouteKind::Direct;
        }
        if self.allowed.is_empty() {
            return RouteKind::Outbox;
        }
        if self.allowed.contains(name) {
            return RouteKind::Outbox;
        }
        RouteKind::Direct
    }

    /// Whether routing is on — the copy of [`OutboxSettings::enabled`] this policy was built
    /// with.
    #[must_use]
    pub const fn enabled(&self) -> bool {
        self.enabled
    }

    /// The allow list this policy was built with.
    #[must_use]
    pub fn allowed_types(&self) -> &MessageTypeNames {
        &self.allowed
    }

    /// The disallow list this policy was built with.
    #[must_use]
    pub fn disallowed_types(&self) -> &MessageTypeNames {
        &self.disallowed
    }
}

/// The single implementation of the "a name may not appear in both lists" rule (ADR 0033
/// Amendment C). Called by [`OutboxSettings::allowed_types`], [`OutboxSettings::disallowed_types`],
/// [`OutboxSettings::from_env`], the `serde` `TryFrom` and [`OutboxPolicy::from_settings`] — one
/// implementation, every call site names the field it just touched (or, where there is no single
/// "just touched" field, `"disallowed_types"`).
pub(crate) fn check_disjoint(
    key: &str,
    allowed: &MessageTypeNames,
    disallowed: &MessageTypeNames,
) -> Result<(), SettingsError> {
    if allowed.names().iter().any(|name| disallowed.contains(name)) {
        return Err(SettingsError::out_of_range(
            key.to_string(),
            "a message type may not appear in both allowed_types and disallowed_types",
        ));
    }
    Ok(())
}

/// Which way a message went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RouteKind {
    /// Staged in the outbox, inside the caller's transaction.
    Outbox,
    /// Published straight to the transport, outside any transaction.
    Direct,
}

impl RouteKind {
    /// `"outbox"` / `"direct"` — the span field and metric label value.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Outbox => "outbox",
            Self::Direct => "direct",
        }
    }

    /// `true` for [`Self::Outbox`].
    #[must_use]
    pub const fn is_outbox(self) -> bool {
        matches!(self, Self::Outbox)
    }
}
