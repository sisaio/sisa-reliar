//! `Envelope<T>` / `SerializedEnvelope` and their builder (SRS §9, §9.1, ADR 0003, ADR 0011).

use core::fmt;

use bytes::Bytes;

use crate::{
    ConversationId, CorrelationId, CorrelationMetadata, HeaderError, Headers, Message, MessageId,
    MessageType, Metadata,
};

/// An envelope: a typed or serialized body plus the metadata Reliar understands and the custom
/// headers it does not (§9). `Envelope != OutboxRecord != InboxRecord` (§17) — nothing here
/// carries delivery state (attempts, leases, dead-letter bookkeeping).
#[non_exhaustive]
pub struct Envelope<T> {
    /// The envelope's own identity.
    pub id: MessageId,
    /// The message's stable contract identity — `T::TYPE`/`T::VERSION`, never chosen ad hoc.
    pub message_type: MessageType,
    /// The message body: typed on the application side, `bytes::Bytes` once serialized.
    pub body: T,
    /// Canonical, typed framework metadata — the single source of truth (ADR 0004).
    pub metadata: Metadata,
    /// Private: preserves [`Headers`]' validation invariants — mutate only through
    /// [`Self::headers_mut`]/[`Self::set_headers`].
    pub(crate) headers: Option<Headers>,
}

/// The persistence/transport form: an envelope whose body has already been serialized to bytes.
pub type SerializedEnvelope = Envelope<Bytes>;

impl<T> Envelope<T> {
    /// The envelope's custom headers, if any were set.
    #[must_use]
    pub fn headers(&self) -> Option<&Headers> {
        self.headers.as_ref()
    }

    /// Mutably accesses the envelope's custom headers, lazily allocating an empty [`Headers`]
    /// the first time this is called.
    pub fn headers_mut(&mut self) -> &mut Headers {
        self.headers.get_or_insert_with(Headers::default)
    }

    /// Replaces the whole header map. The rehydration path for providers and transport mappers,
    /// which read back an already-validated map rather than inserting key by key.
    pub fn set_headers(&mut self, headers: Option<Headers>) {
        self.headers = headers;
    }

    /// Converts the body, keeping every other field. The only conversion between typed and
    /// serialized envelopes — no field is ever re-declared, so none can be dropped in the
    /// process (ADR 0003).
    ///
    /// ```
    /// use bytes::Bytes;
    /// use reliar_core::Envelope;
    /// # #[derive(serde::Serialize, serde::Deserialize)]
    /// # struct Ping;
    /// # impl reliar_core::Message for Ping { const TYPE: &'static str = "ping"; const VERSION: u16 = 1; }
    /// let envelope = Envelope::builder(Ping).build();
    /// let serialized: Envelope<Bytes> = envelope.map_body(|_| Bytes::from_static(b"{}"));
    /// assert_eq!(serialized.body.as_ref(), b"{}");
    /// ```
    #[must_use]
    pub fn map_body<U>(self, f: impl FnOnce(T) -> U) -> Envelope<U> {
        Envelope {
            id: self.id,
            message_type: self.message_type,
            body: f(self.body),
            metadata: self.metadata,
            headers: self.headers,
        }
    }

    /// Fallible variant of [`Self::map_body`], for `SerializedEnvelope -> Envelope<T>` via a
    /// [`Serializer`](crate::Serializer).
    ///
    /// # Errors
    ///
    /// Returns whatever error `f` returns, unchanged.
    pub fn try_map_body<U, E>(self, f: impl FnOnce(T) -> Result<U, E>) -> Result<Envelope<U>, E> {
        Ok(Envelope {
            id: self.id,
            message_type: self.message_type,
            body: f(self.body)?,
            metadata: self.metadata,
            headers: self.headers,
        })
    }
}

impl<T: Message> Envelope<T> {
    /// Starts building an envelope for `body`. `message_type` is derived from `T::TYPE`/
    /// `T::VERSION` and cannot be passed in (ADR 0010).
    ///
    /// ```
    /// use reliar_core::Envelope;
    ///
    /// #[derive(serde::Serialize, serde::Deserialize)]
    /// struct OrderCreated { order_id: u64 }
    ///
    /// impl reliar_core::Message for OrderCreated {
    ///     const TYPE: &'static str = "orders.created";
    ///     const VERSION: u16 = 1;
    /// }
    ///
    /// let envelope = Envelope::builder(OrderCreated { order_id: 42 })
    ///     .tenant("acme")
    ///     .header("x-import-batch", "2026-09-04")?
    ///     .build();
    ///
    /// assert_eq!(envelope.message_type.to_string(), "orders.created.v1");
    /// assert_eq!(envelope.metadata.tenant_id.as_deref(), Some("acme"));
    /// # Ok::<(), reliar_core::HeaderError>(())
    /// ```
    pub fn builder(body: T) -> EnvelopeBuilder<T> {
        EnvelopeBuilder::new(body)
    }
}

impl SerializedEnvelope {
    /// Rehydration entry point for providers and transport mappers, which have a `MessageType`
    /// read from storage or the wire rather than from a Rust type (ADR 0011).
    #[must_use]
    pub fn from_parts(
        id: MessageId,
        message_type: MessageType,
        body: Bytes,
        metadata: Metadata,
        headers: Option<Headers>,
    ) -> Self {
        Self {
            id,
            message_type,
            body,
            metadata,
            headers,
        }
    }
}

/// Elides the body unconditionally: a typed body may be arbitrary application data and a
/// serialized one is raw payload bytes, and neither belongs in a log line (§33, ADR 0003).
impl<T> fmt::Debug for Envelope<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Envelope")
            .field("id", &self.id)
            .field("message_type", &self.message_type)
            .field("body", &"<elided>")
            .field("metadata", &self.metadata)
            .field("headers", &self.headers)
            .finish()
    }
}

impl<T: PartialEq> PartialEq for Envelope<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
            && self.message_type == other.message_type
            && self.body == other.body
            && self.metadata == other.metadata
            && self.headers == other.headers
    }
}

/// `Clone` only where `T: Clone` — nothing in Reliar requires it (the dispatcher moves owned
/// records into publish tasks, SRS §9.1); the impl exists for tests and host code.
impl<T: Clone> Clone for Envelope<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id,
            message_type: self.message_type.clone(),
            body: self.body.clone(),
            metadata: self.metadata.clone(),
            headers: self.headers.clone(),
        }
    }
}

/// Builds an [`Envelope<T>`]. Obtained from [`Envelope::builder`].
#[must_use]
pub struct EnvelopeBuilder<T> {
    id: Option<MessageId>,
    body: T,
    metadata: Metadata,
    headers: Option<Headers>,
}

/// Elides the body: an in-progress envelope's body is arbitrary application data (§33).
impl<T> fmt::Debug for EnvelopeBuilder<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeBuilder")
            .field("id", &self.id)
            .field("body", &"<elided>")
            .field("metadata", &self.metadata)
            .field("headers", &self.headers)
            .finish()
    }
}

impl<T: Message> EnvelopeBuilder<T> {
    fn new(body: T) -> Self {
        Self {
            id: None,
            body,
            metadata: Metadata::default(),
            headers: None,
        }
    }

    /// Overrides the generated id. Defaults to a fresh `UUIDv7`.
    pub fn id(mut self, id: MessageId) -> Self {
        self.id = Some(id);
        self
    }

    /// Replaces the whole metadata struct, including its correlation metadata. Conversation
    /// rooting is decided by *value*, not by call order: if the replacement's `conversation_id`
    /// is still [`crate::ConversationId::UNSET`], [`Self::build`] roots it at the envelope's own
    /// id regardless of an earlier [`Self::conversation`] call; a non-`UNSET` value (including
    /// one copied from a causing message) is kept.
    pub fn metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = metadata;
        self
    }

    /// Replaces the correlation metadata (correlation id, conversation id, causation, request
    /// id) as a group. Same value-decides-rooting rule as [`Self::metadata`].
    pub fn correlation(mut self, correlation: CorrelationMetadata) -> Self {
        self.metadata.correlation = correlation;
        self
    }

    /// Sets the business correlation id.
    pub fn correlation_id(mut self, id: CorrelationId) -> Self {
        self.metadata.correlation.correlation_id = Some(id);
        self
    }

    /// Joins an existing conversation — typically the causing message's own `conversation_id`.
    /// [`Self::build`] keeps this value as long as nothing later replaces it with
    /// [`Self::metadata`] or [`Self::correlation`] (setter order matters only in that sense: the
    /// last write to `conversation_id` wins, same as any other field).
    pub fn conversation(mut self, id: ConversationId) -> Self {
        self.metadata.correlation.conversation_id = id;
        self
    }

    /// Records the message that caused this one.
    pub fn causation(mut self, parent: MessageId) -> Self {
        self.metadata.correlation.causation_id = Some(parent);
        self
    }

    /// Sets the owning tenant.
    pub fn tenant(mut self, tenant_id: impl Into<String>) -> Self {
        self.metadata.tenant_id = Some(tenant_id.into());
        self
    }

    /// Sets the time after which the message SHALL NOT be published (§12.2).
    pub fn expires_at(mut self, at: time::OffsetDateTime) -> Self {
        self.metadata.delivery.expires_at = Some(at);
        self
    }

    /// Sets the W3C Trace Context to carry verbatim. Reliar never invents or re-derives it
    /// (ADR 0004, ADR 0020).
    pub fn trace(mut self, traceparent: impl Into<String>, tracestate: Option<String>) -> Self {
        self.metadata.trace.traceparent = Some(traceparent.into());
        self.metadata.trace.tracestate = tracestate;
        self
    }

    /// Sets one custom header. Returns `Err` if `k` uses the reserved `reliar-` prefix or
    /// breaches a cap (see [`Headers::insert`]).
    ///
    /// # Errors
    ///
    /// Returns [`HeaderError`] under the same conditions as [`Headers::insert`].
    pub fn header(
        mut self,
        k: impl Into<String>,
        v: impl Into<String>,
    ) -> Result<Self, HeaderError> {
        self.headers
            .get_or_insert_with(Headers::default)
            .insert(k, v)?;
        Ok(self)
    }

    /// Builds the envelope. `message_type` is `MessageType::of::<T>()`. `conversation_id`:
    /// **iff** it is still [`crate::ConversationId::UNSET`], it becomes this envelope's own id
    /// (an un-correlated message roots its own conversation); any other value — set via
    /// [`Self::conversation`], [`Self::correlation`], or [`Self::metadata`] — is kept verbatim.
    /// Rooting is decided by the value alone, never by which setter was called or in what order
    /// (ADR 0011).
    #[must_use]
    pub fn build(mut self) -> Envelope<T> {
        let id = self.id.unwrap_or_default();
        if self.metadata.correlation.conversation_id.is_unset() {
            self.metadata.correlation.conversation_id = ConversationId::from_uuid(id.as_uuid());
        }
        Envelope {
            id,
            message_type: MessageType::of::<T>(),
            body: self.body,
            metadata: self.metadata,
            headers: self.headers,
        }
    }
}
