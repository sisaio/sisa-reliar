//! Envelope ↔ NATS message mapping (SRS §15–§16, ADR 0026, contract §2).

use core::fmt;
use std::str::FromStr;

use bytes::Bytes;
use reliar_core::{
    ContentType, ConversationId, CorrelationId, EndpointAddress, EnvelopeMapper, Headers,
    MessageId, MessageType, Metadata, RequestId, SerializedEnvelope,
};
use uuid::Uuid;

use crate::error::NatsMapError;
use crate::headers as h;

/// The NATS wire form of an envelope: the header block and the payload, and nothing else.
///
/// It deliberately carries **no subject and no reply subject**. Those are routing, owned by
/// [`crate::SubjectResolver`] (ADR 0027) and by the subscription on the receiving side — keeping
/// it out is what stops a NATS routing concept from becoming part of the envelope mapping
/// (SRS §12).
#[derive(Clone)]
#[non_exhaustive]
pub struct NatsWireMessage {
    /// The projected headers (ADR 0026 §1). Never contains a payload byte.
    pub headers: async_nats::HeaderMap,
    /// The serialized envelope body, byte for byte — never re-wrapped (SRS §16).
    pub payload: Bytes,
}

impl NatsWireMessage {
    /// Builds a wire message from an already-projected header block and payload.
    #[must_use]
    pub fn new(headers: async_nats::HeaderMap, payload: Bytes) -> Self {
        Self { headers, payload }
    }

    /// Consumes it into `(headers, payload)` — what `Context::publish_with_headers` wants.
    #[must_use]
    pub fn into_parts(self) -> (async_nats::HeaderMap, Bytes) {
        (self.headers, self.payload)
    }

    /// The number of bytes the server counts against `max_payload`: when there is at least one
    /// header, the NATS/1.0 header block (`"NATS/1.0\r\n"`, then `"{name}: {value}\r\n"` per
    /// header, then `"\r\n"`) plus the payload; with **no** headers, the payload length alone —
    /// mirroring `async-nats`'s own `Client::check_payload_size`, which only adds the header
    /// block's byte count when the header map is non-empty. Used by the publisher's pre-flight
    /// guard (S2, contract §4.2).
    #[must_use]
    pub fn wire_len(&self) -> usize {
        if self.headers.is_empty() {
            return self.payload.len();
        }
        let mut len = "NATS/1.0\r\n".len() + "\r\n".len();
        for (name, values) in self.headers.iter() {
            let name: &str = name.as_ref();
            for value in values {
                len += name.len() + ": ".len() + value.as_str().len() + "\r\n".len();
            }
        }
        len + self.payload.len()
    }
}

/// Elides every header value and the payload bytes: header names and the payload length only
/// (Phase-1 preamble; §17.1; SRS §33).
impl fmt::Debug for NatsWireMessage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let names: Vec<&str> = self.headers.iter().map(|(name, _)| name.as_ref()).collect();
        f.debug_struct("NatsWireMessage")
            .field("header_names", &names)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Drops `subject`, `reply` and `status`: broker routing and protocol state, not envelope data.
/// Phase 3's consumer converts here and reads routing from its subscription (ADR 0026 §4).
impl From<async_nats::Message> for NatsWireMessage {
    fn from(message: async_nats::Message) -> Self {
        Self {
            headers: message.headers.unwrap_or_default(),
            payload: message.payload,
        }
    }
}

/// Projects the canonical envelope onto NATS headers + a raw payload, and back (SRS §15–§16).
///
/// Stateless, `Copy`, and cheap: one per publisher, or one per call — it makes no difference.
#[derive(Clone, Copy, Debug, Default)]
#[non_exhaustive]
pub struct NatsEnvelopeMapper;

impl EnvelopeMapper<NatsWireMessage> for NatsEnvelopeMapper {
    type Error = NatsMapError;

    fn encode(&self, envelope: &SerializedEnvelope) -> Result<NatsWireMessage, NatsMapError> {
        let mut headers = async_nats::HeaderMap::new();

        // Custom headers first, framework headers second, so a framework value overrides a
        // colliding custom one (ADR 0026 §2). `reliar-*` cannot occur here: core rejects it at
        // `Headers::insert`. The one collision core does *not* reserve — a custom key naming an
        // unprefixed W3C trace header — is arbitrated case-insensitively right here so `encode`
        // never emits two casings of one framework name (ADR 0026 Amendment A).
        if let Some(custom) = envelope.headers() {
            let mut traceparent_from_custom = false;
            let mut tracestate_from_custom = false;

            for (key, value) in custom.iter() {
                if starts_with_ci(key, h::NATS_PREFIX) {
                    return Err(NatsMapError::ReservedHeaderName {
                        key: key.to_string(),
                    });
                }

                if let Some(canonical) = trace_override_name(key) {
                    let framework_value_set = if canonical == h::TRACEPARENT {
                        envelope.metadata.trace.traceparent.is_some()
                    } else {
                        envelope.metadata.trace.tracestate.is_some()
                    };
                    if framework_value_set {
                        // Dropped: the framework value overrides and is written below (SRS §14).
                        continue;
                    }
                    let written = if canonical == h::TRACEPARENT {
                        &mut traceparent_from_custom
                    } else {
                        &mut tracestate_from_custom
                    };
                    if *written {
                        // A second spelling with no framework value to arbitrate: `Headers` is a
                        // map, so picking one would make the wire depend on hash order.
                        return Err(NatsMapError::DuplicateHeader { header: canonical });
                    }
                    headers.insert(
                        async_nats::HeaderName::from_static(canonical),
                        header_value(canonical, value)?,
                    );
                    *written = true;
                    continue;
                }

                let name = async_nats::HeaderName::from_str(key).map_err(|_| {
                    NatsMapError::UnsupportedHeaderName {
                        key: key.to_string(),
                    }
                })?;
                // Core's `Headers::insert` already rejects every control character (a superset
                // of the '\r'/'\n' check `HeaderValue::from_str` performs), so this cannot fail
                // in practice; handled without a panic regardless, naming the header rather than
                // guessing at the key's legality (ADR 0026 Amendment B).
                let value = async_nats::HeaderValue::from_str(value).map_err(|_| {
                    NatsMapError::InvalidHeaderValue {
                        header: key.to_string(),
                    }
                })?;
                headers.insert(name, value);
            }
        }

        insert_framework_headers(&mut headers, envelope)?;

        Ok(NatsWireMessage::new(headers, envelope.body.clone()))
    }

    fn decode(&self, message: NatsWireMessage) -> Result<SerializedEnvelope, NatsMapError> {
        ScannedHeaders::scan(&message.headers)?.into_envelope(message.payload)
    }
}

/// Returns the canonical lowercase framework name (`traceparent`/`tracestate`) when `key`
/// case-insensitively names one of §14's unprefixed W3C trace headers — the one collision core's
/// own reserved-prefix check does not catch, and the one `encode` must arbitrate itself
/// (ADR 0026 §2, Amendment A).
fn trace_override_name(key: &str) -> Option<&'static str> {
    if key.eq_ignore_ascii_case(h::TRACEPARENT) {
        Some(h::TRACEPARENT)
    } else if key.eq_ignore_ascii_case(h::TRACESTATE) {
        Some(h::TRACESTATE)
    } else {
        None
    }
}

/// Writes headers 1–9 of the projection table (contract §2.3) directly, then delegates headers
/// 10–17 (timestamps, routing, W3C trace context, the dedup key) to
/// [`insert_routing_trace_and_dedup_headers`]. A framework `insert` always wins over anything a
/// colliding custom header already wrote for a name core does not reserve
/// (`traceparent`/`tracestate`) — the override [`EnvelopeMapper::encode`] arbitrates before
/// calling here.
fn insert_framework_headers(
    headers: &mut async_nats::HeaderMap,
    envelope: &SerializedEnvelope,
) -> Result<(), NatsMapError> {
    headers.insert(
        async_nats::HeaderName::from_static(h::MESSAGE_ID),
        header_value(h::MESSAGE_ID, &envelope.id.to_string())?,
    );
    headers.insert(
        async_nats::HeaderName::from_static(h::MESSAGE_TYPE),
        header_value(h::MESSAGE_TYPE, envelope.message_type.name())?,
    );
    headers.insert(
        async_nats::HeaderName::from_static(h::MESSAGE_VERSION),
        header_value(
            h::MESSAGE_VERSION,
            &envelope.message_type.version().to_string(),
        )?,
    );
    headers.insert(
        async_nats::HeaderName::from_static(h::CONTENT_TYPE),
        header_value(
            h::CONTENT_TYPE,
            envelope.metadata.delivery.content_type.as_str(),
        )?,
    );
    if let Some(correlation_id) = &envelope.metadata.correlation.correlation_id {
        headers.insert(
            async_nats::HeaderName::from_static(h::CORRELATION_ID),
            header_value(h::CORRELATION_ID, correlation_id.as_str())?,
        );
    }
    if !envelope.metadata.correlation.conversation_id.is_unset() {
        headers.insert(
            async_nats::HeaderName::from_static(h::CONVERSATION_ID),
            header_value(
                h::CONVERSATION_ID,
                &envelope.metadata.correlation.conversation_id.to_string(),
            )?,
        );
    }
    if let Some(causation_id) = envelope.metadata.correlation.causation_id {
        headers.insert(
            async_nats::HeaderName::from_static(h::CAUSATION_ID),
            header_value(h::CAUSATION_ID, &causation_id.to_string())?,
        );
    }
    if let Some(request_id) = envelope.metadata.correlation.request_id {
        headers.insert(
            async_nats::HeaderName::from_static(h::REQUEST_ID),
            header_value(h::REQUEST_ID, &request_id.to_string())?,
        );
    }
    if let Some(tenant_id) = &envelope.metadata.tenant_id {
        headers.insert(
            async_nats::HeaderName::from_static(h::TENANT_ID),
            header_value(h::TENANT_ID, tenant_id)?,
        );
    }

    insert_routing_trace_and_dedup_headers(headers, envelope)
}

/// Split out of [`insert_framework_headers`] purely to keep each function short — the two halves
/// have no semantic relationship to each other.
fn insert_routing_trace_and_dedup_headers(
    headers: &mut async_nats::HeaderMap,
    envelope: &SerializedEnvelope,
) -> Result<(), NatsMapError> {
    if let Some(sent_at) = envelope.metadata.delivery.sent_at {
        headers.insert(
            async_nats::HeaderName::from_static(h::SENT_AT),
            header_value(h::SENT_AT, &rfc3339_utc(sent_at, h::SENT_AT)?)?,
        );
    }
    if let Some(expires_at) = envelope.metadata.delivery.expires_at {
        headers.insert(
            async_nats::HeaderName::from_static(h::EXPIRES_AT),
            header_value(h::EXPIRES_AT, &rfc3339_utc(expires_at, h::EXPIRES_AT)?)?,
        );
    }
    if let Some(source) = &envelope.metadata.routing.source {
        headers.insert(
            async_nats::HeaderName::from_static(h::SOURCE),
            header_value(h::SOURCE, source.as_str())?,
        );
    }
    if let Some(destination) = &envelope.metadata.routing.destination {
        headers.insert(
            async_nats::HeaderName::from_static(h::DESTINATION),
            header_value(h::DESTINATION, destination.as_str())?,
        );
    }
    if let Some(reply_to) = &envelope.metadata.routing.reply_to {
        headers.insert(
            async_nats::HeaderName::from_static(h::REPLY_TO),
            header_value(h::REPLY_TO, reply_to.as_str())?,
        );
    }
    if let Some(traceparent) = &envelope.metadata.trace.traceparent {
        headers.insert(
            async_nats::HeaderName::from_static(h::TRACEPARENT),
            header_value(h::TRACEPARENT, traceparent)?,
        );
    }
    if let Some(tracestate) = &envelope.metadata.trace.tracestate {
        headers.insert(
            async_nats::HeaderName::from_static(h::TRACESTATE),
            header_value(h::TRACESTATE, tracestate)?,
        );
    }

    // Always written (#17): the transport's own dedup key, falling back to the message id
    // (ADR 0026 §5).
    let dedup_value = envelope
        .metadata
        .delivery
        .deduplication_id
        .clone()
        .unwrap_or_else(|| envelope.id.to_string());
    headers.insert(
        async_nats::HeaderName::from_static(h::NATS_MSG_ID),
        header_value(h::NATS_MSG_ID, &dedup_value)?,
    );

    Ok(())
}

/// Which projected/ignored/custom bucket a decoded header name falls into (contract §2.5),
/// decided by case-insensitive comparison against each known name directly — never by
/// allocating a lowercased copy of the name first (minor perf finding, S1 review #14).
enum HeaderKind {
    MessageId,
    MessageType,
    MessageVersion,
    ContentType,
    CorrelationId,
    ConversationId,
    CausationId,
    RequestId,
    TenantId,
    SentAt,
    ExpiresAt,
    Source,
    Destination,
    ReplyTo,
    Traceparent,
    Tracestate,
    NatsMsgId,
    /// Broker bookkeeping (`Nats-Stream`, `Nats-Sequence`, …) — ignored (contract §2.5).
    NatsBookkeeping,
    /// A `reliar-*` header this decoder does not recognise — forward compatibility with a newer
    /// producer (contract §2.5).
    UnknownReliar,
    Custom,
}

fn classify_header(name: &str) -> HeaderKind {
    if name.eq_ignore_ascii_case(h::MESSAGE_ID) {
        HeaderKind::MessageId
    } else if name.eq_ignore_ascii_case(h::MESSAGE_TYPE) {
        HeaderKind::MessageType
    } else if name.eq_ignore_ascii_case(h::MESSAGE_VERSION) {
        HeaderKind::MessageVersion
    } else if name.eq_ignore_ascii_case(h::CONTENT_TYPE) {
        HeaderKind::ContentType
    } else if name.eq_ignore_ascii_case(h::CORRELATION_ID) {
        HeaderKind::CorrelationId
    } else if name.eq_ignore_ascii_case(h::CONVERSATION_ID) {
        HeaderKind::ConversationId
    } else if name.eq_ignore_ascii_case(h::CAUSATION_ID) {
        HeaderKind::CausationId
    } else if name.eq_ignore_ascii_case(h::REQUEST_ID) {
        HeaderKind::RequestId
    } else if name.eq_ignore_ascii_case(h::TENANT_ID) {
        HeaderKind::TenantId
    } else if name.eq_ignore_ascii_case(h::SENT_AT) {
        HeaderKind::SentAt
    } else if name.eq_ignore_ascii_case(h::EXPIRES_AT) {
        HeaderKind::ExpiresAt
    } else if name.eq_ignore_ascii_case(h::SOURCE) {
        HeaderKind::Source
    } else if name.eq_ignore_ascii_case(h::DESTINATION) {
        HeaderKind::Destination
    } else if name.eq_ignore_ascii_case(h::REPLY_TO) {
        HeaderKind::ReplyTo
    } else if name.eq_ignore_ascii_case(h::TRACEPARENT) {
        HeaderKind::Traceparent
    } else if name.eq_ignore_ascii_case(h::TRACESTATE) {
        HeaderKind::Tracestate
    } else if name.eq_ignore_ascii_case(h::NATS_MSG_ID) {
        HeaderKind::NatsMsgId
    } else if starts_with_ci(name, h::NATS_PREFIX) {
        HeaderKind::NatsBookkeeping
    } else if starts_with_ci(name, h::RELIAR_PREFIX) {
        HeaderKind::UnknownReliar
    } else {
        HeaderKind::Custom
    }
}

/// The raw string form of every header `decode` recognises, borrowed straight out of the wire
/// message's own `HeaderMap` — nothing here is copied until [`Self::into_envelope`] assembles the
/// owned `Metadata` a caller can keep past the wire message's lifetime.
#[derive(Default)]
struct ScannedHeaders<'a> {
    message_id: Option<&'a str>,
    message_type: Option<&'a str>,
    message_version: Option<&'a str>,
    content_type: Option<&'a str>,
    correlation_id: Option<&'a str>,
    conversation_id: Option<&'a str>,
    causation_id: Option<&'a str>,
    request_id: Option<&'a str>,
    tenant_id: Option<&'a str>,
    sent_at: Option<&'a str>,
    expires_at: Option<&'a str>,
    source: Option<&'a str>,
    destination: Option<&'a str>,
    reply_to: Option<&'a str>,
    traceparent: Option<&'a str>,
    tracestate: Option<&'a str>,
    nats_msg_id: Option<&'a str>,
    custom: Option<Headers>,
}

impl<'a> ScannedHeaders<'a> {
    fn scan(headers: &'a async_nats::HeaderMap) -> Result<Self, NatsMapError> {
        let mut scanned = Self::default();

        for (name, values) in headers.iter() {
            let raw_name: &str = name.as_ref();

            match classify_header(raw_name) {
                HeaderKind::MessageId => {
                    assign_once(&mut scanned.message_id, values, h::MESSAGE_ID)?;
                }
                HeaderKind::MessageType => {
                    assign_once(&mut scanned.message_type, values, h::MESSAGE_TYPE)?;
                }
                HeaderKind::MessageVersion => {
                    assign_once(&mut scanned.message_version, values, h::MESSAGE_VERSION)?;
                }
                HeaderKind::ContentType => {
                    assign_once(&mut scanned.content_type, values, h::CONTENT_TYPE)?;
                }
                HeaderKind::CorrelationId => {
                    assign_once(&mut scanned.correlation_id, values, h::CORRELATION_ID)?;
                }
                HeaderKind::ConversationId => {
                    assign_once(&mut scanned.conversation_id, values, h::CONVERSATION_ID)?;
                }
                HeaderKind::CausationId => {
                    assign_once(&mut scanned.causation_id, values, h::CAUSATION_ID)?;
                }
                HeaderKind::RequestId => {
                    assign_once(&mut scanned.request_id, values, h::REQUEST_ID)?;
                }
                HeaderKind::TenantId => {
                    assign_once(&mut scanned.tenant_id, values, h::TENANT_ID)?;
                }
                HeaderKind::SentAt => {
                    assign_once(&mut scanned.sent_at, values, h::SENT_AT)?;
                }
                HeaderKind::ExpiresAt => {
                    assign_once(&mut scanned.expires_at, values, h::EXPIRES_AT)?;
                }
                HeaderKind::Source => {
                    assign_once(&mut scanned.source, values, h::SOURCE)?;
                }
                HeaderKind::Destination => {
                    assign_once(&mut scanned.destination, values, h::DESTINATION)?;
                }
                HeaderKind::ReplyTo => {
                    assign_once(&mut scanned.reply_to, values, h::REPLY_TO)?;
                }
                HeaderKind::Traceparent => {
                    assign_once(&mut scanned.traceparent, values, h::TRACEPARENT)?;
                }
                HeaderKind::Tracestate => {
                    assign_once(&mut scanned.tracestate, values, h::TRACESTATE)?;
                }
                HeaderKind::NatsMsgId => {
                    assign_once(&mut scanned.nats_msg_id, values, h::NATS_MSG_ID)?;
                }
                HeaderKind::NatsBookkeeping | HeaderKind::UnknownReliar => {}
                HeaderKind::Custom => {
                    // Several values: encode never writes one, so the first value wins.
                    let value = values
                        .first()
                        .map(async_nats::HeaderValue::as_str)
                        .unwrap_or_default();
                    scanned
                        .custom
                        .get_or_insert_with(Headers::default)
                        .insert(raw_name.to_string(), value.to_string())
                        .map_err(|err| NatsMapError::RejectedHeader {
                            key: raw_name.to_string(),
                            source: err,
                        })?;
                }
            }
        }

        Ok(scanned)
    }

    /// Parses every scanned raw value into typed `Metadata` and assembles the envelope
    /// (contract §2.5), via [`Self::parse_identity`] for the four required headers and
    /// [`Self::parse_metadata`] for everything optional.
    fn into_envelope(self, payload: Bytes) -> Result<SerializedEnvelope, NatsMapError> {
        let (id, message_id_raw, message_type, content_type) = self.parse_identity()?;
        let (mut metadata, custom) = self.parse_metadata(message_id_raw)?;
        metadata.delivery.content_type = content_type;
        Ok(SerializedEnvelope::from_parts(
            id,
            message_type,
            payload,
            metadata,
            custom,
        ))
    }

    /// Parses the four required headers (§2.5): the message id, its raw string form (needed for
    /// the `Nats-Msg-Id` dedup comparison in [`Self::parse_metadata`]), the message type, and the
    /// content type. Borrows rather than clones — only the two fields that must outlive the wire
    /// message (`message_type`'s name, `content_type`) are copied into an owned `String`.
    fn parse_identity(
        &self,
    ) -> Result<(MessageId, &'a str, MessageType, ContentType), NatsMapError> {
        let message_id_raw = self.message_id.ok_or(NatsMapError::MissingHeader {
            header: h::MESSAGE_ID,
        })?;
        let message_type_raw = self.message_type.ok_or(NatsMapError::MissingHeader {
            header: h::MESSAGE_TYPE,
        })?;
        let message_version_raw = self.message_version.ok_or(NatsMapError::MissingHeader {
            header: h::MESSAGE_VERSION,
        })?;
        let content_type_raw = self.content_type.ok_or(NatsMapError::MissingHeader {
            header: h::CONTENT_TYPE,
        })?;

        let id = Uuid::parse_str(message_id_raw)
            .map(MessageId::from_uuid)
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::MESSAGE_ID,
            })?;
        let version: u16 =
            message_version_raw
                .parse()
                .map_err(|_| NatsMapError::MalformedHeader {
                    header: h::MESSAGE_VERSION,
                })?;
        // `MessageType::from_parts` is core's deliberately unvalidated rehydration path (ADR
        // 0011) and would happily accept an empty name, producing a `MessageType` that renders as
        // `".v1"`. Emptiness is the only name rule this mapper enforces on decode (ADR 0026
        // Amendment C, contract §2.5, U17) — a non-empty foreign name is accepted verbatim.
        if message_type_raw.is_empty() {
            return Err(NatsMapError::MalformedHeader {
                header: h::MESSAGE_TYPE,
            });
        }
        let message_type = MessageType::from_parts(message_type_raw.to_string(), version);
        let content_type = ContentType::parse(content_type_raw.to_string()).map_err(|_| {
            NatsMapError::MalformedHeader {
                header: h::CONTENT_TYPE,
            }
        })?;

        Ok((id, message_id_raw, message_type, content_type))
    }

    /// Parses every optional header into `Metadata` (content type excluded — the caller fills
    /// that in from [`Self::parse_identity`]) plus the decoded custom headers.
    fn parse_metadata(
        self,
        message_id_raw: &str,
    ) -> Result<(Metadata, Option<Headers>), NatsMapError> {
        let correlation_id = self
            .correlation_id
            .map(CorrelationId::parse)
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::CORRELATION_ID,
            })?;
        let conversation_id = match self.conversation_id {
            Some(raw) => Uuid::parse_str(raw)
                .map(ConversationId::from_uuid)
                .map_err(|_| NatsMapError::MalformedHeader {
                    header: h::CONVERSATION_ID,
                })?,
            None => ConversationId::UNSET,
        };
        let causation_id = self
            .causation_id
            .map(|raw| Uuid::parse_str(raw).map(MessageId::from_uuid))
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::CAUSATION_ID,
            })?;
        let request_id = self
            .request_id
            .map(|raw| Uuid::parse_str(raw).map(RequestId::from_uuid))
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::REQUEST_ID,
            })?;
        let sent_at = self
            .sent_at
            .map(|raw| {
                time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
            })
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader { header: h::SENT_AT })?;
        let expires_at = self
            .expires_at
            .map(|raw| {
                time::OffsetDateTime::parse(raw, &time::format_description::well_known::Rfc3339)
            })
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::EXPIRES_AT,
            })?;
        let source = self
            .source
            .map(EndpointAddress::parse)
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader { header: h::SOURCE })?;
        let destination = self
            .destination
            .map(EndpointAddress::parse)
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::DESTINATION,
            })?;
        let reply_to = self
            .reply_to
            .map(EndpointAddress::parse)
            .transpose()
            .map_err(|_| NatsMapError::MalformedHeader {
                header: h::REPLY_TO,
            })?;

        // ADR 0026 §5: a dedup key equal to the message id decodes as "not set" — encode always
        // writes one, so a genuinely distinct value is the only case a caller chose it.
        let deduplication_id = self
            .nats_msg_id
            .filter(|v| *v != message_id_raw)
            .map(str::to_string);

        let mut metadata = Metadata::default();
        metadata.correlation.correlation_id = correlation_id;
        metadata.correlation.conversation_id = conversation_id;
        metadata.correlation.causation_id = causation_id;
        metadata.correlation.request_id = request_id;
        metadata.trace.traceparent = self.traceparent.map(str::to_string);
        metadata.trace.tracestate = self.tracestate.map(str::to_string);
        metadata.routing.source = source;
        metadata.routing.destination = destination;
        metadata.routing.reply_to = reply_to;
        metadata.delivery.sent_at = sent_at;
        metadata.delivery.expires_at = expires_at;
        metadata.delivery.deduplication_id = deduplication_id;
        metadata.tenant_id = self.tenant_id.map(str::to_string);

        Ok((metadata, self.custom))
    }
}

/// Writes `s` through [`async_nats::HeaderValue::from_str`] (never the panicking `From<&str>`
/// conversion), naming `header` on failure (contract's "no `&str` into an `async-nats` header"
/// rule).
fn header_value(header: &str, s: &str) -> Result<async_nats::HeaderValue, NatsMapError> {
    async_nats::HeaderValue::from_str(s).map_err(|_| NatsMapError::InvalidHeaderValue {
        header: header.to_string(),
    })
}

/// Formats `at` as RFC 3339 in UTC (`…Z`), naming `header` on the (practically unreachable)
/// formatting failure.
fn rfc3339_utc(at: time::OffsetDateTime, header: &'static str) -> Result<String, NatsMapError> {
    at.to_offset(time::UtcOffset::UTC)
        .format(&time::format_description::well_known::Rfc3339)
        .map_err(|_| NatsMapError::InvalidHeaderValue {
            header: header.to_string(),
        })
}

/// Records one occurrence of a framework header into `slot`. A second occurrence — a genuine
/// repeat, or the same header under a different casing hashing to a distinct wire entry — is a
/// [`NatsMapError::DuplicateHeader`]. A single wire entry already carrying more than one value is
/// folded into the same error: `encode` never produces one, so on decode it is indistinguishable
/// from a second, later occurrence of the same header. A wire entry with **zero** values is not
/// semantically a duplicate, but `encode` never produces that shape either, so this crate's own
/// `HeaderMap` usage can never trigger it; rather than add an error variant nothing can ever
/// construct, the zero-value case is reported as `DuplicateHeader` too.
fn assign_once<'a>(
    slot: &mut Option<&'a str>,
    values: &'a [async_nats::HeaderValue],
    header: &'static str,
) -> Result<(), NatsMapError> {
    if slot.is_some() || values.len() != 1 {
        return Err(NatsMapError::DuplicateHeader { header });
    }
    *slot = Some(values[0].as_str());
    Ok(())
}

/// Case-insensitive prefix check that avoids allocating a lowercased copy of `s`.
fn starts_with_ci(s: &str, prefix: &str) -> bool {
    s.len() >= prefix.len() && s.as_bytes()[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
}
