# reliar-core

The pure envelope/message model every other Reliar crate builds on: identity newtypes, the
`Message` contract, `MessageType`, `ContentType`, `Serializer`/`JsonSerializer`, typed `Metadata`,
a validating `Headers` map, and `Envelope<T>`/`SerializedEnvelope` with its builder
(SRS §9–§17).

**Pure.** No storage or transport dependency — no `sqlx`, no broker client, no routing concept (a
Kafka partition key, a Rabbit exchange, a NATS subject option). Every other Reliar crate depends on
this one; this one depends on nothing Reliar-specific (ADR 0002). Enforced in CI by
`cargo tree -p reliar-core -e normal`.

## What this crate ships

- **Identity:** `MessageId`, `ConversationId`, `RequestId`, `CorrelationId` (validated,
  UUIDv7-backed newtypes).
- **Message identity:** the `Message` trait (`TYPE`/`VERSION` — never
  `std::any::type_name::<T>()`, ADR 0010) and `MessageType`.
- **Serialization:** the `Serializer` trait and the default `JsonSerializer` (feature `json`,
  enabled by default).
- **Metadata:** `Metadata` (`CorrelationMetadata`, `TraceContext`, `RoutingMetadata`,
  `DeliveryMetadata`, `EndpointAddress`) — the single source of truth for everything Reliar
  understands; never duplicated into headers (ADR 0004).
- **Headers:** a validating newtype (`Headers`), never a bare `HashMap` — rejects the reserved
  `reliar-` prefix, control characters, and cap breaches.
- **Envelope:** `Envelope<T>` / `SerializedEnvelope` (`= Envelope<bytes::Bytes>`) and
  `EnvelopeBuilder`, the one conversion point between typed and wire forms (ADR 0003).
- **Transport mapping (contract only in Phase 1):** the `EnvelopeMapper<M>` trait; no
  implementation ships until Phase 2.
- **Shared primitives (ADR 0032):** `Publisher` (a capability every transport implements),
  `Classify`/`FailureKind` (a publish error's transient/permanent verdict), and `SettingsError` —
  vocabulary more than one capability needs, not storage/transport-specific itself.

See `../../docs/architecture/envelope.md` for the model explained, and
`../../docs/architecture/phase1-contract.md` §2 for the frozen signatures.

## Payload and header safety

No `Debug`/`Display` in this crate ever prints a payload byte or a custom header value (SRS §33).
`Envelope`'s `Debug` elides `body` for every `T`; `Headers`' `Debug` shows keys and redacts every
value. Every error is a hand-rolled, `#[non_exhaustive]` enum with a wired `source()` — no
`thiserror`, no `anyhow`.

## Features

| Feature | Default | Enables |
|---|---|---|
| `json` | yes | `JsonSerializer` + `serde_json`. |
| `serde` | no | `Serialize`/`Deserialize` on `Metadata` and its parts, for a host that wants to persist or log them itself. Not required by any Reliar API. |

## Testing

`cargo test -p reliar-core --all-features`. Every test lives in `tests/` against the public API;
`benches/serialization.rs` (criterion, `cargo bench -p reliar-core --features json`) covers the
`Envelope<T>` ⇄ `SerializedEnvelope` cost through `JsonSerializer`.
