# The envelope model

Owner: `reliar-core`. Frozen signatures: `phase1-contract.md` §2. Full crate map:
`overview.md`.

`reliar-core` is pure — no storage, no transport, no broker client, no routing concept (a Kafka
partition key, a Rabbit exchange, a NATS subject option belongs to a transport crate, never here,
ADR 0002). Everything below is what every other Reliar crate builds on.

## `Envelope<T>` and `SerializedEnvelope`

```text
Envelope<T>            id, message_type, body: T,        metadata, headers
        │  Serializer::serialize / Envelope::map_body
        ▼
SerializedEnvelope      id, message_type, body: Bytes,    metadata, headers      (= Envelope<Bytes>)
```

One generic type on both sides of the serialization boundary (ADR 0003) — converting between them
(`Envelope::map_body`/`try_map_body`) can never drop or duplicate a field. `SerializedEnvelope` is
what `OutboxStore`, `OutboxRecord` and (Phase 2) `EnvelopeMapper` all see; nothing downstream of
`enqueue` ever touches the typed `T` again.

## Message identity

A `Message` fixes its own `TYPE`/`VERSION` as associated constants — **never**
`std::any::type_name::<T>()` or a module path (ADR 0010). Renaming or moving the Rust type is
therefore safe, and two distinct Rust types that happen to share `TYPE`/`VERSION` render
identically on the wire (`MessageType::of::<T>()` → `"{name}.v{version}"`, e.g.
`orders.created.v1`).

## Metadata is canonical; headers are not a second copy

`Envelope::metadata` (`CorrelationMetadata`/`TraceContext`/`RoutingMetadata`/`DeliveryMetadata`) is
the **single source of truth** for everything Reliar understands. A value here is never duplicated
into `Headers`, and Reliar never reads a framework value back out of a header (ADR 0004) — a
(Phase 2) `EnvelopeMapper` is the only place `Metadata` is projected onto wire headers.

`Headers` is a validated newtype, not a bare map: it rejects the reserved `reliar-` prefix
case-insensitively, an empty key, any cap breach, and a control character in either the key or the
value (a header-injection defence — the same rule `CorrelationId`, `EndpointAddress` and
`ContentType` carry). It is lazily allocated: an envelope with no custom headers holds `None`, not
an empty map.

## Conversation rooting

`EnvelopeBuilder::build` roots an un-correlated message as its own conversation: it tests
`metadata.correlation.conversation_id.is_unset()` (the nil-UUID sentinel) and, only if still unset,
replaces it with the envelope's own `id`. Any real conversation id set via `.conversation(id)` or
copied from a causing message's `conversation_id` is left untouched. This is decided by the
**value**, not by which builder method was called — see `phase1-contract.md` §2.6 for the exact
rule and its edge cases.

## Payload safety

No Reliar type's `Debug` ever prints payload bytes or custom header values (SRS §33). `Envelope`'s
`Debug` elides `body` for every `T`; `Headers`' `Debug` shows keys and redacts every value. This is
why `OutboxRecord` (`reliar-outbox`) can safely *derive* `Debug` — its only sensitive content sits
one level down, behind these manual impls.
