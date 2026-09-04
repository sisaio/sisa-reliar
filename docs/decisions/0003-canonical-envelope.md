# ADR 0003 — One canonical `Envelope<T>` for typed and serialized messages

**Status:** Accepted — 2026-09-04
**SRS:** §9, §9.1, §11, §12, §17, §43.A.2

## Context

Every messaging library needs a place to hang the things that are not the message body: ids,
correlation, trace context, routing, delivery hints. Three shapes are common: put them in a
per-transport struct (they drift), pass them as loose function arguments (they get dropped), or
carry them in one envelope alongside the body.

Reliar additionally has to cross a serialization boundary: application handlers want
`OrderCreated`, while the outbox table and every broker want bytes. If the typed and serialized
sides are two unrelated structs, every field must be listed twice and they will diverge.

## Decision

- **One generic type, `Envelope<T>`**, holding `id`, `message_type`, `body: T`, typed `metadata`,
  and lazily-allocated private `headers` (§9). The serialized form is the **same type**:
  `pub type SerializedEnvelope = Envelope<bytes::Bytes>`.
- `Envelope::map_body` converts between them (`Envelope<T> → SerializedEnvelope` on the way out,
  and back on rehydration), so no field is ever re-declared and no `From` impl can lose one.
- `Metadata` is **typed and structured** — `correlation`, `trace`, `routing`, `delivery`,
  `tenant_id` — not a string map (§12). Framework concepts Reliar understands get a field.
- Construction goes through `Envelope::builder(body)` (ADR 0011). `message_type` is derived from
  `T::TYPE`/`T::VERSION` and cannot be passed in (§10.1, ADR 0010).
- `Envelope<T>` SHALL NOT require `T: Clone`; the dispatcher moves owned records into publish
  tasks. `Debug` on `SerializedEnvelope` elides the payload bytes (§33).
- `reliar-core` owns the envelope and depends on nothing Reliar-specific — no sqlx, no broker
  client, no transport routing concepts (§12's transport-field exclusion list).

## Consequences

- Adding a metadata concept is one field in one place; the transport mapper and the provider
  both see it. Adding a transport-specific one is impossible in core, which is the point.
- `SerializedEnvelope` inherits `Envelope`'s generic machinery, so a provider or mapper that
  rehydrates a row builds the same type the application built — proven by the round-trip
  criterion (§43.A.4).
- The persistence shape is deliberately **not** this type: `OutboxRecord` wraps a
  `SerializedEnvelope` plus delivery state (ADR 0005). Merging them would put `attempts` and
  `locked_by` on the wire.
- `Metadata` being a struct rather than a map means an unknown field from a newer writer must be
  handled by the JSONB contract, not by the type (ADR 0012).
- `T: Message` (hence `Serialize + DeserializeOwned`) is required to build an envelope, so
  `serde` is a non-optional dependency of `reliar-core`.

## Alternatives considered

- **Separate `Message<T>` and `RawMessage` types.** Rejected: two field lists to keep in sync,
  and the mapper/provider round-trip becomes a hand-written conversion that can silently drop.
- **`HashMap<String, String>` metadata (MassTransit-style loose properties).** Rejected: no
  types, no compile-time rename safety, and it collapses §14's canonical-source rule.
- **Envelope as a wire format (JSON wrapper around the body).** Rejected: §15/§16 prefer native
  broker headers + a raw body, so the envelope is an in-process model, not a wire schema.
