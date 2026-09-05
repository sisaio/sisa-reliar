# reliar-transport-nats

Reliar's first real transport: `NatsEnvelopeMapper` projects the canonical `Envelope` onto NATS
headers plus a raw payload and back (SRS §15–§16), and `SubjectResolver` keeps NATS subject
selection a transport-side concern, out of `reliar-core` (SRS §12, §18, ADR 0027).

Depends on `reliar-core` only (plus `async-nats`) — `Publisher`, `Classify`, `FailureKind` and
`SettingsError` all live in `reliar-core` (ADR 0032). No other provider crate depends on it and it
depends on no other provider.

## Status: S2 of 3

This is slice S2 of the Phase 2 NATS transport (`../../docs/architecture/phase2-contract.md`,
backlog card RELIAR-33 in the sibling `sisa-reliar-backlog` repo). It ships everything S1
shipped plus:

- `NatsPublisher` — encodes, resolves the subject, publishes through `JetStream`, and **awaits
  the server's ack** before returning `Ok` (ADR 0028)
- `NatsPublishError` / `NatsConfigError` — hand-rolled, `#[non_exhaustive]`, every variant's
  `Classify` verdict fixed and tested (ADR 0030)

S1 shipped:

- `NatsEnvelopeMapper` — encode/decode between `SerializedEnvelope` and `NatsWireMessage`
- the `headers` module — every `reliar-*`/`Nats-Msg-Id`/W3C header name this crate writes
- `SubjectResolver`, `PrefixSubjects`, `DestinationSubjects` — subject selection
- `NatsSettings` — publisher configuration, `Default` + builder + opt-in `from_env`

S3 adds the end-to-end test, `examples/nats-pub-sub`, and the guide.

## Guarantees

- **The wire form carries no routing.** `NatsWireMessage` has no subject and no reply subject —
  those belong to a `SubjectResolver` and to the receiving subscription, never to the mapper.
- **Every canonical field is written exactly once, and never duplicated into a custom header.**
  Three things make this hold: `reliar_core::Headers` refuses the `reliar-` prefix on insert,
  `encode` rejects a custom key in NATS's own reserved `Nats-` namespace, and for the two W3C trace
  names core does *not* reserve (`traceparent`/`tracestate`) `encode` itself arbitrates a
  case-insensitive collision — the framework value wins when set, the custom spelling is written
  canonically otherwise, and a second colliding spelling with nothing to arbitrate is a permanent
  error (ADR 0026 Amendment A).
- **`decode` never panics.** A missing or malformed required header is a permanent `NatsMapError`;
  an unrecognised `reliar-*`/`Nats-*` header is ignored for forward compatibility.
- **`SubjectResolver` is pure and synchronous** — resolution is a function of the envelope alone.
- **`publish` only returns `Ok` after the server's ack.** `NatsPublisher` ships no Core-NATS
  (fire-and-forget) path — durability would otherwise be a lie (ADR 0028). `publish_batch`
  pipelines sends in `batch_pipeline_depth` windows and awaits acks positionally; the v0.1
  `reliar-outbox` `OutboxDispatcher` calls `publish` only, so this override and
  `batch_pipeline_depth` are reachable today solely through a direct `publish_batch` caller (a
  dispatcher batch path is tracked separately, RELIAR-39).
- **Every `NatsPublishError` variant has a fixed, tested `Classify` verdict.**
- **The library never reads the environment implicitly.** Only `NatsSettings::from_env` touches
  `std::env`, and only when called.
- **No `Display`/`Debug` in this crate ever prints a header value, a payload byte, or a server
  address** — only header names/keys, the subject (routing configuration, not user data), and
  numeric facts.
- **It never creates a stream, never connects, and never reads the environment.** `NatsPublisher`
  wraps an application-owned `JetStream` context (ADR 0029); the application owns the connection
  and every stream it publishes into.

## Example

```rust
use reliar_core::{Envelope, EnvelopeMapper, Message};
use reliar_transport_nats::{NatsEnvelopeMapper, PrefixSubjects, SubjectResolver};

#[derive(serde::Serialize, serde::Deserialize)]
struct OrderCreated { order_id: u64 }
impl Message for OrderCreated {
    const TYPE: &'static str = "orders.created";
    const VERSION: u16 = 1;
}

let envelope = Envelope::builder(OrderCreated { order_id: 42 })
    .build()
    .map_body(|_| bytes::Bytes::from_static(b"{}"));

let mapper = NatsEnvelopeMapper::default();
let wire = mapper.encode(&envelope)?;

let resolver = PrefixSubjects::new("app")?;
let subject = resolver.subject(&envelope)?;
assert_eq!(subject.as_str(), "app.orders.created.v1");
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Composition

```rust,ignore
let client = async_nats::connect(&std::env::var("NATS_URL")?).await?;   // the app reads the env
let js = async_nats::jetstream::new(client);
// The stream is created by the application or the operator — never by Reliar.

let publisher = NatsPublisher::new(js, NatsSettings::default().subject_prefix("app"))?;
let dispatcher = OutboxDispatcher::builder(store, publisher).build()?;
dispatcher.run(cancel).await?;
```

## Testing

`cargo test -p reliar-transport-nats` runs the pure mapper/subject/settings tests with no server,
plus one NATS-touching binary (`tests/nats/`, ADR 0031 §4): it reuses `NATS_URL` when set, or
starts its own ephemeral `nats:2.14-alpine -js` container otherwise, and drops it before exiting.
`cargo bench -p reliar-transport-nats` records a `NatsPublisher::publish` baseline when `NATS_URL`
is set and skips cleanly otherwise.
