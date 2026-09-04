# ADR 0020 — Metrics are a static-dispatch hook trait; Reliar ships no exporter

**Status:** Accepted — 2026-09-04
**SRS:** §5, §26, §33, §33.1, §19.3, §43.A.25–26

## Context

§5 and §26 require metrics; §33 constrained only names and cardinality; the **mechanism** was
undefined. The three candidate mechanisms have very different costs for a library:

- **Depend on OpenTelemetry** — drags an exporter, a runtime and a transitive dependency tree into
  every host, and forces the host's OTel version to match Reliar's.
- **Depend on the `metrics` facade** — much lighter, but still a dependency and still an opinion
  about which facade the host uses.
- **A trait the host implements** — zero dependency, but one more thing for the host to write.

Separately, the two signals operators actually alert on — outbox lag and dead count — are not
derivable from the dispatcher's own activity. They need a **store query**, which is why `stats`
exists on `OutboxStore` at all (ADR 0008).

## Decision

- **Metrics are a static-dispatch hook trait, `OutboxMetrics`, with a no-op default.** Every method
  has a default empty body, so the hook costs nothing when unused and **adding an instrument later
  is not a breaking change**. `NoopMetrics` is the default type parameter:
  `OutboxDispatcher<S, P, M = NoopMetrics>`.
- **No library crate depends on an OpenTelemetry exporter, a collector, or a metrics backend.** The
  host owns exporters.
- Behind an **optional `metrics` feature**, a `MetricsFacade` implements the trait over the
  `metrics` crate. Its metric names are a **public contract**: `reliar_outbox_claimed_total`,
  `reliar_outbox_published_total{message_type}`, `reliar_outbox_retried_total{kind}`,
  `reliar_outbox_dead_total{reason}`, `reliar_outbox_publish_duration_seconds{message_type}`,
  `reliar_outbox_pending`, `reliar_outbox_expired_pending`, `reliar_outbox_oldest_pending_age_seconds`,
  `reliar_outbox_purged_total{state}`. The `reliar_outbox_` prefix is a **metric namespace**, not a
  table name — Prometheus names cannot contain `.` — so it is unaffected by ADR 0017's schema
  naming and SHALL NOT be renamed when tables are.
- **Labels are bounded to `message_type`, `kind`, `reason`, `state`.** `message_id`,
  `correlation_id`, `tenant_id`, `worker_id` and `last_error` SHALL **NEVER** be metric labels.
  Cardinality explosions are the operator's problem to suffer and ours to prevent.
- `pending` and `oldest_pending_age` are fed from `OutboxStore::stats`, polled on a configurable
  interval (default 15 s), not per batch. **Both count claimable rows only**: an expired pending row
  can never be published, so including it would make the lag gauge grow without bound and page an
  operator about a backlog that does not exist. Expired rows get their own `expired_pending` gauge.
  *(Added 2026-09-04 after review 2 of the Phase-1 contract.)*
- **Spans** use the fixed names `reliar.outbox.{enqueue,claim,publish,retry,dead,purge}` with the
  §33.1 field lists. **`skip_all` is mandatory** on every instrumented function, so no `Envelope` or
  `OutboxRecord` can be printed into a field by accident.
- **Payloads and custom header values are never logged**, at any level, including `trace`. This is
  asserted with a recording `tracing` subscriber over the claim/publish/fail path (§43.A.26), and it
  extends to error `Display` output (`last_error` is truncated and credential-free, §17.1).
- **Trace context is carried, never invented.** The application sets
  `Metadata.trace.traceparent`/`tracestate` at enqueue; Reliar forwards them untouched; the
  transport mapper writes them as the W3C headers (ADR 0004). No `tracing-opentelemetry` dependency.
- Log levels are fixed: `error` — the loop itself is failing · `warn` — a message went dead, or an
  outcome write affected fewer rows than expected · `info` — start/stop and a **secret-free** config
  summary · `debug` — per-batch counts · `trace` — still never payloads.

## Consequences

- A host that wants Prometheus enables one feature; a host with a bespoke metrics stack implements
  eight methods; a host that wants neither pays nothing — the no-op monomorphizes away.
- `tracing` remains a direct dependency of the library crates. That is accepted (§31 explicitly does
  not abstract `tracing`): it is a facade, not a backend, and it is what the ecosystem uses.
- The metric names are a compatibility surface: renaming one breaks dashboards, so they change only
  via an ADR.
- Bounded labels mean an operator cannot slice `published_total` by tenant. That is deliberate; the
  per-tenant view belongs in logs or a database query, not in a time series.
- `stats` polling adds a periodic count query. On a large table that is not free, which is why the
  interval is configurable and why the index shapes in §24.1 include the partial indexes it needs.

## Alternatives considered

- **Depend on OpenTelemetry directly.** Rejected: an exporter in a library is an imposition, and
  version-locks the host.
- **Depend on the `metrics` facade unconditionally.** Rejected: still an opinion; kept as an
  optional feature so hosts that use it get an adapter for free.
- **Emit metrics as `tracing` events and let the host bridge them.** Rejected: loses instrument
  types (counter vs gauge vs histogram) and makes cardinality control the host's problem.
- **Allow `tenant_id` as a label, gated by a setting.** Rejected: a setting that can take down a
  metrics backend is a footgun; the rule is absolute.
