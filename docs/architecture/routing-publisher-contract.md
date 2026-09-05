# Routing-publisher contract — **WITHDRAWN**

**Status: withdrawn 2026-09-05 by [ADR 0036](../decisions/0036-outbox-enqueue-and-publisher-passthrough.md),
which supersedes ADR 0033.** Do not build against this file.

This file froze the surface of `reliar-outbox` 0.3.0 / `reliar-store-postgres` 0.3.0: a **routing
rule** (`OutboxSettings.enabled` / `allowed_types` / `disallowed_types` → `OutboxPolicy` →
`RouteKind`) that decided, per message type, whether one `publish` call staged the message in the
caller's transaction or sent it straight to the transport. The human withdrew that rule one day
after it shipped, because it let configuration decide a message's durability behind an identical
call site — and, for a disallowed type, published to the broker inside the caller's open
transaction.

**The live contract is [`outbox-publisher-contract.md`](outbox-publisher-contract.md).** In it, the
call site names the guarantee:

| 0.3.0 (this file) | 0.4.0 (the live contract) |
|---|---|
| `outbox.in_transaction(&mut tx).publish(&e).await?` | `outbox.enqueue(&mut tx, &e).await?` |
| `outbox.publish_direct(&e).await?` | `outbox.publish(&e).await?` — `OutboxPublisher` is itself a `reliar_core::Publisher` |
| `OutboxPolicy`, `RouteKind`, `ScopedOutboxPublisher`, `RouteError`, `DirectPublishError`, `MessageTypeNames` | removed, no replacement |
| `RELIAR_OUTBOX_ENABLED` / `_ALLOWED_TYPES` / `_DISALLOWED_TYPES` | removed; not read, not rejected |
| `OutboxStaging<Tx>`, `PostgresOutboxStore::enqueue`/`enqueue_with` | **unchanged** |

The reasoning, the rejected alternatives and the full removal list are in ADR 0036. The record of
what 0.3.0 actually shipped is [ADR 0033](../decisions/0033-outbox-routing-publisher.md), kept
verbatim; the signatures this file carried are recoverable from git history
(`git show 389625f -- docs/architecture/routing-publisher-contract.md`).
