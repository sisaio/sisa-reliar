# ADR 0007 — At-least-once publication, with both duplicate windows documented

**Status:** Accepted — 2026-09-04
**SRS:** §22, §22.1, §23.2, §26.1, §43.A.11

## Context

The publish and the "mark published" write happen against two different systems — a broker and a
database — with no shared transaction. There is no ordering of those two operations that makes
them atomic:

- publish first, then mark: a crash in between republishes on lease expiry (a duplicate);
- mark first, then publish: a crash in between loses the message entirely (data loss).

Distributed transactions (XA/2PC) could close the gap in theory. In practice no broker Reliar
targets supports them usefully, and the operational cost of a transaction coordinator is far
larger than the cost of idempotent consumers.

## Decision

- Reliar **publishes first, records the outcome second**, and therefore guarantees **durable
  at-least-once publication**. Losing a message is unacceptable; delivering it twice is not.
- Reliar SHALL NOT claim exactly-once broker publication, in the README, in rustdoc, or anywhere
  else. Duplicate delivery is a documented, expected outcome that consumers handle — with the
  Inbox (§27, Phase 3), with idempotent handlers, or with broker-side suppression.
- **Two duplicate windows are named, rustdoc'd on `OutboxDispatcher`, stated in the README, and
  each has a test** (§43.A.11):
  1. **The crash window** (§22): publish succeeds → worker dies before `complete` → lease expires
     → another worker republishes.
  2. **The slow-batch window** (§22.1): no crash at all. A healthy worker's lease expires
     mid-batch, a second worker reclaims the tail and republishes it while the first is still
     publishing. In practice this is the common one.
- A **third, smaller window** exists at shutdown: publishes still in flight when `drain_timeout`
  expires are released and may be republished (§26.1, ADR 0014). It is logged at `warn` with a
  count and documented with the other two.
- Mitigations are required but explicitly **do not close** the windows: lease renewal and a
  per-publish timeout (§21.1) shrink window 2; the `locked_by` guard stops the losing worker from
  corrupting state (ADR 0008); `deduplication_id` → the broker's native dedup key (`Nats-Msg-Id`)
  narrows it at the broker (§12.3). None makes publication exactly-once.

## Consequences

- Consumers must be idempotent. That is a stated adoption requirement, not a footnote.
- `complete` is safe to retry: the `locked_by` guard makes a repeated `complete` a no-op, so a
  failed outcome write can be retried with backoff before the batch is abandoned (§23.2).
- A message is never silently lost: every path either publishes, retries, or lands in an
  inspectable dead state (ADR 0009).
- Test suites must *assert* duplication rather than avoid it — the recording publisher observing
  the same id twice is a passing test, not a bug (§43.A.11).
- Honest marketing costs adopters who wanted "exactly-once". Accepted; the alternative is a
  promise the system cannot keep.

## Alternatives considered

- **Mark-then-publish (at-most-once).** Rejected: silent message loss, which the outbox pattern
  exists to prevent.
- **XA / two-phase commit across Postgres and the broker.** Rejected: broker support is absent or
  unusable, a coordinator becomes a new single point of failure, and prepared transactions block
  vacuum — a worse failure mode than a duplicate.
- **Claiming "effectively-once" via broker dedup.** Rejected as dishonest: dedup windows are
  finite and best-effort, and the claim would stop consumers from doing the work they must do.
