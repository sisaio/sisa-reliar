# Security policy

## Supported versions

Reliar is pre-1.0. Security fixes land on the **latest published minor** only; there are no
long-term support branches yet. This table is updated at each minor release.

| Version | Supported |
| ------- | --------- |
| 0.1.x (unreleased) | ✅ |

## Reporting a vulnerability

**Do not open a public issue.** Report privately through GitHub Security Advisories:

<https://github.com/sisaio/sisa-reliar/security/advisories/new>

Please include the affected crate and version, a description of the impact, and the smallest
reproduction you have. We aim to acknowledge within 3 working days and to ship a fix or a
mitigation plan within 30 days, coordinating disclosure with you and publishing a RustSec advisory.

## Scope notes

Reliar is a library: it holds no credentials and opens no listening sockets. Findings most relevant
to us are SQL injection or query-construction flaws, unsound concurrency that could drop or
duplicate a message beyond the documented at-least-once window, denial of service in a worker loop,
and leaking payloads or credentials through logs, error `Display`, or `Debug`. Supply-chain issues
in dependencies are tracked by `cargo deny` and `cargo audit` in `security.yaml`.
