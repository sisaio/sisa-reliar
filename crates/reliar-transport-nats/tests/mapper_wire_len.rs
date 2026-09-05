//! `NatsWireMessage::wire_len` byte-exactness (contract §7, U15; S1 review major 7 / minor 7):
//! with at least one header, the `"NATS/1.0\r\n"` line, one `"{name}: {value}\r\n"` per header
//! value (a multi-value header counted once per value), the trailing `"\r\n"`, and
//! `payload.len()`; with **zero** headers, `payload.len()` alone — mirroring `async-nats`'s own
//! `Client::check_payload_size`, which only adds the header block's byte count when the header
//! map is non-empty (round-2 review m5). It decides S2's permanent `PayloadTooLarge` verdict, so
//! an off-by-`n` here would silently dead-letter a publishable message or silently let an
//! oversized one through.

use async_nats::{HeaderMap, HeaderName};
use bytes::Bytes;
use reliar_transport_nats::NatsWireMessage;

const NATS_LINE: &str = "NATS/1.0\r\n";
const TRAILING: &str = "\r\n";

fn header_line(name: &str, value: &str) -> String {
    format!("{name}: {value}\r\n")
}

/// With no headers at all, `async-nats`'s `Client::check_payload_size` counts the payload alone —
/// it never adds the `NATS/1.0`/terminator overhead when the header map is empty (round-2 review
/// m5). `wire_len` must match, or the pre-flight `max_payload` guard would reject a message the
/// server would have accepted (an off-by-12 false positive) with `max_payload` set exactly to the
/// server's own limit.
#[test]
fn zero_headers_and_an_empty_payload() {
    let wire = NatsWireMessage::new(HeaderMap::new(), Bytes::new());
    assert_eq!(wire.wire_len(), 0);
}

#[test]
fn zero_headers_with_a_nonempty_payload() {
    let payload = Bytes::from_static(b"hello world");
    let wire = NatsWireMessage::new(HeaderMap::new(), payload.clone());
    assert_eq!(wire.wire_len(), payload.len());
}

#[test]
fn one_header_and_a_payload() {
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("x-one"), "value-one");
    let payload = Bytes::from_static(b"{}");
    let wire = NatsWireMessage::new(headers, payload.clone());

    let expected =
        NATS_LINE.len() + header_line("x-one", "value-one").len() + TRAILING.len() + payload.len();
    assert_eq!(wire.wire_len(), expected);
}

#[test]
fn several_headers_and_a_payload() {
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("x-one"), "value-one");
    headers.insert(HeaderName::from_static("x-two"), "value-two");
    headers.insert(HeaderName::from_static("x-three"), "v3");
    let payload = Bytes::from_static(b"the quick brown fox");
    let wire = NatsWireMessage::new(headers, payload.clone());

    let expected = NATS_LINE.len()
        + header_line("x-one", "value-one").len()
        + header_line("x-two", "value-two").len()
        + header_line("x-three", "v3").len()
        + TRAILING.len()
        + payload.len();
    assert_eq!(wire.wire_len(), expected);
}

/// A multi-value header is counted **once per value** — `to_bytes`/the server write one wire line
/// per value under the same name, never a single combined line.
#[test]
fn a_multi_value_header_is_counted_once_per_value() {
    let mut headers = HeaderMap::new();
    headers.append(HeaderName::from_static("x-multi"), "first");
    headers.append(HeaderName::from_static("x-multi"), "second");
    headers.append(HeaderName::from_static("x-multi"), "third");
    let payload = Bytes::from_static(b"payload");
    let wire = NatsWireMessage::new(headers, payload.clone());

    let expected = NATS_LINE.len()
        + header_line("x-multi", "first").len()
        + header_line("x-multi", "second").len()
        + header_line("x-multi", "third").len()
        + TRAILING.len()
        + payload.len();
    assert_eq!(wire.wire_len(), expected);
}

/// A mix of a single-value and a multi-value header, to prove the count-once-per-value rule
/// composes with an ordinary header rather than being a special case only tested in isolation.
#[test]
fn a_mix_of_single_and_multi_value_headers() {
    let mut headers = HeaderMap::new();
    headers.insert(HeaderName::from_static("x-single"), "only-value");
    headers.append(HeaderName::from_static("x-multi"), "a");
    headers.append(HeaderName::from_static("x-multi"), "b");
    let wire = NatsWireMessage::new(headers, Bytes::new());

    let expected = NATS_LINE.len()
        + header_line("x-single", "only-value").len()
        + header_line("x-multi", "a").len()
        + header_line("x-multi", "b").len()
        + TRAILING.len();
    assert_eq!(wire.wire_len(), expected);
}
