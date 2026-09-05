//! `OutboxPolicy::decide` reproduces every row of SRS §20.2's truth table, in the documented
//! evaluation order — disallow wins, an empty allow list means everything, a non-empty allow
//! list is exhaustive (§43.D1, §43.D12, ADR 0033).

use reliar_core::MessageType;
use reliar_outbox::{MessageTypeNames, OutboxPolicy, OutboxSettings, RouteKind};

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn names(list: &[&str]) -> MessageTypeNames {
    MessageTypeNames::try_from_iter("test", list.iter().copied()).expect("valid test names")
}

// A test helper, not itself a `#[test]` function: clippy's "allow unwrap/expect in tests"
// exemption only covers `#[test]` bodies, so it is granted explicitly here.
#[allow(clippy::expect_used)]
fn policy(enabled: bool, allowed: &[&str], disallowed: &[&str]) -> OutboxPolicy {
    let settings = OutboxSettings::default()
        .enabled(enabled)
        .allowed_types(names(allowed))
        .expect("non-overlapping test fixtures")
        .disallowed_types(names(disallowed))
        .expect("non-overlapping test fixtures");
    OutboxPolicy::from_settings(&settings).expect("non-overlapping test fixtures")
}

fn mt(name: &str) -> MessageType {
    MessageType::from_parts(name.to_string(), 1)
}

/// One case per row so a missing row is visible at a glance, rather than scattered across
/// several `#[test]` functions that could silently drift out of sync with each other.
struct Case {
    enabled: bool,
    allowed: &'static [&'static str],
    disallowed: &'static [&'static str],
    type_name: &'static str,
    expected: RouteKind,
}

#[test]
fn every_row_of_the_truth_table() {
    use RouteKind::{Direct, Outbox};

    let cases = [
        // enabled = false -> Direct, regardless of the lists (row 1).
        Case {
            enabled: false,
            allowed: &[],
            disallowed: &[],
            type_name: "a",
            expected: Direct,
        },
        Case {
            enabled: false,
            allowed: &["a"],
            disallowed: &["z"],
            type_name: "a",
            expected: Direct,
        },
        Case {
            enabled: false,
            allowed: &["a"],
            disallowed: &["z"],
            type_name: "z",
            expected: Direct,
        },
        // enabled = true, both lists empty -> Outbox for everything (row 2).
        Case {
            enabled: true,
            allowed: &[],
            disallowed: &[],
            type_name: "a",
            expected: Outbox,
        },
        Case {
            enabled: true,
            allowed: &[],
            disallowed: &[],
            type_name: "z",
            expected: Outbox,
        },
        // enabled = true, empty allow, non-empty disallow -> the disallowed type is Direct,
        // every other type stays Outbox (row 3 — the primary rollout shape, "everything except
        // c").
        Case {
            enabled: true,
            allowed: &[],
            disallowed: &["c"],
            type_name: "c",
            expected: Direct,
        },
        Case {
            enabled: true,
            allowed: &[],
            disallowed: &["c"],
            type_name: "a",
            expected: Outbox,
        },
        // enabled = true, non-empty allow, type is allowed and not disallowed -> Outbox (row 4).
        Case {
            enabled: true,
            allowed: &["a"],
            disallowed: &["b"],
            type_name: "a",
            expected: Outbox,
        },
        // enabled = true, non-empty allow, type is not in it -> Direct (row 5) — including the
        // type that is in the disallow list and one in neither list.
        Case {
            enabled: true,
            allowed: &["a"],
            disallowed: &["b"],
            type_name: "b",
            expected: Direct,
        },
        Case {
            enabled: true,
            allowed: &["a"],
            disallowed: &["b"],
            type_name: "c",
            expected: Direct,
        },
        Case {
            enabled: true,
            allowed: &["a"],
            disallowed: &[],
            type_name: "c",
            expected: Direct,
        },
    ];

    for case in cases {
        let policy = policy(case.enabled, case.allowed, case.disallowed);
        assert_eq!(
            policy.decide(&mt(case.type_name)),
            case.expected,
            "enabled={} allowed={:?} disallowed={:?} type={}",
            case.enabled,
            case.allowed,
            case.disallowed,
            case.type_name
        );
    }
}

/// §43.D1 / R2: disabling the policy overrides **both** lists at once, even when the type is
/// simultaneously named in a populated (non-overlapping) allow list and a populated disallow
/// list — step 1 short-circuits before either list is consulted.
#[test]
fn disabled_ignores_both_lists_even_when_populated() {
    let policy = policy(false, &["a"], &["z"]);
    assert_eq!(policy.decide(&mt("a")), RouteKind::Direct);
    assert_eq!(policy.decide(&mt("z")), RouteKind::Direct);
    assert_eq!(policy.decide(&mt("unrelated")), RouteKind::Direct);
}
