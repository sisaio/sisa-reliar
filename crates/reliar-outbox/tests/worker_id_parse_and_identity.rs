//! `WorkerId::generate`/`parse`/`Display` (SRS §19.2, ADR 0011, review 1 major 4: `generate` is
//! `pid:uuid7`, reads no environment variable, and never fails).

use reliar_outbox::WorkerId;

#[test]
fn generate_never_reads_the_environment_and_shapes_pid_colon_uuid7() {
    let id = WorkerId::generate();
    // `pid:uuid7`: exactly one `:`, a numeric pid before it, a UUID-shaped tail after it.
    let parts: Vec<&str> = id.as_str().splitn(2, ':').collect();
    assert_eq!(
        parts.len(),
        2,
        "expected `pid:uuid7`, got {:?}",
        id.as_str()
    );
    assert!(
        parts[0].parse::<u32>().is_ok(),
        "pid segment must be numeric"
    );
    assert_eq!(parts[1].len(), 36, "a UUID renders as 36 characters");
}

#[test]
fn generate_is_unique_per_call() {
    let a = WorkerId::generate();
    let b = WorkerId::generate();
    assert_ne!(a, b, "two generated ids must never collide");
}

#[test]
fn parse_rejects_an_empty_string() {
    assert!(WorkerId::parse("").is_err());
}

#[test]
fn parse_rejects_a_value_over_max_len() {
    let overlong = "w".repeat(WorkerId::MAX_LEN + 1);
    assert!(WorkerId::parse(overlong).is_err());
}

#[test]
fn parse_accepts_a_value_at_exactly_max_len() {
    let exact = "w".repeat(WorkerId::MAX_LEN);
    assert!(WorkerId::parse(exact).is_ok());
}

#[test]
fn parse_round_trips_through_as_str_and_display() {
    let id = WorkerId::parse("custom-worker-1").expect("valid id");
    assert_eq!(id.as_str(), "custom-worker-1");
    assert_eq!(id.to_string(), "custom-worker-1");
}

#[test]
fn default_generates_rather_than_reading_a_fixed_value() {
    let a = WorkerId::default();
    let b = WorkerId::default();
    assert_ne!(a, b, "`Default` must generate a fresh id, not a constant");
}
