use super::*;
use proptest::prelude::*;

/// A key's place, spread over every scale: a small number, or any.
fn place() -> impl Strategy<Value = u64> {
    prop_oneof![0..64_u64, 0..100_000_u64, any::<u64>()]
}

/// The span of `keys`' places, as a first pass finds it.
fn span_of(keys: &[(u64, u64)]) -> Places {
    let mut span = Places::default();
    for &(place, _) in keys {
        span.add(place);
    }
    span
}

/// The keys `first` to go first that the passes take to free `needed`, in the order the last
/// pass meets them.
fn taken(first: First, keys: &[(u64, u64)], needed: u64) -> Vec<bool> {
    let mut count = Count::new(first, span_of(keys), 1);
    for &(place, bytes) in keys {
        count.add(place, bytes);
    }
    let mut stop = count.stop(needed);
    keys.iter()
        .map(|&(place, bytes)| stop.takes(place, bytes))
        .collect()
}

/// Ranges are equal steps over the span, the first holding its least and the last its most;
/// a place outside it, as one moved between passes, falls in the range nearest.
#[test]
fn ranges_are_equal_steps_over_the_span() {
    let mut span = Places::default();
    span.add(5_000);
    span.add(1_000);
    span.add(1_000 + 1_024 * 7 - 1);
    let ranges = Ranges::over(span);
    assert_eq!(ranges.step, 7);
    assert_eq!(ranges.of(1_000), 0);
    assert_eq!(ranges.of(1_006), 0);
    assert_eq!(ranges.of(1_007), 1);
    assert_eq!(ranges.of(1_000 + 1_024 * 7 - 1), RANGES - 1);
    assert_eq!(ranges.of(0), 0, "before the span");
    assert_eq!(ranges.of(u64::MAX), RANGES - 1, "past it");
    let one = Ranges::over(span_of(&[(42, 1)]));
    assert_eq!((one.of(42), one.step), (0, 1), "a span of one place");
    let whole = Ranges::over(span_of(&[(0, 1), (u64::MAX, 1)]));
    assert_eq!(whole.of(u64::MAX), RANGES - 1);
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(
        std::env::var("PROPTEST_CASES").ok().and_then(|n| n.parse().ok()).unwrap_or(2_048)
    ))]

    /// Against sorting the keys: the passes free what is needed, no more than a key past it,
    /// or every key when that is not enough; and every key taken comes no later than any kept
    /// but by less than a range's step, from whichever end keys go first.
    #[test]
    fn the_passes_take_what_a_sort_would_to_a_step(
        keys in prop::collection::vec((place(), 1..500_u64), 0..200),
        share in 0.0..1.2_f64,
        least in any::<bool>(),
    ) {
        let first = if least { First::Least } else { First::Greatest };
        let all: u64 = keys.iter().map(|(_, bytes)| bytes).sum();
        let needed = (all as f64 * share) as u64;
        let taken = taken(first, &keys, needed);
        let freed: u64 = keys.iter().zip(&taken).filter(|(_, t)| **t).map(|((_, b), _)| b).sum();
        if needed >= all {
            prop_assert!(taken.iter().all(|t| *t), "every key, with too few");
        } else {
            prop_assert!(freed >= needed, "freed {} of {}", freed, needed);
            let most = keys.iter().map(|(_, bytes)| *bytes).max().unwrap_or(0);
            prop_assert!(freed < needed + most, "freed {} for {}", freed, needed);
        }
        let step = Ranges::over(span_of(&keys)).step;
        let places = |want: bool| keys.iter().zip(&taken).filter(move |(_, t)| **t == want)
            .map(|((place, _), _)| *place);
        match first {
            First::Least => if let (Some(last), Some(kept)) = (places(true).max(), places(false).min()) {
                prop_assert!(last < kept.saturating_add(step), "taken up to {}, kept from {}", last, kept);
            },
            First::Greatest => if let (Some(last), Some(kept)) = (places(true).min(), places(false).max()) {
                prop_assert!(last.saturating_add(step) > kept, "taken down to {}, kept to {}", last, kept);
            },
        }
    }

    /// Of the range the bytes end in, the keys taken are spread over the pass, not its first:
    /// in any stretch of the keys of that range, the share taken is the share wanted, but for
    /// a key.
    #[test]
    fn of_the_last_range_keys_go_evenly_over_the_pass(
        keys in 10..2_000_usize,
        wanted in 1..1_000_usize,
    ) {
        let wanted = wanted.min(keys);
        // All in one place, so one range, of a byte each.
        let all: Vec<(u64, u64)> = vec![(7, 1); keys];
        let taken = taken(First::Least, &all, wanted as u64);
        prop_assert_eq!(taken.iter().filter(|t| **t).count(), wanted);
        let half = keys / 2;
        let early = taken[..half].iter().filter(|t| **t).count();
        let even = wanted * half / keys;
        prop_assert!(early.abs_diff(even) <= 1, "{} of the first half, {} even", early, even);
    }
}

#[test]
fn nothing_needed_takes_nothing() {
    let keys = [(0, 10), (5, 10), (u64::MAX, 10)];
    assert_eq!(taken(First::Least, &keys, 0), [false; 3]);
    assert_eq!(taken(First::Greatest, &keys, 0), [false; 3]);
}

#[test]
fn keys_go_from_the_end_asked_for() {
    // Places far apart, each a range of its own.
    let keys = [(100, 10), (100_000, 10), (1_000_000, 10), (1, 10)];
    assert_eq!(taken(First::Least, &keys, 20), [true, false, false, true]);
    assert_eq!(
        taken(First::Greatest, &keys, 20),
        [false, true, true, false]
    );
}

/// Within the range the bytes end in, keys go evenly over the pass until the rest is made
/// up: here half of four, every other from the first.
#[test]
fn of_the_last_range_only_enough_go() {
    let keys = [
        (1_000, 10),
        (1_000, 10),
        (1_000, 10),
        (1_000, 10),
        (5_000_000, 10),
    ];
    assert_eq!(
        taken(First::Least, &keys, 20),
        [true, false, true, false, false]
    );
}

/// Once the keys the passes before counted to go have gone, the last is done: the keys
/// after them take nothing.
#[test]
fn a_pass_is_done_once_the_keys_counted_have_gone() {
    let keys = [(1, 10), (1_000_000, 10), (1_000_000, 10), (100_000_000, 10)];
    let mut count = Count::new(First::Least, span_of(&keys), 1);
    for &(place, bytes) in &keys {
        count.add(place, bytes);
    }
    let mut stop = count.stop(20);
    assert!(stop.takes(1, 10));
    assert!(!stop.done(), "one of the range's still to go");
    assert!(stop.takes(1_000_000, 10), "the first of two, wanting one");
    assert!(stop.done());
    assert!(!stop.takes(1_000_000, 10));
    assert!(!stop.takes(100_000_000, 10));
    let mut all = count.stop(u64::MAX);
    for &(place, bytes) in &keys {
        assert!(all.takes(place, bytes));
    }
    assert!(!all.done(), "with every key to go, never done early");
}

/// What was made lately: a burst counts at once; the bytes a whole span made count through
/// the span after it, unless more is made since; looks further apart than a span are scaled
/// to one; the first look knows of nothing made.
#[test]
fn lately_counts_a_burst_at_once_and_a_quiet_span_once_it_has_passed() {
    let span = HANDLES_PRUNED;
    let mut lately = Lately::default();
    assert_eq!(lately.at(0, 1_000), 0, "the first look");
    assert_eq!(lately.at(span / 10, 1_500), 500, "made since");
    assert_eq!(lately.at(span / 2, 3_000), 2_000);
    assert_eq!(lately.at(span, 3_000), 2_000, "a span made 2,000");
    assert_eq!(
        lately.at(3 * span / 2, 3_100),
        2_000,
        "the last span's, the more"
    );
    assert_eq!(
        lately.at(3 * span / 2, 6_100),
        3_100,
        "a burst since, the more"
    );
    assert_eq!(lately.at(2 * span, 6_100), 3_100, "the span that made it");
    assert_eq!(lately.at(3 * span, 6_100), 0, "a quiet span");
    assert_eq!(
        lately.at(7 * span, 10_100),
        1_000,
        "four spans' worth, scaled to one"
    );
}

#[test]
fn a_count_takes_eight_kib() {
    assert!(size_of::<Count>() <= 8 * 1024 + 64);
}

/// Few keys are read whole; from [`SAMPLED_FROM`] on, one shard in [`STRIDE`], each key read
/// counted that many times over.
#[test]
fn many_keys_are_counted_from_a_sample() {
    assert_eq!(stride(0), 1);
    assert_eq!(stride(SAMPLED_FROM - 1), 1);
    assert_eq!(stride(SAMPLED_FROM), STRIDE);
    let mut span = Places::default();
    span.add(0);
    span.add(1_023);
    let mut count = Count::new(First::Least, span, STRIDE);
    count.add(0, 10);
    count.add(1_023, 10);
    // Each key read stands for 16: of the 16 met in the first range, needing 100 bytes, ten
    // go, and none of the last.
    let mut stop = count.stop(100);
    let taken = (0..STRIDE).filter(|_| stop.takes(0, 10)).count();
    assert_eq!(taken, 10);
    assert!(stop.done());
    assert!(!stop.takes(1_023, 10));
}
