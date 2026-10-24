use super::*;
use proptest::prelude::*;

fn nz(n: u64) -> NonZeroU64 {
    NonZeroU64::new(n).unwrap()
}

/// Arrival times, non-decreasing, with many requests landing together.
fn arrivals() -> impl Strategy<Value = Vec<u64>> {
    prop::collection::vec(
        prop_oneof![3 => Just(0u64), 2 => 1u64..50, 1 => 50u64..3000],
        1..120,
    )
    .prop_map(|gaps| {
        gaps.iter()
            .scan(0u64, |at, gap| {
                *at += gap;
                Some(*at)
            })
            .collect()
    })
}

fn run(model: &mut impl Model, times: &[u64]) -> Vec<bool> {
    times.iter().map(|&at| model.decide(at)).collect()
}

proptest! {
    /// A request is admitted exactly when fewer than `limit` were admitted in the
    /// `window` before it.
    #[test]
    fn rolling_window_is_its_definition(times in arrivals(), limit in 1u64..8, window in 1u64..2000) {
        let admitted = run(&mut RollingWindow::new(nz(limit), nz(window)), &times);
        for (i, &at) in times.iter().enumerate() {
            let inside = (0..i).filter(|&j| admitted[j] && at - times[j] < window).count() as u64;
            prop_assert_eq!(admitted[i], inside < limit, "request {} at {}", i, at);
        }
    }

    /// A request is admitted exactly when fewer than `limit` were admitted in its window
    /// of the clock.
    #[test]
    fn clock_window_is_its_definition(times in arrivals(), limit in 1u64..8, window in 1u64..2000) {
        let admitted = run(&mut FixedWindow::new(nz(limit), nz(window), Alignment::Clock), &times);
        for (i, &at) in times.iter().enumerate() {
            let inside = (0..i)
                .filter(|&j| admitted[j] && times[j] / window == at / window)
                .count() as u64;
            prop_assert_eq!(admitted[i], inside < limit, "request {} at {}", i, at);
        }
    }

    /// A window opens with the first request that finds none open and lasts `window`; a
    /// request is admitted exactly when fewer than `limit` were admitted in it.
    #[test]
    fn first_request_window_is_its_definition(times in arrivals(), limit in 1u64..8, window in 1u64..2000) {
        let admitted = run(&mut FixedWindow::new(nz(limit), nz(window), Alignment::FirstRequest), &times);
        let mut opened = times[0];
        for (i, &at) in times.iter().enumerate() {
            if at - opened >= window {
                opened = at;
            }
            let inside = (0..i)
                .filter(|&j| admitted[j] && times[j] >= opened)
                .count() as u64;
            prop_assert_eq!(admitted[i], inside < limit, "request {} at {}", i, at);
        }
    }

    /// GCRA and the counting bucket are one contract written two ways.
    #[test]
    fn token_bucket_is_a_counting_bucket(
        times in arrivals(), limit in 1u64..8, window in 1u64..2000, capacity in 1u64..12,
    ) {
        let gcra = run(&mut TokenBucket::new(nz(limit), nz(window), nz(capacity)), &times);
        let counting = run(&mut CountingBucket::new(nz(limit), nz(window), nz(capacity)), &times);
        prop_assert_eq!(gcra, counting);
    }

    /// In any span from one admission to another, at most the rate over the span plus the
    /// bucket are admitted.
    #[test]
    fn token_bucket_admits_at_most_rate_plus_capacity(
        times in arrivals(), limit in 1u64..8, window in 1u64..2000, capacity in 1u64..12,
    ) {
        let admitted = run(&mut TokenBucket::new(nz(limit), nz(window), nz(capacity)), &times);
        let at: Vec<u64> = times.iter().zip(&admitted).filter(|(_, a)| **a).map(|(t, _)| *t).collect();
        for first in 0..at.len() {
            for last in first..at.len() {
                let count = u128::try_from(last - first + 1).unwrap();
                let span = u128::from(at[last] - at[first]);
                prop_assert!(
                    count * u128::from(window) <= u128::from(limit) * span + u128::from(capacity) * u128::from(window),
                    "{} admitted in {} ns", count, span,
                );
            }
        }
    }

    /// Traffic with at most `limit` in any `window` is never refused by a bucket that holds
    /// one window's worth: such a bucket shaves nothing an exact rolling window admits.
    #[test]
    fn a_window_sized_bucket_admits_all_a_rolling_window_admits(
        times in arrivals(), limit in 1u64..8, window in 1u64..2000,
    ) {
        let admitted = run(&mut RollingWindow::new(nz(limit), nz(window)), &times);
        let conforming: Vec<u64> = times.iter().zip(&admitted).filter(|(_, a)| **a).map(|(t, _)| *t).collect();
        let bucket = run(&mut TokenBucket::new(nz(limit), nz(window), nz(limit)), &conforming);
        prop_assert!(bucket.iter().all(|a| *a), "{:?} refused some of {:?}", bucket, conforming);
    }
}

#[test]
fn a_new_token_bucket_admits_its_capacity_at_once() {
    let mut bucket = TokenBucket::new(nz(100), nz(1_000_000_000), nz(20));
    let at_once = (0..30).filter(|_| bucket.decide(0)).count();
    assert_eq!(at_once, 20);
    assert!(!bucket.decide(9_999_999), "a request's interval is 10 ms");
    assert!(bucket.decide(10_000_000));
}

#[test]
fn a_time_that_goes_back_is_taken_as_the_last() {
    let mut window = RollingWindow::new(nz(1), nz(100));
    assert!(window.decide(1_000));
    assert!(
        !window.decide(10),
        "still inside the window of the request at 1,000"
    );
    assert!(window.decide(1_100));
}

/// Ten admitted at the start of one window count as five halfway through the next, though
/// a whole window has passed: the approximation refuses what the exact window admits.
#[test]
fn the_approximation_refuses_what_a_rolling_window_admits() {
    let (limit, window) = (nz(10), nz(1_000));
    let mut approximate = ApproximateRollingWindow::new(limit, window);
    let mut exact = RollingWindow::new(limit, window);
    for _ in 0..10 {
        assert!(approximate.decide(0));
        assert!(exact.decide(0));
    }
    let approximate = (0..10).filter(|_| approximate.decide(1_500)).count();
    let exact = (0..10).filter(|_| exact.decide(1_500)).count();
    assert_eq!((approximate, exact), (5, 10));
}

/// Ten admitted at the end of one window count as five halfway through the next, though
/// they are still inside the last window: the approximation admits what the exact window
/// refuses.
#[test]
fn the_approximation_admits_what_a_rolling_window_refuses() {
    let (limit, window) = (nz(10), nz(1_000));
    let mut approximate = ApproximateRollingWindow::new(limit, window);
    let mut exact = RollingWindow::new(limit, window);
    for _ in 0..10 {
        assert!(approximate.decide(990));
        assert!(exact.decide(990));
    }
    let approximate = (0..10).filter(|_| approximate.decide(1_500)).count();
    let exact = (0..10).filter(|_| exact.decide(1_500)).count();
    assert_eq!((approximate, exact), (5, 0));
}

/// A window's worth at the end of one window and another at the start of the next.
#[test]
fn a_clock_window_admits_twice_its_limit_across_a_boundary() {
    let mut window = FixedWindow::new(nz(100), nz(1_000), Alignment::Clock);
    let end = (0..100).filter(|_| window.decide(999)).count();
    let start = (0..100).filter(|_| window.decide(1_000)).count();
    assert_eq!(end + start, 200);
}

/// Every model, behind one type, so that one property can be checked against all of them.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Any {
    Fixed(FixedWindow),
    Rolling(RollingWindow),
    Approximate(ApproximateRollingWindow),
    Bucket(TokenBucket),
    Counting(CountingBucket),
}

macro_rules! each {
    ($any:expr, $model:ident => $body:expr) => {
        match $any {
            Any::Fixed($model) => $body,
            Any::Rolling($model) => $body,
            Any::Approximate($model) => $body,
            Any::Bucket($model) => $body,
            Any::Counting($model) => $body,
        }
    };
}

impl Model for Any {
    fn remaining(&self, now: u64) -> u64 {
        each!(self, model => model.remaining(now))
    }
    fn charge(&mut self, now: u64) -> Charge {
        each!(self, model => model.charge(now))
    }
    fn release(&mut self, charge: Charge) {
        each!(self, model => model.release(charge))
    }
    fn retry_at(&self, now: u64) -> Option<u64> {
        each!(self, model => model.retry_at(now))
    }
    fn forgettable_at(&self) -> u64 {
        each!(self, model => model.forgettable_at())
    }
}

/// The kinds of model, by number: 0 and 1 fixed windows on the clock and from the first
/// request, 2 rolling, 3 approximate, 4 the bucket, 5 the counting bucket.
const KINDS: u8 = 6;

fn make(kind: u8, limit: u64, window: u64, capacity: u64) -> Any {
    let (limit, window, capacity) = (nz(limit), nz(window), nz(capacity));
    match kind {
        0 => Any::Fixed(FixedWindow::new(limit, window, Alignment::Clock)),
        1 => Any::Fixed(FixedWindow::new(limit, window, Alignment::FirstRequest)),
        2 => Any::Rolling(RollingWindow::new(limit, window)),
        3 => Any::Approximate(ApproximateRollingWindow::new(limit, window)),
        4 => Any::Bucket(TokenBucket::new(limit, window, capacity)),
        _ => Any::Counting(CountingBucket::new(limit, window, capacity)),
    }
}

/// A new model of the same kind and settings.
fn new_like(model: &Any) -> Any {
    match model {
        Any::Fixed(m) => Any::Fixed(FixedWindow {
            start: None,
            count: 0,
            last: 0,
            ..m.clone()
        }),
        Any::Rolling(m) => Any::Rolling(RollingWindow {
            admitted: VecDeque::new(),
            last: 0,
            ..m.clone()
        }),
        Any::Approximate(m) => Any::Approximate(ApproximateRollingWindow {
            index: None,
            previous: 0,
            current: 0,
            last: 0,
            ..m.clone()
        }),
        Any::Bucket(m) => Any::Bucket(TokenBucket {
            arrival: None,
            last: 0,
            ..m.clone()
        }),
        Any::Counting(m) => Any::Counting(CountingBucket {
            tokens: m.capacity,
            owed: 0,
            last: 0,
            ..m.clone()
        }),
    }
}

/// A model of any kind with a history behind it, and the last time in that history.
fn used() -> impl Strategy<Value = (Any, u64)> {
    (0..KINDS, 1u64..8, 1u64..2000, 1u64..12, arrivals()).prop_map(
        |(kind, limit, window, capacity, times)| {
            let mut model = make(kind, limit, window, capacity);
            run(&mut model, &times);
            (model, *times.last().unwrap())
        },
    )
}

/// `times`, moved on to start at `from`.
fn from(from: u64, times: Vec<u64>) -> Vec<u64> {
    times.into_iter().map(|at| at + from).collect()
}

/// Two copies of `model`: one that took `before`, reserved a request `gap` later, took
/// `between` and released the reservation; and one that took `before` and only what the
/// first admitted of `between`, charged without asking. The first is `None` if the
/// reservation found no room. Also returns the latest time either saw.
fn released(model: Any, before: &[u64], gap: u64, between: Vec<u64>) -> (Option<Any>, Any, u64) {
    let (mut with, mut without) = (model.clone(), model);
    run(&mut with, before);
    run(&mut without, before);
    let reserved = before.last().unwrap() + gap;
    if !with.admits(reserved) {
        return (None, without, reserved);
    }
    let receipt = with.charge(reserved);
    let between = from(reserved, between);
    for (&at, admitted) in between.iter().zip(run(&mut with, &between)) {
        if admitted {
            without.charge(at);
        }
    }
    with.release(receipt);
    (Some(with), without, *between.last().unwrap())
}

/// A token bucket with a history behind it, owing up to `owed` more charged at its end,
/// and the last time in that history.
fn owing() -> impl Strategy<Value = (TokenBucket, u64)> {
    owing_with(1..8)
}

/// [`owing`], refilling `limits` requests a window.
fn owing_with(limits: std::ops::Range<u64>) -> impl Strategy<Value = (TokenBucket, u64)> {
    (limits, 1u64..2000, 1u64..12, arrivals(), 0u64..20).prop_map(
        |(limit, window, capacity, times, owed)| {
            let mut bucket = TokenBucket::new(nz(limit), nz(window), nz(capacity));
            run(&mut bucket, &times);
            let last = *times.last().unwrap();
            for _ in 0..owed {
                bucket.charge(last);
            }
            (bucket, last)
        },
    )
}

proptest! {
    /// What remains within a wait is how many charged one after another each find the
    /// bucket admitting again within that wait, and of at most so many, no more than them.
    #[test]
    fn remaining_within_a_wait_is_how_many_owe_no_longer(
        (mut bucket, last) in owing(), later in 0u64..5000, wait in 0u64..20_000, most in 0u64..30
    ) {
        let now = last + later;
        let within = |bucket: &TokenBucket| bucket.retry_at(now).is_none_or(|at| at - now <= wait);
        let remaining = bucket.remaining_within(now, wait, u64::MAX);
        prop_assert_eq!(bucket.remaining_within(now, wait, most), remaining.min(most));
        for i in 0..remaining {
            prop_assert!(within(&bucket), "charge {} of {} not within the wait", i + 1, remaining);
            bucket.charge(now);
        }
        prop_assert!(!within(&bucket), "one more than the {} within the wait", remaining);
    }

    /// Charging so many at once is charging them one by one.
    #[test]
    fn charging_many_at_once_is_charging_them_one_by_one(
        (bucket, last) in owing(), later in 0u64..5000, count in 0u64..30
    ) {
        let now = last + later;
        let (mut at_once, mut one_by_one) = (bucket.clone(), bucket);
        at_once.charge_many(now, count);
        for _ in 0..count {
            one_by_one.charge(now);
        }
        prop_assert_eq!(at_once, one_by_one);
    }

    /// Charged within a wait as of now, a bucket charges what remains within it.
    #[test]
    fn charging_within_a_wait_now_charges_what_remains_within_it(
        (bucket, last) in owing(), later in 0u64..5000, wait in 0u64..20_000, most in 0u64..30
    ) {
        let now = last + later;
        let (mut within, mut apart) = (bucket.clone(), bucket);
        let remaining = apart.remaining_within(now, wait, most);
        apart.charge_many(now, remaining);
        prop_assert_eq!(within.charge_within(now, now, wait, most), remaining);
        prop_assert_eq!(within, apart);
    }

    /// Charged as of a time no earlier than its last, with room to owe all of them, a bucket
    /// is charged as it would have been then.
    #[test]
    fn charging_within_as_of_an_earlier_time_is_charging_then(
        (bucket, last) in owing(), then in 0u64..5000, later in 0u64..5000, count in 0u64..30
    ) {
        let (at, now) = (last + then, last + then + later);
        let (mut within, mut then) = (bucket.clone(), bucket);
        then.charge_many(at, count);
        prop_assert_eq!(within.charge_within(at, now, u64::MAX / 4, count), count);
        prop_assert_eq!(within.behind(now), then.behind(now));
    }

    /// Charging as of any earlier time, before what the bucket was last charged at too,
    /// never frees room; and, all of it owed, never takes more than charging now would.
    /// (With a wait, what is forgiven past it is rounded to a request from each time, and
    /// either can leave the bucket under an interval further behind.)
    #[test]
    fn charging_in_the_past_frees_nothing_and_takes_no_more_than_now(
        (bucket, last) in owing(), back in 0u64..5000, later in 0u64..5000,
        wait in 0u64..20_000, most in 0u64..30
    ) {
        let now = last + later;
        let at = now.saturating_sub(back);
        let mut waited = bucket.clone();
        waited.charge_within(at, now, wait, most);
        let ample = u64::MAX / 4;
        let (mut past, mut present) = (bucket.clone(), bucket.clone());
        past.charge_within(at, now, ample, most);
        present.charge_within(now, now, ample, most);
        for t in [now, now + 1, now + 500, now + 5000] {
            prop_assert!(waited.behind(t) >= bucket.behind(t), "freed room at {}", t);
            prop_assert!(past.behind(t) <= present.behind(t), "took more than now at {}", t);
        }
    }

    /// Charges taken out of order leave a bucket at least as far behind as in order.
    #[test]
    fn charges_out_of_order_leave_a_bucket_no_emptier(
        (bucket, last) in owing(), first in 0u64..5000, gap in 0u64..5000, later in 0u64..5000,
        (a, b) in (0u64..10, 0u64..10)
    ) {
        let (early, late) = (last + first, last + first + gap);
        let now = late + later;
        let ample = u64::MAX / 4;
        let (mut in_order, mut out_of_order) = (bucket.clone(), bucket);
        in_order.charge_within(early, now, ample, a);
        in_order.charge_within(late, now, ample, b);
        out_of_order.charge_within(late, now, ample, b);
        out_of_order.charge_within(early, now, ample, a);
        prop_assert!(out_of_order.behind(now) >= in_order.behind(now));
    }

    /// Charged within a wait in one step is charged one at a time.
    #[test]
    fn charging_within_at_once_is_charging_one_at_a_time(
        (bucket, last) in owing(), back in 0u64..5000, later in 0u64..5000,
        wait in 0u64..20_000, most in 0u64..30
    ) {
        let now = last + later;
        let at = now.saturating_sub(back);
        let (mut at_once, mut one_at_a_time) = (bucket.clone(), bucket);
        let charged = at_once.charge_within(at, now, wait, most);
        let mut counted = 0;
        while counted < most && one_at_a_time.charge_within(at, now, wait, 1) == 1 {
            counted += 1;
        }
        prop_assert_eq!(charged, counted);
        prop_assert_eq!(at_once, one_at_a_time);
    }

    /// Rebased as of now, a bucket is set behind and charged on top, as now.
    #[test]
    fn rebasing_now_is_setting_behind_and_charging_on_top(
        (bucket, last) in owing(), later in 0u64..5000, behind in 0u128..100_000,
        extra in 0u64..30
    ) {
        let now = last + later;
        let (mut rebased, mut apart) = (bucket.clone(), bucket);
        rebased.rebase(now, behind, extra);
        apart.set_behind(now, behind);
        apart.charge_many(now, extra);
        prop_assert_eq!(rebased, apart);
    }

    /// Rebased as of an earlier time, a bucket is as far behind then as it is set, its
    /// extra charged on top, and refills from then.
    #[test]
    fn rebasing_as_of_an_earlier_time_refills_from_then(
        (mut bucket, last) in owing(), back in 0u64..5000, later in 0u64..5000,
        behind in 0u128..100_000, extra in 0u64..30
    ) {
        let now = last + later;
        let at = now.saturating_sub(back);
        bucket.rebase(at, behind, extra);
        let set = behind + u128::from(extra) * bucket.window;
        for t in [now, now + 1, now + 500, now + 5000] {
            let refilled = u128::from(t - at) * bucket.limit;
            prop_assert_eq!(bucket.behind(t), set.saturating_sub(refilled));
        }
    }

    /// What remains between two times is the most of so many requests arriving between them
    /// that a bucket admits one by one, each as early as it would admit it; and charged
    /// within no wait as of the first, they leave it as they would. One request a window, so
    /// that a time to admit again is a whole nanosecond: otherwise each wait one by one is
    /// rounded up to one, and leaves the bucket that much further behind.
    #[test]
    fn remaining_between_is_the_most_admitted_arriving_between(
        (bucket, last) in owing_with(1..2), from in 0u64..5000, span in 0u64..5000, most in 0u64..30
    ) {
        let (first, end) = (last + from, last + from + span);
        let mut one_by_one = bucket.clone();
        let (mut t, mut admitted) = (first, 0);
        while admitted < most && t <= end {
            if one_by_one.decide(t) {
                admitted += 1;
            } else {
                match one_by_one.retry_at(t) {
                    Some(at) => t = at,
                    None => break,
                }
            }
        }
        prop_assert_eq!(bucket.remaining_between(first, end, most), admitted);
        let mut charged = bucket;
        prop_assert_eq!(charged.charge_within(first, end, 0, admitted), admitted);
        for t in [end, end + 1, end + 500, end + 5000] {
            prop_assert_eq!(charged.behind(t), one_by_one.behind(t));
        }
    }

    /// Requests arriving at one instant are decided as remaining then; earlier, before the
    /// bucket's last charge too, never more of them, as it has refilled less.
    #[test]
    fn remaining_between_is_no_more_for_earlier_times(
        (bucket, last) in owing(), from in 0u64..5000, span in 0u64..5000,
        earlier in 0u64..10_000, most in 0u64..30
    ) {
        let now = last + from;
        prop_assert_eq!(bucket.remaining_between(now, now, most), bucket.remaining_within(now, 0, most));
        let (first, end) = (now, now + span);
        let (back_first, back_end) = (first.saturating_sub(earlier), end.saturating_sub(earlier));
        prop_assert!(
            bucket.remaining_between(back_first, back_end, most)
                <= bucket.remaining_between(first, end, most)
        );
    }
}

/// Times out of order, the last before the first, are taken as requests at the last.
#[test]
fn remaining_between_takes_a_first_after_the_last_as_the_last() {
    let mut bucket = TokenBucket::new(nz(1), nz(10), nz(2));
    bucket.charge_many(100, 2);
    assert_eq!(
        bucket.remaining_between(130, 115, 5),
        bucket.remaining_between(115, 115, 5)
    );
    assert_eq!(bucket.remaining_between(115, 115, 5), 1);
}

/// Counts of any size are taken in one step, and terms of any size without overflowing.
#[test]
fn a_bucket_takes_any_count_at_once() {
    let mut bucket = TokenBucket::new(nz(1), nz(1), nz(1));
    assert_eq!(bucket.remaining_within(0, u64::MAX, u64::MAX), u64::MAX);
    bucket.charge_many(0, u64::MAX);
    assert_eq!(bucket.retry_at(0), Some(u64::MAX));
    let most = nz(u64::MAX);
    let mut bucket = TokenBucket::new(most, most, most);
    let _ = bucket.remaining_within(u64::MAX, u64::MAX, u64::MAX);
    bucket.charge_many(0, u64::MAX);
    bucket.charge_many(u64::MAX, u64::MAX);
    let _ = bucket.remaining_within(u64::MAX, u64::MAX, u64::MAX);
}

proptest! {
    /// Asking a model anything changes nothing in it.
    #[test]
    fn questions_change_nothing((model, last) in used(), later in 0u64..5000) {
        let now = last + later;
        let before = model.clone();
        let _ = (model.admits(now), model.remaining(now), model.retry_at(now), model.forgettable_at());
        prop_assert_eq!(model, before);
    }

    /// What remains is how many are admitted one after another at that moment.
    #[test]
    fn remaining_is_how_many_are_admitted((mut model, last) in used(), later in 0u64..5000) {
        let now = last + later;
        let remaining = model.remaining(now);
        for i in 0..remaining {
            prop_assert!(model.decide(now), "request {} of {} remaining refused", i + 1, remaining);
        }
        prop_assert!(!model.decide(now), "one more than the {} remaining admitted", remaining);
    }

    /// A refused request would be admitted at the time it is told to retry, and not a
    /// nanosecond before.
    #[test]
    fn retry_at_is_the_first_moment_with_room((model, last) in used(), later in 0u64..5000) {
        let now = last + later;
        match model.retry_at(now) {
            None => prop_assert!(model.admits(now)),
            Some(at) => {
                prop_assert!(at > now);
                prop_assert!(!model.admits(at - 1), "room before {}", at);
                prop_assert!(model.admits(at), "no room at {}", at);
            }
        }
    }

    /// Once forgettable, a key decides as a new one would.
    #[test]
    fn a_forgettable_key_is_as_new((mut model, last) in used(), rest in arrivals()) {
        let rest = from(model.forgettable_at().max(last), rest);
        let mut fresh = new_like(&model);
        prop_assert_eq!(run(&mut model, &rest), run(&mut fresh, &rest));
    }

    /// Several rules are charged together or not at all.
    #[test]
    fn rules_are_charged_all_or_nothing(
        (first, last) in used(), (second, other) in used(), later in 0u64..5000,
    ) {
        let now = last.max(other) + later;
        let (mut a, mut b) = (first.clone(), second.clone());
        let admitted = decide_all(&mut [&mut a, &mut b], now);
        prop_assert_eq!(admitted, first.admits(now) && second.admits(now));
        let (mut expected_a, mut expected_b) = (first, second);
        if admitted {
            expected_a.charge(now);
            expected_b.charge(now);
        }
        prop_assert_eq!((a, b), (expected_a, expected_b));
    }

    /// The bucket written two ways stays one contract through charges made without room
    /// and releases too.
    #[test]
    fn the_buckets_agree_on_everything(
        limit in 1u64..8, window in 1u64..2000, capacity in 1u64..12,
        steps in prop::collection::vec((0u64..3000, 0u8..4), 1..100),
    ) {
        let mut gcra = TokenBucket::new(nz(limit), nz(window), nz(capacity));
        let mut counting = CountingBucket::new(nz(limit), nz(window), nz(capacity));
        let (mut now, mut receipts) = (0, Vec::new());
        for (gap, step) in steps {
            now += gap;
            match step {
                0 => prop_assert_eq!(gcra.decide(now), counting.decide(now)),
                1 => receipts.push((gcra.charge(now), counting.charge(now))),
                2 => {
                    if let Some((a, b)) = receipts.pop() {
                        gcra.release(a);
                        counting.release(b);
                    }
                }
                _ => {}
            }
            prop_assert_eq!(gcra.remaining(now), counting.remaining(now));
            prop_assert_eq!(gcra.retry_at(now), counting.retry_at(now));
            // Any time already past says the same: forgettable now.
            prop_assert_eq!(gcra.forgettable_at().max(now), counting.forgettable_at().max(now));
        }
    }

    /// Releasing a reservation leaves a window on the clock or a rolling window as if it
    /// had never been made, whatever came between.
    #[test]
    fn a_released_charge_is_as_if_never_made(
        kind in prop::sample::select(vec![0u8, 2, 3]), limit in 1u64..8, window in 1u64..2000,
        before in arrivals(), gap in 0u64..3000, between in arrivals(), rest in arrivals(),
    ) {
        let (with, mut without, latest) = released(make(kind, limit, window, 1), &before, gap, between);
        prop_assume!(with.is_some());
        let mut with = with.unwrap();
        prop_assert_eq!(with.remaining(latest), without.remaining(latest));
        prop_assert_eq!(with.retry_at(latest), without.retry_at(latest));
        let rest = from(latest, rest);
        prop_assert_eq!(run(&mut with, &rest), run(&mut without, &rest));
    }

    /// A released bucket charge gives back at most one request more than never charging.
    #[test]
    fn a_released_bucket_charge_gives_back_at_most_one_more(
        limit in 1u64..8, window in 1u64..2000, capacity in 1u64..12,
        before in arrivals(), gap in 0u64..3000, between in arrivals(),
    ) {
        let (with, without, now) = released(make(4, limit, window, capacity), &before, gap, between);
        prop_assume!(with.is_some());
        let (got, owed) = (with.unwrap().remaining(now), without.remaining(now));
        prop_assert!(owed <= got && got <= owed + 1, "{} remaining after the release, {} without the charge", got, owed);
    }
}

/// A reservation released once its window has become the one before no longer counts in
/// the share carried over.
#[test]
fn a_released_charge_from_the_window_before_no_longer_counts() {
    let (limit, window) = (nz(2), nz(1_000));
    let mut with = ApproximateRollingWindow::new(limit, window);
    let receipt = with.charge(900);
    assert!(with.decide(1_000));
    with.release(receipt);
    let mut without = ApproximateRollingWindow::new(limit, window);
    assert!(without.decide(1_000));
    assert!(
        without.decide(1_000),
        "nothing carried over from the window before"
    );
    assert!(
        with.decide(1_000),
        "the released charge is not carried over either"
    );
}

/// A released reservation that opened a window leaves it open: what follows counts in
/// it, where without the reservation the next request opens a window of its own.
#[test]
fn a_released_charge_leaves_its_first_request_window_open() {
    let mut with = FixedWindow::new(nz(1), nz(1_000), Alignment::FirstRequest);
    let receipt = with.charge(0);
    with.release(receipt);
    let mut without = FixedWindow::new(nz(1), nz(1_000), Alignment::FirstRequest);
    assert!(with.decide(900) && without.decide(900));
    assert!(with.decide(1_000), "the window opened at 0 has ended");
    assert!(
        !without.decide(1_000),
        "the window opened at 900 runs to 1,900"
    );
}

/// Something done to a token bucket, `gap` nanoseconds after the last, or before it: a
/// bucket takes an earlier time as the latest it was given.
#[derive(Clone, Debug)]
enum Use {
    Charge,
    Release,
    SetBehind(u128),
}

fn uses() -> impl Strategy<Value = Vec<(i64, Use)>> {
    let gap = prop_oneof![2 => Just(0i64), 2 => 1i64..50, 1 => 50i64..3000, 1 => -200i64..0];
    let what = prop_oneof![
        6 => Just(Use::Charge),
        1 => Just(Use::Release),
        1 => (0u128..20_000).prop_map(Use::SetBehind),
    ];
    prop::collection::vec((gap, what), 0..80)
}

proptest! {
    /// A bucket kept as its state, and made again from it under its terms for every use,
    /// answers every question as one kept whole.
    #[test]
    fn a_bucket_kept_as_its_state_is_the_bucket(
        uses in uses(), limit in 1u64..8, window in 1u64..2000, capacity in 1u64..8
    ) {
        let (limit, window, capacity) = (nz(limit), nz(window), nz(capacity));
        let mut whole = TokenBucket::new(limit, window, capacity);
        let mut state = whole.state();
        let mut now: u64 = 0;
        for (gap, what) in uses {
            now = now.saturating_add_signed(gap);
            let mut kept = TokenBucket::from_state(limit, window, capacity, state);
            match what {
                Use::Charge => {
                    whole.charge(now);
                    kept.charge(now);
                }
                Use::Release => {
                    let at = Charge { at: now };
                    whole.release(at);
                    kept.release(at);
                }
                Use::SetBehind(behind) => {
                    whole.set_behind(now, behind);
                    kept.set_behind(now, behind);
                }
            }
            state = kept.state();
            let kept = TokenBucket::from_state(limit, window, capacity, state);
            for at in [now, now + 1, now + 500, now + 5000] {
                prop_assert_eq!(kept.admits(at), whole.admits(at));
                prop_assert_eq!(kept.remaining(at), whole.remaining(at));
                prop_assert_eq!(kept.retry_at(at), whole.retry_at(at));
                prop_assert_eq!(kept.behind(at), whole.behind(at));
            }
            prop_assert_eq!(kept.forgettable_at(), whole.forgettable_at());
        }
    }
}
