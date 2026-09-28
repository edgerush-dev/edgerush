//! Whether a test that failed was waiting on the code or on the machine.
//!
//! Under heavy load the whole test process sometimes stops for longer than any test waits
//! (one traced at 64 s). A test that then fails says it waited for something that never
//! happened, which is also what a real fault says. A [`Watch`] ticked by a test's wait loop
//! notes every gap between its ticks longer than [`NOTED`], and, where the system says so,
//! how much of the gap the thread spent running: a thread that barely ran was stood still
//! by the machine, one that ran throughout was kept busy by something on it. A failure
//! message carries [`Watch::note`], so that the two are told apart.

use std::time::{Duration, Instant};

/// The shortest gap between ticks worth a mention: a wait loop turns every 20 ms, and the
/// shortest deadlines the tests run on are a few hundred milliseconds.
pub(crate) const NOTED: Duration = Duration::from_millis(250);

/// A gap between two ticks of a wait loop, and how long the thread ran in it, where known.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pause {
    pub(crate) gap: Duration,
    pub(crate) ran: Option<Duration>,
}

impl Pause {
    /// What the pause says about a failure that came after it.
    fn describe(&self) -> String {
        let Pause { gap, ran } = *self;
        match ran {
            Some(ran) if ran * 2 < gap => format!(
                "the test process stood still for {gap:?}, running {ran:?} of it: the machine \
                 stalled it, and a deadline on real time may have passed meanwhile"
            ),
            Some(ran) => format!(
                "the test's thread was kept busy for {gap:?} between turns, running {ran:?} \
                 of it"
            ),
            None => format!("the test went {gap:?} between turns"),
        }
    }
}

/// The longest pause a wait loop has seen, from its ticks.
#[derive(Debug, Default)]
pub(crate) struct Watch {
    last: Option<(Instant, Option<Duration>)>,
    longest: Option<Pause>,
}

impl Watch {
    /// Notes a tick of the wait loop, now.
    pub(crate) fn tick(&mut self) {
        self.tick_at(Instant::now(), ran_so_far());
    }

    /// Notes a tick at `now`, the thread having run `ran` in all by then, where known.
    fn tick_at(&mut self, now: Instant, ran: Option<Duration>) {
        if let Some((then, ran_then)) = self.last {
            let gap = now.saturating_duration_since(then);
            if gap >= NOTED && self.longest.is_none_or(|longest| gap > longest.gap) {
                let ran = ran
                    .zip(ran_then)
                    .map(|(now, then)| now.saturating_sub(then));
                self.longest = Some(Pause { gap, ran });
            }
        }
        self.last = Some((now, ran));
    }

    /// The longest pause seen, if any was long enough to note.
    pub(crate) fn longest(&self) -> Option<Pause> {
        self.longest
    }

    /// The longest pause as a failure message's last words: empty if there was none worth
    /// a mention, else what it says, in brackets, after a space.
    pub(crate) fn note(&self) -> String {
        self.longest
            .map(|pause| format!(" ({})", pause.describe()))
            .unwrap_or_default()
    }
}

/// How long this thread has run on a CPU since it began: the first field of Linux's
/// `/proc/thread-self/schedstat`, in nanoseconds. Elsewhere, unknown.
fn ran_so_far() -> Option<Duration> {
    if !cfg!(target_os = "linux") {
        return None;
    }
    let stat = std::fs::read_to_string("/proc/thread-self/schedstat").ok()?;
    let nanos = stat.split_whitespace().next()?.parse().ok()?;
    Some(Duration::from_nanos(nanos))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: Duration = Duration::from_millis(1);

    /// Ticks at these offsets from a start, the thread having run the paired time in all.
    fn watched(ticks: &[(u64, Option<u64>)]) -> Watch {
        let start = Instant::now();
        let mut watch = Watch::default();
        for &(at, ran) in ticks {
            watch.tick_at(start + MS * at as u32, ran.map(|ran| MS * ran as u32));
        }
        watch
    }

    #[test]
    fn gaps_shorter_than_noted_are_not_mentioned() {
        let watch = watched(&[(0, Some(0)), (20, Some(20)), (269, Some(30))]);
        assert_eq!(watch.longest(), None);
        assert_eq!(watch.note(), "");
    }

    #[test]
    fn a_gap_the_thread_barely_ran_in_is_the_machine_standing_still() {
        let watch = watched(&[(0, Some(100)), (20, Some(110)), (2_020, Some(130))]);
        assert_eq!(
            watch.longest(),
            Some(Pause {
                gap: 2_000 * MS,
                ran: Some(20 * MS)
            })
        );
        assert!(
            watch.note().contains("stood still for 2s"),
            "{}",
            watch.note()
        );
    }

    #[test]
    fn a_gap_the_thread_ran_through_is_it_kept_busy() {
        let watch = watched(&[(0, Some(0)), (1_000, Some(900))]);
        assert!(
            watch.note().contains("kept busy for 1s"),
            "{}",
            watch.note()
        );
    }

    #[test]
    fn where_running_time_is_unknown_the_gap_alone_is_said() {
        let watch = watched(&[(0, None), (500, None)]);
        assert_eq!(
            watch.note(),
            " (the test went 500ms between turns)",
            "{}",
            watch.note()
        );
    }

    #[test]
    fn the_longest_gap_is_the_one_kept() {
        let watch = watched(&[
            (0, Some(0)),
            (300, Some(10)),
            (1_300, Some(20)),
            (1_700, Some(30)),
        ]);
        assert_eq!(watch.longest().map(|pause| pause.gap), Some(1_000 * MS));
    }

    /// A thread put to sleep is not running: on Linux the watch says the process stood
    /// still, and elsewhere how long the gap was.
    #[test]
    fn a_real_sleep_is_seen_as_standing_still() {
        let mut watch = Watch::default();
        watch.tick();
        std::thread::sleep(Duration::from_millis(400));
        watch.tick();
        let note = watch.note();
        if cfg!(target_os = "linux") {
            assert!(note.contains("stood still"), "{note}");
        } else {
            assert!(note.contains("between turns"), "{note}");
        }
    }
}
