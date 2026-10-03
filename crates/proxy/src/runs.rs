//! Small frames of a body copied together into runs, for what keeps a body's frames for
//! later: the retry recording and a mirror's queue ([03 §6](../../docs/03-data-plane.md)).
//!
//! A list of the frames themselves holds an entry for each, however small, and a client
//! chooses how small they are: an HTTP/1 body of one-byte chunks is a frame a byte, and 64 KiB
//! of them is 65,536 entries. So frames under [`COPIED_BELOW`] that come one after another are
//! copied onto a run, while a larger one is kept as it is, the same buffer as the one sent —
//! and so is a small one with no other small one after it, so that a body of one small frame,
//! and a mirror that takes each frame as it comes, cost no copy. Every run but the last is
//! full, so what they hold is the bytes kept and at most a run's room more.

/// The largest frame copied onto a run rather than kept as it is: the size below which a
/// frame is copied out of a read block rather than cut from it
/// ([13 §7](../../docs/13-http1-upstream.md)).
pub(crate) const COPIED_BELOW: usize = 4 * 1024;

/// The most a run holds: a read block's worth.
pub(crate) const RUN: usize = 16 * 1024;

/// Copies as much of `data` onto `run` as fits within [`RUN`], and returns the rest. The
/// run's room grows by doubling, never past [`RUN`], so that a run of one-byte frames is not
/// made again for every byte; a copy is not paid for by anything it was copied from, so
/// `pay` is asked for each growth, in bytes, before it is made
/// ([14 §8](../../docs/14-downstream-server.md)).
///
/// # Errors
///
/// What `pay` refuses with; the run is left as it was.
pub(crate) fn fill<'a, E>(
    run: &mut Vec<u8>,
    data: &'a [u8],
    pay: impl FnOnce(usize) -> Result<(), E>,
) -> Result<&'a [u8], E> {
    let take = data.len().min(RUN.saturating_sub(run.len()));
    let (now, rest) = data.split_at(take);
    let wanted = run.len() + take;
    if wanted > run.capacity() {
        let room = wanted.next_power_of_two().min(RUN);
        pay(room - run.capacity())?;
        run.reserve_exact(room - run.len());
    }
    run.extend_from_slice(now);
    Ok(rest)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Pays for anything.
    fn free(_more: usize) -> Result<(), ()> {
        Ok(())
    }

    #[test]
    fn a_run_takes_what_fits_and_gives_back_the_rest() {
        let mut run = Vec::new();
        assert_eq!(fill(&mut run, b"abc", free), Ok(&b""[..]));
        assert_eq!(run, b"abc");
        let big = vec![b'x'; RUN];
        let rest = fill(&mut run, &big, free).unwrap();
        assert_eq!(run.len(), RUN);
        assert_eq!(rest.len(), 3);
        assert_eq!(
            fill(&mut run, b"more", free),
            Ok(&b"more"[..]),
            "a full run took more"
        );
    }

    /// Every growth is paid for before it is made, all of the run's room in the end, and a
    /// growth refused leaves the run as it was.
    #[test]
    fn a_run_is_paid_for_before_it_grows_and_a_refusal_changes_nothing() {
        let mut run = Vec::new();
        let mut paid = 0;
        for _ in 0..100 {
            let rest = fill(&mut run, b"x", |more| {
                paid += more;
                Ok::<_, ()>(())
            });
            assert_eq!(rest, Ok(&b""[..]));
        }
        assert_eq!(paid, run.capacity());
        let before = (run.clone(), run.capacity());
        assert_eq!(
            fill(&mut run, &[b'y'; 100], |_| Err("no room")),
            Err("no room")
        );
        assert_eq!((run.clone(), run.capacity()), before);
    }

    #[test]
    fn a_run_of_bytes_grows_by_doubling_and_never_past_its_most() {
        let mut run = Vec::new();
        let mut grown = 0;
        let mut room = run.capacity();
        for _ in 0..RUN {
            assert_eq!(fill(&mut run, b"x", free), Ok(&b""[..]));
            if run.capacity() != room {
                grown += 1;
                room = run.capacity();
            }
        }
        assert_eq!(run.len(), RUN);
        assert!(run.capacity() <= RUN, "room {} past {RUN}", run.capacity());
        assert!(grown <= 16, "made again {grown} times");
    }

    proptest! {
        /// However the frames come, the runs hold every byte in order, every run but the
        /// last is full, and none has room past its most.
        #[test]
        fn runs_hold_every_byte_in_order(
            frames in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..COPIED_BELOW), 0..64),
        ) {
            let mut runs = vec![Vec::new()];
            for frame in &frames {
                let mut rest = frame.as_slice();
                while let Some(run) = runs.last_mut() {
                    rest = fill(run, rest, free).unwrap();
                    if rest.is_empty() {
                        break;
                    }
                    runs.push(Vec::new());
                }
            }
            let whole: Vec<u8> = frames.concat();
            prop_assert_eq!(runs.concat(), whole);
            for run in &runs[..runs.len() - 1] {
                prop_assert_eq!(run.len(), RUN);
            }
            for run in &runs {
                prop_assert!(run.capacity() <= RUN);
            }
        }
    }
}
