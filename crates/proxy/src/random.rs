//! The randomness the data plane brings to the request core: [`random`] for spreading
//! requests, and [`unguessable`] for what a client is shown.

use std::cell::{Cell, RefCell};
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

/// How many bytes a thread draws from BoringSSL at a time: a request's ID takes ten, so one
/// draw serves 256 of them.
const DRAWN: usize = 2560;

/// `N` bytes from BoringSSL's generator, which no number of them seen lets anyone work out
/// the next of, unlike [`random`]'s: a request's ID is shown to its client, and must not
/// let it work out anyone else's (RFC 9562 §6.9). Every thread keeps a buffer of its own,
/// so a request takes no lock and makes one call into BoringSSL for many requests rather
/// than one each. `None` only if BoringSSL could not give them, which it never does: it
/// aborts the process instead (`RAND_bytes` in its `rand.h`).
pub(crate) fn unguessable<const N: usize>() -> Option<[u8; N]> {
    struct Drawn {
        bytes: [u8; DRAWN],
        /// Where the bytes not yet given begin.
        at: usize,
    }
    thread_local! {
        static DRAWN_BYTES: RefCell<Drawn> = const {
            RefCell::new(Drawn { bytes: [0; DRAWN], at: DRAWN })
        };
    }
    DRAWN_BYTES.with_borrow_mut(|drawn| {
        if DRAWN - drawn.at < N {
            boring::rand::rand_bytes(&mut drawn.bytes).ok()?;
            drawn.at = 0;
        }
        let given: [u8; N] = drawn.bytes.get(drawn.at..drawn.at + N)?.try_into().ok()?;
        // Never given twice.
        drawn.at += N;
        Some(given)
    })
}

/// A number that is uniform over `u64`. Every thread has a generator of its own (SplitMix64,
/// seeded once from the operating system by way of the standard library's hasher keys), so
/// a request takes no lock and shares no cache line for it.
pub(crate) fn random() -> u64 {
    thread_local! {
        static STATE: Cell<u64> = Cell::new(RandomState::new().build_hasher().finish());
    }
    STATE.with(|state| {
        let next = state.get().wrapping_add(0x9E37_79B9_7F4A_7C15);
        state.set(next);
        let mixed = (next ^ (next >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        mixed ^ (mixed >> 31)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn numbers_do_not_repeat() {
        let numbers: HashSet<u64> = (0..10_000).map(|_| random()).collect();
        assert_eq!(numbers.len(), 10_000);
    }

    #[test]
    fn every_bit_comes_up_about_half_of_the_time() {
        let mut ones = [0_u32; 64];
        for _ in 0..10_000 {
            let number = random();
            for (bit, count) in ones.iter_mut().enumerate() {
                *count += u32::from(number >> bit & 1 == 1);
            }
        }
        // Ten standard deviations either side of the mean.
        assert!(
            ones.iter().all(|count| (4_500..=5_500).contains(count)),
            "{ones:?}"
        );
    }

    #[test]
    fn unguessable_bytes_are_never_given_twice() {
        // More than one draw's worth, so that a refill is crossed.
        let given: HashSet<[u8; 10]> = (0..DRAWN).map(|_| unguessable::<10>().unwrap()).collect();
        assert_eq!(given.len(), DRAWN);
    }

    #[test]
    fn unguessable_bytes_are_spread_over_every_bit() {
        let mut ones = [0_u32; 80];
        for _ in 0..10_000 {
            let bytes: [u8; 10] = unguessable().unwrap();
            for (bit, count) in ones.iter_mut().enumerate() {
                *count += u32::from(bytes[bit / 8] >> (bit % 8) & 1 == 1);
            }
        }
        // Ten standard deviations either side of the mean.
        assert!(
            ones.iter().all(|count| (4_500..=5_500).contains(count)),
            "{ones:?}"
        );
    }

    #[test]
    fn threads_do_not_share_unguessable_bytes() {
        let here: Vec<[u8; 10]> = (0..4).map(|_| unguessable().unwrap()).collect();
        let there = std::thread::spawn(|| {
            (0..4)
                .map(|_| unguessable().unwrap())
                .collect::<Vec<[u8; 10]>>()
        });
        assert_ne!(here, there.join().unwrap());
    }

    #[test]
    fn threads_do_not_share_a_sequence() {
        let here: Vec<u64> = (0..4).map(|_| random()).collect();
        let there = std::thread::spawn(|| (0..4).map(|_| random()).collect::<Vec<u64>>());
        assert_ne!(here, there.join().unwrap());
    }
}
