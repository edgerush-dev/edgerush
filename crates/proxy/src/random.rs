//! The randomness the data plane brings to the request core: for spreading requests, not
//! for secrets.

use std::cell::Cell;
use std::collections::hash_map::RandomState;
use std::hash::{BuildHasher, Hasher};

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
    fn threads_do_not_share_a_sequence() {
        let here: Vec<u64> = (0..4).map(|_| random()).collect();
        let there = std::thread::spawn(|| (0..4).map(|_| random()).collect::<Vec<u64>>());
        assert_ne!(here, there.join().unwrap());
    }
}
