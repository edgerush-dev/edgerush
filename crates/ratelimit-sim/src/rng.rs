//! A small seeded generator, so that a run is the same on every machine and needs no crate.

/// SplitMix64: a 64-bit state moved on by a constant and mixed. Good enough to spread
/// traffic about; nothing here needs more.
#[derive(Clone, Debug)]
pub struct Rng(u64);

impl Rng {
    /// A generator starting from `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_4767_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A number below `bound`, or 0 if `bound` is 0. The high half of a 128-bit product:
    /// no division, and a bias too small to matter here.
    pub fn below(&mut self, bound: u64) -> u64 {
        let wide = u128::from(self.next_u64()) * u128::from(bound);
        // The product of two 64-bit numbers shifted down by 64 always fits in 64 bits.
        u64::try_from(wide >> 64).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_gives_the_same_numbers() {
        let numbers = |seed| {
            let mut rng = Rng::new(seed);
            (0..100).map(|_| rng.next_u64()).collect::<Vec<_>>()
        };
        assert_eq!(numbers(7), numbers(7));
        assert_ne!(numbers(7), numbers(8));
    }

    #[test]
    fn below_stays_below_and_reaches_every_value() {
        let mut rng = Rng::new(1);
        let mut seen = [false; 7];
        for _ in 0..1000 {
            let n = rng.below(7);
            assert!(n < 7);
            seen[usize::try_from(n).unwrap()] = true;
        }
        assert!(seen.iter().all(|s| *s));
        assert_eq!(rng.below(0), 0);
    }
}
