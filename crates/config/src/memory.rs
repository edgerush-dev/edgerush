//! How a data plane's memory limit is shared out ([03 §9], [06 §8]): first its keys' cap
//! and room beside it, then storage for requests and the connections, of what is left.
//!
//! Here, not in the proxy, because a control plane works out the same default cap from the
//! DataPlane it serves, and a data plane without one from the limit it reads at start: the
//! two must agree. Whichever sets the cap, the proxy takes what it sets aside out of the
//! limit before it sizes the rest.
//!
//! The cap left to the default is the larger of a quarter of the limit and what the
//! workers' storage and connections could never use, less the keys' headroom: a small pod
//! shares its memory, and a big one gives its keys what nothing else would take.
//!
//! [03 §9]: ../../../docs/03-data-plane.md
//! [06 §8]: ../../../docs/06-rate-limiting.md

use std::num::NonZeroU64;

/// What a worker may hold for requests unless it is told otherwise, and the most a limit
/// gives it: 14 §8's proposal, to be checked against measurement before it is adopted.
pub const STORAGE_CEILING: usize = 256 * 1024 * 1024;

/// The most connections a worker holds before it stops accepting (14 §8): open ones, ones
/// on their way to it and ones lingering to their close. Fewer where the open files or the
/// memory allow fewer.
pub const CONNECTIONS_PER_WORKER: usize = 32_768;

/// What an idle TCP connection costs the pod at most, of the kinds measured: TLS with
/// HTTP/2, after one request, 21.5–21.9 KB of the cgroup's memory (18.2 KB the process's,
/// 4.0 KB the kernel's), rounded up. Plain HTTP/1 is 7.7 KB, HTTP/2 16.4 KB, TLS HTTP/1
/// 16.4 KB (laptop, 4 workers, `bench/idle.py`, 03 §9).
pub const CONNECTION_COST: u64 = 22 * 1024;

/// The most a worker's storage and connections can use together: the storage ceiling, and
/// every connection it may hold at the dearest kind's cost. About 960 MiB.
// Widening casts: a `usize` is no wider than a `u64` on any target.
pub const WORKER_MOST: u64 =
    STORAGE_CEILING as u64 + CONNECTIONS_PER_WORKER as u64 * CONNECTION_COST;

/// The cap when there is no limit to work it from: 256 MiB.
pub const UNLIMITED_KEY_BYTES: u64 = 256 << 20;

/// The default cap for a pod of `limit` bytes (none if it has none) running `workers`
/// workers: a quarter of the limit, or, where larger, what the workers' storage and
/// connections could never use ([`WORKER_MOST`] each) less the keys' headroom
/// ([`set_aside`]). At least one byte.
pub fn default_max_key_bytes(limit: Option<u64>, workers: NonZeroU64) -> u64 {
    let Some(limit) = limit else {
        return UNLIMITED_KEY_BYTES;
    };
    let unused = limit.saturating_sub(workers.get().saturating_mul(WORKER_MOST));
    // Of `unused`, as much as leaves room for the cap and half again: two thirds.
    let beyond = unused / 3 * 2 + unused % 3 * 2 / 3;
    (limit / 4).max(beyond).max(1)
}

/// What a cap of `max_key_bytes` sets aside of the pod's limit: the cap and half again.
/// Keys freed are not all given back to the system at once: #602 measured the process's
/// resident memory at up to 1.17× its keys' cap, and half again leaves room for what was not
/// measured.
pub const fn set_aside(max_key_bytes: u64) -> u64 {
    max_key_bytes.saturating_add(max_key_bytes / 2)
}

/// Whether a pod of `limit` bytes can hold a cap of `max_key_bytes` at all: what it sets
/// aside is below the limit. The DataPlane's schema refuses a cap that fails this (05 §1);
/// one that passes leaves the rest of the pod squeezed toward its floors, as the start says.
pub const fn fits(max_key_bytes: u64, limit: u64) -> bool {
    set_aside(max_key_bytes) < limit
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MIB: u64 = 1024 * 1024;
    const GIB: u64 = 1024 * MIB;

    fn workers(count: u64) -> NonZeroU64 {
        NonZeroU64::new(count).unwrap()
    }

    #[test]
    fn a_worker_can_use_about_960_mib() {
        // 256 MiB of storage and 32,768 connections at 22 KiB: 704 MiB.
        assert_eq!(WORKER_MOST, 960 * MIB);
    }

    #[test]
    fn with_no_limit_the_cap_is_256_mib() {
        assert_eq!(default_max_key_bytes(None, workers(4)), 256 * MIB);
    }

    /// The figures the default was chosen by, on 4 workers (Lucas, #609).
    #[test]
    fn a_small_pod_gives_its_keys_a_quarter_and_a_big_one_what_nothing_else_uses() {
        let four = workers(4);
        for (limit, cap) in [
            (256 * MIB, 64 * MIB),
            (512 * MIB, 128 * MIB),
            (GIB, 256 * MIB),
            (2 * GIB, 512 * MIB),
            (4 * GIB, GIB),
            // Where the two meet: the workers use 3,840 MiB, and a quarter, 1,536 MiB, and
            // half again is the 2,304 MiB beyond them.
            (6 * GIB, 1536 * MIB),
            // Two thirds of what is beyond the workers: (8,192 - 3,840) MiB.
            (8 * GIB, 4352 * MIB * 2 / 3),
            (64 * GIB, 61_696 * MIB * 2 / 3),
        ] {
            assert_eq!(default_max_key_bytes(Some(limit), four), cap, "{limit}");
        }
    }

    #[test]
    fn more_workers_leave_less_beyond_them() {
        assert!(
            default_max_key_bytes(Some(64 * GIB), workers(16))
                < default_max_key_bytes(Some(64 * GIB), workers(4))
        );
        // 16 workers use 15 GiB of the 16: a quarter is more than what is beyond them.
        assert_eq!(default_max_key_bytes(Some(16 * GIB), workers(16)), 4 * GIB);
    }

    #[test]
    fn a_cap_sets_aside_half_again() {
        assert_eq!(set_aside(128 * MIB), 192 * MIB);
        assert_eq!(set_aside(u64::MAX), u64::MAX);
    }

    #[test]
    fn a_cap_fits_while_what_it_sets_aside_is_below_the_limit() {
        assert!(fits(GIB, 2 * GIB));
        assert!(!fits(GIB, 1536 * MIB));
        assert!(fits(GIB, 1536 * MIB + 1));
    }

    proptest! {
        /// Whatever the limit and the workers, the default cap fits the pod, and what it
        /// sets aside leaves the workers at least what they could use, or three quarters of
        /// the pod, whichever is less.
        #[test]
        fn the_default_always_fits_and_leaves_the_workers_their_room(
            limit in 1u64..=1 << 50,
            count in 1u64..=256,
        ) {
            let cap = default_max_key_bytes(Some(limit), workers(count));
            let left = limit.saturating_sub(set_aside(cap));
            if limit >= 2 {
                prop_assert!(fits(cap, limit));
            }
            let room = count.saturating_mul(WORKER_MOST).min(limit - limit / 4 - limit / 8);
            prop_assert!(left + 1 >= room, "{left} < {room}");
            prop_assert!(cap >= limit / 4);
        }
    }
}
