//! The open files and the memory the process may hold, and what that allows each worker:
//! connections, and storage for requests ([03 §9] in the docs).
//!
//! The process raises its soft open-file limit to its hard limit as it starts, before it
//! opens a socket ([`edgerush_control_service::open_files`]): under containerd 2.x a
//! container is given 1,024, about a thousand sockets for clients and upstreams together.
//! What a worker may then hold follows from what was got, so that a worker stops at its
//! cap — where the fair share between listeners and the pause that costs nothing are —
//! rather than at the descriptors running out, where accepting fails and is tried again.
//!
//! A pod's memory limit is enforced by the kernel through its cgroup: at `memory.max` the
//! OOM killer ends the process, every connection with it, and at `memory.high`, where one
//! is set, the pod is throttled with reclaim. A worker's storage for requests is a fixed
//! number that knows nothing of either, so it is sized from the lower of the two as the
//! process starts, as Envoy Gateway sizes Envoy's heap from the pod's limit.
//!
//! The rate limits' keys come first ([`edgerush_config::memory`]): their cap, the config's
//! or else worked out from the limit, and half as much again, are set aside. Of what is
//! left, half, shared among the workers, is for what the storage account counts, never more
//! than a worker's own bound, which is also what it has without a limit; the rest is for
//! what it does not — connection and TLS state, the kernel's socket buffers, what the
//! allocator keeps.
//!
//! That rest holds the connections, which no storage account counts: so a worker's
//! connection cap is no more than it, shared out, at what the dearest kind of idle
//! connection was measured to cost the pod.
//!
//! [03 §9]: ../../../docs/03-data-plane.md

use crate::per_core::CONNECTIONS_PER_WORKER;
use edgerush_config::memory::{default_max_key_bytes, set_aside};
use edgerush_control_service::open_files::OpenFiles;
use edgerush_proxy::connections::{CONNECTION, QUIC_MOST};
use edgerush_proxy::{H1Limits, KeyCap};
use std::num::NonZeroUsize;

/// The least storage a worker is given, however small the limit: room for 128 heads of the
/// largest size, so that no limit leaves a worker unable to read a request.
const STORAGE_FLOOR: usize = 8 * 1024 * 1024;

/// What an idle TCP connection costs the pod at most
/// ([`edgerush_config::memory::CONNECTION_COST`]). An HTTP/3 connection, dearer, counts as
/// up to three connections of this cost ([`quic_weight`]) rather than raising it for every
/// connection.
const CONNECTION_COST: u64 = edgerush_config::memory::CONNECTION_COST;

/// Where the pod's limit is read, in a cgroup namespace or not.
#[cfg(target_os = "linux")]
const CGROUPS: &str = "/sys/fs/cgroup";

/// Open files kept for what is not a client's connection: the listening sockets, the
/// scrape's, the health checker's probes, files. Never more than half of a small limit.
const RESERVE: u64 = 1024;

/// Open files a client's connection takes: its own, and in most cases one to its upstream.
const PER_CONNECTION: u64 = 2;

/// The rate limits' keys as the data plane starts: the cap `stated` by its first config
/// (the outer none where the mode keeps no keys, the inner where it leaves the cap to the
/// data plane), and the cap a config leaving it out runs at, worked out from `memory` and
/// `workers`. The cap it starts with is the most any later config has it hold.
pub(crate) fn key_cap(stated: Option<Option<u64>>, memory: Memory, workers: NonZeroUsize) -> Keys {
    let limit = match memory {
        Memory::NoLimit => None,
        Memory::Limit { bytes, .. } => Some(bytes),
    };
    let count = std::num::NonZeroU64::try_from(workers).unwrap_or(std::num::NonZeroU64::MIN);
    let default = default_max_key_bytes(limit, count);
    let (most, whence) = match (stated, limit) {
        (None, _) => (0, OFF),
        (Some(Some(stated)), _) => (stated, "the config's"),
        (Some(None), None) => (default, "no limit to work from"),
        (Some(None), Some(limit)) if default == limit / 4 => (default, "a quarter of the limit"),
        (Some(None), Some(_)) => (default, "what the workers leave, less half again"),
    };
    Keys {
        cap: KeyCap { default, most },
        whence,
    }
}

/// Where a key cap comes from when the mode keeps no keys.
const OFF: &str = "rate limiting off";

/// The rate limits' keys as the data plane starts: what they may hold, and where the cap
/// it started with came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Keys {
    pub(crate) cap: KeyCap,
    pub(crate) whence: &'static str,
}

/// What `memory` leaves storage and connections once what a key cap of `most` sets aside
/// is taken out, or nothing if it takes all.
pub(crate) fn beside_keys(memory: Memory, most: u64) -> Memory {
    match memory {
        Memory::NoLimit => Memory::NoLimit,
        Memory::Limit { bytes, from } => Memory::Limit {
            bytes: bytes.saturating_sub(set_aside(most)),
            from,
        },
    }
}

/// How many connections each of `workers` may hold, and what set it: under `open_files`,
/// what the limit leaves after the reserve, two open files to a connection, shared out; and
/// under `memory`, what storage leaves of it ([`storage_per_worker`]) at
/// [`CONNECTION_COST`] a connection, shared out — the fewer of the two, never more than
/// [`CONNECTIONS_PER_WORKER`], and never none. And what an HTTP/3 connection counts as
/// against that ([`quic_weight`]).
pub(crate) fn connections_per_worker(
    open_files: OpenFiles,
    memory: Memory,
    workers: NonZeroUsize,
) -> Connections {
    let workers = u64::try_from(workers.get()).unwrap_or(u64::MAX);
    let bounded = |each: u64| {
        usize::try_from(each)
            .unwrap_or(usize::MAX)
            .clamp(1, CONNECTIONS_PER_WORKER)
    };
    let by_files = open_files
        .limit()
        .map(|limit| bounded((limit - RESERVE.min(limit / 2)) / PER_CONNECTION / workers));
    // What memory allows a worker before it is bounded: what an HTTP/3 connection counts as
    // turns on how far that is above the cap.
    let memory_allows = match memory {
        Memory::NoLimit => None,
        Memory::Limit { .. } => Some(beside_storage(memory, workers) / CONNECTION_COST / workers),
    };
    let by_memory = memory_allows.map(bounded);
    let (each, set_by) = match (by_files, by_memory) {
        (Some(files), Some(memory)) if memory < files => (memory, "memory"),
        (None, Some(memory)) if memory < CONNECTIONS_PER_WORKER => (memory, "memory"),
        (Some(files), _) if files < CONNECTIONS_PER_WORKER => (files, "open files"),
        _ => (CONNECTIONS_PER_WORKER, "a worker's bound"),
    };
    Connections {
        each,
        set_by,
        quic: quic_weight(each, memory_allows),
    }
}

/// What storage leaves of `memory`'s limit on `workers` workers: storage takes half up to
/// its ceiling, and what it cannot use is left with the rest.
fn beside_storage(memory: Memory, workers: u64) -> u64 {
    let Memory::Limit { bytes, .. } = memory else {
        return u64::MAX;
    };
    let count = usize::try_from(workers)
        .ok()
        .and_then(NonZeroUsize::new)
        .unwrap_or(NonZeroUsize::MIN);
    let storage = u64::try_from(storage_per_worker(memory, count)).unwrap_or(u64::MAX);
    bytes.saturating_sub(storage.saturating_mul(workers))
}

/// Each of `workers`' share of the open files: the limit less the reserve, divided among
/// them. Its client connections and its sockets to upstreams count against it together, so
/// that what the connection cap leaves of it goes to upstream sockets and no worker runs
/// the process out of files under its cap (03 §9). None where there is no limit.
pub(crate) fn descriptors_per_worker(
    open_files: OpenFiles,
    workers: NonZeroUsize,
) -> Option<usize> {
    let workers = u64::try_from(workers.get()).unwrap_or(u64::MAX);
    open_files.limit().map(|limit| {
        usize::try_from((limit - RESERVE.min(limit / 2)) / workers).unwrap_or(usize::MAX)
    })
}

/// What an HTTP/3 connection counts as, in sixteenths of a connection ([`CONNECTION`]),
/// against a cap of `each` connections where memory allows a worker `memory` of them
/// (`None` with no limit): `3 × each / memory` connections, rounded up, and never less than
/// one. An HTTP/3 connection costs three connections' memory ([`QUIC_MOST`]) and holds no
/// open file of its own, so it counts as all three where memory sets the cap, and as one
/// where memory allows three times the cap or more, or has no limit.
///
/// No mix of TCP and HTTP/3 connections the cap then admits holds more memory than
/// `memory` connections' worth: with `tcp + w × quic ≤ each`, `w ≥ 3 × each / memory` and
/// `memory ≥ each`, `tcp + 3 × quic ≤ (memory / each) × (tcp + w × quic) ≤ memory`. A
/// single weight picked by which bound is lowest would not do: at one, a cap set by open
/// files just under what memory allows would admit three times the memory in HTTP/3.
fn quic_weight(each: usize, memory: Option<u64>) -> usize {
    let Some(memory) = memory else {
        return CONNECTION;
    };
    let each = u64::try_from(each).unwrap_or(u64::MAX);
    let most = u64::try_from(QUIC_MOST).unwrap_or(u64::MAX);
    // With less memory than the cap, which is never none, nothing is to spare.
    if memory < each {
        return QUIC_MOST;
    }
    let weight = most.saturating_mul(each).div_ceil(memory);
    usize::try_from(weight)
        .unwrap_or(QUIC_MOST)
        .clamp(CONNECTION, QUIC_MOST)
}

/// How many connections a worker may hold, which limit set it, and what an HTTP/3
/// connection counts as.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Connections {
    pub(crate) each: usize,
    pub(crate) set_by: &'static str,
    /// In sixteenths of a connection, from one connection to three ([`quic_weight`]).
    pub(crate) quic: usize,
}

/// What memory the pod may use, as its cgroup says.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Memory {
    /// No limit, or none that could be read: no cgroup v2, a platform without cgroups, or
    /// `max` in both files.
    NoLimit,
    /// The lower of `memory.max` and `memory.high`, and which of them it is.
    #[cfg_attr(
        not(target_os = "linux"),
        allow(dead_code, reason = "only Linux has cgroups to read")
    )]
    Limit { bytes: u64, from: &'static str },
}

/// The pod's memory limit, read from the process's own cgroup (Linux, cgroup v2).
#[cfg(target_os = "linux")]
pub(crate) fn memory() -> Memory {
    let Ok(cgroups) = std::fs::read_to_string("/proc/self/cgroup") else {
        return Memory::NoLimit;
    };
    let Some(own) = cgroup_of(&cgroups) else {
        return Memory::NoLimit;
    };
    let directory = std::path::Path::new(CGROUPS).join(own.trim_start_matches('/'));
    let read = |file: &str| {
        std::fs::read_to_string(directory.join(file))
            .ok()
            .and_then(|written| limit_in(&written))
    };
    lower(read("memory.max"), read("memory.high"))
}

/// There are no cgroups to read.
#[cfg(not(target_os = "linux"))]
pub(crate) fn memory() -> Memory {
    Memory::NoLimit
}

/// The process's cgroup v2 path in what `/proc/self/cgroup` says: the line `0::<path>`,
/// `/` inside a cgroup namespace. None where there is no cgroup v2.
#[cfg_attr(
    not(target_os = "linux"),
    allow(dead_code, reason = "only Linux has cgroups to read")
)]
fn cgroup_of(cgroups: &str) -> Option<&str> {
    cgroups.lines().find_map(|line| line.strip_prefix("0::"))
}

/// The limit a cgroup's `memory.max` or `memory.high` says, none for `max`. What cannot be
/// read is taken as no limit, never as a small one.
#[cfg_attr(
    not(target_os = "linux"),
    allow(dead_code, reason = "only Linux has cgroups to read")
)]
fn limit_in(written: &str) -> Option<u64> {
    written.trim().parse().ok()
}

/// The lower of the two limits that are there, and which it is.
#[cfg_attr(
    not(target_os = "linux"),
    allow(dead_code, reason = "only Linux has cgroups to read")
)]
fn lower(max: Option<u64>, high: Option<u64>) -> Memory {
    match (max, high) {
        (None, None) => Memory::NoLimit,
        (Some(bytes), None) => Memory::Limit {
            bytes,
            from: "memory.max",
        },
        (Some(max), Some(high)) if max <= high => Memory::Limit {
            bytes: max,
            from: "memory.max",
        },
        (_, Some(bytes)) => Memory::Limit {
            bytes,
            from: "memory.high",
        },
    }
}

/// What each of `workers` may hold for requests under `memory`, what the keys leave of the
/// limit ([`beside_keys`]): half of it shared out, never more than a worker's own bound nor
/// less than [`STORAGE_FLOOR`].
pub(crate) fn storage_per_worker(memory: Memory, workers: NonZeroUsize) -> usize {
    let ceiling = H1Limits::default().storage;
    let Memory::Limit { bytes, .. } = memory else {
        return ceiling;
    };
    let workers = u64::try_from(workers.get()).unwrap_or(u64::MAX);
    let each = bytes / 2 / workers;
    usize::try_from(each)
        .unwrap_or(usize::MAX)
        .clamp(STORAGE_FLOOR.min(ceiling), ceiling)
}

/// What the start says of the pod's `memory`, the keys' cap and what it sets aside, and
/// the storage it allows each worker.
pub(crate) fn described_memory(memory: Memory, keys: Keys, storage: usize) -> String {
    const MIB: u64 = 1024 * 1024;
    let each = format!("{} MiB storage a worker", storage / (1024 * 1024));
    let cap = keys.cap.most;
    let held = if keys.whence == OFF {
        format!("no keys ({OFF})")
    } else {
        format!("keys {} MiB ({})", cap / MIB, keys.whence)
    };
    match memory {
        Memory::NoLimit => format!("memory: no limit; {held}; {each}"),
        Memory::Limit { bytes, from } => {
            let aside = set_aside(cap);
            let aside = match (cap, aside >= bytes) {
                (0, _) => String::new(),
                (_, false) => format!(", {} MiB set aside", aside / MIB),
                (_, true) => format!(
                    ", {} MiB set aside: nothing left beside them, storage and connections at their floors",
                    aside / MIB
                ),
            };
            format!(
                "memory: limit {} MiB ({from}); {held}{aside}; {each}",
                bytes / MIB
            )
        }
    }
}

/// What the start says of the open files, and the connections each worker may hold and
/// what set that.
pub(crate) fn described(open_files: OpenFiles, connections: Connections) -> String {
    let each = format!(
        "{} connections a worker, by {}",
        connections.each, connections.set_by
    );
    match open_files.described() {
        Some(files) => format!("{files}; {each}"),
        None => each,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn workers(count: usize) -> NonZeroUsize {
        NonZeroUsize::new(count).unwrap()
    }

    fn limit(now: u64) -> OpenFiles {
        OpenFiles::Raised {
            was: Some(1024),
            now: Some(now),
        }
    }

    fn bound(each: usize, set_by: &'static str, quic: usize) -> Connections {
        Connections { each, set_by, quic }
    }

    #[test]
    fn the_cap_is_the_fewer_of_what_open_files_and_memory_allow() {
        // 512 MiB, 4 workers: 256 MiB at 22 KiB a connection, 11,915, 2,978 each. Memory
        // sets the cap, so an HTTP/3 connection counts as all it costs.
        assert_eq!(
            connections_per_worker(limit(524_288), limited(512 * MIB), workers(4)),
            bound(2_978, "memory", QUIC_MOST)
        );
        // 1 GiB beside a hard limit of 65,536 open files: memory's 5,957 is fewer than
        // the files' 8,064.
        assert_eq!(
            connections_per_worker(limit(65_536), limited(1024 * MIB), workers(4)),
            bound(5_957, "memory", QUIC_MOST)
        );
        // 8 GiB: storage at its ceiling, 1 GiB, and memory would allow more than a worker's
        // bound, 83,409 a worker. An HTTP/3 connection counts as 3 × 32,768 / 83,409 of
        // one, 1.18, rounded up to the next sixteenth: 19.
        assert_eq!(
            connections_per_worker(limit(524_288), limited(8192 * MIB), workers(4)),
            bound(CONNECTIONS_PER_WORKER, "a worker's bound", 19)
        );
        // A small memory limit with no open-file limit to speak of.
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, limited(64 * MIB), workers(2)),
            bound(744, "memory", QUIC_MOST)
        );
        // Open files fewer than memory allows, far fewer: an HTTP/3 connection, holding no
        // file of its own, counts as one.
        assert_eq!(
            connections_per_worker(limit(4_096), limited(8192 * MIB), workers(1)),
            bound(1_536, "open files", CONNECTION)
        );
        // However small, never none.
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, limited(0), workers(1)),
            bound(1, "memory", QUIC_MOST)
        );
    }

    /// A worker's share of the files is the limit less the reserve (half of a small limit),
    /// divided among the workers: twice its cap where open files set the cap, so that
    /// what the cap leaves goes to its upstream sockets. None with no limit.
    #[test]
    fn a_workers_share_of_the_files_is_the_limit_less_the_reserve_shared_out() {
        assert_eq!(descriptors_per_worker(limit(1_024), workers(1)), Some(512));
        assert_eq!(
            descriptors_per_worker(limit(4_096), workers(2)),
            Some(1_536)
        );
        assert_eq!(
            descriptors_per_worker(limit(65_536), workers(8)),
            Some(8_064)
        );
        assert_eq!(
            descriptors_per_worker(limit(524_288), workers(8)),
            Some(65_408)
        );
        assert_eq!(descriptors_per_worker(OpenFiles::NoLimit, workers(4)), None);
        let cap = connections_per_worker(limit(4_096), Memory::NoLimit, workers(2));
        assert_eq!(
            Some(cap.each * 2),
            descriptors_per_worker(limit(4_096), workers(2))
        );
    }

    /// With no memory limit nothing is sized from memory, and an HTTP/3 connection counts as
    /// any other.
    #[test]
    fn with_no_memory_limit_an_http3_connection_counts_as_one() {
        for files in [
            limit(1024),
            limit(65_536),
            limit(524_288),
            OpenFiles::NoLimit,
        ] {
            let cap = connections_per_worker(files, Memory::NoLimit, workers(4));
            assert_eq!(cap.quic, CONNECTION, "{cap:?}");
        }
    }

    proptest::proptest! {
        /// Whatever the limits, no mix of TCP and HTTP/3 connections the cap lets a worker
        /// hold costs more memory than the limit allows a worker: at what an idle TCP
        /// connection costs, an HTTP/3 connection costing three. Nor does an HTTP/3
        /// connection ever count as less than one connection or more than three.
        #[test]
        fn no_mix_the_cap_admits_holds_more_than_memory_allows(
            mebibytes in 0_u64..65_536,
            files in proptest::option::of(0_u64..2_000_000),
            count in 1_usize..64,
        ) {
            let files = match files {
                Some(now) => limit(now),
                None => OpenFiles::NoLimit,
            };
            let cap = connections_per_worker(files, limited(mebibytes * MIB), workers(count));
            proptest::prop_assert!((CONNECTION..=QUIC_MOST).contains(&cap.quic), "{:?}", cap);
            let allowed = beside_storage(limited(mebibytes * MIB), count as u64)
                / CONNECTION_COST
                / count as u64;
            // A cap is never none, however little memory there is.
            proptest::prop_assume!(allowed >= cap.each as u64);
            let room = cap.each * CONNECTION;
            for quic in 0..=room / cap.quic {
                let tcp = (room - quic * cap.quic) / CONNECTION;
                let costs = tcp as u64 + 3 * quic as u64;
                proptest::prop_assert!(
                    costs <= allowed,
                    "{} TCP and {} HTTP/3 cost {} of {}: {:?}", tcp, quic, costs, allowed, cap
                );
            }
        }
    }

    #[test]
    fn a_limit_high_enough_leaves_the_cap_as_it_is() {
        // containerd 2.x's hard limit, raised to: room for the whole cap on up to seven
        // workers, and on eight a little less, (524,288 - 1,024) / 2 / 8.
        assert_eq!(
            connections_per_worker(limit(524_288), Memory::NoLimit, workers(7)).each,
            CONNECTIONS_PER_WORKER
        );
        assert_eq!(
            connections_per_worker(limit(524_288), Memory::NoLimit, workers(8)).each,
            32_704
        );
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, Memory::NoLimit, workers(64)).each,
            CONNECTIONS_PER_WORKER
        );
        let unlimited = OpenFiles::Raised {
            was: None,
            now: None,
        };
        assert_eq!(
            connections_per_worker(unlimited, Memory::NoLimit, workers(4)).each,
            CONNECTIONS_PER_WORKER
        );
    }

    #[test]
    fn a_lower_limit_is_shared_out_after_the_reserve_two_files_to_a_connection() {
        // (65,536 - 1,024) / 2 / 4
        assert_eq!(
            connections_per_worker(limit(65_536), Memory::NoLimit, workers(4)).each,
            8_064
        );
        // More workers than that leaves less to each.
        assert_eq!(
            connections_per_worker(limit(524_288), Memory::NoLimit, workers(16)).each,
            16_352
        );
    }

    #[test]
    fn a_small_limit_keeps_half_for_the_reserve_and_never_leaves_a_worker_none() {
        // The 1,024 a process is given if it cannot raise it: 512 kept, 256 connections.
        assert_eq!(
            connections_per_worker(limit(1024), Memory::NoLimit, workers(1)).each,
            256
        );
        assert_eq!(
            connections_per_worker(limit(1024), Memory::NoLimit, workers(4)).each,
            64
        );
        let kept = OpenFiles::Kept {
            now: Some(16),
            why: std::io::ErrorKind::PermissionDenied,
        };
        assert_eq!(
            connections_per_worker(kept, Memory::NoLimit, workers(64)).each,
            1
        );
        assert_eq!(
            connections_per_worker(limit(0), Memory::NoLimit, workers(1)).each,
            1
        );
    }

    #[test]
    fn the_start_says_what_became_of_the_limit_and_what_each_worker_may_hold() {
        assert_eq!(
            described(
                limit(524_288),
                bound(32_768, "a worker's bound", CONNECTION)
            ),
            "open files: soft limit 1024 raised to 524288; 32768 connections a worker, by a worker's bound"
        );
        let same = OpenFiles::Raised {
            was: Some(4096),
            now: Some(4096),
        };
        assert_eq!(
            described(same, bound(1_792, "open files", CONNECTION)),
            "open files: soft limit 4096; 1792 connections a worker, by open files"
        );
        let kept = OpenFiles::Kept {
            now: Some(1024),
            why: std::io::ErrorKind::PermissionDenied,
        };
        assert_eq!(
            described(kept, bound(256, "open files", CONNECTION)),
            "open files: soft limit 1024, not raised (permission denied); 256 connections a worker, by open files"
        );
        assert_eq!(
            described(OpenFiles::NoLimit, bound(32_768, "memory", QUIC_MOST)),
            "32768 connections a worker, by memory"
        );
    }

    const MIB: u64 = 1024 * 1024;

    fn limited(bytes: u64) -> Memory {
        Memory::Limit {
            bytes,
            from: "memory.max",
        }
    }

    #[test]
    fn the_cgroup_is_the_v2_line_of_proc_self_cgroup() {
        // In a cgroup namespace, as a pod's container is.
        assert_eq!(cgroup_of("0::/\n"), Some("/"));
        assert_eq!(
            cgroup_of("0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-2.scope\n"),
            Some("/kubepods.slice/kubepods-pod1.slice/cri-containerd-2.scope")
        );
        // Hybrid hierarchies list v1 controllers before it.
        assert_eq!(
            cgroup_of("12:memory:/docker/abc\n0::/docker/abc\n"),
            Some("/docker/abc")
        );
        // cgroup v1 only: no limit is read.
        assert_eq!(cgroup_of("4:memory:/docker/abc\n"), None);
        assert_eq!(cgroup_of(""), None);
    }

    #[test]
    fn a_limit_is_a_number_and_max_or_anything_else_is_none() {
        assert_eq!(limit_in("536870912\n"), Some(512 * MIB));
        assert_eq!(limit_in("max\n"), None);
        assert_eq!(limit_in(""), None);
        assert_eq!(limit_in("-1\n"), None);
    }

    #[test]
    fn the_limit_is_the_lower_of_max_and_high() {
        assert_eq!(lower(None, None), Memory::NoLimit);
        assert_eq!(lower(Some(GIB), None), limited(GIB));
        assert_eq!(
            lower(Some(GIB), Some(900 * MIB)),
            Memory::Limit {
                bytes: 900 * MIB,
                from: "memory.high"
            }
        );
        assert_eq!(lower(Some(GIB), Some(2 * GIB)), limited(GIB));
        assert_eq!(
            lower(None, Some(GIB)),
            Memory::Limit {
                bytes: GIB,
                from: "memory.high"
            }
        );
    }

    const GIB: u64 = 1024 * MIB;

    #[test]
    fn storage_is_half_the_limit_shared_out_within_the_floor_and_the_ceiling() {
        let ceiling = H1Limits::default().storage;
        assert_eq!(storage_per_worker(Memory::NoLimit, workers(4)), ceiling);
        // 512 MiB, 4 workers: 64 MiB each.
        assert_eq!(storage_per_worker(limited(512 * MIB), workers(4)), 64 << 20);
        // 2 GiB, 4 workers: 256 MiB each, the ceiling.
        assert_eq!(storage_per_worker(limited(2 * GIB), workers(4)), ceiling);
        // 8 GiB, 4 workers: still the ceiling.
        assert_eq!(storage_per_worker(limited(8 * GIB), workers(4)), ceiling);
        // 64 MiB, 16 workers: 2 MiB each, raised to the floor.
        assert_eq!(
            storage_per_worker(limited(64 * MIB), workers(16)),
            STORAGE_FLOOR
        );
        assert_eq!(storage_per_worker(limited(0), workers(1)), STORAGE_FLOOR);
    }

    /// Storage takes half of what the keys leave up to its ceiling, and the connections
    /// what storage leaves: past the ceiling, all of it.
    #[test]
    fn the_connections_have_what_storage_leaves() {
        // 1 GiB, 4 workers: storage 128 MiB each, half; the connections the other half,
        // 512 MiB, 23,831, 5,957 each.
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, limited(GIB), workers(4)).each,
            5_957
        );
        // 4 GiB: storage at its ceiling, 1 GiB in all; the connections 3 GiB, 142,987,
        // 35,746 each, more than a worker's bound. An HTTP/3 connection counts as
        // 3 x 32,768 / 35,746 of one, 2.75, rounded up to the next sixteenth: 45.
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, limited(4 * GIB), workers(4)),
            bound(CONNECTIONS_PER_WORKER, "a worker's bound", 45)
        );
        // Storage raised to its floor takes from the connections: 64 MiB on 16 workers,
        // 8 MiB each, 128 MiB, leaves none, and a worker still one.
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, limited(64 * MIB), workers(16)).each,
            1
        );
    }

    /// The figures #609's default was chosen by, on 4 workers: the keys' cap, what it sets
    /// aside, storage and the connections, from what the cgroup says.
    #[test]
    fn the_keys_come_out_of_the_limit_first() {
        let four = workers(4);
        let planned = |limit: u64| {
            let keys = key_cap(Some(None), limited(limit), four);
            let rest = beside_keys(limited(limit), keys.cap.most);
            let connections = connections_per_worker(OpenFiles::NoLimit, rest, four);
            (
                keys.cap.most,
                storage_per_worker(rest, four),
                connections.each,
            )
        };
        // 512 MiB: a quarter, 128 MiB, and 192 MiB set aside; of the 320 MiB left, 40 MiB
        // storage each, and 160 MiB of connections, 7,447, 1,861 each.
        assert_eq!(planned(512 * MIB), (128 * MIB, 40 << 20, 1_861));
        // 2 GiB: 512 MiB, 768 MiB set aside; 160 MiB each, 640 MiB of connections, 7,447
        // each.
        assert_eq!(planned(2 * GIB), (512 * MIB, 160 << 20, 7_447));
        // 8 GiB: two thirds of what the workers' 3,840 MiB leave; the workers then have
        // all they can use.
        assert_eq!(
            planned(8 * GIB),
            (4352 * MIB * 2 / 3, 256 << 20, CONNECTIONS_PER_WORKER)
        );
    }

    #[test]
    fn the_cap_the_data_plane_starts_with_is_the_configs_or_worked_out() {
        let four = workers(4);
        let default = 128 * MIB;
        let cap = |most| KeyCap { default, most };
        assert_eq!(
            key_cap(None, limited(512 * MIB), four),
            Keys {
                cap: cap(0),
                whence: OFF
            },
            "keeping no keys, it keeps no room for them"
        );
        assert_eq!(
            key_cap(Some(Some(MIB)), limited(512 * MIB), four).cap,
            cap(MIB)
        );
        assert_eq!(
            key_cap(Some(None), limited(512 * MIB), four).cap,
            cap(default)
        );
        assert_eq!(
            key_cap(Some(None), Memory::NoLimit, four).cap,
            KeyCap {
                default: 256 * MIB,
                most: 256 * MIB
            }
        );
        // A cap that takes all leaves nothing, never less.
        assert_eq!(beside_keys(limited(GIB), GIB), limited(0));
        assert_eq!(beside_keys(Memory::NoLimit, GIB), Memory::NoLimit);
    }

    #[test]
    fn the_start_says_what_the_pod_may_use_and_what_each_worker_may_hold() {
        let four = workers(4);
        let keys = |stated, memory| key_cap(stated, memory, four);
        let pod = limited(512 * MIB);
        assert_eq!(
            described_memory(pod, keys(None, pod), 64 << 20),
            "memory: limit 512 MiB (memory.max); no keys (rate limiting off); 64 MiB storage a worker"
        );
        assert_eq!(
            described_memory(pod, keys(Some(None), pod), 40 << 20),
            "memory: limit 512 MiB (memory.max); keys 128 MiB (a quarter of the limit), 192 MiB set aside; 40 MiB storage a worker"
        );
        assert_eq!(
            described_memory(
                limited(8 * GIB),
                keys(Some(None), limited(8 * GIB)),
                256 << 20
            ),
            "memory: limit 8192 MiB (memory.max); keys 2901 MiB (what the workers leave, less half again), 4351 MiB set aside; 256 MiB storage a worker"
        );
        assert_eq!(
            described_memory(pod, keys(Some(Some(400 * MIB)), pod), 8 << 20),
            "memory: limit 512 MiB (memory.max); keys 400 MiB (the config's), 600 MiB set aside: nothing left beside them, storage and connections at their floors; 8 MiB storage a worker"
        );
        assert_eq!(
            described_memory(
                Memory::NoLimit,
                keys(Some(None), Memory::NoLimit),
                256 << 20
            ),
            "memory: no limit; keys 256 MiB (no limit to work from); 256 MiB storage a worker"
        );
    }

    /// What is read from the process's own cgroup agrees with its files, read by hand.
    #[cfg(target_os = "linux")]
    #[test]
    fn the_pods_limit_is_what_its_cgroup_files_say() {
        let cgroups = std::fs::read_to_string("/proc/self/cgroup").unwrap_or_default();
        let expected = cgroup_of(&cgroups).map_or(Memory::NoLimit, |own| {
            let directory = std::path::Path::new(CGROUPS).join(own.trim_start_matches('/'));
            let file = |name: &str| {
                std::fs::read_to_string(directory.join(name))
                    .ok()
                    .and_then(|text| text.trim().parse::<u64>().ok())
            };
            lower(file("memory.max"), file("memory.high"))
        });
        assert_eq!(memory(), expected);
    }
}
