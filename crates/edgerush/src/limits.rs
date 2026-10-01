//! The open files and the memory the process may hold, and what that allows each worker:
//! connections, and storage for requests ([03 §9] in the docs).
//!
//! A container is given the open-file limits of whatever starts it, and under containerd
//! 2.x that is a soft limit of 1,024 with a hard limit of 524,288: about a thousand
//! sockets for the whole process, clients and upstreams together, and nothing in a pod's
//! spec can change it. So the process raises its soft limit to its hard limit as it
//! starts, as Go's runtime does for every Go program and HAProxy does for itself. The hard
//! limit is the administrator's, and it is never asked for more.
//!
//! What a worker may then hold follows from what was got, so that a worker stops at its
//! cap — where the fair share between listeners and the pause that costs nothing are —
//! rather than at the descriptors running out, where accepting fails and is tried again.
//!
//! A pod's memory limit is enforced by the kernel through its cgroup: at `memory.max` the
//! OOM killer ends the process, every connection with it, and at `memory.high`, where one
//! is set, the pod is throttled with reclaim. A worker's storage for requests is a fixed
//! number that knows nothing of either, so it is sized from the lower of the two as the
//! process starts, as Envoy Gateway sizes Envoy's heap from the pod's limit: half of it,
//! shared among the workers, for what the storage account counts, and half for what it does
//! not — connection and TLS state, the kernel's socket buffers, what the allocator keeps.
//! Never more than a worker's own bound, which is also what it has without a limit.
//!
//! [03 §9]: ../../../docs/03-data-plane.md

use crate::per_core::CONNECTIONS_PER_WORKER;
use edgerush_proxy::H1Limits;
use std::num::NonZeroUsize;

/// The least storage a worker is given, however small the limit: room for 128 heads of the
/// largest size, so that no limit leaves a worker unable to read a request.
const STORAGE_FLOOR: usize = 8 * 1024 * 1024;

/// Where the pod's limit is read, in a cgroup namespace or not.
#[cfg(target_os = "linux")]
const CGROUPS: &str = "/sys/fs/cgroup";

/// Open files kept for what is not a client's connection: the listening sockets, the
/// scrape's, the health checker's probes, files. Never more than half of a small limit.
const RESERVE: u64 = 1024;

/// Open files a client's connection takes: its own, and in most cases one to its upstream.
const PER_CONNECTION: u64 = 2;

/// What became of the open-file limit at the start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OpenFiles {
    /// The platform has no such limit (Windows).
    #[cfg_attr(unix, allow(dead_code, reason = "Unix has a limit"))]
    NoLimit,
    /// The soft limit was `was`, and is `now`; `None` is no limit at all.
    #[cfg_attr(not(unix), allow(dead_code, reason = "only Unix has a limit to raise"))]
    Raised { was: Option<u64>, now: Option<u64> },
    /// The soft limit is `now`, and could not be raised to the hard limit.
    #[cfg_attr(not(unix), allow(dead_code, reason = "only Unix has a limit to keep"))]
    Kept {
        now: Option<u64>,
        why: std::io::ErrorKind,
    },
}

impl OpenFiles {
    /// The soft limit the process runs under, `None` where there is none.
    fn limit(self) -> Option<u64> {
        match self {
            Self::NoLimit => None,
            Self::Raised { now, .. } | Self::Kept { now, .. } => now,
        }
    }
}

/// Raises the soft open-file limit to the hard limit, where the platform has one.
#[cfg(unix)]
pub(crate) fn raise_open_files() -> OpenFiles {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let before = getrlimit(Resource::Nofile);
    // A hard limit of none cannot be the soft one: Linux holds the soft limit to
    // `fs.nr_open`, 1,048,576 unless changed.
    let wanted = before.maximum.or(Some(1 << 20));
    // `None` is no limit at all: there is nothing to raise it to.
    if before.current.is_none() || before.current >= wanted {
        return OpenFiles::Raised {
            was: before.current,
            now: before.current,
        };
    }
    let raised = Rlimit {
        current: wanted,
        maximum: before.maximum,
    };
    match setrlimit(Resource::Nofile, raised) {
        Ok(()) => OpenFiles::Raised {
            was: before.current,
            now: getrlimit(Resource::Nofile).current,
        },
        Err(error) => OpenFiles::Kept {
            now: before.current,
            why: std::io::Error::from(error).kind(),
        },
    }
}

/// There is no open-file limit to raise.
#[cfg(not(unix))]
pub(crate) fn raise_open_files() -> OpenFiles {
    OpenFiles::NoLimit
}

/// How many connections each of `workers` may hold under `open_files`: what the limit
/// leaves after the reserve, two open files to a connection, shared out — never more than
/// [`CONNECTIONS_PER_WORKER`], and never none.
pub(crate) fn connections_per_worker(open_files: OpenFiles, workers: NonZeroUsize) -> usize {
    let Some(limit) = open_files.limit() else {
        return CONNECTIONS_PER_WORKER;
    };
    let usable = limit - RESERVE.min(limit / 2);
    let workers = u64::try_from(workers.get()).unwrap_or(u64::MAX);
    let each = usable / PER_CONNECTION / workers;
    usize::try_from(each)
        .unwrap_or(usize::MAX)
        .clamp(1, CONNECTIONS_PER_WORKER)
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

/// What each of `workers` may hold for requests under `memory`: half of the limit shared
/// out, never more than a worker's own bound nor less than [`STORAGE_FLOOR`].
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

/// What the start says of the pod's memory and the storage it allows each worker.
pub(crate) fn described_memory(memory: Memory, storage: usize) -> String {
    let each = format!("{} MiB storage a worker", storage / (1024 * 1024));
    match memory {
        Memory::NoLimit => format!("memory: no limit; {each}"),
        Memory::Limit { bytes, from } => {
            format!(
                "memory: limit {} MiB ({from}); {each}",
                bytes / (1024 * 1024)
            )
        }
    }
}

/// What the start says of the open files and the connections they allow each worker.
pub(crate) fn described(open_files: OpenFiles, connections: usize) -> String {
    let each = format!("{connections} connections a worker");
    let shown = |limit: Option<u64>| limit.map_or("none".to_owned(), |limit| limit.to_string());
    match open_files {
        OpenFiles::NoLimit => each,
        OpenFiles::Raised { was, now } if was == now => {
            format!("open files: soft limit {}; {each}", shown(now))
        }
        OpenFiles::Raised { was, now } => format!(
            "open files: soft limit {} raised to {}; {each}",
            shown(was),
            shown(now)
        ),
        OpenFiles::Kept { now, why } => format!(
            "open files: soft limit {}, not raised ({why}); {each}",
            shown(now)
        ),
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

    #[test]
    fn a_limit_high_enough_leaves_the_cap_as_it_is() {
        // containerd 2.x's hard limit, raised to: room for the whole cap on up to seven
        // workers, and on eight a little less, (524,288 - 1,024) / 2 / 8.
        assert_eq!(
            connections_per_worker(limit(524_288), workers(7)),
            CONNECTIONS_PER_WORKER
        );
        assert_eq!(connections_per_worker(limit(524_288), workers(8)), 32_704);
        assert_eq!(
            connections_per_worker(OpenFiles::NoLimit, workers(64)),
            CONNECTIONS_PER_WORKER
        );
        let unlimited = OpenFiles::Raised {
            was: None,
            now: None,
        };
        assert_eq!(
            connections_per_worker(unlimited, workers(4)),
            CONNECTIONS_PER_WORKER
        );
    }

    #[test]
    fn a_lower_limit_is_shared_out_after_the_reserve_two_files_to_a_connection() {
        // (65,536 - 1,024) / 2 / 4
        assert_eq!(connections_per_worker(limit(65_536), workers(4)), 8_064);
        // More workers than that leaves less to each.
        assert_eq!(connections_per_worker(limit(524_288), workers(16)), 16_352);
    }

    #[test]
    fn a_small_limit_keeps_half_for_the_reserve_and_never_leaves_a_worker_none() {
        // The 1,024 a process is given if it cannot raise it: 512 kept, 256 connections.
        assert_eq!(connections_per_worker(limit(1024), workers(1)), 256);
        assert_eq!(connections_per_worker(limit(1024), workers(4)), 64);
        let kept = OpenFiles::Kept {
            now: Some(16),
            why: std::io::ErrorKind::PermissionDenied,
        };
        assert_eq!(connections_per_worker(kept, workers(64)), 1);
        assert_eq!(connections_per_worker(limit(0), workers(1)), 1);
    }

    #[test]
    fn the_start_says_what_became_of_the_limit_and_what_each_worker_may_hold() {
        assert_eq!(
            described(limit(524_288), 32_768),
            "open files: soft limit 1024 raised to 524288; 32768 connections a worker"
        );
        let same = OpenFiles::Raised {
            was: Some(4096),
            now: Some(4096),
        };
        assert_eq!(
            described(same, 1_792),
            "open files: soft limit 4096; 1792 connections a worker"
        );
        let kept = OpenFiles::Kept {
            now: Some(1024),
            why: std::io::ErrorKind::PermissionDenied,
        };
        assert_eq!(
            described(kept, 256),
            "open files: soft limit 1024, not raised (permission denied); 256 connections a worker"
        );
        assert_eq!(
            described(OpenFiles::NoLimit, 32_768),
            "32768 connections a worker"
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

    #[test]
    fn the_start_says_what_the_pod_may_use_and_what_each_worker_may_hold() {
        assert_eq!(
            described_memory(limited(512 * MIB), 64 << 20),
            "memory: limit 512 MiB (memory.max); 64 MiB storage a worker"
        );
        assert_eq!(
            described_memory(Memory::NoLimit, 256 << 20),
            "memory: no limit; 256 MiB storage a worker"
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

    /// The soft limit is the hard limit once it has been raised, or as near as the kernel
    /// lets it be where the hard limit is none.
    #[cfg(unix)]
    #[test]
    fn raising_takes_the_soft_limit_to_the_hard_one() {
        use rustix::process::{Resource, getrlimit};
        let opened = raise_open_files();
        let now = getrlimit(Resource::Nofile);
        assert!(matches!(opened, OpenFiles::Raised { .. }), "{opened:?}");
        match now.maximum {
            Some(hard) => assert_eq!(now.current, Some(hard)),
            None => assert!(now.current >= Some(1 << 20), "{now:?}"),
        }
        assert_eq!(opened.limit(), now.current);
    }
}
