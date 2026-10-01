//! The open files the process may hold, and the connections that allows each worker
//! ([03 §9] in the docs).
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
//! [03 §9]: ../../../docs/03-data-plane.md

use crate::per_core::CONNECTIONS_PER_WORKER;
use std::num::NonZeroUsize;

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
