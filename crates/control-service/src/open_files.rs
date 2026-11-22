//! The open-file limit, raised as a process starts ([03 §9](../../../docs/03-data-plane.md)).
//!
//! A container is given the open-file limits of whatever starts it, and under containerd
//! 2.x that is a soft limit of 1,024 with a hard limit of 524,288: about a thousand
//! sockets for the whole process, and nothing in a pod's spec can change it. So each of
//! EdgeRush's processes — the data plane, the controller and the rate-limiting service —
//! raises its soft limit to its hard limit as it starts, before it opens anything, as Go's
//! runtime does for every Go program and HAProxy does for itself. The hard limit is the
//! administrator's, and it is never asked for more.

/// What became of the open-file limit at the start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenFiles {
    /// The platform has no such limit (Windows).
    NoLimit,
    /// The soft limit was `was`, and is `now`; `None` is no limit at all.
    Raised {
        /// The soft limit found.
        was: Option<u64>,
        /// The soft limit now.
        now: Option<u64>,
    },
    /// The soft limit is `now`, and could not be raised to the hard limit.
    Kept {
        /// The soft limit, as found.
        now: Option<u64>,
        /// Why it could not be raised.
        why: std::io::ErrorKind,
    },
}

impl OpenFiles {
    /// The soft limit the process runs under, `None` where there is none.
    #[must_use]
    pub fn limit(self) -> Option<u64> {
        match self {
            Self::NoLimit => None,
            Self::Raised { now, .. } | Self::Kept { now, .. } => now,
        }
    }

    /// What a start says of the open files: the soft limit found, and what it was raised
    /// to or why not. Nothing where the platform has no limit.
    #[must_use]
    pub fn described(self) -> Option<String> {
        let shown = |limit: Option<u64>| limit.map_or("none".to_owned(), |limit| limit.to_string());
        match self {
            Self::NoLimit => None,
            Self::Raised { was, now } if was == now => {
                Some(format!("open files: soft limit {}", shown(now)))
            }
            Self::Raised { was, now } => Some(format!(
                "open files: soft limit {} raised to {}",
                shown(was),
                shown(now)
            )),
            Self::Kept { now, why } => Some(format!(
                "open files: soft limit {}, not raised ({why})",
                shown(now)
            )),
        }
    }
}

/// Raises the soft open-file limit to the hard limit, where the platform has one.
#[cfg(unix)]
#[must_use]
pub fn raise() -> OpenFiles {
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
#[must_use]
pub fn raise() -> OpenFiles {
    OpenFiles::NoLimit
}

#[cfg(test)]
mod tests;
