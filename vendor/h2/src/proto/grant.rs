//! EdgeRush: what a connection may allocate for what its peer sends before its owner next
//! looks (15 §3 of the design docs).
//!
//! A server's owner charges the memory a connection holds to an account of its own, and h2
//! allocates as frames arrive, before anyone could refuse them. So the owner reserves an
//! amount before each poll and sets it here; each allocation for the peer's streams and
//! header blocks is taken from it before it is made, and one it cannot cover is refused.
//! After the poll the owner charges what h2 then holds and lets the rest go.

use std::sync::atomic::{AtomicUsize, Ordering};

/// The amount that means no limit: a client's, and a server's whose owner charges nothing.
const UNLIMITED: usize = usize::MAX;

#[derive(Debug)]
pub(crate) struct Grant {
    left: AtomicUsize,
    /// The largest allocation refused since the owner last asked.
    wanted: AtomicUsize,
}

impl Grant {
    pub(crate) fn unlimited() -> Self {
        Grant {
            left: AtomicUsize::new(UNLIMITED),
            wanted: AtomicUsize::new(0),
        }
    }

    /// What may be allocated from now on.
    pub(crate) fn set(&self, bytes: usize) {
        self.left.store(bytes, Ordering::Relaxed);
    }

    /// Takes `bytes` for an allocation about to be made, if they are left; if not, notes
    /// them as wanted.
    pub(crate) fn take(&self, bytes: usize) -> bool {
        let taken = self
            .left
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                if left == UNLIMITED {
                    Some(UNLIMITED)
                } else {
                    left.checked_sub(bytes)
                }
            })
            .is_ok();
        if !taken {
            self.wanted.fetch_max(bytes, Ordering::Relaxed);
        }
        taken
    }

    /// The largest allocation refused since this was last asked, or 0.
    pub(crate) fn take_wanted(&self) -> usize {
        self.wanted.swap(0, Ordering::Relaxed)
    }

    /// Gives back `bytes` taken for what has since been let go of.
    pub(crate) fn give(&self, bytes: usize) {
        let _ = self
            .left
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |left| {
                Some(if left == UNLIMITED {
                    UNLIMITED
                } else {
                    left.saturating_add(bytes).min(UNLIMITED - 1)
                })
            });
    }
}

/// What decoded header fields hold, by allocation rather than by RFC 9113's measure of a
/// header list: a map's capacity, used or not, an allocation for each name and value (the
/// allocator rounds each up and keeps room of its own), and the target's pieces. Standard
/// names cost nothing and are counted anyway, which errs towards refusing.
pub(crate) mod held {
    use http::header::{HeaderMap, HeaderName, HeaderValue};
    use http::{Request, Response, Uri};

    /// The allocator's own room for one allocation, and what it rounds a small one up by.
    pub(crate) const ALLOCATION: usize = 16;

    /// What one place in a header map takes: its entry (name, value, hash, links) and its
    /// slot in the map's index.
    pub(crate) const FIELD: usize =
        std::mem::size_of::<HeaderName>() + std::mem::size_of::<HeaderValue>() + 16;

    pub(crate) fn map(map: &HeaderMap) -> usize {
        let fields: usize = map
            .iter()
            .map(|(name, value)| name.as_str().len() + value.len() + 2 * ALLOCATION)
            .sum();
        map.capacity().saturating_mul(FIELD) + fields + ALLOCATION
    }

    /// Each piece an allocation of its own, as the pseudo-field it was made from was: what a
    /// stream is charged for its head as it opens is never less than what it then holds.
    pub(crate) fn uri(uri: &Uri) -> usize {
        pseudo(uri.path_and_query().map(|path| path.as_str()))
            + pseudo(uri.authority().map(|authority| authority.as_str()))
            + pseudo(uri.scheme_str())
    }

    pub(crate) fn request<B>(request: &Request<B>) -> usize {
        map(request.headers()) + uri(request.uri())
    }

    pub(crate) fn response(response: &Response<()>) -> usize {
        map(response.headers())
    }

    /// A pseudo-field's value, each an allocation of its own.
    pub(crate) fn pseudo(value: Option<&str>) -> usize {
        value.map_or(0, |value| value.len() + ALLOCATION)
    }
}
