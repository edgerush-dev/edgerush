//! A namespace's EndpointSlices as a list and a watch keep them: what to ask the API server
//! next, and the slices its answers leave.
//!
//! The protocol is Kubernetes' own. A list, read a page at a time, gives every slice and
//! the version it is as of; a watch from that version gives each change after it, and
//! bookmarks that move the version on without one. A watch that ends is begun again from
//! the last version. A version the server no longer has (410 Gone) means the changes since
//! are lost, so the slices are listed afresh. Until such a list has been read whole, the
//! slices it replaces are kept, so that the endpoints in use stay in use while it lasts.

use k8s_openapi::List;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::WatchEvent;
use std::collections::BTreeMap;

/// A namespace's EndpointSlices, kept by a list and a watch.
#[derive(Debug, Clone, Default)]
pub struct Watch {
    /// The slices, by name, as of `version`.
    slices: BTreeMap<String, EndpointSlice>,
    /// The version the slices are as of, which a watch resumes from: none until a list has
    /// been read whole, and none again while one is needed.
    version: Option<String>,
    /// A list being read: the slices of its pages so far, and the token of the next.
    listing: Option<Listing>,
    /// Whether a list has been read whole.
    listed: bool,
}

/// A list part read.
#[derive(Debug, Clone)]
struct Listing {
    slices: BTreeMap<String, EndpointSlice>,
    next: String,
}

/// What to ask the API server for next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    /// A page of a list: the first, or the one this token continues to.
    List {
        /// The token the last page gave, none for the first.
        continue_from: Option<String>,
    },
    /// A watch from this version.
    Watch {
        /// The version the slices are as of.
        from: String,
    },
}

/// An answer the watch cannot take.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum WatchError {
    /// The server no longer has the version or the list asked from (410 Gone): the slices
    /// are listed afresh, those kept until the list is read whole.
    #[error("the version asked from is gone: listing afresh")]
    Gone,
    /// A list's last page, or a change, that says no version: the slices are listed afresh.
    #[error("no resource version: listing afresh")]
    NoVersion,
    /// A watch event while no watch is open, which a client never asks for: passed over.
    #[error("a change while not watching")]
    NotWatching,
    /// An error the server sent in the watch: the watch is begun again from the version
    /// the slices are as of.
    #[error("the watch failed: {0}")]
    Failed(String),
}

impl Watch {
    /// A watch that has listed nothing yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// What to ask for next.
    #[must_use]
    pub fn request(&self) -> Request {
        match (&self.listing, &self.version) {
            (Some(listing), _) => Request::List {
                continue_from: Some(listing.next.clone()),
            },
            (None, Some(version)) => Request::Watch {
                from: version.clone(),
            },
            (None, None) => Request::List {
                continue_from: None,
            },
        }
    }

    /// Whether a list has been read whole: the slices are the namespace's, as of some
    /// version, rather than none yet.
    #[must_use]
    pub fn listed(&self) -> bool {
        self.listed
    }

    /// The slices.
    pub fn slices(&self) -> impl Iterator<Item = &EndpointSlice> {
        self.slices.values()
    }

    /// A page of the list asked for. Whether the slices changed: only at the last page,
    /// when the list replaces them whole.
    ///
    /// # Errors
    ///
    /// [`WatchError::NoVersion`] for a last page that says no version, and the list begins
    /// again.
    pub fn page(&mut self, page: List<EndpointSlice>) -> Result<bool, WatchError> {
        let mut slices = self
            .listing
            .take()
            .map(|listing| listing.slices)
            .unwrap_or_default();
        // A slice without a name, which Kubernetes never sends, is passed over.
        slices.extend(
            page.items
                .into_iter()
                .filter_map(|slice| Some((slice.metadata.name.clone()?, slice))),
        );
        let metadata = page.metadata;
        if let Some(next) = metadata.continue_.filter(|next| !next.is_empty()) {
            self.listing = Some(Listing { slices, next });
            return Ok(false);
        }
        let Some(version) = metadata.resource_version.filter(|v| !v.is_empty()) else {
            self.version = None;
            return Err(WatchError::NoVersion);
        };
        self.version = Some(version);
        self.listed = true;
        let changed = slices != self.slices;
        self.slices = slices;
        Ok(changed)
    }

    /// A list or a watch the server answered 410 Gone: the version, or the list's token,
    /// is too old. The slices are listed afresh, and kept until that list is read whole.
    pub fn gone(&mut self) {
        self.listing = None;
        self.version = None;
    }

    /// An event of the watch. Whether the slices changed.
    ///
    /// # Errors
    ///
    /// [`WatchError::Gone`] and [`WatchError::NoVersion`] when the slices must be listed
    /// afresh, which [`Watch::request`] then asks for; [`WatchError::Failed`] for an error
    /// the server sent, the watch to be begun again from where it was;
    /// [`WatchError::NotWatching`] for an event while no watch is open, passed over.
    pub fn event(&mut self, event: WatchEvent<EndpointSlice>) -> Result<bool, WatchError> {
        if self.listing.is_some() || self.version.is_none() {
            return Err(WatchError::NotWatching);
        }
        let (slice, deleted) = match event {
            WatchEvent::Added(slice) | WatchEvent::Modified(slice) => (slice, false),
            WatchEvent::Deleted(slice) => (slice, true),
            WatchEvent::Bookmark {
                resource_version, ..
            } => {
                if resource_version.is_empty() {
                    self.version = None;
                    return Err(WatchError::NoVersion);
                }
                self.version = Some(resource_version);
                return Ok(false);
            }
            WatchEvent::ErrorStatus(status) => {
                if status.code == Some(410) {
                    self.gone();
                    return Err(WatchError::Gone);
                }
                return Err(WatchError::Failed(
                    status.message.unwrap_or_else(|| "no message".to_owned()),
                ));
            }
            WatchEvent::ErrorOther(_) => {
                return Err(WatchError::Failed("an error of no known kind".to_owned()));
            }
        };
        let Some(version) = slice
            .metadata
            .resource_version
            .clone()
            .filter(|v| !v.is_empty())
        else {
            self.version = None;
            return Err(WatchError::NoVersion);
        };
        self.version = Some(version);
        // A slice without a name, which Kubernetes never sends, is passed over.
        let Some(name) = slice.metadata.name.clone() else {
            return Ok(false);
        };
        let changed = if deleted {
            self.slices.remove(&name).is_some()
        } else {
            self.slices.insert(name, slice.clone()).as_ref() != Some(&slice)
        };
        Ok(changed)
    }
}

#[cfg(test)]
mod tests;
