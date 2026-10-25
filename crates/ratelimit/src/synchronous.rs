//! The synchronous mode's owner (06 §4): a service pod deciding every request under the keys
//! it owns, as data planes ask, and copying what changed to the pods that would take over
//! from it, once a cycle of its own.
//!
//! Its keys are local mode's store ([`Local`]), tracking changes: a bucket a key and nothing
//! else — no record of any data plane, since a decision is its own charge — so that a
//! pod's connections decide on every core at once. A request whose keys have several
//! owners asks each of them in parallel; one that admitted it while another refused is
//! given the charge back ([`GiveBack`]). A data plane never sends a request again: past its
//! time it decides as its config says, so nothing here remembers requests.

#[cfg(test)]
mod tests;

pub use crate::local::{Copy, Decision, Handles, Key, Shared};

use crate::local::Local;
use crate::service::Contract;
use std::hash::BuildHasher;

/// A request to decide: its keys that this owner holds, each with its contract, admitted
/// under all of them or none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Decide {
    /// Its number among the data plane's requests on the stream, for the answer to name.
    pub id: u64,
    /// The keys, with their contracts.
    pub keys: Vec<(Box<[u8]>, Contract)>,
}

/// An owner's answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Decided {
    /// The request's number.
    pub id: u64,
    /// Admitted, or refused and by which of its keys here, and for how long.
    pub decision: Decision,
}

/// Charges to give back: a request this owner admitted that another refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GiveBack {
    /// The keys it was admitted under here, with their contracts.
    pub keys: Vec<(Box<[u8]>, Contract)>,
}

/// The keys a pod holds, as owner and as the pod that would take over.
#[derive(Debug)]
pub struct Owner<S> {
    keys: Local<S>,
}

impl<S: BuildHasher + Clone> Owner<S> {
    /// No keys yet, hashed by `hasher`, decided through `connections` sets of handles.
    #[must_use]
    pub fn new(hasher: S, connections: u64) -> Self {
        Self {
            keys: Local::tracking(hasher, connections),
        }
    }

    /// Decides `request` at `now`, through a connection's `handles`.
    pub fn decide(&self, handles: &mut Handles<S>, request: &Decide, now: u64) -> Decided {
        Decided {
            id: request.id,
            decision: self.keys.decide(handles, &request.keys, now),
        }
    }

    /// Gives back at `now` what `back` names: one request's charge under each key.
    pub fn give_back(&self, handles: &mut Handles<S>, back: &GiveBack, now: u64) {
        self.keys.give_back(handles, &back.keys, now);
    }

    /// The state at `now` of the keys connections changed since their last copy, gathered
    /// from their handles ([`Handles::take_changed`]): what goes to the other pods, once a
    /// cycle.
    #[must_use]
    pub fn copies(&self, changed: &[Shared], now: u64) -> Vec<Copy> {
        self.keys.copies(changed, now)
    }

    /// Takes in another pod's `copies` at `now`, to carry on from if it goes.
    pub fn install(&self, copies: &[Copy], now: u64) {
        self.keys.install(copies, now);
    }

    /// The keys themselves, for the sweep, the cap and what is counted.
    #[must_use]
    pub fn keys(&self) -> &Local<S> {
        &self.keys
    }
}
