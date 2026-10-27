//! The config stream between a control plane and its data planes
//! ([02 §4](../../../docs/02-architecture.md)): its messages, what the control plane sends
//! each data plane, and what a data plane makes of what it is sent.
//!
//! A pure crate: no I/O, no clocks. The stream itself, gRPC over mTLS, is its callers'.
//!
//! A data plane opens the stream by saying what it holds ([`Held`]). The control plane
//! sends it what it lacks of the latest [`Offer`], a resource a message: each certificate
//! at a version it does not hold, then the config, which names each certificate it needs at
//! its version, then the names of certificates no longer offered ([`Session`]). The stream is ordered and lasts until it breaks, so what is sent
//! is taken as held; a data plane that dials again says again what it holds. The data
//! plane holds the latest config until it has every certificate the config names, at the
//! version it names, then runs the two together, whole, and answers with what it runs and the config it refused,
//! if it refused one ([`Report`], [`Staging`]). A config equal to the one running, with
//! certificates equal to its own, is kept, and acked under its new version. A certificate
//! the running config uses stays with it until another config runs.

use std::collections::BTreeMap;

mod offer;
mod session;
mod staging;
pub mod wire;

pub use offer::{Offer, OfferError, Versioned, Versions};
pub use session::Session;
pub use staging::{Staging, Taken};

/// What a data plane holds, or runs: a config's version and each certificate's.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Held {
    /// The config's version; none before any.
    pub config: Option<String>,
    /// Each certificate's version, by name.
    pub certificates: BTreeMap<String, String>,
}

/// What a data plane says after a config it was sent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Report {
    /// The config it runs, and the certificates that config uses.
    pub running: Held,
    /// The latest config, if it refused it.
    pub refused: Option<Refusal>,
}

/// A config a data plane refused, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    /// The config's version.
    pub config: String,
    /// Why.
    pub why: String,
}

/// A message the stream's protocol does not allow: the stream is given up, and dialled
/// again.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Malformed {
    /// The first message was not what the data plane holds.
    #[error("the stream did not open with what the data plane holds")]
    NoHello,
    /// What the data plane holds, said again.
    #[error("the data plane said again what it holds")]
    HelloAgain,
    /// A resource without its version.
    #[error("a resource without a version")]
    Unversioned,
    /// A certificate without its name.
    #[error("a certificate without a name")]
    Unnamed,
}

impl From<&Held> for wire::Held {
    fn from(held: &Held) -> Self {
        Self {
            config: held.config.clone().unwrap_or_default(),
            certificates: held.certificates.clone(),
        }
    }
}

impl TryFrom<wire::Held> for Held {
    type Error = Malformed;

    fn try_from(held: wire::Held) -> Result<Self, Malformed> {
        for (name, version) in &held.certificates {
            if name.is_empty() {
                return Err(Malformed::Unnamed);
            }
            if version.is_empty() {
                return Err(Malformed::Unversioned);
            }
        }
        Ok(Self {
            config: Some(held.config).filter(|version| !version.is_empty()),
            certificates: held.certificates,
        })
    }
}

impl From<&Report> for wire::FromDataPlane {
    fn from(report: &Report) -> Self {
        Self {
            said: Some(wire::Said::Report(wire::Report {
                running: Some((&report.running).into()),
                refused: report.refused.as_ref().map(|refusal| wire::Refusal {
                    config: refusal.config.clone(),
                    why: refusal.why.clone(),
                }),
            })),
        }
    }
}

impl TryFrom<wire::Report> for Report {
    type Error = Malformed;

    fn try_from(report: wire::Report) -> Result<Self, Malformed> {
        let refused = match report.refused {
            Some(refusal) if refusal.config.is_empty() => return Err(Malformed::Unversioned),
            Some(refusal) => Some(Refusal {
                config: refusal.config,
                why: refusal.why,
            }),
            None => None,
        };
        Ok(Self {
            running: report.running.unwrap_or_default().try_into()?,
            refused,
        })
    }
}

#[cfg(test)]
mod tests;
