//! What the control plane offers, and the versions it names it by.

use crate::wire;
use edgerush_config::{Certificate, Config};
use prost::Message;
use std::collections::BTreeMap;

/// The longest message either end of the stream sends or takes: 64 MiB, for a config is
/// sent whole, in one.
pub const MAX_MESSAGE: usize = 64 << 20;

/// Names resources' versions: a count from the control plane's start, marked with when
/// it started, so that two of its runs never name two resources alike (02 §4).
#[derive(Debug, Clone)]
pub struct Versions {
    start: u64,
    count: u64,
}

impl Versions {
    /// Versions for a control plane that started at `start`, in nanoseconds since the
    /// Unix epoch.
    pub fn new(start: u64) -> Self {
        Self { start, count: 0 }
    }

    fn next(&mut self) -> String {
        self.count += 1;
        format!("{}.{}", self.start, self.count)
    }
}

/// A resource at its version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Versioned<T> {
    /// Its version.
    pub version: String,
    /// The resource.
    pub value: T,
}

/// What the control plane offers its data planes: the latest config, and each certificate
/// it names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Offer {
    /// The config, without its certificates.
    pub(crate) config: Versioned<Config>,
    /// The config as sent.
    pub(crate) json: Vec<u8>,
    /// Its certificates, by name.
    pub(crate) certificates: BTreeMap<String, Versioned<Certificate>>,
}

/// A config that cannot be offered.
#[derive(Debug, thiserror::Error)]
pub enum OfferError {
    /// Limits travel apart, as a resource of their own (07 §2), which the stream does not
    /// carry yet.
    #[error("a config with rate limits cannot be sent yet")]
    Limits,
    /// The config could not be written as JSON.
    #[error("the config cannot be written: {0}")]
    Written(#[from] serde_json::Error),
    /// A resource, written, is longer than a message may be.
    #[error("{what} is {bytes} bytes written, more than the {limit} a message may hold")]
    TooLarge {
        /// What: the config, or a certificate by name.
        what: String,
        /// How long its message is.
        bytes: usize,
        /// How long one may be.
        limit: usize,
    },
}

impl Offer {
    /// The offer of `config`: the versions of `last` for what has not changed since, and
    /// new ones from `versions` for what has.
    pub fn new(
        config: &Config,
        last: Option<&Offer>,
        versions: &mut Versions,
    ) -> Result<Self, OfferError> {
        Self::within(config, last, versions, MAX_MESSAGE)
    }

    /// As [`Offer::new`], each message at most `limit` long.
    pub(crate) fn within(
        config: &Config,
        last: Option<&Offer>,
        versions: &mut Versions,
        limit: usize,
    ) -> Result<Self, OfferError> {
        if config.limits.is_some() {
            return Err(OfferError::Limits);
        }
        let mut bare = config.clone();
        let mut certificates = BTreeMap::new();
        for (name, certificate) in std::mem::take(&mut bare.certificates) {
            let versioned = match last.and_then(|last| last.certificates.get(&name)) {
                Some(kept) if kept.value == certificate => kept.clone(),
                _ => Versioned {
                    version: versions.next(),
                    value: certificate,
                },
            };
            certificates.insert(name, versioned);
        }
        // A config names the versions of its certificates, so one changed is a new config.
        let (config, json) = match last {
            Some(last) if last.config.value == bare && last.certificates == certificates => {
                (last.config.clone(), last.json.clone())
            }
            _ => {
                let json = serde_json::to_vec(&bare)?;
                let config = Versioned {
                    version: versions.next(),
                    value: bare,
                };
                (config, json)
            }
        };
        let offer = Self {
            config,
            json,
            certificates,
        };
        // Refused here, where it can be said, rather than by the stream as it is sent.
        let too_large = |what: String, bytes: usize| OfferError::TooLarge { what, bytes, limit };
        for message in crate::Session::holding_nothing().send(&offer) {
            let bytes = message.encoded_len();
            if bytes > limit {
                return Err(match message.resource {
                    Some(wire::Resource::Certificate(certificate)) => {
                        too_large(format!("the certificate {}", certificate.name), bytes)
                    }
                    _ => too_large("the config".to_owned(), bytes),
                });
            }
        }
        Ok(offer)
    }

    /// The config's version.
    pub fn version(&self) -> &str {
        &self.config.version
    }

    /// Each certificate's version, by name.
    pub fn certificates(&self) -> impl Iterator<Item = (&str, &str)> {
        self.certificates
            .iter()
            .map(|(name, certificate)| (name.as_str(), certificate.version.as_str()))
    }
}
