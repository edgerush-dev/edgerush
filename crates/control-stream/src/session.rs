//! The control plane's side of one data plane's stream.

use crate::encode::{CONFIG_VERSION, ConfigVersion, OLDEST_CONFIG_VERSION};
use crate::{Held, Malformed, Offer, Report, wire};

/// One data plane's stream, at the control plane: what the data plane is taken to hold,
/// and so what it is to be sent.
#[derive(Debug)]
pub struct Session {
    held: Held,
    /// The config version it is written: the data plane's, or this control plane's if
    /// that is older.
    version: ConfigVersion,
    /// The data plane's own config version.
    reads: u32,
    /// The gateway's addresses last sent, if any have been.
    gateway: Option<Vec<std::net::IpAddr>>,
}

/// Why a control plane does not serve a stream a data plane opened.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum OpenError {
    /// What the protocol does not allow.
    #[error(transparent)]
    Malformed(#[from] Malformed),
    /// A data plane older than this control plane still writes for.
    #[error(
        "config version {version} is no longer written: this control plane writes {oldest} to {newest}"
    )]
    DataPlaneTooOld {
        /// The data plane's.
        version: u32,
        /// The oldest this control plane writes.
        oldest: u32,
        /// This control plane's own.
        newest: u32,
    },
    /// A data plane newer than this control plane can write for.
    #[error(
        "the data plane reads config version {oldest} and after, and this control plane writes {newest} at most"
    )]
    ControlPlaneTooOld {
        /// The oldest the data plane reads.
        oldest: u32,
        /// This control plane's own.
        newest: u32,
    },
}

impl Session {
    /// The stream a data plane opened with `first`, which says what it holds, its config
    /// version and the oldest it still reads. It is written at the older of its version and
    /// this control plane's, and refused if that is one either end no longer has.
    pub fn open(first: wire::FromDataPlane) -> Result<Self, OpenError> {
        let Some(wire::Said::Hello(hello)) = first.said else {
            return Err(Malformed::NoHello.into());
        };
        let written = hello.config_version.min(CONFIG_VERSION);
        if written < hello.oldest_config_version {
            return Err(OpenError::ControlPlaneTooOld {
                oldest: hello.oldest_config_version,
                newest: CONFIG_VERSION,
            });
        }
        let version = ConfigVersion::served(written).ok_or(OpenError::DataPlaneTooOld {
            version: hello.config_version,
            oldest: OLDEST_CONFIG_VERSION,
            newest: CONFIG_VERSION,
        })?;
        Ok(Self {
            held: hello.held.unwrap_or_default().try_into()?,
            version,
            reads: hello.config_version,
            gateway: None,
        })
    }

    /// A stream to a data plane at `version` that holds nothing.
    pub(crate) fn holding_nothing(version: ConfigVersion) -> Self {
        Self {
            held: Held::default(),
            version,
            reads: version.get(),
            gateway: None,
        }
    }

    /// The config version the data plane is written.
    pub fn version(&self) -> ConfigVersion {
        self.version
    }

    /// The data plane's own config version.
    pub fn reads(&self) -> u32 {
        self.reads
    }

    /// What the data plane is taken to hold.
    pub fn held(&self) -> &Held {
        &self.held
    }

    /// What to send for `offer`, in order: each certificate the data plane does not hold
    /// at its version, then the config, if it does not hold it, then the names of the
    /// certificates no longer offered. Once sent, it is taken as held.
    pub fn send(&mut self, offer: &Offer) -> Vec<wire::ToDataPlane> {
        let mut messages = Vec::new();
        for (name, certificate) in &offer.certificates {
            if self.held.certificates.get(name) == Some(&certificate.version) {
                continue;
            }
            messages.push(wire::ToDataPlane {
                resource: Some(wire::Resource::Certificate(wire::Certificate {
                    name: name.clone(),
                    version: certificate.version.clone(),
                    chain: certificate.value.chain.clone(),
                    key: certificate.value.key.clone(),
                })),
            });
            self.held
                .certificates
                .insert(name.clone(), certificate.version.clone());
        }
        if self.held.config.as_ref() != Some(&offer.config.version) {
            messages.push(wire::ToDataPlane {
                resource: Some(wire::Resource::Config(wire::Config {
                    version: offer.config.version.clone(),
                    // One for each version served, so the version's place is in it.
                    json: offer.json[self.version.index()].clone(),
                    requires: offer
                        .certificates
                        .iter()
                        .map(|(name, certificate)| (name.clone(), certificate.version.clone()))
                        .collect(),
                })),
            });
            self.held.config = Some(offer.config.version.clone());
        }
        let removed: Vec<String> = self
            .held
            .certificates
            .keys()
            .filter(|name| !offer.certificates.contains_key(*name))
            .cloned()
            .collect();
        if !removed.is_empty() {
            for name in &removed {
                self.held.certificates.remove(name);
            }
            messages.push(wire::ToDataPlane {
                resource: Some(wire::Resource::Removed(wire::Removed { names: removed })),
            });
        }
        messages
    }

    /// What to send of the gateway's `addresses`: all of them, once on a stream and again
    /// each time they are other than those last sent; none otherwise.
    pub fn gateway(&mut self, addresses: &[std::net::IpAddr]) -> Option<wire::ToDataPlane> {
        if self.gateway.as_deref() == Some(addresses) {
            return None;
        }
        self.gateway = Some(addresses.to_vec());
        Some(wire::ToDataPlane {
            resource: Some(wire::Resource::Gateway(wire::Gateway {
                addresses: addresses.iter().map(ToString::to_string).collect(),
            })),
        })
    }

    /// What a message after the first says: none, from a data plane that says something
    /// this control plane does not know.
    pub fn heard(&self, message: wire::FromDataPlane) -> Result<Option<Report>, Malformed> {
        match message.said {
            Some(wire::Said::Report(report)) => Ok(Some(report.try_into()?)),
            Some(wire::Said::Hello(_)) => Err(Malformed::HelloAgain),
            None => Ok(None),
        }
    }
}
