//! The control plane's side of one data plane's stream.

use crate::{Held, Malformed, Offer, Report, wire};

/// One data plane's stream, at the control plane: what the data plane is taken to hold,
/// and so what it is to be sent.
#[derive(Debug)]
pub struct Session {
    held: Held,
}

impl Session {
    /// The stream a data plane opened with `first`, which says what it holds.
    pub fn open(first: wire::FromDataPlane) -> Result<Self, Malformed> {
        match first.said {
            Some(wire::Said::Hello(held)) => Ok(Self {
                held: held.try_into()?,
            }),
            _ => Err(Malformed::NoHello),
        }
    }

    /// A stream to a data plane that holds nothing.
    pub(crate) fn holding_nothing() -> Self {
        Self {
            held: Held::default(),
        }
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
                    json: offer.json.clone(),
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
