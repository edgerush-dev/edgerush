//! The data plane's side of the stream: what it was sent, held until it can run.

use crate::encode::{CONFIG_VERSION, OLDEST_CONFIG_VERSION};
use crate::{Held, Malformed, Refusal, Report, Versioned, wire};
use edgerush_config::{Certificate, Config};
use std::collections::BTreeMap;

/// What a data plane was sent, and what it runs.
#[derive(Debug, Default)]
pub struct Staging {
    /// The certificates offered, by name.
    certificates: BTreeMap<String, Versioned<Certificate>>,
    /// The latest config sent.
    latest: Option<Latest>,
    /// What runs, with its own certificates: one no longer offered stays with it.
    running: Option<Run>,
    /// What was last given to run, until it is said to run or to have failed.
    given: Option<Run>,
    /// The latest config, if it was refused: cleared when another is sent, which is all
    /// that can run after it.
    refused: Option<Refusal>,
    /// What was refused, so that it is not given to run again unchanged. Versions are
    /// never named twice, so it matches nothing once another config is sent.
    refused_run: Option<Held>,
}

/// The latest config sent.
#[derive(Debug)]
struct Latest {
    version: String,
    /// None if it could not be read.
    config: Option<Config>,
    /// The certificates it needs, at the versions it needs.
    requires: BTreeMap<String, String>,
}

/// A config, without its certificates, and the certificates it names.
#[derive(Debug, Clone)]
struct Run {
    version: String,
    config: Config,
    certificates: BTreeMap<String, Versioned<Certificate>>,
}

impl Run {
    fn held(&self) -> Held {
        Held {
            config: Some(self.version.clone()),
            certificates: self
                .certificates
                .iter()
                .map(|(name, certificate)| (name.clone(), certificate.version.clone()))
                .collect(),
        }
    }

    /// Whether `other` is the same config with the same certificates, whatever their
    /// versions.
    fn same(&self, other: &Run) -> bool {
        self.config == other.config
            && self.certificates.len() == other.certificates.len()
            && self.certificates.iter().zip(&other.certificates).all(
                |((name, mine), (other_name, theirs))| {
                    name == other_name && mine.value == theirs.value
                },
            )
    }
}

/// What a message sent leaves the data plane to do.
#[derive(Debug)]
pub enum Taken {
    /// Nothing: no config can run yet, or none is new.
    Nothing,
    /// This config, its certificates in it, to run. The caller says how that went with
    /// [`Staging::ran`] or [`Staging::failed`], before the next message is taken.
    Run(Box<Config>),
    /// This, to be said: the latest config equals the one running and is kept, or it
    /// could not be read and is refused.
    Say(Report),
    /// The addresses of the gateway's own pods, to be refused from now on: each time they
    /// change, whole.
    Gateway(Vec<std::net::IpAddr>),
}

impl Staging {
    /// A data plane that holds nothing.
    pub fn new() -> Self {
        Self::default()
    }

    /// What it holds, to open a stream with: the version of the latest config sent, and
    /// of each certificate offered; and the config versions this build reads.
    pub fn hello(&self) -> wire::FromDataPlane {
        let held = Held {
            config: self.latest.as_ref().map(|latest| latest.version.clone()),
            certificates: self
                .certificates
                .iter()
                .map(|(name, certificate)| (name.clone(), certificate.version.clone()))
                .collect(),
        };
        wire::FromDataPlane {
            said: Some(wire::Said::Hello(wire::Hello {
                held: Some((&held).into()),
                config_version: CONFIG_VERSION,
                oldest_config_version: OLDEST_CONFIG_VERSION,
            })),
        }
    }

    /// What the data plane runs, and the latest config, if it refused it.
    pub fn report(&self) -> Report {
        Report {
            running: self.running.as_ref().map(Run::held).unwrap_or_default(),
            refused: self.refused.clone(),
        }
    }

    /// Takes a message sent.
    pub fn take(&mut self, message: wire::ToDataPlane) -> Result<Taken, Malformed> {
        match message.resource {
            // A resource of a kind this data plane does not know.
            None => Ok(Taken::Nothing),
            Some(wire::Resource::Gateway(gateway)) => gateway
                .addresses
                .into_iter()
                .map(|address| address.parse().map_err(|_| Malformed::Address(address)))
                .collect::<Result<_, _>>()
                .map(Taken::Gateway),
            Some(wire::Resource::Certificate(certificate)) => {
                if certificate.name.is_empty() {
                    return Err(Malformed::Unnamed);
                }
                if certificate.version.is_empty() {
                    return Err(Malformed::Unversioned);
                }
                self.certificates.insert(
                    certificate.name,
                    Versioned {
                        version: certificate.version,
                        value: Certificate {
                            chain: certificate.chain,
                            key: certificate.key,
                        },
                    },
                );
                Ok(self.next())
            }
            Some(wire::Resource::Config(config)) => {
                if config.version.is_empty() {
                    return Err(Malformed::Unversioned);
                }
                self.refused = None;
                let read = serde_json::from_slice::<Config>(&config.json);
                let refused = read.as_ref().err().map(|error| Refusal {
                    config: config.version.clone(),
                    why: format!("the config cannot be read: {error}"),
                });
                self.latest = Some(Latest {
                    version: config.version,
                    config: read.ok(),
                    requires: config.requires,
                });
                if let Some(refusal) = refused {
                    self.refused = Some(refusal);
                    return Ok(Taken::Say(self.report()));
                }
                Ok(self.next())
            }
            Some(wire::Resource::Removed(removed)) => {
                for name in &removed.names {
                    self.certificates.remove(name);
                }
                Ok(Taken::Nothing)
            }
        }
    }

    /// What the latest config leaves to do now.
    fn next(&mut self) -> Taken {
        let Some(Latest {
            version,
            config: Some(config),
            requires,
        }) = &self.latest
        else {
            return Taken::Nothing;
        };
        let mut certificates = BTreeMap::new();
        for (name, version) in requires {
            match self.certificates.get(name) {
                Some(certificate) if &certificate.version == version => {
                    certificates.insert(name.clone(), certificate.clone());
                }
                // Held until every certificate it names is here, at the version it names.
                _ => return Taken::Nothing,
            }
        }
        let run = Run {
            version: version.clone(),
            config: config.clone(),
            certificates,
        };
        if let Some(running) = &mut self.running
            && running.same(&run)
        {
            if running.held() == run.held() {
                return Taken::Nothing;
            }
            // The same, under new versions: kept, and acked as they are.
            *running = run;
            return Taken::Say(self.report());
        }
        if self.refused_run.as_ref() == Some(&run.held()) {
            return Taken::Nothing;
        }
        let mut whole = run.config.clone();
        whole.certificates = run
            .certificates
            .iter()
            .map(|(name, certificate)| (name.clone(), certificate.value.clone()))
            .collect();
        self.given = Some(run);
        Taken::Run(Box::new(whole))
    }

    /// The config last given to run now runs: what to say, if one was given.
    pub fn ran(&mut self) -> Option<Report> {
        let run = self.given.take()?;
        self.running = Some(run);
        Some(self.report())
    }

    /// The config last given to run was refused, for `why`: what to say, if one was given.
    /// What runs goes on running.
    pub fn failed(&mut self, why: String) -> Option<Report> {
        let run = self.given.take()?;
        self.refused = Some(Refusal {
            config: run.version.clone(),
            why,
        });
        self.refused_run = Some(run.held());
        Some(self.report())
    }
}
