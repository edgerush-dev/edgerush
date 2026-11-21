//! The leader's snapshot ([04 §2](../../../docs/04-control-plane.md)): what a translation
//! cannot read back from the objects, per data plane served, which the other replicas
//! translate from rather than keep their own. Written as JSON, with the config version it
//! was taken at; this knows nothing of the listener it crosses.
//!
//! A config it carries is written as the stream writes one, and its certificates, keys
//! included, and its limits beside it, as the stream sends them: in fields of the
//! snapshot's own, never through the config's, which no file a config is read from may
//! carry a key through.
//!
//! The one config version ([`CONFIG_VERSION`]) is the snapshot's as well as the config's,
//! and it crosses versions as a config does (02 §4): a change to the shape of either raises
//! it, and adds its step here, applied to what is written for a replica below it and to
//! what is read from one, so that a replica of the next release takes in the snapshot of
//! one still running the last, as an upgrade has it. A snapshot is written at the older of
//! the leader's version and its reader's; one of a version outside the reader's window
//! ([`OLDEST_CONFIG_VERSION`] to [`CONFIG_VERSION`]) is refused whole, and its reader
//! starts clean, loudly. No version has added or removed anything yet, so every one is the
//! shape this build writes.

use edgerush_config::{Certificate, Config, Limits};
use edgerush_control_stream::{CONFIG_VERSION, ConfigVersion, OLDEST_CONFIG_VERSION};
use edgerush_planes::data_plane::{DataPlane, DataPlaneSpec};
use edgerush_translate::AcceptedPolicy;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What the leader holds of the data planes it serves that their objects cannot give back.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Snapshot {
    /// How new it is: raised by the leader at each change, and carried on by a new leader
    /// from the snapshot it took, so that the replica taking the lease takes the one with
    /// the highest of those the ready replicas hold.
    pub count: u64,
    /// Each data plane's, by its DataPlane as [`Planes`](crate::Planes) names it.
    pub planes: BTreeMap<String, Record>,
}

/// What the leader holds of one data plane that its objects cannot give back.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Record {
    /// The version of each RateLimitPolicy that applied in the config last handed on:
    /// their specs say the names each holds, and each applies where a later spec is refused
    /// (04 §4).
    pub policies: Vec<AcceptedPolicy>,
    /// Its DataPlane of the generation the operator last accepted, served while a later
    /// one waits on it ([05 §3](../../../docs/05-operator.md)); none before one is.
    pub accepted: Option<DataPlane>,
    /// Its DataPlane of the generation served, where a later one is held back whole (04
    /// §4); none where none is.
    pub served: Option<DataPlane>,
    /// The config last handed on, with its certificates and limits, where the one now
    /// translated is refused whole or its Gateway is held (04 §2); none where the config
    /// handed on is the one translated.
    pub config: Option<Config>,
}

/// A snapshot that cannot be taken in.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// It is of a config version this build does not read.
    #[error(
        "the snapshot is of config version {version}, and this build reads {OLDEST_CONFIG_VERSION} to {CONFIG_VERSION}"
    )]
    Version {
        /// The version it is of.
        version: u32,
    },
    /// It is not a snapshot as its version writes one.
    #[error("the snapshot cannot be read: {0}")]
    Unreadable(#[source] serde_json::Error),
}

impl Snapshot {
    /// The snapshot as it crosses to a replica that reads `version`: the older of its and
    /// this build's.
    ///
    /// # Errors
    ///
    /// Where it cannot be written as JSON.
    pub fn encode(&self, version: ConfigVersion) -> Result<Vec<u8>, serde_json::Error> {
        // No version has added or removed anything yet, so every one written is the shape
        // this build writes.
        let written = Written {
            config_version: version.get(),
            count: self.count,
            planes: self
                .planes
                .iter()
                .map(|(name, record)| (name.clone(), WrittenRecord::of(record)))
                .collect(),
        };
        serde_json::to_vec(&written)
    }

    /// The snapshot `bytes` write, if they are one of a config version this build reads.
    ///
    /// # Errors
    ///
    /// Where they are of a version outside the window, or are no snapshot of theirs.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotError> {
        // The version first: what follows it is read as that version writes it.
        let Of { config_version } =
            serde_json::from_slice(bytes).map_err(SnapshotError::Unreadable)?;
        if ConfigVersion::served(config_version).is_none() {
            return Err(SnapshotError::Version {
                version: config_version,
            });
        }
        let written: Written = serde_json::from_slice(bytes).map_err(SnapshotError::Unreadable)?;
        Ok(Self {
            count: written.count,
            planes: written
                .planes
                .into_iter()
                .map(|(name, record)| (name, record.taken()))
                .collect(),
        })
    }
}

/// The version a snapshot is of, read before the rest.
#[derive(Deserialize)]
struct Of {
    config_version: u32,
}

/// A snapshot as it is written.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Written {
    config_version: u32,
    count: u64,
    planes: BTreeMap<String, WrittenRecord>,
}

/// A data plane's record as it is written.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WrittenRecord {
    policies: Vec<AcceptedPolicy>,
    accepted: Option<WrittenDataPlane>,
    served: Option<WrittenDataPlane>,
    config: Option<WrittenConfig>,
}

/// A DataPlane as a snapshot holds it: its metadata and spec, which its settings are made
/// of, and not its status, which says what the operator and the control plane make of it
/// now rather than what the snapshot was taken of.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WrittenDataPlane {
    metadata: ObjectMeta,
    spec: DataPlaneSpec,
}

/// A config as a snapshot holds it: the config as the stream writes it, and beside it what
/// the stream sends apart from it.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WrittenConfig {
    config: Config,
    certificates: BTreeMap<String, WrittenCertificate>,
    limits: Option<Limits>,
}

/// A certificate and its key, in PEM, as the stream sends them.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WrittenCertificate {
    chain: String,
    key: String,
}

impl WrittenConfig {
    fn of(config: &Config) -> Self {
        Self {
            config: config.clone(),
            certificates: config
                .certificates
                .iter()
                .map(|(name, certificate)| {
                    let written = WrittenCertificate {
                        chain: certificate.chain.clone(),
                        key: certificate.key.clone(),
                    };
                    (name.clone(), written)
                })
                .collect(),
            limits: config.limits.clone(),
        }
    }

    fn taken(self) -> Config {
        let mut config = self.config;
        config.certificates = self
            .certificates
            .into_iter()
            .map(|(name, written)| {
                let certificate = Certificate {
                    chain: written.chain,
                    key: written.key,
                };
                (name, certificate)
            })
            .collect();
        config.limits = self.limits;
        config
    }
}

impl WrittenRecord {
    fn of(record: &Record) -> Self {
        let written = |data_plane: &DataPlane| WrittenDataPlane {
            metadata: data_plane.metadata.clone(),
            spec: data_plane.spec.clone(),
        };
        Self {
            policies: record.policies.clone(),
            accepted: record.accepted.as_ref().map(written),
            served: record.served.as_ref().map(written),
            config: record.config.as_ref().map(WrittenConfig::of),
        }
    }

    fn taken(self) -> Record {
        let taken = |written: WrittenDataPlane| DataPlane {
            metadata: written.metadata,
            spec: written.spec,
            status: None,
        };
        Record {
            policies: self.policies,
            accepted: self.accepted.map(taken),
            served: self.served.map(taken),
            config: self.config.map(WrittenConfig::taken),
        }
    }
}

#[cfg(test)]
mod tests;
