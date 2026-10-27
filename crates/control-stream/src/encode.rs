//! The versioned encoder ([02 §4](../../../docs/02-architecture.md)): the one config model,
//! written for the config version a data plane reads.
//!
//! A version that adds something to the encoding has it written only for data planes at
//! that version or after; one that removes something has it still written for those
//! before. Each such version adds its step here, applied to what the model writes for a
//! data plane below it. None has yet: version 1 is the model as it is.
//!
//! Each end has a window: the versions from [`OLDEST_CONFIG_VERSION`] to its own
//! [`CONFIG_VERSION`]. A config is written at the older of the two ends' versions, and a
//! stream is refused if that falls outside either end's window: a data plane older than
//! the control plane still writes for, or a control plane older than the data plane still
//! reads.
//!
//! Each end has a window: the versions from [`OLDEST_CONFIG_VERSION`] to its own
//! [`CONFIG_VERSION`]. A config is written at the older of the two ends' versions, and a
//! stream is refused if that falls outside either end's window: a data plane older than
//! the control plane still writes for, or a control plane older than the data plane still
//! reads.

use edgerush_config::Config;

/// The config version this build writes and reads: raised by one whenever the encoding
/// gains or loses something, apart from releases' numbers.
pub const CONFIG_VERSION: u32 = 1;

/// The oldest config version this build still writes, as a control plane, and reads, as a
/// data plane: raised when what an older version needs is no longer written.
pub const OLDEST_CONFIG_VERSION: u32 = 1;

/// A config version this build's control plane writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConfigVersion(u32);

impl ConfigVersion {
    /// `version`, if it is written.
    pub fn served(version: u32) -> Option<Self> {
        (OLDEST_CONFIG_VERSION..=CONFIG_VERSION)
            .contains(&version)
            .then_some(Self(version))
    }

    /// Each version written, oldest first.
    pub(crate) fn all() -> impl Iterator<Item = Self> {
        (OLDEST_CONFIG_VERSION..=CONFIG_VERSION).map(Self)
    }

    /// Its place among those written, oldest first: within them, as only a version written
    /// is made one.
    pub(crate) fn index(self) -> usize {
        (self.0 - OLDEST_CONFIG_VERSION) as usize
    }

    /// The version's number.
    pub fn get(self) -> u32 {
        self.0
    }
}

/// `config`, without its certificates, as a data plane at `version` reads it.
pub(crate) fn encode(
    config: &Config,
    _version: ConfigVersion,
) -> Result<Vec<u8>, serde_json::Error> {
    // No version has added or removed anything yet, so every one served reads the model as
    // it is written.
    serde_json::to_vec(config)
}
