//! What every pod of one cluster is started with.

use crate::{Time, ms};

/// How a pod watches the others, and the cluster's shape. Every pod of a cluster is to have
/// the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Config {
    /// The time between a pod's heartbeats to every other.
    pub heartbeat: Time,
    /// How long a pod goes without hearing a majority of the voting set before it stops
    /// serving: at least two heartbeats less than `silence`, so that a pod cut off stops before
    /// the others take its shards.
    pub stop_serving: Time,
    /// How long a pod goes unheard before the leader takes it out, or a voter takes its leader
    /// for gone.
    pub silence: Time,
    /// The most a pod waits, beyond `silence`, before it starts an election: each waits a part
    /// of it of its own, so that two rarely start together.
    pub election_jitter: Time,
    /// The time between an owner's copies of what changed to its replicas.
    pub cycle: Time,
    /// The number of shards keys are split into; fixed for the cluster's life.
    pub shards: u16,
    /// Replicas each shard keeps beside its owner, as far as there are members for them.
    pub replicas: usize,
    /// How long the control plane waits for pods it named to form, while one holding the
    /// latest table any of them had runs, before it names them again without requiring that
    /// table: one cut off from the rest would hold the service down until the cut healed.
    pub form_wait: Time,
    /// The most a pod's keys may take, owned and copies, as `keys.max_bytes` caps a service
    /// pod's: past it, at its next sweep, it evicts keys, those it may forget soonest first,
    /// down to nine tenths, the replicas of its own told; none, no cap.
    pub max_key_bytes: Option<u64>,
    /// The most one part of a copy or a handoff carries, by the store's count of its keys'
    /// bytes, at least one key a part: a shard's whole copy goes in as many parts as it takes,
    /// so that what a message carries, and what its receiver holds of one, stays within this
    /// however many keys a shard holds.
    pub part_bytes: u64,
    /// The most bytes of copies of many parts, by the same count, an owner has unanswered to
    /// one replica, across its shards: each part goes once its answers leave room for a part
    /// this size, one always when none is unanswered, so that what either end holds of copies
    /// in progress stays within this however many keys or shards there are.
    pub window_bytes: u64,
}

/// Why a config cannot be used.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConfigError {
    /// A heartbeat of nothing, or none at all.
    Heartbeat,
    /// `stop_serving` not more than a heartbeat, or not two heartbeats less than `silence`.
    StopServing,
    /// No shards.
    Shards,
    /// A cycle of nothing.
    Cycle,
    /// Parts of nothing.
    PartBytes,
}

impl Config {
    /// The defaults: heartbeats every 100 ms, a pod stopping after 400 ms without a majority
    /// and taken out after 600 ms, elections spread over 200 ms, copies every 10 ms, 256 shards
    /// with two replicas each, forming waiting 5 s on a pod holding the latest table, no cap
    /// on keys, and copies in parts of 1 MiB, 2 MiB of them unanswered at most.
    #[must_use]
    pub const fn defaults() -> Self {
        Self {
            heartbeat: ms(100),
            stop_serving: ms(400),
            silence: ms(600),
            election_jitter: ms(200),
            cycle: ms(10),
            shards: 256,
            replicas: 2,
            form_wait: ms(5_000),
            max_key_bytes: None,
            part_bytes: 1 << 20,
            window_bytes: 2 << 20,
        }
    }

    /// Whether it can be used.
    ///
    /// # Errors
    ///
    /// The first [`ConfigError`] it has.
    pub fn check(&self) -> Result<(), ConfigError> {
        if self.heartbeat == 0 {
            return Err(ConfigError::Heartbeat);
        }
        let spare = self.heartbeat.saturating_mul(2);
        if self.stop_serving <= self.heartbeat
            || self.stop_serving.saturating_add(spare) > self.silence
        {
            return Err(ConfigError::StopServing);
        }
        if self.shards == 0 {
            return Err(ConfigError::Shards);
        }
        if self.cycle == 0 {
            return Err(ConfigError::Cycle);
        }
        if self.part_bytes == 0 {
            return Err(ConfigError::PartBytes);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_defaults_are_usable_and_a_buffer_short_of_two_heartbeats_is_not() {
        assert_eq!(Config::defaults().check(), Ok(()));
        let mut config = Config::defaults();
        config.stop_serving = ms(500);
        assert_eq!(config.check(), Err(ConfigError::StopServing));
        config.stop_serving = ms(100);
        assert_eq!(config.check(), Err(ConfigError::StopServing));
        config.stop_serving = ms(400);
        config.shards = 0;
        assert_eq!(config.check(), Err(ConfigError::Shards));
        config.shards = 256;
        config.part_bytes = 0;
        assert_eq!(config.check(), Err(ConfigError::PartBytes));
    }
}
