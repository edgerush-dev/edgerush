//! What a pod runs the store with, from its file's `pod.address` and `store` section
//! (`edgerush_config::ServiceStore`), and the member id it takes for each life.
//!
//! Every setting left out has the store's default (`Config::defaults`), and the timings are
//! checked against each other as the store checks them. The keys' cap is the file's
//! `keys.max_bytes`, as today's modes have it; how big a copy's part is, and how many bytes of
//! parts go unanswered, are the service's to fix, not the file's.

use super::pod::CLIENTS;
use edgerush_cluster::{Config, ConfigError, NodeId, Time, ms};
use edgerush_config::{STORE_MOST_MS, ServiceFile};

/// What a pod runs the store with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Settings {
    /// Where the other pods reach it, as it tells them.
    pub address: String,
    /// Addresses it asks as it starts, until it is a member.
    pub seeds: Vec<String>,
    /// What every pod of the cluster is to share.
    pub config: Config,
    /// Whether it decides a key it would forward from its copy, its owner unheard.
    pub forwarder_decides: bool,
}

/// Why a file's store settings cannot be used, each naming the setting.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SettingsError {
    /// A `store` section, but nowhere for the other pods to reach this one.
    #[error("`pod.address` is needed with a `store` section")]
    NoAddress,
    /// An address, but no store to give it in.
    #[error("`pod.address` is for the store, and there is no `store` section")]
    NoStore,
    /// A timing past [`STORE_MOST_MS`].
    #[error("`store.{0}`: at most an hour, {STORE_MOST_MS} ms")]
    TooLong(&'static str),
    /// A heartbeat of nothing.
    #[error("`store.heartbeat_ms`: a number of milliseconds, at least 1")]
    Heartbeat,
    /// A pod would stop between two heartbeats that both came, or one cut off would serve on
    /// after the others took its shards.
    #[error(
        "`store.stop_serving_ms` {stop_serving} must be more than a heartbeat ({heartbeat} ms) \
         and at least {spare} ms less than `store.silence_ms` {silence}"
    )]
    StopServing {
        /// `store.stop_serving_ms`.
        stop_serving: u64,
        /// `store.heartbeat_ms`.
        heartbeat: u64,
        /// Two heartbeats.
        spare: u64,
        /// `store.silence_ms`.
        silence: u64,
    },
    /// No shards.
    #[error("`store.shards`: a number of shards, at least 1")]
    Shards,
    /// Copies sent without end.
    #[error("`store.cycle_ms`: a number of milliseconds, at least 1")]
    Cycle,
    /// One of the service's own settings, which no file sets.
    #[error("the store's settings the service fixes cannot be used: {0:?}")]
    Fixed(ConfigError),
}

/// The store's settings in `file`, its keys capped at `max_key_bytes`: none when it has no
/// `store` section.
///
/// # Errors
///
/// The first [`SettingsError`] the file has.
pub fn settings(file: &ServiceFile, max_key_bytes: u64) -> Result<Option<Settings>, SettingsError> {
    let (address, store) = match (&file.pod.address, &file.store) {
        (None, None) => return Ok(None),
        (None, Some(_)) => return Err(SettingsError::NoAddress),
        (Some(_), None) => return Err(SettingsError::NoStore),
        (Some(address), Some(store)) => (address, store),
    };
    let defaults = Config::defaults();
    let millis = |name, set: Option<u64>, default: Time| match set {
        None => Ok(default),
        Some(set) if set <= STORE_MOST_MS => Ok(ms(set)),
        Some(_) => Err(SettingsError::TooLong(name)),
    };
    let config = Config {
        heartbeat: millis("heartbeat_ms", store.heartbeat_ms, defaults.heartbeat)?,
        stop_serving: millis(
            "stop_serving_ms",
            store.stop_serving_ms,
            defaults.stop_serving,
        )?,
        silence: millis("silence_ms", store.silence_ms, defaults.silence)?,
        election_jitter: millis(
            "election_jitter_ms",
            store.election_jitter_ms,
            defaults.election_jitter,
        )?,
        cycle: millis("cycle_ms", store.cycle_ms, defaults.cycle)?,
        skew_warning: millis(
            "skew_warning_ms",
            store.skew_warning_ms,
            defaults.skew_warning,
        )?,
        shards: store.shards.unwrap_or(defaults.shards),
        replicas: store.replicas.unwrap_or(defaults.replicas),
        max_key_bytes: Some(max_key_bytes),
        ..defaults
    };
    config.check().map_err(|error| match error {
        ConfigError::Heartbeat => SettingsError::Heartbeat,
        ConfigError::StopServing => {
            let heartbeat = config.heartbeat / ms(1);
            SettingsError::StopServing {
                stop_serving: config.stop_serving / ms(1),
                heartbeat,
                spare: heartbeat.saturating_mul(2),
                silence: config.silence / ms(1),
            }
        }
        ConfigError::Shards => SettingsError::Shards,
        ConfigError::Cycle => SettingsError::Cycle,
        ConfigError::PartBytes => SettingsError::Fixed(error),
    })?;
    Ok(Some(Settings {
        address: address.clone(),
        seeds: store.seeds.clone(),
        config,
        forwarder_decides: store.forwarder_decides.unwrap_or(true),
    }))
}

/// A member id for this life of the pod: a restarted pod is a new member, which no earlier
/// life's id may name. 63 random bits, below every client's id ([`CLIENTS`]) and the control
/// plane's: two lives share one only by a chance of one in 2^63, and no clock set back can
/// give one an earlier life's.
///
/// # Errors
///
/// BoringSSL's, when its generator fails.
pub fn member_id() -> Result<NodeId, boring::error::ErrorStack> {
    let mut bytes = [0; 8];
    boring::rand::rand_bytes(&mut bytes)?;
    Ok(u64::from_le_bytes(bytes) % CLIENTS)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A file with `pod`'s more and `store`'s section, if any.
    fn file(pod: &str, store: Option<&str>) -> ServiceFile {
        let store = store.map_or(String::new(), |store| format!("store: {{ {store} }}\n"));
        serde_saphyr::from_str(&format!(
            "pod: {{ id: 0, listen: 0.0.0.0:7000{pod} }}\nmembership: {{ pods: {{ 0: rl-0:7000 }} }}\n{store}"
        ))
        .unwrap()
    }

    const ADDRESS: &str = ", address: rl-0.rl:7000";

    #[test]
    fn a_file_without_a_store_has_none() {
        assert_eq!(settings(&file("", None), 1 << 20), Ok(None));
    }

    #[test]
    fn what_is_left_out_has_the_stores_default_and_the_keys_their_cap() {
        let settings = settings(&file(ADDRESS, Some("")), 1 << 20)
            .unwrap()
            .unwrap();
        assert_eq!(
            settings,
            Settings {
                address: "rl-0.rl:7000".to_owned(),
                seeds: Vec::new(),
                config: Config {
                    max_key_bytes: Some(1 << 20),
                    ..Config::defaults()
                },
                forwarder_decides: true,
            }
        );
    }

    #[test]
    fn each_setting_said_is_the_stores() {
        let store = "seeds: [rl-1:7000, rl-2:7000], shards: 64, replicas: 1, heartbeat_ms: 50, \
                     stop_serving_ms: 200, silence_ms: 300, election_jitter_ms: 100, \
                     cycle_ms: 20, skew_warning_ms: 500, forwarder_decides: false";
        let settings = settings(&file(ADDRESS, Some(store)), 1 << 30)
            .unwrap()
            .unwrap();
        assert_eq!(settings.seeds, ["rl-1:7000", "rl-2:7000"]);
        assert!(!settings.forwarder_decides);
        assert_eq!(
            settings.config,
            Config {
                heartbeat: ms(50),
                stop_serving: ms(200),
                silence: ms(300),
                election_jitter: ms(100),
                cycle: ms(20),
                shards: 64,
                replicas: 1,
                max_key_bytes: Some(1 << 30),
                skew_warning: ms(500),
                ..Config::defaults()
            }
        );
    }

    #[test]
    fn an_address_goes_with_a_store_and_a_store_with_an_address() {
        assert_eq!(
            settings(&file("", Some("")), 1),
            Err(SettingsError::NoAddress)
        );
        assert_eq!(
            settings(&file(ADDRESS, None), 1),
            Err(SettingsError::NoStore)
        );
    }

    #[test]
    fn timings_that_cannot_be_used_are_refused_by_name() {
        for (store, refused) in [
            ("heartbeat_ms: 0", SettingsError::Heartbeat),
            ("cycle_ms: 0", SettingsError::Cycle),
            ("shards: 0", SettingsError::Shards),
            // A buffer short of two heartbeats, and a stop no longer than a heartbeat.
            (
                "stop_serving_ms: 401",
                SettingsError::StopServing {
                    stop_serving: 401,
                    heartbeat: 100,
                    spare: 200,
                    silence: 600,
                },
            ),
            (
                "heartbeat_ms: 400",
                SettingsError::StopServing {
                    stop_serving: 400,
                    heartbeat: 400,
                    spare: 800,
                    silence: 600,
                },
            ),
            // Past an hour, which would overflow the store's clock if it were let through.
            ("silence_ms: 3600001", SettingsError::TooLong("silence_ms")),
            (
                "election_jitter_ms: 18446744073709551615",
                SettingsError::TooLong("election_jitter_ms"),
            ),
        ] {
            assert_eq!(
                settings(&file(ADDRESS, Some(store)), 1),
                Err(refused),
                "{store}"
            );
        }
        // At the edges: an hour, and a stop exactly two heartbeats short.
        assert!(settings(&file(ADDRESS, Some("skew_warning_ms: 3600000")), 1).is_ok());
        assert!(settings(&file(ADDRESS, Some("stop_serving_ms: 400")), 1).is_ok());
    }

    #[test]
    fn a_member_id_is_new_each_life_and_never_a_clients() {
        let ids: Vec<NodeId> = (0..64).map(|_| member_id().unwrap()).collect();
        assert!(ids.iter().all(|id| *id < CLIENTS), "{ids:?}");
        let mut apart = ids.clone();
        apart.sort_unstable();
        apart.dedup();
        assert_eq!(apart.len(), ids.len(), "{ids:?}");
        // Not stuck below half the range either: 64 ids all under 2^62 is one in 2^64.
        assert!(ids.iter().any(|id| *id >= CLIENTS / 2), "{ids:?}");
    }
}
