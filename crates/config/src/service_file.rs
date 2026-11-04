//! What a pod of the rate-limiting service reads from its config file
//! ([06 §4](../../../docs/06-rate-limiting.md)): the mode it serves, the pod itself, the pods
//! it agrees with, the store's settings, what its keys may hold and the files its mutual TLS
//! is read from. Each
//! setting left out has its value, which the service gives it; the service checks what the
//! types cannot, such as a pod's timings against each other.

use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;

/// A rate-limiting service pod's config file, as `edgerush-ratelimit-service --config`
/// reads it once, at the start; its certificate files are read again every second.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(
    feature = "schema",
    schemars(title = "EdgeRush rate-limiting service config file")
)]
#[serde(deny_unknown_fields)]
pub struct ServiceFile {
    /// The rate-limiting mode the pod serves: a pod serves one. Asynchronous when left out.
    #[serde(default)]
    pub mode: Option<ServiceMode>,
    /// The pod itself.
    pub pod: ServicePod,
    /// The pods among which a majority agrees which of them serve.
    pub membership: ServiceMembership,
    /// The store's settings, which every pod of a cluster is to share; left out, the pod has
    /// none. Nothing runs the store from them yet.
    #[serde(default)]
    pub store: Option<ServiceStore>,
    /// What the pod's keys may hold; left out, each has its value.
    #[serde(default)]
    pub keys: ServiceKeys,
    /// The files of the pod's mutual TLS, with data planes and with the other pods alike;
    /// left out, the pod is served plain, to anyone.
    #[serde(default)]
    pub tls: Option<ServicePodTls>,
}

/// The rate-limiting mode a service pod serves.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case")]
pub enum ServiceMode {
    /// Data planes decide from their copies of the keys and share what they admitted every
    /// cycle.
    Asynchronous,
    /// The key's owner decides every request.
    Synchronous,
}

/// A service pod: which it is, where it is reached, and its cycle.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ServicePod {
    /// Which of the `membership`'s pods this one is.
    pub id: u32,
    /// Where it serves data planes and the other pods, `host:port`.
    pub listen: SocketAddr,
    /// Where the other pods reach it, `host:port`, as it tells them in its hellos and as it
    /// asks to join the store. Needed with a `store` section, and only with one.
    #[serde(default)]
    pub address: Option<String>,
    /// Where `/metrics` shows whether the pod serves, how often and how long it has gone
    /// without a majority and, in the asynchronous mode, what its keys hold; nowhere when
    /// left out.
    #[serde(default)]
    pub metrics: Option<SocketAddr>,
    /// Milliseconds of a cycle: in the asynchronous mode the data planes', and in the
    /// synchronous mode the pod's own, between its copies of what changed. 10 when left
    /// out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub cycle_ms: Option<u64>,
}

/// The pods among which a majority agrees which of them serve, and how long each waits on
/// the others.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ServiceMembership {
    /// Every pod, this one too, by its id: where it is reached, `host:port`.
    pub pods: BTreeMap<u32, String>,
    /// Milliseconds a pod goes unheard before the others take it out of the view. 600 when
    /// left out.
    #[serde(default)]
    pub silence_ms: Option<u64>,
    /// Milliseconds a pod goes without hearing a majority before it stops serving: more
    /// than a heartbeat, and two heartbeats less than `silence_ms` at least, so that a pod
    /// cut off from the others stops before they take it out. Longer tolerates longer
    /// hiccups between the pods, and leaves a pod gone out of reach serving for longer. 400
    /// when left out.
    #[serde(default)]
    pub stop_serving_ms: Option<u64>,
}

/// The most milliseconds any of the store's timings may be: an hour, far past any use, and
/// short of where a time on the store's clock, in nanoseconds, would overflow.
pub const STORE_MOST_MS: u64 = 3_600_000;

/// The store's settings: the pods a new one asks first, and how every pod of the cluster
/// watches the others and shapes it. The service checks the timings against each other.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ServiceStore {
    /// Pods' addresses, `host:port`, which a pod starting asks who answers there until it is
    /// a member. None when left out.
    #[serde(default)]
    pub seeds: Vec<String>,
    /// The number of shards keys are split into, fixed for the cluster's life: a pod of
    /// another number does not join. 256 when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub shards: Option<u16>,
    /// Replicas each shard keeps beside its owner, as far as there are pods for them. 2 when
    /// left out.
    #[serde(default)]
    pub replicas: Option<usize>,
    /// Milliseconds between a pod's heartbeats to every other. 100 when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = STORE_MOST_MS)))]
    pub heartbeat_ms: Option<u64>,
    /// Milliseconds a pod goes without hearing a majority of the voting pods before it stops
    /// serving: more than a heartbeat, and two heartbeats less than `silence_ms` at least, so
    /// that a pod cut off stops before the others take its shards. 400 when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(max = STORE_MOST_MS)))]
    pub stop_serving_ms: Option<u64>,
    /// Milliseconds a pod goes unheard before the leader takes it out, or a voting pod takes
    /// its leader for gone. 600 when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(max = STORE_MOST_MS)))]
    pub silence_ms: Option<u64>,
    /// The most milliseconds a pod waits, beyond `silence_ms`, before it stands for leader:
    /// each waits a part of it of its own, so that two rarely stand together. 200 when left
    /// out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(max = STORE_MOST_MS)))]
    pub election_jitter_ms: Option<u64>,
    /// Milliseconds between an owner's copies of what changed to its replicas. 10 when left
    /// out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1, max = STORE_MOST_MS)))]
    pub cycle_ms: Option<u64>,
    /// Milliseconds a peer's wall clock may be off from a pod's before the pod warns of it.
    /// 250 when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(max = STORE_MOST_MS)))]
    pub skew_warning_ms: Option<u64>,
}

/// What a service pod's keys may hold.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ServiceKeys {
    /// Bytes the keys may hold, past which the least recently changed are evicted. 256 MiB
    /// when left out.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub max_bytes: Option<u64>,
}

/// The files of a service pod's mutual TLS, each PEM and read again every second, each
/// relative to the config file's directory unless it is absolute.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct ServicePodTls {
    /// The certificate the pod presents to data planes and to the other pods, then the
    /// intermediates.
    pub certificate: PathBuf,
    /// Its private key.
    pub key: PathBuf,
    /// The authorities trusted to vouch for every caller.
    pub authorities: PathBuf,
    /// The SPIFFE ID of the data plane the pod serves. A caller is told apart by the ID its
    /// certificate carries: the pod's own is the pods', this one the data plane's, and any
    /// other is refused.
    pub data_plane_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(yaml: &str) -> Result<ServiceFile, serde_saphyr::Error> {
        serde_saphyr::from_str(yaml)
    }

    const LEAST: &str =
        "pod: { id: 0, listen: 0.0.0.0:7000 }\nmembership: { pods: { 0: rl-0:7000 } }\n";

    #[test]
    fn a_file_with_every_section_is_read_as_it_says() {
        let yaml = "\
mode: synchronous
pod:
  id: 1
  listen: 0.0.0.0:7000
  address: rl-1:7000
  metrics: 0.0.0.0:9090
  cycle_ms: 5
membership:
  pods:
    0: rl-0:7000
    1: rl-1:7000
  silence_ms: 2000
  stop_serving_ms: 1000
store:
  seeds: [rl-0:7000, rl-2:7000]
  shards: 64
  replicas: 1
  heartbeat_ms: 50
  stop_serving_ms: 200
  silence_ms: 300
  election_jitter_ms: 100
  cycle_ms: 20
  skew_warning_ms: 500
keys:
  max_bytes: 1048576
tls:
  certificate: pod.pem
  key: pod.key
  authorities: ca.pem
  data_plane_id: spiffe://cluster.local/ns/edge/sa/edge
";
        assert_eq!(
            read(yaml).unwrap(),
            ServiceFile {
                mode: Some(ServiceMode::Synchronous),
                pod: ServicePod {
                    id: 1,
                    listen: "0.0.0.0:7000".parse().unwrap(),
                    address: Some("rl-1:7000".to_owned()),
                    metrics: Some("0.0.0.0:9090".parse().unwrap()),
                    cycle_ms: Some(5),
                },
                membership: ServiceMembership {
                    pods: [(0, "rl-0:7000".to_owned()), (1, "rl-1:7000".to_owned())].into(),
                    silence_ms: Some(2_000),
                    stop_serving_ms: Some(1_000),
                },
                store: Some(ServiceStore {
                    seeds: vec!["rl-0:7000".to_owned(), "rl-2:7000".to_owned()],
                    shards: Some(64),
                    replicas: Some(1),
                    heartbeat_ms: Some(50),
                    stop_serving_ms: Some(200),
                    silence_ms: Some(300),
                    election_jitter_ms: Some(100),
                    cycle_ms: Some(20),
                    skew_warning_ms: Some(500),
                }),
                keys: ServiceKeys {
                    max_bytes: Some(1 << 20),
                },
                tls: Some(ServicePodTls {
                    certificate: "pod.pem".into(),
                    key: "pod.key".into(),
                    authorities: "ca.pem".into(),
                    data_plane_id: "spiffe://cluster.local/ns/edge/sa/edge".to_owned(),
                }),
            }
        );
    }

    #[test]
    fn what_is_left_out_is_left_to_the_service() {
        let file = read(LEAST).unwrap();
        assert_eq!(file.mode, None);
        assert_eq!((file.pod.metrics, file.pod.cycle_ms), (None, None));
        assert_eq!((file.pod.address, file.store), (None, None));
        assert_eq!(
            (file.membership.silence_ms, file.membership.stop_serving_ms),
            (None, None)
        );
        assert_eq!(file.keys, ServiceKeys::default());
        assert_eq!(file.tls, None);
        let file = read(&format!("{LEAST}store: {{}}\n")).unwrap();
        assert_eq!(file.store, Some(ServiceStore::default()));
    }

    #[test]
    fn a_setting_it_does_not_know_or_one_missing_is_refused() {
        for yaml in [
            // Unknown, at the top and within a section.
            format!("{LEAST}shards: 256\n"),
            "pod: { id: 0, listen: 0.0.0.0:7000, port: 7000 }\nmembership: { pods: { 0: rl-0:7000 } }\n".to_owned(),
            // A mode the service does not serve.
            format!("{LEAST}mode: local\n"),
            // The pod, or the pods it agrees with, not said.
            "membership: { pods: { 0: rl-0:7000 } }\n".to_owned(),
            "pod: { id: 0, listen: 0.0.0.0:7000 }\n".to_owned(),
            "pod: { listen: 0.0.0.0:7000 }\nmembership: { pods: { 0: rl-0:7000 } }\n".to_owned(),
            // Not an address.
            "pod: { id: 0, listen: nowhere }\nmembership: { pods: { 0: rl-0:7000 } }\n".to_owned(),
            // The store's: one it does not know, the control plane's own, a count of shards
            // past what a cluster can have, seeds not a list.
            format!("{LEAST}store: {{ moves: 4 }}\n"),
            format!("{LEAST}store: {{ form_wait_ms: 5000 }}\n"),
            format!("{LEAST}store: {{ shards: 65536 }}\n"),
            format!("{LEAST}store: {{ seeds: rl-0:7000 }}\n"),
            // The TLS files go together.
            format!("{LEAST}tls: {{ certificate: pod.pem, key: pod.key, authorities: ca.pem }}\n"),
        ] {
            assert!(read(&yaml).is_err(), "{yaml}");
        }
    }
}
