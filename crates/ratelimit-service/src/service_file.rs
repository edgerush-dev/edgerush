//! What a pod runs as, from its config file (`edgerush_config::ServiceFile`): the file's
//! bytes read, what the types cannot check of them checked, and the values of what they
//! leave out filled in.
//!
//! Here rather than in the binary so that a fuzz target can reach the reading and checking
//! as the binary does them: whatever bytes it is given, neither may fail, and a refusal is
//! told without failing.

use crate::cluster::settings::{Settings, SettingsError, settings};
use crate::secure::PodFiles;
use edgerush_config::ServiceFile;
use std::net::{IpAddr, SocketAddr};
use std::path::Path;

/// What `keys.max_bytes` is when left out: 256 MiB.
pub const MAX_KEY_BYTES: u64 = 256 << 20;

/// What a pod runs as: both modes, on its store.
#[derive(Debug, PartialEq, Eq)]
pub struct PodSettings {
    /// Where it serves data planes and the other pods.
    pub listen: SocketAddr,
    /// Where it serves its metrics and readiness, if anywhere.
    pub metrics: Option<SocketAddr>,
    /// Its credentials, or none to be served plain.
    pub tls: Option<PodFiles>,
    /// The store it serves both modes on.
    pub store: Settings,
}

/// Why a pod's file cannot be run, each naming what is wrong.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// The file is not YAML, or not a pod's file, told by the line and column of the mistake
    /// and never by quoting the lines around it. In a box, as the parser's error is large.
    #[error("{0}")]
    Parse(Box<serde_saphyr::Error>),
    /// Keys that may hold nothing.
    #[error("`keys.max_bytes`: a number of bytes, at least 1")]
    KeyBytes,
    /// No store to run the modes on.
    #[error("a pod runs on the store: it needs `store`, and `pod.address` or `POD_IP`")]
    NoStore,
    /// A store the pod could not run on.
    #[error("{0}")]
    Store(#[from] SettingsError),
}

/// What a pod runs as, from the file `yaml` states, its TLS files relative to `directory`
/// unless absolute, and reached at `own`, the address it was given, where the file says no
/// `pod.address`.
///
/// # Errors
///
/// The first [`FileError`] the file has.
pub fn parsed(
    yaml: &[u8],
    directory: &Path,
    own: Option<IpAddr>,
) -> Result<PodSettings, FileError> {
    // Without the lines around a mistake, which the parser quotes by default: what is said
    // of a file refused goes to the log, as the data plane's does.
    let options = serde_saphyr::options! { with_snippet: false };
    let file: ServiceFile = serde_saphyr::from_slice_with_options(yaml, options)
        .map_err(|error| FileError::Parse(Box::new(error)))?;
    checked(file, directory, own)
}

/// What the types cannot check of `file`, and the values of what it leaves out.
fn checked(
    file: ServiceFile,
    directory: &Path,
    own: Option<IpAddr>,
) -> Result<PodSettings, FileError> {
    let max_key_bytes = file.keys.max_bytes.unwrap_or(MAX_KEY_BYTES);
    if max_key_bytes == 0 {
        return Err(FileError::KeyBytes);
    }
    let store = settings(&file, max_key_bytes, own)?.ok_or(FileError::NoStore)?;
    let tls = file.tls.map(|tls| PodFiles {
        certificate: directory.join(tls.certificate),
        key: directory.join(tls.key),
        authorities: directory.join(tls.authorities),
        trust_domain: tls.trust_domain,
    });
    Ok(PodSettings {
        listen: file.pod.listen,
        metrics: file.pod.metrics,
        tls,
        store,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The settings from `yaml`, its TLS files relative to `/etc/edgerush`, a refusal as it
    /// is told.
    fn parsed_from(yaml: &str) -> Result<PodSettings, String> {
        parsed(yaml.as_bytes(), Path::new("/etc/edgerush"), None).map_err(|error| error.to_string())
    }

    /// A pod's file, on the store, with `more` after its sections.
    fn with(more: &str) -> String {
        sections("", &format!("store: {{}}\n{more}"))
    }

    /// A pod's file with its `pod` section's `more`, and `rest` after it.
    fn sections(pod: &str, rest: &str) -> String {
        format!("pod: {{ listen: 0.0.0.0:7000, address: rl-0.rl:7000{pod} }}\n{rest}")
    }

    /// A pod runs on the store, and is refused without it.
    #[test]
    fn a_pod_runs_on_the_store() {
        assert_eq!(
            parsed_from(&with("")).unwrap().store.address,
            "rl-0.rl:7000"
        );
        assert_eq!(
            parsed_from("pod: { listen: 0.0.0.0:7000 }\n").err(),
            Some(
                "a pod runs on the store: it needs `store`, and `pod.address` or `POD_IP`"
                    .to_owned()
            )
        );
    }

    /// A pod in Kubernetes reads the one file every pod does, and is reached at the address
    /// it was given.
    #[test]
    fn a_file_without_the_pods_address_takes_the_one_it_was_given() {
        let yaml = b"pod: { listen: 0.0.0.0:7000 }
store: {}
";
        let own = Some("10.1.2.3".parse().unwrap());
        let settings = parsed(yaml, Path::new("/etc/edgerush"), own).unwrap();
        assert_eq!(settings.store.address, "10.1.2.3:7000");
    }

    #[test]
    fn a_mistake_is_told_without_the_lines_around_it() {
        let said = parsed_from(&with("shards: 256\n")).unwrap_err();
        assert!(said.contains("shards"), "{said}");
        assert!(!said.contains("rl-0.rl:7000"), "{said}");
    }

    /// Every pod serves both modes: the file names none, and the synchronous mode's switch is
    /// read beside the store's other settings, on whatever pod.
    #[test]
    fn the_file_names_no_mode_and_reads_every_setting() {
        assert!(
            !parsed_from(&sections("", "store: { forwarder_decides: false }\n"))
                .unwrap()
                .store
                .forwarder_decides
        );
        assert!(parsed_from(&with("")).unwrap().store.forwarder_decides);
        // What a pod served before it served both, what the asynchronous mode's pods agreed by
        // before they ran on the store, and the data planes' cycle, which each batch says, are
        // no fields the file knows.
        for gone in [
            with("mode: asynchronous\n"),
            with("mode: synchronous\n"),
            sections(", id: 0", "store: {}\n"),
            with("membership: { pods: { 0: rl-0:7000 } }\n"),
            sections(", cycle_ms: 10", "store: {}\n"),
        ] {
            let said = parsed_from(&gone).unwrap_err();
            assert!(said.contains("unknown field"), "{said}");
        }
    }

    #[test]
    fn what_keys_may_hold_and_where_metrics_go_are_said_or_left_out() {
        let plain = parsed_from(&with("")).unwrap();
        assert_eq!(plain.metrics, None);
        assert_eq!(plain.store.config.max_key_bytes, Some(256 << 20));
        let said = parsed_from(&format!(
            "keys: {{ max_bytes: 1048576 }}\n{}",
            sections(", metrics: 127.0.0.1:9090", "store: {}\n")
        ))
        .unwrap();
        assert_eq!(said.metrics, Some("127.0.0.1:9090".parse().unwrap()));
        assert_eq!(said.store.config.max_key_bytes, Some(1 << 20));
        assert_eq!(
            parsed_from(&with("keys: { max_bytes: 0 }\n")),
            Err("`keys.max_bytes`: a number of bytes, at least 1".to_owned())
        );
        assert!(parsed_from(&with("keys: { max_bytes: lots }\n")).is_err());
        assert!(parsed_from(&sections(", metrics: nowhere", "store: {}\n")).is_err());
    }

    #[test]
    fn a_store_the_pod_could_not_run_on_is_refused_as_the_file_is_read() {
        assert_eq!(
            parsed_from(&sections("", "")).err(),
            Some("`pod.address` is for the store, and there is no `store` section".to_owned())
        );
        assert_eq!(
            parsed_from("pod: { listen: 0.0.0.0:7000 }\nstore: {}\n").err(),
            Some(
                "`pod.address`, or an address in `POD_IP`, is needed with a `store` section"
                    .to_owned()
            )
        );
        assert_eq!(
            parsed_from(&with("")).map(|_| ()).err(),
            None,
            "a store of its defaults"
        );
        assert_eq!(
            parsed_from(&sections("", "store: { stop_serving_ms: 500 }\n")).err(),
            Some(
                "`store.stop_serving_ms` 500 must be more than a heartbeat (100 ms) and at \
                 least 200 ms less than `store.silence_ms` 600"
                    .to_owned()
            )
        );
    }

    #[test]
    fn the_certificate_files_go_together_or_the_pod_is_served_plain() {
        assert_eq!(parsed_from(&with("")).unwrap().tls, None);
        let files = [
            "certificate: pod.pem",
            "key: pod.key",
            "authorities: ca.pem",
            "trust_domain: edge.cluster.local",
        ];
        let tls =
            |given: &[&str]| parsed_from(&with(&format!("tls: {{ {} }}\n", given.join(", "))));
        assert_eq!(
            tls(&files).unwrap().tls,
            Some(PodFiles {
                certificate: "/etc/edgerush/pod.pem".into(),
                key: "/etc/edgerush/pod.key".into(),
                authorities: "/etc/edgerush/ca.pem".into(),
                trust_domain: "edge.cluster.local".to_owned(),
            })
        );
        for left_out in 0..files.len() {
            let mut given = files.to_vec();
            given.remove(left_out);
            assert!(tls(&given).is_err(), "{given:?}");
        }
    }

    /// The file the operator writes for a ControlPlane's service, `service` as it declares
    /// it, as the pod at `10.1.2.3` runs it.
    fn the_operators(service: &str) -> PodSettings {
        use edgerush_planes::objects::{Install, Object, for_control_plane};
        let control_plane = serde_saphyr::from_str(&format!(
            "apiVersion: gateway.edgerush.dev/v1alpha1
kind: ControlPlane
metadata: {{ name: payments, namespace: edge, uid: uid-of-payments }}
spec:
  watch: {{ namespaces: {{ from: Same }} }}
  rateLimiting: {{ service: {service} }}
"
        ))
        .unwrap();
        let install = Install {
            image: "edgerush:test".to_owned(),
            cluster_domain: "cluster.local".to_owned(),
        };
        let objects = for_control_plane(&control_plane, &Default::default(), &install).unwrap();
        let file = objects
            .iter()
            .find_map(|object| match object {
                Object::ConfigMap(config_map) => config_map.data.as_ref()?.get("service.yaml"),
                _ => None,
            })
            .unwrap();
        let own = Some("10.1.2.3".parse().unwrap());
        parsed(file.as_bytes(), Path::new("/etc/edgerush/ratelimit"), own).unwrap()
    }

    /// What the operator writes is a file a pod runs, as the ControlPlane declares it: each
    /// pod at its own address, finding the others through their headless Service, its
    /// certificate the one the control plane writes.
    #[test]
    fn a_pod_runs_the_file_the_operator_writes() {
        let settings = the_operators("{ shards: 64, copies: 1, maxKeyBytes: 1048576 }");
        assert_eq!(settings.listen, "0.0.0.0:7000".parse().unwrap());
        assert_eq!(settings.metrics, Some("0.0.0.0:9090".parse().unwrap()));
        assert_eq!(settings.store.address, "10.1.2.3:7000");
        assert_eq!(
            settings.store.seeds,
            ["edgerush-ratelimit-payments-peers.edge.svc:7000"]
        );
        assert_eq!(settings.store.config.shards, 64);
        assert_eq!(settings.store.config.replicas, 1);
        assert_eq!(settings.store.config.max_key_bytes, Some(1 << 20));
        assert!(settings.store.forwarder_decides);
        assert_eq!(
            settings.tls,
            Some(PodFiles {
                certificate: "/etc/edgerush/identity/tls.crt".into(),
                key: "/etc/edgerush/identity/tls.key".into(),
                authorities: "/etc/edgerush/identity/ca.crt".into(),
                trust_domain: "payments.cluster.local".to_owned(),
            })
        );
        let defaults = the_operators("{}");
        assert_eq!(defaults.store.config.shards, 256);
        assert_eq!(defaults.store.config.replicas, 2);
        assert_eq!(defaults.store.config.max_key_bytes, Some(MAX_KEY_BYTES));
        assert!(
            !the_operators("{ forwarderDecides: false }")
                .store
                .forwarder_decides
        );
    }
}
