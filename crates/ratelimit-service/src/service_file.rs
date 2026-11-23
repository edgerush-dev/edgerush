//! What a pod runs as, from its config file (`edgerush_config::ServiceFile`): the file's
//! bytes read, what the types cannot check of them checked, and the values of what they
//! leave out filled in.
//!
//! Here rather than in the binary so that a fuzz target can reach the reading and checking
//! as the binary does them: whatever bytes it is given, neither may fail, and a refusal is
//! told without failing.

use crate::cluster::settings::{Settings, SettingsError, settings};
use crate::secure::PodFiles;
use edgerush_config::{ServiceFile, ServiceMode};
use std::net::SocketAddr;
use std::path::Path;

/// What `keys.max_bytes` is when left out: 256 MiB.
pub const MAX_KEY_BYTES: u64 = 256 << 20;

/// What a pod runs as.
#[derive(Debug, PartialEq, Eq)]
pub struct PodSettings {
    /// Where it serves data planes and the other pods.
    pub listen: SocketAddr,
    /// Where it serves its metrics and readiness, if anywhere.
    pub metrics: Option<SocketAddr>,
    /// Its credentials, or none to be served plain.
    pub tls: Option<PodFiles>,
    /// The mode it serves, and the store it serves it on.
    pub serves: Serves,
}

/// The mode a pod serves, with the store it serves it on.
#[derive(Debug, PartialEq, Eq)]
pub enum Serves {
    /// Its data planes deciding from their copies, on this store; each batch says their
    /// cycle.
    Asynchronous(Settings),
    /// The key's owner deciding every request, on this store.
    Synchronous(Settings),
}

impl Serves {
    /// The mode it is.
    #[must_use]
    pub fn mode(&self) -> ServiceMode {
        match self {
            Self::Asynchronous(_) => ServiceMode::Asynchronous,
            Self::Synchronous(_) => ServiceMode::Synchronous,
        }
    }
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
    /// A setting of the mode the pod does not serve, which it would ignore as if it did
    /// something.
    #[error("`{field}` is the {owner} mode's")]
    OtherModes {
        /// The setting, as the file names it.
        field: &'static str,
        /// The mode it is for.
        owner: &'static str,
    },
    /// No store to run the mode on.
    #[error("the {0} mode runs on the store: it needs `store` and `pod.address`")]
    NoStore(&'static str),
    /// A store the pod could not run on.
    #[error("{0}")]
    Store(#[from] SettingsError),
}

/// What a pod runs as, from the file `yaml` states, its TLS files relative to `directory`
/// unless absolute.
///
/// # Errors
///
/// The first [`FileError`] the file has.
pub fn parsed(yaml: &[u8], directory: &Path) -> Result<PodSettings, FileError> {
    // Without the lines around a mistake, which the parser quotes by default: what is said
    // of a file refused goes to the log, as the data plane's does.
    let options = serde_saphyr::options! { with_snippet: false };
    let file: ServiceFile = serde_saphyr::from_slice_with_options(yaml, options)
        .map_err(|error| FileError::Parse(Box::new(error)))?;
    checked(file, directory)
}

/// What the types cannot check of `file`, and the values of what it leaves out.
fn checked(file: ServiceFile, directory: &Path) -> Result<PodSettings, FileError> {
    let max_key_bytes = file.keys.max_bytes.unwrap_or(MAX_KEY_BYTES);
    if max_key_bytes == 0 {
        return Err(FileError::KeyBytes);
    }
    let mode = file.mode.unwrap_or(ServiceMode::Asynchronous);
    if mode == ServiceMode::Asynchronous
        && file
            .store
            .as_ref()
            .is_some_and(|store| store.forwarder_decides.is_some())
    {
        return Err(FileError::OtherModes {
            field: "store.forwarder_decides",
            owner: "synchronous",
        });
    }
    let name = match mode {
        ServiceMode::Synchronous => "synchronous",
        ServiceMode::Asynchronous => "asynchronous",
    };
    let store = settings(&file, max_key_bytes)?.ok_or(FileError::NoStore(name))?;
    let tls = file.tls.map(|tls| PodFiles {
        certificate: directory.join(tls.certificate),
        key: directory.join(tls.key),
        authorities: directory.join(tls.authorities),
        data_plane: tls.data_plane_id,
    });
    let pod = file.pod;
    let serves = match mode {
        ServiceMode::Synchronous => Serves::Synchronous(store),
        ServiceMode::Asynchronous => Serves::Asynchronous(store),
    };
    Ok(PodSettings {
        listen: pod.listen,
        metrics: pod.metrics,
        tls,
        serves,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The settings from `yaml`, its TLS files relative to `/etc/edgerush`, a refusal as it
    /// is told.
    fn parsed_from(yaml: &str) -> Result<PodSettings, String> {
        parsed(yaml.as_bytes(), Path::new("/etc/edgerush")).map_err(|error| error.to_string())
    }

    /// An asynchronous pod's file, on the store, with `more` after its sections.
    fn with(more: &str) -> String {
        sections("", &format!("store: {{}}\n{more}"))
    }

    /// A pod's file with its `pod` section's `more`, and `rest` after it.
    fn sections(pod: &str, rest: &str) -> String {
        format!("pod: {{ listen: 0.0.0.0:7000, address: rl-0.rl:7000{pod} }}\n{rest}")
    }

    /// An asynchronous pod's store.
    fn asynchronous(settings: PodSettings) -> Settings {
        match settings.serves {
            Serves::Asynchronous(store) => store,
            Serves::Synchronous(_) => panic!("a synchronous pod"),
        }
    }

    #[test]
    fn the_mode_is_asynchronous_unless_said_and_either_runs_on_the_store() {
        let parsed = parsed_from(&with("")).unwrap();
        assert_eq!(parsed.serves.mode(), ServiceMode::Asynchronous);
        assert_eq!(asynchronous(parsed).address, "rl-0.rl:7000");
        let said = parsed_from(&with("mode: eventual\n")).unwrap_err();
        assert!(said.contains("eventual"), "{said}");
        // Each mode runs on the store, and is refused without it.
        for mode in ["asynchronous", "synchronous"] {
            assert_eq!(
                parsed_from(&format!("mode: {mode}\npod: {{ listen: 0.0.0.0:7000 }}\n")),
                Err(format!(
                    "the {mode} mode runs on the store: it needs `store` and `pod.address`"
                )),
                "{mode}"
            );
        }
        let parsed = parsed_from(&format!("mode: synchronous\n{}", with(""))).unwrap();
        let Serves::Synchronous(store) = parsed.serves else {
            panic!("an asynchronous pod")
        };
        assert_eq!(store.address, "rl-0.rl:7000");
    }

    #[test]
    fn a_mistake_is_told_without_the_lines_around_it() {
        let said = parsed_from(&with("shards: 256\n")).unwrap_err();
        assert!(said.contains("shards"), "{said}");
        assert!(!said.contains("rl-0.rl:7000"), "{said}");
    }

    #[test]
    fn each_mode_takes_only_what_it_reads() {
        // Deciding from a copy is the synchronous mode's alone.
        assert_eq!(
            parsed_from(&sections("", "store: { forwarder_decides: true }\n")),
            Err("`store.forwarder_decides` is the synchronous mode's".to_owned())
        );
        // What the asynchronous mode's pods agreed by before they ran on the store is no
        // field the file knows, and nor is their data planes' cycle, which each batch says.
        for gone in [
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
        assert_eq!(asynchronous(plain).config.max_key_bytes, Some(256 << 20));
        let said = parsed_from(&format!(
            "keys: {{ max_bytes: 1048576 }}\n{}",
            sections(", metrics: 127.0.0.1:9090", "store: {}\n")
        ))
        .unwrap();
        assert_eq!(said.metrics, Some("127.0.0.1:9090".parse().unwrap()));
        assert_eq!(asynchronous(said).config.max_key_bytes, Some(1 << 20));
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
            Some("`pod.address` is needed with a `store` section".to_owned())
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
            "data_plane_id: spiffe://cluster.local/ns/edge/sa/edge",
        ];
        let tls =
            |given: &[&str]| parsed_from(&with(&format!("tls: {{ {} }}\n", given.join(", "))));
        assert_eq!(
            tls(&files).unwrap().tls,
            Some(PodFiles {
                certificate: "/etc/edgerush/pod.pem".into(),
                key: "/etc/edgerush/pod.key".into(),
                authorities: "/etc/edgerush/ca.pem".into(),
                data_plane: "spiffe://cluster.local/ns/edge/sa/edge".to_owned(),
            })
        );
        for left_out in 0..files.len() {
            let mut given = files.to_vec();
            given.remove(left_out);
            assert!(tls(&given).is_err(), "{given:?}");
        }
    }
}
