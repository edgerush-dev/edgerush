//! The harness's config file: read, compiled, and looked at again for changes.
//!
//! A change is a change of the file's bytes. Time stamps are not trusted: they are coarse
//! on some file systems and a restored file brings its old one along.

use edgerush_config::{Compiled, Config, ConfigError, compile};
use std::fmt::{self, Display, Formatter};
use std::path::PathBuf;
use std::{fs, io};

/// A config file and what was in it when it was last looked at.
#[derive(Debug)]
pub(crate) struct ConfigFile {
    path: PathBuf,
    /// The bytes, or why there were none.
    seen: Result<Vec<u8>, io::ErrorKind>,
}

impl ConfigFile {
    /// Reads the file for the first time.
    pub(crate) fn open(path: PathBuf) -> Result<(Self, Compiled), Rejected> {
        let bytes = fs::read(&path).map_err(Rejected::Read)?;
        let compiled = compiled(&bytes)?;
        let seen = Ok(bytes);
        let file = Self { path, seen };
        Ok((file, compiled))
    }

    /// Reads the file again. `None` while it is as it was the last time — whether that was
    /// a config, one that was rejected or no file at all, so that nothing is said twice.
    /// A file caught half written is rejected, and read again when it is whole.
    pub(crate) fn changed(&mut self) -> Option<Result<Compiled, Rejected>> {
        let read = fs::read(&self.path);
        let as_it_was = match (&read, &self.seen) {
            (Ok(now), Ok(before)) => now == before,
            (Err(now), Err(before)) => now.kind() == *before,
            _ => false,
        };
        if as_it_was {
            return None;
        }
        let (seen, outcome) = match read {
            Ok(bytes) => {
                let outcome = compiled(&bytes);
                (Ok(bytes), outcome)
            }
            Err(error) => (Err(error.kind()), Err(Rejected::Read(error))),
        };
        self.seen = seen;
        Some(outcome)
    }
}

fn compiled(yaml: &[u8]) -> Result<Compiled, Rejected> {
    let config: Config =
        serde_saphyr::from_slice(yaml).map_err(|error| Rejected::Parse(Box::new(error)))?;
    compile(&config).map_err(|problems| Rejected::Invalid(Problems(problems)))
}

/// Why a config file cannot be run.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Rejected {
    /// The file cannot be read.
    #[error("it cannot be read: {0}")]
    Read(io::Error),
    /// The file is not YAML, or not a config. In a box, as the parser's error is large
    /// and would be carried by every result on the way.
    #[error("{0}")]
    Parse(Box<serde_saphyr::Error>),
    /// The config has problems.
    #[error("{0}")]
    Invalid(Problems),
}

/// Every problem of a config, a line each.
#[derive(Debug)]
pub(crate) struct Problems(Vec<ConfigError>);

impl Display for Problems {
    fn fmt(&self, to: &mut Formatter<'_>) -> fmt::Result {
        for (at, problem) in self.0.iter().enumerate() {
            if at > 0 {
                writeln!(to)?;
            }
            write!(to, "{problem}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOTHING: &str = "listeners: {}\nroutes: []\nupstreams: {}\n";
    const ONE_UPSTREAM: &str =
        "listeners: {}\nroutes: []\nupstreams: { web: { load_balancer: p2c, endpoints: [] } }\n";

    /// A file of the test's own in the system's temporary directory, gone with the test.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(test: &str, content: &str) -> Self {
            let name = format!("edgerush-{}-config_file-{test}.yaml", std::process::id());
            let path = std::env::temp_dir().join(name);
            fs::write(&path, content).unwrap();
            Self(path)
        }

        fn open(&self) -> Result<(ConfigFile, Compiled), Rejected> {
            ConfigFile::open(self.0.clone())
        }

        fn write(&self, content: &str) {
            fs::write(&self.0, content).unwrap();
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _gone_already = fs::remove_file(&self.0);
        }
    }

    #[test]
    fn a_file_is_compiled_when_it_is_opened() {
        let (_, compiled) = Scratch::new("opened", ONE_UPSTREAM).open().unwrap();
        assert_eq!(compiled.upstreams.len(), 1);
    }

    #[test]
    fn a_changed_file_is_compiled_again() {
        let scratch = Scratch::new("changed-again", NOTHING);
        let (mut file, compiled) = scratch.open().unwrap();
        assert_eq!(compiled.upstreams.len(), 0);
        scratch.write(ONE_UPSTREAM);
        let compiled = file.changed().unwrap().unwrap();
        assert_eq!(compiled.upstreams.len(), 1);
    }

    #[test]
    fn a_file_that_is_not_there_cannot_be_opened() {
        let scratch = Scratch::new("never-there", "");
        fs::remove_file(&scratch.0).unwrap();
        let rejected = scratch.open().unwrap_err();
        assert!(matches!(rejected, Rejected::Read(_)), "{rejected}");
        assert!(rejected.to_string().starts_with("it cannot be read: "));
    }

    #[test]
    fn what_is_not_a_config_is_rejected_with_its_place() {
        let scratch = Scratch::new("not-a-config", "listeners: {}\nroutes: 7\n");
        let rejected = scratch.open().unwrap_err();
        assert!(matches!(rejected, Rejected::Parse(_)), "{rejected}");
        assert!(rejected.to_string().contains("line 2"), "{rejected}");
    }

    #[test]
    fn every_problem_of_a_config_is_told() {
        let yaml = r#"
listeners:
  web: { address: "127.0.0.1:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: shop
    listeners: [web, nowhere]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: / }
        forward: { backends: [{ upstream: gone, weight: 1 }] }
upstreams: {}
"#;
        let rejected = Scratch::new("problems", yaml).open().unwrap_err();
        assert!(matches!(rejected, Rejected::Invalid(_)), "{rejected}");
        let told = rejected.to_string();
        assert_eq!(told.lines().count(), 2, "{told}");
        assert!(told.contains("nowhere") && told.contains("gone"), "{told}");
    }

    #[test]
    fn a_file_that_stays_as_it_is_has_not_changed() {
        let scratch = Scratch::new("unchanged", NOTHING);
        let (mut file, _) = scratch.open().unwrap();
        assert!(file.changed().is_none());
        // Written again with the same bytes: a new time stamp is not a change.
        scratch.write(NOTHING);
        assert!(file.changed().is_none());
    }

    #[test]
    fn other_bytes_are_a_change_and_are_told_once() {
        let scratch = Scratch::new("changed", NOTHING);
        let (mut file, _) = scratch.open().unwrap();
        scratch.write(ONE_UPSTREAM);
        let compiled = file.changed().unwrap().unwrap();
        assert_eq!(compiled.upstreams.len(), 1);
        assert!(file.changed().is_none());
    }

    #[test]
    fn a_change_for_the_worse_is_rejected_once_and_a_repair_is_a_change_again() {
        let scratch = Scratch::new("broken", NOTHING);
        let (mut file, _) = scratch.open().unwrap();
        scratch.write("routes: [");
        assert!(matches!(file.changed(), Some(Err(Rejected::Parse(_)))));
        assert!(file.changed().is_none());

        // Back to the very bytes that are running: compiled again all the same, as what
        // was seen last is the broken file.
        scratch.write(NOTHING);
        assert!(matches!(file.changed(), Some(Ok(_))));
        assert!(file.changed().is_none());
    }

    #[test]
    fn a_file_that_goes_away_is_rejected_once_and_is_a_change_when_it_is_back() {
        let scratch = Scratch::new("removed", NOTHING);
        let (mut file, _) = scratch.open().unwrap();
        fs::remove_file(&scratch.0).unwrap();
        assert!(matches!(file.changed(), Some(Err(Rejected::Read(_)))));
        assert!(file.changed().is_none());
        scratch.write(NOTHING);
        assert!(matches!(file.changed(), Some(Ok(_))));
    }
}
