//! The harness's config file: read, compiled, and looked at again for changes.
//!
//! The file names the files of each certificate rather than holding it
//! ([07 §1](../../../docs/07-config-and-dsl.md)): the harness reads them, as the control
//! plane will read Secrets, each relative to the config file's directory unless its path
//! is absolute.
//!
//! A change is a change of the bytes of the file, or of a certificate file it names: that is
//! how a certificate is rotated, with the config file untouched. Time stamps are not
//! trusted: they are coarse on some file systems and a restored file brings its old one
//! along.

use edgerush_config::{
    Certificate, CertificateFiles, Compiled, Config, ConfigError, HarnessFile, Matches, compile,
    compile_with_matches,
};
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::path::{Path, PathBuf};
use std::{fs, io};

/// A config file, and what it and the certificate files it named held when last looked at.
#[derive(Debug)]
pub(crate) struct ConfigFile {
    path: PathBuf,
    /// The config file's bytes, or why there were none.
    seen: Result<Vec<u8>, io::ErrorKind>,
    /// Each certificate file the config named, as it was read for it.
    certificates: Vec<Seen>,
}

/// A certificate file as it was last read: where from, and its bytes or why there were none.
struct Seen {
    path: PathBuf,
    bytes: Result<Vec<u8>, io::ErrorKind>,
}

impl fmt::Debug for Seen {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // The bytes may be a private key's: whatever prints this must not print them.
        f.debug_struct("Seen")
            .field("path", &self.path)
            .field("bytes", &self.bytes.as_ref().map(Vec::len))
            .finish()
    }
}

/// Whether what was read now is what was read before.
fn same(now: &io::Result<Vec<u8>>, before: &Result<Vec<u8>, io::ErrorKind>) -> bool {
    match (now, before) {
        (Ok(now), Ok(before)) => now == before,
        (Err(now), Err(before)) => now.kind() == *before,
        _ => false,
    }
}

impl ConfigFile {
    /// Reads the file for the first time.
    pub(crate) fn open(path: PathBuf) -> Result<(Self, Compiled), Rejected> {
        let bytes = fs::read(&path).map_err(Rejected::Read)?;
        let (certificates, compiled) = compiled(&bytes, directory(&path));
        let compiled = compiled?;
        let seen = Ok(bytes);
        let file = Self {
            path,
            seen,
            certificates,
        };
        Ok((file, compiled))
    }

    /// Reads the file again, and the certificate files it named. `None` while all are as
    /// they were the last time — whether that was a config, one that was rejected or no
    /// file at all, so that nothing is said twice. A file caught half written is rejected,
    /// and read again when it is whole; so is a certificate caught half rotated, which the
    /// data plane refuses as a key that is not its certificate's.
    pub(crate) fn changed(&mut self) -> Option<Result<Compiled, Rejected>> {
        let read = fs::read(&self.path);
        let as_it_was = same(&read, &self.seen)
            && self
                .certificates
                .iter()
                .all(|seen| same(&fs::read(&seen.path), &seen.bytes));
        if as_it_was {
            return None;
        }
        let (seen, certificates, outcome) = match read {
            Ok(bytes) => {
                let (certificates, outcome) = compiled(&bytes, directory(&self.path));
                (Ok(bytes), certificates, outcome)
            }
            Err(error) => (Err(error.kind()), Vec::new(), Err(Rejected::Read(error))),
        };
        self.seen = seen;
        self.certificates = certificates;
        Some(outcome)
    }
}

/// The directory a config file's relative paths are taken from: its own.
fn directory(path: &Path) -> &Path {
    path.parent().unwrap_or(Path::new(""))
}

/// Compiles the config `yaml` states, with the certificates read from the files it names,
/// relative to `directory`; and says what was read of each of those files, for the next
/// look to compare with. What compiles is what was read: a file that changes after is
/// another change.
fn compiled(yaml: &[u8], directory: &Path) -> (Vec<Seen>, Result<Compiled, Rejected>) {
    let file = match parsed(yaml) {
        Ok(file) => file,
        Err(rejected) => return (Vec::new(), Err(rejected)),
    };
    let (seen, certificates) = certificates(&file.certificates, directory);
    let outcome = certificates.and_then(|certificates| {
        let config = file.into_config(certificates);
        compile(&config).map_err(|problems| Rejected::Invalid(Problems(problems)))
    });
    (seen, outcome)
}

/// The file `yaml` states.
fn parsed(yaml: &[u8]) -> Result<HarnessFile, Rejected> {
    // Without the lines around a mistake, which the parser quotes by default: what a
    // rejection says goes to the log, and a key pasted into the file would go with it. The
    // fuzz target `config` reads with these same options: change both.
    let options = serde_saphyr::options! { with_snippet: false };
    serde_saphyr::from_slice_with_options(yaml, options)
        .map_err(|error| Rejected::Parse(Box::new(error)))
}

/// The config at `path` as `edgerush explain` and `edgerush test` read it: compiled as the
/// harness compiles it, with every listener's matches kept, and its certificates known by
/// name alone. No certificate file is opened, so a config can be explained where its keys
/// are not ([22 §3](../../../docs/22-explain-and-test.md)).
pub(crate) fn offline(path: &Path) -> Result<(Config, Compiled, Matches), Rejected> {
    let yaml = fs::read(path).map_err(Rejected::Read)?;
    let file = parsed(&yaml)?;
    let named = file
        .certificates
        .keys()
        .map(|name| {
            let unread = Certificate {
                chain: String::new(),
                key: String::new(),
            };
            (name.clone(), unread)
        })
        .collect();
    let config = file.into_config(named);
    let (compiled, matches) =
        compile_with_matches(&config).map_err(|problems| Rejected::Invalid(Problems(problems)))?;
    Ok((config, compiled, matches))
}

/// Reads each certificate from the files `named` for it, relative to `directory`; and says
/// what was read of each file.
fn certificates(
    named: &BTreeMap<String, CertificateFiles>,
    directory: &Path,
) -> (Vec<Seen>, Result<BTreeMap<String, Certificate>, Rejected>) {
    let mut seen = Vec::new();
    let mut read = BTreeMap::new();
    let mut unread = Vec::new();
    for (name, files) in named {
        let mut text = |file: &Path| {
            let path = directory.join(file);
            let bytes = fs::read(&path);
            seen.push(Seen {
                path: path.clone(),
                bytes: bytes.as_ref().map(Clone::clone).map_err(io::Error::kind),
            });
            let text = bytes.and_then(|bytes| {
                String::from_utf8(bytes)
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "it is not UTF-8"))
            });
            text.map_err(|error| {
                unread.push(Unread {
                    name: name.clone(),
                    path,
                    error,
                });
            })
            .ok()
        };
        let chain = text(&files.chain_file);
        let key = text(&files.key_file);
        if let (Some(chain), Some(key)) = (chain, key) {
            read.insert(name.clone(), Certificate { chain, key });
        }
    }
    let read = if unread.is_empty() {
        Ok(read)
    } else {
        Err(Rejected::Certificates(Unreadable(unread)))
    };
    (seen, read)
}

/// Why a config file cannot be run.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Rejected {
    /// The file cannot be read.
    #[error("it cannot be read: {0}")]
    Read(io::Error),
    /// The file is not YAML, or not a config, told by the line and column of the mistake
    /// and never by quoting the lines around it. In a box, as the parser's error is large
    /// and would be carried by every result on the way.
    #[error("{0}")]
    Parse(Box<serde_saphyr::Error>),
    /// Files of its certificates cannot be read.
    #[error("{0}")]
    Certificates(Unreadable),
    /// The config has problems.
    #[error("{0}")]
    Invalid(Problems),
}

/// Every certificate file that cannot be read, a line each, told by its path.
#[derive(Debug)]
pub(crate) struct Unreadable(Vec<Unread>);

/// A certificate file that cannot be read.
#[derive(Debug)]
struct Unread {
    /// The certificate's name.
    name: String,
    /// Where the file was looked for.
    path: PathBuf,
    error: io::Error,
}

impl Display for Unreadable {
    fn fmt(&self, to: &mut Formatter<'_>) -> fmt::Result {
        for (at, unread) in self.0.iter().enumerate() {
            if at > 0 {
                writeln!(to)?;
            }
            write!(
                to,
                "certificate `{}`: {} cannot be read: {}",
                unread.name,
                unread.path.display(),
                unread.error
            )?;
        }
        Ok(())
    }
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
        assert_eq!(compiled.upstreams().len(), 1);
    }

    #[test]
    fn a_changed_file_is_compiled_again() {
        let scratch = Scratch::new("changed-again", NOTHING);
        let (mut file, compiled) = scratch.open().unwrap();
        assert_eq!(compiled.upstreams().len(), 0);
        scratch.write(ONE_UPSTREAM);
        let compiled = file.changed().unwrap().unwrap();
        assert_eq!(compiled.upstreams().len(), 1);
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

    /// A key pasted into the config file is refused, and told by the place of the mistake,
    /// not by quoting the lines around it: those are the key's, and what is told goes to
    /// the log.
    #[test]
    fn a_rejected_config_does_not_repeat_its_private_keys() {
        let yaml = r#"
listeners: {}
routes: []
upstreams: {}
certificates:
  shop:
    chain_file: shop.crt
    key: |
      -----BEGIN PRIVATE KEY-----
      NOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEY0
      NOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEYNOTAKEY1
      -----END PRIVATE KEY-----
"#;
        let rejected = Scratch::new("keys", yaml).open().unwrap_err();
        assert!(matches!(rejected, Rejected::Parse(_)), "{rejected}");
        let told = rejected.to_string();
        assert!(told.contains("line 8"), "{told}");
        assert!(!told.contains("NOTAKEY"), "{told}");
    }

    /// A directory of the test's own in the system's temporary directory, for a config file
    /// and the files it names; what is written into it goes with the test.
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(test: &str) -> Self {
            let name = format!("edgerush-{}-config_file-{test}", std::process::id());
            let path = std::env::temp_dir().join(name);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        /// Writes `content` to the file `name` in it, and says where that is.
        fn write(&self, name: &str, content: &str) -> PathBuf {
            let path = self.0.join(name);
            fs::write(&path, content).unwrap();
            path
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            // The files in it one by one, then the directory itself, which is refused if
            // anything else is left in it.
            if let Ok(entries) = fs::read_dir(&self.0) {
                for entry in entries.flatten() {
                    let _gone_already = fs::remove_file(entry.path());
                }
            }
            let _gone_already = fs::remove_dir(&self.0);
        }
    }

    /// A config whose `https` listener presents the certificate `shop`, read from
    /// `chain_file` and `key_file`.
    fn presenting_shop(chain_file: &str, key_file: &str) -> String {
        format!(
            r#"listeners:
  web: {{ address: "127.0.0.1:8443", protocol: https, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate, tls: {{ certificates: [shop] }} }}
routes: []
upstreams: {{}}
certificates:
  shop: {{ chain_file: '{chain_file}', key_file: '{key_file}' }}
"#
        )
    }

    /// A certificate is read from the two files the config file names for it: a path
    /// relative to the config file's directory, wherever the harness was started from, or
    /// an absolute one.
    #[test]
    fn a_certificate_is_read_from_its_files_relative_to_the_config_file() {
        let scratch = ScratchDir::new("certificate-files");
        scratch.write("shop.crt", "the chain");
        let key = scratch.write("shop.key", "the key");
        let yaml = presenting_shop("shop.crt", &key.display().to_string());
        let (_, compiled) = ConfigFile::open(scratch.write("config.yaml", &yaml)).unwrap();
        let shop = &compiled.listeners()[0].tls.as_ref().unwrap().certificates[0];
        assert_eq!(shop.name, "shop");
        assert_eq!(shop.certificate.chain, "the chain");
        assert_eq!(shop.certificate.key, "the key");
    }

    /// A certificate file that cannot be read refuses the config, told by its path; every
    /// such file is told.
    #[test]
    fn a_certificate_file_that_cannot_be_read_is_refused_by_its_path() {
        let scratch = ScratchDir::new("certificate-unread");
        let yaml = presenting_shop("shop.crt", "shop.key");
        let rejected = ConfigFile::open(scratch.write("config.yaml", &yaml)).unwrap_err();
        assert!(matches!(rejected, Rejected::Certificates(_)), "{rejected}");
        let told = rejected.to_string();
        assert_eq!(told.lines().count(), 2, "{told}");
        for file in ["shop.crt", "shop.key"] {
            let path = scratch.0.join(file);
            let unread = format!("certificate `shop`: {} cannot be read", path.display());
            assert!(told.contains(&unread), "{told}");
        }
    }

    /// A config presenting `shop` from `shop.crt` and `shop.key` in a directory of its own,
    /// opened.
    fn opened_with_shop(scratch: &ScratchDir) -> ConfigFile {
        scratch.write("shop.crt", "chain 1");
        scratch.write("shop.key", "key 1");
        let yaml = presenting_shop("shop.crt", "shop.key");
        let (file, _) = ConfigFile::open(scratch.write("config.yaml", &yaml)).unwrap();
        file
    }

    /// What `shop` is in what `file` compiled now that it has changed.
    fn shop_after_change(file: &mut ConfigFile) -> (String, String) {
        let compiled = file.changed().expect("a change").unwrap();
        let shop = &compiled.listeners()[0].tls.as_ref().unwrap().certificates[0];
        (shop.certificate.chain.clone(), shop.certificate.key.clone())
    }

    /// A certificate whose files change is read again with the config file untouched:
    /// that is how one is rotated. Files as they were are no change.
    #[test]
    fn a_certificate_whose_files_change_is_read_again() {
        let scratch = ScratchDir::new("certificate-rotated");
        let mut file = opened_with_shop(&scratch);
        assert!(file.changed().is_none());
        scratch.write("shop.key", "key 1");
        assert!(file.changed().is_none());

        scratch.write("shop.crt", "chain 2");
        scratch.write("shop.key", "key 2");
        assert_eq!(
            shop_after_change(&mut file),
            ("chain 2".into(), "key 2".into())
        );
        assert!(file.changed().is_none());
    }

    /// A certificate caught half way through its rotation, its chain new and its key not
    /// yet, is read as it is, for the data plane to refuse the pair; the rest of the
    /// rotation is a change of its own, and the whole pair is read.
    #[test]
    fn a_certificate_caught_half_rotated_is_read_again_when_it_is_whole() {
        let scratch = ScratchDir::new("certificate-half-rotated");
        let mut file = opened_with_shop(&scratch);
        scratch.write("shop.crt", "chain 2");
        assert_eq!(
            shop_after_change(&mut file),
            ("chain 2".into(), "key 1".into())
        );
        assert!(file.changed().is_none());
        scratch.write("shop.key", "key 2");
        assert_eq!(
            shop_after_change(&mut file),
            ("chain 2".into(), "key 2".into())
        );
    }

    /// What the harness holds of a certificate's files, to compare with, is printed without
    /// them: one of them is a private key.
    #[test]
    fn what_is_held_of_a_certificate_is_printed_without_it() {
        let scratch = ScratchDir::new("certificate-printed");
        let file = opened_with_shop(&scratch);
        let printed = format!("{file:?}");
        assert!(printed.contains("shop.key"), "{printed}");
        // "key 1", the key file's bytes as a `Vec<u8>` prints them.
        assert!(!printed.contains("107, 101, 121, 32, 49"), "{printed}");
    }

    /// A certificate file that goes away is told once, as the config file's going is, and
    /// is a change again when it is back.
    #[test]
    fn a_certificate_file_that_goes_away_is_told_once_and_is_a_change_when_it_is_back() {
        let scratch = ScratchDir::new("certificate-removed");
        let mut file = opened_with_shop(&scratch);
        fs::remove_file(scratch.0.join("shop.key")).unwrap();
        assert!(matches!(
            file.changed(),
            Some(Err(Rejected::Certificates(_)))
        ));
        assert!(file.changed().is_none());
        scratch.write("shop.key", "key 2");
        assert_eq!(
            shop_after_change(&mut file),
            ("chain 1".into(), "key 2".into())
        );
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
        assert_eq!(compiled.upstreams().len(), 1);
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
