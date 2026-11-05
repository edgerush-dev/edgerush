//! Forming the cluster before Kubernetes (question 5): the store's `Authority` run against
//! pods at addresses it is given, as the controller will run it against the pods the API shows,
//! its naming kept in a file of its own. Test tooling, for the bench's example program and the
//! socket tests (`examples/form.rs`); from stage 7 the controller plays this part, its naming
//! kept in the resource's status.
//!
//! An address runs a member from that member's first answer until the address refuses
//! connections, its process gone, or answers as another member, restarted. One that answers
//! nothing but does not refuse — cut off, or hung — still runs, as a pod cut off stays in the
//! API, and the `Authority` waits on it as it does on any pod running.
//!
//! A naming is written to the file before any pod is told of it: one sent and not kept could be
//! named again, under the same generation, to other pods.

use super::pod::{TICK, ask};
use crate::grpc::{Client, Error};
use edgerush_cluster::{Authority, Config, Message, Naming, NodeId, Time};
use edgerush_cluster_synchronous::Value;
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::fmt::Write as _;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

/// Why forming cannot go on.
#[derive(Debug, thiserror::Error)]
pub enum FormingError {
    /// The file could not be read or written.
    #[error("{path}: {error}")]
    File {
        /// The file.
        path: PathBuf,
        /// What failed.
        error: std::io::Error,
    },
    /// The file holds something other than a naming: its line, from 1.
    #[error("{path}, line {line}: not a naming")]
    Malformed {
        /// The file.
        path: PathBuf,
        /// The line.
        line: usize,
    },
}

/// The naming kept in the file at `path`; none if there is no file, as before any cluster was
/// formed.
///
/// # Errors
///
/// A file that cannot be read, or that holds something other than a naming.
pub fn read(path: &Path) -> Result<Option<Naming>, FormingError> {
    let text = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(FormingError::File {
                path: path.to_owned(),
                error,
            });
        }
    };
    parse(&text)
        .map(Some)
        .map_err(|line| FormingError::Malformed {
            path: path.to_owned(),
            line,
        })
}

/// Keeps `naming` in the file at `path`: written whole beside it, synced, then put in its
/// place, so that a stop part-way leaves the naming before it.
///
/// # Errors
///
/// When the file cannot be written.
pub fn write(path: &Path, naming: &Naming) -> Result<(), FormingError> {
    let failed = |error| FormingError::File {
        path: path.to_owned(),
        error,
    };
    let mut beside = OsString::from(path.as_os_str());
    beside.push(".new");
    let beside = PathBuf::from(beside);
    let mut file = std::fs::File::create(&beside).map_err(failed)?;
    file.write_all(text(naming).as_bytes()).map_err(failed)?;
    file.sync_all().map_err(failed)?;
    drop(file);
    std::fs::rename(&beside, path).map_err(failed)
}

/// A naming as the file has it: a line each, `generation`, `latest` with the table's term and
/// version, and `named` with a member's id and address.
fn text(naming: &Naming) -> String {
    let mut text = format!(
        "generation {}\nlatest {} {}\n",
        naming.generation, naming.latest.0, naming.latest.1
    );
    for (member, address) in &naming.named {
        // Writing to a string cannot fail.
        let _ = writeln!(text, "named {member} {address}");
    }
    text
}

/// The naming `text` holds, or the line, from 1, that is not one of its.
fn parse(text: &str) -> Result<Naming, usize> {
    let mut naming = Naming::default();
    let (mut generation, mut latest) = (false, false);
    for (at, line) in text.lines().enumerate() {
        let line_number = at + 1;
        let words: Vec<&str> = line.split(' ').collect();
        let number = |word: &str| word.parse::<u64>().map_err(|_| line_number);
        match words.as_slice() {
            ["generation", value] if !generation => {
                naming.generation = number(value)?;
                generation = true;
            }
            ["latest", term, version] if !latest => {
                naming.latest = (number(term)?, number(version)?);
                latest = true;
            }
            ["named", member, address] if !address.is_empty() => {
                let member: NodeId = number(member)?;
                if naming.named.insert(member, (*address).to_owned()).is_some() {
                    return Err(line_number);
                }
            }
            [""] => {}
            _ => return Err(line_number),
        }
    }
    if generation && latest {
        Ok(naming)
    } else {
        Err(text.lines().count() + 1)
    }
}

/// What came back of asking an address.
enum Heard {
    /// An answer from the member there, and what it said, if anything.
    Answer {
        address: String,
        from: NodeId,
        message: Option<Message<Value>>,
    },
    /// No answer, the address refusing connections: its process gone.
    Refused { address: String },
    /// No answer, the address not refusing: cut off, or slow.
    Silent { address: String },
}

/// The control plane's part in forming, against pods at the addresses it is given.
pub struct Forming {
    authority: Authority,
    file: PathBuf,
    /// The generation last kept in the file.
    kept: u64,
    heartbeat: Time,
    /// The member each address runs, from its first answer.
    members: BTreeMap<String, NodeId>,
    clients: BTreeMap<String, Client>,
    /// Addresses with no member yet, being asked who they are, and when each was last asked.
    asking: BTreeSet<String>,
    asked: BTreeMap<String, Time>,
}

impl Forming {
    /// For pods started with `config`, its naming kept in the file at `file`: resumed from it if
    /// there is one, as the control plane restarted.
    ///
    /// # Errors
    ///
    /// When the file cannot be read, or holds no naming.
    pub fn new(config: &Config, file: PathBuf) -> Result<Self, FormingError> {
        let authority = match read(&file)? {
            Some(naming) => Authority::resumed(config, naming),
            None => Authority::new(config),
        };
        Ok(Self {
            kept: authority.generation(),
            authority,
            file,
            heartbeat: config.heartbeat,
            members: BTreeMap::new(),
            clients: BTreeMap::new(),
            asking: BTreeSet::new(),
            asked: BTreeMap::new(),
        })
    }

    /// The generation last named, 0 for none.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.authority.generation()
    }

    /// Forms the cluster of the pods at `addresses`, as the API would show them running, for as
    /// long as it is left to: every [`TICK`], asking each address with no member yet who it is,
    /// and doing as the `Authority` says.
    ///
    /// # Errors
    ///
    /// When a naming cannot be kept in the file: nothing is told of it, and forming stops.
    pub async fn run(mut self, addresses: Vec<String>) -> Result<(), FormingError> {
        let started = Instant::now();
        let (heard, mut hearing) = mpsc::unbounded_channel();
        let mut ticks = tokio::time::interval(TICK);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = ticks.tick() => {
                    let now = Time::try_from(started.elapsed().as_nanos()).unwrap_or(Time::MAX);
                    self.tick(now, &addresses, &heard)?;
                }
                Some(news) = hearing.recv() => self.hear(news),
            }
        }
    }

    /// One tick at `now`: who each new address runs asked, the `Authority` ticked with the pods
    /// running, a new naming kept, then what it says sent.
    fn tick(
        &mut self,
        now: Time,
        addresses: &[String],
        heard: &mpsc::UnboundedSender<Heard>,
    ) -> Result<(), FormingError> {
        for address in addresses {
            let due = self
                .asked
                .get(address)
                .is_none_or(|at| now.saturating_sub(*at) >= self.heartbeat);
            if !self.members.contains_key(address) && !self.asking.contains(address) && due {
                self.asking.insert(address.clone());
                self.asked.insert(address.clone(), now);
                // Any probe names the member that answers it; what it reports is of no life of
                // the `Authority`'s, and is not handed to it.
                self.send(address, Message::Probe { life: 0, round: 0 }, heard);
            }
        }
        let running: BTreeMap<NodeId, String> = self
            .members
            .iter()
            .map(|(address, member)| (*member, address.clone()))
            .collect();
        let out = self.authority.tick::<Value>(now, &running);
        if self.authority.generation() != self.kept {
            write(&self.file, &self.authority.naming())?;
            self.kept = self.authority.generation();
        }
        for (to, message) in out {
            if let Some(address) = running.get(&to) {
                self.send(address, message, heard);
            }
        }
        Ok(())
    }

    /// Asks the address `address` `message`, what comes of it to be heard.
    fn send(
        &mut self,
        address: &str,
        message: Message<Value>,
        heard: &mpsc::UnboundedSender<Heard>,
    ) {
        let client = match self.clients.get(address) {
            Some(client) => client.clone(),
            None => match Client::new(address) {
                Ok(client) => {
                    self.clients.insert(address.to_owned(), client.clone());
                    client
                }
                Err(_) => {
                    let _ended = heard.send(Heard::Refused {
                        address: address.to_owned(),
                    });
                    return;
                }
            },
        };
        let (address, heard) = (address.to_owned(), heard.clone());
        tokio::spawn(async move {
            let news = match ask::<Value>(&client, message).await {
                Ok((from, message)) => Heard::Answer {
                    address,
                    from,
                    message,
                },
                Err(Error::Status(_) | Error::Address(_)) if refuses(&address).await => {
                    Heard::Refused { address }
                }
                Err(_) => Heard::Silent { address },
            };
            let _ended = heard.send(news);
        });
    }

    /// Takes in what came of asking an address.
    fn hear(&mut self, news: Heard) {
        match news {
            Heard::Answer {
                address,
                from,
                message,
            } => {
                self.asking.remove(&address);
                // An address answering as another member: its pod restarted, the member before
                // gone with its life.
                let before = self.members.insert(address, from);
                if let Some(message) = message
                    && before == Some(from)
                {
                    self.authority.receive(from, message);
                }
            }
            Heard::Refused { address } => {
                self.asking.remove(&address);
                self.members.remove(&address);
            }
            Heard::Silent { address } => {
                self.asking.remove(&address);
            }
        }
    }
}

/// How long a refusal is waited for: on Windows a connection to a port nothing listens on is
/// refused only after its tries over some two seconds, where Linux refuses at once.
const REFUSED_WITHIN: std::time::Duration = std::time::Duration::from_secs(5);

/// Whether `address` refuses a plain TCP connection: nothing listens there, its process gone.
async fn refuses(address: &str) -> bool {
    matches!(
        tokio::time::timeout(REFUSED_WITHIN, TcpStream::connect(address)).await,
        Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn naming() -> Naming {
        Naming {
            generation: 3,
            named: [
                (17, "127.0.0.1:7001".to_owned()),
                (42, "rl-1.rl:7000".to_owned()),
            ]
            .into(),
            latest: (12_884_901_888, 7),
        }
    }

    /// A file of the scratch directory of its own, for one test.
    fn path(name: &str) -> PathBuf {
        let directory =
            std::env::temp_dir().join(format!("edgerush-forming-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        directory.join(name)
    }

    #[test]
    fn a_naming_written_is_read_back_whole() {
        let file = path("whole");
        write(&file, &naming()).unwrap();
        assert_eq!(read(&file).unwrap(), Some(naming()));
        let none = Naming::default();
        write(&file, &none).unwrap();
        assert_eq!(read(&file).unwrap(), Some(none));
        std::fs::remove_file(&file).unwrap();
    }

    #[test]
    fn no_file_is_no_naming() {
        assert_eq!(read(&path("never-written")).unwrap(), None);
    }

    #[test]
    fn a_write_stopped_part_way_leaves_the_naming_before() {
        let file = path("torn");
        write(&file, &naming()).unwrap();
        let mut beside = OsString::from(file.as_os_str());
        beside.push(".new");
        std::fs::write(&beside, "generation 4\nlat").unwrap();
        assert_eq!(read(&file).unwrap(), Some(naming()));
        std::fs::remove_file(&file).unwrap();
        std::fs::remove_file(&beside).unwrap();
    }

    #[test]
    fn anything_but_a_naming_is_refused_at_its_line() {
        for (text, line) in [
            ("", 1),
            ("generation 3\n", 2),
            ("latest 1 2\n", 2),
            ("generation 3\nlatest 1 2\ngeneration 4\n", 3),
            ("generation x\nlatest 1 2\n", 1),
            ("generation 3\nlatest 1\n", 2),
            ("generation 3\nlatest 1 2\nnamed 5\n", 3),
            ("generation 3\nlatest 1 2\nnamed 5 a:1\nnamed 5 b:1\n", 4),
            ("generation 3\nlatest 1 2\nwho 5 a:1\n", 3),
        ] {
            assert_eq!(parse(text), Err(line), "{text:?}");
        }
        assert_eq!(parse(&text(&naming())), Ok(naming()));
    }
}
