//! Names resolved for as long as they are wanted ([03 §6](../../../docs/03-data-plane.md)):
//! each looked up again as its answer's time to live runs out, and what it resolves to
//! handed on. The data plane's are those its `dns` upstreams name, handed to the proxy; the
//! control plane's, those of the gateway's own headless Services, whose pods data planes
//! are not to send to.
//!
//! A name is asked for its A and AAAA records, of each nameserver in turn, over UDP and,
//! for an answer cut short, again over TCP ([`edgerush_dns`] writes and reads them). An
//! answer, that the name has addresses or that it has none, is taken in; a lookup that
//! fails, no nameserver answering in time or each failing, keeps what was taken in last,
//! and the first lookup of a name that fails is taken in as none, so that a data plane
//! waiting on it to start serves. Either is kept for its time to live, and neither for less
//! than [`Pace::floor`], so that a short one cannot make a lookup spin.

use edgerush_dns::{Answer, Kind, Malformed, nameservers, query, read};
use std::collections::{BTreeSet, HashMap};
use std::fmt::Display;
use std::hash::{BuildHasher, RandomState};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::rc::Rc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, UdpSocket};
use tokio::task::JoinHandle;

/// What is told what fails, with the name it failed for.
type Said = Rc<dyn Fn(&str, &dyn Display)>;

/// Where `resolv.conf` is, on Linux.
const RESOLV_CONF: &str = "/etc/resolv.conf";

/// How long answers are kept at least, and how long a nameserver is waited for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Pace {
    /// The least time an answer, or a failure, is kept.
    pub(crate) floor: Duration,
    /// How long a nameserver has to answer one query, over UDP or over TCP.
    pub(crate) patience: Duration,
}

impl Default for Pace {
    /// 5 s, the least linkerd2-proxy holds its answers for, and 2 s.
    fn default() -> Self {
        Self {
            floor: Duration::from_secs(5),
            patience: Duration::from_secs(2),
        }
    }
}

/// The nameservers `/etc/resolv.conf` lists, each on port 53: none where there is no such
/// file, as on Windows.
pub(crate) fn system_nameservers() -> Vec<SocketAddr> {
    std::fs::read_to_string(RESOLV_CONF)
        .map(|text| {
            nameservers(&text)
                .into_iter()
                .map(|address| SocketAddr::new(address, 53))
                .collect()
        })
        .unwrap_or_default()
}

/// Why a lookup failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum Failure {
    /// There is no nameserver to ask.
    #[error("no nameserver to ask: {RESOLV_CONF} lists none")]
    NoNameserver,
    /// None answered, or each failed: what the last one did.
    #[error("{0}")]
    Unanswered(String),
}

/// What a lookup came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Looked {
    /// The name's addresses, A and AAAA together, none if it has none, and how long that
    /// may be kept.
    Answered(Vec<IpAddr>, Duration),
    /// No answer, and why.
    Failed(Failure),
}

/// Who is handed what a name resolves to: its addresses, none for a name that resolves to
/// none or was never answered for, or nothing at all for a name no longer resolved, to be
/// forgotten; or why it could not take them.
type Taker = Rc<dyn Fn(&str, Option<Vec<IpAddr>>) -> Result<(), String>>;

/// Resolves the names `wanted` holds, asking `servers` at `pace`, for as long as it is
/// polled, each answer handed to `take`: a name `wanted` comes to hold is resolved from
/// then on, and one it no longer holds is no longer resolved, and `take` is told to forget
/// it. `said` is told each failure that differs from the last for its name, with the name,
/// and what `take` could not take. In a `LocalSet`, on a thread and runtime of its own.
pub(crate) async fn resolve_names(
    mut wanted: tokio::sync::watch::Receiver<BTreeSet<String>>,
    servers: Vec<SocketAddr>,
    pace: Pace,
    take: impl Fn(&str, Option<Vec<IpAddr>>) -> Result<(), String> + 'static,
    said: impl Fn(&str, &dyn Display) + 'static,
) {
    let said: Said = Rc::new(said);
    let take: Taker = Rc::new(take);
    let servers: Rc<[SocketAddr]> = servers.into();
    let mut resolving: HashMap<String, JoinHandle<()>> = HashMap::new();
    loop {
        let now = wanted.borrow_and_update().clone();
        let gone: Vec<String> = resolving
            .keys()
            .filter(|name| !now.contains(*name))
            .cloned()
            .collect();
        for name in gone {
            if let Some(task) = resolving.remove(&name) {
                task.abort();
            }
            if let Err(error) = take(&name, None) {
                said(&name, &error);
            }
        }
        for name in now {
            if resolving.contains_key(&name) {
                continue;
            }
            let task = tokio::task::spawn_local(keep_resolved(
                Rc::clone(&take),
                Rc::clone(&servers),
                pace,
                name.clone(),
                Rc::clone(&said),
            ));
            resolving.insert(name, task);
        }
        if wanted.changed().await.is_err() {
            return;
        }
    }
}

/// Looks `name` up for as long as it is wanted, as the module says.
async fn keep_resolved(
    take: Taker,
    servers: Rc<[SocketAddr]>,
    pace: Pace,
    name: String,
    said: Said,
) {
    let ids = RandomState::new();
    let mut asked: u64 = 0;
    let mut taken_in = false;
    let mut last_told: Option<Failure> = None;
    loop {
        let looked = look_up(&servers, &name, pace.patience, || {
            asked += 1;
            // Two bytes of a keyed hash: an id no one else can foresee.
            u16::try_from(ids.hash_one(asked) & 0xffff).unwrap_or_default()
        })
        .await;
        let keep = match looked {
            Looked::Answered(addresses, ttl) => {
                taken_in = true;
                last_told = None;
                if let Err(error) = take(&name, Some(addresses)) {
                    said(&name, &error);
                }
                ttl
            }
            Looked::Failed(failure) => {
                if !taken_in {
                    taken_in = true;
                    if let Err(error) = take(&name, Some(Vec::new())) {
                        said(&name, &error);
                    }
                }
                if last_told.as_ref() != Some(&failure) {
                    said(&name, &failure);
                    last_told = Some(failure);
                }
                pace.floor
            }
        };
        tokio::time::sleep(keep.max(pace.floor)).await;
    }
}

/// Looks `name` up, its A and then its AAAA records, of `servers` in turn, each given
/// `patience` to answer, each query's id drawn from `id`.
pub(crate) async fn look_up(
    servers: &[SocketAddr],
    name: &str,
    patience: Duration,
    mut id: impl FnMut() -> u16,
) -> Looked {
    if servers.is_empty() {
        return Looked::Failed(Failure::NoNameserver);
    }
    let mut addresses = Vec::new();
    // The least of the times to live of what was found, else of what said there is none.
    let mut found: Option<u32> = None;
    let mut none: Option<u32> = None;
    for kind in [Kind::A, Kind::Aaaa] {
        match ask(servers, name, kind, id(), patience).await {
            Ok(Answer::Addresses {
                addresses: these,
                ttl,
            }) => {
                addresses.extend(these);
                found = Some(found.map_or(ttl, |found| found.min(ttl)));
            }
            Ok(Answer::Nothing { ttl }) => {
                let ttl = ttl.unwrap_or(0);
                none = Some(none.map_or(ttl, |none| none.min(ttl)));
            }
            // `ask` gives back an answer of addresses or of none, or why there was none.
            Ok(Answer::Truncated | Answer::Failed(_)) => {}
            Err(why) => return Looked::Failed(Failure::Unanswered(why)),
        }
    }
    let seconds = found.or(none).unwrap_or(0);
    Looked::Answered(addresses, Duration::from_secs(u64::from(seconds)))
}

/// `name`'s records of `kind`, of the first of `servers` to answer: an answer of addresses
/// or of none, or why no server gave one, the last's failure.
async fn ask(
    servers: &[SocketAddr],
    name: &str,
    kind: Kind,
    id: u16,
    patience: Duration,
) -> Result<Answer, String> {
    let Ok(message) = query(id, name, kind) else {
        return Err(format!("`{name}` cannot be asked for"));
    };
    let mut why = String::new();
    for &server in servers {
        let exchange = Exchange {
            server,
            message: &message,
            name,
            kind,
            id,
            patience,
        };
        let answered = match exchange.over_udp().await {
            Ok(Answer::Truncated) => exchange.over_tcp().await,
            other => other,
        };
        match answered {
            Ok(answer @ (Answer::Addresses { .. } | Answer::Nothing { .. })) => return Ok(answer),
            Ok(Answer::Failed(code)) => why = format!("{server} failed, response code {code}"),
            Ok(Answer::Truncated) => why = format!("{server} cut its answer short over TCP"),
            Err(failed) => why = format!("{server}: {failed}"),
        }
    }
    Err(why)
}

/// One query of one nameserver.
struct Exchange<'a> {
    server: SocketAddr,
    message: &'a [u8],
    name: &'a str,
    kind: Kind,
    id: u16,
    patience: Duration,
}

impl Exchange<'_> {
    /// The answer the server gives over UDP, a datagram that is no answer to the query
    /// passed over, for its patience at most.
    async fn over_udp(&self) -> Result<Answer, String> {
        let any = if self.server.is_ipv4() {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0)
        } else {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 0)
        };
        let exchange = async {
            // A port of its own each time, the kernel's choice, and only the server's
            // datagrams.
            let socket = UdpSocket::bind(any)
                .await
                .map_err(|error| error.to_string())?;
            socket
                .connect(self.server)
                .await
                .map_err(|error| error.to_string())?;
            socket
                .send(self.message)
                .await
                .map_err(|error| error.to_string())?;
            let mut buffer = vec![0; usize::from(edgerush_dns::UDP_SIZE)];
            loop {
                let length = socket
                    .recv(&mut buffer)
                    .await
                    .map_err(|error| error.to_string())?;
                match read(&buffer[..length], self.id, self.name, self.kind) {
                    Err(Malformed::NotOurs) => {}
                    other => return other.map_err(|error| error.to_string()),
                }
            }
        };
        tokio::time::timeout(self.patience, exchange)
            .await
            .unwrap_or_else(|_| Err(format!("no answer within {:?}", self.patience)))
    }

    /// The answer the server gives over TCP, each way led by its length, for its patience
    /// at most.
    async fn over_tcp(&self) -> Result<Answer, String> {
        let exchange = async {
            let mut stream = TcpStream::connect(self.server)
                .await
                .map_err(|error| error.to_string())?;
            let length = u16::try_from(self.message.len()).map_err(|error| error.to_string())?;
            let mut framed = length.to_be_bytes().to_vec();
            framed.extend_from_slice(self.message);
            stream
                .write_all(&framed)
                .await
                .map_err(|error| error.to_string())?;
            let mut length = [0; 2];
            stream
                .read_exact(&mut length)
                .await
                .map_err(|error| error.to_string())?;
            let mut answer = vec![0; usize::from(u16::from_be_bytes(length))];
            stream
                .read_exact(&mut answer)
                .await
                .map_err(|error| error.to_string())?;
            read(&answer, self.id, self.name, self.kind).map_err(|error| error.to_string())
        };
        tokio::time::timeout(self.patience, exchange)
            .await
            .unwrap_or_else(|_| Err(format!("no answer over TCP within {:?}", self.patience)))
    }
}

#[cfg(test)]
pub(crate) mod tests;
