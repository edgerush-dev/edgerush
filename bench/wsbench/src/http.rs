//! `wsbench http`: plain requests, closed loop, from many client addresses (12 item 3) — a
//! rate limit keyed by the client then has as many keys as there are addresses, as a data
//! plane's per-client limit has in front of many clients, each of them quiet.
//!
//! ```text
//! wsbench http URL [--host NAME] [--connections N] [--addresses N] [--seconds S]
//!                  [--threads N]
//! ```
//!
//! Connection `i` comes from address `i mod addresses`, of 127.10.0.1 on (Linux answers
//! all of 127/8 on loopback), and sends a GET, reads its answer whole, and sends the next,
//! until the time is up. URL is `http://` or `https://` with an IP address. It prints one
//! line of JSON: requests answered 200, 429 and otherwise, connections that failed, the
//! rate, and latency percentiles in milliseconds.

use super::{Options, STALL, connector, fill, head};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::time::{Instant, timeout};

/// Where the connections go, and how.
pub(super) struct Target {
    address: SocketAddr,
    request: Vec<u8>,
    host: String,
    tls: Option<boring::ssl::SslConnector>,
}

impl Target {
    pub(super) fn new(url: &str, options: &Options) -> Result<Self, String> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("https://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("http://") {
            (false, rest)
        } else {
            return Err(format!("not an http:// or https:// URL: {url}"));
        };
        let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
        let address = authority
            .parse()
            .map_err(|_| format!("not an IP address and port: {authority}"))?;
        let host: String = options.get("host", authority.to_owned())?;
        let path = if path.is_empty() { "/" } else { path };
        let tls = tls
            .then(connector)
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            address,
            request: format!("GET {path} HTTP/1.1\r\nhost: {host}\r\n\r\n").into_bytes(),
            host,
            tls,
        })
    }

    /// A connection from `from`.
    async fn open(&self, from: IpAddr) -> io::Result<Box<dyn super::Io>> {
        let socket = TcpSocket::new_v4()?;
        socket.bind(SocketAddr::new(from, 0))?;
        let stream = socket.connect(self.address).await?;
        stream.set_nodelay(true)?;
        Ok(match &self.tls {
            Some(connector) => {
                let config = connector.configure().map_err(io::Error::other)?;
                let stream = tokio_boring::connect(config, &self.host, stream)
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?;
                Box::new(stream)
            }
            None => Box::new(stream),
        })
    }
}

/// The `addresses` client addresses' `index`th: 127.10.0.1 on, 250 to a third octet.
fn address(index: u64) -> IpAddr {
    let index = u32::try_from(index).unwrap_or(u32::MAX);
    IpAddr::V4(Ipv4Addr::new(
        127,
        10 + (index / 62_500) as u8,
        ((index / 250) % 250) as u8,
        (index % 250 + 1) as u8,
    ))
}

/// What the connections had answered.
#[derive(Default)]
pub(super) struct Tally {
    took: Vec<u32>,
    ok: u64,
    refused: u64,
    other: u64,
    failed: u64,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.took.extend(other.took);
        self.ok += other.ok;
        self.refused += other.refused;
        self.other += other.other;
        self.failed += other.failed;
    }

    pub(super) fn report(mut self, seconds: f64) -> String {
        self.took.sort_unstable();
        let at = |share: f64| {
            let last = self.took.len().checked_sub(1)?;
            let index = ((share * self.took.len() as f64) as usize).min(last);
            Some(f64::from(self.took[index]) / 1000.0)
        };
        let json =
            |value: Option<f64>| value.map_or("null".to_owned(), |value| format!("{value:.3}"));
        format!(
            "{{\"wsbench\": \"http\", \"ok\": {}, \"refused\": {}, \"other\": {}, \
             \"failed\": {}, \"seconds\": {seconds}, \"rate\": {:.1}, \"p50\": {}, \
             \"p99\": {}, \"p99.9\": {}}}",
            self.ok,
            self.refused,
            self.other,
            self.failed,
            self.took.len() as f64 / seconds,
            json(at(0.5)),
            json(at(0.99)),
            json(at(0.999)),
        )
    }
}

/// `connections` from `addresses` client addresses, kept busy for `seconds`; all of them
/// open before the clock starts.
pub(super) async fn run(
    target: Arc<Target>,
    connections: u64,
    addresses: u64,
    seconds: f64,
) -> Tally {
    let addresses = addresses.max(1);
    let opening: Vec<_> = (0..connections)
        .map(|index| {
            let target = Arc::clone(&target);
            tokio::spawn(async move {
                timeout(STALL, target.open(address(index % addresses)))
                    .await
                    .ok()?
                    .ok()
            })
        })
        .collect();
    let mut opened = Vec::with_capacity(opening.len());
    for connection in opening {
        opened.push(connection.await.ok().flatten());
    }
    let deadline = Instant::now() + Duration::from_secs_f64(seconds);
    let running: Vec<_> = opened
        .into_iter()
        .map(|connection| tokio::spawn(busy(Arc::clone(&target), connection, deadline)))
        .collect();
    let mut tally = Tally::default();
    for connection in running {
        tally.add(connection.await.unwrap_or_else(|_| Tally {
            failed: 1,
            ..Tally::default()
        }));
    }
    tally
}

/// One connection: a request at a time until `deadline`.
async fn busy(
    target: Arc<Target>,
    connection: Option<Box<dyn super::Io>>,
    deadline: Instant,
) -> Tally {
    let mut tally = Tally::default();
    let Some(mut io) = connection else {
        tally.failed += 1;
        return tally;
    };
    let mut buffer = Vec::with_capacity(16 * 1024);
    while Instant::now() < deadline {
        let sent = Instant::now();
        let answered = timeout(STALL, async {
            io.write_all(&target.request).await?;
            let end = head(&mut io, &mut buffer).await?;
            let status = buffer.get(9..12).map(<[u8]>::to_vec).unwrap_or_default();
            let length: usize = super::field(&buffer[..end], b"content-length")
                .and_then(|length| std::str::from_utf8(length).ok()?.parse().ok())
                .ok_or_else(|| io::Error::other("no content-length"))?;
            while buffer.len() < end + length {
                fill(&mut io, &mut buffer).await?;
            }
            buffer.drain(..end + length);
            Ok::<_, io::Error>(status)
        })
        .await;
        match answered {
            Ok(Ok(status)) => {
                tally.took.push(super::micros(sent.elapsed()));
                match &status[..] {
                    b"200" => tally.ok += 1,
                    b"429" => tally.refused += 1,
                    _ => tally.other += 1,
                }
            }
            _ => {
                tally.failed += 1;
                return tally;
            }
        }
    }
    tally
}
