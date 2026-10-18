//! `wsbench http`: plain requests, closed loop, from many client addresses (12 item 3) — a
//! rate limit keyed by the client then has as many keys as there are addresses, as a data
//! plane's per-client limit has in front of many clients, each of them quiet.
//!
//! ```text
//! wsbench http URL [--host NAME] [--connections N] [--addresses N] [--seconds S]
//!                  [--resident N] [--threads N]
//! ```
//!
//! Connection `i` comes from address `i mod addresses`, of 127.10.0.1 on (Linux answers
//! all of 127/8 on loopback), and sends a GET, reads its answer whole, and sends the next,
//! until the time is up. URL is `http://` or `https://` with an IP address. With
//! `--resident`, that many more addresses, after the run's own, are met first, once each:
//! a connection, one GET answered, closed, a few hundred at a time — keys a limit then
//! holds, quiet, beside the run's busy ones, without a connection kept open for each. It
//! prints one line of JSON: requests answered 200, 429 and otherwise, connections that
//! failed, the rate, latency percentiles in milliseconds, and the resident addresses met
//! and missed.

use super::{Options, STALL, connector, fill, head};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
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
    /// Resident addresses answered, and those that were not.
    met: u64,
    missed: u64,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.took.extend(other.took);
        self.ok += other.ok;
        self.refused += other.refused;
        self.other += other.other;
        self.failed += other.failed;
        self.met += other.met;
        self.missed += other.missed;
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
             \"p99\": {}, \"p99.9\": {}, \"resident_met\": {}, \
             \"resident_missed\": {}}}",
            self.ok,
            self.refused,
            self.other,
            self.failed,
            self.took.len() as f64 / seconds,
            json(at(0.5)),
            json(at(0.99)),
            json(at(0.999)),
            self.met,
            self.missed,
        )
    }
}

/// Resident addresses met at once.
const MEETING: u64 = 256;

/// `connections` from `addresses` client addresses, kept busy for `seconds`; all of them
/// open before the clock starts, and `resident` more addresses met once each before that.
pub(super) async fn run(
    target: Arc<Target>,
    connections: u64,
    addresses: u64,
    seconds: f64,
    resident: u64,
) -> Tally {
    let addresses = addresses.max(1);
    let met = meet(Arc::clone(&target), addresses, resident).await;
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
    let mut tally = Tally {
        met,
        missed: resident - met,
        ..Tally::default()
    };
    for connection in running {
        tally.add(connection.await.unwrap_or_else(|_| Tally {
            failed: 1,
            ..Tally::default()
        }));
    }
    tally
}

/// Addresses `from` to `from + count`, each met once: a connection, one GET answered, and
/// closed; `MEETING` at a time. How many were answered.
async fn meet(target: Arc<Target>, from: u64, count: u64) -> u64 {
    let next = Arc::new(AtomicU64::new(from));
    let end = from.saturating_add(count);
    let meeting: Vec<_> = (0..MEETING.min(count))
        .map(|_| {
            let (target, next) = (Arc::clone(&target), Arc::clone(&next));
            tokio::spawn(async move {
                let mut met = 0;
                let mut buffer = Vec::with_capacity(4 * 1024);
                loop {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    if index >= end {
                        return met;
                    }
                    let answered = timeout(STALL, async {
                        let mut io = target.open(address(index)).await?;
                        buffer.clear();
                        exchange(&target, &mut io, &mut buffer).await?;
                        io.shutdown().await
                    })
                    .await;
                    met += u64::from(matches!(answered, Ok(Ok(()))));
                }
            })
        })
        .collect();
    let mut met = 0;
    for task in meeting {
        met += task.await.unwrap_or(0);
    }
    met.min(count)
}

/// One request on `io` and its answer read whole: its status.
async fn exchange(
    target: &Target,
    io: &mut Box<dyn super::Io>,
    buffer: &mut Vec<u8>,
) -> io::Result<Vec<u8>> {
    io.write_all(&target.request).await?;
    let end = head(io, buffer).await?;
    let status = buffer.get(9..12).map(<[u8]>::to_vec).unwrap_or_default();
    let length: usize = super::field(&buffer[..end], b"content-length")
        .and_then(|length| std::str::from_utf8(length).ok()?.parse().ok())
        .ok_or_else(|| io::Error::other("no content-length"))?;
    while buffer.len() < end + length {
        fill(io, buffer).await?;
    }
    buffer.drain(..end + length);
    Ok(status)
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
        let answered = timeout(STALL, exchange(&target, &mut io, &mut buffer)).await;
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// The resident addresses are met once each, after the run's own, and counted; the run's
    /// connections then keep only their own addresses busy.
    #[test]
    fn resident_addresses_are_met_once_each() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let seen: Arc<Mutex<HashMap<IpAddr, u64>>> = Arc::default();
            let counting = Arc::clone(&seen);
            tokio::spawn(async move {
                while let Ok((mut stream, peer)) = listener.accept().await {
                    let counting = Arc::clone(&counting);
                    tokio::spawn(async move {
                        let mut buffer = [0; 1024];
                        while let Ok(read) = stream.read(&mut buffer).await {
                            if read == 0 {
                                return;
                            }
                            *counting.lock().unwrap().entry(peer.ip()).or_default() += 1;
                            let answer = b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n";
                            if stream.write_all(answer).await.is_err() {
                                return;
                            }
                        }
                    });
                }
            });
            let target = Arc::new(Target {
                address,
                request: b"GET / HTTP/1.1\r\nhost: bench\r\n\r\n".to_vec(),
                host: "bench".to_owned(),
                tls: None,
            });
            let tally = run(target, 2, 2, 0.2, 50).await;
            assert_eq!((tally.met, tally.missed, tally.failed), (50, 0, 0));
            assert!(tally.ok > 2, "{}", tally.ok);
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 52);
            for index in 2..52 {
                assert_eq!(seen.get(&super::address(index)), Some(&1), "{index}");
            }
        });
    }
}
