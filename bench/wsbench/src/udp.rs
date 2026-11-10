//! `wsbench udp-serve`, `udp` and `udp-churn`: UDP datagrams and their echoes, for a proxy's
//! UDP sessions (24 step 5).
//!
//! ```text
//! wsbench udp-serve ADDRESS [--threads N]
//! wsbench udp ADDRESS [--sessions N] [--seconds S] [--size BYTES] [--rate PER_SECOND]
//!                     [--threads N]
//! wsbench udp-churn ADDRESS --rate PER_SECOND [--seconds S] [--size BYTES] [--most N]
//!                           [--threads N]
//! ```
//!
//! `udp-serve` sends every datagram back to where it came from, unchanged. `udp` keeps
//! `sessions` client sockets busy, each one datagram and its echo at a time — a session
//! each at the proxy, kept for the whole run — or with `--rate` sends them on a schedule
//! spread over the sessions, each timed from when it was due. Each session's first
//! exchange is made before the clock starts, so what is timed is datagrams, not a
//! session's making. `udp-churn` makes a client socket for every datagram, at a rate: a
//! new session each, its first datagram and the echo timed, and the socket closed.
//!
//! A datagram carries its number in its first eight bytes, so that an echo that comes after
//! its datagram was given up on is not taken for the next one's. One that has no echo
//! within [`LOST`] is lost: counted as failed, and its session sends the next.
//!
//! `udp-churn` sends each datagram from a source of its own, never used twice in a run: a
//! port from 1024 to 32767 of 127.20.0.1, then of 127.20.0.2 and on (Linux answers all of
//! 127/8 on loopback). A source must be new, or the proxy would find the session an earlier
//! datagram made; and outside the kernel's ephemeral range, which the proxy's own sockets
//! to its backend take one port each from — a client drawing from it as well would be
//! refused a port once those run short, and measure itself. Its line says how many were
//! answered and lost in each second of the run, by when they were due, as `by_second`.
//!
//! A client prints one line of JSON, as the WebSocket clients do.

use super::{Tally, micros, waited};
use std::io;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::{Instant, timeout};

/// How long an echo may take before its datagram is lost.
pub(super) const LOST: Duration = Duration::from_secs(1);
/// The bytes of a datagram's number, which every datagram starts with.
pub(super) const NUMBER: usize = 8;
/// The largest datagram, either way.
const LARGEST: usize = 65_535;
/// How often a session's first datagram is sent before the session counts as failed.
const TRIES: u32 = 3;
/// The sources `udp-churn` draws from: these ports of each address in turn.
const FIRST_PORT: u16 = 1024;
const LAST_PORT: u16 = 32767;
const FIRST_SOURCE: Ipv4Addr = Ipv4Addr::new(127, 20, 0, 1);

/// The echo backend: `tasks` of them reading the one socket.
pub(super) async fn serve(address: SocketAddr, tasks: usize) -> io::Result<()> {
    let socket = Arc::new(UdpSocket::bind(address).await?);
    let serving: Vec<_> = (0..tasks.max(1))
        .map(|_| tokio::spawn(echoing(Arc::clone(&socket))))
        .collect();
    for task in serving {
        task.await.map_err(io::Error::other)??;
    }
    Ok(())
}

async fn echoing(socket: Arc<UdpSocket>) -> io::Result<()> {
    let mut buffer = vec![0; LARGEST];
    loop {
        let (length, from) = socket.recv_from(&mut buffer).await?;
        // A client gone before its echo is no reason to stop answering the others.
        let _sent = socket.send_to(&buffer[..length], from).await;
    }
}

/// A datagram of `size` bytes, numbered `number`.
pub(super) fn datagram(size: usize, number: u64) -> Vec<u8> {
    let mut datagram = vec![b'x'; size.max(NUMBER)];
    datagram[..NUMBER].copy_from_slice(&number.to_be_bytes());
    datagram
}

/// Sends `datagram` and waits for its echo, letting earlier datagrams' late echoes go by:
/// whether it came back whole within [`LOST`].
async fn exchange(socket: &UdpSocket, datagram: &[u8], buffer: &mut [u8]) -> bool {
    timeout(LOST, async {
        socket.send(datagram).await?;
        loop {
            let length = socket.recv(buffer).await?;
            if buffer[..length.min(NUMBER)] == datagram[..NUMBER] {
                return Ok::<_, io::Error>(buffer[..length] == *datagram);
            }
        }
    })
    .await
    .is_ok_and(|echoed| echoed.unwrap_or(false))
}

/// A socket of its own for `source`, connected to `target`, so that it takes only the
/// target's datagrams.
async fn connected(source: SocketAddr, target: SocketAddr) -> io::Result<UdpSocket> {
    let socket = UdpSocket::bind(source).await?;
    socket.connect(target).await?;
    Ok(socket)
}

/// The kernel's choice of port, of the target's kind of address.
fn any_port(target: SocketAddr) -> SocketAddr {
    match target {
        SocketAddr::V4(_) => (Ipv4Addr::LOCALHOST, 0).into(),
        SocketAddr::V6(_) => (std::net::Ipv6Addr::LOCALHOST, 0).into(),
    }
}

/// A session: its socket, once a first datagram of it has been echoed.
async fn opened(target: SocketAddr, size: usize) -> Option<UdpSocket> {
    let socket = connected(any_port(target), target).await.ok()?;
    let mut buffer = vec![0; LARGEST];
    for _ in 0..TRIES {
        if exchange(&socket, &datagram(size, 0), &mut buffer).await {
            return Some(socket);
        }
    }
    None
}

/// `sessions` kept busy for `seconds`, or sending `rate` datagrams a second among them. All
/// of them are made, a datagram echoed on each, before the clock starts.
pub(super) async fn echo(
    target: SocketAddr,
    sessions: u64,
    seconds: f64,
    size: usize,
    rate: Option<f64>,
) -> Tally {
    let opening: Vec<_> = (0..sessions)
        .map(|_| tokio::spawn(opened(target, size)))
        .collect();
    let mut sockets = Vec::with_capacity(opening.len());
    for session in opening {
        sockets.push(session.await.ok().flatten());
    }
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(seconds);
    let every = rate.map(|rate| Duration::from_secs_f64(sessions as f64 / rate));
    let running: Vec<_> = sockets
        .into_iter()
        .zip(0..sessions)
        .map(|(socket, index)| {
            // The first of each session's datagrams spread over the first interval.
            let first = every.map(|every| start + every.mul_f64(index as f64 / sessions as f64));
            tokio::spawn(busy(socket, size, deadline, first.zip(every)))
        })
        .collect();
    let mut tally = Tally::default();
    for session in running {
        tally.add(session.await.unwrap_or_else(|_| Tally {
            failed: 1,
            ..Tally::default()
        }));
    }
    tally
}

/// One session of `echo`: a datagram at a time until `deadline`, when each is due if
/// `schedule` says. A lost one is counted and the next sent.
async fn busy(
    socket: Option<UdpSocket>,
    size: usize,
    deadline: Instant,
    schedule: Option<(Instant, Duration)>,
) -> Tally {
    let mut tally = Tally::default();
    let Some(socket) = socket else {
        tally.failed += 1;
        return tally;
    };
    let mut buffer = vec![0; LARGEST];
    let mut due = schedule.map(|(first, _)| first);
    for number in 1.. {
        let sent = match (&mut due, schedule) {
            (Some(due), Some((_, every))) => {
                if *due >= deadline {
                    break;
                }
                let at = waited(*due).await;
                *due += every;
                at
            }
            _ => {
                let now = Instant::now();
                if now >= deadline {
                    break;
                }
                now
            }
        };
        if exchange(&socket, &datagram(size, number), &mut buffer).await {
            tally.took.push(micros(sent.elapsed()));
        } else {
            tally.failed += 1;
        }
    }
    tally
}

/// The source of `udp-churn`'s datagram `index`: none is another's, and none is in the
/// kernel's ephemeral range. `None` once 127/8 has no more.
pub(super) fn source(index: u64) -> Option<SocketAddr> {
    let ports = u64::from(LAST_PORT - FIRST_PORT) + 1;
    let address = u64::from(u32::from(FIRST_SOURCE)).checked_add(index / ports)?;
    let address = Ipv4Addr::from(u32::try_from(address).ok()?);
    // Below 127.255.255.255, loopback's broadcast address.
    if !address.is_loopback() || address.octets()[1..] == [255, 255, 255] {
        return None;
    }
    let port = FIRST_PORT + u16::try_from(index % ports).ok()?;
    Some(SocketAddr::new(IpAddr::V4(address), port))
}

/// `rate` new sessions a second for `seconds`, each a datagram from a source of its own and
/// its echo; no more than `most` waiting at a time, and one that would be more is failed.
pub(super) async fn churn(
    target: SocketAddr,
    rate: f64,
    seconds: f64,
    size: usize,
    most: usize,
) -> Tally {
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(seconds);
    let every = Duration::from_secs_f64(1.0 / rate);
    let waiting = Arc::new(AtomicUsize::new(0));
    let mut running = Vec::new();
    let mut tally = Tally {
        by_second: vec![[0, 0]; seconds.ceil() as usize],
        ..Tally::default()
    };
    let mut due = start;
    let mut index = 0;
    while due < deadline {
        let second = (due - start).as_secs() as usize;
        let started = waited(due).await;
        match source(index) {
            Some(source) if waiting.load(Ordering::Relaxed) < most => {
                waiting.fetch_add(1, Ordering::Relaxed);
                let waiting = Arc::clone(&waiting);
                running.push((
                    second,
                    tokio::spawn(async move {
                        let took = once(source, target, index, size, started).await;
                        waiting.fetch_sub(1, Ordering::Relaxed);
                        took
                    }),
                ));
            }
            _ => tally.lost_in(second),
        }
        due += every;
        index += 1;
    }
    for (second, session) in running {
        match session.await {
            Ok(Some(took)) => {
                tally.took.push(took);
                tally.by_second[second][0] += 1;
            }
            _ => tally.lost_in(second),
        }
    }
    tally
}

/// One datagram of `udp-churn` from `source`, timed from `started`: how long its echo took.
async fn once(
    source: SocketAddr,
    target: SocketAddr,
    number: u64,
    size: usize,
    started: Instant,
) -> Option<u32> {
    let socket = connected(source, target).await.ok()?;
    let mut buffer = vec![0; LARGEST];
    exchange(&socket, &datagram(size, number), &mut buffer)
        .await
        .then(|| micros(started.elapsed()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn runtime() -> tokio::runtime::Runtime {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    }

    #[test]
    fn a_datagram_starts_with_its_number() {
        let datagram = datagram(12, 0x0102);
        assert_eq!(datagram, b"\0\0\0\0\0\0\x01\x02xxxx");
        assert_eq!(super::datagram(3, 7).len(), NUMBER);
    }

    /// Every source is new and outside Linux's ephemeral range (32768 on), and they run
    /// from one loopback address to the next.
    #[test]
    fn sources_are_each_new_and_below_the_ephemeral_range() {
        let ports = u64::from(LAST_PORT - FIRST_PORT) + 1;
        assert_eq!(source(0), Some("127.20.0.1:1024".parse().unwrap()));
        assert_eq!(source(ports - 1), Some("127.20.0.1:32767".parse().unwrap()));
        assert_eq!(source(ports), Some("127.20.0.2:1024".parse().unwrap()));
        let mut seen = std::collections::HashSet::new();
        for index in 0..3 * ports {
            let source = source(index).unwrap();
            assert!(source.port() < 32768, "{source}");
            assert!(seen.insert(source), "{source} twice");
        }
        // 127.20.0.1 to 127.255.255.254, and no further.
        let addresses = u64::from(u32::from(Ipv4Addr::new(127, 255, 255, 254)))
            - u64::from(u32::from(FIRST_SOURCE))
            + 1;
        assert_eq!(
            source(addresses * ports - 1),
            Some("127.255.255.254:32767".parse().unwrap())
        );
        assert_eq!(source(addresses * ports), None);
    }

    /// An echo that comes after its datagram was given up on is let go by, and the next
    /// datagram's own echo is the one taken.
    #[test]
    fn a_late_echo_is_not_the_next_datagrams() {
        runtime().block_on(async {
            let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let client = connected(
                any_port(backend.local_addr().unwrap()),
                backend.local_addr().unwrap(),
            )
            .await
            .unwrap();
            let mut buffer = vec![0; LARGEST];
            let late = datagram(16, 1);
            let next = datagram(16, 2);
            let answering = tokio::spawn(async move {
                let mut buffer = vec![0; LARGEST];
                let (_, from) = backend.recv_from(&mut buffer).await.unwrap();
                // The first datagram's echo, and the second's, sent once the second came.
                let (length, _) = backend.recv_from(&mut buffer).await.unwrap();
                backend.send_to(&datagram(16, 1), from).await.unwrap();
                backend.send_to(&buffer[..length], from).await.unwrap();
            });
            assert!(!exchange(&client, &late, &mut buffer).await);
            assert!(exchange(&client, &next, &mut buffer).await);
            answering.await.unwrap();
        });
    }

    /// An echo cut short, or of other bytes, is no echo.
    #[test]
    fn an_echo_that_differs_is_not_counted() {
        runtime().block_on(async {
            let backend = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let at = backend.local_addr().unwrap();
            let client = connected(any_port(at), at).await.unwrap();
            tokio::spawn(async move {
                let mut buffer = vec![0; LARGEST];
                let (length, from) = backend.recv_from(&mut buffer).await.unwrap();
                backend.send_to(&buffer[..length - 1], from).await.unwrap();
            });
            let mut buffer = vec![0; LARGEST];
            assert!(!exchange(&client, &datagram(16, 3), &mut buffer).await);
        });
    }

    /// The backend and the clients against each other, over loopback: kept sessions busy and
    /// paced, a new source for every churned datagram, each echoed, and churned datagrams
    /// lost where nothing echoes them.
    #[test]
    fn clients_are_echoed_by_the_backend() {
        runtime().block_on(async {
            let probe = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let address = probe.local_addr().unwrap();
            drop(probe);
            tokio::spawn(serve(address, 2));
            tokio::time::sleep(Duration::from_millis(50)).await;
            let busy = echo(address, 4, 0.3, 64, None).await;
            assert_eq!(busy.failed, 0);
            assert!(busy.took.len() > 4, "{}", busy.took.len());
            let paced = echo(address, 4, 0.5, 64, Some(100.0)).await;
            assert_eq!(paced.failed, 0);
            assert!(
                (40..=60).contains(&paced.took.len()),
                "{}",
                paced.took.len()
            );
            let churned = churn(address, 50.0, 1.5, 64, 16).await;
            assert_eq!(churned.failed, 0);
            assert!(
                (73..=77).contains(&churned.took.len()),
                "{}",
                churned.took.len()
            );
            assert_eq!(churned.by_second.len(), 2);
            assert_eq!(churned.by_second[0], [50, 0]);
            assert_eq!(churned.by_second[1][1], 0);
            // With nothing to echo them, churned datagrams are lost in the second they were
            // due. In the same test as the churn above, after it: every churn draws the same
            // sources, which two at once would each hold from the other.
            let silent = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let lost = churn(silent.local_addr().unwrap(), 20.0, 1.0, 64, 64).await;
            assert!(lost.took.is_empty());
            assert_eq!(lost.by_second, vec![[0, 20]]);
            assert_eq!(lost.failed, 20);
        });
    }
}
