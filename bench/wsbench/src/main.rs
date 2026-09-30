//! WebSocket load for EdgeRush's macro benchmark (bench/README.md), over HTTP/1.1 upgrades:
//!
//! ```text
//! wsbench serve ADDRESS [--threads N]
//! wsbench echo URL [--host NAME] [--connections N] [--seconds S] [--size BYTES]
//!                  [--rate MESSAGES_PER_SECOND] [--threads N]
//! wsbench churn URL --rate CONNECTIONS_PER_SECOND [--host NAME] [--seconds S]
//!                   [--size BYTES] [--most N] [--threads N]
//! ```
//!
//! `serve` answers a handshake with its 101 and then sends every frame back as it came,
//! unmasked, a Close with a Close and its end of the connection; a request that is no
//! handshake is answered 200, so that a proxy's readiness can be asked for. `echo` keeps
//! its connections busy, a message sent and its echo read before the next; with `--rate` it
//! sends them on a schedule instead, spread over the connections, and times each from when
//! it was due, so that a proxy falling behind shows as latency rather than as fewer
//! messages. `churn` opens a connection for each message at a rate: handshake, message and
//! its echo (what is timed), then a Close each way. URL is `ws://` or `wss://` with an IP
//! address; `--host` is the name asked for, and the name TLS is asked for by.
//!
//! A client prints one line of JSON: messages echoed, those that failed, the seconds they
//! were sent in, their rate, and latency percentiles in milliseconds.

#![forbid(unsafe_code)]

mod frames;

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{Instant, sleep_until, timeout};

/// How long one step of a client may take before its connection counts as failed.
const STALL: Duration = Duration::from_secs(5);
/// The most a request's head may be, either way.
const HEAD: usize = 64 * 1024;

trait Io: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> Io for T {}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match run(&args) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("wsbench: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(args: &[String]) -> Result<(), String> {
    let (command, rest) = args
        .split_first()
        .ok_or("no command: serve, echo or churn")?;
    let (place, flags) = rest.split_first().ok_or("no address or URL")?;
    let options = Options::read(flags)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(options.get("threads", 2)?)
        .enable_all()
        .build()
        .map_err(|error| error.to_string())?;
    match command.as_str() {
        "serve" => {
            let address: SocketAddr = place
                .parse()
                .map_err(|_| format!("not an address: {place}"))?;
            runtime
                .block_on(serve(address))
                .map_err(|error| error.to_string())
        }
        "echo" | "churn" => {
            let target = Arc::new(Target::new(place, &options)?);
            let seconds: f64 = options.get("seconds", 10.0)?;
            let size = options.get("size", 64)?;
            let tally = if command == "echo" {
                let connections = options.get("connections", 64)?;
                let rate = options.optional("rate")?;
                runtime.block_on(echo(target, connections, seconds, size, rate))
            } else {
                let rate: f64 = options.optional("rate")?.ok_or("churn needs --rate")?;
                let most = options.get("most", 1024)?;
                runtime.block_on(churn(target, rate, seconds, size, most))
            };
            println!("{}", tally.report(command, seconds));
            Ok(())
        }
        other => Err(format!("no command {other}")),
    }
}

/// `--name value` pairs.
struct Options(HashMap<String, String>);

impl Options {
    fn read(flags: &[String]) -> Result<Self, String> {
        let mut read = HashMap::new();
        for pair in flags.chunks(2) {
            let [name, value] = pair else {
                return Err(format!("no value for {}", pair[0]));
            };
            let name = name
                .strip_prefix("--")
                .ok_or(format!("not a flag: {name}"))?;
            read.insert(name.to_owned(), value.clone());
        }
        Ok(Self(read))
    }

    fn optional<T: FromStr>(&self, name: &str) -> Result<Option<T>, String> {
        self.0
            .get(name)
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| format!("--{name}: cannot read {value}"))
            })
            .transpose()
    }

    fn get<T: FromStr>(&self, name: &str, default: T) -> Result<T, String> {
        Ok(self.optional(name)?.unwrap_or(default))
    }
}

/// Where a client's connections go, and how.
struct Target {
    address: SocketAddr,
    host: String,
    path: String,
    tls: Option<boring::ssl::SslConnector>,
}

impl Target {
    fn new(url: &str, options: &Options) -> Result<Self, String> {
        let (tls, rest) = if let Some(rest) = url.strip_prefix("wss://") {
            (true, rest)
        } else if let Some(rest) = url.strip_prefix("ws://") {
            (false, rest)
        } else {
            return Err(format!("not a ws:// or wss:// URL: {url}"));
        };
        let (authority, path) = rest.split_at(rest.find('/').unwrap_or(rest.len()));
        let address = authority
            .parse()
            .map_err(|_| format!("not an IP address and port: {authority}"))?;
        let tls = tls
            .then(connector)
            .transpose()
            .map_err(|error| error.to_string())?;
        Ok(Self {
            address,
            host: options.get("host", authority.to_owned())?,
            path: if path.is_empty() {
                "/".to_owned()
            } else {
                path.to_owned()
            },
            tls,
        })
    }

    /// A connection that has made its handshake, and what came after the 101's head.
    async fn open(&self, seed: u64) -> io::Result<(Box<dyn Io>, Vec<u8>)> {
        let stream = TcpStream::connect(self.address).await?;
        stream.set_nodelay(true)?;
        let mut io: Box<dyn Io> = match &self.tls {
            Some(connector) => {
                let config = connector.configure().map_err(io::Error::other)?;
                let stream = tokio_boring::connect(config, &self.host, stream)
                    .await
                    .map_err(|error| io::Error::other(error.to_string()))?;
                Box::new(stream)
            }
            None => Box::new(stream),
        };
        let key = frames::key(seed);
        let mut request = format!(
            "GET {} HTTP/1.1\r\nhost: {}\r\nupgrade: websocket\r\nconnection: upgrade\r\n\
             sec-websocket-version: 13\r\nsec-websocket-key: ",
            self.path, self.host
        )
        .into_bytes();
        request.extend_from_slice(&key);
        request.extend_from_slice(b"\r\n\r\n");
        io.write_all(&request).await?;
        let mut buffer = Vec::with_capacity(4096);
        let end = head(&mut io, &mut buffer).await?;
        let answer = &buffer[..end];
        if !answer.starts_with(b"HTTP/1.1 101 ") {
            let line = answer
                .split(|&byte| byte == b'\r')
                .next()
                .unwrap_or_default();
            return Err(io::Error::other(format!(
                "not switched: {}",
                String::from_utf8_lossy(line)
            )));
        }
        if field(answer, b"sec-websocket-accept") != Some(&frames::accept(&key)[..]) {
            return Err(io::Error::other("switched with the wrong Accept"));
        }
        buffer.drain(..end);
        Ok((io, buffer))
    }
}

/// A TLS client that takes any certificate and asks for HTTP/1.1, and resumes no session:
/// every connection is a full handshake.
fn connector() -> Result<boring::ssl::SslConnector, boring::error::ErrorStack> {
    let mut builder = boring::ssl::SslConnector::builder(boring::ssl::SslMethod::tls())?;
    builder.set_verify(boring::ssl::SslVerifyMode::NONE);
    builder.set_alpn_protos(b"\x08http/1.1")?;
    Ok(builder.build())
}

/// Reads more of a connection into `buffer`; its end is an error.
async fn fill(io: &mut (impl AsyncRead + Unpin + ?Sized), buffer: &mut Vec<u8>) -> io::Result<()> {
    buffer.reserve(16 * 1024);
    if io.read_buf(buffer).await? == 0 {
        return Err(io::ErrorKind::UnexpectedEof.into());
    }
    Ok(())
}

/// Reads until `buffer` holds a whole head, and says where it ends.
async fn head(
    io: &mut (impl AsyncRead + Unpin + ?Sized),
    buffer: &mut Vec<u8>,
) -> io::Result<usize> {
    loop {
        if let Some(at) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            return Ok(at + 4);
        }
        if buffer.len() > HEAD {
            return Err(io::Error::other("a head too large"));
        }
        fill(io, buffer).await?;
    }
}

/// The value of the field `name` in `head`, the first if there are more.
fn field<'a>(head: &'a [u8], name: &[u8]) -> Option<&'a [u8]> {
    head.split(|&byte| byte == b'\n').skip(1).find_map(|line| {
        let line = line.strip_suffix(b"\r").unwrap_or(line);
        let colon = line.iter().position(|&byte| byte == b':')?;
        line[..colon]
            .eq_ignore_ascii_case(name)
            .then(|| line[colon + 1..].trim_ascii())
    })
}

/// Reads until `buffer` starts with a whole frame, and returns its header.
async fn frame(
    io: &mut (impl AsyncRead + Unpin + ?Sized),
    buffer: &mut Vec<u8>,
) -> io::Result<frames::Header> {
    loop {
        if let Some(header) = frames::Header::read(buffer)
            && buffer.len() >= header.whole()
        {
            return Ok(header);
        }
        fill(io, buffer).await?;
    }
}

/// The echo backend.
async fn serve(address: SocketAddr) -> io::Result<()> {
    let listener = TcpListener::bind(address).await?;
    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let _ended = answer(stream).await;
        });
    }
}

/// One connection to the backend: requests answered 200 until one is a handshake, and then
/// its frames echoed.
async fn answer(mut stream: TcpStream) -> io::Result<()> {
    stream.set_nodelay(true)?;
    let mut buffer = Vec::with_capacity(16 * 1024);
    loop {
        let end = head(&mut stream, &mut buffer).await?;
        let key = field(&buffer[..end], b"sec-websocket-key").map(<[u8]>::to_vec);
        buffer.drain(..end);
        let Some(key) = key else {
            stream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok")
                .await?;
            continue;
        };
        let mut switched = b"HTTP/1.1 101 Switching Protocols\r\nupgrade: websocket\r\n\
                             connection: upgrade\r\nsec-websocket-accept: "
            .to_vec();
        switched.extend_from_slice(&frames::accept(&key));
        switched.extend_from_slice(b"\r\n\r\n");
        stream.write_all(&switched).await?;
        return echoing(stream, buffer).await;
    }
}

/// Every whole frame sent back unmasked, those of one read in one write; a Close answered
/// with a Close and the end of the connection.
async fn echoing(mut stream: TcpStream, mut buffer: Vec<u8>) -> io::Result<()> {
    let mut out = Vec::with_capacity(16 * 1024);
    let mut payload = Vec::new();
    loop {
        let mut used = 0;
        let mut closing = false;
        while let Some(header) = frames::Header::read(&buffer[used..]) {
            if buffer.len() - used < header.whole() {
                break;
            }
            let masked = &buffer[used + header.length..used + header.whole()];
            payload.clear();
            payload.extend(
                masked
                    .iter()
                    .enumerate()
                    .map(|(at, byte)| header.mask.map_or(*byte, |mask| byte ^ mask[at % 4])),
            );
            frames::write(&mut out, header.opcode, &payload, None);
            used += header.whole();
            if header.opcode == frames::CLOSE {
                closing = true;
                break;
            }
        }
        buffer.drain(..used);
        if !out.is_empty() {
            stream.write_all(&out).await?;
            out.clear();
        }
        if closing {
            // The server ends the TCP connection first (RFC 6455 §7.1.1).
            return stream.shutdown().await;
        }
        match fill(&mut stream, &mut buffer).await {
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            other => other?,
        }
    }
}

/// What a client's connections did.
#[derive(Default)]
struct Tally {
    /// Each message's latency, in microseconds.
    took: Vec<u32>,
    failed: u64,
}

impl Tally {
    fn add(&mut self, other: Tally) {
        self.took.extend(other.took);
        self.failed += other.failed;
    }

    fn report(mut self, command: &str, seconds: f64) -> String {
        self.took.sort_unstable();
        let at = |share: f64| {
            let last = self.took.len().checked_sub(1)?;
            let index = ((share * self.took.len() as f64) as usize).min(last);
            Some(f64::from(self.took[index]) / 1000.0)
        };
        let json =
            |value: Option<f64>| value.map_or("null".to_owned(), |value| format!("{value:.3}"));
        format!(
            "{{\"wsbench\": \"{command}\", \"messages\": {}, \"failed\": {}, \"seconds\": {seconds}, \
             \"rate\": {:.1}, \"p50\": {}, \"p99\": {}, \"p99.9\": {}}}",
            self.took.len(),
            self.failed,
            self.took.len() as f64 / seconds,
            json(at(0.5)),
            json(at(0.99)),
            json(at(0.999)),
        )
    }
}

fn micros(took: Duration) -> u32 {
    u32::try_from(took.as_micros()).unwrap_or(u32::MAX)
}

/// A mask for connection `index`'s frames: the proxies read none of them.
fn mask(index: u64) -> [u8; 4] {
    (index as u32).wrapping_mul(0x9e37_79b9).to_be_bytes()
}

/// Waits for `due`, and says when what was due then is timed from: `due` itself if it has
/// passed already — what went before ran late, and a proxy that holds messages up shows as
/// their latency rather than as fewer of them — and otherwise when the wait ended, as the
/// timer wakes up to a millisecond after `due` and that is no proxy's.
async fn waited(due: Instant) -> Instant {
    if Instant::now() >= due {
        return due;
    }
    sleep_until(due).await;
    Instant::now()
}

/// `connections` kept busy for `seconds`, or sending `rate` messages a second among them.
async fn echo(
    target: Arc<Target>,
    connections: u64,
    seconds: f64,
    size: usize,
    rate: Option<f64>,
) -> Tally {
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(seconds);
    let every = rate.map(|rate| Duration::from_secs_f64(connections as f64 / rate));
    let running: Vec<_> = (0..connections)
        .map(|index| {
            let target = Arc::clone(&target);
            // The first of each connection's messages spread over the first interval.
            let first = every.map(|every| start + every.mul_f64(index as f64 / connections as f64));
            tokio::spawn(busy(target, index, size, deadline, first.zip(every)))
        })
        .collect();
    let mut tally = Tally::default();
    for connection in running {
        tally.add(connection.await.unwrap_or_else(|_| Tally {
            took: Vec::new(),
            failed: 1,
        }));
    }
    tally
}

/// One connection of `echo`: a message at a time until `deadline`, when each is due if
/// `schedule` says, and then a Close each way.
async fn busy(
    target: Arc<Target>,
    index: u64,
    size: usize,
    deadline: Instant,
    schedule: Option<(Instant, Duration)>,
) -> Tally {
    let mut tally = Tally::default();
    let Ok(Ok((mut io, mut buffer))) = timeout(STALL, target.open(index)).await else {
        tally.failed += 1;
        return tally;
    };
    let mask = mask(index);
    let mut message = Vec::new();
    frames::write(&mut message, frames::TEXT, &vec![b'x'; size], Some(mask));
    let mut due = schedule.map(|(first, _)| first);
    loop {
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
        let exchanged = timeout(STALL, async {
            io.write_all(&message).await?;
            let header = frame(&mut io, &mut buffer).await?;
            buffer.drain(..header.whole());
            Ok::<_, io::Error>(header)
        })
        .await;
        match exchanged {
            Ok(Ok(header)) if header.opcode == frames::TEXT && header.payload == size => {
                tally.took.push(micros(sent.elapsed()));
            }
            _ => {
                tally.failed += 1;
                return tally;
            }
        }
    }
    close(&mut io, &mut buffer, mask).await;
    tally
}

/// A Close sent, the peer's read, and the connection read to its end: the server ends it
/// first (RFC 6455 §7.1.1).
async fn close(io: &mut Box<dyn Io>, buffer: &mut Vec<u8>, mask: [u8; 4]) {
    let mut close = Vec::new();
    frames::write(
        &mut close,
        frames::CLOSE,
        &1000_u16.to_be_bytes(),
        Some(mask),
    );
    let _closed = timeout(STALL, async {
        io.write_all(&close).await?;
        loop {
            let header = frame(io, buffer).await?;
            buffer.drain(..header.whole());
            if header.opcode == frames::CLOSE {
                break;
            }
        }
        while io.read_buf(buffer).await? > 0 {
            buffer.clear();
        }
        Ok::<_, io::Error>(())
    })
    .await;
}

/// `rate` connections a second for `seconds`, each a handshake, a message and its echo, and
/// a Close each way; no more than `most` at a time, and one that would be more is failed.
async fn churn(target: Arc<Target>, rate: f64, seconds: f64, size: usize, most: usize) -> Tally {
    let start = Instant::now();
    let deadline = start + Duration::from_secs_f64(seconds);
    let every = Duration::from_secs_f64(1.0 / rate);
    let under_way = Arc::new(AtomicUsize::new(0));
    let mut running = Vec::new();
    let mut tally = Tally::default();
    let mut due = start;
    let mut index = 0;
    while due < deadline {
        let started = waited(due).await;
        if under_way.load(Ordering::Relaxed) >= most {
            tally.failed += 1;
        } else {
            under_way.fetch_add(1, Ordering::Relaxed);
            let target = Arc::clone(&target);
            let under_way = Arc::clone(&under_way);
            running.push(tokio::spawn(async move {
                let took = once(&target, index, size, started).await;
                under_way.fetch_sub(1, Ordering::Relaxed);
                took
            }));
        }
        due += every;
        index += 1;
    }
    for connection in running {
        match connection.await {
            Ok(Some(took)) => tally.took.push(took),
            _ => tally.failed += 1,
        }
    }
    tally
}

/// One connection of `churn`, timed from `started`: how long its handshake and echo took.
async fn once(target: &Target, index: u64, size: usize, started: Instant) -> Option<u32> {
    let (mut io, mut buffer) = timeout(STALL, target.open(index)).await.ok()?.ok()?;
    let mask = mask(index);
    let mut message = Vec::new();
    frames::write(&mut message, frames::TEXT, &vec![b'x'; size], Some(mask));
    let header = timeout(STALL, async {
        io.write_all(&message).await?;
        frame(&mut io, &mut buffer).await
    })
    .await
    .ok()?
    .ok()?;
    let took = micros(started.elapsed());
    buffer.drain(..header.whole());
    close(&mut io, &mut buffer, mask).await;
    (header.opcode == frames::TEXT && header.payload == size).then_some(took)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_field_is_found_by_its_name_in_any_case() {
        let head = b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
                     Sec-WebSocket-Accept:  abc= \r\n\r\n";
        assert_eq!(field(head, b"sec-websocket-accept"), Some(&b"abc="[..]));
        assert_eq!(field(head, b"sec-websocket-key"), None);
    }

    #[test]
    fn a_report_has_the_percentiles_in_milliseconds() {
        let tally = Tally {
            took: (1..=1000).rev().collect(),
            failed: 2,
        };
        let report = tally.report("echo", 2.0);
        assert!(report.contains("\"messages\": 1000"), "{report}");
        assert!(report.contains("\"failed\": 2"), "{report}");
        assert!(report.contains("\"rate\": 500.0"), "{report}");
        assert!(report.contains("\"p50\": 0.501"), "{report}");
        assert!(report.contains("\"p99.9\": 1.000"), "{report}");
        let empty = Tally::default().report("churn", 1.0);
        assert!(empty.contains("\"p50\": null"), "{empty}");
    }

    /// A message whose time has passed is timed from it; one waited for, from the wake,
    /// which the clock, read after the timer, puts after its time.
    #[test]
    fn a_passed_time_is_kept_and_a_wait_is_timed_from_its_end() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        runtime.block_on(async {
            let passed = Instant::now();
            tokio::time::sleep(Duration::from_millis(2)).await;
            assert_eq!(waited(passed).await, passed);
            let due = Instant::now() + Duration::from_millis(5);
            assert!(waited(due).await > due);
        });
    }

    /// The backend and both clients against each other, over loopback.
    #[test]
    fn clients_are_echoed_by_the_backend() {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(answer(stream));
                }
            });
            let target = Arc::new(Target {
                address,
                host: "bench.example.com".to_owned(),
                path: "/ws".to_owned(),
                tls: None,
            });
            let busy = echo(Arc::clone(&target), 4, 0.3, 300, None).await;
            assert_eq!(busy.failed, 0);
            assert!(busy.took.len() > 4, "{}", busy.took.len());
            let paced = echo(Arc::clone(&target), 4, 0.5, 64, Some(100.0)).await;
            assert_eq!(paced.failed, 0);
            assert!(
                (40..=60).contains(&paced.took.len()),
                "{}",
                paced.took.len()
            );
            let churned = churn(target, 50.0, 0.3, 64, 16).await;
            assert_eq!(churned.failed, 0);
            assert!(
                (13..=17).contains(&churned.took.len()),
                "{}",
                churned.took.len()
            );
        });
    }
}
