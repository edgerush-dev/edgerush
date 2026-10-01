//! `edgerush proxy`: a data plane run from a config file, without Kubernetes. A harness for
//! development and tests — in a cluster the config comes from the control plane.
//!
//! The file is the config model as it is, nothing added; what is about the process and not
//! about routing (where `/metrics` is served, how many workers there are, whose a new
//! connection is) is on the command line. Listeners are bound once, at the start. After
//! that the file is read again every [`POLL`] and a config that has changed takes over
//! without dropping a request, while one that cannot be run is told and changes nothing.
//!
//! Requests are served thread-per-core ([`crate::per_core`]), the model that was chosen
//! from the benchmark ([03 §2] in the docs): one data plane for the process, whose config
//! and counters every worker shares, and upstream connections that belong to a worker
//! alone. Scrapes are answered away from the workers, on a runtime of their own.
//!
//! Told to stop — Ctrl-C, or SIGTERM on Unix, which is how Kubernetes asks — the data
//! plane drains ([03 §10] in the docs): nothing new is accepted, what is under way is
//! finished, and the process exits once every connection has gone or [`DRAIN`] is up. A
//! second signal while draining exits at once.
//!
//! Workers past the first share the port of every listener (`SO_REUSEPORT`), which only
//! Unix has; a single worker has the port to itself and needs nothing of the kind, so
//! that is the shape the harness runs in on Windows.

use crate::balance::Loads;
use crate::bind::{Port, datagrams, listen};
use crate::config_file::{ConfigFile, Rejected};
use crate::per_core::{self, Accept};
use edgerush_config::Compiled;
use edgerush_proxy::{H1Limits, Proxy, ProxyError};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::runtime::Builder;

pub(crate) const USAGE: &str = "\
Usage: edgerush proxy --config <FILE> [OPTIONS]

Runs a data plane from a config file, without Kubernetes: a harness for development.
The file is read again every second, and a changed config takes over without dropping a
request. Listeners are bound once: one that is new or has moved takes a restart.
Ctrl-C or SIGTERM drains it: what is under way is finished, for up to 29 seconds; a
second one stops it at once.

Options:
      --config <FILE>      The config, in YAML
      --metrics <ADDRESS>  Serve /metrics there, as in 127.0.0.1:9090 [default: nowhere]
      --accept <BY>        Whose a new connection is. balanced: the worker that holds the
                           fewest; kernel: the one the kernel gave it to, by its hash of
                           the addresses [default: balanced]
      --workers <N>        How many threads serve requests. More than one shares every
                           listener's port, which needs SO_REUSEPORT (Unix)
                           [default: one for every CPU]
      --idle-per-destination <N>
                           How many idle connections a worker keeps to one destination.
                           For benchmarks at stated bounds; there is no configuration
                           for these [default: 8]
      --idle-total <N>     How many it keeps in all, the same way [default: 256]
      --accept-batch <N>   How many connections a worker accepts before its other work
                           goes first, the same way [default: 1]
  -h, --help               Print help
";

/// How often the file is looked at. It is a small file, read by the thread that has
/// nothing else to do.
const POLL: Duration = Duration::from_secs(1);

/// How long a draining process waits for its connections before it exits anyway: inside
/// Kubernetes' default 30 s grace, and past the 25 s a worker gives its connections, the
/// sweep that brings the drain to it and the flush of what it then closes.
const DRAIN: Duration = Duration::from_secs(29);

/// How often a draining process looks at what its workers still hold.
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// Runs `edgerush proxy` with the arguments after its name. Returns the exit status: once
/// the data plane has drained, or at once when there is nothing to run or no way to.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match parse(args) {
        Ok(Parsed::Help) => write!(stdout, "{USAGE}").map(|()| 0),
        Ok(Parsed::Run(options)) => match run(*options, stderr) {
            Ok(()) => Ok(0),
            Err(failure) => writeln!(stderr, "error: {failure}").map(|()| 1),
        },
        Err(error) => write!(stderr, "error: {error}\n\n{USAGE}").map(|()| crate::EXIT_USAGE),
    };
    written.unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    /// Boxed because it is much the larger of the two and a `Parsed` is passed by
    /// value; the bounds a worker runs under are most of its size.
    Run(Box<Options>),
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    config: PathBuf,
    metrics: Option<SocketAddr>,
    /// `None` is one for every CPU the process may use.
    workers: Option<NonZeroUsize>,
    accept: Accept,
    /// What a worker will not go beyond. There is no configuration for these; what a
    /// benchmark needs is a way to say which it held the worker to
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    limits: H1Limits,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'--config <FILE>' is required")]
    NoConfig,
    #[error("'{0}' is not an address to listen on, such as 127.0.0.1:9090")]
    Address(String),
    #[error("'{0}' is not a number of workers: 1 or more")]
    Workers(String),
    #[error("'{0}' is not a way to place connections: balanced or kernel")]
    Accept(String),
    #[error("'{1}' is not a number of idle connections for '{0}': 0 or more")]
    Idle(&'static str, String),
    #[error("'{0}' is not a number of connections to accept at once: 1 or more")]
    AcceptBatch(String),
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut config = None;
    let mut metrics: Option<SocketAddr> = None;
    let mut workers = None;
    let mut accept = None;
    let mut limits = H1Limits::default();
    let (mut per_destination, mut total, mut batch) = (false, false, false);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--config" => {
                let file = args.next().ok_or(UsageError::NoValue("--config"))?;
                if config.replace(PathBuf::from(file)).is_some() {
                    return Err(UsageError::Twice("--config"));
                }
            }
            "--metrics" => {
                let address = args.next().ok_or(UsageError::NoValue("--metrics"))?;
                let address = address.parse().map_err(|_| UsageError::Address(address))?;
                if metrics.replace(address).is_some() {
                    return Err(UsageError::Twice("--metrics"));
                }
            }
            "--workers" => {
                let count = args.next().ok_or(UsageError::NoValue("--workers"))?;
                let count = count.parse().map_err(|_| UsageError::Workers(count))?;
                if workers.replace(count).is_some() {
                    return Err(UsageError::Twice("--workers"));
                }
            }
            "--idle-per-destination" => {
                let many = args
                    .next()
                    .ok_or(UsageError::NoValue("--idle-per-destination"))?;
                limits.idle_per_destination = many
                    .parse()
                    .map_err(|_| UsageError::Idle("--idle-per-destination", many))?;
                if std::mem::replace(&mut per_destination, true) {
                    return Err(UsageError::Twice("--idle-per-destination"));
                }
            }
            "--idle-total" => {
                let many = args.next().ok_or(UsageError::NoValue("--idle-total"))?;
                limits.idle_total = many
                    .parse()
                    .map_err(|_| UsageError::Idle("--idle-total", many))?;
                if std::mem::replace(&mut total, true) {
                    return Err(UsageError::Twice("--idle-total"));
                }
            }
            "--accept-batch" => {
                let many = args.next().ok_or(UsageError::NoValue("--accept-batch"))?;
                limits.accept_batch = many
                    .parse()
                    .ok()
                    .filter(|&many: &usize| many > 0)
                    .ok_or(UsageError::AcceptBatch(many))?;
                if std::mem::replace(&mut batch, true) {
                    return Err(UsageError::Twice("--accept-batch"));
                }
            }
            "--accept" => {
                let by = args.next().ok_or(UsageError::NoValue("--accept"))?;
                let by = match by.as_str() {
                    "balanced" => Accept::Balanced,
                    "kernel" => Accept::Kernel,
                    _ => return Err(UsageError::Accept(by)),
                };
                if accept.replace(by).is_some() {
                    return Err(UsageError::Twice("--accept"));
                }
            }
            _ => return Err(UsageError::Unexpected(arg)),
        }
    }
    let config = config.ok_or(UsageError::NoConfig)?;
    Ok(Parsed::Run(Box::new(Options {
        config,
        metrics,
        workers,
        accept: accept.unwrap_or(Accept::Balanced),
        limits,
    })))
}

/// Why there is no data plane to run.
#[derive(Debug, thiserror::Error)]
enum Failure {
    #[error("config {} cannot be run:\n{rejected}", path.display())]
    Config { path: PathBuf, rejected: Rejected },
    #[error("config {} cannot be run:\n{error}", path.display())]
    Proxy { path: PathBuf, error: ProxyError },
    #[error("cannot listen on {address} for {what}: {error}")]
    Listen {
        what: String,
        address: SocketAddr,
        error: io::Error,
    },
    #[error("cannot start the runtime: {0}")]
    Runtime(io::Error),
}

fn run(options: Options, stderr: &mut impl Write) -> Result<(), Failure> {
    let Options {
        config: path,
        metrics,
        workers,
        accept,
        limits,
    } = options;
    let workers = workers
        .or_else(|| thread::available_parallelism().ok())
        .unwrap_or(NonZeroUsize::MIN);
    // Before a socket is opened, as every socket is an open file.
    let open_files = crate::limits::raise_open_files();
    let connections = crate::limits::connections_per_worker(open_files, workers);
    // One worker is alone on every listener's port and needs nothing of the kernel; it is
    // from the second on that they share one, which is what SO_REUSEPORT is for.
    let port = if workers == NonZeroUsize::MIN {
        Port::Own
    } else {
        Port::Shared
    };

    let (mut file, config) = match ConfigFile::open(path.clone()) {
        Ok(opened) => opened,
        Err(rejected) => return Err(Failure::Config { path, rejected }),
    };
    let mut bound: Vec<Bound> = config.listeners.iter().map(Bound::from).collect();
    let proxy = Proxy::new(config, workers)
        .map(Arc::new)
        .map_err(|error| Failure::Proxy { path, error })?;

    // Every socket is open before a request is served on any, so that a port that is
    // taken stops a harness that has done nothing yet.
    let mut sockets = Vec::with_capacity(workers.get());
    for _ in 0..workers.get() {
        let mut of_this_one = Vec::with_capacity(bound.len());
        for listener in &mut bound {
            let what = format!("listener \"{}\"", listener.name);
            let socket = open(what, listener.listens_on, port)?;
            // A port that the operating system chose for the first worker is the port of
            // those after it.
            listener.listens_on = socket.local_addr().unwrap_or(listener.listens_on);
            // HTTP/3 on the port TCP has: what `Alt-Svc` will tell clients (16 §5).
            let udp = if listener.http3 {
                let what = format!("listener \"{}\" (HTTP/3)", listener.name);
                let address = listener.listens_on;
                Some(datagrams(address, port).map_err(|error| Failure::Listen {
                    what,
                    address,
                    error,
                })?)
            } else {
                None
            };
            of_this_one.push((socket, udp));
        }
        sockets.push(of_this_one);
    }
    let metrics = metrics
        .map(|address| open("metrics".to_owned(), address, Port::Own))
        .transpose()?;

    for listener in &bound {
        let also = if listener.http3 { ", HTTP/3 too" } else { "" };
        say(
            stderr,
            format_args!(
                "listener \"{}\" is on {}{also}",
                listener.name, listener.listens_on
            ),
        );
    }
    // Listened for before a request is served, so that no signal finds the default
    // action, which is to die on the spot.
    let (stopping, stop) = mpsc::channel();
    stop_signals(stopping.clone()).map_err(Failure::Runtime)?;
    // Every worker runs on a thread of its own, which stays for as long as the process.
    let loads =
        per_core::start(&proxy, sockets, accept, limits, connections).map_err(Failure::Runtime)?;
    health_checks(Arc::clone(&proxy)).map_err(Failure::Runtime)?;
    if let Some(socket) = metrics {
        let address = socket.local_addr().map_err(Failure::Runtime)?;
        scrapes(Arc::clone(&proxy), socket).map_err(Failure::Runtime)?;
        say(stderr, format_args!("metrics are on {address}"));
    }
    let plural = if workers == NonZeroUsize::MIN {
        ""
    } else {
        "s"
    };
    say(
        stderr,
        format_args!("{workers} worker{plural}, thread-per-core"),
    );
    say(
        stderr,
        format_args!("{}", crate::limits::described(open_files, connections)),
    );

    // Until told to stop; `stopping` is still held here, so the channel cannot close.
    while let Err(RecvTimeoutError::Timeout) = stop.recv_timeout(POLL) {
        let Some(changed) = file.changed() else {
            continue;
        };
        let config = match changed {
            Ok(config) => config,
            Err(rejected) => {
                say(
                    stderr,
                    format_args!("config rejected, the one before it runs on:\n{rejected}"),
                );
                continue;
            }
        };
        let warnings = restart_needed(&bound, &config);
        // Published once, for every worker at once: none of them sees the old config after
        // another has seen the new one.
        match proxy.reload(config) {
            Ok(()) => say(stderr, format_args!("config reloaded")),
            Err(error) => {
                say(
                    stderr,
                    format_args!("config rejected, the one before it runs on:\n{error}"),
                );
                continue;
            }
        }
        for warning in warnings {
            say(stderr, format_args!("warning: {warning}"));
        }
    }
    drop(stopping);
    say(stderr, format_args!("draining"));
    proxy.drain();
    match drained(&loads, &stop, DRAIN) {
        Drained::Empty => say(stderr, format_args!("drained")),
        Drained::OutOfTime(held) => say(
            stderr,
            format_args!("out of time draining: {held} connections cut off"),
        ),
        Drained::Stopped(held) => say(
            stderr,
            format_args!("stopped while draining: {held} connections cut off"),
        ),
    }
    Ok(())
}

/// How a drain ended.
#[derive(Debug, PartialEq, Eq)]
enum Drained {
    /// Every connection went.
    Empty,
    /// Time was up with this many still held.
    OutOfTime(usize),
    /// Told to stop again, with this many still held.
    Stopped(usize),
}

/// Waits until the workers hold no connection, `within` is up, or `stop` says to stop
/// again.
fn drained(loads: &Loads, stop: &Receiver<()>, within: Duration) -> Drained {
    let until = Instant::now() + within;
    loop {
        let held: usize = loads.now().iter().sum();
        if held == 0 {
            return Drained::Empty;
        }
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return Drained::OutOfTime(held);
        }
        match stop.recv_timeout(DRAIN_POLL.min(left)) {
            Ok(()) => return Drained::Stopped(held),
            // With nobody left to say stop, the drain runs its course.
            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
        }
    }
}

/// Says on `stopping`, from a thread of its own, every time the process is told to stop:
/// Ctrl-C, and on Unix SIGTERM, which is how Kubernetes asks. Both are listened for
/// before this returns.
fn stop_signals(stopping: Sender<()>) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_io().build()?;
    let entered = runtime.enter();
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    drop(entered);
    // Ctrl-C is listened for from the first poll of what waits for it, on that thread.
    let (listening, listens) = mpsc::channel();
    thread::Builder::new()
        .name("signals".to_owned())
        .spawn(move || {
            runtime.block_on(async move {
                loop {
                    let mut interrupt = std::pin::pin!(tokio::signal::ctrl_c());
                    let told = std::future::poll_fn(|cx| {
                        #[cfg(unix)]
                        if terminate.poll_recv(cx).is_ready() {
                            return std::task::Poll::Ready(Ok(()));
                        }
                        let polled = interrupt.as_mut().poll(cx);
                        let _listening = listening.send(());
                        polled
                    })
                    .await;
                    // A signal that cannot be listened for is one that is not heard; the
                    // others still are, and so is being killed.
                    if told.is_err() || stopping.send(()).is_err() {
                        return;
                    }
                }
            });
        })?;
    let _listening = listens.recv();
    Ok(())
}

/// Answers scrapes on `socket`, on a small runtime and a thread of their own: what a
/// scraper asks for is added up over every worker's shard, and no worker's time goes on
/// it ([03 §2] in the docs). Our own server serves them, so the thread runs a `LocalSet`
/// as a worker's does.
fn scrapes(proxy: Arc<Proxy>, socket: std::net::TcpListener) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    // The socket is handed to the runtime that is entered, as a worker's are.
    let entered = runtime.enter();
    let socket = TcpListener::from_std(socket)?;
    drop(entered);
    thread::Builder::new()
        .name("metrics".to_owned())
        .spawn(move || {
            tokio::task::LocalSet::new().block_on(&runtime, proxy.serve_metrics(socket));
        })?;
    Ok(())
}

/// Probes the upstreams that ask for health checks, on a runtime and a thread of their
/// own: a worker saturated with requests must not starve the checks that decide where
/// its requests go ([03 §6] in the docs).
fn health_checks(proxy: Arc<Proxy>) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    thread::Builder::new()
        .name("health".to_owned())
        .spawn(move || {
            tokio::task::LocalSet::new().block_on(&runtime, proxy.check_health());
        })?;
    Ok(())
}

/// A listener that has a socket.
#[derive(Debug, PartialEq, Eq)]
struct Bound {
    name: String,
    /// The address the config gave it then: what a later config's is compared with.
    address: SocketAddr,
    /// The address its socket has, which is another when the config left the port to the
    /// operating system.
    listens_on: SocketAddr,
    /// It serves HTTP/3 as well, on the same port over UDP.
    http3: bool,
}

impl From<&edgerush_config::CompiledListener> for Bound {
    fn from(listener: &edgerush_config::CompiledListener) -> Self {
        Self {
            name: listener.name.clone(),
            address: listener.address,
            listens_on: listener.address,
            http3: listener.http3.is_some(),
        }
    }
}

fn open(what: String, address: SocketAddr, port: Port) -> Result<std::net::TcpListener, Failure> {
    listen(address, port).map_err(|error| Failure::Listen {
        what,
        address,
        error,
    })
}

/// What a config asks of the listeners that only a restart can give: sockets are opened
/// once, and a reload changes what is served on them, not where they are.
fn restart_needed(bound: &[Bound], config: &Compiled) -> Vec<String> {
    let mut warnings = Vec::new();
    for listener in &config.listeners {
        match bound.iter().find(|bound| bound.name == listener.name) {
            None => warnings.push(format!(
                "listener \"{}\" is new: it is listened on after a restart",
                listener.name
            )),
            Some(bound) if bound.address != listener.address => warnings.push(format!(
                "listener \"{}\" has moved from {} to {}: it stays where it was until a restart",
                listener.name, bound.address, listener.address
            )),
            Some(bound) if bound.http3 != listener.http3.is_some() => warnings.push(format!(
                "listener \"{}\" has HTTP/3 turned {}: its UDP socket is opened or closed at a restart",
                listener.name,
                if bound.http3 { "off" } else { "on" }
            )),
            Some(_) => {}
        }
    }
    for bound in bound {
        if !config.listeners.iter().any(|l| l.name == bound.name) {
            warnings.push(format!(
                "listener \"{}\" is gone: its socket stays open, with no routes (404), until a restart",
                bound.name
            ));
        }
    }
    warnings
}

/// A line for whoever runs the harness. With nobody there to read it, it runs on.
fn say(stderr: &mut impl Write, line: std::fmt::Arguments<'_>) {
    let _nobody_reads = writeln!(stderr, "{line}");
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Config, compile};

    fn parsed(args: &[&str]) -> Result<Parsed, UsageError> {
        parse(args.iter().map(ToString::to_string))
    }

    #[test]
    fn a_config_is_all_that_is_needed() {
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: None,
            workers: None,
            accept: Accept::Balanced,
            limits: H1Limits::default(),
        };
        assert_eq!(
            parsed(&["--config", "dev.yaml"]),
            Ok(Parsed::Run(Box::new(options)))
        );
    }

    #[test]
    fn the_bounds_on_idle_connections_are_as_the_command_line_says() {
        // There is no configuration for these; what a benchmark needs is to hold both
        // clients to the same ones, and to be able to say which they were.
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: None,
            workers: None,
            accept: Accept::Balanced,
            limits: H1Limits {
                idle_per_destination: 64,
                idle_total: 512,
                ..H1Limits::default()
            },
        };
        assert_eq!(
            parsed(&[
                "--config",
                "dev.yaml",
                "--idle-per-destination",
                "64",
                "--idle-total",
                "512",
            ]),
            Ok(Parsed::Run(Box::new(options)))
        );
        // Keeping none is a number like any other; a number it is not, is not.
        assert!(matches!(
            parsed(&["--config", "dev.yaml", "--idle-per-destination", "0"]),
            Ok(Parsed::Run(_))
        ));
        assert_eq!(
            parsed(&["--config", "dev.yaml", "--idle-total", "many"]),
            Err(UsageError::Idle("--idle-total", "many".to_owned()))
        );
        assert_eq!(
            parsed(&[
                "--config",
                "dev.yaml",
                "--idle-total",
                "1",
                "--idle-total",
                "2"
            ]),
            Err(UsageError::Twice("--idle-total"))
        );
    }

    #[test]
    fn the_workers_are_as_the_command_line_says() {
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: None,
            workers: NonZeroUsize::new(4),
            accept: Accept::Balanced,
            limits: H1Limits::default(),
        };
        assert_eq!(
            parsed(&["--config", "dev.yaml", "--workers", "4"]),
            Ok(Parsed::Run(Box::new(options)))
        );
    }

    #[test]
    fn metrics_are_served_where_the_command_line_says() {
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: Some("[::]:9090".parse().unwrap()),
            workers: None,
            accept: Accept::Balanced,
            limits: H1Limits::default(),
        };
        assert_eq!(
            parsed(&["--metrics", "[::]:9090", "--config", "dev.yaml"]),
            Ok(Parsed::Run(Box::new(options)))
        );
    }

    #[test]
    fn connections_are_balanced_unless_they_are_left_to_the_kernel() {
        let config = ["--config", "dev.yaml"];
        let Ok(Parsed::Run(options)) = parsed(&[&config[..], &["--accept", "kernel"]].concat())
        else {
            panic!("a command line that can be run");
        };
        assert_eq!(options.accept, Accept::Kernel);
        let Ok(Parsed::Run(options)) = parsed(&[&config[..], &["--accept", "balanced"]].concat())
        else {
            panic!("a command line that can be run");
        };
        assert_eq!(options.accept, Accept::Balanced);
        assert_eq!(
            parsed(&[&config[..], &["--accept", "luck"]].concat()),
            Err(UsageError::Accept("luck".to_owned()))
        );
    }

    #[test]
    fn an_address_that_cannot_be_listened_on_is_refused() {
        assert_eq!(
            parsed(&["--config", "dev.yaml", "--metrics", "localhost:9090"]),
            Err(UsageError::Address("localhost:9090".to_owned()))
        );
    }

    #[test]
    fn help_is_help_wherever_it_stands() {
        assert_eq!(parsed(&["--help"]), Ok(Parsed::Help));
        assert_eq!(parsed(&["--config", "dev.yaml", "-h"]), Ok(Parsed::Help));
    }

    #[test]
    fn a_command_line_that_cannot_be_acted_on_is_refused() {
        assert_eq!(parsed(&[]), Err(UsageError::NoConfig));
        assert_eq!(
            parsed(&["--metrics", "[::]:9090"]),
            Err(UsageError::NoConfig)
        );
        assert_eq!(parsed(&["--config"]), Err(UsageError::NoValue("--config")));
        assert_eq!(
            parsed(&["--config", "a.yaml", "--metrics"]),
            Err(UsageError::NoValue("--metrics"))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml", "--config", "b.yaml"]),
            Err(UsageError::Twice("--config"))
        );
        assert_eq!(
            parsed(&["a.yaml"]),
            Err(UsageError::Unexpected("a.yaml".to_owned()))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml", "--accept"]),
            Err(UsageError::NoValue("--accept"))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml", "--workers", "1", "--workers", "2"]),
            Err(UsageError::Twice("--workers"))
        );
        for workers in ["0", "-1", "many"] {
            assert_eq!(
                parsed(&["--config", "a.yaml", "--workers", workers]),
                Err(UsageError::Workers(workers.to_owned()))
            );
        }
    }

    #[test]
    fn a_usage_error_comes_with_the_usage_and_its_exit_status() {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(["--bogus".to_owned()].into_iter(), &mut stdout, &mut stderr);
        assert_eq!(status, crate::EXIT_USAGE);
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).unwrap();
        assert!(stderr.starts_with("error: unexpected argument '--bogus'"));
        assert!(stderr.contains(USAGE));
    }

    fn listeners(listeners: &[(&str, &str)]) -> Compiled {
        let mut yaml = String::from("routes: []\nupstreams: {}\nlisteners:\n");
        for (name, address) in listeners {
            yaml += &format!(
                "  {name}: {{ address: \"{address}\", protocol: http, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
            );
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
    }

    #[test]
    fn listeners_that_are_as_they_were_need_no_restart() {
        let config = listeners(&[("admin", "127.0.0.1:81"), ("web", "[::]:80")]);
        let bound: Vec<Bound> = config.listeners.iter().map(Bound::from).collect();
        assert_eq!(restart_needed(&bound, &config), [""; 0]);
    }

    #[test]
    fn a_listener_that_is_new_or_has_moved_or_is_gone_needs_a_restart() {
        let before = listeners(&[("admin", "127.0.0.1:81"), ("web", "[::]:80")]);
        let bound: Vec<Bound> = before.listeners.iter().map(Bound::from).collect();
        let after = listeners(&[("api", "[::]:8080"), ("web", "[::]:8000")]);
        assert_eq!(
            restart_needed(&bound, &after),
            [
                "listener \"api\" is new: it is listened on after a restart",
                "listener \"web\" has moved from [::]:80 to [::]:8000: it stays where it was \
                 until a restart",
                "listener \"admin\" is gone: its socket stays open, with no routes (404), \
                 until a restart",
            ]
        );
    }

    /// A drain ends as soon as the last connection goes.
    #[test]
    fn a_drain_ends_when_the_last_connection_goes() {
        let loads = Loads::new(2, 8);
        let (one, two) = (loads.hold(0), loads.hold(1));
        let (_stopping, stop) = mpsc::channel();
        let going = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(one);
            thread::sleep(Duration::from_millis(50));
            drop(two);
        });
        let began = Instant::now();
        assert_eq!(
            drained(&loads, &stop, Duration::from_secs(10)),
            Drained::Empty
        );
        assert!(began.elapsed() < Duration::from_secs(2));
        going.join().unwrap();
    }

    /// A connection that outlasts the drain is not waited for.
    #[test]
    fn a_drain_ends_when_time_is_up() {
        let loads = Loads::new(1, 8);
        let _held = loads.hold(0);
        let (_stopping, stop) = mpsc::channel();
        let began = Instant::now();
        let within = Duration::from_millis(300);
        assert_eq!(drained(&loads, &stop, within), Drained::OutOfTime(1));
        let took = began.elapsed();
        assert!(took >= within && took < within * 3, "{took:?}");
    }

    /// Told to stop again, a drain ends at once, and so does one nobody can stop.
    #[test]
    fn a_drain_ends_when_told_to_stop_again() {
        let loads = Loads::new(1, 8);
        let _held = loads.hold(0);
        let (stopping, stop) = mpsc::channel();
        stopping.send(()).unwrap();
        let began = Instant::now();
        assert_eq!(
            drained(&loads, &stop, Duration::from_secs(10)),
            Drained::Stopped(1)
        );
        assert!(began.elapsed() < Duration::from_secs(1));
        drop(stopping);
        let within = Duration::from_millis(200);
        assert_eq!(drained(&loads, &stop, within), Drained::OutOfTime(1));
    }
}
