//! `edgerush proxy`: a data plane, its config from a file, a harness for development and
//! tests, or from the control plane's config stream ([`crate::stream`]).
//!
//! The file is the config model as it is, nothing added; what is about the process and not
//! about routing (where `/metrics` is served, how many workers there are, whose a new
//! connection is) is on the command line. Listeners are bound once, at the start: from the
//! file, or from the stream's first config, which is waited for. After that the file is
//! read again every [`POLL`], or the stream looked at every [`STREAM_POLL`], and a config
//! that has changed takes over without dropping a request, while one that cannot be run is
//! told and changes nothing.
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

use crate::bind::{Port, datagrams, listen};
use crate::config_file::Problems;
use crate::config_file::{ConfigFile, Rejected};
use crate::essential::{Ended, Essential, Told};
use crate::per_core::{self, Accept};
use crate::stream::{FromStream, Stream, StreamError};
use edgerush_config::{Compiled, Config, compile};
use edgerush_control_service::Files;
use edgerush_kubernetes::{ApiError, ApiServer};
use edgerush_proxy::{H1Limits, Proxy, ProxyError};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::runtime::Builder;

pub(crate) const USAGE: &str = "\
Usage: edgerush proxy --config <FILE> [OPTIONS]
       edgerush proxy --control-plane <ADDRESS> --certificate <FILE> --key <FILE>
                      --authorities <FILE> [OPTIONS]

Runs a data plane. With --config, from a config file, without Kubernetes: a harness for
development. The file, and the certificate files it names, are read again every second,
and a change takes over without dropping a request. With --control-plane, from the config
stream that control plane serves: nothing is bound or served until its first config runs,
which is waited for, and each config after it takes over as a file's does; the certificate,
key and authorities are PEM files, read again every second. Listeners are bound once: one
that is new or has moved takes a restart.
Ctrl-C or SIGTERM drains it: what is under way is finished, for up to 29 seconds; a
second one stops it at once. SIGUSR1 opens the access logs' files again, for rotation.

Options:
      --config <FILE>      The config, in YAML
      --control-plane <ADDRESS>
                           The control plane's config stream, as host:port; its
                           certificate must name the host
      --certificate <FILE> The data plane's certificate on the stream, then its
                           intermediates
      --key <FILE>         Its private key
      --authorities <FILE> The stream's authorities, which the control plane's
                           certificate must be signed by
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

/// How often the config stream is looked at for a config to run: what it hands over waits
/// in a channel, so looking costs nothing.
const STREAM_POLL: Duration = Duration::from_millis(50);

/// How long a draining process waits for its connections before it exits anyway: inside
/// Kubernetes' default 30 s grace, and past the 25 s a worker gives its connections, the
/// sweep that brings the drain to it and the flush of what it then closes.
const DRAIN: Duration = Duration::from_secs(29);

/// How often a draining process looks at what its workers still hold.
const DRAIN_POLL: Duration = Duration::from_millis(100);

/// How long a drained process waits for its access-log records to be written: what is left
/// of Kubernetes' grace after [`DRAIN`], less the exit itself. Writing what the workers hold
/// takes a few batches' writes.
const LOGS: Duration = Duration::from_millis(500);

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
    source: Source,
    metrics: Option<SocketAddr>,
    /// `None` is one for every CPU the process may use.
    workers: Option<NonZeroUsize>,
    accept: Accept,
    /// What a worker will not go beyond. There is no configuration for these; what a
    /// benchmark needs is a way to say which it held the worker to
    /// ([13 §7](../../../docs/13-http1-upstream.md)).
    limits: H1Limits,
}

/// Where the config comes from.
#[derive(Debug, PartialEq, Eq)]
enum Source {
    File(PathBuf),
    ControlPlane(Stream),
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'--config <FILE>' or '--control-plane <ADDRESS>' is required")]
    NoConfig,
    #[error("'--config' and '--control-plane' are not given together")]
    Both,
    #[error("'{0}' is not a control plane's address and port, such as edgerush-control:9443")]
    ControlPlane(String),
    #[error(
        "'--control-plane' needs '--certificate', '--key' and '--authorities', and they need it"
    )]
    Stream,
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
    let mut control_plane = None;
    let (mut certificate, mut key, mut authorities) = (None, None, None);
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
            "--control-plane" => {
                let address = args.next().ok_or(UsageError::NoValue("--control-plane"))?;
                let port = address
                    .rsplit_once(':')
                    .map(|(_, port)| port.parse::<u16>());
                if !matches!(port, Some(Ok(_))) {
                    return Err(UsageError::ControlPlane(address));
                }
                if control_plane.replace(address).is_some() {
                    return Err(UsageError::Twice("--control-plane"));
                }
            }
            option @ ("--certificate" | "--key" | "--authorities") => {
                let (slot, option) = match option {
                    "--certificate" => (&mut certificate, "--certificate"),
                    "--key" => (&mut key, "--key"),
                    _ => (&mut authorities, "--authorities"),
                };
                let file = args.next().ok_or(UsageError::NoValue(option))?;
                if slot.replace(PathBuf::from(file)).is_some() {
                    return Err(UsageError::Twice(option));
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
    let stream = match (control_plane, certificate, key, authorities) {
        (Some(address), Some(certificate), Some(key), Some(authorities)) => Some(Stream {
            address,
            files: Files {
                certificate,
                key,
                authorities,
            },
        }),
        (None, None, None, None) => None,
        _ => return Err(UsageError::Stream),
    };
    let source = match (config, stream) {
        (Some(file), None) => Source::File(file),
        (None, Some(stream)) => Source::ControlPlane(stream),
        (Some(_), Some(_)) => return Err(UsageError::Both),
        (None, None) => return Err(UsageError::NoConfig),
    };
    Ok(Parsed::Run(Box::new(Options {
        source,
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
    #[error("{config} cannot be run:\n{error}")]
    Proxy { config: String, error: ProxyError },
    #[error(
        "config {} cannot be run:\nupstream `{upstream}`: its endpoints come from Kubernetes, and {why}",
        path.display()
    )]
    Unfollowable {
        path: PathBuf,
        upstream: String,
        why: ApiError,
    },
    #[error(transparent)]
    Stream(#[from] StreamError),
    #[error("cannot listen on {address} for {what}: {error}")]
    Listen {
        what: String,
        address: SocketAddr,
        error: io::Error,
    },
    #[error("cannot start the runtime: {0}")]
    Runtime(io::Error),
    #[error(transparent)]
    Workers(#[from] per_core::NotStarted),
    #[error("{0}: the data plane stops, to be started afresh")]
    Ended(Ended),
    #[error("{0} while the data plane drained")]
    PanickedDraining(Ended),
}

/// How long a data plane with discovered upstreams, or upstreams that resolve a name, waits
/// for the first list of every namespace it follows and the first answer for every name
/// before it serves anyway, as Envoy waits for its clusters (03 §6).
const FIRST_LISTS: Duration = Duration::from_secs(15);

/// How often that wait looks.
const FIRST_LISTS_POLL: Duration = Duration::from_millis(20);

/// The first upstream of `config` whose endpoints come from Kubernetes, if `outside` says
/// why there is no cluster's API server to follow them through: such an upstream would
/// never have an endpoint.
fn unfollowable<'a>(config: &'a Compiled, outside: Option<&ApiError>) -> Option<&'a str> {
    outside?;
    config
        .upstreams()
        .iter()
        .find(|upstream| upstream.discovery.is_some())
        .map(|upstream| upstream.name.as_str())
}

fn run(options: Options, stderr: &mut impl Write) -> Result<(), Failure> {
    let Options {
        source,
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
    let memory = crate::limits::memory();
    let connections = crate::limits::connections_per_worker(open_files, memory, workers);
    let mut limits = limits;
    limits.storage = crate::limits::storage_per_worker(memory, workers);
    limits.descriptors = crate::limits::descriptors_per_worker(open_files, workers);
    // One worker is alone on every listener's port and needs nothing of the kernel; it is
    // from the second on that they share one, which is what SO_REUSEPORT is for.
    let port = if workers == NonZeroUsize::MIN {
        Port::Own
    } else {
        Port::Shared
    };

    // The cluster's API server, if the data plane is in one: what discovered upstreams are
    // followed through, a config with one refused without it.
    let cluster = ApiServer::in_cluster();
    let outside = cluster.as_ref().err().cloned();
    // Listened for before anything else, so that no signal finds the default action, which
    // is to die on the spot, a data plane waiting for its first config among them. The
    // access logs are opened again once there is a proxy to have them.
    let (stopping, stop) = mpsc::channel();
    let logged: Arc<OnceLock<Arc<Proxy>>> = Arc::default();
    let logs = Arc::clone(&logged);
    signals(stopping.clone(), move || {
        if let Some(proxy) = logs.get() {
            proxy.reopen_logs();
        }
    })
    .map_err(Failure::Runtime)?;
    let (mut configs, config, named, first_ran) = match source {
        Source::File(path) => {
            let (file, config) = match ConfigFile::open(path.clone()) {
                Ok(opened) => opened,
                Err(rejected) => return Err(Failure::Config { path, rejected }),
            };
            if let Some(upstream) = unfollowable(&config, outside.as_ref()) {
                return Err(Failure::Unfollowable {
                    path,
                    upstream: upstream.to_owned(),
                    why: outside.unwrap_or(ApiError::NoServer),
                });
            }
            let named = format!("config {}", path.display());
            (Configs::File(file), config, named, None)
        }
        Source::ControlPlane(stream) => {
            let handing = crate::stream::start(&stream, &stopping)?;
            say(
                stderr,
                format_args!(
                    "waiting for the first config from the control plane at {}",
                    stream.address
                ),
            );
            match first(&handing, &stop, outside.as_ref(), stderr) {
                First::Config(config, ran) => {
                    let named = "the control plane's config".to_owned();
                    (Configs::Stream(handing), *config, named, Some(ran))
                }
                First::Stop => return Ok(()),
                First::Failed(ended) => return Err(Failure::Ended(ended)),
            }
        }
    };
    let mut bound: Vec<Bound> = config.listeners().iter().map(Bound::from).collect();
    // Limits checked but enforced in none is said once, so that a mode left out is seen.
    let unenforced = (!config.rate_limiting().enforces()).then_some(config.limits().len());
    let proxy = Proxy::new(config, workers)
        .map(Arc::new)
        .map_err(|error| Failure::Proxy {
            config: named.clone(),
            error,
        })?;
    let _first = logged.set(Arc::clone(&proxy));
    // Before anything is discovered or resolved, so that nothing is sent to these first.
    let denied = denied(
        std::env::var("POD_IPS").ok().as_deref(),
        std::env::var("KUBERNETES_SERVICE_HOST").ok().as_deref(),
    );
    proxy.deny(denied, |upstream, address, why| {
        say(
            &mut io::stderr(),
            format_args!("upstream {upstream}: {address} is not sent to, as {why}"),
        );
    });

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
    if let Some(limits @ 1..) = unenforced {
        say(
            stderr,
            format_args!(
                "rate limiting is off: {limits} limit{} checked, none enforced",
                if limits == 1 { "" } else { "s" }
            ),
        );
    }
    proxy
        .start_rate_limits(rate_limits_failed(&stopping))
        .map_err(|error| Failure::Proxy {
            config: named,
            error,
        })?;
    if let Ok(api) = cluster {
        kubernetes(Arc::clone(&proxy), api, &stopping).map_err(Failure::Runtime)?;
    }
    resolver(Arc::clone(&proxy), &stopping).map_err(Failure::Runtime)?;
    // Nothing accepted until every namespace followed has been listed once and every name
    // looked up once, so that a client waits rather than be answered 503 for want of a
    // list or an answer; for so long at most.
    match first_lists(&proxy, &stop, FIRST_LISTS) {
        Next::Look => {}
        Next::Stop => return Ok(()),
        Next::Failed(ended) => return Err(Failure::Ended(ended)),
    }
    let unlisted = proxy.unlisted();
    if !unlisted.is_empty() {
        say(
            stderr,
            format_args!(
                "serving before the first EndpointSlices of {}: their upstreams answer 503 until they come",
                unlisted.join(", ")
            ),
        );
    }
    let unresolved = proxy.unresolved();
    if !unresolved.is_empty() {
        say(
            stderr,
            format_args!(
                "serving before the first answers for {}: their upstreams answer 503 until they come",
                unresolved.join(", ")
            ),
        );
    }
    // Every worker runs on a thread of its own, which stays for as long as the process.
    let loads = per_core::start(&proxy, sockets, accept, limits, connections, &stopping)?;
    health_checks(Arc::clone(&proxy), &stopping).map_err(Failure::Runtime)?;
    if let Some(socket) = metrics {
        let address = socket.local_addr().map_err(Failure::Runtime)?;
        scrapes(Arc::clone(&proxy), socket, &stopping).map_err(Failure::Runtime)?;
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
    say(
        stderr,
        format_args!(
            "{}",
            crate::limits::described_memory(memory, limits.storage)
        ),
    );

    if let Some(ran) = first_ran {
        let _gone = ran.send(Ok(()));
        say(stderr, format_args!("config from the control plane runs"));
    }

    // Until told to stop; `stopping` is still held here, so the channel cannot close.
    loop {
        match next(&stop, configs.poll()) {
            Next::Look => {}
            Next::Stop => break,
            Next::Failed(ended) => return Err(Failure::Ended(ended)),
        }
        match &mut configs {
            Configs::File(file) => {
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
                let reloaded = runnable(&config, outside.as_ref())
                    .and_then(|()| reloaded(&proxy, &bound, config));
                match reloaded {
                    Ok(warnings) => {
                        say(stderr, format_args!("config reloaded"));
                        for warning in warnings {
                            say(stderr, format_args!("warning: {warning}"));
                        }
                    }
                    Err(why) => say(
                        stderr,
                        format_args!("config rejected, the one before it runs on:\n{why}"),
                    ),
                }
            }
            Configs::Stream(handing) => {
                while let Ok(from) = handing.try_recv() {
                    let (config, ran) = match from {
                        FromStream::Said(line) => {
                            say(stderr, format_args!("{line}"));
                            continue;
                        }
                        FromStream::Run { config, ran } => (config, ran),
                    };
                    let reloaded = compiled(&config).and_then(|config| {
                        runnable(&config, outside.as_ref())?;
                        reloaded(&proxy, &bound, config)
                    });
                    match &reloaded {
                        Ok(warnings) => {
                            say(stderr, format_args!("config from the control plane runs"));
                            for warning in warnings {
                                say(stderr, format_args!("warning: {warning}"));
                            }
                        }
                        Err(why) => say(
                            stderr,
                            format_args!(
                                "config from the control plane refused, the one before it runs on:\n{why}"
                            ),
                        ),
                    }
                    let _gone = ran.send(reloaded.map(|_| ()));
                }
            }
        }
    }
    // A config handed over from here on is refused as the data plane stops.
    drop(configs);
    drop(stopping);
    say(stderr, format_args!("draining"));
    proxy.drain();
    // The workers' TCP connections, and every connection the data plane counts open,
    // HTTP/3's included, whose QUIC connections the balancer never sees: whichever is more.
    let held = || {
        loads
            .now()
            .iter()
            .sum::<usize>()
            .max(proxy.open_connections())
    };
    let (how, panicked) = drained(held, &stop, DRAIN);
    match how {
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
    let finishing = Arc::clone(&proxy);
    match logs_written(&how, &stop, LOGS, move |within| {
        finishing.finish_logs(within)
    }) {
        Logged::Written => {}
        Logged::OutOfTime => say(
            stderr,
            format_args!("out of time writing the access logs: what was left is lost"),
        ),
        Logged::Stopped => say(
            stderr,
            format_args!("stopped: the access logs' records not yet written are lost"),
        ),
    }
    // A panic as the workers drained fails the process all the same, once the drain has let
    // the other workers' requests finish (03 §2): an exit status of 0 would hide it.
    match panicked {
        Some(ended) => Err(Failure::PanickedDraining(ended)),
        None => Ok(()),
    }
}

/// Where the configs after the first come from.
enum Configs {
    File(ConfigFile),
    Stream(Receiver<FromStream>),
}

impl Configs {
    /// How often the main loop looks for one.
    fn poll(&self) -> Duration {
        match self {
            Self::File(_) => POLL,
            Self::Stream(_) => STREAM_POLL,
        }
    }
}

/// How the wait for the stream's first config ended.
enum First {
    /// With it, compiled, and where to say whether it runs.
    Config(
        Box<Compiled>,
        tokio::sync::oneshot::Sender<Result<(), String>>,
    ),
    Stop,
    Failed(Ended),
}

/// Waits for the stream's first config that can be run, saying what the stream says
/// meanwhile, and each config refused, here and on the stream.
fn first(
    handing: &Receiver<FromStream>,
    told: &Receiver<Told>,
    outside: Option<&ApiError>,
    stderr: &mut impl Write,
) -> First {
    loop {
        match next(told, STREAM_POLL) {
            Next::Look => {}
            Next::Stop => return First::Stop,
            Next::Failed(ended) => return First::Failed(ended),
        }
        while let Ok(from) = handing.try_recv() {
            let (config, ran) = match from {
                FromStream::Said(line) => {
                    say(stderr, format_args!("{line}"));
                    continue;
                }
                FromStream::Run { config, ran } => (config, ran),
            };
            let runs = compiled(&config).and_then(|config| {
                runnable(&config, outside)?;
                Ok(config)
            });
            match runs {
                Ok(config) => return First::Config(Box::new(config), ran),
                Err(why) => {
                    say(
                        stderr,
                        format_args!("config from the control plane refused:\n{why}"),
                    );
                    let _gone = ran.send(Err(why));
                }
            }
        }
    }
}

/// A config from the stream, compiled as a file's is.
fn compiled(config: &Config) -> Result<Compiled, String> {
    compile(config).map_err(|problems| Problems(problems).to_string())
}

/// Whether `config` can be run here: not with an upstream whose endpoints come from
/// Kubernetes, where `outside` says why there is no cluster to follow them through.
fn runnable(config: &Compiled, outside: Option<&ApiError>) -> Result<(), String> {
    match (unfollowable(config, outside), outside) {
        (Some(upstream), Some(why)) => Err(format!(
            "upstream `{upstream}`: its endpoints come from Kubernetes, and {why}"
        )),
        _ => Ok(()),
    }
}

/// Has `proxy` run `config`, published once, for every worker at once, so that none of
/// them sees the old config after another has seen the new one: what takes a restart, its
/// listeners being bound once, or why it was refused.
fn reloaded(proxy: &Proxy, bound: &[Bound], config: Compiled) -> Result<Vec<String>, String> {
    let warnings = restart_needed(bound, &config);
    proxy.reload(config).map_err(|error| error.to_string())?;
    Ok(warnings)
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

/// What became of the access logs' last records.
#[derive(Debug, PartialEq, Eq)]
enum Logged {
    /// Written.
    Written,
    /// Not all written within the wait.
    OutOfTime,
    /// Not waited for: told to stop again (03 §10).
    Stopped,
}

/// Has the access logs' last records written by `finish`, given up to `within`, unless told
/// to stop: after a drain a second signal stopped, not at all, and while they are being
/// written, at once. A second signal exits at once (03 §10); whatever was not written is
/// lost, and said to be.
fn logs_written(
    how: &Drained,
    stop: &Receiver<Told>,
    within: Duration,
    finish: impl FnOnce(Duration) -> bool + Send + 'static,
) -> Logged {
    if matches!(how, Drained::Stopped(_)) {
        return Logged::Stopped;
    }
    let (done, written) = mpsc::channel();
    // On a thread of its own, so that this one still hears a signal; left behind if one
    // comes, and ended with the process.
    let _writing = thread::spawn(move || {
        let _told = done.send(finish(within));
    });
    let mut listening = true;
    loop {
        match written.recv_timeout(DRAIN_POLL) {
            Ok(true) => return Logged::Written,
            Ok(false) | Err(RecvTimeoutError::Disconnected) => return Logged::OutOfTime,
            Err(RecvTimeoutError::Timeout) => {}
        }
        // With nobody left to say stop, the wait runs its course.
        while listening {
            match stop.try_recv() {
                Ok(Told::Stop) => return Logged::Stopped,
                Ok(Told::Ended(_)) => {}
                Err(mpsc::TryRecvError::Empty) => break,
                Err(mpsc::TryRecvError::Disconnected) => listening = false,
            }
        }
    }
}

/// What the main loop does next.
#[derive(Debug, PartialEq, Eq)]
enum Next {
    /// Looks at the config file again: `every` passed with nothing said.
    Look,
    /// Stops: a signal came.
    Stop,
    /// Fails: something the data plane cannot do without ended ([`crate::essential`]).
    Failed(Ended),
}

/// What `told` says within `every`.
fn next(told: &Receiver<Told>, every: Duration) -> Next {
    match told.recv_timeout(every) {
        Err(RecvTimeoutError::Timeout) => Next::Look,
        Ok(Told::Stop) => Next::Stop,
        Ok(Told::Ended(ended)) => Next::Failed(ended),
        // Never while the loop holds a sender of its own; were it to, nothing could stop it.
        Err(RecvTimeoutError::Disconnected) => Next::Stop,
    }
}

/// Waits until nothing is `held` — no connection, over TCP or QUIC — `within` is up, or
/// `stop` says to stop again; and says the first watched task or thread that panicked
/// meanwhile, which does not cut the drain short.
fn drained(
    held: impl Fn() -> usize,
    stop: &Receiver<Told>,
    within: Duration,
) -> (Drained, Option<Ended>) {
    let until = Instant::now() + within;
    let mut panicked = None;
    loop {
        let held = held();
        if held == 0 {
            return (Drained::Empty, panicked);
        }
        let left = until.saturating_duration_since(Instant::now());
        if left.is_zero() {
            return (Drained::OutOfTime(held), panicked);
        }
        match stop.recv_timeout(DRAIN_POLL.min(left)) {
            Ok(Told::Stop) => return (Drained::Stopped(held), panicked),
            // A panic is remembered; the other workers' requests still finish.
            Ok(Told::Ended(ended)) if !ended.finished => {
                panicked.get_or_insert(ended);
            }
            // With nobody left to say stop, the drain runs its course.
            // What ends as the workers drain, their accepting first, was meant to.
            Ok(Told::Ended(_))
            | Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {}
        }
    }
}

/// Says on `stopping`, from a thread of its own, every time the process is told to stop:
/// Ctrl-C, and on Unix SIGTERM, which is how Kubernetes asks. On Unix it also calls
/// `reopen` at every SIGUSR1, which asks for the access logs' files to be opened again
/// once they have been moved aside, as it asks NGINX and Envoy ([21 §4] in the docs). All
/// are listened for before this returns.
///
/// [21 §4]: ../../../docs/21-access-logs.md
fn signals(stopping: Sender<Told>, reopen: impl Fn() + Send + 'static) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_io().build()?;
    let entered = runtime.enter();
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    #[cfg(unix)]
    let mut rotate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined1())?;
    #[cfg(not(unix))]
    drop(reopen);
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
                        while let std::task::Poll::Ready(Some(())) = rotate.poll_recv(cx) {
                            reopen();
                        }
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
                    if told.is_err() || stopping.send(Told::Stop).is_err() {
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
/// as a worker's does. Told on `tell` if it ever ends ([`crate::essential`]).
fn scrapes(
    proxy: Arc<Proxy>,
    socket: std::net::TcpListener,
    tell: &Sender<Told>,
) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    // The socket is handed to the runtime that is entered, as a worker's are.
    let entered = runtime.enter();
    let socket = TcpListener::from_std(socket)?;
    drop(entered);
    let tell = tell.clone();
    thread::Builder::new()
        .name("metrics".to_owned())
        .spawn(move || {
            let mut essential = Essential::new("the scrape".to_owned(), &tell);
            tokio::task::LocalSet::new().block_on(&runtime, proxy.serve_metrics(socket));
            essential.finished();
        })?;
    Ok(())
}

/// Probes the upstreams that ask for health checks, on a runtime and a thread of their
/// own: a worker saturated with requests must not starve the checks that decide where
/// its requests go ([03 §6] in the docs). Told on `tell` if it ever ends
/// ([`crate::essential`]).
fn health_checks(proxy: Arc<Proxy>, tell: &Sender<Told>) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let tell = tell.clone();
    thread::Builder::new()
        .name("health".to_owned())
        .spawn(move || {
            let mut essential = Essential::new("the health checker".to_owned(), &tell);
            tokio::task::LocalSet::new().block_on(&runtime, proxy.check_health());
            essential.finished();
        })?;
    Ok(())
}

/// Follows the namespaces the running config's discovered upstreams are in, through `api`,
/// on a thread and runtime of its own, as the health checks are (03 §6); what fails is said
/// on stderr with its namespace.
fn kubernetes(proxy: Arc<Proxy>, api: ApiServer, tell: &Sender<Told>) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let tell = tell.clone();
    thread::Builder::new()
        .name("kubernetes".to_owned())
        .spawn(move || {
            let mut essential = Essential::new("the EndpointSlice watch".to_owned(), &tell);
            let follows = proxy.follow_kubernetes(api, |namespace, failure| {
                say(
                    &mut io::stderr(),
                    format_args!("EndpointSlices of namespace {namespace}: {failure}"),
                );
            });
            tokio::task::LocalSet::new().block_on(&runtime, follows);
            essential.finished();
        })?;
    Ok(())
}

/// What the data plane refuses to send to beside the ranges none sends to (03 §6): the pod's
/// own addresses, `pod_ips` as the Downward API gives `status.podIPs`, comma-separated; and
/// the API server's, `api_server` as a pod is given it. What is not an address is passed
/// over: outside a cluster there are none.
fn denied(pod_ips: Option<&str>, api_server: Option<&str>) -> edgerush_proxy::egress::Denied {
    edgerush_proxy::egress::Denied {
        own: pod_ips
            .into_iter()
            .flat_map(|ips| ips.split(','))
            .filter_map(|ip| ip.trim().parse().ok())
            .collect(),
        api_server: api_server.and_then(|host| host.trim().parse().ok()),
    }
}

/// Resolves the names the running config's upstreams resolve, of the nameservers
/// `/etc/resolv.conf` lists, on a thread and runtime of its own, as EndpointSlices are
/// followed (03 §6); what fails is said on stderr with its name.
fn resolver(proxy: Arc<Proxy>, tell: &Sender<Told>) -> io::Result<()> {
    let runtime = Builder::new_current_thread().enable_all().build()?;
    let tell = tell.clone();
    thread::Builder::new()
        .name("resolver".to_owned())
        .spawn(move || {
            let mut essential = Essential::new("the resolver".to_owned(), &tell);
            let resolving = crate::resolver::resolve_names(
                proxy,
                crate::resolver::system_nameservers(),
                crate::resolver::Pace::default(),
                |name, failure| {
                    say(&mut io::stderr(), format_args!("name {name}: {failure}"));
                },
            );
            tokio::task::LocalSet::new().block_on(&runtime, resolving);
            essential.finished();
        })?;
    Ok(())
}

/// Waits until every namespace the running config follows has been listed once, and every
/// name it resolves looked up once, or for `most` at most, [`FIRST_LISTS`] when it runs:
/// [`Next::Look`] then, or what the main loop was told meanwhile.
fn first_lists(proxy: &Proxy, told: &Receiver<Told>, most: Duration) -> Next {
    let until = Instant::now() + most;
    while (!proxy.unlisted().is_empty() || !proxy.unresolved().is_empty()) && Instant::now() < until
    {
        match next(told, FIRST_LISTS_POLL) {
            Next::Look => {}
            other => return other,
        }
    }
    Next::Look
}

/// What the proxy calls if its rate-limiting thread fails, a thread it starts itself when a
/// rule first limits: the main loop told, as a guard tells it ([`crate::essential`]).
fn rate_limits_failed(tell: &Sender<Told>) -> impl Fn() + Send + Sync + 'static {
    let tell = tell.clone();
    move || {
        let ended = Ended {
            what: "the rate-limiting thread".to_owned(),
            finished: false,
        };
        // With nobody left to tell, the process is ending anyway.
        let _told = tell.send(Told::Ended(ended));
    }
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
    for listener in config.listeners() {
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
        if !config.listeners().iter().any(|l| l.name == bound.name) {
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
    use edgerush_proxy::connections::{Loads, QUIC_MOST};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn parsed(args: &[&str]) -> Result<Parsed, UsageError> {
        parse(args.iter().map(ToString::to_string))
    }

    #[test]
    fn the_pods_own_and_the_api_servers_addresses_are_read_from_the_environment() {
        let both = denied(Some("10.244.0.5,fd00:10:244::5"), Some("10.96.0.1"));
        assert_eq!(
            both.own,
            [
                "10.244.0.5".parse::<std::net::IpAddr>().unwrap(),
                "fd00:10:244::5".parse().unwrap()
            ]
        );
        assert_eq!(both.api_server, Some("10.96.0.1".parse().unwrap()));
        let one = denied(Some("10.244.0.5"), Some("fd00::1"));
        assert_eq!(one.own.len(), 1);
        assert_eq!(one.api_server, Some("fd00::1".parse().unwrap()));
        assert_eq!(
            denied(None, None),
            edgerush_proxy::egress::Denied::default()
        );
        assert_eq!(
            denied(Some(" , not-an-address,"), Some("kubernetes.default")),
            edgerush_proxy::egress::Denied::default()
        );
    }

    /// A data plane holds until each name it resolves has been looked up, for so long at
    /// most, and then holds no more.
    #[test]
    fn it_holds_for_the_first_answer_for_each_name_for_so_long_at_most() {
        let config: Config = serde_saphyr::from_str(
            "listeners: {}\nroutes: []\nupstreams:\n  web: { load_balancer: p2c, endpoints: { dns: \"web.shop.svc.cluster.local:8080\" } }\n",
        )
        .unwrap();
        let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
        let (_tell, told) = mpsc::channel();
        let held = Instant::now();
        assert!(matches!(
            first_lists(&proxy, &told, Duration::from_millis(300)),
            Next::Look
        ));
        assert!(
            held.elapsed() >= Duration::from_millis(300),
            "it did not hold"
        );
        proxy
            .reresolve([("web.shop.svc.cluster.local".to_owned(), Some(Vec::new()))])
            .unwrap();
        let held = Instant::now();
        assert!(matches!(
            first_lists(&proxy, &told, Duration::from_secs(10)),
            Next::Look
        ));
        assert!(held.elapsed() < Duration::from_secs(1), "it held on");
    }

    #[test]
    fn a_config_is_all_that_is_needed() {
        let options = Options {
            source: Source::File(PathBuf::from("dev.yaml")),
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
            source: Source::File(PathBuf::from("dev.yaml")),
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
            source: Source::File(PathBuf::from("dev.yaml")),
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
            source: Source::File(PathBuf::from("dev.yaml")),
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
    fn the_config_comes_from_a_file_or_a_control_plane_never_both() {
        let stream = [
            "--control-plane",
            "edgerush-control.edgerush.svc:9443",
            "--certificate",
            "c",
            "--key",
            "k",
            "--authorities",
            "a",
        ];
        let Ok(Parsed::Run(options)) = parsed(&stream) else {
            panic!("not run")
        };
        assert_eq!(
            options.source,
            Source::ControlPlane(Stream {
                address: "edgerush-control.edgerush.svc:9443".to_owned(),
                files: Files {
                    certificate: PathBuf::from("c"),
                    key: PathBuf::from("k"),
                    authorities: PathBuf::from("a"),
                },
            })
        );
        let mut both = vec!["--config", "a.yaml"];
        both.extend(stream);
        assert_eq!(parsed(&both), Err(UsageError::Both));
        // Each of the stream's four left out.
        for left_out in 0..4 {
            let args: Vec<&str> = stream
                .chunks(2)
                .enumerate()
                .filter(|(i, _)| *i != left_out)
                .flat_map(|(_, pair)| pair.iter().copied())
                .collect();
            assert_eq!(parsed(&args), Err(UsageError::Stream), "{args:?}");
        }
        for address in ["edgerush-control", "edgerush-control:port", "[::1]:99999"] {
            assert_eq!(
                parsed(&["--control-plane", address]),
                Err(UsageError::ControlPlane(address.to_owned()))
            );
        }
        assert_eq!(
            parsed(&["--key", "k", "--key", "l"]),
            Err(UsageError::Twice("--key"))
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
                "  {name}: {{ address: \"{address}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
            );
        }
        let config: Config = serde_saphyr::from_str(&yaml).unwrap();
        compile(&config).unwrap()
    }

    #[test]
    fn listeners_that_are_as_they_were_need_no_restart() {
        let config = listeners(&[("admin", "127.0.0.1:81"), ("web", "[::]:80")]);
        let bound: Vec<Bound> = config.listeners().iter().map(Bound::from).collect();
        assert_eq!(restart_needed(&bound, &config), [""; 0]);
    }

    #[test]
    fn a_listener_that_is_new_or_has_moved_or_is_gone_needs_a_restart() {
        let before = listeners(&[("admin", "127.0.0.1:81"), ("web", "[::]:80")]);
        let bound: Vec<Bound> = before.listeners().iter().map(Bound::from).collect();
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
        let loads = Loads::new(2, 8, QUIC_MOST, 1);
        let (one, two) = (loads.hold(0, 0), loads.hold(1, 0));
        let (_stopping, stop) = mpsc::channel();
        let going = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            drop(one);
            thread::sleep(Duration::from_millis(50));
            drop(two);
        });
        let began = Instant::now();
        assert_eq!(
            drained(|| loads.now().iter().sum(), &stop, Duration::from_secs(10)),
            (Drained::Empty, None)
        );
        assert!(began.elapsed() < Duration::from_secs(2));
        going.join().unwrap();
    }

    /// A drain waits on whatever is held, connections the balancer never sees (HTTP/3's)
    /// as well as those it does.
    #[test]
    fn a_drain_waits_on_connections_the_balancer_does_not_see() {
        let loads = Loads::new(1, 8, QUIC_MOST, 1);
        let quic = Arc::new(AtomicUsize::new(1));
        let going = Arc::clone(&quic);
        let (_stopping, stop) = mpsc::channel();
        let gone = thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            going.store(0, Ordering::SeqCst);
        });
        let began = Instant::now();
        let held = || {
            let tcp: usize = loads.now().iter().sum();
            tcp.max(quic.load(Ordering::SeqCst))
        };
        assert_eq!(
            drained(held, &stop, Duration::from_secs(10)),
            (Drained::Empty, None)
        );
        assert!(began.elapsed() >= Duration::from_millis(200));
        gone.join().unwrap();
    }

    /// A connection that outlasts the drain is not waited for.
    #[test]
    fn a_drain_ends_when_time_is_up() {
        let loads = Loads::new(1, 8, QUIC_MOST, 1);
        let _held = loads.hold(0, 0);
        let (_stopping, stop) = mpsc::channel();
        let began = Instant::now();
        let within = Duration::from_millis(300);
        assert_eq!(
            drained(|| loads.now().iter().sum(), &stop, within),
            (Drained::OutOfTime(1), None)
        );
        let took = began.elapsed();
        assert!(took >= within && took < within * 3, "{took:?}");
    }

    /// Told to stop again, a drain ends at once, and so does one nobody can stop.
    #[test]
    fn a_drain_ends_when_told_to_stop_again() {
        let loads = Loads::new(1, 8, QUIC_MOST, 1);
        let _held = loads.hold(0, 0);
        let (stopping, stop) = mpsc::channel();
        stopping.send(Told::Stop).unwrap();
        let began = Instant::now();
        assert_eq!(
            drained(|| loads.now().iter().sum(), &stop, Duration::from_secs(10)),
            (Drained::Stopped(1), None)
        );
        assert!(began.elapsed() < Duration::from_secs(1));
        drop(stopping);
        let within = Duration::from_millis(200);
        assert_eq!(
            drained(|| loads.now().iter().sum(), &stop, within),
            (Drained::OutOfTime(1), None)
        );
    }

    /// After a drain a second signal stopped, the access logs are not waited for: the
    /// process goes at once, what was not written lost (03 §10, C42).
    #[test]
    fn after_a_stopped_drain_the_logs_are_not_waited_for() {
        let (_stopping, stop) = mpsc::channel();
        let asked = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let asking = Arc::clone(&asked);
        let began = Instant::now();
        let logged = logs_written(
            &Drained::Stopped(3),
            &stop,
            Duration::from_secs(5),
            move |_| {
                asking.store(true, std::sync::atomic::Ordering::SeqCst);
                true
            },
        );
        assert_eq!(logged, Logged::Stopped);
        assert!(
            !asked.load(std::sync::atomic::Ordering::SeqCst),
            "waited for the logs"
        );
        assert!(began.elapsed() < Duration::from_millis(100));
    }

    /// A signal while the logs are being written cuts the wait short; otherwise the wait
    /// says whether they were written in time.
    #[test]
    fn a_signal_while_the_logs_are_written_cuts_the_wait_short() {
        let (stopping, stop) = mpsc::channel();
        let began = Instant::now();
        let saying = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            stopping.send(Told::Stop).unwrap();
            stopping
        });
        let logged = logs_written(&Drained::Empty, &stop, Duration::from_secs(5), |_| {
            thread::sleep(Duration::from_secs(3));
            true
        });
        assert_eq!(logged, Logged::Stopped);
        let took = began.elapsed();
        assert!(took < Duration::from_secs(1), "{took:?}");
        let stopping = saying.join().unwrap();

        let written = logs_written(&Drained::Empty, &stop, Duration::from_secs(5), |_| true);
        assert_eq!(written, Logged::Written);
        let late = logs_written(
            &Drained::OutOfTime(1),
            &stop,
            Duration::from_secs(5),
            |_| false,
        );
        assert_eq!(late, Logged::OutOfTime);
        drop(stopping);
        // With nobody left to say stop, the wait runs its course.
        let alone = logs_written(&Drained::Empty, &stop, Duration::from_secs(5), |_| {
            thread::sleep(Duration::from_millis(50));
            true
        });
        assert_eq!(alone, Logged::Written);
    }

    /// What ends as the workers drain — their accepting first of all — was meant to: the
    /// drain goes on until the last connection goes.
    #[test]
    fn a_drain_is_not_cut_short_by_what_ends_as_it_drains() {
        let loads = Loads::new(1, 8, QUIC_MOST, 1);
        let held = loads.hold(0, 0);
        let (stopping, stop) = mpsc::channel();
        let accepting = Ended {
            what: "worker 0's accepting on listener \"web\"".to_owned(),
            finished: true,
        };
        stopping.send(Told::Ended(accepting)).unwrap();
        let going = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            drop(held);
        });
        assert_eq!(
            drained(|| loads.now().iter().sum(), &stop, Duration::from_secs(10)),
            (Drained::Empty, None)
        );
        going.join().unwrap();
    }

    /// A watched thread that panics as the workers drain does not cut the drain short — the
    /// other workers' requests still finish — but is remembered, the first of them, for the
    /// process to fail with once the drain is done (03 §2; review A01-02, C3).
    #[test]
    fn a_panic_as_the_workers_drain_is_remembered_and_the_drain_goes_on() {
        let loads = Loads::new(1, 8, QUIC_MOST, 1);
        let held = loads.hold(0, 0);
        let (stopping, stop) = mpsc::channel();
        let panicked = |what: &str| Ended {
            what: what.to_owned(),
            finished: false,
        };
        stopping
            .send(Told::Ended(panicked("worker 1's sweep and timers")))
            .unwrap();
        stopping
            .send(Told::Ended(Ended {
                what: "worker 0's accepting on listener \"web\"".to_owned(),
                finished: true,
            }))
            .unwrap();
        stopping
            .send(Told::Ended(panicked("the health checker")))
            .unwrap();
        let going = thread::spawn(move || {
            thread::sleep(Duration::from_millis(100));
            drop(held);
        });
        let (how, first) = drained(|| loads.now().iter().sum(), &stop, Duration::from_secs(10));
        going.join().unwrap();
        assert_eq!(how, Drained::Empty);
        assert_eq!(first, Some(panicked("worker 1's sweep and timers")));
        assert_eq!(
            Failure::PanickedDraining(panicked("worker 1's sweep and timers")).to_string(),
            "worker 1's sweep and timers panicked while the data plane drained"
        );
    }

    /// The main loop looks at the config again while nothing is said, stops when a signal
    /// comes, and fails at once when something the data plane cannot do without ends.
    #[test]
    fn the_main_loop_fails_when_something_essential_ends() {
        let (tell, told) = mpsc::channel();
        assert_eq!(next(&told, Duration::from_millis(10)), Next::Look);
        tell.send(Told::Stop).unwrap();
        assert_eq!(next(&told, Duration::from_secs(5)), Next::Stop);
        let sweep = Ended {
            what: "worker 0's sweep and timers".to_owned(),
            finished: false,
        };
        tell.send(Told::Ended(sweep.clone())).unwrap();
        assert_eq!(
            next(&told, Duration::from_secs(5)),
            Next::Failed(sweep.clone())
        );
        assert_eq!(
            Failure::Ended(sweep).to_string(),
            "worker 0's sweep and timers panicked: the data plane stops, to be started afresh"
        );
    }

    /// The rate-limiting thread failing, as the proxy tells it, fails the main loop as a
    /// worker's sweep does.
    #[test]
    fn the_rate_limiting_thread_failing_fails_the_main_loop() {
        let (tell, told) = mpsc::channel();
        rate_limits_failed(&tell)();
        let Next::Failed(ended) = next(&told, Duration::from_secs(5)) else {
            panic!("the main loop goes on");
        };
        assert_eq!(
            Failure::Ended(ended).to_string(),
            "the rate-limiting thread panicked: the data plane stops, to be started afresh"
        );
    }
}
