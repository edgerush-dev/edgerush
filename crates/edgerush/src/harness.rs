//! `edgerush proxy`: a data plane run from a config file, without Kubernetes. A harness for
//! development and tests — in a cluster the config comes from the control plane.
//!
//! The file is the config model as it is, nothing added; what is about the process and not
//! about routing (where `/metrics` is served) is on the command line. Listeners are bound
//! once, at the start. After that the file is read again every [`POLL`] and a config that
//! has changed takes over without dropping a request, while one that cannot be run is
//! told and changes nothing.
//!
//! Both threading models that were measured against each other can be run
//! ([`Threading`]). Thread-per-core is the one that was chosen, here still as the
//! experiment made of what there was ([`crate::per_core`]): a whole data plane for every
//! worker, which is what gives each its own upstream connections.

use crate::bind::{Port, listen};
use crate::config_file::{ConfigFile, Rejected};
use crate::per_core::{self, Accept};
use edgerush_config::Compiled;
use edgerush_proxy::{Proxy, ProxyError};
use std::convert::Infallible;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Builder;

pub(crate) const USAGE: &str = "\
Usage: edgerush proxy --config <FILE> [OPTIONS]

Runs a data plane from a config file, without Kubernetes: a harness for development.
The file is read again every second, and a changed config takes over without dropping a
request. Listeners are bound once: one that is new or has moved takes a restart.

Options:
      --config <FILE>      The config, in YAML
      --metrics <ADDRESS>  Serve /metrics there, as in 127.0.0.1:9090 [default: nowhere]
      --threading <MODEL>  work-stealing: one runtime whose workers share everything;
                           thread-per-core: for every worker a runtime, sockets on shared
                           ports (SO_REUSEPORT) and upstream connections of its own, and
                           no /metrics [default: work-stealing]
      --accept <BY>        With thread-per-core, whose a new connection is. balanced: the
                           worker that holds the fewest; kernel: the one the kernel gave
                           it to, by its hash of the addresses [default: balanced]
      --workers <N>        How many threads serve requests [default: one for every CPU]
  -h, --help               Print help
";

/// How often the file is looked at. It is a small file, read by the thread that has
/// nothing else to do.
const POLL: Duration = Duration::from_secs(1);

/// Runs `edgerush proxy` with the arguments after its name. Returns the exit status, and
/// only when there is nothing to run or no way to: a data plane runs until it is killed.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match parse(args) {
        Ok(Parsed::Help) => write!(stdout, "{USAGE}").map(|()| 0),
        Ok(Parsed::Run(options)) => match run(options, stderr) {
            Err(failure) => writeln!(stderr, "error: {failure}").map(|()| 1),
        },
        Err(error) => write!(stderr, "error: {error}\n\n{USAGE}").map(|()| crate::EXIT_USAGE),
    };
    written.unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Run(Options),
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    config: PathBuf,
    metrics: Option<SocketAddr>,
    threading: Threading,
    /// `None` is one for every CPU the process may use.
    workers: Option<NonZeroUsize>,
    accept: Accept,
}

/// How the threads that serve requests share the work ([03 §2] in the docs).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Threading {
    /// One runtime: connections are accepted in one place and their tasks move to
    /// whichever worker has time. One data plane, one pool of upstream connections.
    WorkStealing,
    /// A single-threaded runtime for every worker, each with a socket of its own on
    /// every listener's port and a data plane of its own: a connection stays with the
    /// worker the kernel gave it to, and so do the upstream connections it uses.
    ThreadPerCore,
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
    #[error("'{0}' is not a threading model: work-stealing or thread-per-core")]
    Threading(String),
    #[error("'{0}' is not a number of workers: 1 or more")]
    Workers(String),
    #[error("'--metrics' is not served with thread-per-core, where every worker counts alone")]
    MetricsPerCore,
    #[error("'{0}' is not a way to place connections: balanced or kernel")]
    Accept(String),
    #[error("'--accept' is for thread-per-core: work-stealing has all connections in one place")]
    AcceptPerCore,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut config = None;
    let mut metrics = None;
    let mut threading = None;
    let mut workers = None;
    let mut accept = None;
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
            "--threading" => {
                let model = args.next().ok_or(UsageError::NoValue("--threading"))?;
                let model = match model.as_str() {
                    "work-stealing" => Threading::WorkStealing,
                    "thread-per-core" => Threading::ThreadPerCore,
                    _ => return Err(UsageError::Threading(model)),
                };
                if threading.replace(model).is_some() {
                    return Err(UsageError::Twice("--threading"));
                }
            }
            "--workers" => {
                let count = args.next().ok_or(UsageError::NoValue("--workers"))?;
                let count = count.parse().map_err(|_| UsageError::Workers(count))?;
                if workers.replace(count).is_some() {
                    return Err(UsageError::Twice("--workers"));
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
    let threading = threading.unwrap_or(Threading::WorkStealing);
    if threading == Threading::ThreadPerCore && metrics.is_some() {
        return Err(UsageError::MetricsPerCore);
    }
    if threading == Threading::WorkStealing && accept.is_some() {
        return Err(UsageError::AcceptPerCore);
    }
    Ok(Parsed::Run(Options {
        config,
        metrics,
        threading,
        workers,
        accept: accept.unwrap_or(Accept::Balanced),
    }))
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

fn run(options: Options, stderr: &mut impl Write) -> Result<Infallible, Failure> {
    let Options {
        config: path,
        metrics,
        threading,
        workers,
        accept,
    } = options;
    let workers = workers
        .or_else(|| thread::available_parallelism().ok())
        .unwrap_or(NonZeroUsize::MIN);
    let (data_planes, port) = match threading {
        Threading::WorkStealing => (NonZeroUsize::MIN, Port::Own),
        Threading::ThreadPerCore => (workers, Port::Shared),
    };

    let (mut file, configs) = match ConfigFile::open(path.clone(), data_planes) {
        Ok(opened) => opened,
        Err(rejected) => return Err(Failure::Config { path, rejected }),
    };
    let mut bound: Vec<Bound> = configs
        .first()
        .map(|config| config.listeners.iter().map(Bound::from).collect())
        .unwrap_or_default();
    let proxies = configs
        .into_iter()
        .map(|config| Proxy::new(config).map(Arc::new))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| Failure::Proxy { path, error })?;

    // Every socket is open before a request is served on any, so that a port that is
    // taken stops a harness that has done nothing yet.
    let mut sockets = Vec::with_capacity(proxies.len());
    for _ in &proxies {
        let mut of_this_one = Vec::with_capacity(bound.len());
        for listener in &mut bound {
            let what = format!("listener \"{}\"", listener.name);
            let socket = open(what, listener.listens_on, port)?;
            // A port that the operating system chose for the first data plane is the
            // port of those after it.
            listener.listens_on = socket.local_addr().unwrap_or(listener.listens_on);
            of_this_one.push(socket);
        }
        sockets.push(of_this_one);
    }
    let metrics = metrics
        .map(|address| open("metrics".to_owned(), address, Port::Own).map(|s| (s, address)))
        .transpose()?;

    for listener in &bound {
        say(
            stderr,
            format_args!(
                "listener \"{}\" is on {}",
                listener.name, listener.listens_on
            ),
        );
    }
    // A runtime with workers of its own runs by being there, for as long as it is there.
    let _running = match threading {
        Threading::WorkStealing => {
            let runtime = Builder::new_multi_thread()
                .worker_threads(workers.get())
                .enable_all()
                .build()
                .map_err(Failure::Runtime)?;
            // Sockets are handed to the runtime that is entered.
            let entered = runtime.enter();
            for (proxy, sockets) in proxies.iter().zip(sockets) {
                for (position, socket) in sockets.into_iter().enumerate() {
                    let socket = TcpListener::from_std(socket).map_err(Failure::Runtime)?;
                    runtime.spawn(Arc::clone(proxy).serve(position, socket));
                }
            }
            if let (Some((socket, address)), Some(proxy)) = (metrics, proxies.first()) {
                let address = socket.local_addr().unwrap_or(address);
                let socket = TcpListener::from_std(socket).map_err(Failure::Runtime)?;
                runtime.spawn(Arc::clone(proxy).serve_metrics(socket));
                say(stderr, format_args!("metrics are on {address}"));
            }
            drop(entered);
            Some(runtime)
        }
        Threading::ThreadPerCore => {
            per_core::start(&proxies, sockets, accept).map_err(Failure::Runtime)?;
            None
        }
    };
    say(
        stderr,
        format_args!("{} workers, {}", workers, threading.name()),
    );

    loop {
        thread::sleep(POLL);
        let Some(changed) = file.changed() else {
            continue;
        };
        let configs = match changed {
            Ok(configs) => configs,
            Err(rejected) => {
                say(
                    stderr,
                    format_args!("config rejected, the one before it runs on:\n{rejected}"),
                );
                continue;
            }
        };
        let warnings = configs
            .first()
            .map(|config| restart_needed(&bound, config))
            .unwrap_or_default();
        // What a data plane refuses is in the config, so the first refuses what any would.
        let reloaded = proxies
            .iter()
            .zip(configs)
            .try_for_each(|(proxy, config)| proxy.reload(config));
        match reloaded {
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
}

impl Threading {
    fn name(self) -> &'static str {
        match self {
            Self::WorkStealing => "work-stealing",
            Self::ThreadPerCore => "thread-per-core",
        }
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
}

impl From<&edgerush_config::CompiledListener> for Bound {
    fn from(listener: &edgerush_config::CompiledListener) -> Self {
        Self {
            name: listener.name.clone(),
            address: listener.address,
            listens_on: listener.address,
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
            threading: Threading::WorkStealing,
            workers: None,
            accept: Accept::Balanced,
        };
        assert_eq!(parsed(&["--config", "dev.yaml"]), Ok(Parsed::Run(options)));
    }

    #[test]
    fn metrics_are_served_where_the_command_line_says() {
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: Some("[::]:9090".parse().unwrap()),
            threading: Threading::WorkStealing,
            workers: None,
            accept: Accept::Balanced,
        };
        assert_eq!(
            parsed(&["--metrics", "[::]:9090", "--config", "dev.yaml"]),
            Ok(Parsed::Run(options))
        );
    }

    #[test]
    fn the_threading_model_and_the_workers_are_as_the_command_line_says() {
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: None,
            threading: Threading::ThreadPerCore,
            workers: NonZeroUsize::new(4),
            accept: Accept::Balanced,
        };
        let args = ["--config", "dev.yaml", "--workers", "4"];
        assert_eq!(
            parsed(&[&args[..], &["--threading", "thread-per-core"]].concat()),
            Ok(Parsed::Run(options))
        );
        let Ok(Parsed::Run(options)) =
            parsed(&[&args[..], &["--threading", "work-stealing"]].concat())
        else {
            panic!("a command line that can be run");
        };
        assert_eq!(options.threading, Threading::WorkStealing);
    }

    #[test]
    fn connections_are_balanced_unless_they_are_left_to_the_kernel() {
        let per_core = ["--config", "dev.yaml", "--threading", "thread-per-core"];
        let Ok(Parsed::Run(options)) = parsed(&[&per_core[..], &["--accept", "kernel"]].concat())
        else {
            panic!("a command line that can be run");
        };
        assert_eq!(options.accept, Accept::Kernel);
        assert_eq!(
            parsed(&[&per_core[..], &["--accept", "luck"]].concat()),
            Err(UsageError::Accept("luck".to_owned()))
        );
        assert_eq!(
            parsed(&["--config", "dev.yaml", "--accept", "balanced"]),
            Err(UsageError::AcceptPerCore)
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
            parsed(&["--config", "a.yaml", "--metrics", "localhost:9090"]),
            Err(UsageError::Address("localhost:9090".to_owned()))
        );
        assert_eq!(
            parsed(&["a.yaml"]),
            Err(UsageError::Unexpected("a.yaml".to_owned()))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml", "--threading", "fibers"]),
            Err(UsageError::Threading("fibers".to_owned()))
        );
        for workers in ["0", "-1", "many"] {
            assert_eq!(
                parsed(&["--config", "a.yaml", "--workers", workers]),
                Err(UsageError::Workers(workers.to_owned()))
            );
        }
        assert_eq!(
            parsed(&[
                "--config",
                "a.yaml",
                "--threading",
                "thread-per-core",
                "--metrics",
                "[::]:9090"
            ]),
            Err(UsageError::MetricsPerCore)
        );
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
            yaml += &format!("  {name}: {{ address: \"{address}\", protocol: http }}\n");
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
}
