//! `edgerush proxy`: a data plane run from a config file, without Kubernetes. A harness for
//! development and tests — in a cluster the config comes from the control plane.
//!
//! The file is the config model as it is, nothing added; what is about the process and not
//! about routing (where `/metrics` is served) is on the command line. Listeners are bound
//! once, at the start. After that the file is read again every [`POLL`] and a config that
//! has changed takes over without dropping a request, while one that cannot be run is
//! told and changes nothing.

use crate::bind::listen;
use crate::config_file::{ConfigFile, Rejected};
use edgerush_config::Compiled;
use edgerush_proxy::{Proxy, ProxyError};
use std::convert::Infallible;
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;

pub(crate) const USAGE: &str = "\
Usage: edgerush proxy --config <FILE> [--metrics <ADDRESS>]

Runs a data plane from a config file, without Kubernetes: a harness for development.
The file is read again every second, and a changed config takes over without dropping a
request. Listeners are bound once: one that is new or has moved takes a restart.

Options:
      --config <FILE>      The config, in YAML
      --metrics <ADDRESS>  Serve /metrics there, as in 127.0.0.1:9090 [default: nowhere]
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
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut config = None;
    let mut metrics = None;
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
            _ => return Err(UsageError::Unexpected(arg)),
        }
    }
    let config = config.ok_or(UsageError::NoConfig)?;
    Ok(Parsed::Run(Options { config, metrics }))
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
    } = options;
    let (mut file, config) = match ConfigFile::open(path.clone()) {
        Ok(opened) => opened,
        Err(rejected) => return Err(Failure::Config { path, rejected }),
    };
    let bound: Vec<Bound> = config.listeners.iter().map(Bound::from).collect();
    let proxy = Proxy::new(config).map_err(|error| Failure::Proxy { path, error })?;
    let proxy = Arc::new(proxy);

    // Every socket is open before a request is served on any, so that a port that is
    // taken stops a harness that has done nothing yet.
    let mut sockets = Vec::with_capacity(bound.len());
    for listener in &bound {
        let what = format!("listener \"{}\"", listener.name);
        sockets.push(open(what, listener.address)?);
    }
    let metrics = metrics
        .map(|address| open("metrics".to_owned(), address).map(|socket| (socket, address)))
        .transpose()?;

    let runtime = Runtime::new().map_err(Failure::Runtime)?;
    // Sockets are handed to the runtime that is entered.
    let entered = runtime.enter();
    for (position, (listener, socket)) in bound.iter().zip(sockets).enumerate() {
        let (socket, address) = registered(socket, listener.address)?;
        say(
            stderr,
            format_args!("listener \"{}\" is on {address}", listener.name),
        );
        runtime.spawn(Arc::clone(&proxy).serve(position, socket));
    }
    if let Some((socket, address)) = metrics {
        let (socket, address) = registered(socket, address)?;
        say(stderr, format_args!("metrics are on {address}"));
        runtime.spawn(Arc::clone(&proxy).serve_metrics(socket));
    }
    drop(entered);

    loop {
        thread::sleep(POLL);
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
}

/// A listener that has a socket: its name and the address the config gave it then.
#[derive(Debug, PartialEq, Eq)]
struct Bound {
    name: String,
    address: SocketAddr,
}

impl From<&edgerush_config::CompiledListener> for Bound {
    fn from(listener: &edgerush_config::CompiledListener) -> Self {
        Self {
            name: listener.name.clone(),
            address: listener.address,
        }
    }
}

fn open(what: String, address: SocketAddr) -> Result<std::net::TcpListener, Failure> {
    listen(address).map_err(|error| Failure::Listen {
        what,
        address,
        error,
    })
}

/// The socket in the runtime's hands, and the address it really has: the config may have
/// left the port to the operating system.
fn registered(
    socket: std::net::TcpListener,
    asked_for: SocketAddr,
) -> Result<(TcpListener, SocketAddr), Failure> {
    let address = socket.local_addr().unwrap_or(asked_for);
    let socket = TcpListener::from_std(socket).map_err(Failure::Runtime)?;
    Ok((socket, address))
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
        };
        assert_eq!(parsed(&["--config", "dev.yaml"]), Ok(Parsed::Run(options)));
    }

    #[test]
    fn metrics_are_served_where_the_command_line_says() {
        let options = Options {
            config: PathBuf::from("dev.yaml"),
            metrics: Some("[::]:9090".parse().unwrap()),
        };
        assert_eq!(
            parsed(&["--metrics", "[::]:9090", "--config", "dev.yaml"]),
            Ok(Parsed::Run(options))
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
