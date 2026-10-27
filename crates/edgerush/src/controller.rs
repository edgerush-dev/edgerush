//! `edgerush controller`: the command line around `edgerush_controller`
//! ([04](../../../docs/04-control-plane.md)) — its arguments, the data plane's settings,
//! the API server found as a pod finds it, and what is said of each translation. Until the
//! config stream reaches data planes, a translation is said as `validate` says it, and with
//! `--write-config` written to a file.

use crate::validate::{described, read};
use edgerush_controller::{Debounce, Said, control};
use edgerush_kubernetes::{ApiError, ApiServer, Scope};
use edgerush_translate::{Plane, Translation};
use std::fmt::Write as _;
use std::future::{Future, poll_fn};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::pin::pin;
use tokio::runtime::Builder;

pub(crate) const USAGE: &str = "\
Usage: edgerush controller --data-plane <FILE> [--namespace <NAME>]... [--write-config <FILE>]

Runs a control plane for one data plane, in a cluster: follows the IngressClasses,
Ingresses, Services and Secrets of its scope through the API server, found as a pod finds
it, and translates them each time they change, once every kind has been listed whole. What
each translation came to is said on stderr, as `edgerush validate` says it.
Runs until Ctrl-C, or SIGTERM on Unix, and then exits 0; exits 2 when the command line or
the data plane's settings cannot be used, or it is not in a cluster.

Options:
      --data-plane <FILE>    The data plane's settings, in YAML, as `edgerush validate`
                             reads them
      --namespace <NAME>     A namespace to watch, given once for each; left out, the
                             whole cluster. IngressClasses are the cluster's, and are
                             followed in it either way
      --write-config <FILE>  Write each config translated to FILE, replacing it whole:
                             YAML as `edgerush proxy --config` reads it, every field
                             stated, without the certificates it names or the
                             rate-limiting engine's limits, which a config file never
                             holds
  -h, --help                 Print help
";

/// Runs `edgerush controller` with the arguments after its name. Returns the exit status.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let options = match parse(args) {
        Ok(Parsed::Help) => return write!(stdout, "{USAGE}").map_or(1, |()| 0),
        Ok(Parsed::Control(options)) => options,
        Err(error) => {
            return write!(stderr, "error: {error}\n\n{USAGE}").map_or(1, |()| crate::EXIT_USAGE);
        }
    };
    let started = read::<Plane>(&options.data_plane)
        .map_err(|failed| failed.to_string())
        .and_then(|plane| {
            let api = ApiServer::in_cluster().map_err(|error| outside(&error))?;
            Ok((plane, api))
        });
    let (plane, api) = match started {
        Ok(started) => started,
        Err(failure) => {
            return writeln!(stderr, "error: {failure}").map_or(1, |()| crate::EXIT_USAGE);
        }
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return writeln!(stderr, "error: {error}").map_or(1, |()| 1),
    };
    let stopped = tokio::task::LocalSet::new().block_on(
        &runtime,
        control_until(api, options, plane, told_to_stop(), |line: &str| {
            let _nobody_reads = writeln!(io::stderr(), "{line}");
        }),
    );
    match stopped {
        Ok(()) => 0,
        Err(error) => writeln!(stderr, "error: {error}").map_or(1, |()| 1),
    }
}

/// Why a control plane cannot run here.
fn outside(error: &ApiError) -> String {
    format!("the API server cannot be reached as a pod reaches it: {error}")
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Control(Options),
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    data_plane: PathBuf,
    /// The scope: its namespaces, or the cluster.
    scope: Vec<Scope>,
    write_config: Option<PathBuf>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'{0}' is required")]
    Required(&'static str),
    #[error("namespace '{0}' is given twice")]
    NamespaceTwice(String),
    #[error("'--namespace {0}': {1}")]
    Namespace(String, ApiError),
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut data_plane = None;
    let mut write_config = None;
    let mut scope = Vec::new();
    while let Some(arg) = args.next() {
        let option = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--data-plane" => "--data-plane",
            "--write-config" => "--write-config",
            "--namespace" => "--namespace",
            _ => return Err(UsageError::Unexpected(arg)),
        };
        let value = args.next().ok_or(UsageError::NoValue(option))?;
        let slot = match option {
            "--namespace" => {
                let namespace = Scope::namespace(&value)
                    .map_err(|error| UsageError::Namespace(value.clone(), error))?;
                if scope.contains(&namespace) {
                    return Err(UsageError::NamespaceTwice(value));
                }
                scope.push(namespace);
                continue;
            }
            "--data-plane" => &mut data_plane,
            _ => &mut write_config,
        };
        if slot.replace(PathBuf::from(value)).is_some() {
            return Err(UsageError::Twice(option));
        }
    }
    if scope.is_empty() {
        scope.push(Scope::Cluster);
    }
    Ok(Parsed::Control(Options {
        data_plane: data_plane.ok_or(UsageError::Required("--data-plane <FILE>"))?,
        scope,
        write_config,
    }))
}

/// Runs the control plane through `api` until `stop` is done, each line of what it says
/// given to `say`.
async fn control_until(
    api: ApiServer,
    options: Options,
    plane: Plane,
    stop: impl Future<Output = io::Result<()>>,
    say: impl Fn(&str) + 'static,
) -> io::Result<()> {
    let write_config = options.write_config;
    let scope = options.scope;
    let controlling = tokio::task::spawn_local(async move {
        control(api, &scope, &plane, Debounce::default(), move |said| {
            say(&told(&said, write_config.as_deref()));
        })
        .await;
    });
    let stopped = stop.await;
    controlling.abort();
    stopped
}

/// What is said of `said`, the translation written to `write_config` first if it is one.
fn told(said: &Said<'_>, write_config: Option<&Path>) -> String {
    match said {
        Said::Waiting(unlisted) => {
            format!("waiting to have listed {}", unlisted.join(", "))
        }
        Said::Failed { followed, failure } => format!("{followed}: {failure}"),
        Said::Translated(translation) => {
            let mut text = described(translation);
            if let Some(path) = write_config {
                // Writing to a `String` cannot fail.
                let _ = match written(translation, path) {
                    Ok(()) => write!(text, "written to {}", path.display()),
                    Err(error) => {
                        write!(text, "error: {} cannot be written: {error}", path.display())
                    }
                };
            }
            text.trim_end().to_owned()
        }
        Said::Refused {
            translation,
            problems,
        } => {
            let mut text = described(translation);
            let _ = write!(
                text,
                "the data plane would refuse the config, which is not handed on:"
            );
            for problem in *problems {
                let _ = write!(text, "\n  {problem}");
            }
            text
        }
    }
}

/// Writes `translation`'s config to `path` whole: into a file beside it, then put in its
/// place, so that nothing reading `path` ever finds half of one.
fn written(translation: &Translation, path: &Path) -> io::Result<()> {
    let yaml = serde_saphyr::to_string(&translation.config).map_err(io::Error::other)?;
    let mut beside = path.as_os_str().to_owned();
    beside.push(".partial");
    std::fs::write(&beside, yaml)?;
    std::fs::rename(&beside, path)
}

/// Done when the process is told to stop: Ctrl-C, or on Unix SIGTERM, which is how
/// Kubernetes asks.
async fn told_to_stop() -> io::Result<()> {
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut interrupt = pin!(tokio::signal::ctrl_c());
    poll_fn(|cx| {
        #[cfg(unix)]
        if terminate.poll_recv(cx).is_ready() {
            return std::task::Poll::Ready(Ok(()));
        }
        interrupt.as_mut().poll(cx)
    })
    .await
}

#[cfg(test)]
mod tests;
