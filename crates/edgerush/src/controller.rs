//! `edgerush controller`: the command line around `edgerush_controller`
//! ([04](../../../docs/04-control-plane.md)) — its arguments, the data plane's settings,
//! the API server found as a pod finds it, the config stream it serves, and what is said of
//! each translation and each data plane. A translation is said as `validate` says it, and
//! with `--write-config` written to a file.

use crate::validate::{described, read};
use edgerush_control_service::{Credentials, Files, Heard, SslAcceptor, acceptor, reread, serve};
use edgerush_control_stream::{Offer, OfferError, Versions};
use edgerush_controller::{Debounce, Said, control};
use edgerush_kubernetes::{ApiError, ApiServer, Scope};
use edgerush_translate::{Plane, Translation};
use std::cell::RefCell;
use std::fmt::Write as _;
use std::future::{Future, poll_fn};
use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot, watch};

pub(crate) const USAGE: &str = "\
Usage: edgerush controller --data-plane <FILE> [--namespace <NAME>]... [--write-config <FILE>]
         [--listen <ADDRESS> --certificate <FILE> --key <FILE> --authorities <FILE>]

Runs a control plane for one data plane, in a cluster: follows the IngressClasses,
Ingresses, Services and Secrets of its scope through the API server, found as a pod finds
it, and translates them each time they change, once every kind has been listed whole. What
each translation came to is said on stderr, as `edgerush validate` says it.
With --listen, it serves the config stream there, gRPC over mutual TLS that data planes
dial: each config translated is offered to every data plane, and what each says of it is
said on stderr. The certificate, key and authorities are PEM files, read again every
second, so that ones replaced serve the next connection.
Runs until Ctrl-C, or SIGTERM on Unix, and then exits 0; exits 2 when the command line, the
data plane's settings or the stream's certificates cannot be used, or it is not in a
cluster.

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
      --listen <ADDRESS>     Serve the config stream on ADDRESS, such as [::]:9443; given
                             with the three below
      --certificate <FILE>   The control plane's certificate on the stream, then its
                             intermediates
      --key <FILE>           Its private key
      --authorities <FILE>   The stream's authorities, which every data plane's
                             certificate must be signed by
  -h, --help                 Print help
";

/// How often the stream's certificate files are read again.
const REREAD: Duration = Duration::from_secs(1);

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
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return writeln!(stderr, "error: {error}").map_or(1, |()| 1),
    };
    let started = read::<Plane>(&options.data_plane)
        .map_err(|failed| failed.to_string())
        .and_then(|plane| {
            let stream = match &options.stream {
                Some(stream) => Some(runtime.block_on(credentials(stream))?),
                None => None,
            };
            let api = ApiServer::in_cluster().map_err(|error| outside(&error))?;
            Ok((plane, stream, api))
        });
    let (plane, credentials, api) = match started {
        Ok(started) => started,
        Err(failure) => {
            return writeln!(stderr, "error: {failure}").map_or(1, |()| crate::EXIT_USAGE);
        }
    };
    let serving = match (&options.stream, credentials) {
        (Some(stream), Some(credentials)) => {
            let listener = match std::net::TcpListener::bind(stream.listen) {
                Ok(listener) => listener,
                Err(error) => {
                    return writeln!(stderr, "error: {}: {error}", stream.listen).map_or(1, |()| 1);
                }
            };
            Some(Serving {
                listener,
                files: stream.files.clone(),
                credentials,
                reread: REREAD,
                versions: Versions::new(started_at()),
            })
        }
        _ => None,
    };
    let stopped = tokio::task::LocalSet::new().block_on(
        &runtime,
        control_until(
            api,
            options,
            plane,
            serving,
            told_to_stop(),
            |line: &str| {
                let _nobody_reads = writeln!(io::stderr(), "{line}");
            },
        ),
    );
    match stopped {
        Ok(()) => 0,
        Err(error) => writeln!(stderr, "error: {error}").map_or(1, |()| 1),
    }
}

/// When this control plane started, in nanoseconds since the Unix epoch, which marks the
/// versions it names: a clock before the epoch counts from it.
fn started_at() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        })
}

/// The stream's credentials, read and checked to be usable.
async fn credentials(stream: &Stream) -> Result<Credentials, String> {
    let failed = |error: String| format!("the stream's certificates: {error}");
    let credentials = stream
        .files
        .read()
        .await
        .map_err(|error| failed(error.to_string()))?;
    acceptor(&credentials).map_err(|error| failed(error.to_string()))?;
    Ok(credentials)
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
    stream: Option<Box<Stream>>,
}

/// The config stream, as the command line gives it.
#[derive(Debug, PartialEq, Eq)]
struct Stream {
    listen: SocketAddr,
    files: Files,
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
    #[error("'--listen {0}' is not an address and a port, such as [::]:9443")]
    Listen(String),
    #[error("'--listen', '--certificate', '--key' and '--authorities' are given together")]
    Together,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut data_plane = None;
    let mut write_config = None;
    let mut listen = None;
    let mut certificate = None;
    let mut key = None;
    let mut authorities = None;
    let mut scope = Vec::new();
    while let Some(arg) = args.next() {
        let option = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--data-plane" => "--data-plane",
            "--write-config" => "--write-config",
            "--namespace" => "--namespace",
            "--listen" => "--listen",
            "--certificate" => "--certificate",
            "--key" => "--key",
            "--authorities" => "--authorities",
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
            "--listen" => {
                let address = value.parse().map_err(|_| UsageError::Listen(value))?;
                if listen.replace(address).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--data-plane" => &mut data_plane,
            "--certificate" => &mut certificate,
            "--key" => &mut key,
            "--authorities" => &mut authorities,
            _ => &mut write_config,
        };
        if slot.replace(PathBuf::from(value)).is_some() {
            return Err(UsageError::Twice(option));
        }
    }
    if scope.is_empty() {
        scope.push(Scope::Cluster);
    }
    let stream = match (listen, certificate, key, authorities) {
        (Some(listen), Some(certificate), Some(key), Some(authorities)) => Some(Box::new(Stream {
            listen,
            files: Files {
                certificate,
                key,
                authorities,
            },
        })),
        (None, None, None, None) => None,
        _ => return Err(UsageError::Together),
    };
    Ok(Parsed::Control(Options {
        data_plane: data_plane.ok_or(UsageError::Required("--data-plane <FILE>"))?,
        scope,
        write_config,
        stream,
    }))
}

/// The config stream, ready to be served.
struct Serving {
    listener: std::net::TcpListener,
    files: Files,
    /// What the files held as it started.
    credentials: Credentials,
    /// How often the files are read again.
    reread: Duration,
    versions: Versions,
}

/// Runs the control plane through `api` until `stop` is done, serving the config stream
/// too if `serving`, each line of what it says given to `say`.
async fn control_until(
    api: ApiServer,
    options: Options,
    plane: Plane,
    serving: Option<Serving>,
    stop: impl Future<Output = io::Result<()>>,
    say: impl Fn(&str) + 'static,
) -> io::Result<()> {
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let write_config = options.write_config;
    let scope = options.scope;
    let mut tasks = Vec::new();
    let mut served = None;
    let offering = match serving {
        Some(serving) => {
            let (offers, offered) = watch::channel(None);
            let (stopping, stopped) = oneshot::channel::<()>();
            let tls = watch::Sender::new(Arc::new(
                acceptor(&serving.credentials).map_err(io::Error::other)?,
            ));
            serving.listener.set_nonblocking(true)?;
            let listener = tokio::net::TcpListener::from_std(serving.listener)?;
            // Heard on the server's own tasks, and said here, where `say` lives.
            let (heard, mut hearing) = mpsc::unbounded_channel();
            served = Some((
                stopping,
                tokio::spawn(serve(
                    listener,
                    tls.subscribe(),
                    offered,
                    move |peer, said| {
                        let _stopped = heard.send(told_of(peer, &said));
                    },
                    async move {
                        let _ = stopped.await;
                    },
                )),
            ));
            let telling = Rc::clone(&say);
            tasks.push(tokio::task::spawn_local(async move {
                while let Some(line) = hearing.recv().await {
                    telling(&line);
                }
            }));
            let telling = Rc::clone(&say);
            tasks.push(tokio::task::spawn_local(reread(
                serving.files,
                serving.credentials,
                serving.reread,
                move |changed| telling(&replaced(changed, &tls)),
            )));
            Some(Offering {
                versions: RefCell::new(serving.versions),
                offers,
            })
        }
        None => None,
    };
    let telling = Rc::clone(&say);
    tasks.push(tokio::task::spawn_local(async move {
        control(api, &scope, &plane, Debounce::default(), move |said| {
            let mut text = told(&said, write_config.as_deref());
            if let (Said::Translated(translation), Some(offering)) = (&said, &offering) {
                // Writing to a `String` cannot fail.
                let _ = match offering.offer(translation) {
                    Ok(version) => write!(text, "\noffered as config {version}"),
                    Err(error) => write!(text, "\nnot offered: {error}"),
                };
            }
            telling(&text);
        })
        .await;
    }));
    let stopped = stop.await;
    if let Some((stopping, server)) = served {
        let _ = stopping.send(());
        if let Ok(Err(error)) = server.await {
            say(&format!("error: {error}"));
        }
    }
    for task in tasks {
        task.abort();
    }
    stopped
}

/// The latest offer, as data planes are sent it.
struct Offering {
    versions: RefCell<Versions>,
    offers: watch::Sender<Option<Arc<Offer>>>,
}

impl Offering {
    /// Offers `translation`'s config to every data plane: its version, said as offered.
    fn offer(&self, translation: &Translation) -> Result<String, OfferError> {
        let last = self.offers.borrow().clone();
        let offer = Offer::new(
            &translation.config,
            last.as_deref(),
            &mut self.versions.borrow_mut(),
        )?;
        let version = offer.version().to_owned();
        self.offers.send_replace(Some(Arc::new(offer)));
        Ok(version)
    }
}

/// What is said of the stream's certificate files changing: the new ones put to use for the
/// next connection, or kept from it if they cannot be used.
fn replaced(
    changed: Result<&Credentials, &edgerush_control_service::FilesError>,
    tls: &watch::Sender<Arc<SslAcceptor>>,
) -> String {
    let kept = "and the last are kept";
    match changed.map(acceptor) {
        Ok(Ok(replaced)) => {
            tls.send_replace(Arc::new(replaced));
            "the stream's certificates are replaced".to_owned()
        }
        Ok(Err(error)) => format!("the stream's certificates cannot be used, {kept}: {error}"),
        Err(error) => format!("the stream's certificates cannot be read, {kept}: {error}"),
    }
}

/// What is said of what was heard of the data plane at `peer`.
fn told_of(peer: SocketAddr, heard: &Heard) -> String {
    let what = match heard {
        Heard::Refused(why) => format!("refused in the handshake: {why}"),
        Heard::Opened {
            held,
            version,
            written,
        } => {
            let holding = held.config.as_deref().unwrap_or("no config");
            if version == written {
                format!("opened at config version {version}, holding {holding}")
            } else {
                format!("opened at config version {version}, written {written}, holding {holding}")
            }
        }
        Heard::Reported(report) => {
            let running = report.running.config.as_deref().unwrap_or("no config");
            match &report.refused {
                Some(refused) => format!(
                    "refused config {}: {}; runs {running}",
                    refused.config, refused.why
                ),
                None => format!("runs config {running}"),
            }
        }
        Heard::Closed(None) => "closed the stream".to_owned(),
        Heard::Closed(Some(why)) => format!("the stream ended: {why}"),
    };
    format!("data plane {peer}: {what}")
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
