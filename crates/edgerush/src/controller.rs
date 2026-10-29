//! `edgerush controller`: the command line around `edgerush_controller`
//! ([04](../../../docs/04-control-plane.md)) — its arguments, the data plane's settings,
//! the API server found as a pod finds it, the config stream and the admission webhook it
//! serves, and what is said of each translation and each data plane. A translation is said
//! as `validate` says it, and with `--write-config` written to a file.

use crate::validate::{described, read};
use edgerush_config::Certificate;
use edgerush_control_service::webhook::{self, Bounds};
use edgerush_control_service::{
    CertificateFiles, Credentials, Files, FilesError, Heard, SslAcceptor, acceptor, reread, serve,
    webhook_acceptor,
};
use edgerush_control_stream::{Offer, OfferError, Versions};
use edgerush_controller::admission::{self, Asked};
use edgerush_controller::lease::{self, elect};
use edgerush_controller::status::{Addresses, address};
use edgerush_controller::{Debounce, Said, control};
use edgerush_kubernetes::{ApiError, ApiServer, Listed, Retry, Scope, follow};
use edgerush_translate::{Plane, Translation};
use http::StatusCode;
use k8s_openapi::api::networking::v1::IngressClass;
use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::future::{Future, poll_fn};
use std::io::{self, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::runtime::Builder;
use tokio::sync::{mpsc, oneshot, watch};

pub(crate) const USAGE: &str = "\
Usage: edgerush controller --data-plane <FILE> [--namespace <NAME>]... [--write-config <FILE>]
         [--listen <ADDRESS> --certificate <FILE> --key <FILE> --authorities <FILE>
          [--own-name <NAME>]...]
         [--status-service <NAMESPACE>/<NAME> | --status-address <ADDRESS>...]
         [--leader-lease <NAMESPACE>/<NAME>]
         [--webhook-listen <ADDRESS> --webhook-certificate <FILE> --webhook-key <FILE>]

Runs a control plane for one data plane, in a cluster: follows the IngressClasses,
Ingresses, Services and Secrets of its scope through the API server, found as a pod finds
it, and translates them each time they change, once every kind has been listed whole. What
each translation came to is said on stderr, as `edgerush validate` says it.
With --listen, it serves the config stream there, gRPC over mutual TLS that data planes
dial: each config translated is offered to every data plane, and what each says of it is
said on stderr. The certificate, key and authorities are PEM files, read again every
second, so that ones replaced serve the next connection. With --own-name, the addresses
the names resolve to, the gateway's own pods, are sent on the stream too, for data planes
never to send a request to.
With --status-service or --status-address, each Ingress served says the data plane's
addresses in its status, and each of the class that is not says none. With --leader-lease,
the replicas elect one by that Lease, which alone writes status; every one serves.
With --webhook-listen, it serves the admission webhook there, HTTPS, reviews POSTed to
/validate: an Ingress of the data plane's class is refused where, alone, it does not
translate into something the data plane accepts. Its certificate and key are read again
every second; until the IngressClasses are listed it answers 503, which the API server
takes as an outage, admitting the Ingress where the webhook fails open.
Runs until Ctrl-C, or SIGTERM on Unix, and then exits 0; exits 2 when the command line, the
data plane's settings or the stream's or the webhook's certificates cannot be used, or it is
not in a cluster.

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
      --own-name <NAME>      A headless Service's name whose pods are the gateway's own:
                             the data planes', the control plane's or the rate-limiting
                             service's, given once for each; looked up again as each
                             answer's time to live runs out, with --listen
      --status-service <NAMESPACE>/<NAME>
                             The data plane's Service, followed: its load balancer's
                             addresses, else its external IPs, are what each Ingress
                             served says in its status
      --status-address <ADDRESS>
                             An address each Ingress served says in its status, an IP or a
                             host name, given once for each; for a cluster with no load
                             balancer, instead of --status-service
      --leader-lease <NAMESPACE>/<NAME>
                             The Lease the controller's replicas elect a leader by, which
                             alone writes status; left out, there is no election
      --webhook-listen <ADDRESS>
                             Serve the admission webhook on ADDRESS, such as [::]:8443;
                             given with the two below
      --webhook-certificate <FILE>
                             The webhook's certificate, then its intermediates
      --webhook-key <FILE>   Its private key
  -h, --help                 Print help
";

/// How often the stream's and the webhook's certificate files are read again.
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
            let webhook = match &options.webhook {
                Some(webhook) => Some(runtime.block_on(webhook_certificate(&webhook.files))?),
                None => None,
            };
            let api = ApiServer::in_cluster().map_err(|error| outside(&error))?;
            Ok((plane, stream, webhook, api))
        });
    let (plane, credentials, webhook_certificate, api) = match started {
        Ok(started) => started,
        Err(failure) => {
            return writeln!(stderr, "error: {failure}").map_or(1, |()| crate::EXIT_USAGE);
        }
    };
    // The election's own client, so that its writes never wait behind the follows'.
    let elector = match &options.leader {
        Some(_) => match ApiServer::in_cluster() {
            Ok(api) => Some(Elector {
                api,
                identity: identity(),
                timing: lease::Timing::default(),
            }),
            Err(error) => {
                return writeln!(stderr, "error: {}", outside(&error))
                    .map_or(1, |()| crate::EXIT_USAGE);
            }
        },
        None => None,
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
                own: Own {
                    names: options.own_names.clone(),
                    nameservers: crate::resolver::system_nameservers(),
                    pace: crate::resolver::Pace::default(),
                },
            })
        }
        _ => None,
    };
    let hooking = match (&options.webhook, webhook_certificate) {
        (Some(webhook), Some(certificate)) => {
            let listener = match std::net::TcpListener::bind(webhook.listen) {
                Ok(listener) => listener,
                Err(error) => {
                    return writeln!(stderr, "error: {}: {error}", webhook.listen)
                        .map_or(1, |()| 1);
                }
            };
            // The webhook's own client, so that its follow waits behind no other.
            let api = match ApiServer::in_cluster() {
                Ok(api) => api,
                Err(error) => {
                    return writeln!(stderr, "error: {}", outside(&error))
                        .map_or(1, |()| crate::EXIT_USAGE);
                }
            };
            Some(Hooking {
                api,
                listener,
                files: webhook.files.clone(),
                certificate,
                reread: REREAD,
            })
        }
        _ => None,
    };
    let stopped = tokio::task::LocalSet::new().block_on(
        &runtime,
        control_until(
            api,
            *options,
            plane,
            Listeners {
                stream: serving,
                webhook: hooking,
            },
            elector,
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

/// The webhook's certificate, read and checked to be usable.
async fn webhook_certificate(files: &CertificateFiles) -> Result<Certificate, String> {
    let failed = |error: String| format!("the webhook's certificate: {error}");
    let certificate = files
        .read()
        .await
        .map_err(|error| failed(error.to_string()))?;
    webhook_acceptor(&certificate).map_err(|error| failed(error.to_string()))?;
    Ok(certificate)
}

/// Why a control plane cannot run here.
fn outside(error: &ApiError) -> String {
    format!("the API server cannot be reached as a pod reaches it: {error}")
}

#[derive(Debug, PartialEq)]
enum Parsed {
    Help,
    Control(Box<Options>),
}

#[derive(Debug, PartialEq)]
struct Options {
    data_plane: PathBuf,
    /// The scope: its namespaces, or the cluster.
    scope: Vec<Scope>,
    write_config: Option<PathBuf>,
    stream: Option<Box<Stream>>,
    /// The names whose addresses are the gateway's own pods, sent on the stream.
    own_names: Vec<String>,
    /// Where the addresses Ingress statuses say come from, if they are written.
    status: Option<Addresses>,
    /// The Lease a leader is elected by, as its namespace and name, if one is.
    leader: Option<(String, String)>,
    webhook: Option<Box<Webhook>>,
}

/// The config stream, as the command line gives it.
#[derive(Debug, PartialEq, Eq)]
struct Stream {
    listen: SocketAddr,
    files: Files,
}

/// The admission webhook, as the command line gives it.
#[derive(Debug, PartialEq, Eq)]
struct Webhook {
    listen: SocketAddr,
    files: CertificateFiles,
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
    #[error("'--status-service {0}' is not a namespace and a name, as edge/edgerush")]
    StatusService(String),
    #[error("'--status-service' and '--status-address' are not given together")]
    Status,
    #[error("'--leader-lease {0}' is not a namespace and a name, as edge/edgerush-control")]
    LeaderLease(String),
    #[error("'--webhook-listen {0}' is not an address and a port, such as [::]:8443")]
    WebhookListen(String),
    #[error("'--webhook-listen', '--webhook-certificate' and '--webhook-key' are given together")]
    WebhookTogether,
    #[error("'--own-name {0}': {1}")]
    OwnName(String, edgerush_config::DnsNameError),
    #[error("'--own-name {0}' is given twice")]
    OwnNameTwice(String),
    #[error("'--own-name' is for the config stream, and needs '--listen'")]
    OwnNameWithoutStream,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut data_plane = None;
    let mut write_config = None;
    let mut listen = None;
    let mut certificate = None;
    let mut key = None;
    let mut authorities = None;
    let mut status_service = None;
    let mut leader = None;
    let mut webhook_listen = None;
    let mut webhook_certificate = None;
    let mut webhook_key = None;
    let mut status_addresses = Vec::new();
    let mut own_names: Vec<String> = Vec::new();
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
            "--status-service" => "--status-service",
            "--status-address" => "--status-address",
            "--leader-lease" => "--leader-lease",
            "--webhook-listen" => "--webhook-listen",
            "--webhook-certificate" => "--webhook-certificate",
            "--webhook-key" => "--webhook-key",
            "--own-name" => "--own-name",
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
            "--status-service" => {
                let named = named(&value)
                    .map(|(namespace, name)| Addresses::Service { namespace, name })
                    .ok_or_else(|| UsageError::StatusService(value.clone()))?;
                if status_service.replace(named).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--status-address" => {
                status_addresses.push(address(&value));
                continue;
            }
            "--own-name" => {
                let host = edgerush_config::DnsName::host(&value)
                    .map_err(|error| UsageError::OwnName(value.clone(), error))?;
                if own_names.contains(&host) {
                    return Err(UsageError::OwnNameTwice(value));
                }
                own_names.push(host);
                continue;
            }
            "--leader-lease" => {
                let named = named(&value).ok_or_else(|| UsageError::LeaderLease(value.clone()))?;
                if leader.replace(named).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--listen" => {
                let address = value.parse().map_err(|_| UsageError::Listen(value))?;
                if listen.replace(address).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--webhook-listen" => {
                let address = value
                    .parse()
                    .map_err(|_| UsageError::WebhookListen(value))?;
                if webhook_listen.replace(address).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--data-plane" => &mut data_plane,
            "--certificate" => &mut certificate,
            "--key" => &mut key,
            "--authorities" => &mut authorities,
            "--webhook-certificate" => &mut webhook_certificate,
            "--webhook-key" => &mut webhook_key,
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
    let webhook = match (webhook_listen, webhook_certificate, webhook_key) {
        (Some(listen), Some(certificate), Some(key)) => Some(Box::new(Webhook {
            listen,
            files: CertificateFiles { certificate, key },
        })),
        (None, None, None) => None,
        _ => return Err(UsageError::WebhookTogether),
    };
    if !own_names.is_empty() && stream.is_none() {
        return Err(UsageError::OwnNameWithoutStream);
    }
    let status = match (status_service, status_addresses.is_empty()) {
        (Some(_), false) => return Err(UsageError::Status),
        (Some(service), true) => Some(service),
        (None, false) => Some(Addresses::Stated(status_addresses)),
        (None, true) => None,
    };
    Ok(Parsed::Control(Box::new(Options {
        data_plane: data_plane.ok_or(UsageError::Required("--data-plane <FILE>"))?,
        scope,
        write_config,
        stream,
        own_names,
        status,
        leader,
        webhook,
    })))
}

/// A namespace and a name, as `<namespace>/<name>` says them.
fn named(value: &str) -> Option<(String, String)> {
    value
        .split_once('/')
        .filter(|(namespace, name)| {
            Scope::namespace(namespace).is_ok() && !name.is_empty() && !name.contains('/')
        })
        .map(|(namespace, name)| (namespace.to_owned(), name.to_owned()))
}

/// The election, ready to be run.
struct Elector {
    /// A client of its own, so that its writes never wait behind the follows'.
    api: ApiServer,
    /// This replica, as the Lease names its holder.
    identity: String,
    timing: lease::Timing,
}

/// This replica, as a Lease names its holder: its pod's name, which the environment gives
/// as its host name, and when it started, so that a pod started again is another.
fn identity() -> String {
    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "edgerush".to_owned());
    format!("{host}_{}", started_at())
}

/// The admission webhook, ready to be served.
struct Hooking {
    /// A client of its own, for the IngressClasses it follows.
    api: ApiServer,
    listener: std::net::TcpListener,
    files: CertificateFiles,
    /// What the files held as it started.
    certificate: Certificate,
    /// How often the files are read again.
    reread: Duration,
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
    /// The names whose addresses are the gateway's own pods, the nameservers they are asked
    /// of, and at what pace.
    own: Own,
}

/// The names whose addresses are the gateway's own pods, and how they are resolved.
struct Own {
    names: Vec<String>,
    nameservers: Vec<SocketAddr>,
    pace: crate::resolver::Pace,
}

/// What the control plane serves, besides writing status.
struct Listeners {
    /// The config stream, if it is served.
    stream: Option<Serving>,
    /// The admission webhook, if it is served.
    webhook: Option<Hooking>,
}

/// Runs the control plane through `api` until `stop` is done, serving what `listeners`
/// holds too, each line of what it says given to `say`.
async fn control_until(
    api: ApiServer,
    options: Options,
    plane: Plane,
    listeners: Listeners,
    elector: Option<Elector>,
    stop: impl Future<Output = io::Result<()>>,
    say: impl Fn(&str) + 'static,
) -> io::Result<()> {
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let write_config = options.write_config;
    let status = options.status;
    let mut electing = Vec::new();
    let leading = match (options.leader, elector) {
        (Some((namespace, name)), Some(elector)) => {
            let (leads, leading) = watch::channel(false);
            let lease = format!("Lease {namespace}/{name}");
            let telling = Rc::clone(&say);
            let failed = lease.clone();
            electing.push(tokio::task::spawn_local(async move {
                elect(
                    &elector.api,
                    &namespace,
                    &name,
                    &elector.identity,
                    elector.timing,
                    &leads,
                    |error| telling(&format!("{failed}: {error}")),
                )
                .await;
            }));
            let (mut told, telling) = (leading.clone(), Rc::clone(&say));
            electing.push(tokio::task::spawn_local(async move {
                while told.changed().await.is_ok() {
                    let leads = *told.borrow_and_update();
                    telling(&if leads {
                        format!("leading, by {lease}")
                    } else {
                        "no longer leading".to_owned()
                    });
                }
            }));
            Some(leading)
        }
        _ => None,
    };
    let scope = options.scope;
    let mut tasks = Vec::new();
    let mut served = None;
    let offering = match listeners.stream {
        Some(serving) => {
            let (offers, offered) = watch::channel(None);
            let (gateway, addresses) = watch::channel::<Arc<[IpAddr]>>(Arc::from([]));
            if !serving.own.names.is_empty() {
                tasks.push(tokio::task::spawn_local(resolve_own(
                    serving.own,
                    gateway,
                    Rc::clone(&say),
                )));
            }
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
                    addresses,
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
    // The webhook, on a thread of its own: what it says is said here, where `say` lives.
    let mut hooked = None;
    if let Some(hooking) = listeners.webhook {
        let (said, mut hearing) = mpsc::unbounded_channel();
        let (stopping, stopped) = oneshot::channel::<()>();
        let (ending, ended) = oneshot::channel::<()>();
        hooked = Some((stopping, ended));
        hook(hooking, plane.clone(), said, stopped, ending)?;
        let telling = Rc::clone(&say);
        tasks.push(tokio::task::spawn_local(async move {
            while let Some(line) = hearing.recv().await {
                telling(&line);
            }
        }));
    }
    let telling = Rc::clone(&say);
    tasks.push(tokio::task::spawn_local(async move {
        control(
            api,
            &scope,
            &plane,
            Debounce::default(),
            status,
            leading,
            move |said| {
                let mut text = told(&said, write_config.as_deref());
                if let (Said::Translated(translation), Some(offering)) = (&said, &offering) {
                    // Writing to a `String` cannot fail.
                    let _ = match offering.offer(translation) {
                        Ok(version) => write!(text, "\noffered as config {version}"),
                        Err(error) => write!(text, "\nnot offered: {error}"),
                    };
                }
                telling(&text);
            },
        )
        .await;
    }));
    let stopped = stop.await;
    if let Some((stopping, server)) = served {
        let _ = stopping.send(());
        if let Ok(Err(error)) = server.await {
            say(&format!("error: {error}"));
        }
    }
    if let Some((stopping, ended)) = hooked {
        let _ = stopping.send(());
        let _ended = ended.await;
    }
    for task in tasks.into_iter().chain(electing) {
        task.abort();
    }
    stopped
}

/// Resolves `own`'s names for as long as it is polled, and hands `gateway` every address
/// they resolve to, together, in order, each once, each time it changes: the gateway's own
/// pods, which data planes are sent and never send a request to (03 §6). A name's failure
/// is said once, with the name.
async fn resolve_own(own: Own, gateway: watch::Sender<Arc<[IpAddr]>>, say: Rc<dyn Fn(&str)>) {
    let (_kept, wanted) = watch::channel(own.names.iter().cloned().collect());
    let by_name: Rc<RefCell<BTreeMap<String, Vec<IpAddr>>>> = Rc::default();
    let taking = Rc::clone(&by_name);
    crate::resolver::resolve_names(
        wanted,
        own.nameservers,
        own.pace,
        move |name, addresses| {
            let mut by_name = taking.borrow_mut();
            match addresses {
                Some(addresses) => by_name.insert(name.to_owned(), addresses),
                None => by_name.remove(name),
            };
            let all: BTreeSet<IpAddr> = by_name.values().flatten().copied().collect();
            let all: Arc<[IpAddr]> = all.into_iter().collect();
            gateway.send_if_modified(|sent| {
                let changed = *sent != all;
                *sent = Arc::clone(&all);
                changed
            });
            Ok(())
        },
        move |name, failure| say(&format!("own name {name}: {failure}")),
    )
    .await;
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

/// Serves the webhook `hooking` holds on a thread and runtime of its own (04 §3), so that a
/// flood of reviews, which anyone in the cluster can send, costs only its answers: the
/// IngressClasses followed on its own client, each review judged for the data plane `plane`
/// describes, and its certificate files read again. What it says goes to `said`, a line each;
/// it stops once `stop` is done, or dropped, and tells `ended` as it ends.
///
/// # Errors
///
/// If its certificate cannot be used, its listener cannot be made, or the thread or its
/// runtime cannot be started.
fn hook(
    hooking: Hooking,
    plane: Plane,
    said: mpsc::UnboundedSender<String>,
    stop: oneshot::Receiver<()>,
    ended: oneshot::Sender<()>,
) -> io::Result<()> {
    let acceptor = webhook_acceptor(&hooking.certificate).map_err(io::Error::other)?;
    hooking.listener.set_nonblocking(true)?;
    let runtime = Builder::new_current_thread().enable_all().build()?;
    std::thread::Builder::new()
        .name("webhook".to_owned())
        .spawn(move || {
            let serving = async move {
                let (listed, classes) = watch::channel(Arc::new(Listed::<IngressClass>::default()));
                let failed = said.clone();
                let api = hooking.api;
                let following = tokio::task::spawn_local(async move {
                    follow(&api, &Scope::Cluster, Retry::default(), listed, |failure| {
                        let _gone = failed.send(format!(
                            "ingressclasses in the cluster, for the webhook: {failure}"
                        ));
                    })
                    .await;
                });
                let tls = watch::Sender::new(Arc::new(acceptor));
                let replacing = said.clone();
                let accepting = tls.subscribe();
                let rereading = tokio::task::spawn_local(reread(
                    hooking.files,
                    hooking.certificate,
                    hooking.reread,
                    move |changed| {
                        let _gone = replacing.send(webhook_replaced(changed, &tls));
                    },
                ));
                match tokio::net::TcpListener::from_std(hooking.listener) {
                    Ok(listener) => {
                        webhook::serve(
                            listener,
                            accepting,
                            Bounds::default(),
                            move |body| review(&plane, &classes.borrow(), body),
                            async move {
                                let _ = stop.await;
                            },
                        )
                        .await;
                    }
                    Err(error) => {
                        let _gone = said.send(format!("error: the webhook's listener: {error}"));
                    }
                }
                following.abort();
                rereading.abort();
            };
            tokio::task::LocalSet::new().block_on(&runtime, serving);
            let _ = ended.send(());
        })?;
    Ok(())
}

/// The webhook's answer to the review `body`, an Ingress of the data plane `plane`
/// describes judged alone, as `classes` say which is its: 503 until they are listed, as
/// there is nothing to say which is; 400 for what is not a review.
fn review(plane: &Plane, classes: &Listed<IngressClass>, body: &[u8]) -> webhook::Answer {
    let answer = |status, body: Vec<u8>| webhook::Answer { status, body };
    if !classes.listed {
        let why = "the IngressClasses are not listed yet";
        return answer(StatusCode::SERVICE_UNAVAILABLE, why.as_bytes().to_vec());
    }
    match admission::read(body) {
        Ok(review) => {
            let refused = match &review.asked {
                Asked::Ingress(ingress) => admission::judge(plane, &classes.objects, ingress),
                Asked::Other => None,
            };
            answer(
                StatusCode::OK,
                admission::answer(&review.uid, refused.as_deref()),
            )
        }
        Err(error) => answer(StatusCode::BAD_REQUEST, error.to_string().into_bytes()),
    }
}

/// What is said of the webhook's certificate files changing: the new ones put to use for
/// the next connection, or kept from it if they cannot be used.
fn webhook_replaced(
    changed: Result<&Certificate, &FilesError>,
    tls: &watch::Sender<Arc<SslAcceptor>>,
) -> String {
    let kept = "and the last is kept";
    match changed.map(webhook_acceptor) {
        Ok(Ok(replaced)) => {
            tls.send_replace(Arc::new(replaced));
            "the webhook's certificate is replaced".to_owned()
        }
        Ok(Err(error)) => format!("the webhook's certificate cannot be used, {kept}: {error}"),
        Err(error) => format!("the webhook's certificate cannot be read, {kept}: {error}"),
    }
}

/// What is said of the stream's certificate files changing: the new ones put to use for the
/// next connection, or kept from it if they cannot be used.
fn replaced(
    changed: Result<&Credentials, &FilesError>,
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
        Said::Status {
            ingress,
            addresses,
            written,
        } => {
            let said: Vec<String> = addresses
                .iter()
                .filter_map(|address| address.ip.clone().or_else(|| address.hostname.clone()))
                .collect();
            let said = if said.is_empty() {
                "no address".to_owned()
            } else {
                said.join(", ")
            };
            match written {
                Ok(()) => format!("status of {ingress}: {said}"),
                Err(error) => format!("status of {ingress} cannot be written ({said}): {error}"),
            }
        }
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
