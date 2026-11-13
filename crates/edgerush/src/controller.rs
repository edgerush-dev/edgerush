//! `edgerush controller`: the command line around `edgerush_controller`
//! ([04](../../../docs/04-control-plane.md)) — its arguments, the data plane's settings,
//! the API server found as a pod finds it, the config stream and the admission webhook it
//! serves, and what is said of each translation and each data plane. A translation is said
//! as `validate` says it, and with `--write-config` written to a file.

use crate::validate::{described, read};
use bytes::Bytes;
use edgerush_certificates::authority::{Kind, Written};
use edgerush_config::Certificate;
use edgerush_control_service::metrics;
use edgerush_control_service::webhook::{self, Bounds};
use edgerush_control_service::{
    CertificateFiles, Credentials, Files, FilesError, Heard, SslAcceptor, acceptor, reread, serve,
    webhook_acceptor,
};
use edgerush_control_stream::{Offer, OfferError, Versions};
use edgerush_controller::acks::{Pods, Reporter, Runs};
use edgerush_controller::admission::{self, Asked};
use edgerush_controller::data_plane::{self, follow_data_planes};
use edgerush_controller::expiry::Expiry;
use edgerush_controller::issuing::{self, Issuer};
use edgerush_controller::status::{Addresses, address};
use edgerush_controller::{Acks, Debounce, Reports, Said, control};
use edgerush_kubernetes::lease::{self, elect};
use edgerush_kubernetes::{ApiError, ApiServer, Listed, Retry, SERVICE_ACCOUNT, Scope, follow};
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
Usage: edgerush controller (--data-plane <FILE> | --control-plane <NAME> [--cluster-domain <DOMAIN>])
         [--namespace <NAME>]... [--write-config <FILE>]
         [--listen <ADDRESS> (--certificate <FILE> --key <FILE> --authorities <FILE>
                              | --authority <SECRET> [--authority-provided])
          [--own-name <NAME>]...]
         [--status-service <NAMESPACE>/<NAME> | --status-address <ADDRESS>...]
         [--leader-lease <NAMESPACE>/<NAME>]
         [--webhook-listen <ADDRESS> --webhook-certificate <FILE> --webhook-key <FILE>]
         [--metrics <ADDRESS>]

Runs a control plane for one data plane, in a cluster: follows the IngressClasses,
Ingresses, Services and Secrets of its scope through the API server, found as a pod finds
it, and translates them each time they change, once every kind has been listed whole. What
each translation came to is said on stderr, as `edgerush validate` says it.
The data plane's settings are a file's, with --data-plane, or, with --control-plane, the
DataPlane of the pod's own namespace that names that ControlPlane, followed: a generation
of it is acted on once the operator's Accepted condition is of it, the last accepted kept
meanwhile, and nothing is translated until one is. What a DataPlane has no field for takes
its default: no authorities for wss backends, the data plane's own settings left out.
With --listen, it serves the config stream there, gRPC over mutual TLS that data planes
dial: each config translated is offered to every data plane, and what each says of it is
said on stderr. The certificate, key and authorities are PEM files, read again every
second, so that ones replaced serve the next connection. With --own-name, the addresses
the names resolve to, the gateway's own pods, are sent on the stream too, for data planes
never to send a request to.
Where the data plane is a Gateway, its GatewayClass, the Gateway and each HTTPRoute of it
say in their status what is served of them, the Gateway programmed once every ready pod of
the data plane runs its generation, as the data planes say on the stream.
With --status-service or --status-address, each Ingress served says the data plane's
addresses in its status, each of the class that is not says none, and the Gateway says
them. With --leader-lease, the replicas elect one by that Lease, which alone writes
status; every one serves, and reports what its data planes run in a ConfigMap of the
Lease's namespace, <LEASE>-acks-<HOSTNAME>, owned by its Pod where EDGERUSH_POD_UID gives
the Pod's UID, for the leader to merge.
With --webhook-listen, it serves the admission webhook there, HTTPS, reviews POSTed to
/validate: an Ingress of the data plane's class is refused where, alone, it does not
translate into something the data plane accepts. Its certificate and key are read again
every second; until the IngressClasses are listed it answers 503, which the API server
takes as an outage, admitting the Ingress where the webhook fails open.
A BackendTLSPolicy's `wellKnownCACertificates: System` trusts the authorities its own
system does, read as it starts: the PEM file SSL_CERT_FILE names, else the system's bundle,
/etc/ssl/certs/ca-certificates.crt, /etc/ssl/cert.pem or /etc/pki/tls/certs/ca-bundle.crt,
the first there is. With none, the backends of such a policy fail closed.
A certificate handed to the data plane expired or not yet valid is served as any other,
and said on stderr, once for each version of it handed out, by the leader alone where
replicas elect one, and again by each new leader. With --metrics, every replica serves its
own metrics there, plain HTTP, at /metrics: when each certificate handed out expires.
Runs until Ctrl-C, or SIGTERM on Unix, and then exits 0; exits 2 when the command line, the
data plane's settings, the system's authorities or the stream's or the webhook's
certificates cannot be used, or it is not in a cluster.

Options:
      --data-plane <FILE>    The data plane's settings, in YAML, as `edgerush validate`
                             reads them
      --control-plane <NAME> The ControlPlane this is, whose DataPlane, in the pod's own
                             namespace, says the data plane's settings; instead of
                             --data-plane
      --cluster-domain <DOMAIN>
                             The cluster's DNS domain, with --control-plane;
                             cluster.local when left out
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
      --authority <SECRET>   Instead of those three, with --control-plane: the Secret of
                             the pod's own namespace that holds the authority it signs
                             with, made by the leader if it is not there; each replica
                             signs its own certificate from it, and the leader writes
                             each DataPlane's into a Secret its pods mount
      --authority-provided   The authority is the platform's, never made or written
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
      --metrics <ADDRESS>    Serve /metrics on ADDRESS, such as [::]:9090
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
    let started = crate::system_authorities::read()
        .map_err(|error| format!("the system's authorities: {error}"))
        .and_then(|system_authorities| {
            let plane = match &options.settings {
                Settings::File(path) => {
                    let plane = read::<Plane>(path).map_err(|failed| failed.to_string())?;
                    Source::Fixed(Plane {
                        system_authorities,
                        ..plane
                    })
                }
                Settings::DataPlane {
                    control_plane,
                    cluster_domain,
                } => Source::Followed {
                    api: ApiServer::in_cluster().map_err(|error| outside(&error))?,
                    namespace: own_namespace()?,
                    own: data_plane::Own {
                        control_plane: control_plane.clone(),
                        cluster_domain: cluster_domain.clone(),
                        system_authorities,
                    },
                },
            };
            let stream = match options.stream.as_deref() {
                Some(Stream {
                    credentials: StreamCredentials::Files(files),
                    ..
                }) => Some(Initial::Files(runtime.block_on(credentials(files))?)),
                Some(Stream {
                    credentials: StreamCredentials::Authority { secret, kind },
                    ..
                }) => {
                    let Settings::DataPlane {
                        control_plane,
                        cluster_domain,
                    } = &options.settings
                    else {
                        return Err(UsageError::AuthorityWithoutControlPlane.to_string());
                    };
                    Some(Initial::Signed {
                        api: Box::new(ApiServer::in_cluster().map_err(|error| outside(&error))?),
                        issuer: Issuer {
                            namespace: own_namespace_name()?,
                            control_plane: control_plane.clone(),
                            cluster_domain: cluster_domain.clone(),
                            authority: secret.clone(),
                            kind: *kind,
                        },
                    })
                }
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
            Ok(api) => match ApiServer::in_cluster() {
                Ok(reports) => Some(Elector {
                    api,
                    reports,
                    identity: identity(),
                    timing: lease::Timing::default(),
                }),
                Err(error) => {
                    return writeln!(stderr, "error: {}", outside(&error))
                        .map_or(1, |()| crate::EXIT_USAGE);
                }
            },
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
                credentials: match (credentials, &stream.credentials) {
                    (Initial::Files(credentials), StreamCredentials::Files(files)) => {
                        Credentialing::Files {
                            files: files.clone(),
                            credentials,
                            reread: REREAD,
                        }
                    }
                    (Initial::Signed { api, issuer }, _) => Credentialing::Signed { api, issuer },
                    (Initial::Files(_), StreamCredentials::Authority { .. }) => {
                        return writeln!(stderr, "error: the stream's credentials are not its own")
                            .map_or(1, |()| 1);
                    }
                },
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
    let metrics = match options.metrics {
        Some(address) => match std::net::TcpListener::bind(address) {
            Ok(listener) => Some(listener),
            Err(error) => return writeln!(stderr, "error: {address}: {error}").map_or(1, |()| 1),
        },
        None => None,
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
                metrics,
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

/// Now, in seconds since the Unix epoch: a clock before the epoch counts from it.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}

/// The stream's credentials, read and checked to be usable.
async fn credentials(files: &Files) -> Result<Credentials, String> {
    let failed = |error: String| format!("the stream's certificates: {error}");
    let credentials = files
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
pub(crate) fn outside(error: &ApiError) -> String {
    format!("the API server cannot be reached as a pod reaches it: {error}")
}

#[derive(Debug, PartialEq)]
enum Parsed {
    Help,
    Control(Box<Options>),
}

#[derive(Debug, PartialEq)]
struct Options {
    settings: Settings,
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
    /// Where its own metrics are served, if they are.
    metrics: Option<SocketAddr>,
}

/// Where the data plane's settings come from.
#[derive(Debug, PartialEq, Eq)]
enum Settings {
    /// A file, as the development harness has them.
    File(PathBuf),
    /// The DataPlane of the pod's own namespace that names this ControlPlane (05 §3).
    DataPlane {
        control_plane: String,
        cluster_domain: String,
    },
}

/// The data plane's settings, ready to be served.
enum Source {
    /// As they are, for good.
    Fixed(Plane),
    /// As the DataPlanes of `namespace` say them, followed on a client of their own.
    Followed {
        api: ApiServer,
        namespace: Scope,
        own: data_plane::Own,
    },
}

/// The pod's own namespace, as its service account says it.
fn own_namespace() -> Result<Scope, String> {
    let name = own_namespace_name()?;
    Scope::namespace(&name).map_err(|error| format!("the pod's namespace: {error}"))
}

/// The name of the pod's own namespace, as its service account says it.
pub(crate) fn own_namespace_name() -> Result<String, String> {
    let path = Path::new(SERVICE_ACCOUNT).join("namespace");
    let written = std::fs::read_to_string(&path)
        .map_err(|error| format!("the pod's namespace, {}: {error}", path.display()))?;
    let name = written.trim().to_owned();
    Scope::namespace(&name).map_err(|error| format!("the pod's namespace: {error}"))?;
    Ok(name)
}

/// The next certificate the issuer signs that has not been taken yet, as the stream's
/// credentials, waited for until it signs one; none once it is gone. Each is taken once.
async fn next_signed(signed: &mut watch::Receiver<Option<Written>>) -> Option<Credentials> {
    loop {
        // At once for one not yet seen; else once another comes.
        signed.changed().await.ok()?;
        if let Some(written) = signed.borrow_and_update().clone() {
            return Some(Credentials {
                certificate: Certificate {
                    chain: written.certificate,
                    key: written.key,
                },
                authorities: written.bundle,
            });
        }
    }
}

/// The config stream, as the command line gives it.
#[derive(Debug, PartialEq, Eq)]
struct Stream {
    listen: SocketAddr,
    credentials: StreamCredentials,
}

/// Where the stream's certificate comes from.
#[derive(Debug, PartialEq, Eq)]
enum StreamCredentials {
    /// Files, read again every second.
    Files(Files),
    /// Signed by the control plane from its authority, kept in this Secret (05 §2).
    Authority { secret: String, kind: Kind },
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
    #[error("'--data-plane' and '--control-plane' are not given together")]
    Settings,
    #[error("'--control-plane {0}' is not a ControlPlane's name, a DNS label")]
    ControlPlane(String),
    #[error("'--cluster-domain {0}': {1}")]
    ClusterDomain(String, edgerush_config::DnsNameError),
    #[error("'--cluster-domain' is for '--control-plane'")]
    ClusterDomainWithoutControlPlane,
    #[error("namespace '{0}' is given twice")]
    NamespaceTwice(String),
    #[error("'--namespace {0}': {1}")]
    Namespace(String, ApiError),
    #[error("'--listen {0}' is not an address and a port, such as [::]:9443")]
    Listen(String),
    #[error(
        "'--listen' is given with '--certificate', '--key' and '--authorities', or with '--authority'"
    )]
    Together,
    #[error("'--authority {0}' is not a Secret's name")]
    Authority(String),
    #[error("'--authority-provided' is for '--authority'")]
    AuthorityProvidedAlone,
    #[error("'--authority' signs in its ControlPlane's trust domain, and needs '--control-plane'")]
    AuthorityWithoutControlPlane,
    #[error("'--status-service {0}' is not a namespace and a name, as edge/edgerush")]
    StatusService(String),
    #[error("'--status-service' and '--status-address' are not given together")]
    Status,
    #[error("'--leader-lease {0}' is not a namespace and a name, as edge/edgerush-control")]
    LeaderLease(String),
    #[error("'--webhook-listen {0}' is not an address and a port, such as [::]:8443")]
    WebhookListen(String),
    #[error("'--metrics {0}' is not an address and a port, such as [::]:9090")]
    Metrics(String),
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
    let mut control_plane = None;
    let mut cluster_domain = None;
    let mut write_config = None;
    let mut listen = None;
    let mut certificate = None;
    let mut key = None;
    let mut authorities = None;
    let mut authority = None;
    let mut provided = false;
    let mut status_service = None;
    let mut leader = None;
    let mut webhook_listen = None;
    let mut webhook_certificate = None;
    let mut webhook_key = None;
    let mut metrics = None;
    let mut status_addresses = Vec::new();
    let mut own_names: Vec<String> = Vec::new();
    let mut scope = Vec::new();
    while let Some(arg) = args.next() {
        let option = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--data-plane" => "--data-plane",
            "--control-plane" => "--control-plane",
            "--cluster-domain" => "--cluster-domain",
            "--write-config" => "--write-config",
            "--namespace" => "--namespace",
            "--listen" => "--listen",
            "--certificate" => "--certificate",
            "--key" => "--key",
            "--authorities" => "--authorities",
            "--authority" => "--authority",
            "--authority-provided" => {
                if provided {
                    return Err(UsageError::Twice("--authority-provided"));
                }
                provided = true;
                continue;
            }
            "--status-service" => "--status-service",
            "--status-address" => "--status-address",
            "--leader-lease" => "--leader-lease",
            "--webhook-listen" => "--webhook-listen",
            "--webhook-certificate" => "--webhook-certificate",
            "--webhook-key" => "--webhook-key",
            "--own-name" => "--own-name",
            "--metrics" => "--metrics",
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
            "--control-plane" => {
                // A ControlPlane's name is a DNS label, as a namespace's is (05 §3).
                if Scope::namespace(&value).is_err() {
                    return Err(UsageError::ControlPlane(value));
                }
                if control_plane.replace(value).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--cluster-domain" => {
                let domain = edgerush_config::DnsName::host(&value)
                    .map_err(|error| UsageError::ClusterDomain(value.clone(), error))?;
                if cluster_domain.replace(domain).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--authority" => {
                if Scope::named(&value).is_err() {
                    return Err(UsageError::Authority(value));
                }
                if authority.replace(value).is_some() {
                    return Err(UsageError::Twice(option));
                }
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
            "--metrics" => {
                let address = value.parse().map_err(|_| UsageError::Metrics(value))?;
                if metrics.replace(address).is_some() {
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
    if provided && authority.is_none() {
        return Err(UsageError::AuthorityProvidedAlone);
    }
    let stream = match (listen, certificate, key, authorities, authority) {
        (Some(listen), Some(certificate), Some(key), Some(authorities), None) => {
            Some(Box::new(Stream {
                listen,
                credentials: StreamCredentials::Files(Files {
                    certificate,
                    key,
                    authorities,
                }),
            }))
        }
        (Some(listen), None, None, None, Some(secret)) => Some(Box::new(Stream {
            listen,
            credentials: StreamCredentials::Authority {
                secret,
                kind: if provided { Kind::Provided } else { Kind::Made },
            },
        })),
        (None, None, None, None, None) => None,
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
    let settings = match (data_plane, control_plane, cluster_domain) {
        (Some(_), Some(_), _) => return Err(UsageError::Settings),
        (Some(_), None, Some(_)) => return Err(UsageError::ClusterDomainWithoutControlPlane),
        (Some(path), None, None) => Settings::File(path),
        (None, Some(control_plane), domain) => Settings::DataPlane {
            control_plane,
            cluster_domain: domain.unwrap_or_else(|| "cluster.local".to_owned()),
        },
        (None, None, _) => {
            return Err(UsageError::Required(
                "--data-plane <FILE> or --control-plane <NAME>",
            ));
        }
    };
    if let (
        Some(Stream {
            credentials: StreamCredentials::Authority { .. },
            ..
        }),
        Settings::File(_),
    ) = (stream.as_deref(), &settings)
    {
        return Err(UsageError::AuthorityWithoutControlPlane);
    }
    Ok(Parsed::Control(Box::new(Options {
        settings,
        scope,
        write_config,
        stream,
        own_names,
        status,
        leader,
        webhook,
        metrics,
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
    /// Another, for the report of what this replica's data planes run (04 §6).
    reports: ApiServer,
    /// This replica, as the Lease names its holder.
    identity: String,
    timing: lease::Timing,
}

/// This replica, as a Lease names its holder: its pod's name, which the environment gives
/// as its host name, and when it started, so that a pod started again is another.
pub(crate) fn identity() -> String {
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

/// The stream's credentials as it starts.
enum Initial {
    /// What its files held.
    Files(Credentials),
    /// None yet: signed once the authority is read, through a client of their own.
    Signed { api: Box<ApiServer>, issuer: Issuer },
}

/// The stream's credentials, ready to be kept.
enum Credentialing {
    /// Its files, what they held as it started, and how often they are read again.
    Files {
        files: Files,
        credentials: Credentials,
        reread: Duration,
    },
    /// Signed from the authority, by the issuer (05 §2).
    Signed { api: Box<ApiServer>, issuer: Issuer },
}

/// The config stream, ready to be served.
struct Serving {
    listener: std::net::TcpListener,
    credentials: Credentialing,
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
    /// The control plane's own metrics, if they are served.
    metrics: Option<std::net::TcpListener>,
}

/// Runs the control plane through `api` until `stop` is done, serving what `listeners`
/// holds too, each line of what it says given to `say`.
async fn control_until(
    api: ApiServer,
    options: Options,
    source: Source,
    listeners: Listeners,
    elector: Option<Elector>,
    stop: impl Future<Output = io::Result<()>>,
    say: impl Fn(&str) + 'static,
) -> io::Result<()> {
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let write_config = options.write_config;
    let status = options.status;
    let mut electing = Vec::new();
    // What each data plane this replica serves runs, as it says it (04 §6).
    let runs: Rc<RefCell<Runs>> = Rc::default();
    let (pods, running) = watch::channel(Pods::new());
    let mut reports = None;
    let leading = match (options.leader, elector) {
        (Some((namespace, name)), Some(elector)) => {
            let (leads, leading) = watch::channel(false);
            let lease = format!("Lease {namespace}/{name}");
            // Every replica reports what its data planes run, for the leader to merge.
            let reporter = Reporter {
                namespace: namespace.clone(),
                lease: name.clone(),
                pod: std::env::var("HOSTNAME").unwrap_or_else(|_| "edgerush".to_owned()),
                uid: std::env::var("EDGERUSH_POD_UID").ok(),
            };
            reports = Some(Reports {
                namespace: namespace.clone(),
                lease: name.clone(),
                own: reporter.name(),
            });
            let (reporting, telling) = (elector.reports, Rc::clone(&say));
            let (running, failed) = (running.clone(), format!("report {}", reporter.name()));
            electing.push(tokio::task::spawn_local(async move {
                reporter
                    .publish(&reporting, running, |error| {
                        telling(&format!("{failed} cannot be written: {error}"));
                    })
                    .await;
            }));
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
    // The data plane's settings, as they are or as its DataPlane says them. Held for as
    // long as the control plane runs, as nothing more is said of fixed ones.
    let (settings, planes) = watch::channel(None);
    let _fixed = match source {
        Source::Fixed(plane) => {
            settings.send_replace(Some(plane));
            Some(settings)
        }
        Source::Followed {
            api,
            namespace,
            own,
        } => {
            let telling = Rc::clone(&say);
            tasks.push(tokio::task::spawn_local(follow_data_planes(
                api,
                namespace,
                own,
                settings,
                move |line| telling(line),
            )));
            None
        }
    };
    // The dates of the certificates handed out (04 §2, 08 §1): every replica keeps them
    // for its gauge, and the leader says those out of date.
    let expiry = Rc::new(RefCell::new(Expiry::new()));
    let (exposed, exposition) = watch::channel(Bytes::from(expiry.borrow().exposition()));
    let mut measured = None;
    if let Some(listener) = listeners.metrics {
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let (stopping, stopped) = oneshot::channel::<()>();
        measured = Some((
            stopping,
            tokio::spawn(metrics::serve(
                listener,
                exposition,
                metrics::DEADLINE,
                async move {
                    let _ = stopped.await;
                },
            )),
        ));
    }
    if let Some(mut told) = leading.clone() {
        let (held, telling) = (Rc::clone(&expiry), Rc::clone(&say));
        tasks.push(tokio::task::spawn_local(async move {
            while told.changed().await.is_ok() {
                if *told.borrow_and_update() {
                    for line in held.borrow().led(unix_now()) {
                        telling(&line);
                    }
                }
            }
        }));
    }
    let leads = leading.clone();
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
            serving.listener.set_nonblocking(true)?;
            let listener = tokio::net::TcpListener::from_std(serving.listener)?;
            // Heard on the server's own tasks, and said here, where `say` lives.
            let (heard, mut hearing) = mpsc::unbounded_channel();
            let hear = move |peer, said| {
                let _stopped = heard.send((told_of(peer, &said), ran(peer, &said)));
            };
            let until = async move {
                let _ = stopped.await;
            };
            match serving.credentials {
                Credentialing::Files {
                    files,
                    credentials,
                    reread: every,
                } => {
                    let tls = watch::Sender::new(Arc::new(
                        acceptor(&credentials).map_err(io::Error::other)?,
                    ));
                    served = Some((
                        stopping,
                        tokio::spawn(serve(
                            listener,
                            tls.subscribe(),
                            offered,
                            addresses,
                            hear,
                            until,
                        )),
                    ));
                    let telling = Rc::clone(&say);
                    tasks.push(tokio::task::spawn_local(reread(
                        files,
                        credentials,
                        every,
                        move |changed| telling(&replaced(changed, &tls)),
                    )));
                }
                Credentialing::Signed { api, issuer } => {
                    // Its own certificate, as the issuer signs it: the stream is served from
                    // the first on, each after it taking the next connection (05 §2).
                    let (own, mut signed) = watch::channel(None);
                    let telling = Rc::clone(&say);
                    tasks.push(tokio::task::spawn_local(issuing::issue(
                        *api,
                        issuer,
                        leading.clone(),
                        own,
                        move |line| telling(line),
                    )));
                    let telling = Rc::clone(&say);
                    served = Some((
                        stopping,
                        tokio::task::spawn_local(async move {
                            // The first it can serve with: one it cannot is said, and the
                            // next waited for.
                            let tls = loop {
                                let Some(first) = next_signed(&mut signed).await else {
                                    return Ok(());
                                };
                                match acceptor(&first) {
                                    Ok(first) => break watch::Sender::new(Arc::new(first)),
                                    Err(error) => telling(&format!(
                                        "the stream's certificate cannot be used: {error}"
                                    )),
                                }
                            };
                            let accepting = tls.subscribe();
                            tokio::task::spawn_local(async move {
                                while let Some(next) = next_signed(&mut signed).await {
                                    match acceptor(&next) {
                                        Ok(next) => {
                                            tls.send_replace(Arc::new(next));
                                            telling("the stream's certificate replaced");
                                        }
                                        Err(error) => telling(&format!(
                                            "the stream's certificate cannot be used: {error}"
                                        )),
                                    }
                                }
                            });
                            serve(listener, accepting, offered, addresses, hear, until).await
                        }),
                    ));
                }
            }
            let (telling, taking, telling_pods) = (Rc::clone(&say), Rc::clone(&runs), pods.clone());
            tasks.push(tokio::task::spawn_local(async move {
                while let Some((line, ran)) = hearing.recv().await {
                    telling(&line);
                    if let Some(ran) = ran {
                        let mut runs = taking.borrow_mut();
                        ran.into(&mut runs);
                        said_runs(&telling_pods, &runs);
                    }
                }
            }));
            Some(Offering {
                versions: RefCell::new(serving.versions),
                offers,
                runs: Rc::clone(&runs),
                pods: pods.clone(),
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
        hook(hooking, planes.clone(), said, stopped, ending)?;
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
            planes,
            Debounce::default(),
            status,
            leading,
            Acks {
                own: running,
                reports,
            },
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
                if let Said::Translated(translation) = &said {
                    // Without an election, this replica is the only one, and leads.
                    let leading = leads.as_ref().is_none_or(|leads| *leads.borrow());
                    let out_of_date = expiry.borrow_mut().take(
                        &translation.config.certificates,
                        unix_now(),
                        leading,
                    );
                    exposed.send_replace(Bytes::from(expiry.borrow().exposition()));
                    for line in out_of_date {
                        telling(&line);
                    }
                }
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
    if let Some((stopping, server)) = measured {
        let _ = stopping.send(());
        let _ended = server.await;
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
    /// What each data plane runs, each version offered among it.
    runs: Rc<RefCell<Runs>>,
    pods: watch::Sender<Pods>,
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
        let generation = translation.gateway_status.as_ref().map(|s| s.generation);
        let mut runs = self.runs.borrow_mut();
        runs.offered(&version, generation);
        said_runs(&self.pods, &runs);
        self.offers.send_replace(Some(Arc::new(offer)));
        Ok(version)
    }
}

/// What a data plane said of the config it holds or runs.
enum Ran {
    /// It runs this, serves none of it of these listeners (each with why), and refused
    /// that, if it did.
    Runs {
        pod: IpAddr,
        running: Option<String>,
        refused: Option<(String, String)>,
        unserved: BTreeMap<String, String>,
    },
    /// Its stream ended.
    Closed(IpAddr),
}

impl Ran {
    /// Takes it into `runs`.
    fn into(self, runs: &mut Runs) {
        match self {
            Self::Runs {
                pod,
                running,
                refused,
                unserved,
            } => runs.reported(
                pod,
                running.as_deref(),
                refused
                    .as_ref()
                    .map(|(version, why)| (version.as_str(), why.as_str())),
                unserved,
            ),
            Self::Closed(pod) => runs.closed(pod),
        }
    }
}

/// What `heard` of the data plane at `peer` says of the config it runs, if anything.
fn ran(peer: SocketAddr, heard: &Heard) -> Option<Ran> {
    let pod = peer.ip();
    match heard {
        Heard::Refused(_) => None,
        // What it serves of the config it holds comes in a report after.
        Heard::Opened { held, .. } => Some(Ran::Runs {
            pod,
            running: held.config.clone(),
            refused: None,
            unserved: BTreeMap::new(),
        }),
        Heard::Reported(report) => Some(Ran::Runs {
            pod,
            running: report.running.config.clone(),
            refused: report
                .refused
                .as_ref()
                .map(|refused| (refused.config.clone(), refused.why.clone())),
            unserved: report
                .listening
                .unserved
                .iter()
                .map(|unserved| (unserved.listener.clone(), unserved.why.clone()))
                .collect(),
        }),
        Heard::Closed(_) => Some(Ran::Closed(pod)),
    }
}

/// Hands on what each data plane runs, as `runs` has it, where it changed.
fn said_runs(pods: &watch::Sender<Pods>, runs: &Runs) {
    let now = runs.pods();
    pods.send_if_modified(|said| {
        let changed = *said != now;
        *said = now.clone();
        changed
    });
}

/// Serves the webhook `hooking` holds on a thread and runtime of its own (04 §3), so that a
/// flood of reviews, which anyone in the cluster can send, costs only its answers: the
/// IngressClasses followed on its own client, each review judged for the data plane the
/// settings `planes` holds as it comes describe, and its certificate files read again. What it says goes to `said`, a line each;
/// it stops once `stop` is done, or dropped, and tells `ended` as it ends.
///
/// # Errors
///
/// If its certificate cannot be used, its listener cannot be made, or the thread or its
/// runtime cannot be started.
fn hook(
    hooking: Hooking,
    planes: watch::Receiver<Option<Plane>>,
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
                            move |body| review(planes.borrow().as_ref(), &classes.borrow(), body),
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
/// describes judged alone, as `classes` say which is its: 503 until there are settings and
/// the classes are listed, as there is nothing to say which is; 400 for what is not a
/// review.
fn review(plane: Option<&Plane>, classes: &Listed<IngressClass>, body: &[u8]) -> webhook::Answer {
    let answer = |status, body: Vec<u8>| webhook::Answer { status, body };
    let Some(plane) = plane else {
        let why = "the data plane's settings are not known yet";
        return answer(StatusCode::SERVICE_UNAVAILABLE, why.as_bytes().to_vec());
    };
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
        Said::GatewayApi(served) => {
            let kinds: Vec<String> = served
                .iter()
                .map(|(kind, served)| format!("\n  {}: {served}", kind.crd()))
                .collect();
            format!("the Gateway API's kinds:{}", kinds.concat())
        }
        Said::Failed { followed, failure } => format!("{followed}: {failure}"),
        Said::GatewayApiStatus { object, written } => match written {
            Ok(()) => format!("status of {object} written"),
            Err(error) => format!("status of {object} cannot be written: {error}"),
        },
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
pub(crate) async fn told_to_stop() -> io::Result<()> {
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
