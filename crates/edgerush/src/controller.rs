//! `edgerush controller`: the command line around `edgerush_controller`
//! ([04](../../../docs/04-control-plane.md)) — its arguments, the data plane's settings,
//! the API server found as a pod finds it, the config stream and the admission webhook it
//! serves, and what is said of each translation and each data plane. A translation is said
//! as `validate` says it, and with `--write-config` written to a file.

use crate::validate::{described, read};
use bytes::Bytes;
use edgerush_certificates::authority::{Kind, Written};
use edgerush_config::{Certificate, Config};
use edgerush_control_service::webhook::{self, Bounds};
use edgerush_control_service::{
    CertificateFiles, Credentials, Files, FilesError, Heard, Served, SslAcceptor, acceptor, reread,
    serve, webhook_acceptor,
};
use edgerush_control_service::{health, metrics};
use edgerush_control_stream::{Offer, OfferError, Versions};
use edgerush_controller::acks::{Pods, Reporter, Runs};
use edgerush_controller::admission::{self, Asked};
use edgerush_controller::data_plane::{self, follow_data_planes};
use edgerush_controller::expiry::Expiry;
use edgerush_controller::issuing::{self, Issuer};
use edgerush_controller::own_pods::{Replica, follow_own_pods};
use edgerush_controller::snapshot::Snapshot;
use edgerush_controller::unimplementable::Unimplementable;
use edgerush_controller::{
    Acks, Classes, Debounce, Membership, Planes, Reports, Said, ServiceNow, SettingsOf, Snapshots,
    WatchScope, control,
};
use edgerush_kubernetes::lease::{self, elect};
use edgerush_kubernetes::{ApiError, ApiServer, Listed, Retry, SERVICE_ACCOUNT, Scope, follow};
use edgerush_planes::data_plane::DataPlane;
use edgerush_translate::{Plane, Translation};
use http::StatusCode;
use k8s_openapi::api::networking::v1::IngressClass;
use std::cell::RefCell;
use std::collections::BTreeMap;
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
         [--namespace <NAME>... | --namespace-selector <SELECTOR>] [--write-config <FILE>]
         [--listen <ADDRESS> (--certificate <FILE> --key <FILE> --authorities <FILE>
                              | --authority <SECRET> [--authority-provided])]
         [--leader-lease <NAMESPACE>/<NAME>]
         [--webhook-listen <ADDRESS> [--webhook-certificate <FILE> --webhook-key <FILE>]]
         [--metrics <ADDRESS>] [--health <ADDRESS>] [--peers <ADDRESS>]

Runs a control plane for its data planes, in a cluster: follows the IngressClasses,
Ingresses, Services and Secrets of its scope through the API server, found as a pod finds
it, and translates them for each data plane on its own each time they change, once every
kind has been listed whole. What each translation came to is said on stderr, as `edgerush
validate` says it, under the DataPlane it is of.
The data planes' settings are a file's, one, with --data-plane, or, with --control-plane,
those of every DataPlane of the pod's own namespace that names that ControlPlane, followed:
a generation of each is acted on once the operator's Accepted condition is of it, its last
accepted kept meanwhile, and nothing is translated for it until one is. One added, edited
or let go of leaves the others as they were. What a DataPlane has no field for takes
its default: no authorities for wss backends, the data plane's own settings left out.
With --listen, it serves the config stream there, gRPC over mutual TLS that data planes
dial: each config translated is offered to the data plane whose certificate carries the
SPIFFE ID the settings state, its DataPlane's with --control-plane, or to any where a file
states none, and what each says of it is said on stderr; another is refused, and told
why. The certificate, key and authorities are PEM files, read again every second, so that
ones replaced serve the next connection. With --control-plane, the
addresses of the gateway's own pods, every endpoint of its own Service and of each data
plane's, ready or not, as the EndpointSlices of the pod's own namespace say them, are sent
on the stream too, for data planes never to send a request to.
Where a data plane is a Gateway, its GatewayClass, the Gateway and each HTTPRoute of it
say in their status what is served of them, the Gateway programmed once every ready pod of
that data plane runs its generation, as the data planes say on the stream.
With --control-plane, it writes each data plane's Service, edgerush-proxy-<DATAPLANE>, as
its DataPlane shapes it, listing the ports each config handed on binds and Ingress's two;
each Ingress served says that Service's addresses in its status, each of the class that is
not says none, and the Gateway says them: a LoadBalancer's, once its load balancer gives
them, any other's its cluster IPs. Each Ingress of a class <CONTROLPLANE>.<NAME> that no
DataPlane of that name naming the ControlPlane may still serve says none too, its scope
followed while no DataPlane is served. With --data-plane there is no such Service, and no
status says an address. With --leader-lease, the replicas elect one by that Lease, which
alone writes the Service and status; every one serves, and reports what its data planes run in a ConfigMap of the
Lease's namespace, <LEASE>-acks-<HOSTNAME>, owned by its Pod where EDGERUSH_POD_UID gives
the Pod's UID, for the leader to merge.
With --webhook-listen, it serves the admission webhook there, HTTPS, reviews POSTed to
/validate: an Ingress of a data plane's class is refused where, alone, it does not
translate into something the data plane accepts. Its certificate and key are files read
again every second, or, with --authority and no files, the control plane's own certificate,
as the stream is served with, each one signed taking the next connection; nothing is
answered until the first is. Until the IngressClasses are listed it answers 503, which the
API server takes as an outage, admitting the Ingress where the webhook fails open.
A BackendTLSPolicy's `wellKnownCACertificates: System` trusts the authorities its own
system does, read as it starts: the PEM file SSL_CERT_FILE names, else the system's bundle,
/etc/ssl/certs/ca-certificates.crt, /etc/ssl/cert.pem or /etc/pki/tls/certs/ca-bundle.crt,
the first there is. With none, the backends of such a policy fail closed.
A certificate handed to the data plane expired or not yet valid is served as any other,
and said on stderr, once for each version of it handed out, by the leader alone where
replicas elect one, and again by each new leader. With --metrics, every replica serves its
own metrics there, plain HTTP, at /metrics: when each certificate handed out expires.
With --health, every replica answers its readiness probe there, plain HTTP, at /ready:
503 until it is ready, once the stream serves with its first certificate or at once
without --listen, and 200 from then on; each connection one request, done within a second.
With --peers, the replicas share what translation cannot read back from the objects (04
§2): each serves the snapshot it holds there, gRPC over mutual TLS with its own certificate,
taking the control plane's own identity alone; one that follows finds the leader, the pod
the Lease names among the endpoints of its own Service, follows its snapshot, and translates
from it; and none is ready until it holds one.
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
      --namespace-selector <SELECTOR>
                             Instead of --namespace: the namespaces whose labels
                             SELECTOR matches, written as the API server reads one,
                             such as team=payments,env in (canary,prod),!legacy;
                             followed as they come to match, each taken in once every
                             kind of it has been listed, and stop; empty, every
                             namespace
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
      --leader-lease <NAMESPACE>/<NAME>
                             The Lease the controller's replicas elect a leader by, which
                             alone writes the Service and status; left out, there is no
                             election
      --webhook-listen <ADDRESS>
                             Serve the admission webhook on ADDRESS, such as [::]:8443;
                             given with the two below, or with --authority alone
      --webhook-certificate <FILE>
                             The webhook's certificate, then its intermediates
      --webhook-key <FILE>   Its private key
      --metrics <ADDRESS>    Serve /metrics on ADDRESS, such as [::]:9090
      --health <ADDRESS>     Serve /ready on ADDRESS, such as [::]:8081
      --peers <ADDRESS>      Serve the replicas' snapshots on ADDRESS, such as [::]:9444,
                             and follow the leader's there; with --authority and
                             --leader-lease
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
                    namespace_name: own_namespace_name()?,
                    own: data_plane::Own {
                        control_plane: control_plane.clone(),
                        namespace: own_namespace_name()?,
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
                            scope: options.scope.clone(),
                            control_plane: control_plane.clone(),
                            cluster_domain: cluster_domain.clone(),
                            authority: secret.clone(),
                            kind: *kind,
                        },
                    })
                }
                None => None,
            };
            let webhook = match options.webhook.as_deref() {
                Some(Webhook {
                    files: Some(files), ..
                }) => Some(runtime.block_on(webhook_certificate(files))?),
                _ => None,
            };
            let api = ApiServer::in_cluster().map_err(|error| outside(&error))?;
            // The gateway's own pods, where the stream is served of a followed DataPlane, on
            // a client of their own.
            let own_pods = match (&options.settings, &options.stream) {
                (Settings::DataPlane { control_plane, .. }, Some(_)) => Some(OwnPods {
                    api: ApiServer::in_cluster().map_err(|error| outside(&error))?,
                    namespace: own_namespace_name()?,
                    control_plane: control_plane.clone(),
                }),
                _ => None,
            };
            Ok((plane, stream, webhook, api, own_pods))
        });
    let (plane, credentials, webhook_certificate, api, own_pods) = match started {
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
                    (Initial::Signed { api, issuer }, _) => Credentialing::Signed {
                        api,
                        issuer,
                        own: watch::channel(None).0,
                    },
                    (Initial::Files(_), StreamCredentials::Authority { .. }) => {
                        return writeln!(stderr, "error: the stream's credentials are not its own")
                            .map_or(1, |()| 1);
                    }
                },
                versions: Versions::new(started_at()),
                own: own_pods,
            })
        }
        _ => None,
    };
    let certifying = match (options.webhook.as_deref(), webhook_certificate) {
        (
            Some(Webhook {
                files: Some(files), ..
            }),
            Some(certificate),
        ) => Some(Certifying::Files {
            files: files.clone(),
            certificate,
            reread: REREAD,
        }),
        // The control plane's own certificate, which the issuer signs for the stream too.
        (Some(Webhook { files: None, .. }), _) => match serving.as_ref().map(|s| &s.credentials) {
            Some(Credentialing::Signed { own, .. }) => Some(Certifying::Signed(own.subscribe())),
            _ => {
                return writeln!(
                    stderr,
                    "error: the webhook's certificate is not signed here"
                )
                .map_or(1, |()| 1);
            }
        },
        _ => None,
    };
    let hooking = match (&options.webhook, certifying) {
        (Some(webhook), Some(certifying)) => {
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
                certifying,
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
    let health = match options.health {
        Some(address) => match std::net::TcpListener::bind(address) {
            Ok(listener) => Some(listener),
            Err(error) => return writeln!(stderr, "error: {address}: {error}").map_or(1, |()| 1),
        },
        None => None,
    };
    let peers = match options.peers {
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
                health,
                peers,
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
    /// The scope: its namespaces, the cluster, or a selector's namespaces.
    scope: WatchScope,
    write_config: Option<PathBuf>,
    stream: Option<Box<Stream>>,
    /// The Lease a leader is elected by, as its namespace and name, if one is.
    leader: Option<(String, String)>,
    webhook: Option<Box<Webhook>>,
    /// Where its own metrics are served, if they are.
    metrics: Option<SocketAddr>,
    /// Where its readiness probe is answered, if it is.
    health: Option<SocketAddr>,
    /// Where it serves the replicas' snapshots, and follows the leader's, if it does.
    peers: Option<SocketAddr>,
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
    /// As the DataPlanes of the pod's own namespace and of its watch scope say them,
    /// followed on a client of their own.
    Followed {
        api: ApiServer,
        /// That namespace's name, the ControlPlane's.
        namespace_name: String,
        own: data_plane::Own,
    },
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

/// The next certificate the issuer signs that has not been taken yet, waited for until it
/// signs one; none once it is gone. Each is taken once.
async fn next_written(signed: &mut watch::Receiver<Option<Written>>) -> Option<Written> {
    loop {
        // At once for one not yet seen; else once another comes.
        signed.changed().await.ok()?;
        if let Some(written) = signed.borrow_and_update().clone() {
            return Some(written);
        }
    }
}

/// [`next_written`], as the stream's credentials.
async fn next_signed(signed: &mut watch::Receiver<Option<Written>>) -> Option<Credentials> {
    next_written(signed).await.map(|written| Credentials {
        certificate: Certificate {
            chain: written.certificate,
            key: written.key,
        },
        authorities: written.bundle,
    })
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
    /// Its certificate's files; none for the control plane's own, which it signs (05 §2).
    files: Option<CertificateFiles>,
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
    #[error("'--namespace' and '--namespace-selector' are not given together")]
    NamespacesAndSelector,
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
    #[error("'--leader-lease {0}' is not a namespace and a name, as edge/edgerush-control")]
    LeaderLease(String),
    #[error("'--webhook-listen {0}' is not an address and a port, such as [::]:8443")]
    WebhookListen(String),
    #[error("'--metrics {0}' is not an address and a port, such as [::]:9090")]
    Metrics(String),
    #[error("'--health {0}' is not an address and a port, such as [::]:8081")]
    Health(String),
    #[error("'--peers {0}' is not an address and a port, such as [::]:9444")]
    Peers(String),
    #[error(
        "'--peers' is for '--authority', whose certificates the replicas know each other by, and '--leader-lease'"
    )]
    PeersAlone,
    #[error(
        "'--webhook-listen' is given with '--webhook-certificate' and '--webhook-key', or alone with '--authority'"
    )]
    WebhookTogether,
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
    let mut leader = None;
    let mut webhook_listen = None;
    let mut webhook_certificate = None;
    let mut webhook_key = None;
    let mut metrics = None;
    let mut health = None;
    let mut peers = None;
    let mut scope = Vec::new();
    let mut selector = None;
    while let Some(arg) = args.next() {
        let option = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--data-plane" => "--data-plane",
            "--control-plane" => "--control-plane",
            "--cluster-domain" => "--cluster-domain",
            "--write-config" => "--write-config",
            "--namespace" => "--namespace",
            "--namespace-selector" => "--namespace-selector",
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
            "--leader-lease" => "--leader-lease",
            "--webhook-listen" => "--webhook-listen",
            "--webhook-certificate" => "--webhook-certificate",
            "--webhook-key" => "--webhook-key",
            "--metrics" => "--metrics",
            "--health" => "--health",
            "--peers" => "--peers",
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
            // The API server reads it, and says why where it cannot (04 §1).
            "--namespace-selector" => {
                if selector.replace(value).is_some() {
                    return Err(UsageError::Twice(option));
                }
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
            "--health" => {
                let address = value.parse().map_err(|_| UsageError::Health(value))?;
                if health.replace(address).is_some() {
                    return Err(UsageError::Twice(option));
                }
                continue;
            }
            "--peers" => {
                let address = value.parse().map_err(|_| UsageError::Peers(value))?;
                if peers.replace(address).is_some() {
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
    let scope = match (selector, scope.is_empty()) {
        (Some(_), false) => return Err(UsageError::NamespacesAndSelector),
        (Some(selector), true) => WatchScope::Selector(selector),
        (None, true) => WatchScope::Parts(vec![Scope::Cluster]),
        (None, false) => WatchScope::Parts(scope),
    };
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
    let signs = matches!(
        stream.as_deref(),
        Some(Stream {
            credentials: StreamCredentials::Authority { .. },
            ..
        })
    );
    let webhook = match (webhook_listen, webhook_certificate, webhook_key) {
        (Some(listen), Some(certificate), Some(key)) => Some(Box::new(Webhook {
            listen,
            files: Some(CertificateFiles { certificate, key }),
        })),
        // Served with the control plane's own certificate, as the stream is.
        (Some(listen), None, None) if signs => Some(Box::new(Webhook {
            listen,
            files: None,
        })),
        (None, None, None) => None,
        _ => return Err(UsageError::WebhookTogether),
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
    if peers.is_some() && !(signs && leader.is_some()) {
        return Err(UsageError::PeersAlone);
    }
    Ok(Parsed::Control(Box::new(Options {
        settings,
        scope,
        write_config,
        stream,
        leader,
        webhook,
        metrics,
        health,
        peers,
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
    certifying: Certifying,
}

/// Where the webhook's certificate comes from.
enum Certifying {
    /// Files, what they held as it started, and how often they are read again.
    Files {
        files: CertificateFiles,
        certificate: Certificate,
        reread: Duration,
    },
    /// The control plane's own, each as the issuer signs it (05 §2).
    Signed(watch::Receiver<Option<Written>>),
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
    /// Signed from the authority, by the issuer (05 §2), each handed to `own`, which the
    /// webhook too may be served from.
    Signed {
        api: Box<ApiServer>,
        issuer: Issuer,
        own: watch::Sender<Option<Written>>,
    },
}

/// The config stream, ready to be served.
struct Serving {
    listener: std::net::TcpListener,
    credentials: Credentialing,
    versions: Versions,
    /// Where the gateway's own pods are followed, if they are.
    own: Option<OwnPods>,
}

/// Where the gateway's own pods are followed: the EndpointSlices of the pod's own
/// namespace, for the Services of `control_plane` and its data planes, and of each data
/// plane's own namespace, for its Service (03 §6).
struct OwnPods {
    api: ApiServer,
    namespace: String,
    control_plane: String,
}

/// What the control plane serves, besides writing status.
struct Listeners {
    /// The config stream, if it is served.
    stream: Option<Serving>,
    /// The admission webhook, if it is served.
    webhook: Option<Hooking>,
    /// The control plane's own metrics, if they are served.
    metrics: Option<std::net::TcpListener>,
    /// Its readiness, if its probe is answered.
    health: Option<std::net::TcpListener>,
    /// The replicas' snapshots, if they share them.
    peers: Option<std::net::TcpListener>,
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
    let mut electing = Vec::new();
    // What each data plane this replica serves runs, as it says it (04 §6).
    let runs: Rc<RefCell<Runs>> = Rc::default();
    let (pods, running) = watch::channel(Pods::new());
    let mut reports = None;
    // Whom the Lease names its holder, where the replicas share a snapshot: the leader a
    // follower follows (04 §2).
    let mut holder = None;
    // Where the replicas share a snapshot: whether this one leads, and whether it is elected.
    let mut taking = None;
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
            let (holding, held) = watch::channel(None);
            let sharing = options.peers.is_some();
            // Where the replicas share a snapshot, one elected leads only once it has taken
            // the newest there is (04 §2): until then, the election's word is its own.
            let elected_out = if sharing {
                holder = Some(held);
                let (elected_out, elected) = watch::channel(false);
                taking = Some((leads, elected));
                elected_out
            } else {
                leads
            };
            electing.push(tokio::task::spawn_local(async move {
                elect(
                    &elector.api,
                    &namespace,
                    &name,
                    &elector.identity,
                    elector.timing,
                    &elected_out,
                    sharing.then_some(&holding),
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
    // The data planes' settings, as they are or as their DataPlanes say them. Held for as
    // long as the control plane runs, as nothing more is said of fixed ones.
    let (settings, planes) = watch::channel(Planes::new());
    // A ControlPlane's classes, by which the Ingresses no DataPlane may still serve lose
    // their address (04 §6); the development harness has none.
    // The replicas' snapshot, where they share one (04 §2): the leader takes its records
    // into it, a follower the leader's; and the settings a record's DataPlane is served with.
    let snapshot = options
        .peers
        .map(|_| watch::channel(None::<Arc<Snapshot>>).0);
    let mut settings_of: Option<Rc<SettingsOf>> = None;
    let mut peers_listener = listeners.peers;
    let (_fixed, classes) = match source {
        Source::Fixed(plane) => {
            // The development harness's one data plane, of no name: no Service is written
            // (05 §2).
            settings.send_replace(Planes::from([(
                String::new(),
                edgerush_controller::Settings {
                    plane,
                    service: None,
                    generated: false,
                    data_plane: None,
                },
            )]));
            (Some(settings), None)
        }
        Source::Followed {
            api,
            namespace_name,
            own,
        } => {
            let (keeping, kept) = watch::channel(None);
            let classes = Classes {
                control_plane: own.control_plane.clone(),
                namespace: namespace_name,
                kept,
            };
            let made = own.clone();
            settings_of = Some(Rc::new(move |data_plane: &DataPlane| {
                data_plane::settings(data_plane, &made).ok()
            }));
            let telling = Rc::clone(&say);
            tasks.push(tokio::task::spawn_local(follow_data_planes(
                api,
                scope.clone(),
                own,
                settings,
                keeping,
                snapshot.as_ref().map(watch::Sender::subscribe),
                move |line| telling(line),
            )));
            (None, Some(classes))
        }
    };
    // The dates of the certificates handed out (04 §2, 08 §1): every replica keeps them
    // for its gauge, and the leader says those out of date.
    let expiry = Rc::new(RefCell::new(Expiry::new()));
    // The RateLimitPolicies each data plane leaves out, of every replica's translations
    // (04 §6): served beside the certificates'.
    let unimplementable = RefCell::new(Unimplementable::new());
    let exposed_text = |expiry: &Expiry, unimplementable: &Unimplementable| {
        Bytes::from(expiry.exposition() + &unimplementable.exposition())
    };
    let (exposed, exposition) =
        watch::channel(exposed_text(&expiry.borrow(), &unimplementable.borrow()));
    // Whether this replica is ready, for its readiness probe: once the stream serves, with
    // its first certificate, or at once where it serves none.
    let (readies, ready) = watch::channel(false);
    // Whether the stream serves, which readiness waits on.
    let (streams, streaming) = watch::channel(false);
    let mut probed = None;
    // The replicas' listener, where they share a snapshot.
    let mut shared = None;
    if let Some(listener) = listeners.health {
        listener.set_nonblocking(true)?;
        let listener = tokio::net::TcpListener::from_std(listener)?;
        let (stopping, stopped) = oneshot::channel::<()>();
        probed = Some((
            stopping,
            tokio::spawn(health::serve(
                listener,
                ready,
                health::DEADLINE,
                async move {
                    let _ = stopped.await;
                },
            )),
        ));
    }
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
            let (offers, offered) = watch::channel(Served::default());
            // Whom the stream serves follows the settings, so that a data plane is let in or
            // refused as its DataPlane is served or not, before anything is translated for it;
            // what was offered to one no longer served is forgotten with it.
            let (mut told, whose) = (planes.clone(), offers.clone());
            let (forgetting, telling_pods) = (Rc::clone(&runs), pods.clone());
            tasks.push(tokio::task::spawn_local(async move {
                loop {
                    let next = {
                        let now = told.borrow_and_update();
                        let mut runs = forgetting.borrow_mut();
                        runs.serving(now.values().map(|settings| settings.plane.id.as_deref()));
                        said_runs(&telling_pods, &runs);
                        whom(&now)
                    };
                    whose.send_if_modified(|served| served.serve(next));
                    if told.changed().await.is_err() {
                        return;
                    }
                }
            }));
            let (gateway, addresses) = watch::channel::<Arc<[IpAddr]>>(Arc::from([]));
            let (replicas_out, replicas) = watch::channel::<Arc<[Replica]>>(Arc::from([]));
            if let Some(own) = serving.own {
                let telling = Rc::clone(&say);
                tasks.push(tokio::task::spawn_local(follow_own_pods(
                    own.api,
                    own.namespace,
                    own.control_plane,
                    planes.clone(),
                    gateway,
                    snapshot.is_some().then_some(replicas_out),
                    move |line| telling(line),
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
                    streams.send_replace(true);
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
                Credentialing::Signed { api, issuer, own } => {
                    // Its own certificate, as the issuer signs it: the stream is served from
                    // the first on, each after it taking the next connection (05 §2).
                    let mut signed = own.subscribe();
                    // The replicas' snapshots, served with that certificate too, and the
                    // leader's followed (04 §2).
                    if let (Some(snapshot), Some(listener), Some(holder)) =
                        (&snapshot, peers_listener.take(), holder.take())
                    {
                        let leaf = issuer.own_leaf();
                        let peers = peers::Peers {
                            id: leaf.spiffe_id.clone(),
                            name: leaf.dns.first().cloned().unwrap_or_default(),
                            port: listener.local_addr()?.port(),
                        };
                        // One that cannot follow the leader at all starts clean in time.
                        tasks.push(tokio::task::spawn_local(peers::clean_after(
                            peers::CLEAN_AFTER,
                            snapshot.clone(),
                            Rc::clone(&say),
                        )));
                        let (credentialing, credentials) = watch::channel(None);
                        tasks.push(tokio::task::spawn_local(peers::credentials(
                            own.subscribe(),
                            credentialing,
                        )));
                        listener.set_nonblocking(true)?;
                        let listener = tokio::net::TcpListener::from_std(listener)?;
                        let (stopping, stopped) = oneshot::channel::<()>();
                        shared = Some((
                            stopping,
                            tokio::task::spawn_local(peers::serve_snapshot(
                                listener,
                                credentials.clone(),
                                peers.id.clone(),
                                snapshot.subscribe(),
                                Rc::clone(&say),
                                async move {
                                    let _ = stopped.await;
                                },
                            )),
                        ));
                        if let Some((leads, elected)) = taking.take() {
                            tasks.push(tokio::task::spawn_local(peers::take_over(
                                peers.clone(),
                                std::env::var("HOSTNAME").unwrap_or_default(),
                                peers::GATHER,
                                elected.clone(),
                                replicas.clone(),
                                credentials.clone(),
                                snapshot.clone(),
                                leads,
                                Rc::clone(&say),
                            )));
                            // Followed until elected, not until it leads: one elected asks
                            // the others itself.
                            tasks.push(tokio::task::spawn_local(peers::follow_leader(
                                peers,
                                elected,
                                holder,
                                replicas.clone(),
                                credentials,
                                snapshot.clone(),
                                Rc::clone(&say),
                            )));
                        }
                    }
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
                            streams.send_replace(true);
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
                served: offers,
                runs: Rc::clone(&runs),
                pods: pods.clone(),
            })
        }
        None => {
            streams.send_replace(true);
            None
        }
    };
    tasks.push(tokio::task::spawn_local(readiness(
        streaming,
        snapshot.as_ref().map(watch::Sender::subscribe),
        readies,
    )));
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
            classes,
            Debounce::default(),
            leading,
            Acks {
                own: running,
                reports,
            },
            snapshot
                .clone()
                .zip(settings_of)
                .map(|(snapshot, settings)| Snapshots { snapshot, settings }),
            move |said| {
                let mut text = told(&said, write_config.as_deref());
                let offered = match (&said, &offering) {
                    (
                        Said::Translated {
                            translation, id, ..
                        },
                        Some(offering),
                    ) => Some(offering.offer(translation, *id)),
                    // The leader's: of no generation this replica knows its Gateway's to be.
                    (Said::Kept { config, id, .. }, Some(offering)) => {
                        Some(offering.offer_config(config, None, *id))
                    }
                    _ => None,
                };
                // Writing to a `String` cannot fail.
                let _ = match offered {
                    Some(Ok(version)) => write!(text, "\noffered as config {version}"),
                    Some(Err(error)) => write!(text, "\nnot offered: {error}"),
                    None => Ok(()),
                };
                telling(&text);
                // The certificates handed on, the leader's kept among them, for the gauge.
                let handed = match &said {
                    Said::Translated {
                        translation,
                        data_plane,
                        ..
                    } => {
                        unimplementable.borrow_mut().take(data_plane, translation);
                        Some(&translation.config)
                    }
                    Said::Kept { config, .. } => Some(*config),
                    _ => None,
                };
                if let Some(config) = handed {
                    // Without an election, this replica is the only one, and leads.
                    let leading = leads.as_ref().is_none_or(|leads| *leads.borrow());
                    let out_of_date =
                        expiry
                            .borrow_mut()
                            .take(&config.certificates, unix_now(), leading);
                    exposed.send_replace(exposed_text(&expiry.borrow(), &unimplementable.borrow()));
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
    if let Some((stopping, server)) = probed {
        let _ = stopping.send(());
        let _ended = server.await;
    }
    if let Some((stopping, server)) = shared {
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

/// Publishes on `readies` whether this replica is ready, as each part changes: once the
/// stream serves, as `streaming` says, and, where the replicas share a snapshot, once this
/// one holds one in `holding` (04 §2), the leader its own, taken as it translates, a
/// follower the leader's. Once the stream is done saying, the last it said stands.
async fn readiness(
    mut streaming: watch::Receiver<bool>,
    mut holding: Option<watch::Receiver<Option<Arc<Snapshot>>>>,
    readies: watch::Sender<bool>,
) {
    let mut open = true;
    loop {
        let holds = holding
            .as_mut()
            .is_none_or(|holding| holding.borrow_and_update().is_some());
        let now = *streaming.borrow_and_update() && holds;
        readies.send_if_modified(|ready| std::mem::replace(ready, now) != now);
        let held = async {
            match holding.as_mut() {
                Some(holding) => holding.changed().await.is_ok(),
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            changed = streaming.changed(), if open => open = changed.is_ok(),
            still = held => if !still { return },
            () = readies.closed() => return,
        }
    }
}

/// The latest offer, as data planes are sent it.
struct Offering {
    versions: RefCell<Versions>,
    /// Whom the stream serves, and what it offers them.
    served: watch::Sender<Served>,
    /// What each data plane runs, each version offered among it.
    runs: Rc<RefCell<Runs>>,
    pods: watch::Sender<Pods>,
}

impl Offering {
    /// Offers `translation`'s config to the data plane whose certificate carries `id`, or to
    /// any the stream serves where it is none, built on what it was offered last: its
    /// version, said as offered. Every other data plane's offer is left as it was.
    fn offer(&self, translation: &Translation, id: Option<&str>) -> Result<String, OfferError> {
        let generation = translation.gateway_status.as_ref().map(|s| s.generation);
        self.offer_config(&translation.config, generation, id)
    }

    /// Offers `config` as [`Self::offer`] does a translation's: of its Gateway's
    /// `generation`, where it serves one whose generation it is known to be of.
    fn offer_config(
        &self,
        config: &Config,
        generation: Option<i64>,
        id: Option<&str>,
    ) -> Result<String, OfferError> {
        // Never a data plane no longer served: a config is said only for those `control`
        // serves now.
        let mut served = self.served.borrow().clone();
        let last = served.offer_to(id).ok().flatten().cloned();
        let offer = Offer::new(config, last.as_deref(), &mut self.versions.borrow_mut())?;
        let version = offer.version().to_owned();
        let mut runs = self.runs.borrow_mut();
        runs.offered(id, &version, generation);
        said_runs(&self.pods, &runs);
        served.offered(id, Arc::new(offer));
        self.served.send_replace(served);
        Ok(version)
    }
}

/// Whom the stream serves of `planes`: each data plane whose certificate carries the ID
/// its settings state, any the stream's authorities vouch for where a settings file states
/// none, and none without settings (05 §3).
fn whom(planes: &Planes) -> Served {
    Served::of(planes.values().map(|settings| settings.plane.id.as_deref()))
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
/// settings `planes` holds as it comes describe, and its certificate files read again, or
/// each certificate the issuer signs taken, nothing answered until the first. What it says
/// goes to `said`, a line each; it stops once `stop` is done, or dropped, and tells `ended`
/// as it ends.
///
/// # Errors
///
/// If its certificate files cannot be used, its listener cannot be made, or the thread or
/// its runtime cannot be started.
fn hook(
    hooking: Hooking,
    planes: watch::Receiver<Planes>,
    said: mpsc::UnboundedSender<String>,
    mut stop: oneshot::Receiver<()>,
    ended: oneshot::Sender<()>,
) -> io::Result<()> {
    let from_files = match &hooking.certifying {
        Certifying::Files { certificate, .. } => {
            Some(webhook_acceptor(certificate).map_err(io::Error::other)?)
        }
        Certifying::Signed(_) => None,
    };
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
                let replacing = said.clone();
                let (accepting, rereading) = match (hooking.certifying, from_files) {
                    (
                        Certifying::Files {
                            files,
                            certificate,
                            reread: every,
                        },
                        Some(acceptor),
                    ) => {
                        let tls = watch::Sender::new(Arc::new(acceptor));
                        let accepting = tls.subscribe();
                        let rereading = tokio::task::spawn_local(reread(
                            files,
                            certificate,
                            every,
                            move |changed| {
                                let _gone = replacing.send(webhook_replaced(changed, &tls));
                            },
                        ));
                        (accepting, rereading)
                    }
                    (Certifying::Signed(mut signed), _) => {
                        // Nothing taken from the listener until the first it can serve with:
                        // the API server, unanswered, admits as for an outage.
                        let first = tokio::select! {
                            first = first_signed(&mut signed, &replacing) => first,
                            _ = &mut stop => None,
                        };
                        let Some(first) = first else {
                            following.abort();
                            return;
                        };
                        let tls = watch::Sender::new(Arc::new(first));
                        let accepting = tls.subscribe();
                        let rereading = tokio::task::spawn_local(async move {
                            while let Some(next) = next_written(&mut signed).await {
                                let _gone = replacing.send(webhook_signed(&next, &tls));
                            }
                        });
                        (accepting, rereading)
                    }
                    (Certifying::Files { .. }, None) => {
                        following.abort();
                        return;
                    }
                };
                match tokio::net::TcpListener::from_std(hooking.listener) {
                    Ok(listener) => {
                        webhook::serve(
                            listener,
                            accepting,
                            Bounds::default(),
                            move |body| review(&planes.borrow(), &classes.borrow(), body),
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
fn review(planes: &Planes, classes: &Listed<IngressClass>, body: &[u8]) -> webhook::Answer {
    let answer = |status, body: Vec<u8>| webhook::Answer { status, body };
    if planes.is_empty() {
        let why = "the data plane's settings are not known yet";
        return answer(StatusCode::SERVICE_UNAVAILABLE, why.as_bytes().to_vec());
    }
    if !classes.listed {
        let why = "the IngressClasses are not listed yet";
        return answer(StatusCode::SERVICE_UNAVAILABLE, why.as_bytes().to_vec());
    }
    match admission::read(body) {
        Ok(review) => {
            let refused = match &review.asked {
                // Only the data plane whose class it is says anything of it.
                Asked::Ingress(ingress) => planes.values().find_map(|settings| {
                    admission::judge(&settings.plane, &classes.objects, ingress)
                }),
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

/// The first certificate the issuer signs that the webhook can serve with, those it cannot
/// said to `said`; none once the issuer is gone.
async fn first_signed(
    signed: &mut watch::Receiver<Option<Written>>,
    said: &mpsc::UnboundedSender<String>,
) -> Option<SslAcceptor> {
    loop {
        let written = next_written(signed).await?;
        match webhook_acceptor(&own_certificate(&written)) {
            Ok(acceptor) => return Some(acceptor),
            Err(error) => {
                let _gone = said.send(format!("the webhook's certificate cannot be used: {error}"));
            }
        }
    }
}

/// The certificate and key of the control plane's own certificate `written`.
fn own_certificate(written: &Written) -> Certificate {
    Certificate {
        chain: written.certificate.clone(),
        key: written.key.clone(),
    }
}

/// What is said of the issuer signing the control plane's own certificate again: put to use
/// for the webhook's next connection, or kept from it if it cannot be used.
fn webhook_signed(written: &Written, tls: &watch::Sender<Arc<SslAcceptor>>) -> String {
    match webhook_acceptor(&own_certificate(written)) {
        Ok(replaced) => {
            tls.send_replace(Arc::new(replaced));
            "the webhook's certificate is replaced".to_owned()
        }
        Err(error) => {
            format!("the webhook's certificate cannot be used, and the last is kept: {error}")
        }
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

/// The line that says which data plane a translation is of, where it is of one of several
/// DataPlanes: none for the development harness's one.
fn of_data_plane(data_plane: &str) -> String {
    if data_plane.is_empty() {
        String::new()
    } else {
        format!(
            "{data_plane}:
"
        )
    }
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
        Said::Namespace { namespace, now } => match now {
            Membership::Joining => {
                format!("namespace {namespace} matches; taken in once every kind of it is listed")
            }
            Membership::Joined => format!("namespace {namespace} taken in"),
            Membership::Left => format!("namespace {namespace} no longer matches; dropped"),
        },
        Said::GatewayApiStatus { object, written } => match written {
            Ok(()) => format!("status of {object} written"),
            Err(error) => format!("status of {object} cannot be written: {error}"),
        },
        Said::Service { service, now } => match now {
            ServiceNow::Applied(ports) => {
                format!("Service {service} written: {}", ports.join(", "))
            }
            ServiceNow::Refused(error) => format!("Service {service} cannot be written: {error}"),
            ServiceNow::NotOurs => format!(
                "Service {service} is not its DataPlane's, and is left alone; no status says its addresses"
            ),
            ServiceNow::NoPort => format!(
                "Service {service} is left as it is: the config binds no port, and no Service is without one"
            ),
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
        Said::Translated {
            data_plane,
            translation,
            ..
        } => {
            let mut text = of_data_plane(data_plane);
            text.push_str(&described(translation));
            if let Some(path) = write_config {
                // Writing to a `String` cannot fail.
                let _ = match written(&translation.config, path) {
                    Ok(()) => write!(text, "written to {}", path.display()),
                    Err(error) => {
                        write!(text, "error: {} cannot be written: {error}", path.display())
                    }
                };
            }
            text.trim_end().to_owned()
        }
        Said::Refused {
            data_plane,
            translation,
            problems,
        } => {
            let mut text = of_data_plane(data_plane);
            text.push_str(&described(translation));
            let _ = write!(
                text,
                "the data plane would refuse the config, which is not handed on:"
            );
            for problem in *problems {
                let _ = write!(text, "\n  {problem}");
            }
            text
        }
        Said::Kept {
            data_plane, config, ..
        } => {
            let mut text = of_data_plane(data_plane);
            text.push_str(
                "the leader's config last handed on is handed on in place of this replica's \
                 translation, refused whole, or of its Gateway, held",
            );
            if let Some(path) = write_config {
                let _ = match written(config, path) {
                    Ok(()) => write!(text, "; written to {}", path.display()),
                    Err(error) => {
                        write!(
                            text,
                            "; error: {} cannot be written: {error}",
                            path.display()
                        )
                    }
                };
            }
            text
        }
        Said::HeldBack { data_plane, why } => format!(
            "{}its DataPlane as it now is is held back, the generation before served: {why}",
            of_data_plane(data_plane)
        ),
    }
}

/// Writes `config` to `path` whole: into a file beside it, then put in its place, so that
/// nothing reading `path` ever finds half of one.
fn written(config: &Config, path: &Path) -> io::Result<()> {
    let yaml = serde_saphyr::to_string(config).map_err(io::Error::other)?;
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

mod peers;

#[cfg(test)]
mod tests;
