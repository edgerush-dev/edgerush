//! The rate-limiting service pod (a prototype):
//!
//! ```text
//! edgerush-ratelimit-service --config service.yaml
//! ```
//!
//! The file (`edgerush_config::ServiceFile`, its schema `schema/ratelimit-service.schema.json`)
//! is read once, at the start, and checked (`edgerush_ratelimit_service::service_file`):
//!
//! ```yaml
//! pod:
//!   listen: 0.0.0.0:7000
//!   address: rl-0.rl:7000
//!   metrics: 0.0.0.0:9090     # nowhere when left out
//! store:
//!   seeds: [rl-0:7000]        # none when left out
//!   shards: 256               # each as the store's defaults when left out
//!   replicas: 2
//!   heartbeat_ms: 100
//!   stop_serving_ms: 400
//!   silence_ms: 600
//!   election_jitter_ms: 200
//!   cycle_ms: 10
//!   skew_warning_ms: 250
//!   forwarder_decides: true   # a replica decides an unheard owner's synchronous keys from
//!                             # its copy
//! keys:
//!   max_bytes: 268435456      # 256 MiB when left out
//! tls:                        # served plain, to anyone, when left out
//!   certificate: pod.pem
//!   key: pod.key
//!   authorities: ca.pem
//!   data_plane_id: spiffe://cluster.local/ns/edge/sa/edge
//! ```
//!
//! A pod serves both modes, on one cluster store (`edgerush_cluster`, the pod of both modes
//! `edgerush_cluster_modes`): each data plane's stream is its mode's, `Decide` or `Exchange`,
//! and each key's top bit says its mode, so the file names none. `store` is what it runs the
//! store with, which every pod of a cluster is to share, and `pod.address` where the other
//! pods and data planes reach it; both are needed (`edgerush_ratelimit_service::cluster::settings`).
//! A pod takes a member id of its own each time it starts, shows its store's `/metrics` and what
//! each mode counts, says at `/ready` whether it is ready, and, told to stop, hands its shards on
//! before it does. `keys.max_bytes` is what its keys of both modes may hold, past which those
//! that may be forgotten soonest are evicted, whatever their mode.
//!
//! The asynchronous mode applies each data plane's batch to the keys it owns, by the data
//! planes' cycle the batch says, forwards the entries of keys another pod owns to it and
//! relays its answers back. The synchronous mode forwards a request for a key not its own to
//! the key's owner, which a replica holding its lease decides itself, from its copy, while the
//! owner is unheard, unless `store.forwarder_decides` says not.
//!
//! `tls`'s files, relative to the config file's directory unless absolute, are read again
//! every second, and serve the service with mutual TLS (`edgerush_ratelimit_service::secure`):
//! the pod presents `certificate` to data planes and to the other pods alike, trusts the
//! `authorities` to vouch for every caller, and tells them apart by the SPIFFE ID their
//! certificate carries: its own is the pods', `data_plane_id` the data plane's it serves,
//! and any other is refused. It dials the other pods checking theirs for its own ID.

use boring::ssl::SslConnector;
use edgerush_cluster_modes::Pod;
use edgerush_control_service::open_files::{self, OpenFiles};
use edgerush_control_service::reread;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::mode::Mode;
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer};
use edgerush_ratelimit_service::cluster::settings::member_id;
use edgerush_ratelimit_service::scrape::{self, Counted};
use edgerush_ratelimit_service::secure::{self, PodFiles, PodTls};
use edgerush_ratelimit_service::service_file::{PodSettings, parsed};
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::server::NamedService;

/// How often the certificate files are read again, to take in a Secret replaced.
const REREAD: Duration = Duration::from_secs(1);

const USAGE: &str = "usage: edgerush-ratelimit-service --config <file>";

/// The config file the command line names: `--config <file>`, and nothing else.
fn config_path(mut args: impl Iterator<Item = String>) -> Result<PathBuf, String> {
    match (args.next().as_deref(), args.next(), args.next()) {
        (Some("--config"), Some(path), None) => Ok(path.into()),
        _ => Err(USAGE.to_owned()),
    }
}

/// The pod's settings from the config file at `path`.
fn read(path: &Path) -> Result<PodSettings, String> {
    let yaml = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let directory = path.parent().unwrap_or(Path::new(""));
    parsed(&yaml, directory).map_err(|error| format!("{}: {error}", path.display()))
}

/// A pod answers on one thread, both modes': a stream's answers and its connection's writes
/// share it, so that what one read brings in is answered in one write, where a connection on
/// another thread would take the answers a few at a time as they are made.
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

fn main() -> ExitCode {
    run(std::env::args().skip(1))
}

/// The pod, from its command line `args` to its end.
fn run(args: impl Iterator<Item = String>) -> ExitCode {
    let path = match config_path(args) {
        Ok(path) => path,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: {error}");
            return ExitCode::from(2);
        }
    };
    // Before a file is opened, its config and its runtime's own among them, as the data
    // plane does: under containerd 2.x the soft limit is 1,024, for every data plane's
    // connection, the other pods', and the scrape's. Said only where it could not be raised,
    // as nothing here is sized by it.
    let open_files = open_files::raise();
    if let (OpenFiles::Kept { .. }, Some(files)) = (open_files, open_files.described()) {
        eprintln!("edgerush-ratelimit-service: warning: {files}");
    }
    let args = match read(&path) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match runtime() {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: no runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(serve(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(args: PodSettings) -> Result<(), Box<dyn std::error::Error>> {
    let tls = match &args.tls {
        Some(files) => Some(secured(files.clone()).await?),
        None => None,
    };
    let listener = tokio::net::TcpListener::bind(args.listen).await?;
    let (serving, dial) = match tls {
        Some((serving, dialling)) => (Some(serving), Dial::Secured(dialling)),
        None => (None, Dial::Plain),
    };
    let id = member_id()?;
    let stop = Running {
        listener,
        serving,
        metrics: args.metrics,
    };
    let store = args.store;
    let address = store.address.clone();
    let mut node = Pod::new(id, address.clone(), store.config, []);
    // A pod is a member in one life only: its id, new each start, serves as its life, never
    // 0, which would leave its asynchronous answers unnumbered.
    node.set_life(id.max(1));
    // Always on (decided): a synchronous data plane out of reach of a key's owner asks another
    // pod.
    node.set_forwarding(true);
    node.set_forwarder_decides(store.forwarder_decides);
    let pod = StorePod::start(node, store.address, dial, store.seeds);
    eprintln!(
        "edgerush-ratelimit-service: pod, member {id}, on {}, reached at {address}",
        args.listen
    );
    stop.run(pod).await
}

/// Where a store pod serves, and its metrics.
struct Running {
    listener: TcpListener,
    serving: Option<watch::Receiver<Arc<PodTls>>>,
    metrics: Option<SocketAddr>,
}

impl Running {
    /// Serves `pod`, its metrics and readiness too if said, until told to stop: it then hands
    /// its shards on while it still serves, and stops once drained.
    async fn run<M: Mode<Counted: Counted>>(
        self,
        pod: StorePod<M>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(at) = self.metrics {
            let listener = tokio::net::TcpListener::bind(at).await?;
            eprintln!("edgerush-ratelimit-service: metrics and readiness on {at}");
            tokio::spawn(scrape::serve_store(listener, pod.clone()));
        }
        let draining = pod.clone();
        let drained = async move {
            stopped().await;
            eprintln!("edgerush-ratelimit-service: draining");
            draining.drain().await;
        };
        serve_on(self.listener, self.serving, StoreServer::new(pod), drained).await?;
        Ok(())
    }
}

/// The pod's TLS from `files`, as it serves and as it dials, each replaced as the files
/// change: a change that cannot be used is said, and the last that could is kept.
async fn secured(
    files: PodFiles,
) -> Result<(watch::Receiver<Arc<PodTls>>, watch::Receiver<SslConnector>), String> {
    let credentials = files.read().await.map_err(|error| error.to_string())?;
    let tls = PodTls::new(&credentials).map_err(|error| error.to_string())?;
    let (dial, dialling) = watch::channel(tls.connector().clone());
    let (serve, serving) = watch::channel(Arc::new(tls));
    tokio::spawn(reread(
        files,
        credentials,
        REREAD,
        move |changed| match changed.map(PodTls::new) {
            Ok(Ok(tls)) => {
                dial.send_replace(tls.connector().clone());
                serve.send_replace(Arc::new(tls));
                eprintln!("edgerush-ratelimit-service: certificates replaced");
            }
            Ok(Err(error)) => eprintln!("edgerush-ratelimit-service: certificates kept: {error}"),
            Err(error) => eprintln!("edgerush-ratelimit-service: certificates kept: {error}"),
        },
    ));
    Ok((serving, dialling))
}

/// Serves `service` on `listener` until `stop` resolves: with TLS if given, else plain.
async fn serve_on<S>(
    listener: TcpListener,
    tls: Option<watch::Receiver<Arc<PodTls>>>,
    service: S,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), tonic::transport::Error>
where
    S: tower_service::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
            Error = Infallible,
        > + NamedService
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
{
    match tls {
        Some(tls) => {
            let refused = |peer, why| {
                eprintln!("edgerush-ratelimit-service: {peer} refused: {why}");
            };
            secure::serve(listener, tls, service, refused, stop).await
        }
        None => secure::serve_plain(listener, service, stop).await,
    }
}

/// Resolves when the process is told to stop: Ctrl-C, or SIGTERM, which is how Docker and
/// Kubernetes say it.
async fn stopped() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut terminate) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = tokio::signal::ctrl_c() => {}
                _ = terminate.recv() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pod's file, on the store, with `more` after its sections.
    fn with(more: &str) -> String {
        sections("", &format!("store: {{}}\n{more}"))
    }

    /// A pod's file with its `pod` section's `more`, and `rest` after it.
    fn sections(pod: &str, rest: &str) -> String {
        format!("pod: {{ listen: 0.0.0.0:7000, address: rl-0.rl:7000{pod} }}\n{rest}")
    }

    #[test]
    fn the_command_line_names_the_config_file_and_nothing_else() {
        let given = |args: &[&str]| config_path(args.iter().map(|arg| (*arg).to_owned()));
        assert_eq!(
            given(&["--config", "service.yaml"]),
            Ok(PathBuf::from("service.yaml"))
        );
        for args in [
            &[][..],
            &["--config"][..],
            &["service.yaml"][..],
            &["--config", "service.yaml", "--id"][..],
            &["--id", "0"][..],
        ] {
            assert_eq!(given(args), Err(USAGE.to_owned()), "{args:?}");
        }
    }

    /// It raises its soft open-file limit to the hard one before it opens anything, its
    /// config file first, as the data plane does: a container's soft limit of 1,024 would
    /// leave it a thousand files for every data plane's connection, the other pods' and
    /// the scrape's.
    #[cfg(unix)]
    #[test]
    fn it_raises_its_open_file_limit_as_it_starts() {
        use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
        let before = getrlimit(Resource::Nofile);
        // One below what it can be raised to: harmless to tests beside it in one process.
        let lowered = before.maximum.unwrap_or(1 << 20) - 1;
        setrlimit(
            Resource::Nofile,
            Rlimit {
                current: Some(lowered),
                maximum: before.maximum,
            },
        )
        .unwrap();
        let missing = std::env::temp_dir().join("edgerush-no-such-service.yaml");
        let args = ["--config".to_owned(), missing.display().to_string()];
        assert_eq!(run(args.into_iter()), ExitCode::from(2));
        let now = getrlimit(Resource::Nofile);
        match now.maximum {
            Some(hard) => assert_eq!(now.current, Some(hard)),
            None => assert!(now.current >= Some(1 << 20), "{now:?}"),
        }
    }

    #[test]
    fn a_file_is_read_and_its_tls_files_found_beside_it() {
        let directory =
            std::env::temp_dir().join(format!("edgerush-ratelimit-service-{}", std::process::id()));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("service.yaml");
        // An absolute path is taken as it is.
        let elsewhere = std::env::temp_dir().join("elsewhere").join("pod.key");
        std::fs::write(
            &path,
            with(&format!(
                "tls:\n  certificate: pod.pem\n  key: '{}'\n  authorities: ca.pem\n  \
                 data_plane_id: spiffe://cluster.local/ns/edge/sa/edge\n",
                elsewhere.display()
            )),
        )
        .unwrap();
        let file = read(&path);
        let missing = directory.join("missing.yaml");
        let not_read = read(&missing);
        std::fs::remove_dir_all(&directory).unwrap();
        let tls = file.unwrap().tls.unwrap();
        assert_eq!(
            (tls.certificate, tls.key, tls.authorities),
            (
                directory.join("pod.pem"),
                elsewhere,
                directory.join("ca.pem")
            )
        );
        let said = not_read.unwrap_err();
        assert!(
            said.starts_with(&format!("{}: ", missing.display())),
            "{said}"
        );
    }

    #[test]
    fn a_file_refused_is_said_with_its_path() {
        let directory = std::env::temp_dir().join(format!(
            "edgerush-ratelimit-service-refused-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("service.yaml");
        std::fs::write(&path, with("shards: 256\n")).unwrap();
        let refused = read(&path);
        std::fs::remove_dir_all(&directory).unwrap();
        let said = refused.unwrap_err();
        assert!(
            said.starts_with(&format!("{}: ", path.display())) && said.contains("shards"),
            "{said}"
        );
    }

    #[test]
    fn a_pod_runs_on_one_thread() {
        use tokio::runtime::RuntimeFlavor;
        assert_eq!(
            runtime().unwrap().handle().runtime_flavor(),
            RuntimeFlavor::CurrentThread
        );
    }
}
