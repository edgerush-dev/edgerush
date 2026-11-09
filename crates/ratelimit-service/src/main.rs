//! The rate-limiting service pod (a prototype):
//!
//! ```text
//! edgerush-ratelimit-service --config service.yaml
//! ```
//!
//! The file (`edgerush_config::ServiceFile`, its schema `schema/ratelimit-service.schema.json`)
//! is read once, at the start:
//!
//! ```yaml
//! mode: asynchronous          # or synchronous; asynchronous when left out
//! pod:
//!   listen: 0.0.0.0:7000
//!   address: rl-0.rl:7000
//!   metrics: 0.0.0.0:9090     # nowhere when left out
//!   cycle_ms: 10              # the asynchronous mode's; 10 when left out
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
//!   forwarder_decides: true   # the synchronous mode's: a replica decides an unheard owner's
//!                             # keys from its copy
//! keys:
//!   max_bytes: 268435456      # 256 MiB when left out
//! tls:                        # served plain, to anyone, when left out
//!   certificate: pod.pem
//!   key: pod.key
//!   authorities: ca.pem
//!   data_plane_id: spiffe://cluster.local/ns/edge/sa/edge
//! ```
//!
//! A pod serves one mode, on the cluster store (`edgerush_cluster`): `store` is what it runs
//! the store with, which every pod of a cluster is to share, and `pod.address` where the other
//! pods and data planes reach it; both are needed (`edgerush_ratelimit_service::cluster::settings`).
//! A pod takes a member id of its own each time it starts, shows its store's `/metrics` and what
//! its mode counts, says at `/ready` whether it is ready, and, told to stop, hands its shards on
//! before it does. `keys.max_bytes` is what its keys may hold, past which the least recently
//! changed are evicted.
//!
//! The asynchronous mode applies each data plane's batch to the keys it owns, forwards the
//! entries of keys another pod owns to it and relays its answers back, and `pod.cycle_ms` is its
//! data planes' cycle; it refuses `store.forwarder_decides`, as it would not read it. The
//! synchronous mode forwards a request for a key not its own to the key's owner, and refuses
//! `pod.cycle_ms`.
//!
//! `tls`'s files, relative to the config file's directory unless absolute, are read again
//! every second, and serve the service with mutual TLS (`edgerush_ratelimit_service::secure`):
//! the pod presents `certificate` to data planes and to the other pods alike, trusts the
//! `authorities` to vouch for every caller, and tells them apart by the SPIFFE ID their
//! certificate carries: its own is the pods', `data_plane_id` the data plane's it serves,
//! and any other is refused. It dials the other pods checking theirs for its own ID.

use boring::ssl::SslConnector;
use edgerush_cluster::Time;
use edgerush_cluster_asynchronous::Pod as AsyncPod;
use edgerush_config::{ServiceFile, ServiceMode};
use edgerush_control_service::reread;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::mode::Mode;
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer};
use edgerush_ratelimit_service::cluster::settings::{Settings, member_id, settings};
use edgerush_ratelimit_service::scrape::{self, Counted};
use edgerush_ratelimit_service::secure::{self, PodFiles, PodTls};
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
use tonic::transport::server::TcpIncoming;

/// What `keys.max_bytes` is when left out: 256 MiB.
const MAX_KEY_BYTES: u64 = 256 << 20;

/// What `pod.cycle_ms` is when left out.
const CYCLE_MS: u64 = 10;

/// How often the certificate files are read again, to take in a Secret replaced.
const REREAD: Duration = Duration::from_secs(1);

const USAGE: &str = "usage: edgerush-ratelimit-service --config <file>";

#[derive(Debug, PartialEq, Eq)]
struct Args {
    listen: SocketAddr,
    metrics: Option<SocketAddr>,
    tls: Option<PodFiles>,
    serves: Serves,
}

/// The mode a pod serves, with the store it serves it on.
#[derive(Debug, PartialEq, Eq)]
enum Serves {
    /// Its data planes sending every `cycle`.
    Asynchronous {
        store: Settings,
        cycle: Duration,
    },
    Synchronous(Settings),
}

impl Serves {
    fn mode(&self) -> ServiceMode {
        match self {
            Self::Asynchronous { .. } => ServiceMode::Asynchronous,
            Self::Synchronous(_) => ServiceMode::Synchronous,
        }
    }
}

/// The config file the command line names: `--config <file>`, and nothing else.
fn config_path(mut args: impl Iterator<Item = String>) -> Result<PathBuf, String> {
    match (args.next().as_deref(), args.next(), args.next()) {
        (Some("--config"), Some(path), None) => Ok(path.into()),
        _ => Err(USAGE.to_owned()),
    }
}

/// The pod's settings from the config file at `path`.
fn read(path: &Path) -> Result<Args, String> {
    let yaml = std::fs::read(path).map_err(|error| format!("{}: {error}", path.display()))?;
    let directory = path.parent().unwrap_or(Path::new(""));
    parsed(&yaml, directory).map_err(|error| format!("{}: {error}", path.display()))
}

/// The pod's settings from the file `yaml` states, its TLS files relative to `directory`.
fn parsed(yaml: &[u8], directory: &Path) -> Result<Args, String> {
    // Without the lines around a mistake, which the parser quotes by default: what is said
    // of a file refused goes to the log, as the data plane's does.
    let options = serde_saphyr::options! { with_snippet: false };
    let file: ServiceFile =
        serde_saphyr::from_slice_with_options(yaml, options).map_err(|error| error.to_string())?;
    checked(file, directory)
}

/// What the types cannot check of `file`, and the values of what it leaves out.
fn checked(file: ServiceFile, directory: &Path) -> Result<Args, String> {
    let max_key_bytes = file.keys.max_bytes.unwrap_or(MAX_KEY_BYTES);
    if max_key_bytes == 0 {
        return Err("`keys.max_bytes`: a number of bytes, at least 1".to_owned());
    }
    let mode = file.mode.unwrap_or(ServiceMode::Asynchronous);
    // Said, each would be ignored, as if it did something.
    let (field, owner) = match mode {
        ServiceMode::Synchronous => (
            ("pod.cycle_ms", file.pod.cycle_ms.is_some()),
            "asynchronous",
        ),
        ServiceMode::Asynchronous => (
            (
                "store.forwarder_decides",
                file.store
                    .as_ref()
                    .is_some_and(|store| store.forwarder_decides.is_some()),
            ),
            "synchronous",
        ),
    };
    if let (name, true) = field {
        return Err(format!("`{name}` is the {owner} mode's"));
    }
    let name = match mode {
        ServiceMode::Synchronous => "synchronous",
        ServiceMode::Asynchronous => "asynchronous",
    };
    let store = settings(&file, max_key_bytes)
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            format!("the {name} mode runs on the store: it needs `store` and `pod.address`")
        })?;
    let tls = file.tls.map(|tls| PodFiles {
        certificate: directory.join(tls.certificate),
        key: directory.join(tls.key),
        authorities: directory.join(tls.authorities),
        data_plane: tls.data_plane_id,
    });
    let pod = file.pod;
    let serves = match mode {
        ServiceMode::Synchronous => Serves::Synchronous(store),
        ServiceMode::Asynchronous => {
            // A cycle of nothing would tick without end.
            let cycle = pod.cycle_ms.unwrap_or(CYCLE_MS);
            if cycle == 0 {
                return Err("`pod.cycle_ms`: a number of milliseconds, at least 1".to_owned());
            }
            Serves::Asynchronous {
                store,
                cycle: Duration::from_millis(cycle),
            }
        }
    };
    Ok(Args {
        listen: pod.listen,
        metrics: pod.metrics,
        tls,
        serves,
    })
}

/// A pod answers on one thread, either mode's: a stream's answers and its connection's writes
/// share it, so that what one read brings in is answered in one write, where a connection on
/// another thread would take the answers a few at a time as they are made.
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
}

fn main() -> ExitCode {
    let args = match config_path(std::env::args().skip(1)).and_then(|path| read(&path)) {
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

async fn serve(args: Args) -> Result<(), Box<dyn std::error::Error>> {
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
    let mode = args.serves.mode();
    let stop = Running {
        listener,
        serving,
        metrics: args.metrics,
    };
    match args.serves {
        Serves::Synchronous(store) => {
            let address = store.address.clone();
            let pod = StorePod::spawn(id, store.address, store.config, dial, store.seeds);
            // Always on (decided): a data plane out of reach of a key's owner asks another pod.
            pod.set_forwarding(true).await;
            pod.set_forwarder_decides(store.forwarder_decides).await;
            say_started(mode, id, args.listen, &address);
            stop.run(pod).await
        }
        Serves::Asynchronous { store, cycle } => {
            let address = store.address.clone();
            let cycle = Time::try_from(cycle.as_nanos()).unwrap_or(Time::MAX);
            let mut node = AsyncPod::new(id, address.clone(), store.config, [], cycle);
            // A pod is a member in one life only: its id, new each start, serves as its life,
            // never 0, which would leave its answers unnumbered.
            node.set_life(id.max(1));
            let pod = StorePod::start(node, store.address, dial, store.seeds);
            say_started(mode, id, args.listen, &address);
            stop.run(pod).await
        }
    }
}

fn say_started(mode: ServiceMode, id: u64, listen: SocketAddr, address: &str) {
    let mode = match mode {
        ServiceMode::Synchronous => "synchronous",
        ServiceMode::Asynchronous => "asynchronous",
    };
    eprintln!(
        "edgerush-ratelimit-service: {mode} pod, member {id}, on {listen}, reached at {address}"
    );
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
        None => {
            let incoming = TcpIncoming::from(listener).with_nodelay(Some(true));
            tonic::transport::Server::builder()
                .serve_with_incoming_shutdown(service, incoming, stop)
                .await
        }
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

    /// The settings from `yaml`, its TLS files relative to `/etc/edgerush`.
    fn parsed_from(yaml: &str) -> Result<Args, String> {
        parsed(yaml.as_bytes(), Path::new("/etc/edgerush"))
    }

    /// An asynchronous pod's file, on the store, with `more` after its sections.
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

    /// An asynchronous pod's store and its data planes' cycle.
    fn asynchronous(args: Args) -> (Settings, Duration) {
        match args.serves {
            Serves::Asynchronous { store, cycle } => (store, cycle),
            Serves::Synchronous(_) => panic!("a synchronous pod"),
        }
    }

    #[test]
    fn the_mode_is_asynchronous_unless_said_and_either_runs_on_the_store() {
        let parsed = parsed_from(&with("")).unwrap();
        assert_eq!(parsed.serves.mode(), ServiceMode::Asynchronous);
        assert_eq!(asynchronous(parsed).0.address, "rl-0.rl:7000");
        let said = parsed_from(&with("mode: eventual\n")).unwrap_err();
        assert!(said.contains("eventual"), "{said}");
        // Each mode runs on the store, and is refused without it.
        for mode in ["asynchronous", "synchronous"] {
            assert_eq!(
                parsed_from(&format!("mode: {mode}\npod: {{ listen: 0.0.0.0:7000 }}\n")),
                Err(format!(
                    "the {mode} mode runs on the store: it needs `store` and `pod.address`"
                )),
                "{mode}"
            );
        }
        let parsed = parsed_from(&format!("mode: synchronous\n{}", with(""))).unwrap();
        let Serves::Synchronous(store) = parsed.serves else {
            panic!("an asynchronous pod")
        };
        assert_eq!(store.address, "rl-0.rl:7000");
    }

    #[test]
    fn each_mode_takes_only_what_it_reads() {
        // The data planes' cycle is the asynchronous mode's alone; deciding from a copy, the
        // synchronous mode's.
        assert_eq!(
            parsed_from(&format!(
                "mode: synchronous\n{}",
                sections(", cycle_ms: 10", "store: {}\n")
            )),
            Err("`pod.cycle_ms` is the asynchronous mode's".to_owned())
        );
        assert_eq!(
            parsed_from(&sections("", "store: { forwarder_decides: true }\n")),
            Err("`store.forwarder_decides` is the synchronous mode's".to_owned())
        );
        // What the asynchronous mode's pods agreed by before they ran on the store is no
        // field the file knows.
        for gone in [
            sections(", id: 0", "store: {}\n"),
            with("membership: { pods: { 0: rl-0:7000 } }\n"),
        ] {
            let said = parsed_from(&gone).unwrap_err();
            assert!(said.contains("unknown field"), "{said}");
        }
    }

    #[test]
    fn a_cycle_is_a_millisecond_at_least() {
        let cycle = |ms: &str| parsed_from(&sections(&format!(", cycle_ms: {ms}"), "store: {}\n"));
        assert_eq!(
            asynchronous(parsed_from(&with("")).unwrap()).1,
            Duration::from_millis(10)
        );
        assert_eq!(
            asynchronous(cycle("1").unwrap()).1,
            Duration::from_millis(1)
        );
        assert_eq!(
            cycle("0"),
            Err("`pod.cycle_ms`: a number of milliseconds, at least 1".to_owned())
        );
        assert!(cycle("soon").is_err());
    }

    #[test]
    fn what_keys_may_hold_and_where_metrics_go_are_said_or_left_out() {
        let plain = parsed_from(&with("")).unwrap();
        assert_eq!(plain.metrics, None);
        assert_eq!(asynchronous(plain).0.config.max_key_bytes, Some(256 << 20));
        let said = parsed_from(&format!(
            "keys: {{ max_bytes: 1048576 }}\n{}",
            sections(", metrics: 127.0.0.1:9090", "store: {}\n")
        ))
        .unwrap();
        assert_eq!(said.metrics, Some("127.0.0.1:9090".parse().unwrap()));
        assert_eq!(asynchronous(said).0.config.max_key_bytes, Some(1 << 20));
        assert_eq!(
            parsed_from(&with("keys: { max_bytes: 0 }\n")),
            Err("`keys.max_bytes`: a number of bytes, at least 1".to_owned())
        );
        assert!(parsed_from(&with("keys: { max_bytes: lots }\n")).is_err());
        assert!(parsed_from(&sections(", metrics: nowhere", "store: {}\n")).is_err());
    }

    #[test]
    fn a_store_the_pod_could_not_run_on_is_refused_as_the_file_is_read() {
        assert_eq!(
            parsed_from(&sections("", "")).err(),
            Some("`pod.address` is for the store, and there is no `store` section".to_owned())
        );
        assert_eq!(
            parsed_from("pod: { listen: 0.0.0.0:7000 }\nstore: {}\n").err(),
            Some("`pod.address` is needed with a `store` section".to_owned())
        );
        assert_eq!(
            parsed_from(&with("")).map(|_| ()).err(),
            None,
            "a store of its defaults"
        );
        assert_eq!(
            parsed_from(&sections("", "store: { stop_serving_ms: 500 }\n")).err(),
            Some(
                "`store.stop_serving_ms` 500 must be more than a heartbeat (100 ms) and at \
                 least 200 ms less than `store.silence_ms` 600"
                    .to_owned()
            )
        );
    }

    #[test]
    fn the_certificate_files_go_together_or_the_pod_is_served_plain() {
        assert_eq!(parsed_from(&with("")).unwrap().tls, None);
        let files = [
            "certificate: pod.pem",
            "key: pod.key",
            "authorities: ca.pem",
            "data_plane_id: spiffe://cluster.local/ns/edge/sa/edge",
        ];
        let tls =
            |given: &[&str]| parsed_from(&with(&format!("tls: {{ {} }}\n", given.join(", "))));
        assert_eq!(
            tls(&files).unwrap().tls,
            Some(PodFiles {
                certificate: "/etc/edgerush/pod.pem".into(),
                key: "/etc/edgerush/pod.key".into(),
                authorities: "/etc/edgerush/ca.pem".into(),
                data_plane: "spiffe://cluster.local/ns/edge/sa/edge".to_owned(),
            })
        );
        for left_out in 0..files.len() {
            let mut given = files.to_vec();
            given.remove(left_out);
            assert!(tls(&given).is_err(), "{given:?}");
        }
    }
}
