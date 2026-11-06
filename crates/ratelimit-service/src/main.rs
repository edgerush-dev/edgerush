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
//!   id: 0                     # the asynchronous mode's
//!   listen: 0.0.0.0:7000
//!   address: rl-0.rl:7000     # with `store` alone
//!   metrics: 0.0.0.0:9090     # nowhere when left out
//!   cycle_ms: 10              # the asynchronous mode's; 10 when left out
//! membership:                 # the asynchronous mode's
//!   pods:
//!     0: rl-0:7000
//!     1: rl-1:7000
//!     2: rl-2:7000
//!   silence_ms: 600           # 600 when left out
//!   stop_serving_ms: 400      # 400 when left out
//! store:                      # none when left out
//!   seeds: [rl-0:7000]        # none when left out
//!   shards: 256               # each as the store's defaults when left out
//!   replicas: 2
//!   heartbeat_ms: 100
//!   stop_serving_ms: 400
//!   silence_ms: 600
//!   election_jitter_ms: 200
//!   cycle_ms: 10
//!   skew_warning_ms: 250
//! keys:
//!   max_bytes: 268435456      # 256 MiB when left out
//! tls:                        # served plain, to anyone, when left out
//!   certificate: pod.pem
//!   key: pod.key
//!   authorities: ca.pem
//!   data_plane_id: spiffe://cluster.local/ns/edge/sa/edge
//! ```
//!
//! A pod serves one mode. The asynchronous mode needs `pod.id` and `membership`, and
//! `pod.cycle_ms` is its data planes' cycle; the synchronous mode, on the store, refuses all
//! three, as it would not read them.
//!
//! `membership.pods` names every service pod, this one too: the members among which a
//! majority agrees which of them serve (`edgerush_ratelimit::service::membership`).
//! `silence_ms` is how long a pod goes unheard before the others take it out of the view;
//! `stop_serving_ms`, how long a pod goes without hearing a majority before it stops
//! serving: longer than a heartbeat (100 ms), and two heartbeats shorter than the silence at
//! least, so that a pod cut off from the others stops before they take it out, however far
//! apart each side heard the other last and looked. Longer tolerates longer hiccups between
//! the pods, and leaves a pod that has gone out of reach for longer. `keys.max_bytes` is what
//! its keys may hold, past which the least recently changed are evicted; `pod.metrics`,
//! where `/metrics` shows whether the pod serves, how often and how long it has gone without
//! a majority, and what its keys hold.
//!
//! `store` is what the pod runs the cluster store with (`edgerush_cluster`), which every pod
//! of a cluster is to share, and `pod.address` where the other pods reach it, needed with
//! `store` and only with it (`edgerush_ratelimit_service::cluster::settings`). The synchronous
//! mode runs on the store and needs both; a pod takes a member id of its own each time it
//! starts, forwards a request for a key not its own to the key's owner, shows its store's
//! `/metrics` and says at `/ready` whether it is ready, and, told to stop, hands its shards on
//! before it does. The asynchronous mode does not run the store yet.
//!
//! `tls`'s files, relative to the config file's directory unless absolute, are read again
//! every second, and serve the service with mutual TLS (`edgerush_ratelimit_service::secure`):
//! the pod presents `certificate` to data planes and to the other pods alike, trusts the
//! `authorities` to vouch for every caller, and tells them apart by the SPIFFE ID their
//! certificate carries: its own is the pods', `data_plane_id` the data plane's it serves,
//! and any other is refused. It dials the other pods checking theirs for its own ID.

use boring::ssl::SslConnector;
use edgerush_config::{ServiceFile, ServiceMembership, ServiceMode as Mode};
use edgerush_control_service::reread;
use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer};
use edgerush_ratelimit_service::cluster::settings::{Settings, member_id, settings};
use edgerush_ratelimit_service::grpc::{Client, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use edgerush_ratelimit_service::scrape;
use edgerush_ratelimit_service::secure::{self, PodFiles, PodTls};
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tonic::server::NamedService;
use tonic::transport::server::TcpIncoming;

/// What `keys.max_bytes` is when left out: 256 MiB.
const MAX_KEY_BYTES: u64 = 256 << 20;

/// What `pod.cycle_ms` is when left out.
const CYCLE_MS: u64 = 10;

/// What `membership.silence_ms` and `membership.stop_serving_ms` are when left out.
const SILENCE_MS: u64 = 600;
const STOP_SERVING_MS: u64 = 400;

/// How often the certificate files are read again, to take in a Secret replaced.
const REREAD: Duration = Duration::from_secs(1);

const USAGE: &str = "usage: edgerush-ratelimit-service --config <file>";

#[derive(Debug, PartialEq, Eq)]
struct Args {
    listen: SocketAddr,
    max_key_bytes: u64,
    metrics: Option<SocketAddr>,
    tls: Option<PodFiles>,
    serves: Serves,
}

/// The mode a pod serves, with what it serves it on.
#[derive(Debug, PartialEq, Eq)]
enum Serves {
    Asynchronous {
        id: PodId,
        pods: BTreeMap<PodId, String>,
        cycle: Duration,
        silence: Duration,
        stop_serving: Duration,
    },
    /// The store, with what it runs it with.
    Synchronous(Settings),
}

impl Serves {
    fn mode(&self) -> Mode {
        match self {
            Self::Asynchronous { .. } => Mode::Asynchronous,
            Self::Synchronous(_) => Mode::Synchronous,
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
    let store = settings(&file, max_key_bytes).map_err(|error| error.to_string())?;
    let tls = file.tls.map(|tls| PodFiles {
        certificate: directory.join(tls.certificate),
        key: directory.join(tls.key),
        authorities: directory.join(tls.authorities),
        data_plane: tls.data_plane_id,
    });
    let pod = file.pod;
    let serves = match file.mode.unwrap_or(Mode::Asynchronous) {
        Mode::Synchronous => {
            // Said, each would be ignored, as if it did something.
            let asynchronous = [
                ("pod.id", pod.id.is_some()),
                ("pod.cycle_ms", pod.cycle_ms.is_some()),
                ("membership", file.membership.is_some()),
            ];
            if let Some((field, _)) = asynchronous.iter().find(|(_, said)| *said) {
                return Err(format!(
                    "`{field}` is the asynchronous mode's: the synchronous mode runs on the store"
                ));
            }
            Serves::Synchronous(store.ok_or(
                "the synchronous mode runs on the store: it needs `store` and `pod.address`",
            )?)
        }
        Mode::Asynchronous => asynchronous(pod.id, pod.cycle_ms, file.membership)?,
    };
    Ok(Args {
        listen: pod.listen,
        max_key_bytes,
        metrics: pod.metrics,
        tls,
        serves,
    })
}

/// The asynchronous mode's pod: its `id` among the `membership`'s pods, and its timings.
fn asynchronous(
    id: Option<PodId>,
    cycle: Option<u64>,
    membership: Option<ServiceMembership>,
) -> Result<Serves, String> {
    let (Some(id), Some(membership)) = (id, membership) else {
        return Err("the asynchronous mode needs `pod.id` and `membership`".to_owned());
    };
    // A cycle of nothing would tick without end.
    let cycle = cycle.unwrap_or(CYCLE_MS);
    if cycle == 0 {
        return Err("`pod.cycle_ms`: a number of milliseconds, at least 1".to_owned());
    }
    if !membership.pods.contains_key(&id) {
        return Err("`membership.pods` must name this pod too".to_owned());
    }
    let silence = membership.silence_ms.unwrap_or(SILENCE_MS);
    let stop_serving = membership.stop_serving_ms.unwrap_or(STOP_SERVING_MS);
    // A pod would be suspected, or stop, between two heartbeats that both came. And one cut
    // off stops before the others take it out only with two heartbeats to spare: each side
    // may have heard the other last up to a heartbeat apart, and look up to one late.
    let heartbeat = Timing::with_cycle(Duration::from_millis(cycle)).heartbeat;
    let heartbeat = u64::try_from(heartbeat.as_millis()).unwrap_or(u64::MAX);
    let spare = heartbeat.saturating_mul(2);
    if !(heartbeat < stop_serving && stop_serving.saturating_add(spare) <= silence) {
        return Err(format!(
            "`membership.stop_serving_ms` {stop_serving} must be more than a heartbeat \
             ({heartbeat} ms) and at least {spare} ms less than `membership.silence_ms` \
             {silence}"
        ));
    }
    Ok(Serves::Asynchronous {
        id,
        pods: membership.pods,
        cycle: Duration::from_millis(cycle),
        silence: Duration::from_millis(silence),
        stop_serving: Duration::from_millis(stop_serving),
    })
}

/// A synchronous pod answers on one thread: a stream's decisions and its connection's writes
/// share it, so that the asks one read brings in are answered in one write, where a
/// connection on another thread would take the answers a few at a time as they are decided.
/// The asynchronous pod keeps a thread a core.
fn runtime(mode: Mode) -> std::io::Result<tokio::runtime::Runtime> {
    match mode {
        Mode::Synchronous => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build(),
        Mode::Asynchronous => tokio::runtime::Runtime::new(),
    }
}

fn main() -> ExitCode {
    let args = match config_path(std::env::args().skip(1)).and_then(|path| read(&path)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match runtime(args.serves.mode()) {
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
    let dialling = tls.as_ref().map(|(_, dialling)| dialling.clone());
    let serving = tls.map(|(serving, _)| serving);
    let (id, pods, cycle, silence, stop_serving) = match args.serves {
        Serves::Asynchronous {
            id,
            pods,
            cycle,
            silence,
            stop_serving,
        } => (id, pods, cycle, silence, stop_serving),
        Serves::Synchronous(store) => {
            let id = member_id()?;
            let dial = match &dialling {
                Some(dialling) => Dial::Secured(dialling.clone()),
                None => Dial::Plain,
            };
            let address = store.address.clone();
            let pod = StorePod::spawn(id, store.address, store.config, dial, store.seeds);
            // Always on (decided): a data plane out of reach of a key's owner asks another pod.
            pod.set_forwarding(true).await;
            if let Some(at) = args.metrics {
                let listener = tokio::net::TcpListener::bind(at).await?;
                eprintln!("edgerush-ratelimit-service: metrics and readiness on {at}");
                tokio::spawn(scrape::serve_store(listener, pod.clone()));
            }
            eprintln!(
                "edgerush-ratelimit-service: synchronous pod, member {id}, on {}, reached at {address}",
                args.listen
            );
            // Told to stop, it hands its shards on while it still serves, and stops once
            // drained.
            let draining = pod.clone();
            let drained = async move {
                stopped().await;
                eprintln!("edgerush-ratelimit-service: draining");
                draining.drain().await;
            };
            serve_on(listener, serving, StoreServer::new(pod), drained).await?;
            return Ok(());
        }
    };
    let members = pods.keys().copied().collect();
    // A new life at every start: the time it began, which no earlier life shares.
    let incarnation = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    let mut peers = BTreeMap::new();
    for (pod, address) in &pods {
        if *pod != id {
            let client = match &dialling {
                Some(dialling) => Client::secured(address, dialling.clone())?,
                None => Client::new(address)?,
            };
            peers.insert(*pod, client);
        }
    }
    let timing = Timing {
        silence,
        stop_serving,
        ..Timing::with_cycle(cycle)
    };
    let pod = Pod::spawn(id, members, incarnation, timing, peers);
    pod.set_capacity(args.max_key_bytes).await;
    if let Some(address) = args.metrics {
        let listener = tokio::net::TcpListener::bind(address).await?;
        eprintln!("edgerush-ratelimit-service: metrics on {address}");
        tokio::spawn(scrape::serve(listener, pod.clone(), args.max_key_bytes));
    }
    eprintln!("edgerush-ratelimit-service: pod {id} on {}", args.listen);
    serve_on(listener, serving, Server::new(pod), stopped()).await?;
    Ok(())
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

    /// A pod's file with `more` after its two sections.
    fn with(more: &str) -> String {
        format!(
            "pod: {{ id: 0, listen: 0.0.0.0:7000 }}\nmembership: {{ pods: {{ 0: rl-0:7000 }} }}\n{more}"
        )
    }

    /// A pod's file with its `pod` section's `more` and its `membership` section's.
    fn sections(pod: &str, membership: &str) -> String {
        format!(
            "pod: {{ id: 0, listen: 0.0.0.0:7000{pod} }}\nmembership: {{ pods: {{ 0: rl-0:7000 }}{membership} }}\n"
        )
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
    fn a_synchronous_pod_runs_on_one_thread() {
        use tokio::runtime::RuntimeFlavor;
        let flavor = |mode| runtime(mode).unwrap().handle().runtime_flavor();
        assert_eq!(flavor(Mode::Synchronous), RuntimeFlavor::CurrentThread);
        assert_eq!(flavor(Mode::Asynchronous), RuntimeFlavor::MultiThread);
    }

    /// An asynchronous pod's cycle, silence and time to stop serving.
    fn timings(args: Args) -> (Duration, Duration, Duration) {
        match args.serves {
            Serves::Asynchronous {
                cycle,
                silence,
                stop_serving,
                ..
            } => (cycle, silence, stop_serving),
            Serves::Synchronous(_) => panic!("a synchronous pod"),
        }
    }

    #[test]
    fn the_mode_is_asynchronous_unless_said() {
        assert_eq!(
            parsed_from(&with("")).unwrap().serves.mode(),
            Mode::Asynchronous
        );
        let said = parsed_from(&with("mode: eventual\n")).unwrap_err();
        assert!(said.contains("eventual"), "{said}");
        // The synchronous mode runs on the store, and is refused without it.
        let synchronous = |pod: &str, more: &str| {
            format!("mode: synchronous\npod: {{ listen: 0.0.0.0:7000{pod} }}\n{more}")
        };
        assert_eq!(
            parsed_from(&synchronous("", "")),
            Err(
                "the synchronous mode runs on the store: it needs `store` and `pod.address`"
                    .to_owned()
            )
        );
        let on_store =
            |pod: &str| synchronous(&format!(", address: rl-0.rl:7000{pod}"), "store: {}\n");
        let parsed = parsed_from(&on_store("")).unwrap();
        assert_eq!(parsed.serves.mode(), Mode::Synchronous);
        let Serves::Synchronous(store) = parsed.serves else {
            panic!("an asynchronous pod")
        };
        assert_eq!(store.address, "rl-0.rl:7000");
        // Either mode serves its metrics.
        assert_eq!(
            parsed_from(&on_store(", metrics: 127.0.0.1:9090"))
                .unwrap()
                .metrics,
            Some("127.0.0.1:9090".parse().unwrap())
        );
    }

    #[test]
    fn each_mode_takes_only_what_it_reads() {
        // The asynchronous mode's pod is one of its membership's.
        for asynchronous in [
            "pod: { listen: 0.0.0.0:7000 }\nmembership: { pods: { 0: rl-0:7000 } }\n",
            "pod: { id: 0, listen: 0.0.0.0:7000 }\n",
        ] {
            assert_eq!(
                parsed_from(asynchronous),
                Err("the asynchronous mode needs `pod.id` and `membership`".to_owned()),
                "{asynchronous}"
            );
        }
        // The synchronous mode's is the store's, which gives it an id and runs to its own
        // timings: what the asynchronous mode reads in their place is refused.
        let on_store = "mode: synchronous\nstore: {}\n";
        for (field, file) in [
            (
                "pod.id",
                "pod: { id: 0, listen: 0.0.0.0:7000, address: rl-0.rl:7000 }\n",
            ),
            (
                "pod.cycle_ms",
                "pod: { listen: 0.0.0.0:7000, address: rl-0.rl:7000, cycle_ms: 10 }\n",
            ),
            (
                "membership",
                "pod: { listen: 0.0.0.0:7000, address: rl-0.rl:7000 }\n\
                 membership: { pods: { 0: rl-0:7000 } }\n",
            ),
        ] {
            assert_eq!(
                parsed_from(&format!("{on_store}{file}")),
                Err(format!(
                    "`{field}` is the asynchronous mode's: the synchronous mode runs on the store"
                )),
                "{file}"
            );
        }
    }

    #[test]
    fn a_pod_stops_serving_after_a_while_without_a_majority_before_it_is_taken_out() {
        let (_, silence, stop_serving) = timings(parsed_from(&with("")).unwrap());
        assert_eq!(
            (silence, stop_serving),
            (Duration::from_millis(600), Duration::from_millis(400))
        );
        let timed = |silence: &str, stop: &str| {
            parsed_from(&sections(
                "",
                &format!(", silence_ms: {silence}, stop_serving_ms: {stop}"),
            ))
        };
        let (_, silence, stop_serving) = timings(timed("2000", "1000").unwrap());
        assert_eq!(
            (silence, stop_serving),
            (Duration::from_millis(2_000), Duration::from_millis(1_000))
        );
        // Two heartbeats to spare, exactly, is enough.
        let (_, _, stop_serving) = timings(timed("600", "400").unwrap());
        assert_eq!(stop_serving, Duration::from_millis(400));
        for (silence, stop) in [
            ("500", "500"),
            ("500", "600"),
            ("500", "100"),
            ("100", "50"),
            ("500", "400"),
            ("600", "401"),
        ] {
            assert_eq!(
                timed(silence, stop),
                Err(format!(
                    "`membership.stop_serving_ms` {stop} must be more than a heartbeat (100 ms) \
                     and at least 200 ms less than `membership.silence_ms` {silence}"
                )),
                "{silence} {stop}"
            );
        }
        for (silence, stop) in [("soon", "250"), ("500", "soon")] {
            assert!(timed(silence, stop).is_err(), "{silence} {stop}");
        }
    }

    #[test]
    fn a_cycle_is_a_millisecond_at_least() {
        let cycle = |ms: &str| parsed_from(&sections(&format!(", cycle_ms: {ms}"), ""));
        assert_eq!(
            timings(parsed_from(&with("")).unwrap()).0,
            Duration::from_millis(10)
        );
        assert_eq!(timings(cycle("1").unwrap()).0, Duration::from_millis(1));
        assert_eq!(
            cycle("0"),
            Err("`pod.cycle_ms`: a number of milliseconds, at least 1".to_owned())
        );
        assert!(cycle("soon").is_err());
    }

    #[test]
    fn what_keys_may_hold_and_where_metrics_go_are_said_or_left_out() {
        let plain = parsed_from(&with("")).unwrap();
        assert_eq!(plain.max_key_bytes, 256 << 20);
        assert_eq!(plain.metrics, None);
        let said = parsed_from(&format!(
            "keys: {{ max_bytes: 1048576 }}\n{}",
            sections(", metrics: 127.0.0.1:9090", "")
        ))
        .unwrap();
        assert_eq!(said.max_key_bytes, 1 << 20);
        assert_eq!(said.metrics, Some("127.0.0.1:9090".parse().unwrap()));
        assert_eq!(
            parsed_from(&with("keys: { max_bytes: 0 }\n")),
            Err("`keys.max_bytes`: a number of bytes, at least 1".to_owned())
        );
        assert!(parsed_from(&with("keys: { max_bytes: lots }\n")).is_err());
        assert!(parsed_from(&sections(", metrics: nowhere", "")).is_err());
    }

    #[test]
    fn the_pods_named_include_this_one() {
        let other =
            "pod: { id: 1, listen: 0.0.0.0:7000 }\nmembership: { pods: { 0: rl-0:7000 } }\n";
        assert_eq!(
            parsed_from(other),
            Err("`membership.pods` must name this pod too".to_owned())
        );
        let three = "pod: { id: 1, listen: 0.0.0.0:7000 }\n\
                     membership: { pods: { 0: rl-0:7000, 1: rl-1:7000, 2: rl-2:7000 } }\n";
        let Serves::Asynchronous { id, pods, .. } = parsed_from(three).unwrap().serves else {
            panic!("a synchronous pod")
        };
        assert_eq!(id, 1);
        assert_eq!(
            pods,
            [
                (0, "rl-0:7000".to_owned()),
                (1, "rl-1:7000".to_owned()),
                (2, "rl-2:7000".to_owned())
            ]
            .into()
        );
    }

    #[test]
    fn a_store_the_pod_could_not_run_on_is_refused_as_the_file_is_read() {
        let address = sections(", address: rl-0.rl:7000", "");
        assert!(parsed_from(&format!("{address}store: {{}}\n")).is_ok());
        assert_eq!(
            parsed_from(&address).err(),
            Some("`pod.address` is for the store, and there is no `store` section".to_owned())
        );
        assert_eq!(
            parsed_from(&with("store: {}\n")).err(),
            Some("`pod.address` is needed with a `store` section".to_owned())
        );
        assert_eq!(
            parsed_from(&format!("{address}store: {{ stop_serving_ms: 500 }}\n")).err(),
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
