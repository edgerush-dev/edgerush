//! The rate-limiting service pod (a prototype):
//!
//! ```text
//! edgerush-ratelimit-service --id 0 --listen 0.0.0.0:7000 \
//!     --pod 0=rl-0:7000 --pod 1=rl-1:7000 --pod 2=rl-2:7000 [--cycle-ms 10] \
//!     [--max-key-bytes 268435456] [--metrics 0.0.0.0:9090] [--mode asynchronous]
//! ```
//!
//! `--mode` is the rate-limiting mode the pod serves, `asynchronous` unless said, or
//! `synchronous`: a pod serves one. `--cycle-ms` is the data planes' cycle in the
//! asynchronous mode, and in the synchronous mode the pod's own, between its copies of
//! what changed.
//!
//! `--pod` names every service pod, this one too: the members among which a majority agrees
//! which of them serve (`edgerush_ratelimit::service::membership`). `--max-key-bytes` is
//! what its keys may hold, 256 MiB unless said, past which the least recently changed are
//! evicted; `--metrics`, where `/metrics` shows what they hold, nowhere unless said.

use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use edgerush_ratelimit_service::scrape;
use edgerush_ratelimit_service::synchronous::{self, SyncPod, SyncServer};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What `--max-key-bytes` is when left out: 256 MiB.
const MAX_KEY_BYTES: u64 = 256 << 20;

/// The rate-limiting mode a pod serves.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    Asynchronous,
    Synchronous,
}

#[derive(Debug, PartialEq, Eq)]
struct Args {
    mode: Mode,
    id: PodId,
    listen: SocketAddr,
    pods: BTreeMap<PodId, String>,
    cycle: Duration,
    max_key_bytes: u64,
    metrics: Option<SocketAddr>,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut id, mut listen, mut pods, mut cycle) = (None, None, BTreeMap::new(), 10);
    let (mut max_key_bytes, mut metrics, mut mode) = (MAX_KEY_BYTES, None, Mode::Asynchronous);
    while let Some(flag) = args.next() {
        let mut value = || args.next().ok_or_else(|| format!("{flag} needs a value"));
        match flag.as_str() {
            "--id" => id = Some(value()?.parse().map_err(|_| "--id: a number")?),
            "--listen" => listen = Some(value()?.parse().map_err(|_| "--listen: host:port")?),
            "--pod" => {
                let pod = value()?;
                let (id, address) = pod.split_once('=').ok_or("--pod: id=host:port")?;
                pods.insert(
                    id.parse().map_err(|_| "--pod: id=host:port")?,
                    address.to_owned(),
                );
            }
            // A cycle of nothing would tick without end.
            "--cycle-ms" => {
                cycle = value()?
                    .parse()
                    .ok()
                    .filter(|ms| *ms > 0)
                    .ok_or("--cycle-ms: a number of milliseconds, at least 1")?;
            }
            "--max-key-bytes" => {
                max_key_bytes = value()?
                    .parse()
                    .ok()
                    .filter(|bytes| *bytes > 0)
                    .ok_or("--max-key-bytes: a number of bytes, at least 1")?;
            }
            "--mode" => {
                mode = match value()?.as_str() {
                    "asynchronous" => Mode::Asynchronous,
                    "synchronous" => Mode::Synchronous,
                    _ => return Err("--mode: asynchronous or synchronous".to_owned()),
                };
            }
            "--metrics" => {
                metrics = Some(value()?.parse().map_err(|_| "--metrics: host:port")?);
            }
            other => return Err(format!("unknown argument {other}")),
        }
    }
    let id = id.ok_or("--id is required")?;
    if !pods.contains_key(&id) {
        return Err("--pod must name this pod too".to_owned());
    }
    if mode == Mode::Synchronous && metrics.is_some() {
        return Err("--metrics is for the asynchronous mode, for now".to_owned());
    }
    Ok(Args {
        mode,
        id,
        listen: listen.ok_or("--listen is required")?,
        pods,
        cycle: Duration::from_millis(cycle),
        max_key_bytes,
        metrics,
    })
}

/// A synchronous pod runs on one thread: a stream's decisions and its connection's writes
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
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match runtime(args.mode) {
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
    let members = args.pods.keys().copied().collect();
    // A new life at every start: the time it began, which no earlier life shares.
    let incarnation = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(1, |since| {
            u64::try_from(since.as_nanos()).unwrap_or(u64::MAX)
        });
    let mut peers = BTreeMap::new();
    for (id, address) in &args.pods {
        if *id != args.id {
            peers.insert(*id, Client::new(address)?);
        }
    }
    if args.mode == Mode::Synchronous {
        let pod = SyncPod::spawn(
            args.id,
            members,
            incarnation,
            synchronous::Timing::with_copy(args.cycle),
            peers,
        );
        pod.set_capacity(args.max_key_bytes);
        eprintln!(
            "edgerush-ratelimit-service: synchronous pod {} on {}",
            args.id, args.listen
        );
        tonic::transport::Server::builder()
            .serve_with_shutdown(args.listen, SyncServer::new(pod), stopped())
            .await?;
        return Ok(());
    }
    let pod = Pod::spawn(
        args.id,
        members,
        incarnation,
        Timing::with_cycle(args.cycle),
        peers,
    );
    pod.set_capacity(args.max_key_bytes).await;
    if let Some(address) = args.metrics {
        let listener = tokio::net::TcpListener::bind(address).await?;
        eprintln!("edgerush-ratelimit-service: metrics on {address}");
        tokio::spawn(scrape::serve(listener, pod.clone(), args.max_key_bytes));
    }
    eprintln!(
        "edgerush-ratelimit-service: pod {} on {}",
        args.id, args.listen
    );
    tonic::transport::Server::builder()
        .serve_with_shutdown(args.listen, Server::new(pod), stopped())
        .await?;
    Ok(())
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

    fn parsed(args: &[&str]) -> Result<Args, String> {
        parse(args.iter().map(|arg| (*arg).to_owned()))
    }

    const POD: [&str; 6] = [
        "--id",
        "0",
        "--listen",
        "0.0.0.0:7000",
        "--pod",
        "0=rl-0:7000",
    ];

    #[test]
    fn a_synchronous_pod_runs_on_one_thread() {
        use tokio::runtime::RuntimeFlavor;
        let flavor = |mode| runtime(mode).unwrap().handle().runtime_flavor();
        assert_eq!(flavor(Mode::Synchronous), RuntimeFlavor::CurrentThread);
        assert_eq!(flavor(Mode::Asynchronous), RuntimeFlavor::MultiThread);
    }

    #[test]
    fn the_mode_is_asynchronous_unless_said() {
        assert_eq!(parsed(&POD).unwrap().mode, Mode::Asynchronous);
        let with = |more: &[&'static str]| -> Vec<&str> {
            POD.iter().copied().chain(more.iter().copied()).collect()
        };
        assert_eq!(
            parsed(&with(&["--mode", "synchronous"])).unwrap().mode,
            Mode::Synchronous
        );
        assert_eq!(
            parsed(&with(&["--mode", "eventual"])),
            Err("--mode: asynchronous or synchronous".to_owned())
        );
        assert_eq!(
            parsed(&with(&[
                "--mode",
                "synchronous",
                "--metrics",
                "127.0.0.1:9090"
            ])),
            Err("--metrics is for the asynchronous mode, for now".to_owned())
        );
    }

    #[test]
    fn a_cycle_is_a_millisecond_at_least() {
        let with = |ms: &'static str| -> Vec<&str> {
            POD.iter().copied().chain(["--cycle-ms", ms]).collect()
        };
        assert_eq!(parsed(&with("1")).unwrap().cycle, Duration::from_millis(1));
        for ms in ["0", "soon"] {
            assert_eq!(
                parsed(&with(ms)),
                Err("--cycle-ms: a number of milliseconds, at least 1".to_owned())
            );
        }
    }

    #[test]
    fn what_keys_may_hold_and_where_metrics_go_are_said_or_left_out() {
        let plain = parsed(&POD).unwrap();
        assert_eq!(plain.max_key_bytes, 256 << 20);
        assert_eq!(plain.metrics, None);
        let said: Vec<&str> = POD
            .iter()
            .copied()
            .chain(["--max-key-bytes", "1048576", "--metrics", "127.0.0.1:9090"])
            .collect();
        let said = parsed(&said).unwrap();
        assert_eq!(said.max_key_bytes, 1 << 20);
        assert_eq!(said.metrics, Some("127.0.0.1:9090".parse().unwrap()));
        for (flag, value, error) in [
            (
                "--max-key-bytes",
                "0",
                "--max-key-bytes: a number of bytes, at least 1",
            ),
            (
                "--max-key-bytes",
                "lots",
                "--max-key-bytes: a number of bytes, at least 1",
            ),
            ("--metrics", "nowhere", "--metrics: host:port"),
        ] {
            let args: Vec<&str> = POD.iter().copied().chain([flag, value]).collect();
            assert_eq!(parsed(&args), Err(error.to_owned()));
        }
    }
}
