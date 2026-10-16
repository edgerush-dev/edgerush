//! The rate-limiting service pod (a prototype):
//!
//! ```text
//! edgerush-ratelimit-service --id 0 --listen 0.0.0.0:7000 \
//!     --pod 0=rl-0:7000 --pod 1=rl-1:7000 --pod 2=rl-2:7000 [--cycle-ms 10] \
//!     [--max-key-bytes 268435456] [--metrics 0.0.0.0:9090]
//! ```
//!
//! `--pod` names every service pod, this one too: the members among which a majority agrees
//! which of them serve (`edgerush_ratelimit::service::membership`). `--max-key-bytes` is
//! what its keys may hold, 256 MiB unless said, past which the least recently changed are
//! evicted; `--metrics`, where `/metrics` shows what they hold, nowhere unless said.

use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use edgerush_ratelimit_service::scrape;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// What `--max-key-bytes` is when left out: 256 MiB.
const MAX_KEY_BYTES: u64 = 256 << 20;

#[derive(Debug, PartialEq, Eq)]
struct Args {
    id: PodId,
    listen: SocketAddr,
    pods: BTreeMap<PodId, String>,
    cycle: Duration,
    max_key_bytes: u64,
    metrics: Option<SocketAddr>,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut id, mut listen, mut pods, mut cycle) = (None, None, BTreeMap::new(), 10);
    let (mut max_key_bytes, mut metrics) = (MAX_KEY_BYTES, None);
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
            "--cycle-ms" => cycle = value()?.parse().map_err(|_| "--cycle-ms: a number")?,
            "--max-key-bytes" => {
                max_key_bytes = value()?
                    .parse()
                    .ok()
                    .filter(|bytes| *bytes > 0)
                    .ok_or("--max-key-bytes: a number of bytes, at least 1")?;
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
    Ok(Args {
        id,
        listen: listen.ok_or("--listen is required")?,
        pods,
        cycle: Duration::from_millis(cycle),
        max_key_bytes,
        metrics,
    })
}

fn main() -> ExitCode {
    let args = match parse(std::env::args().skip(1)) {
        Ok(args) => args,
        Err(error) => {
            eprintln!("edgerush-ratelimit-service: {error}");
            return ExitCode::from(2);
        }
    };
    let runtime = match tokio::runtime::Runtime::new() {
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
