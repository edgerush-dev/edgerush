//! The rate-limiting service pod (a prototype):
//!
//! ```text
//! edgerush-ratelimit-service --id 0 --listen 0.0.0.0:7000 \
//!     --pod 0=rl-0:7000 --pod 1=rl-1:7000 --pod 2=rl-2:7000 [--cycle-ms 10]
//! ```
//!
//! `--pod` names every service pod, this one too: the members among which a majority agrees
//! which of them serve (`edgerush_ratelimit::service::membership`).

use edgerush_ratelimit::service::PodId;
use edgerush_ratelimit_service::grpc::{Client, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Args {
    id: PodId,
    listen: SocketAddr,
    pods: BTreeMap<PodId, String>,
    cycle: Duration,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let (mut id, mut listen, mut pods, mut cycle) = (None, None, BTreeMap::new(), 10);
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
