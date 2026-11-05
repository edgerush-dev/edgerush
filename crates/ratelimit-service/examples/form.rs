//! Forms the store's cluster of the service pods at the addresses given, as the controller
//! will from stage 7, its naming kept in a file across its restarts (`cluster::forming`).
//! Test tooling for the bench, not shipped.
//!
//! `cargo run --release -p edgerush-ratelimit-service --example form -- POD_FILE NAMING_FILE
//! ADDRESS...`: the store's settings from one pod's service file, which every pod shares; the
//! file it keeps its naming in, made if there is none; and each pod's address, as the API
//! would show the pods running. It runs until stopped, or until it cannot keep a naming.

use edgerush_config::ServiceFile;
use edgerush_ratelimit_service::cluster::forming::Forming;
use edgerush_ratelimit_service::cluster::settings::settings;
use std::process::ExitCode;

const USAGE: &str = "usage: form POD_FILE NAMING_FILE ADDRESS...";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("form: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let mut args = std::env::args().skip(1);
    let (Some(pod), Some(naming)) = (args.next(), args.next()) else {
        return Err(USAGE.to_owned());
    };
    let addresses: Vec<String> = args.collect();
    if addresses.is_empty() {
        return Err(USAGE.to_owned());
    }
    let yaml = std::fs::read(&pod).map_err(|error| format!("{pod}: {error}"))?;
    let file: ServiceFile =
        serde_saphyr::from_slice(&yaml).map_err(|error| format!("{pod}: {error}"))?;
    // The keys' cap is the pods' to keep, not forming's: any the store takes will do.
    let settings = settings(&file, 1 << 20)
        .map_err(|error| format!("{pod}: {error}"))?
        .ok_or_else(|| format!("{pod}: no `store` section"))?;
    let forming =
        Forming::new(&settings.config, naming.into()).map_err(|error| error.to_string())?;
    forming
        .run(addresses)
        .await
        .map_err(|error| error.to_string())
}
