//! Runnable examples are checked through the same command documented for users.

use std::io;
use std::process::Command;

fn check(config: &str, tests: &str) -> io::Result<()> {
    let output = Command::new(env!("CARGO_BIN_EXE_edgerush"))
        .current_dir(concat!(env!("CARGO_MANIFEST_DIR"), "/../.."))
        .args(["test", "--config", config, tests])
        .output()?;
    assert!(
        output.status.success(),
        "{config}:\n{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[test]
fn development_example() -> io::Result<()> {
    check(
        "examples/dev-harness.yaml",
        "examples/dev-harness-tests.yaml",
    )
}

#[test]
fn grpc_example() -> io::Result<()> {
    check("examples/grpc.yaml", "examples/grpc-tests.yaml")
}

#[test]
fn compose_example() -> io::Result<()> {
    check(
        "examples/docker-compose/config/edgerush.yaml",
        "examples/docker-compose/tests.yaml",
    )
}
