//! End-to-end checks of the built binary: the real process, its streams and exit status.

use std::io;
use std::process::{Command, Output};

fn edgerush(args: &[&str]) -> io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_edgerush"))
        .args(args)
        .output()
}

#[test]
fn version_is_printed_and_exit_status_is_zero() -> io::Result<()> {
    let output = edgerush(&["--version"])?;
    assert!(output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        format!("edgerush {}\n", env!("CARGO_PKG_VERSION"))
    );
    assert!(output.stderr.is_empty());
    Ok(())
}

#[test]
fn unknown_argument_exits_with_usage_error() -> io::Result<()> {
    let output = edgerush(&["--bogus"])?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unexpected argument '--bogus'"));
    Ok(())
}
