//! The EdgeRush binary.
//!
//! One executable will carry every role (operator, control plane, data plane) as
//! subcommands. So far there is one: `proxy`, a data plane run from a config file — the
//! development harness.

mod bind;
mod config_file;
mod essential;
mod harness;
mod limits;
mod per_core;

use std::io::{self, Write};
use std::process::ExitCode;

/// Name and version, as printed by `--version`.
const VERSION: &str = concat!(env!("CARGO_PKG_NAME"), " ", env!("CARGO_PKG_VERSION"));

const USAGE: &str = "\
Usage: edgerush [OPTIONS] [COMMAND]

Commands:
  proxy  Run a data plane from a config file, without Kubernetes (development harness)

Options:
  -V, --version  Print version
  -h, --help     Print help
";

/// Exit status for a command line the binary does not understand (the conventional
/// "usage error" code).
const EXIT_USAGE: u8 = 2;

fn main() -> ExitCode {
    let args = std::env::args().skip(1);
    ExitCode::from(run(args, &mut io::stdout(), &mut io::stderr()))
}

/// Runs the command line and returns the process exit status.
///
/// Output streams are parameters so the behaviour can be tested without spawning a
/// process.
fn run(
    mut args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match args.next().as_deref() {
        None | Some("-h" | "--help") => write!(stdout, "{USAGE}").map(|()| 0),
        Some("-V" | "--version") => writeln!(stdout, "{VERSION}").map(|()| 0),
        Some("proxy") => return harness::command(args, stdout, stderr),
        Some(other) => {
            write!(stderr, "error: unexpected argument '{other}'\n\n{USAGE}").map(|()| EXIT_USAGE)
        }
    };
    // A closed or broken output stream is a failure, not a reason to panic.
    written.unwrap_or(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run_with(args: &[&str]) -> (u8, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let status = run(
            args.iter().map(ToString::to_string),
            &mut stdout,
            &mut stderr,
        );
        (
            status,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    #[test]
    fn version_flag_prints_name_and_version() {
        for flag in ["--version", "-V"] {
            let (status, stdout, stderr) = run_with(&[flag]);
            assert_eq!(status, 0);
            assert_eq!(stdout, format!("edgerush {}\n", env!("CARGO_PKG_VERSION")));
            assert!(stderr.is_empty());
        }
    }

    #[test]
    fn help_flag_and_no_arguments_print_usage() {
        for args in [&["--help"][..], &["-h"][..], &[][..]] {
            let (status, stdout, stderr) = run_with(args);
            assert_eq!(status, 0);
            assert_eq!(stdout, USAGE);
            assert!(stderr.is_empty());
        }
    }

    #[test]
    fn unknown_argument_is_a_usage_error_on_stderr() {
        let (status, stdout, stderr) = run_with(&["--bogus"]);
        assert_eq!(status, EXIT_USAGE);
        assert!(stdout.is_empty());
        assert!(stderr.starts_with("error: unexpected argument '--bogus'"));
        assert!(stderr.contains(USAGE));
    }

    #[test]
    fn broken_output_stream_is_reported_as_failure() {
        struct Broken;
        impl Write for Broken {
            fn write(&mut self, _: &[u8]) -> io::Result<usize> {
                Err(io::ErrorKind::BrokenPipe.into())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }
        let status = run(
            ["--version".to_string()].into_iter(),
            &mut Broken,
            &mut Vec::new(),
        );
        assert_eq!(status, 1);
    }
}
