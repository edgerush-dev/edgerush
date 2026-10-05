//! `edgerush test`: the command line around `edgerush_explain`'s test runner
//! ([22 §3](../../../docs/22-explain-and-test.md)) — its arguments, reading the config and the
//! files of tests, and printing what each test came to.

use crate::config_file::{self, Rejected};
use edgerush_explain::runner::{self, Ran};
use edgerush_explain::test_file::{self, Mistake, Prepared};
use edgerush_explain::{Snapshot, Unexplained};
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub(crate) const USAGE: &str = "\
Usage: edgerush test --config <FILE> <TESTS>...

Runs every test in the files of tests against the config, each request decided as the data
plane would decide it, without running one. The config is read as the harness reads it; the
files of its certificates are not. Exits 0 when every test passed, 1 when one failed, and 2
when they could not be run.

Arguments:
  <TESTS>...       Files of tests, in YAML

Options:
      --config <FILE>  The config, in YAML
  -h, --help           Print help
";

/// Exit status for a run in which a test failed.
const EXIT_FAILED: u8 = 1;

/// Runs `edgerush test` with the arguments after its name. Returns the exit status.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match parse(args) {
        Ok(Parsed::Help) => write!(stdout, "{USAGE}").map(|()| 0),
        Ok(Parsed::Test(options)) => match run(&options) {
            Ok((text, failed)) => {
                write!(stdout, "{text}").map(|()| if failed { EXIT_FAILED } else { 0 })
            }
            Err(failure) => writeln!(stderr, "error: {failure}").map(|()| crate::EXIT_USAGE),
        },
        Err(error) => write!(stderr, "error: {error}\n\n{USAGE}").map(|()| crate::EXIT_USAGE),
    };
    written.unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Test(Options),
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    config: PathBuf,
    tests: Vec<PathBuf>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'--config' needs a value")]
    NoValue,
    #[error("'--config' is given twice")]
    Twice,
    #[error("'--config <FILE>' is required")]
    NoConfig,
    #[error("a file of tests is required")]
    NoTests,
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut config = None;
    let mut tests = Vec::new();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--config" => {
                let file = args.next().ok_or(UsageError::NoValue)?;
                if config.replace(PathBuf::from(file)).is_some() {
                    return Err(UsageError::Twice);
                }
            }
            _ if arg.starts_with('-') => return Err(UsageError::Unexpected(arg)),
            _ => tests.push(PathBuf::from(arg)),
        }
    }
    let config = config.ok_or(UsageError::NoConfig)?;
    if tests.is_empty() {
        return Err(UsageError::NoTests);
    }
    Ok(Parsed::Test(Options { config, tests }))
}

/// Why the tests could not be run.
#[derive(Debug, thiserror::Error)]
enum Failed {
    #[error("config {} cannot be read:\n{rejected}", path.display())]
    Config { path: PathBuf, rejected: Rejected },
    #[error("tests {} cannot be read: {error}", path.display())]
    Unreadable { path: PathBuf, error: io::Error },
    #[error("the tests cannot be run:\n{0}")]
    Invalid(String),
    #[error("{} ({}:{}): {error}", name, path.display(), line)]
    Unexplained {
        name: String,
        path: PathBuf,
        line: u64,
        // Boxed: the rest of the error is small, and this is returned by every test run.
        error: Box<Unexplained>,
    },
}

/// Runs every test, once every file has been found valid. The text, and whether a test
/// failed.
fn run(options: &Options) -> Result<(String, bool), Failed> {
    let snapshot = config_file::offline(&options.config).map_err(|rejected| Failed::Config {
        path: options.config.clone(),
        rejected,
    })?;
    let prepared = prepared(&snapshot, &options.tests)?;
    let (mut passed, mut failed) = (0, 0);
    let mut text = String::new();
    for (path, tests) in &prepared {
        for test in tests {
            let ran = runner::run(&snapshot, test).map_err(|error| Failed::Unexplained {
                name: test.name.clone(),
                path: path.clone(),
                line: test.line,
                error: Box::new(error),
            })?;
            said(&mut text, path, test, &ran);
            if ran.passed() {
                passed += 1;
            } else {
                failed += 1;
            }
        }
    }
    let _ = writeln!(text, "{failed} failed, {passed} passed");
    Ok((text, failed > 0))
}

/// Every file's tests, ready to run; or every mistake in every file.
fn prepared(
    snapshot: &Snapshot,
    paths: &[PathBuf],
) -> Result<Vec<(PathBuf, Vec<Prepared>)>, Failed> {
    let mut prepared = Vec::new();
    let mut mistakes = String::new();
    let mut note = |path: &PathBuf, mistake: &Mistake| {
        let _ = match mistake.line {
            0 => writeln!(mistakes, "{}: {}", path.display(), mistake.problem),
            line => writeln!(mistakes, "{}:{line}: {}", path.display(), mistake.problem),
        };
    };
    for path in paths {
        let yaml = fs::read(path).map_err(|error| Failed::Unreadable {
            path: path.clone(),
            error,
        })?;
        match test_file::read(&yaml).map_err(|mistake| vec![mistake]) {
            Ok(file) => match file.prepare(snapshot) {
                Ok(tests) => prepared.push((path.clone(), tests)),
                Err(found) => found.iter().for_each(|mistake| note(path, mistake)),
            },
            Err(found) => found.iter().for_each(|mistake| note(path, mistake)),
        }
    }
    if mistakes.is_empty() {
        Ok(prepared)
    } else {
        Err(Failed::Invalid(mistakes.trim_end().to_owned()))
    }
}

/// What a test came to, as text: a line, and for a failure what differed and its request's
/// explanation.
fn said(text: &mut String, path: &Path, test: &Prepared, ran: &Ran) {
    let place = format!("{}:{}", path.display(), test.line);
    if ran.passed() {
        let _ = writeln!(text, "ok    {} ({place})", test.name);
        return;
    }
    let _ = writeln!(text, "FAIL  {} ({place})", test.name);
    for difference in &ran.differences {
        let _ = writeln!(text, "  {difference}");
    }
    let _ = writeln!(text, "\n{}", ran.explanation);
}

#[cfg(test)]
mod tests {
    use super::*;

    const CONFIG: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: shop
    listeners: [web]
    hostnames: [{ name: shop.example.com, falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /cart } }]
        forward: { backends: [{ upstream: cart, weight: 1 }] }
upstreams:
  cart: { load_balancer: p2c, endpoints: [] }
"#;

    const PASSES: &str = r#"tests:
  - name: the cart
    request: { listener: web, client: 203.0.113.7, protocol: "1.1", method: GET, url: "http://shop.example.com/cart", headers: [] }
    expect: { route: shop, rule: 0, forward: { backends: [{ upstream: cart, weight: 1 }], mirrors: [] } }
"#;

    const FAILS: &str = r#"tests:
  - name: elsewhere
    request: { listener: web, client: 203.0.113.7, protocol: "1.1", method: GET, url: "http://shop.example.com/other", headers: [] }
    expect: { route: shop, rule: 0, forward: { backends: [{ upstream: cart, weight: 1 }], mirrors: [] } }
"#;

    /// Files of the test's own in the system's temporary directory, gone with the test.
    struct Scratch(Vec<PathBuf>);

    impl Scratch {
        fn new(test: &str, files: &[(&str, &str)]) -> Self {
            let paths = files
                .iter()
                .map(|(name, content)| {
                    let name = format!("edgerush-{}-testing-{test}-{name}", std::process::id());
                    let path = std::env::temp_dir().join(name);
                    fs::write(&path, content).unwrap();
                    path
                })
                .collect();
            Self(paths)
        }

        fn path(&self, at: usize) -> String {
            self.0[at].to_str().unwrap().to_owned()
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            for path in &self.0 {
                let _gone_already = fs::remove_file(path);
            }
        }
    }

    /// Runs the command with the config and these files of tests; its exit status, stdout
    /// and stderr, with the scratch files' paths written as their names.
    fn tested(test: &str, config: &str, files: &[(&str, &str)]) -> (u8, String, String) {
        let mut all = vec![("config.yaml", config)];
        all.extend(files);
        let scratch = Scratch::new(test, &all);
        let mut args = vec!["--config".to_owned(), scratch.path(0)];
        args.extend((1..all.len()).map(|at| scratch.path(at)));
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(args.into_iter(), &mut stdout, &mut stderr);
        let named = |text: Vec<u8>| {
            let mut text = String::from_utf8(text).unwrap();
            for (at, (name, _)) in all.iter().enumerate() {
                text = text.replace(&scratch.path(at), name);
            }
            text
        };
        (status, named(stdout), named(stderr))
    }

    #[test]
    fn tests_that_all_pass_exit_0() {
        let (status, stdout, stderr) = tested("passes", CONFIG, &[("tests.yaml", PASSES)]);
        assert_eq!(
            (status, stdout.as_str(), stderr.as_str()),
            (0, "ok    the cart (tests.yaml:2)\n0 failed, 1 passed\n", "")
        );
    }

    #[test]
    fn a_failed_test_says_what_differed_and_explains_its_request_and_exits_1() {
        let files = [("passes.yaml", PASSES), ("fails.yaml", FAILS)];
        let (status, stdout, stderr) = tested("fails", CONFIG, &files);
        assert_eq!(status, 1);
        assert!(stderr.is_empty());
        assert_eq!(
            stdout,
            "\
ok    the cart (passes.yaml:2)
FAIL  elsewhere (fails.yaml:2)
  route: expected shop, got none
  rule: expected 0, got none
  outcome: expected forward, got answer no_route

web (http)  GET http://shop.example.com/other  HTTP/1.1  from 203.0.113.7

  shop rule 0 match 0  path /other is not under /cart

answer    404 no_route

1 failed, 1 passed
"
        );
    }

    #[test]
    fn files_that_are_not_valid_are_said_whole_and_nothing_runs() {
        let invalid = PASSES.replace("expect: { route: shop, rule: 0, ", "expect: { ");
        let unread = "tests: [";
        let files = [
            ("passes.yaml", PASSES),
            ("invalid.yaml", invalid.as_str()),
            ("unread.yaml", unread),
        ];
        let (status, stdout, stderr) = tested("invalid", CONFIG, &files);
        assert_eq!(status, 2);
        assert!(stdout.is_empty());
        assert!(
            stderr.starts_with(
                "error: the tests cannot be run:\ninvalid.yaml:2: a forwarded or redirected request has a route and rule: state them\nunread.yaml: "
            ),
            "{stderr}"
        );
    }

    #[test]
    fn what_cannot_be_read_exits_2() {
        let (status, _, stderr) = tested("config", "listeners: {}\n", &[("tests.yaml", PASSES)]);
        assert_eq!(status, 2);
        assert!(
            stderr.starts_with("error: config config.yaml cannot be read:\n"),
            "{stderr}"
        );

        let missing = std::env::temp_dir().join("edgerush-testing-no-such-tests.yaml");
        let scratch = Scratch::new("missing", &[("config.yaml", CONFIG)]);
        let args = [
            "--config".to_owned(),
            scratch.path(0),
            missing.to_str().unwrap().to_owned(),
        ];
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(args.into_iter(), &mut stdout, &mut stderr);
        assert_eq!(status, 2);
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).unwrap();
        assert!(stderr.starts_with("error: tests "), "{stderr}");
        assert!(stderr.contains(" cannot be read: "), "{stderr}");
    }

    #[test]
    fn the_command_line_names_one_config_and_its_files_of_tests() {
        let parsed = |args: &[&str]| parse(args.iter().map(ToString::to_string));
        assert_eq!(parsed(&["a.yaml", "-h"]), Ok(Parsed::Help));
        assert_eq!(parsed(&["a.yaml"]), Err(UsageError::NoConfig));
        assert_eq!(parsed(&["--config", "c.yaml"]), Err(UsageError::NoTests));
        assert_eq!(parsed(&["--config"]), Err(UsageError::NoValue));
        assert_eq!(
            parsed(&["--config", "c.yaml", "--config", "d.yaml", "a.yaml"]),
            Err(UsageError::Twice)
        );
        assert_eq!(
            parsed(&["--config", "c.yaml", "--verbose", "a.yaml"]),
            Err(UsageError::Unexpected("--verbose".to_owned()))
        );
        assert_eq!(
            parsed(&["a.yaml", "--config", "c.yaml", "b.yaml"]),
            Ok(Parsed::Test(Options {
                config: PathBuf::from("c.yaml"),
                tests: vec![PathBuf::from("a.yaml"), PathBuf::from("b.yaml")],
            }))
        );
    }

    #[test]
    fn a_usage_error_exits_2_with_the_usage() {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(
            ["--config".to_owned(), "c.yaml".to_owned()].into_iter(),
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(status, 2);
        assert!(stdout.is_empty());
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            format!("error: a file of tests is required\n\n{USAGE}")
        );
    }
}
