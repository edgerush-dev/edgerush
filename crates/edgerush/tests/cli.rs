//! End-to-end checks of the built binary: the real process, its streams and exit status.
//!
//! `edgerush proxy` is run on ports the operating system hands out, which it names on
//! stderr, between a client and upstreams made of nothing but `std`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;

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

#[test]
fn proxy_needs_a_config() -> io::Result<()> {
    let output = edgerush(&["proxy"])?;
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.starts_with("error: '--config <FILE>' is required"));
    assert!(stderr.contains("Usage: edgerush proxy"));
    Ok(())
}

#[test]
fn proxy_does_not_start_on_a_config_that_cannot_be_run() -> io::Result<()> {
    let missing = scratch("missing.yaml");
    let output = edgerush(&["proxy", "--config", &missing])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.starts_with("error: config "), "{stderr}");
    assert!(stderr.contains("it cannot be read"), "{stderr}");

    let dangling = scratch("dangling.yaml");
    std::fs::write(&dangling, config("127.0.0.1:0", "elsewhere", None))?;
    let output = edgerush(&["proxy", "--config", &dangling])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("cannot be run:\n"), "{stderr}");
    assert!(stderr.contains("elsewhere"), "{stderr}");
    Ok(())
}

#[test]
fn proxy_does_not_start_when_a_port_is_taken() -> io::Result<()> {
    let taken = TcpListener::bind("127.0.0.1:0")?;
    let address = taken.local_addr()?;
    let file = scratch("taken.yaml");
    std::fs::write(&file, config(&address.to_string(), "up", Some(address)))?;
    let output = edgerush(&["proxy", "--config", &file])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    let expected = format!("error: cannot listen on {address} for listener \"web\": ");
    assert!(stderr.starts_with(&expected), "{stderr}");
    Ok(())
}

#[test]
fn proxy_serves_the_file_and_follows_it() -> io::Result<()> {
    let (one, two) = (upstream("one")?, upstream("two")?);
    let file = scratch("followed.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(one)))?;
    // One worker, so that this runs wherever EdgeRush is developed: it is from the second
    // on that the workers share a port, and only Unix deals connections out among them.
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("1 worker, thread-per-core");
    assert!(get(web, "/")?.contains("x-upstream: one"));

    // Another config: it takes over without a restart.
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(two)))?;
    harness.wait_for("config reloaded");
    assert!(get(web, "/")?.contains("x-upstream: two"));

    // A config that cannot be run: said, and the one before it runs on.
    std::fs::write(&file, config("127.0.0.1:0", "elsewhere", Some(one)))?;
    let rejected = harness.wait_for("config rejected");
    assert_eq!(rejected, "config rejected, the one before it runs on:");
    assert!(harness.wait_for("elsewhere").contains("upstream"));
    assert!(get(web, "/")?.contains("x-upstream: two"));

    // A listener that is new is told of and not bound, and what can be reloaded is.
    let more = config("127.0.0.1:0", "up", Some(one))
        + "  api: { address: \"127.0.0.1:1\", protocol: http }\n";
    std::fs::write(&file, more)?;
    harness.wait_for("config reloaded");
    harness.wait_for("warning: listener \"api\" is new");
    assert!(get(web, "/")?.contains("x-upstream: one"));

    Ok(())
}

/// `/metrics` is not served while every worker counts alone, and the flag says so rather
/// than serving one worker's numbers as if they were the pod's.
#[test]
fn metrics_are_refused_with_the_reason() -> io::Result<()> {
    let file = scratch("no-metrics.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", None))?;
    let output = edgerush(&["proxy", "--config", &file, "--metrics", "127.0.0.1:0"])?;
    assert_eq!(output.status.code(), Some(2));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.starts_with("error: '--metrics' is not served yet"),
        "{stderr}"
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn every_worker_serves_the_file_and_follows_it() -> io::Result<()> {
    let (one, two) = (upstream("one")?, upstream("two")?);
    let file = scratch("per-core.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(one)))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "3"])?;
    // One port for the sockets of all three, though the config left it open.
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("3 workers, thread-per-core");

    // The kernel picks a worker for every connection: enough of them meet all three.
    for _ in 0..24 {
        assert!(get(web, "/")?.contains("x-upstream: one"));
    }
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(two)))?;
    harness.wait_for("config reloaded");
    for _ in 0..24 {
        assert!(get(web, "/")?.contains("x-upstream: two"));
    }
    Ok(())
}

/// A second worker wants a second socket on the one port, which is `SO_REUSEPORT` and is
/// not everywhere. One worker asks nothing of the kernel and runs anywhere.
#[cfg(not(unix))]
#[test]
fn more_than_one_worker_does_not_start_where_a_port_cannot_be_shared() -> io::Result<()> {
    let file = scratch("per-core.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", None))?;
    let output = edgerush(&["proxy", "--config", &file, "--workers", "2"])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.starts_with("error: cannot listen on "), "{stderr}");
    assert!(stderr.contains("SO_REUSEPORT"), "{stderr}");
    Ok(())
}

/// A path for a file of the test's own, as text for the command line.
fn scratch(name: &str) -> String {
    let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join(name);
    path.to_str().expect("a path that is text").to_owned()
}

/// A config whose listener `web` sends everything to the upstream `up` — or to whatever
/// name `backend` is, whether there is such an upstream or not. Listeners come last, so
/// that a test can add more.
fn config(listen: &str, backend: &str, up: Option<SocketAddr>) -> String {
    let endpoints = up.map(|up| format!("\"{up}\"")).unwrap_or_default();
    format!(
        r#"upstreams:
  up: {{ endpoints: [{endpoints}] }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends: [{{ upstream: {backend}, weight: 1 }}]
listeners:
  web: {{ address: "{listen}", protocol: http }}
"#
    )
}

/// Starts an upstream that answers every request with its name in `x-upstream`.
fn upstream(name: &'static str) -> io::Result<SocketAddr> {
    let socket = TcpListener::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    thread::spawn(move || {
        for stream in socket.incoming() {
            let Ok(mut stream) = stream else { continue };
            thread::spawn(move || {
                let mut head = Vec::new();
                let mut byte = [0];
                while !head.ends_with(b"\r\n\r\n") && matches!(stream.read(&mut byte), Ok(1)) {
                    head.push(byte[0]);
                }
                let answer = format!(
                    "HTTP/1.1 200 OK\r\nx-upstream: {name}\r\ncontent-length: 0\r\n\
                     connection: close\r\n\r\n"
                );
                let _gone = stream.write_all(answer.as_bytes());
            });
        }
    });
    Ok(address)
}

/// Sends a request on a connection of its own and reads the answer to its end.
fn get(to: SocketAddr, target: &str) -> io::Result<String> {
    let mut stream = TcpStream::connect(to)?;
    stream.set_read_timeout(Some(PATIENCE))?;
    write!(
        stream,
        "GET {target} HTTP/1.1\r\nhost: harness.test\r\nconnection: close\r\n\r\n"
    )?;
    let mut answer = String::new();
    stream.read_to_string(&mut answer)?;
    Ok(answer)
}

/// How long a test waits for what should come within a second or two.
const PATIENCE: Duration = Duration::from_secs(20);

/// A running `edgerush proxy`, killed when the test is over, and what it says on stderr.
struct Harness {
    process: Child,
    said: Receiver<String>,
}

impl Harness {
    fn start(args: &[&str]) -> io::Result<Self> {
        let mut process = Command::new(env!("CARGO_BIN_EXE_edgerush"))
            .arg("proxy")
            .args(args)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let stderr = process.stderr.take().expect("stderr is piped");
        let (say, said) = mpsc::channel();
        thread::spawn(move || {
            for line in BufReader::new(stderr).lines().map_while(Result::ok) {
                if say.send(line).is_err() {
                    break;
                }
            }
        });
        Ok(Self { process, said })
    }

    /// The next line on stderr that has `part` in it; the lines before it are let go.
    fn wait_for(&mut self, part: &str) -> String {
        let mut passed = Vec::new();
        loop {
            match self.said.recv_timeout(PATIENCE) {
                Ok(line) if line.contains(part) => return line,
                Ok(line) => passed.push(line),
                Err(_) => panic!("no line with {part:?} came; these did: {passed:#?}"),
            }
        }
    }

    fn address_after(&mut self, prefix: &str) -> SocketAddr {
        let line = self.wait_for(prefix);
        let address = line.strip_prefix(prefix).expect("a line that starts so");
        address.parse().expect("an address")
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _exited_already = self.process.kill();
        let _reaped = self.process.wait();
    }
}
