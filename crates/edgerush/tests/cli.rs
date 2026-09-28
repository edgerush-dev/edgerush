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
    let args = [
        "--config",
        &file,
        "--workers",
        "1",
        "--metrics",
        "127.0.0.1:0",
    ];
    let mut harness = Harness::start(&args)?;
    let web = harness.address_after("listener \"web\" is on ");
    let metrics = harness.address_after("metrics are on ");
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
        + "  api: { address: \"127.0.0.1:1\", protocol: http, forwarding: { trusted_proxies: [], trusted_only_headers: [] } }\n";
    std::fs::write(&file, more)?;
    harness.wait_for("config reloaded");
    harness.wait_for("warning: listener \"api\" is new");
    assert!(get(web, "/")?.contains("x-upstream: one"));

    let scrape = get(metrics, "/metrics")?;
    assert!(scrape.starts_with("HTTP/1.1 200 OK\r\n"), "{scrape}");
    assert!(
        scrape.contains("edgerush_config_reloads_total 2\n"),
        "{scrape}"
    );
    let served = "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"} 4\n";
    assert!(scrape.contains(served), "{scrape}");
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

/// The workers count into one set of numbers, not one each: every request is in the
/// scrape, wherever it was served, and one reload is one reload however many workers saw
/// it. Connections are left to the kernel so that they really do land on several workers.
#[cfg(unix)]
#[test]
fn what_every_worker_counts_is_added_up_in_one_scrape() -> io::Result<()> {
    const REQUESTS: usize = 24;
    let (one, two) = (upstream("one")?, upstream("two")?);
    let file = scratch("shared-counters.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(one)))?;
    let args = [
        "--config",
        &file,
        "--workers",
        "3",
        "--accept",
        "kernel",
        "--metrics",
        "127.0.0.1:0",
    ];
    let mut harness = Harness::start(&args)?;
    let web = harness.address_after("listener \"web\" is on ");
    let metrics = harness.address_after("metrics are on ");
    harness.wait_for("3 workers, thread-per-core");

    // A connection of its own for every request, so the kernel spreads them out.
    for _ in 0..REQUESTS {
        assert!(get(web, "/")?.contains("x-upstream: one"));
    }
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(two)))?;
    harness.wait_for("config reloaded");

    let scrape = get(metrics, "/metrics")?;
    let served =
        format!("edgerush_listener_responses_total{{listener=\"web\",class=\"2xx\"}} {REQUESTS}\n");
    assert!(scrape.contains(&served), "{scrape}");
    let accepted =
        format!("edgerush_listener_connections_accepted_total{{listener=\"web\"}} {REQUESTS}\n");
    assert!(scrape.contains(&accepted), "{scrape}");
    assert!(
        scrape.contains("edgerush_config_reloads_total 1\n"),
        "{scrape}"
    );
    Ok(())
}

/// Told to stop, the harness drains (03 §10): a request under way is answered, saying the
/// connection closes; nothing new is taken; and the process exits once the last
/// connection has gone.
#[cfg(unix)]
#[test]
fn told_to_stop_it_finishes_what_is_under_way_and_exits() -> io::Result<()> {
    let (up, arrived, release) = held_upstream()?;
    let file = scratch("drained.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(up)))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "2"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("2 workers, thread-per-core");

    let mut under_way = TcpStream::connect(web)?;
    under_way.set_read_timeout(Some(PATIENCE))?;
    under_way.write_all(b"GET / HTTP/1.1\r\nhost: harness.test\r\n\r\n")?;
    arrived
        .recv_timeout(PATIENCE)
        .expect("the request reached the upstream");
    harness.signal("TERM")?;
    harness.wait_for("draining");
    // Every worker's sweep, which comes every second, has brought it the drain.
    thread::sleep(Duration::from_millis(1500));
    assert!(
        TcpStream::connect(web).is_err(),
        "still accepting while draining"
    );

    release.send(()).expect("the upstream waits");
    let mut answer = String::new();
    under_way.read_to_string(&mut answer)?;
    let answer = answer.to_lowercase();
    assert!(answer.starts_with("http/1.1 200 ok\r\n"), "{answer}");
    assert!(answer.contains("connection: close\r\n"), "{answer}");
    harness.wait_for("drained");
    assert_eq!(harness.exited()?.code(), Some(0));
    Ok(())
}

/// A second signal while draining does not wait for what is under way.
#[cfg(unix)]
#[test]
fn a_second_signal_while_draining_exits_at_once() -> io::Result<()> {
    let (up, arrived, _release) = held_upstream()?;
    let file = scratch("stopped.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(up)))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("1 worker, thread-per-core");

    let mut under_way = TcpStream::connect(web)?;
    under_way.write_all(b"GET / HTTP/1.1\r\nhost: harness.test\r\n\r\n")?;
    arrived
        .recv_timeout(PATIENCE)
        .expect("the request reached the upstream");
    harness.signal("TERM")?;
    harness.wait_for("draining");
    harness.signal("INT")?;
    let said = harness.wait_for("stopped while draining");
    assert_eq!(said, "stopped while draining: 1 connections cut off");
    assert_eq!(harness.exited()?.code(), Some(0));
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
        forward: {{ backends: [{{ upstream: {backend}, weight: 1 }}] }}
listeners:
  web: {{ address: "{listen}", protocol: http, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }} }}
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

/// Starts an upstream that says on `arrived` when a request has come, and answers it
/// once told to on `release`.
#[cfg(unix)]
fn held_upstream() -> io::Result<(SocketAddr, Receiver<()>, mpsc::Sender<()>)> {
    let socket = TcpListener::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    let (arrive, arrived) = mpsc::channel();
    let (release, released) = mpsc::channel::<()>();
    thread::spawn(move || {
        let Ok((mut stream, _)) = socket.accept() else {
            return;
        };
        let mut head = Vec::new();
        let mut byte = [0];
        while !head.ends_with(b"\r\n\r\n") && matches!(stream.read(&mut byte), Ok(1)) {
            head.push(byte[0]);
        }
        let _arrived = arrive.send(());
        if released.recv().is_ok() {
            let _gone = stream.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok");
        }
    });
    Ok((address, arrived, release))
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

    /// Sends the process the signal named, as `kill` names it.
    #[cfg(unix)]
    fn signal(&self, name: &str) -> io::Result<()> {
        let status = Command::new("kill")
            .arg(format!("-{name}"))
            .arg(self.process.id().to_string())
            .status()?;
        assert!(status.success(), "kill -{name} failed");
        Ok(())
    }

    /// Waits for the process to exit, for no longer than [`PATIENCE`].
    #[cfg(unix)]
    fn exited(&mut self) -> io::Result<std::process::ExitStatus> {
        let began = std::time::Instant::now();
        loop {
            if let Some(status) = self.process.try_wait()? {
                return Ok(status);
            }
            assert!(began.elapsed() < PATIENCE, "the process did not exit");
            thread::sleep(Duration::from_millis(20));
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
