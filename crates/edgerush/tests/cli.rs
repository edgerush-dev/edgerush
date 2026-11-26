//! End-to-end checks of the built binary: the real process, its streams and exit status.
//!
//! `edgerush proxy` is run on ports the operating system hands out, which it names on
//! stderr, between a client and upstreams made of nothing but `std`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_certificates::testing::Authority;
use edgerush_config::HarnessFile;
use edgerush_control_service::{Credentials, Heard, Served, acceptor, serve};
use edgerush_control_stream::{Listening, Offer, Versions};
use std::hash::{BuildHasher, RandomState};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, UdpSocket};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::Duration;
use tokio::sync::watch;

fn edgerush(args: &[&str]) -> io::Result<Output> {
    Command::new(env!("CARGO_BIN_EXE_edgerush"))
        .args(args)
        // Outside a cluster, wherever the tests run.
        .env_remove("KUBERNETES_SERVICE_HOST")
        .env_remove("KUBERNETES_SERVICE_PORT")
        .output()
}

/// An upstream whose endpoints come from Kubernetes cannot be followed outside a cluster:
/// a config with one is refused at the start, and on a reload, the one before it running
/// on. Followed within one, it is tested in kind.
#[test]
fn proxy_refuses_discovered_upstreams_outside_a_cluster() -> io::Result<()> {
    let listed = config("127.0.0.1:0", "up", None);
    let discovered = listed.replace(
        "endpoints: { listed: [] }",
        "endpoints: { kubernetes: \"web.shop.svc.cluster.local:http\" }",
    );
    assert_ne!(discovered, listed, "{listed}");
    let outside = "upstream `up`: its endpoints come from Kubernetes, and the environment names no API server";
    let file = scratch("outside-a-cluster.yaml");
    std::fs::write(&file, &discovered)?;
    let output = edgerush(&["proxy", "--config", &file])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains(outside), "{stderr}");

    let one = upstream("one")?;
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(one)))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("1 worker, thread-per-core");
    std::fs::write(&file, &discovered)?;
    assert_eq!(
        harness.wait_for("config rejected"),
        "config rejected, the one before it runs on:"
    );
    assert!(harness.wait_for("upstream `up`").contains(outside));
    assert!(get(web, "/")?.contains("x-upstream: one"));
    Ok(())
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
    assert!(
        stderr.starts_with("error: '--config <FILE>' or '--control-plane <ADDRESS>' is required")
    );
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

/// Rate limits are started before a request is served: a request under a limit is decided
/// by them, though no service pod answers (a key new to the data plane is decided by it
/// alone), and one over the limit is refused.
#[test]
fn proxy_starts_its_rate_limits_before_it_serves() -> io::Result<()> {
    let one = upstream("one")?;
    let limits = scratch("started-limits.yaml");
    std::fs::write(
        &limits,
        "limits:\n  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 2 } } }\n",
    )?;
    let file = scratch("started.yaml");
    let limited = config("127.0.0.1:0", "up", Some(one)).replace(
        "        forward:",
        "        rate_limits: [per-client]\n        forward:",
    ) + &format!(
        "rate_limiting: {{ mode: asynchronous, service: {{ addresses: [\"127.0.0.1:1\"], key_secret: 00112233445566778899aabbccddeeff }} }}\nlimits_file: '{limits}'\n"
    );
    std::fs::write(&file, limited)?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("1 worker, thread-per-core");
    assert!(get(web, "/")?.contains("x-upstream: one"));
    assert!(get(web, "/")?.contains("x-upstream: one"));
    let refused = get(web, "/")?;
    assert!(refused.starts_with("HTTP/1.1 429 "), "{refused}");
    Ok(())
}

/// In local mode the process limits on its own, with no service; with rate limiting left
/// out, the same limits are checked, said at start to be off, and every request goes on.
#[test]
fn proxy_limits_locally_and_says_when_limits_are_off() -> io::Result<()> {
    let one = upstream("one")?;
    let limits = scratch("local-limits.yaml");
    std::fs::write(
        &limits,
        "limits:\n  - { name: per-client, key: client, model: { bucket: { rate: 10/h, burst: 2 } } }\n",
    )?;
    let limited = |section: &str| {
        config("127.0.0.1:0", "up", Some(one)).replace(
            "        forward:",
            "        rate_limits: [per-client]\n        forward:",
        ) + &format!("{section}limits_file: '{limits}'\n")
    };
    let file = scratch("local.yaml");
    std::fs::write(&file, limited("rate_limiting: { mode: local }\n"))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("1 worker, thread-per-core");
    assert!(get(web, "/")?.contains("x-upstream: one"));
    assert!(get(web, "/")?.contains("x-upstream: one"));
    let refused = get(web, "/")?;
    assert!(refused.starts_with("HTTP/1.1 429 "), "{refused}");
    drop(harness);

    let file = scratch("off.yaml");
    std::fs::write(&file, limited(""))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("rate limiting is off: 1 limit checked, none enforced");
    harness.wait_for("1 worker, thread-per-core");
    for _ in 0..4 {
        assert!(get(web, "/")?.contains("x-upstream: one"));
    }
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

    // A listener that is new is listened on as its config takes over, and served.
    let free = free_over_udp_and_tcp()?;
    let more = config("127.0.0.1:0", "up", Some(one)).replace("[web]", "[web, api]")
        + &format!(
            "  api: {{ address: \"{free}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
        );
    std::fs::write(&file, more)?;
    harness.wait_for("config reloaded");
    assert_eq!(harness.address_after("listener \"api\" is on "), free);
    assert!(get(free, "/")?.contains("x-upstream: one"));
    assert!(get(web, "/")?.contains("x-upstream: one"));

    let scrape = get(metrics, "/metrics")?;
    assert!(scrape.starts_with("HTTP/1.1 200 OK\r\n"), "{scrape}");
    assert!(
        scrape.contains("edgerush_config_reloads_total 2\n"),
        "{scrape}"
    );
    let served = "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"} 4\n";
    assert!(scrape.contains(served), "{scrape}");
    let added = "edgerush_listener_responses_total{listener=\"api\",class=\"2xx\"} 1\n";
    assert!(scrape.contains(added), "{scrape}");

    // A listener whose port is taken is said not to be served, the rest of its config
    // running; and it is listened on at the next config, once the port is free.
    let taken = TcpListener::bind("127.0.0.1:0")?;
    let busy = taken.local_addr()?;
    let with_busy = |comment: &str| {
        config("127.0.0.1:0", "up", Some(two)).replace("[web]", "[web, busy]")
            + &format!(
                "  busy: {{ address: \"{busy}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n# {comment}\n"
            )
    };
    std::fs::write(&file, with_busy("taken"))?;
    harness.wait_for("config reloaded");
    let refused = harness.wait_for("for listener \"busy\"");
    assert!(
        refused.starts_with(&format!(
            "warning: cannot listen on {busy} for listener \"busy\": "
        )),
        "{refused}"
    );
    assert!(
        refused.ends_with(": it is not served, and is tried again at the next config"),
        "{refused}"
    );
    assert!(get(web, "/")?.contains("x-upstream: two"));
    drop(taken);
    std::fs::write(&file, with_busy("free"))?;
    harness.wait_for("config reloaded");
    assert_eq!(harness.address_after("listener \"busy\" is on "), busy);
    assert!(get(busy, "/")?.contains("x-upstream: two"));

    // Renamed, it is another listener, on an address the one gone holds until it has
    // drained: not opened beside it, which would share the port between the two, but once
    // it has.
    let renamed = with_busy("renamed").replace("busy", "moved");
    std::fs::write(&file, renamed)?;
    harness.wait_for("config reloaded");
    assert_eq!(
        harness.wait_for("listener \"moved\" is on "),
        format!(
            "warning: listener \"moved\" is on {busy}, which listener \"busy\" holds: it waits \
             for that one to drain"
        )
    );
    harness.wait_for("listener \"busy\" has drained");
    assert_eq!(harness.address_after("listener \"moved\" is on "), busy);
    assert!(get(busy, "/")?.contains("x-upstream: two"));
    Ok(())
}

/// A listener a config takes away is no longer listened on, and its connections drain as
/// at shutdown: one with a request under way is answered, saying it closes. Moved
/// meanwhile, it waits for that drain, then is listened on where it went, its counts from
/// zero (03 §10).
#[test]
fn proxy_drains_a_listener_taken_away_and_moves_it_once_drained() -> io::Result<()> {
    let one = upstream("one")?;
    let (held, arrived, release) = held_upstream()?;
    // Every port it binds drawn, none twice, so that none is another's: one left to the
    // operating system could be given the port `moved` holds here, once that is let go.
    let mut ports: Vec<SocketAddr> = Vec::with_capacity(4);
    while ports.len() < 4 {
        let drawn = free_over_udp_and_tcp()?;
        if !ports.contains(&drawn) {
            ports.push(drawn);
        }
    }
    let (web, first, moved, metrics_on) = (ports[0], ports[1], ports[2], ports[3].to_string());
    let listener = |name: &str, at: &str| {
        format!(
            "  {name}: {{ address: \"{at}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
        )
    };
    let with = |api: Option<SocketAddr>| {
        let on = if api.is_some() { "[web, api]" } else { "[web]" };
        let mut yaml = format!(
            r#"upstreams:
  up: {{ load_balancer: p2c, endpoints: {{ listed: ["{one}"] }} }}
  held: {{ load_balancer: p2c, endpoints: {{ listed: ["{held}"] }} }}
routes:
  - name: everything
    listeners: {on}
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: /held }}
        forward: {{ backends: [{{ upstream: held, weight: 1 }}] }}
      - matches:
          - path: {{ prefix: / }}
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
listeners:
"#
        );
        yaml += &listener("web", &web.to_string());
        if let Some(api) = api {
            yaml += &listener("api", &api.to_string());
        }
        yaml
    };
    let file = scratch("moved-while-draining.yaml");
    std::fs::write(&file, with(Some(first)))?;
    let args = [
        "--config",
        &file,
        "--workers",
        "1",
        "--metrics",
        &metrics_on,
    ];
    let mut harness = Harness::start(&args)?;
    let metrics = harness.address_after("metrics are on ");
    harness.wait_for("1 worker, thread-per-core");
    assert!(get(first, "/")?.contains("x-upstream: one"));

    // A request under way on `api`, held at its upstream, when `api` is taken away.
    let mut under_way = TcpStream::connect(first)?;
    under_way.set_read_timeout(Some(PATIENCE))?;
    write!(
        under_way,
        "GET /held HTTP/1.1\r\nhost: harness.test\r\n\r\n"
    )?;
    arrived
        .recv_timeout(PATIENCE)
        .expect("the request at its upstream");
    std::fs::write(&file, with(None))?;
    harness.wait_for("config reloaded");
    assert!(
        refused_soon(first),
        "a listener taken away is still listened on"
    );

    // Moved while it drains, it waits for the drain.
    std::fs::write(&file, with(Some(moved)))?;
    harness.wait_for("config reloaded");
    assert_eq!(
        harness.wait_for("listener \"api\""),
        "warning: listener \"api\" waits for its earlier sockets to drain"
    );
    assert!(
        TcpStream::connect(moved).is_err(),
        "listened on before the drain ended"
    );

    // Answered, the connection closing, and the drain ends with it.
    release.send(()).expect("the upstream to answer");
    let mut answer = String::new();
    under_way.read_to_string(&mut answer)?;
    let answer = answer.to_ascii_lowercase();
    assert!(answer.starts_with("http/1.1 200 ok\r\n"), "{answer}");
    assert!(answer.contains("\r\nconnection: close\r\n"), "{answer}");
    harness.wait_for("listener \"api\" has drained");
    assert_eq!(harness.address_after("listener \"api\" is on "), moved);
    assert!(get(moved, "/")?.contains("x-upstream: one"));
    // Two answered where it was, counted there, and not here.
    let scrape = get(metrics, "/metrics")?;
    let counted = "edgerush_listener_responses_total{listener=\"api\",class=\"2xx\"} 1\n";
    assert!(scrape.contains(counted), "{scrape}");
    Ok(())
}

/// A port free over both UDP and TCP, for a test that names it to the proxy, for both or
/// for one alone, named at random from 16,384 to 32,767, below every system's ephemeral
/// range (Linux's starts at 32,768, Windows' and macOS's at 49,152). A test lets the port
/// go before the proxy listens on it, at its start or on a reload, and the sockets other
/// tests leave to the system to place can never take it there: only another test's draw
/// can (#566, #594). A port taken, or kept from one protocol (Windows keeps runs of ports
/// from each, #300, #310), is drawn again.
fn free_over_udp_and_tcp() -> io::Result<SocketAddr> {
    let keys = RandomState::new();
    (0..16u64)
        .find_map(|draw| {
            // Below 16,384, so the sum stays below 32,768.
            let port = 16384 + u16::try_from(keys.hash_one(draw) % 16384).unwrap_or_default();
            let udp = UdpSocket::bind(("127.0.0.1", port)).ok()?;
            let address = udp.local_addr().ok()?;
            TcpListener::bind(address).ok().map(|_| address)
        })
        .ok_or_else(|| io::Error::other("no port free over UDP and TCP"))
}

/// A `udp` listener is served beside a TCP-based one on the same port, as a config may
/// have it (24 §2); taken away, it drains, the TCP listener left alone, and goes once its
/// last session has ended; and added again on that port, which the TCP listener keeps, it
/// is served: TCP and UDP ports are not one.
#[test]
fn proxy_serves_a_udp_listener_beside_a_tcp_one_and_follows_it() -> io::Result<()> {
    let (one, echo) = (upstream("one")?, udp_echo()?);
    let with = |shared: SocketAddr, dns: bool| {
        let mut yaml = config(&shared.to_string(), "up", Some(one)).replace(
            "upstreams:\n",
            &format!(
                "upstreams:\n  echo: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"{echo}\"] }} }}\n"
            ),
        );
        if dns {
            yaml = yaml.replace(
                "listeners:\n",
                "udp_routes: [{ name: dns, listeners: [dns], backends: [{ upstream: echo, weight: 1 }] }]\nlisteners:\n",
            );
            // A second's idle bound: its session ends idle, long before the drain's bound.
            yaml.push_str(&format!(
                "  dns: {{ address: \"{shared}\", protocol: udp, session_idle_seconds: 1 }}\n"
            ));
        }
        yaml
    };
    let file = scratch("udp.yaml");
    let args = [
        "--config",
        &file,
        "--workers",
        "1",
        "--metrics",
        "127.0.0.1:0",
    ];
    let (mut harness, shared) = Harness::start_on_free(&file, |at| with(at, true), &args)?;
    let on = format!("listener \"dns\" is on {shared}, over UDP");
    assert_eq!(harness.wait_for("listener \"dns\" is on "), on);
    let metrics = harness.address_after("metrics are on ");
    harness.wait_for("1 worker, thread-per-core");
    assert!(get(shared, "/")?.contains("x-upstream: one"));
    assert_eq!(udp_exchange(shared, b"over UDP")?, b"over UDP");
    // A session, counted as a connection accepted.
    let accepted = "edgerush_listener_connections_accepted_total{listener=\"dns\"} 1\n";
    let scrape = get(metrics, "/metrics")?;
    assert!(scrape.contains(accepted), "{scrape}");

    // Taken away: it drains, until its session has ended, and the TCP listener is left
    // alone.
    std::fs::write(&file, with(shared, false))?;
    harness.wait_for("listener \"dns\" has drained");
    assert!(get(shared, "/")?.contains("x-upstream: one"));

    // Back on the port the TCP listener keeps: served, not held off until a restart.
    std::fs::write(&file, with(shared, true))?;
    // Said once the reload has been.
    assert_eq!(harness.wait_for("listener \"dns\" is on "), on);
    assert_eq!(udp_exchange(shared, b"again")?, b"again");
    assert!(get(shared, "/")?.contains("x-upstream: one"));
    Ok(())
}

/// A listener a reload turns from a TCP-based protocol to `udp`, or back, keeping its name
/// and address, is one taken away and one added, as a listener moved is (03 §10): its
/// old sockets drained, then sockets of its new transport opened, and it is served on them.
#[test]
fn proxy_follows_a_listener_turned_between_tcp_and_udp() -> io::Result<()> {
    let (one, echo) = (upstream("one")?, udp_echo()?);
    let file = scratch("tcp-to-udp.yaml");
    let args = ["--config", &file, "--workers", "1"];
    let (mut harness, at) =
        Harness::start_on_free(&file, |at| config(&at.to_string(), "up", Some(one)), &args)?;
    let over_tcp = config(&at.to_string(), "up", Some(one));
    // A second's idle bound: its session ends idle, long before the drain's bound.
    let over_udp = format!(
        "upstreams:\n  echo: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"{echo}\"] }} }}\n\
         routes: []\n\
         udp_routes: [{{ name: dns, listeners: [web], backends: [{{ upstream: echo, weight: 1 }}] }}]\n\
         listeners:\n  web: {{ address: \"{at}\", protocol: udp, session_idle_seconds: 1 }}\n"
    );
    assert_eq!(harness.address_after("listener \"web\" is on "), at);
    harness.wait_for("1 worker, thread-per-core");
    assert!(get(at, "/")?.contains("x-upstream: one"));

    std::fs::write(&file, &over_udp)?;
    harness.wait_for("listener \"web\" has drained");
    let on_udp = format!("listener \"web\" is on {at}, over UDP");
    assert_eq!(harness.wait_for("listener \"web\" is on "), on_udp);
    assert_eq!(udp_exchange(at, b"over UDP")?, b"over UDP");
    assert!(refused_soon(at), "still listened on over TCP");

    std::fs::write(&file, &over_tcp)?;
    harness.wait_for("listener \"web\" has drained");
    assert_eq!(harness.address_after("listener \"web\" is on "), at);
    assert!(get(at, "/")?.contains("x-upstream: one"));
    Ok(())
}

/// A `udp` listener a reload keeps keeps its socket and its sessions: a client sending all
/// through reload after reload is answered every time, by the endpoint its session was made
/// for, though the reloads gave most of the weight to another upstream; new clients go to
/// that one (24 §5).
#[test]
fn proxy_keeps_a_udp_listeners_sessions_across_reloads() -> io::Result<()> {
    let (a, b) = (udp_naming(b"a")?, udp_naming(b"b")?);
    let with = |at: SocketAddr, other: u32, idle: u32| {
        let up = 1;
        format!(
            "upstreams:\n  up: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"{a}\"] }} }}\n  \
             other: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"{b}\"] }} }}\n\
             routes: []\n\
             udp_routes: [{{ name: dns, listeners: [dns], backends: [{{ upstream: up, weight: {up} }}, {{ upstream: other, weight: {other} }}] }}]\n\
             listeners:\n  dns: {{ address: \"{at}\", protocol: udp, session_idle_seconds: {idle} }}\n"
        )
    };
    let file = scratch("udp-reloads.yaml");
    let args = ["--config", &file, "--workers", "1"];
    let (mut harness, at) = Harness::start_on_free(&file, |at| with(at, 0, 60), &args)?;
    harness.wait_for("listener \"dns\" is on ");
    harness.wait_for("1 worker, thread-per-core");
    let client = UdpSocket::bind("127.0.0.1:0")?;
    client.connect(at)?;
    client.set_read_timeout(Some(PATIENCE))?;
    assert_eq!(asked(&client)?, b"a");

    // Sending all the while, on a thread of its own, as the reloads come.
    let done = Arc::new(AtomicBool::new(false));
    let sending = {
        let (client, done) = (client.try_clone()?, Arc::clone(&done));
        thread::spawn(move || -> io::Result<usize> {
            let mut answered = 0;
            while !done.load(Ordering::Relaxed) {
                assert_eq!(asked(&client)?, b"a", "after {answered} answers");
                answered += 1;
                thread::sleep(Duration::from_millis(5));
            }
            Ok(answered)
        })
    };
    for idle in 61..=63 {
        std::fs::write(&file, with(at, 9, idle))?;
        harness.wait_for("config reloaded");
    }
    // Past the worker's next sweep, which is when it sees a reload.
    thread::sleep(Duration::from_millis(1500));
    done.store(true, Ordering::Relaxed);
    let answered = sending.join().expect("the sender")?;
    assert!(answered > 10, "{answered} answers");
    // Nine in ten go to `other`: twenty new clients all missing it is one chance in 10^20.
    let mut found = false;
    for _ in 0..20 {
        if udp_exchange(at, b"?")? == b"b" {
            found = true;
            break;
        }
    }
    assert!(found, "no new client went to the new upstream");
    Ok(())
}

/// What `client`, connected, is answered with for a datagram of its own.
fn asked(client: &UdpSocket) -> io::Result<Vec<u8>> {
    client.send(b"?")?;
    let mut buffer = [0; 16];
    let len = client.recv(&mut buffer)?;
    Ok(buffer[..len].to_vec())
}

/// Starts a UDP upstream that answers every datagram with `name`.
fn udp_naming(name: &'static [u8]) -> io::Result<SocketAddr> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    thread::spawn(move || {
        let mut buffer = vec![0; 1 << 16];
        while let Ok((_, from)) = socket.recv_from(&mut buffer) {
            let _sent = socket.send_to(name, from);
        }
    });
    Ok(address)
}

/// What `to` answers a datagram `datagram` with, from a socket of the test's own.
fn udp_exchange(to: SocketAddr, datagram: &[u8]) -> io::Result<Vec<u8>> {
    let client = UdpSocket::bind("127.0.0.1:0")?;
    client.connect(to)?;
    client.set_read_timeout(Some(PATIENCE))?;
    client.send(datagram)?;
    let mut buffer = vec![0; 1 << 16];
    let len = client.recv(&mut buffer)?;
    buffer.truncate(len);
    Ok(buffer)
}

/// Starts a UDP upstream that answers every datagram with itself.
fn udp_echo() -> io::Result<SocketAddr> {
    let socket = UdpSocket::bind("127.0.0.1:0")?;
    let address = socket.local_addr()?;
    thread::spawn(move || {
        let mut buffer = vec![0; 1 << 16];
        while let Ok((len, from)) = socket.recv_from(&mut buffer) {
            let _sent = socket.send_to(&buffer[..len], from);
        }
    });
    Ok(address)
}

/// HTTP/3 turned on for a listener a config keeps is served on its port as that config
/// takes over, and turned off is drained and no longer served: its UDP sockets alone are
/// opened and closed, its TCP side served all along (03 §10).
#[test]
fn proxy_turns_a_listeners_http3_on_and_off() -> io::Result<()> {
    let one = upstream("one")?;
    let authority = Authority::new("http3");
    let issued = authority.issue(&["localhost"]);
    let (chain, key) = (scratch("http3-chain.pem"), scratch("http3-key.pem"));
    std::fs::write(&chain, &issued.chain)?;
    std::fs::write(&key, &issued.key)?;
    let with = |at: SocketAddr, http3: bool, comment: &str| {
        let http3 = if http3 {
            ", http3: { alt_svc_max_age: 60, force_retry: false }"
        } else {
            ""
        };
        format!(
            r#"upstreams:
  up: {{ load_balancer: p2c, endpoints: {{ listed: ["{one}"] }} }}
routes:
  - name: everything
    listeners: [secure]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
listeners:
  secure: {{ address: "{at}", protocol: https, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate, tls: {{ certificates: [server], without_sni: server }}{http3} }}
certificates:
  server: {{ chain_file: '{chain}', key_file: '{key}' }}
# {comment}
"#
        )
    };
    let file = scratch("http3.yaml");
    let args = ["--config", &file, "--workers", "1"];
    // Free over UDP too, for the HTTP/3 the second config turns on there.
    let (mut harness, at) = Harness::start_on_free(&file, |at| with(at, false, "off"), &args)?;
    assert_eq!(harness.address_after("listener \"secure\" is on "), at);
    harness.wait_for("1 worker, thread-per-core");
    assert!(
        !negotiates(at, Duration::from_secs(1)),
        "HTTP/3 served while off"
    );

    std::fs::write(&file, with(at, true, "on"))?;
    harness.wait_for("config reloaded");
    assert_eq!(
        harness.address_after("listener \"secure\" serves HTTP/3 on "),
        at
    );
    assert!(negotiates(at, PATIENCE), "HTTP/3 not served once on");
    assert!(TcpStream::connect(at).is_ok(), "its TCP side not served");

    std::fs::write(&file, with(at, false, "off again"))?;
    harness.wait_for("config reloaded");
    harness.wait_for("listener \"secure\" no longer serves HTTP/3");
    harness.wait_for("listener \"secure\"'s HTTP/3 has drained");
    assert!(
        !negotiates(at, Duration::from_secs(1)),
        "HTTP/3 served once off"
    );
    assert!(TcpStream::connect(at).is_ok(), "its TCP side not served");

    // On again once drained: a UDP socket of its own.
    std::fs::write(&file, with(at, true, "on again"))?;
    harness.wait_for("config reloaded");
    assert_eq!(
        harness.address_after("listener \"secure\" serves HTTP/3 on "),
        at
    );
    assert!(negotiates(at, PATIENCE), "HTTP/3 not served once on again");
    Ok(())
}

/// Whether the UDP socket at `address` answers a QUIC client within `wait`, with a version
/// negotiation for a datagram of a version nobody speaks, as large as an Initial: what an
/// HTTP/3 listener answers whatever else it is set up with.
fn negotiates(address: SocketAddr, wait: Duration) -> bool {
    let client = std::net::UdpSocket::bind("127.0.0.1:0").expect("a UDP socket");
    client.set_read_timeout(Some(wait)).expect("a read timeout");
    let mut datagram = vec![0xc0, 0x1a, 0x2a, 0x3a, 0x4a, 8];
    datagram.extend([1; 8]);
    datagram.push(8);
    datagram.extend([2; 8]);
    datagram.resize(1200, 0);
    if client.send_to(&datagram, address).is_err() {
        return false;
    }
    let mut answer = [0; 1500];
    let Ok((read, _)) = client.recv_from(&mut answer) else {
        return false;
    };
    // A long header whose version is 0.
    read > 5 && answer[0] & 0x80 != 0 && answer[1..5] == [0; 4]
}

/// Whether connecting to `address` is refused within [`PATIENCE`]: a socket the data plane
/// closes is closed by its worker a moment after the config that takes it away runs.
fn refused_soon(address: SocketAddr) -> bool {
    let began = std::time::Instant::now();
    while began.elapsed() < PATIENCE {
        if TcpStream::connect(address).is_err() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

/// The config `config` gives, as the control plane offers it.
fn offered(yaml: &str, last: Option<&Offer>, versions: &mut Versions) -> Offer {
    let file: HarnessFile = serde_saphyr::from_str(yaml).expect("a config");
    let config = file.into_config(std::collections::BTreeMap::new(), None);
    Offer::new(&config, last, versions).expect("an offer")
}

/// With `--control-plane`, nothing is served until the first config comes down the stream;
/// then each config after it takes over, with the limits it names, and one that cannot be
/// run is said not to.
#[test]
fn proxy_serves_what_its_control_plane_sends_and_follows_it() -> io::Result<()> {
    let (one, two) = (upstream("one")?, upstream("two")?);
    let authority = Authority::new("stream");
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let address = listener.local_addr()?;
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("stream");
    std::fs::create_dir_all(&dir)?;
    let issued = authority.issue(&["data-plane"]);
    let file = |name: &str, contents: &str| -> io::Result<String> {
        let path = dir.join(name);
        std::fs::write(&path, contents)?;
        Ok(path.to_str().expect("a path that is text").to_owned())
    };
    let (certificate, key, authorities) = (
        file("tls.crt", &issued.chain)?,
        file("tls.key", &issued.key)?,
        file("ca.crt", &authority.pem())?,
    );
    let mut harness = Harness::start(&[
        "--control-plane",
        &address.to_string(),
        "--certificate",
        &certificate,
        "--key",
        &key,
        "--authorities",
        &authorities,
        "--workers",
        "1",
    ])?;
    harness.wait_for("waiting for the first config from the control plane at 127.0.0.1:");
    // The control plane, serving no config yet.
    let runtime = tokio::runtime::Runtime::new()?;
    let (offers, offered_rx) = watch::channel(Served::Anyone(None));
    let presented = Credentials {
        certificate: authority.issue(&["127.0.0.1"]),
        authorities: authority.pem(),
    };
    let tls = acceptor(&presented).expect("the control plane's TLS");
    let (_tls, accepting) = watch::channel(Arc::new(tls));
    listener.set_nonblocking(true)?;
    // What the data plane reports of each config.
    let (report, reports) = mpsc::channel();
    let _serving = runtime.spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).expect("a listener");
        let heard = move |_peer: SocketAddr, heard: Heard| {
            if let Heard::Reported(reported) = heard {
                let _gone = report.send(reported);
            }
        };
        let (_kept, addresses) =
            tokio::sync::watch::channel::<Arc<[std::net::IpAddr]>>(Arc::from([]));
        serve(
            listener,
            accepting,
            offered_rx,
            addresses,
            heard,
            std::future::pending(),
        )
        .await
    });
    let reported = || reports.recv_timeout(PATIENCE).expect("a report");
    let mut versions = Versions::new(1);
    // A first config that cannot be run is refused, and the next waited for.
    let refused = offered(
        &config("127.0.0.1:0", "elsewhere", Some(one)),
        None,
        &mut versions,
    );
    offers.send_replace(Served::Anyone(Some(Arc::new(refused.clone()))));
    assert_eq!(
        harness.wait_for("config from the control plane refused"),
        "config from the control plane refused:"
    );
    let report = reported();
    assert_eq!(report.running.config, None);
    let refusal = report.refused.expect("refused");
    assert_eq!(refusal.config, refused.version());
    assert!(refusal.why.contains("elsewhere"), "{}", refusal.why);
    let first = offered(
        &config("127.0.0.1:0", "up", Some(one)),
        Some(&refused),
        &mut versions,
    );
    offers.send_replace(Served::Anyone(Some(Arc::new(first.clone()))));
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("config from the control plane runs");
    assert!(get(web, "/")?.contains("x-upstream: one"));
    let report = reported();
    assert_eq!(report.running.config.as_deref(), Some(first.version()));
    assert_eq!(report.refused, None);

    // Another config: it takes over without a restart.
    let second = offered(
        &config("127.0.0.1:0", "up", Some(two)),
        Some(&first),
        &mut versions,
    );
    offers.send_replace(Served::Anyone(Some(Arc::new(second.clone()))));
    harness.wait_for("config from the control plane runs");
    assert!(get(web, "/")?.contains("x-upstream: two"));
    assert_eq!(reported().running.config.as_deref(), Some(second.version()));

    // A listener that is new is listened on as its config takes over, as a file's is.
    let free = free_over_udp_and_tcp()?;
    let added = offered(
        &(config("127.0.0.1:0", "up", Some(two)).replace("[web]", "[web, api]")
            + &format!(
                "  api: {{ address: \"{free}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
            )),
        Some(&second),
        &mut versions,
    );
    offers.send_replace(Served::Anyone(Some(Arc::new(added.clone()))));
    harness.wait_for("config from the control plane runs");
    assert_eq!(harness.address_after("listener \"api\" is on "), free);
    assert!(get(free, "/")?.contains("x-upstream: two"));
    assert_eq!(reported().running.config.as_deref(), Some(added.version()));

    // Taken away, a listener is reported draining until it has drained, and then reported
    // no more: at once, as nothing holds it, perhaps before its config's report is made.
    let without = offered(
        &config("127.0.0.1:0", "up", Some(two)),
        Some(&added),
        &mut versions,
    );
    offers.send_replace(Served::Anyone(Some(Arc::new(without.clone()))));
    let mut report = reported();
    assert_eq!(report.running.config.as_deref(), Some(without.version()));
    if report.listening.draining == ["api"] {
        report = reported();
    }
    assert_eq!(report.listening, Listening::default());
    harness.wait_for("listener \"api\" has drained");

    // One whose port is taken is reported not served, and why; with the port free, it is
    // listened on at the next config and reported no more.
    let taken = TcpListener::bind("127.0.0.1:0")?;
    let busy = taken.local_addr()?;
    let with_busy = |backend: SocketAddr| {
        config("127.0.0.1:0", "up", Some(backend)).replace("[web]", "[web, busy]")
            + &format!(
                "  busy: {{ address: \"{busy}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
"
            )
    };
    let unbound = offered(&with_busy(one), Some(&without), &mut versions);
    offers.send_replace(Served::Anyone(Some(Arc::new(unbound.clone()))));
    let report = reported();
    assert_eq!(report.running.config.as_deref(), Some(unbound.version()));
    let [unserved] = report.listening.unserved.as_slice() else {
        panic!("{:?}", report.listening)
    };
    assert_eq!(unserved.listener, "busy");
    assert!(
        unserved
            .why
            .starts_with(&format!("cannot listen on {busy} for listener \"busy\": ")),
        "{}",
        unserved.why
    );
    drop(taken);
    let bound = offered(&with_busy(two), Some(&unbound), &mut versions);
    offers.send_replace(Served::Anyone(Some(Arc::new(bound.clone()))));
    let report = reported();
    assert_eq!(report.running.config.as_deref(), Some(bound.version()));
    assert_eq!(report.listening, Listening::default());
    assert_eq!(harness.address_after("listener \"busy\" is on "), busy);

    // A config that cannot be run: said, and the one before it runs on.
    let third = offered(
        &config("127.0.0.1:0", "elsewhere", Some(one)),
        Some(&bound),
        &mut versions,
    );
    offers.send_replace(Served::Anyone(Some(Arc::new(third.clone()))));
    let report = reported();
    assert_eq!(report.running.config.as_deref(), Some(bound.version()));
    assert_eq!(
        report.refused.map(|refusal| refusal.config),
        Some(third.version().to_owned())
    );
    let refused = harness.wait_for("config from the control plane refused");
    assert_eq!(
        refused,
        "config from the control plane refused, the one before it runs on:"
    );
    assert!(harness.wait_for("elsewhere").contains("upstream"));
    assert!(get(web, "/")?.contains("x-upstream: two"));

    // Limits come as a resource of their own, and a config naming them runs with them;
    // another config with the same limits keeps their counts, and changed limits start
    // afresh.
    let limited = |backend: SocketAddr| {
        config("127.0.0.1:0", "up", Some(backend)).replace(
            "        forward:",
            "        rate_limits: [per-client]\n        forward:",
        ) + "rate_limiting: { mode: local }\n"
    };
    let with_limits = |yaml: &str, burst: u64, last: &Offer, versions: &mut Versions| {
        let file: HarnessFile = serde_saphyr::from_str(yaml).expect("a config");
        let limits = serde_saphyr::from_str(&format!(
            "limits:\n  - {{ name: per-client, key: client, model: {{ bucket: {{ rate: 10/h, burst: {burst} }} }} }}\n"
        ))
        .expect("limits");
        let config = file.into_config(std::collections::BTreeMap::new(), Some(limits));
        Offer::new(&config, Some(last), versions).expect("an offer")
    };
    let counted = with_limits(&limited(two), 2, &third, &mut versions);
    offers.send_replace(Served::Anyone(Some(Arc::new(counted.clone()))));
    let report = reported();
    assert_eq!(report.running.config.as_deref(), Some(counted.version()));
    assert_eq!(report.running.limits.as_deref(), counted.limits());
    assert!(get(web, "/")?.contains("x-upstream: two"));
    assert!(get(web, "/")?.contains("x-upstream: two"));
    let refused = get(web, "/")?;
    assert!(refused.starts_with("HTTP/1.1 429 "), "{refused}");
    let kept = with_limits(&limited(one), 2, &counted, &mut versions);
    assert_eq!(kept.limits(), counted.limits());
    offers.send_replace(Served::Anyone(Some(Arc::new(kept.clone()))));
    assert_eq!(reported().running.config.as_deref(), Some(kept.version()));
    let refused = get(web, "/")?;
    assert!(refused.starts_with("HTTP/1.1 429 "), "{refused}");
    let changed = with_limits(&limited(one), 3, &kept, &mut versions);
    offers.send_replace(Served::Anyone(Some(Arc::new(changed.clone()))));
    assert_eq!(
        reported().running.config.as_deref(),
        Some(changed.version())
    );
    assert!(get(web, "/")?.contains("x-upstream: one"));

    // Certificate files replaced are said to be, by what the stream says as it goes.
    let renewed = authority.issue(&["data-plane"]);
    let put = |path: &str, contents: &str| -> io::Result<()> {
        let beside = format!("{path}.partial");
        std::fs::write(&beside, contents)?;
        std::fs::rename(&beside, path)
    };
    put(&key, &renewed.key)?;
    put(&certificate, &renewed.chain)?;
    harness.wait_for("the config stream's certificates are replaced");
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
/// Told to stop while it waits for its first config, it exits as asked, not as a signal's
/// default action would have it.
#[cfg(unix)]
#[test]
fn told_to_stop_while_it_waits_for_its_first_config_it_exits() -> io::Result<()> {
    let authority = Authority::new("stream");
    // An address nothing listens on.
    let address = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let dir = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("stream-waits");
    std::fs::create_dir_all(&dir)?;
    let issued = authority.issue(&["data-plane"]);
    let file = |name: &str, contents: &str| -> io::Result<String> {
        let path = dir.join(name);
        std::fs::write(&path, contents)?;
        Ok(path.to_str().expect("a path that is text").to_owned())
    };
    let (certificate, key, authorities) = (
        file("tls.crt", &issued.chain)?,
        file("tls.key", &issued.key)?,
        file("ca.crt", &authority.pem())?,
    );
    let mut harness = Harness::start(&[
        "--control-plane",
        &address.to_string(),
        "--certificate",
        &certificate,
        "--key",
        &key,
        "--authorities",
        &authorities,
    ])?;
    harness.wait_for("waiting for the first config from the control plane");
    harness.wait_for("dialled again in");
    harness.signal("TERM")?;
    assert!(harness.exited()?.success());
    Ok(())
}

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

/// A second signal that arrives just after the first, before the first has been acted on,
/// is not lost: every signal stays listened for between one and the next.
#[cfg(unix)]
#[test]
fn a_second_signal_on_the_heels_of_the_first_is_heard() -> io::Result<()> {
    let (up, arrived, _release) = held_upstream()?;
    let file = scratch("stopped-twice-at-once.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", Some(up)))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    let web = harness.address_after("listener \"web\" is on ");
    harness.wait_for("1 worker, thread-per-core");

    let mut under_way = TcpStream::connect(web)?;
    under_way.write_all(b"GET / HTTP/1.1\r\nhost: harness.test\r\n\r\n")?;
    arrived
        .recv_timeout(PATIENCE)
        .expect("the request reached the upstream");
    harness.signals(&["TERM", "INT"])?;
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
    let file = scratch("one-worker-alone.yaml");
    std::fs::write(&file, config("127.0.0.1:0", "up", None))?;
    let output = edgerush(&["proxy", "--config", &file, "--workers", "2"])?;
    assert_eq!(output.status.code(), Some(1));
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.starts_with("error: cannot listen on "), "{stderr}");
    assert!(stderr.contains("SO_REUSEPORT"), "{stderr}");
    Ok(())
}

#[test]
fn explain_says_where_a_request_goes_and_exits_0_or_2() -> io::Result<()> {
    let file = scratch("explain.yaml");
    std::fs::write(&file, config("127.0.0.1:1", "up", None))?;
    let request = [
        "--client",
        "203.0.113.7",
        "--protocol",
        "1.1",
        "--method",
        "GET",
        "--url",
        "http://shop.example.com/cart",
    ];
    let explained = |listener: &str| {
        let mut args = vec!["explain", "--config", &file, "--listener", listener];
        args.extend(request);
        edgerush(&args)
    };

    let output = explained("web")?;
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.starts_with("web (http)  GET http://shop.example.com/cart  HTTP/1.1"),
        "{stdout}"
    );
    assert!(
        stdout.contains("\n→ everything rule 0 match 0  chosen\n"),
        "{stdout}"
    );
    assert!(output.stderr.is_empty());

    let output = explained("nowhere")?;
    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr),
        "error: there is no listener nowhere\n"
    );
    Ok(())
}

#[test]
fn test_runs_a_file_of_tests_and_exits_0_or_1() -> io::Result<()> {
    let config = scratch("test-config.yaml");
    std::fs::write(&config, config_text())?;
    let tests = scratch("tests.yaml");
    let file = |route: &str| {
        format!(
            "tests:\n  - name: everything goes up\n    request: {{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"http://shop.example.com/\", headers: [] }}\n    expect: {{ route: {route}, rule: 0, forward: {{ backends: [{{ upstream: up, weight: 1 }}], mirrors: [] }} }}\n"
        )
    };

    std::fs::write(&tests, file("everything"))?;
    let output = edgerush(&["test", "--config", &config, &tests])?;
    assert_eq!(output.status.code(), Some(0));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("ok    everything goes up ("), "{stdout}");
    assert!(stdout.ends_with("\n0 failed, 1 passed\n"), "{stdout}");

    std::fs::write(&tests, file("elsewhere"))?;
    let output = edgerush(&["test", "--config", &config, &tests])?;
    assert_eq!(output.status.code(), Some(1));
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.starts_with("FAIL  everything goes up ("), "{stdout}");
    assert!(
        stdout.contains("\n  route: expected elsewhere, got everything\n"),
        "{stdout}"
    );
    Ok(())
}

/// A system bundle `SSL_CERT_FILE` names that cannot be read stops the controller at the
/// start, as a BackendTLSPolicy's `wellKnownCACertificates: System` trusts it.
#[test]
fn the_controller_does_not_start_on_a_system_bundle_it_cannot_read() -> io::Result<()> {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("system-authorities");
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(&root)?;
    let data_plane = root.join("data-plane.yaml");
    std::fs::write(
        &data_plane,
        "ingress_class: edge\ngateway: shop/web\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\n",
    )?;
    let missing = root.join("not-there.pem");
    let output = Command::new(env!("CARGO_BIN_EXE_edgerush"))
        .args([
            "controller",
            "--data-plane",
            data_plane.to_str().expect("a path that is text"),
        ])
        .env_remove("KUBERNETES_SERVICE_HOST")
        .env_remove("KUBERNETES_SERVICE_PORT")
        .env("SSL_CERT_FILE", &missing)
        .output()?;
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    let unreadable = format!(
        "error: the system's authorities: {} cannot be read",
        missing.display()
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).starts_with(&unreadable),
        "{output:?}"
    );
    Ok(())
}

/// The config of [`config`] with no upstream to reach: for the commands that only read it.
fn config_text() -> String {
    config("127.0.0.1:1", "up", None)
}

/// Each scratch file is one test's alone: the tests run side by side in one directory, and
/// one that wrote another's file would change the config that test's proxy follows.
#[test]
fn every_scratch_file_is_named_once() {
    let mut names: Vec<&str> = include_str!("cli.rs")
        .split("scratch(\"")
        .skip(1)
        .filter_map(|after| after.split_once('"').map(|(name, _)| name))
        .collect();
    names.sort_unstable();
    let twice: Vec<&&str> = names
        .windows(2)
        .filter(|pair| pair[0] == pair[1])
        .map(|pair| &pair[0])
        .collect();
    assert!(twice.is_empty(), "named more than once: {twice:?}");
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
  up: {{ load_balancer: p2c, endpoints: {{ listed: [{endpoints}] }} }}
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
  web: {{ address: "{listen}", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
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
    /// Every line it has said so far, for a wait that fails to say them all.
    heard: Vec<String>,
    /// How many of `heard` the waits have gone past; those after are still to be read.
    read: usize,
}

/// How many ports a proxy is started on before a start that is refused each time fails
/// the test.
const STARTS: usize = 5;

impl Harness {
    /// The proxy started with `args`, once the config `config` makes for an address free
    /// over UDP and TCP has been written to `file`, and that address. Free when drawn, the
    /// port can still be taken by another test before the proxy listens on it (#566): while
    /// the proxy's start says it cannot listen on the address drawn, it is started again on
    /// another. Both protocols had just been bound there, so a proxy that is refused every
    /// time is one that cannot listen on it at all, and fails the test after [`STARTS`].
    fn start_on_free(
        file: &str,
        config: impl Fn(SocketAddr) -> String,
        args: &[&str],
    ) -> io::Result<(Self, SocketAddr)> {
        let mut refused = Vec::new();
        for _ in 0..STARTS {
            let at = free_over_udp_and_tcp()?;
            std::fs::write(file, config(at))?;
            let mut harness = Self::start(args)?;
            // Every socket is open before any listener is said to be on its address, so
            // the first of these lines says whether the start was refused.
            let first = harness.peek_for(&["\" is on ", "cannot listen on "]);
            if !first.contains(&format!("cannot listen on {at} ")) {
                return Ok((harness, at));
            }
            refused.push(first);
        }
        panic!("all {STARTS} starts were refused: {refused:#?}");
    }

    fn start(args: &[&str]) -> io::Result<Self> {
        let mut process = Command::new(env!("CARGO_BIN_EXE_edgerush"))
            .arg("proxy")
            .args(args)
            // Outside a cluster, wherever the tests run.
            .env_remove("KUBERNETES_SERVICE_HOST")
            .env_remove("KUBERNETES_SERVICE_PORT")
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
        Ok(Self {
            process,
            said,
            heard: Vec::new(),
            read: 0,
        })
    }

    /// The next line on stderr that has `part` in it; the lines before it are let go. One
    /// that does not come fails the test, saying every line said since the start.
    fn wait_for(&mut self, part: &str) -> String {
        self.wait_for_one_of(&[part])
    }

    /// The next line on stderr that has one of `parts` in it, left to be read again, as
    /// are the lines before it.
    fn peek_for(&mut self, parts: &[&str]) -> String {
        let read = self.read;
        let line = self.wait_for_one_of(parts);
        self.read = read;
        line
    }

    /// [`Self::wait_for`], for a line with any one of `parts` in it.
    fn wait_for_one_of(&mut self, parts: &[&str]) -> String {
        loop {
            let line = match self.heard.get(self.read) {
                Some(line) => line.clone(),
                None => match self.said.recv_timeout(PATIENCE) {
                    Ok(line) => {
                        self.heard.push(line.clone());
                        line
                    }
                    Err(_) => panic!(
                        "no line with {parts:?} came; every line said: {:#?}",
                        self.heard
                    ),
                },
            };
            self.read += 1;
            if parts.iter().any(|part| line.contains(part)) {
                return line;
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

    /// Sends the process the signals named, in order, from one shell's own `kill`, so that
    /// they arrive microseconds apart rather than a process start apart.
    #[cfg(unix)]
    fn signals(&self, names: &[&str]) -> io::Result<()> {
        let kills: Vec<String> = names
            .iter()
            .map(|name| format!("kill -{name} $0"))
            .collect();
        let status = Command::new("sh")
            .arg("-c")
            .arg(kills.join(" && "))
            .arg(self.process.id().to_string())
            .status()?;
        assert!(status.success(), "{kills:?} failed");
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
