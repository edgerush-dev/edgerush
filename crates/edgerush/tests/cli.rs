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
use edgerush_control_service::{Credentials, Heard, acceptor, serve};
use edgerush_control_stream::{Listening, Offer, Versions};
use std::io::{self, BufRead, BufReader, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
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
        "rate_limiting: {{ mode: asynchronous, service: {{ addresses: [\"127.0.0.1:1\"] }} }}\nlimits_file: '{limits}'\n"
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
    let free = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
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
    // Every port it binds chosen at once, so that none is another's: one left to the
    // operating system could be given the port `moved` holds here, once that is let go.
    let reserved: Vec<TcpListener> = (0..4)
        .map(|_| TcpListener::bind("127.0.0.1:0"))
        .collect::<io::Result<_>>()?;
    let ports: Vec<SocketAddr> = reserved
        .iter()
        .map(TcpListener::local_addr)
        .collect::<io::Result<_>>()?;
    drop(reserved);
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
    let at = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let with = |http3: bool, comment: &str| {
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
    std::fs::write(&file, with(false, "off"))?;
    let mut harness = Harness::start(&["--config", &file, "--workers", "1"])?;
    assert_eq!(harness.address_after("listener \"secure\" is on "), at);
    harness.wait_for("1 worker, thread-per-core");
    assert!(
        !negotiates(at, Duration::from_secs(1)),
        "HTTP/3 served while off"
    );

    std::fs::write(&file, with(true, "on"))?;
    harness.wait_for("config reloaded");
    assert_eq!(
        harness.address_after("listener \"secure\" serves HTTP/3 on "),
        at
    );
    assert!(negotiates(at, PATIENCE), "HTTP/3 not served once on");
    assert!(TcpStream::connect(at).is_ok(), "its TCP side not served");

    std::fs::write(&file, with(false, "off again"))?;
    harness.wait_for("config reloaded");
    harness.wait_for("listener \"secure\" no longer serves HTTP/3");
    harness.wait_for("listener \"secure\"'s HTTP/3 has drained");
    assert!(
        !negotiates(at, Duration::from_secs(1)),
        "HTTP/3 served once off"
    );
    assert!(TcpStream::connect(at).is_ok(), "its TCP side not served");

    // On again once drained: a UDP socket of its own.
    std::fs::write(&file, with(true, "on again"))?;
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
/// then each config after it takes over, and one that cannot be run is said not to.
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
    let (offers, offered_rx) = watch::channel(None);
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
    offers.send_replace(Some(Arc::new(refused.clone())));
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
    offers.send_replace(Some(Arc::new(first.clone())));
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
    offers.send_replace(Some(Arc::new(second.clone())));
    harness.wait_for("config from the control plane runs");
    assert!(get(web, "/")?.contains("x-upstream: two"));
    assert_eq!(reported().running.config.as_deref(), Some(second.version()));

    // A listener that is new is listened on as its config takes over, as a file's is.
    let free = TcpListener::bind("127.0.0.1:0")?.local_addr()?;
    let added = offered(
        &(config("127.0.0.1:0", "up", Some(two)).replace("[web]", "[web, api]")
            + &format!(
                "  api: {{ address: \"{free}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}\n"
            )),
        Some(&second),
        &mut versions,
    );
    offers.send_replace(Some(Arc::new(added.clone())));
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
    offers.send_replace(Some(Arc::new(without.clone())));
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
    offers.send_replace(Some(Arc::new(unbound.clone())));
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
    offers.send_replace(Some(Arc::new(bound.clone())));
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
    offers.send_replace(Some(Arc::new(third.clone())));
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

#[test]
fn validate_translates_a_directory_of_manifests_and_exits_0_1_or_2() -> io::Result<()> {
    let root = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("validate");
    let manifests = root.join("manifests");
    // Afresh: a run that failed half way leaves its files behind.
    if root.exists() {
        std::fs::remove_dir_all(&root)?;
    }
    std::fs::create_dir_all(manifests.join("shop"))?;
    // A data plane of these more settings; of no Gateway, it binds `http` on 80, and `https`
    // on 443 for the Ingresses' TLS hosts.
    let plane = |more: &str| {
        format!(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\n{more}"
        )
    };
    let data_plane = root.join("data-plane.yaml");
    std::fs::write(&data_plane, plane(""))?;
    // Read from a directory below, from a `List`, beside a kind that is not translated, and
    // without the file that is not YAML.
    std::fs::write(
        manifests.join("shop").join("web.yaml"),
        "apiVersion: v1\nkind: List\nitems:\n  - { apiVersion: networking.k8s.io/v1, kind: IngressClass, metadata: { name: edge }, spec: { controller: edgerush.dev/ingress-controller } }\n  - apiVersion: networking.k8s.io/v1\n    kind: Ingress\n    metadata: { name: web, namespace: shop }\n    spec:\n      ingressClassName: edge\n      rules:\n        - host: shop.example.com\n          http:\n            paths:\n              - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }\n  - { apiVersion: v1, kind: Service, metadata: { name: web, namespace: shop }, spec: { ports: [{ name: http, port: 80 }] } }\n---\napiVersion: apps/v1\nkind: Deployment\nmetadata: { name: web, namespace: shop }\n",
    )?;
    std::fs::write(manifests.join("README.md"), "not manifests")?;
    // Another data plane's: counted, not translated.
    std::fs::write(
        manifests.join("theirs.yaml"),
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata: { name: theirs, namespace: shop }\nspec:\n  ingressClassName: other\n",
    )?;
    let validate = |manifests: &std::path::Path| {
        edgerush(&[
            "validate",
            "--from-manifests",
            manifests.to_str().expect("a path that is text"),
            "--data-plane",
            data_plane.to_str().expect("a path that is text"),
        ])
    };

    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "1 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 1 Ingresses of other classes or none passed over\n"
    );

    // An Ingress that asks for what is not translated yet is left out, and said.
    std::fs::write(
        manifests.join("rewritten.yml"),
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata: { name: rewritten, namespace: shop, annotations: { nginx.ingress.kubernetes.io/rewrite-target: / } }\nspec:\n  ingressClassName: edge\n  rules:\n    - host: shop.example.com\n      http:\n        paths:\n          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }\n",
    )?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Ingress shop/rewritten: not supported yet: annotations\n1 routes and 1 upstreams translated; 1 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 1 Ingresses of other classes or none passed over\n"
    );
    std::fs::remove_file(manifests.join("rewritten.yml"))?;

    // A Secret `tls` names is taken as it will be in the cluster, not looked at, unless
    // asked: the host is secured as named, a route over HTTPS and one redirecting plain
    // HTTP there, and the summary says so. Asked, one not among the manifests leaves its
    // host over plain HTTP, and is said.
    std::fs::write(
        manifests.join("tls.yml"),
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata: { name: secure, namespace: shop }\nspec:\n  ingressClassName: edge\n  tls: [{ hosts: [secure.example.com], secretName: secure }]\n  rules:\n    - host: secure.example.com\n      http:\n        paths:\n          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }\n",
    )?;
    let secured = |checked: bool| {
        let mut args = vec![
            "validate",
            "--from-manifests",
            manifests.to_str().expect("a path that is text"),
            "--data-plane",
            data_plane.to_str().expect("a path that is text"),
        ];
        if checked {
            args.push("--check-secrets");
        }
        edgerush(&args)
    };
    let output = secured(false)?;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "3 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 1 Ingresses of other classes or none passed over\nTLS Secrets were taken as they will be in the cluster, not checked: --check-secrets checks them\n"
    );
    let output = secured(true)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Ingress shop/secure: secure.example.com is refused over HTTPS and redirected there over plain HTTP: Secret shop/secure does not exist\n2 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 1 TLS hosts or entries not served as listed; 0 paths shadowed; 1 Ingresses of other classes or none passed over\n"
    );
    std::fs::remove_file(manifests.join("tls.yml"))?;

    // A path another Ingress already has, the same way on the same host, is translated and
    // said, as it never takes a request.
    std::fs::write(
        manifests.join("shadow.yml"),
        "apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: web2, namespace: shop }
spec:
  ingressClassName: edge
  rules:
    - host: shop.example.com
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }
",
    )?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Ingress shop/web2: path / (Prefix) on shop.example.com is also Ingress shop/web's; which takes it depends on which the cluster made first
2 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 1 paths shadowed; 1 Ingresses of other classes or none passed over
"
    );
    std::fs::remove_file(manifests.join("shadow.yml"))?;

    // A path whose Service is not there is translated, answered 500 until it is, and said.
    std::fs::write(
        manifests.join("gone.yml"),
        "apiVersion: networking.k8s.io/v1
kind: Ingress
metadata: { name: gone, namespace: shop }
spec:
  ingressClassName: edge
  rules:
    - host: gone.example.com
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: gone, port: { number: 80 } } } }
",
    )?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Ingress shop/gone: path / (Prefix) on gone.example.com is answered 500: Service shop/gone does not exist
2 routes and 1 upstreams translated; 0 left out; 1 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 1 Ingresses of other classes or none passed over
"
    );
    std::fs::remove_file(manifests.join("gone.yml"))?;

    // The data plane a Gateway whose listener claims `*.example.com` on the port Ingress's
    // `http` is bound to: the Ingress's shop.example.com takes that host from it there, which
    // is said, and validate exits 1.
    std::fs::write(
        manifests.join("gateway.yml"),
        "apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata: { name: edgerush }
spec: { controllerName: edgerush.dev/gateway-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: web, namespace: edge }
spec:
  gatewayClassName: edgerush
  listeners: [{ name: wild, port: 80, protocol: HTTP, hostname: \"*.example.com\" }]
",
    )?;
    let gateway_plane = root.join("gateway-data-plane.yaml");
    std::fs::write(&gateway_plane, plane("gateway: edge/web\n"))?;
    let output = edgerush(&[
        "validate",
        "--from-manifests",
        manifests.to_str().expect("a path that is text"),
        "--data-plane",
        gateway_plane.to_str().expect("a path that is text"),
    ])?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with(
            "Ingress shop/web claims shop.example.com, taking it from Gateway edge/web listener wild's *.example.com, which does not fall through\n"
        ),
        "{output:?}"
    );
    std::fs::remove_file(manifests.join("gateway.yml"))?;

    // A note fails nothing: a retry's attempts taken as the data plane's most are said, and
    // validate exits 0.
    std::fs::write(
        manifests.join("gateway.yml"),
        "apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata: { name: edgerush }
spec: { controllerName: edgerush.dev/gateway-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: web, namespace: edge }
spec:
  gatewayClassName: edgerush
  listeners: [{ name: http, port: 8080, protocol: HTTP, allowedRoutes: { namespaces: { from: All } } }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: retried, namespace: shop }
spec:
  parentRefs: [{ name: web, namespace: edge }]
  hostnames: [retried.example.com]
  rules: [{ backendRefs: [{ name: web, port: 80 }], retry: { attempts: 100 } }]
",
    )?;
    let output = edgerush(&[
        "validate",
        "--from-manifests",
        manifests.to_str().expect("a path that is text"),
        "--data-plane",
        gateway_plane.to_str().expect("a path that is text"),
    ])?;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.starts_with(
            "note: HTTPRoute shop/retried: rule 0: a retry of 100 attempts is taken as 5, the most the data plane makes\n"
        ),
        "{stdout}"
    );
    assert!(
        stdout.contains("; 0 left out;") && stdout.ends_with("; 1 notes\n"),
        "{stdout}"
    );
    std::fs::remove_file(manifests.join("gateway.yml"))?;

    // An HTTPS listener's Secret is named, and taken as it will be in the cluster, unless
    // validate is asked to check it: then one that is not there leaves the listener out.
    std::fs::write(
        manifests.join("gateway.yml"),
        "apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata: { name: edgerush }
spec: { controllerName: edgerush.dev/gateway-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: web, namespace: edge }
spec:
  gatewayClassName: edgerush
  listeners:
    - name: secure
      port: 8443
      protocol: HTTPS
      hostname: secure.example.com
      tls: { certificateRefs: [{ name: web-tls }] }
",
    )?;
    let secure = |more: &[&str]| {
        let mut args = vec![
            "validate",
            "--from-manifests",
            manifests.to_str().expect("a path that is text"),
            "--data-plane",
            gateway_plane.to_str().expect("a path that is text"),
        ];
        args.extend(more);
        edgerush(&args)
    };
    let output = secure(&[])?;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).ends_with(
            "TLS Secrets were taken as they will be in the cluster, not checked: --check-secrets checks them\n"
        ),
        "{output:?}"
    );
    let output = secure(&["--check-secrets"])?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with(
            "Gateway edge/web: listener secure: a certificate cannot be used: Secret edge/web-tls does not exist\n"
        ),
        "{output:?}"
    );
    std::fs::remove_file(manifests.join("gateway.yml"))?;

    // A config the data plane would refuse: a port's settings trusting a network written
    // wrongly.
    std::fs::write(
        &data_plane,
        plane(
            "ports:\n  80: { forwarding: { trusted_proxies: [\"10.1.2.3/8\"], trusted_only_headers: [] } }\n",
        ),
    )?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.ends_with(
            "0 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 1 Ingresses of other classes or none passed over\nthe data plane would refuse the config:\n  listener `port-80`: `forwarding`: `10.1.2.3/8` has bits set past its prefix; the range is `10.0.0.0/8`\n"
        ),
        "{stdout}"
    );

    // Files that cannot be read are not validated at all.
    let output = validate(&root.join("nowhere"))?;
    assert_eq!(output.status.code(), Some(2), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stderr).starts_with("error: "),
        "{output:?}"
    );
    Ok(())
}

/// A BackendTLSPolicy's `wellKnownCACertificates: System` trusts the authorities of the file
/// `SSL_CERT_FILE` names, which `validate` reads as the controller does; a file named that
/// cannot be read stops either at the start.
#[test]
fn the_systems_authorities_are_those_ssl_cert_file_names() -> io::Result<()> {
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
    let manifests = root.join("manifests.yaml");
    std::fs::write(
        &manifests,
        "apiVersion: networking.k8s.io/v1
kind: IngressClass
metadata: { name: edge }
spec: { controller: edgerush.dev/ingress-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata: { name: edgerush }
spec: { controllerName: edgerush.dev/gateway-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: web, namespace: shop }
spec:
  gatewayClassName: edgerush
  listeners: [{ name: http, port: 8080, protocol: HTTP }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: pages, namespace: shop }
spec:
  parentRefs: [{ name: web }]
  rules: [{ backendRefs: [{ name: web, port: 443 }] }]
---
apiVersion: gateway.networking.k8s.io/v1
kind: BackendTLSPolicy
metadata: { name: public, namespace: shop }
spec:
  targetRefs: [{ group: \"\", kind: Service, name: web }]
  validation: { wellKnownCACertificates: System, hostname: web.shop.example }
---
apiVersion: v1
kind: Service
metadata: { name: web, namespace: shop }
spec: { ports: [{ name: https, port: 443 }] }
",
    )?;
    let bundle = root.join("bundle.pem");
    std::fs::write(
        &bundle,
        edgerush_certificates::testing::certificate(&["root.test"]).chain,
    )?;
    let missing = root.join("not-there.pem");
    let run = |command: &[&str], bundle: &std::path::Path| {
        let mut args = command.to_vec();
        args.extend([
            "--data-plane",
            data_plane.to_str().expect("a path that is text"),
        ]);
        Command::new(env!("CARGO_BIN_EXE_edgerush"))
            .args(args)
            .env_remove("KUBERNETES_SERVICE_HOST")
            .env_remove("KUBERNETES_SERVICE_PORT")
            .env("SSL_CERT_FILE", bundle)
            .output()
    };
    let validate = [
        "validate",
        "--from-manifests",
        manifests.to_str().expect("a path that is text"),
    ];
    let output = run(&validate, &bundle)?;
    assert_eq!(output.status.code(), Some(0), "{output:?}");
    assert!(
        String::from_utf8_lossy(&output.stdout).starts_with(
            "1 routes and 1 upstreams translated; 0 left out; 0 paths without their backend;"
        ),
        "{output:?}"
    );
    let unreadable = format!(
        "error: the system's authorities: {} cannot be read",
        missing.display()
    );
    for command in [&validate[..], &["controller"]] {
        let output = run(command, &missing)?;
        assert_eq!(output.status.code(), Some(2), "{command:?}: {output:?}");
        assert!(
            String::from_utf8_lossy(&output.stderr).starts_with(&unreadable),
            "{command:?}: {output:?}"
        );
    }
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
}

impl Harness {
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
        })
    }

    /// The next line on stderr that has `part` in it; the lines before it are let go. One
    /// that does not come fails the test, saying every line said since the start.
    fn wait_for(&mut self, part: &str) -> String {
        loop {
            match self.said.recv_timeout(PATIENCE) {
                Ok(line) => {
                    self.heard.push(line.clone());
                    if line.contains(part) {
                        return line;
                    }
                }
                Err(_) => panic!(
                    "no line with {part:?} came; every line said: {:#?}",
                    self.heard
                ),
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
