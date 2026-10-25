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

    // A listener that is new is told of and not bound, and what can be reloaded is.
    let more = config("127.0.0.1:0", "up", Some(one))
        + "  api: { address: \"127.0.0.1:1\", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }\n";
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
    let plane = |listeners: &str| {
        format!(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\nlisteners:\n{listeners}"
        )
    };
    let http = "  http: { address: \"[::]:8080\", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }\n";
    let data_plane = root.join("data-plane.yaml");
    std::fs::write(&data_plane, plane(http))?;
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
        "1 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 0 TLS hosts not served over HTTPS; 0 paths shadowed; 1 Ingresses of other classes or none passed over\n"
    );

    // An Ingress that asks for what is not translated yet is left out, and said.
    std::fs::write(
        manifests.join("tls.yml"),
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata: { name: secure, namespace: shop }\nspec:\n  ingressClassName: edge\n  tls: [{ secretName: shop }]\n  rules:\n    - host: shop.example.com\n      http:\n        paths:\n          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }\n",
    )?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Ingress shop/secure: not supported yet: a TLS entry without hosts\n1 routes and 1 upstreams translated; 1 left out; 0 paths without their backend; 0 TLS hosts not served over HTTPS; 0 paths shadowed; 1 Ingresses of other classes or none passed over\n"
    );
    std::fs::remove_file(manifests.join("tls.yml"))?;

    // A host `tls` lists whose Secret is not among the manifests is served over plain HTTP,
    // and said, as it is not served as asked.
    std::fs::write(
        manifests.join("tls.yml"),
        "apiVersion: networking.k8s.io/v1\nkind: Ingress\nmetadata: { name: secure, namespace: shop }\nspec:\n  ingressClassName: edge\n  tls: [{ hosts: [secure.example.com], secretName: secure }]\n  rules:\n    - host: secure.example.com\n      http:\n        paths:\n          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }\n",
    )?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "Ingress shop/secure: secure.example.com is served over HTTP only: Secret shop/secure does not exist\n2 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 1 TLS hosts not served over HTTPS; 0 paths shadowed; 1 Ingresses of other classes or none passed over\n"
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
2 routes and 1 upstreams translated; 0 left out; 0 paths without their backend; 0 TLS hosts not served over HTTPS; 1 paths shadowed; 1 Ingresses of other classes or none passed over
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
2 routes and 1 upstreams translated; 0 left out; 1 paths without their backend; 0 TLS hosts not served over HTTPS; 0 paths shadowed; 1 Ingresses of other classes or none passed over
"
    );
    std::fs::remove_file(manifests.join("gone.yml"))?;

    // A config the data plane would refuse: a `tcp` listener with no route of its own.
    let tcp = "  db: { address: \"[::]:5432\", protocol: tcp, proxy_protocol: off }\n";
    std::fs::write(&data_plane, plane(&format!("{http}{tcp}")))?;
    let output = validate(&manifests)?;
    assert_eq!(output.status.code(), Some(1), "{output:?}");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.ends_with(
            "0 left out; 0 paths without their backend; 0 TLS hosts not served over HTTPS; 0 paths shadowed; 1 Ingresses of other classes or none passed over\nthe data plane would refuse the config:\n  listener `db`: a `tcp` listener needs exactly one TCP route; it has 0\n"
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

/// The config of [`config`] with no upstream to reach: for the commands that only read it.
fn config_text() -> String {
    config("127.0.0.1:1", "up", None)
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
