//! The proxy over real sockets: a client, the proxy and upstreams, all on the loopback
//! interface and on ports the operating system hands out.
//!
//! The upstream answers every request with a description of what it saw, which is how the
//! tests look at the upstream's side; requests to `/echo` get their own body back, frame by
//! frame as it arrives.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_config::{Compiled, Config, compile};
use edgerush_proxy::{Proxy, Upstream, Worker};
use http::{HeaderMap, Method, Request, Response, StatusCode, Version};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Either, Empty, Full};
use hyper::body::{Body, Bytes, Frame, Incoming};
use hyper::service::service_fn;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto;
use std::collections::{BTreeMap, BTreeSet};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

type ClientBody = BoxBody<Bytes, Infallible>;

/// Starts an upstream that says `name` in an `x-upstream` field of every answer.
async fn upstream(name: &'static str) -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let (stream, _) = socket.accept().await.unwrap();
            tokio::spawn(async move {
                let service = service_fn(move |request| async move {
                    Ok::<_, Infallible>(upstream_answer(name, request))
                });
                let _closed = auto::Builder::new(TokioExecutor::new())
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
    address
}

fn upstream_answer(
    name: &'static str,
    request: Request<Incoming>,
) -> Response<Either<Incoming, Full<Bytes>>> {
    let (head, body) = request.into_parts();
    let body = if head.uri.path() == "/echo" {
        Either::Left(body)
    } else {
        let mut seen = format!("{} {} {:?}\n", head.method, head.uri, head.version);
        for (name, value) in &head.headers {
            seen.push_str(&format!("{name}: {}\n", value.to_str().unwrap()));
        }
        Either::Right(Full::new(Bytes::from(seen)))
    };
    let mut response = Response::builder()
        .header("x-upstream", name)
        .header("x-powered-by", "upstream");
    if head.uri.path() == "/hop" {
        // An upstream that talks about its connection to the proxy, and as if it were a
        // proxy that the gateway has to authenticate to.
        response = response
            .header("connection", "x-upstream-hop")
            .header("x-upstream-hop", "1")
            .header("keep-alive", "timeout=5")
            .header("proxy-authenticate", "Basic realm=\"upstream\"")
            .header("proxy-authentication-info", "nextnonce=\"abc\"")
            .header("www-authenticate", "Basic realm=\"origin\"");
    }
    response.body(body).unwrap()
}

/// An address nothing listens on: one that was just given to a socket that is gone again.
async fn dead_endpoint() -> SocketAddr {
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    socket.local_addr().unwrap()
}

/// Starts a proxy for the config, with every listener on a port of its own choosing; the
/// addresses by the listeners' names.
async fn proxy(yaml: &str) -> BTreeMap<String, SocketAddr> {
    reloadable_proxy(yaml).await.1
}

/// Which way these tests reach an upstream. The suite is the same either way — that is
/// the point of it — so it is told rather than written twice:
///
/// ```text
/// cargo test                                  the engine's client
/// EDGERUSH_TEST_UPSTREAM=ours cargo test      EdgeRush's own
/// ```
fn upstream_under_test() -> Upstream {
    match std::env::var("EDGERUSH_TEST_UPSTREAM").as_deref() {
        Ok("ours") => Upstream::Ours,
        _ => Upstream::Hyper,
    }
}

/// The same, with the proxy itself for reloading it.
async fn reloadable_proxy(yaml: &str) -> (Arc<Proxy>, BTreeMap<String, SocketAddr>) {
    let proxy =
        Arc::new(Proxy::new(compiled(yaml), NonZeroUsize::MIN, upstream_under_test()).unwrap());
    let mut addresses = BTreeMap::new();
    let mut sockets = Vec::new();
    for (position, listener) in proxy.listeners().iter().enumerate() {
        // Of the standard library's kind, to be handed to the worker's runtime and to no
        // other, exactly as the harness hands its listeners over.
        let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        socket.set_nonblocking(true).unwrap();
        addresses.insert(listener.clone(), socket.local_addr().unwrap());
        sockets.push((position, socket));
    }
    on_a_worker(Arc::clone(&proxy), sockets);
    (proxy, addresses)
}

/// Serves the listeners the way a data plane does: a thread of its own, a single-threaded
/// runtime and a `LocalSet`, with the worker's share of the data plane made on that
/// thread — the shape the proxy really runs in, and not one where a connection may wander
/// between threads.
fn on_a_worker(proxy: Arc<Proxy>, sockets: Vec<(usize, std::net::TcpListener)>) {
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let entered = runtime.enter();
        let worker = Worker::new(proxy);
        local.spawn_local(Rc::clone(&worker).maintain());
        for (position, socket) in sockets {
            let socket = TcpListener::from_std(socket).unwrap();
            local.spawn_local(Rc::clone(&worker).serve(position, socket));
        }
        drop(entered);
        // Accepting never ends, so neither does this: the thread goes with the process.
        runtime.block_on(local);
    });
}

fn compiled(yaml: &str) -> Compiled {
    let config: Config = serde_saphyr::from_str(yaml).unwrap();
    compile(&config).unwrap()
}

/// A config with the named listeners, each sending every request to the upstream it is
/// paired with and saying which config it is in an `x-config` field of the response.
fn everything_to(listeners: &[(&str, SocketAddr)], config: &str) -> String {
    let mut yaml = String::from("listeners:\n");
    for (at, (listener, _)) in listeners.iter().enumerate() {
        yaml += &format!("  {listener}: {{ address: \"127.0.0.1:{at}\", protocol: http }}\n");
    }
    yaml += "routes:\n";
    for (listener, _) in listeners {
        yaml += &format!(
            r#"  - name: {listener}
    listeners: [{listener}]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        filters:
          - type: response_header_modifier
            set: [{{ name: x-config, value: "{config}" }}]
        backends:
          - {{ upstream: {listener}, weight: 1 }}
"#
        );
    }
    yaml += "upstreams:\n";
    for (listener, upstream) in listeners {
        yaml += &format!("  {listener}: {{ endpoints: [\"{upstream}\"] }}\n");
    }
    yaml
}

/// A shop with a cart, as far as the tests share a config.
async fn shop() -> SocketAddr {
    let cart = upstream("cart").await;
    let pages = upstream("pages").await;
    let dead = dead_endpoint().await;
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: shop
    listeners: [web]
    hostnames:
      - {{ name: shop.example.com, falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: /cart }}
        filters:
          - type: request_header_modifier
            set: [{{ name: X-Gateway, value: edgerush }}]
            remove: [x-debug]
          - type: response_header_modifier
            add: [{{ name: X-Served-By, value: edgerush }}]
            remove: [x-powered-by]
        backends:
          - {{ upstream: cart, weight: 1 }}
      - matches:
          - path: {{ exact: /closed }}
        backends:
          - {{ upstream: cart, weight: 0 }}
      - matches:
          - path: {{ exact: /empty }}
        backends:
          - {{ upstream: empty, weight: 1 }}
      - matches:
          - path: {{ exact: /dead }}
        backends:
          - {{ upstream: dead, weight: 1 }}
      - matches:
          - path: {{ prefix: / }}
        backends:
          - {{ upstream: pages, weight: 1 }}
upstreams:
  cart: {{ endpoints: ["{cart}"] }}
  dead: {{ endpoints: ["{dead}"] }}
  empty: {{ endpoints: [] }}
  pages: {{ endpoints: ["{pages}"] }}
"#
    );
    proxy(&yaml).await["web"]
}

fn client(http2: bool) -> Client<HttpConnector, ClientBody> {
    let mut builder = Client::builder(TokioExecutor::new());
    builder.http2_only(http2);
    builder.build(HttpConnector::new())
}

fn request(method: Method, proxy: SocketAddr, target: &str) -> http::request::Builder {
    Request::builder()
        .method(method)
        .uri(format!("http://{proxy}{target}"))
        .header("host", "shop.example.com")
}

fn get(proxy: SocketAddr, target: &str) -> Request<ClientBody> {
    request(Method::GET, proxy, target)
        .body(Empty::new().boxed())
        .unwrap()
}

/// Sends the request and reads the whole answer: status, headers and body as text.
async fn send(request: Request<ClientBody>) -> (StatusCode, HeaderMap, String) {
    let http2 = request.version() == Version::HTTP_2;
    send_with(&client(http2), request).await
}

/// The same with a client of the caller's, whose connections are kept between requests.
async fn send_with(
    client: &Client<HttpConnector, ClientBody>,
    request: Request<ClientBody>,
) -> (StatusCode, HeaderMap, String) {
    let response = within(client.request(request)).await.unwrap();
    let (head, body) = response.into_parts();
    let body = within(body.collect()).await.unwrap().to_bytes();
    (
        head.status,
        head.headers,
        String::from_utf8(body.to_vec()).unwrap(),
    )
}

/// A test that waits for what never comes should fail, not hang.
async fn within<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("timed out")
}

/// Writes raw bytes to the proxy and reads until it closes the connection.
async fn raw(proxy: SocketAddr, bytes: &str) -> String {
    let mut stream = TcpStream::connect(proxy).await.unwrap();
    stream.write_all(bytes.as_bytes()).await.unwrap();
    let mut answer = String::new();
    within(stream.read_to_string(&mut answer)).await.unwrap();
    answer
}

/// A request body that is written to from the test, a frame at a time.
struct ChannelBody(mpsc::Receiver<Bytes>);

impl Body for ChannelBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        self.0
            .poll_recv(context)
            .map(|data| data.map(|data| Ok(Frame::data(data))))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_goes_to_its_upstream_and_the_answer_comes_back() {
    let proxy = shop().await;
    let (status, headers, seen) = send(get(proxy, "/about?lang=en")).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-upstream"], "pages");
    assert!(seen.starts_with("GET /about?lang=en HTTP/1.1\n"), "{seen}");
    assert!(seen.contains("host: shop.example.com\n"), "{seen}");

    let (status, headers, _) = send(get(proxy, "/cart/items")).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-upstream"], "cart");
}

#[tokio::test(flavor = "multi_thread")]
async fn the_upstream_sees_the_normal_form_of_the_path() {
    let proxy = shop().await;
    let (status, headers, seen) = send(get(proxy, "/pages/../cart//items/%7e?next=/a/../b")).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-upstream"], "cart");
    assert!(
        seen.starts_with("GET /cart/items/~?next=/a/../b "),
        "{seen}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_rule_changes_the_headers_of_request_and_response() {
    let proxy = shop().await;
    let request = request(Method::GET, proxy, "/cart")
        .header("x-debug", "1")
        .header("x-gateway", "spoofed")
        .header("x-other", "kept")
        .body(Empty::new().boxed())
        .unwrap();
    let (status, headers, seen) = send(request).await;
    assert_eq!(status, 200);
    assert!(seen.contains("x-gateway: edgerush\n"), "{seen}");
    assert!(seen.contains("x-other: kept\n"), "{seen}");
    assert!(!seen.contains("x-debug"), "{seen}");
    assert!(!seen.contains("spoofed"), "{seen}");
    assert_eq!(headers["x-served-by"], "edgerush");
    assert!(!headers.contains_key("x-powered-by"));

    // A rule without changes leaves both as they are.
    let (_, headers, _) = send(get(proxy, "/about")).await;
    assert_eq!(headers["x-powered-by"], "upstream");
    assert!(!headers.contains_key("x-served-by"));
}

#[tokio::test(flavor = "multi_thread")]
async fn bodies_stream_in_both_directions() {
    let proxy = shop().await;
    let (frames, body) = mpsc::channel(1);
    let request = request(Method::POST, proxy, "/echo")
        .body(ChannelBody(body).boxed())
        .unwrap();
    let response = within(client(false).request(request)).await.unwrap();
    assert_eq!(response.status(), 200);
    let mut echo = response.into_body();

    // Every frame comes back before the next is sent: nothing waits for the whole body.
    for text in ["one", "two", "three"] {
        frames.send(Bytes::from(text)).await.unwrap();
        let frame = within(echo.frame()).await.unwrap().unwrap();
        assert_eq!(frame.into_data().unwrap(), text);
    }
    drop(frames);
    assert!(within(echo.frame()).await.is_none());
}

#[tokio::test(flavor = "multi_thread")]
async fn a_large_body_comes_through_whole() {
    let proxy = shop().await;
    let sent: Vec<u8> = (0..4_000_000_u32).map(|at| at.to_le_bytes()[0]).collect();
    let request = request(Method::POST, proxy, "/echo")
        .body(Full::new(Bytes::from(sent.clone())).boxed())
        .unwrap();
    let response = within(client(false).request(request)).await.unwrap();
    let echoed = within(response.into_body().collect()).await.unwrap();
    assert!(echoed.to_bytes() == sent);
}

#[tokio::test(flavor = "multi_thread")]
async fn what_cannot_be_placed_is_answered_here() {
    let proxy = shop().await;
    for (target, status) in [
        ("/cart/..%2Fadmin", StatusCode::BAD_REQUEST),
        ("/closed", StatusCode::INTERNAL_SERVER_ERROR),
        ("/empty", StatusCode::SERVICE_UNAVAILABLE),
        ("/dead", StatusCode::BAD_GATEWAY),
    ] {
        let (answered, headers, body) = send(get(proxy, target)).await;
        assert_eq!(answered, status, "{target}");
        assert!(!headers.contains_key("x-upstream"), "{target}");
        assert_eq!(body, "", "{target}");
    }

    let mut elsewhere = get(proxy, "/about");
    elsewhere
        .headers_mut()
        .insert("host", "other.example.org".parse().unwrap());
    assert_eq!(send(elsewhere).await.0, StatusCode::NOT_FOUND);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_has_one_host_for_everything_that_looks_at_it() {
    let (evil, rest) = (upstream("evil").await, upstream("rest").await);
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: by-host-header
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
            headers: [{{ name: Host, value: {{ exact: evil.example }} }}]
        backends:
          - {{ upstream: evil, weight: 1 }}
      - matches:
          - path: {{ prefix: / }}
        backends:
          - {{ upstream: rest, weight: 1 }}
upstreams:
  evil: {{ endpoints: ["{evil}"] }}
  rest: {{ endpoints: ["{rest}"] }}
"#
    );
    let proxy = proxy(&yaml).await["web"];

    // The target says one host and the `Host` field another: the target's is the host,
    // for the rule that looks at the `Host` header as for the upstream (RFC 9112 §3.2.2).
    let spoofed = "GET http://good.example/about HTTP/1.1\r\nHost: evil.example\r\n\
                   Connection: close\r\n\r\n";
    let answer = raw(proxy, spoofed).await;
    assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
    assert!(answer.contains("x-upstream: rest"), "{answer}");
    assert!(answer.contains("host: good.example\n"), "{answer}");
    assert!(!answer.contains("evil.example"), "{answer}");

    // A request that is for that host reaches the rule, by either way of saying so.
    let by_target = "GET http://evil.example/about HTTP/1.1\r\nHost: good.example\r\n\
                     Connection: close\r\n\r\n";
    let by_field = "GET /about HTTP/1.1\r\nHost: evil.example\r\nConnection: close\r\n\r\n";
    for request in [by_target, by_field] {
        let answer = raw(proxy, request).await;
        assert!(answer.contains("x-upstream: evil"), "{answer}");
        assert!(answer.contains("host: evil.example\n"), "{answer}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_request_that_names_no_host_is_rejected() {
    let proxy = shop().await;
    let answer = raw(proxy, "GET /about HTTP/1.1\r\nConnection: close\r\n\r\n").await;
    assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
    let twice = "GET /about HTTP/1.1\r\nHost: shop.example.com\r\nHost: shop.example.com\r\n\
                 Connection: close\r\n\r\n";
    let answer = raw(proxy, twice).await;
    assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_serves_one_request_after_another() {
    let proxy = shop().await;
    let requests = "GET /about HTTP/1.1\r\nHost: shop.example.com\r\n\r\n\
                    GET /cart HTTP/1.1\r\nHost: shop.example.com\r\n\r\n\
                    GET /nowhere HTTP/1.1\r\nHost: other.example.org\r\nConnection: close\r\n\r\n";
    let answers = raw(proxy, requests).await;
    let statuses: Vec<&str> = answers
        .lines()
        .filter(|line| line.starts_with("HTTP/1.1 "))
        .collect();
    assert_eq!(
        statuses,
        [
            "HTTP/1.1 200 OK",
            "HTTP/1.1 200 OK",
            "HTTP/1.1 404 Not Found"
        ]
    );
    assert!(answers.contains("x-upstream: pages"), "{answers}");
    assert!(answers.contains("x-upstream: cart"), "{answers}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cookie_string_split_by_http2_leaves_whole_over_http1() {
    let any = upstream("any").await;
    let proxy = proxy(&everything_to(&[("web", any)], "0")).await["web"];
    // Two cookie fields, which HTTP/2 allows and HTTP/1.1 does not: RFC 9113 §8.2.3.
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("http://{proxy}/account"))
        .header("cookie", "a=1")
        .header("x-between", "1")
        .header("cookie", "b=2")
        .body(Empty::new().boxed())
        .unwrap();
    let (status, _, seen) = send(request).await;
    assert_eq!(status, 200);
    assert!(seen.contains("cookie: a=1; b=2\n"), "{seen}");
    assert_eq!(seen.matches("cookie:").count(), 1, "{seen}");
    assert!(seen.contains("x-between: 1\n"), "{seen}");
}

#[tokio::test(flavor = "multi_thread")]
async fn http2_comes_in_and_http1_goes_out() {
    let proxy = shop().await;
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("http://{proxy}/cart/items?page=3"))
        .body(Empty::new().boxed())
        .unwrap();
    // The authority is the proxy's address, which no route is for.
    assert_eq!(send(request).await.0, StatusCode::NOT_FOUND);

    let any_host = upstream("any").await;
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends:
          - {{ upstream: any, weight: 1 }}
upstreams:
  any: {{ endpoints: ["{any_host}"] }}
"#
    );
    let proxy = self::proxy(&yaml).await["web"];
    let request = Request::builder()
        .version(Version::HTTP_2)
        .uri(format!("http://{proxy}/cart/./items?page=3"))
        .body(Empty::new().boxed())
        .unwrap();
    let (status, headers, seen) = send(request).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-upstream"], "any");
    assert!(
        seen.starts_with("GET /cart/items?page=3 HTTP/1.1\n"),
        "{seen}"
    );
    // The host that was routed on is the host the upstream is told.
    assert!(seen.contains(&format!("host: {proxy}\n")), "{seen}");
}

#[tokio::test(flavor = "multi_thread")]
async fn every_listener_serves_its_own_routes_and_every_endpoint_gets_requests() {
    let (one, two, admin) = (
        upstream("one").await,
        upstream("two").await,
        upstream("admin").await,
    );
    let yaml = format!(
        r#"
listeners:
  admin: {{ address: "127.0.0.1:0", protocol: http }}
  web: {{ address: "127.0.0.1:1", protocol: http }}
routes:
  - name: web
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends:
          - {{ upstream: web, weight: 1 }}
  - name: admin
    listeners: [admin]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: /status }}
        backends:
          - {{ upstream: admin, weight: 1 }}
upstreams:
  admin: {{ endpoints: ["{admin}"] }}
  web: {{ endpoints: ["{one}", "{two}"] }}
"#
    );
    let listeners = proxy(&yaml).await;

    let (status, headers, _) = send(get(listeners["admin"], "/status")).await;
    assert_eq!(
        (status, &headers["x-upstream"]),
        (StatusCode::OK, &"admin".parse().unwrap())
    );
    assert_eq!(
        send(get(listeners["admin"], "/")).await.0,
        StatusCode::NOT_FOUND
    );

    // Forty requests that all go to one of two endpoints: once in 2³⁹ runs.
    let mut served = BTreeMap::new();
    for _ in 0..40 {
        let (status, headers, _) = send(get(listeners["web"], "/status")).await;
        assert_eq!(status, 200);
        *served
            .entry(headers["x-upstream"].to_str().unwrap().to_owned())
            .or_insert(0) += 1;
    }
    assert_eq!(
        served.keys().collect::<Vec<_>>(),
        ["one", "two"],
        "{served:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn what_is_said_about_one_connection_does_not_reach_the_next() {
    let proxy = shop().await;
    let request = "GET /hop HTTP/1.1\r\nHost: shop.example.com\r\nConnection: close, x-hop\r\n\
                   X-Hop: 1\r\nKeep-Alive: timeout=5\r\nTE: trailers, gzip\r\nX-Kept: 1\r\n\r\n";
    let answer = raw(proxy, request).await;
    assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
    let (head, seen) = answer.split_once("\r\n\r\n").unwrap();

    // What the upstream saw of the request.
    assert!(seen.contains("x-kept: 1\n"), "{seen}");
    assert!(seen.contains("te: trailers\n"), "{seen}");
    for gone in ["x-hop", "keep-alive", "connection", "gzip"] {
        assert!(!seen.contains(gone), "{gone} in {seen}");
    }
    // What the client sees of the response; its own connection is closed as it asked.
    assert!(head.contains("x-upstream: pages"), "{head}");
    assert!(head.contains("connection: close"), "{head}");
    for gone in ["x-upstream-hop", "keep-alive"] {
        assert!(!head.contains(gone), "{gone} in {head}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn proxy_authentication_is_between_the_client_and_the_gateway_only() {
    let proxy = shop().await;
    let request = "GET /hop HTTP/1.1\r\nHost: shop.example.com\r\nConnection: close\r\n\
                   Proxy-Authorization: Basic dXNlcjpwYXNz\r\n\
                   Authorization: Bearer for-the-origin\r\n\r\n";
    let answer = raw(proxy, request).await;
    assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
    let (head, seen) = answer.split_once("\r\n\r\n").unwrap();

    // The application behind the gateway is not the proxy those credentials are for.
    assert!(
        seen.contains("authorization: Bearer for-the-origin\n"),
        "{seen}"
    );
    assert!(!seen.contains("proxy-authorization"), "{seen}");
    assert!(!seen.contains("dXNlcjpwYXNz"), "{seen}");
    // And what it says about proxy authentication is not for the gateway's client.
    assert!(
        head.contains("www-authenticate: Basic realm=\"origin\""),
        "{head}"
    );
    assert!(!head.contains("proxy-authenticate"), "{head}");
    assert!(!head.contains("proxy-authentication-info"), "{head}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_header_that_would_take_the_host_away_is_rejected() {
    let proxy = shop().await;
    let request =
        "GET /about HTTP/1.1\r\nHost: shop.example.com\r\nConnection: close, host\r\n\r\n";
    let answer = raw(proxy, request).await;
    assert!(answer.starts_with("HTTP/1.1 400 "), "{answer}");

    // The same request without that option is served: it was the option that was refused.
    let request = "GET /about HTTP/1.1\r\nHost: shop.example.com\r\nConnection: close\r\n\r\n";
    let answer = raw(proxy, request).await;
    assert!(answer.starts_with("HTTP/1.1 200 "), "{answer}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reload_under_load_drops_nothing() {
    let (old, new) = (upstream("old").await, upstream("new").await);
    let (proxy, addresses) = reloadable_proxy(&everything_to(&[("web", old)], "0")).await;
    let web = addresses["web"];

    // Clients that ask as fast as they are answered, each on a connection it keeps.
    let stop = Arc::new(AtomicBool::new(false));
    let clients: Vec<_> = (0..8)
        .map(|_| {
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let client = client(false);
                let (mut answered, mut upstreams) = (0_u32, BTreeSet::new());
                while !stop.load(Ordering::Relaxed) {
                    let (status, headers, _) = send_with(&client, get(web, "/")).await;
                    assert_eq!(status, 200, "after {answered} requests");
                    assert!(headers.contains_key("x-config"));
                    upstreams.insert(headers["x-upstream"].to_str().unwrap().to_owned());
                    answered += 1;
                }
                (answered, upstreams)
            })
        })
        .collect();

    // Meanwhile the config changes back and forth, and ends at the new upstream.
    for round in 1..=51 {
        let upstream = if round % 2 == 1 { new } else { old };
        let config = everything_to(&[("web", upstream)], &round.to_string());
        proxy.reload(compiled(&config)).unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    // A reload is whole at once: the first request after it is served by the new config.
    let (status, headers, _) = send(get(web, "/")).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-upstream"], "new");
    assert_eq!(headers["x-config"], "51");

    stop.store(true, Ordering::Relaxed);
    let mut seen = BTreeSet::new();
    for client in clients {
        let (answered, upstreams) = within(client).await.unwrap();
        assert!(answered > 0);
        seen.extend(upstreams);
    }
    assert_eq!(seen.into_iter().collect::<Vec<_>>(), ["new", "old"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_listener_keeps_its_socket_whatever_happens_to_the_listeners_around_it() {
    let (first, second, admin) = (
        upstream("first").await,
        upstream("second").await,
        upstream("admin").await,
    );
    let (proxy, addresses) = reloadable_proxy(&everything_to(&[("web", first)], "0")).await;
    let web = addresses["web"];
    assert_eq!(send(get(web, "/")).await.1["x-upstream"], "first");

    // A listener that sorts before it moves it down the list; its socket stays its own.
    let with_admin = everything_to(&[("admin", admin), ("web", second)], "1");
    proxy.reload(compiled(&with_admin)).unwrap();
    assert_eq!(send(get(web, "/")).await.1["x-upstream"], "second");

    // Without the listener in the config there is no route for what comes in on its socket.
    proxy
        .reload(compiled(&everything_to(&[("admin", admin)], "2")))
        .unwrap();
    assert_eq!(send(get(web, "/")).await.0, StatusCode::NOT_FOUND);

    proxy
        .reload(compiled(&everything_to(&[("web", first)], "3")))
        .unwrap();
    assert_eq!(send(get(web, "/")).await.1["x-upstream"], "first");
}

/// The value of the sample that begins with `series`, which is its name and labels.
fn sample(scrape: &str, series: &str) -> u64 {
    let line = scrape
        .lines()
        .find(|line| line.starts_with(series))
        .unwrap_or_else(|| panic!("{series} is not in\n{scrape}"));
    line.rsplit(' ').next().unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn what_happens_is_counted_and_a_reload_resets_nothing() {
    let (up, nowhere) = (upstream("up").await, dead_endpoint().await);
    let config = everything_to(&[("dead", nowhere), ("web", up)], "0");
    let (proxy, addresses) = reloadable_proxy(&config).await;
    let (web, dead) = (addresses["web"], addresses["dead"]);

    for _ in 0..3 {
        assert_eq!(send(get(web, "/")).await.0, 200);
    }
    assert_eq!(send(get(web, "/..%2f")).await.0, 400);
    assert_eq!(send(get(dead, "/")).await.0, 502);

    let scrape = proxy.metrics();
    let web_responses = "edgerush_listener_responses_total{listener=\"web\",class=";
    assert_eq!(sample(&scrape, &format!("{web_responses}\"2xx\"}}")), 3);
    assert_eq!(sample(&scrape, &format!("{web_responses}\"4xx\"}}")), 1);
    assert_eq!(sample(&scrape, &format!("{web_responses}\"5xx\"}}")), 0);
    let answers = "edgerush_listener_local_answers_total{listener=";
    assert_eq!(
        sample(&scrape, &format!("{answers}\"web\",reason=\"bad_path\"}}")),
        1
    );
    assert_eq!(
        sample(
            &scrape,
            &format!("{answers}\"dead\",reason=\"upstream_failed\"}}")
        ),
        1
    );
    let head_time = "edgerush_listener_time_to_response_head_seconds";
    assert_eq!(
        sample(&scrape, &format!("{head_time}_count{{listener=\"web\"}}")),
        4
    );
    assert_eq!(
        sample(
            &scrape,
            &format!("{head_time}_bucket{{listener=\"web\",le=\"+Inf\"}}")
        ),
        4
    );
    assert_eq!(
        sample(
            &scrape,
            "edgerush_upstream_requests_total{upstream=\"web\"}"
        ),
        3
    );
    assert_eq!(
        sample(
            &scrape,
            "edgerush_upstream_responses_total{upstream=\"web\",class=\"2xx\"}"
        ),
        3
    );
    assert_eq!(
        sample(
            &scrape,
            "edgerush_upstream_failures_total{upstream=\"web\"}"
        ),
        0
    );
    assert_eq!(
        sample(
            &scrape,
            "edgerush_upstream_requests_total{upstream=\"dead\"}"
        ),
        1
    );
    assert_eq!(
        sample(
            &scrape,
            "edgerush_upstream_failures_total{upstream=\"dead\"}"
        ),
        1
    );
    // Every request came on a connection of its own, and the clients are gone again.
    assert_eq!(
        sample(
            &scrape,
            "edgerush_listener_connections_accepted_total{listener=\"web\"}"
        ),
        4
    );
    let active = "edgerush_listener_connections_active{listener=\"web\"}";
    within(async {
        while sample(&proxy.metrics(), active) != 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;

    // Another config, with the same names: counting goes on from where it was.
    let reloaded = everything_to(&[("dead", nowhere), ("web", up)], "1");
    proxy.reload(compiled(&reloaded)).unwrap();
    assert_eq!(send(get(web, "/")).await.0, 200);
    let scrape = proxy.metrics();
    assert_eq!(sample(&scrape, &format!("{web_responses}\"2xx\"}}")), 4);
    assert_eq!(
        sample(
            &scrape,
            "edgerush_upstream_requests_total{upstream=\"web\"}"
        ),
        4
    );
    assert_eq!(sample(&scrape, "edgerush_config_reloads_total"), 1);
    assert!(sample(&scrape, "edgerush_config_last_reload_timestamp_seconds") > 1_700_000_000);
}

#[tokio::test(flavor = "multi_thread")]
async fn the_scrape_endpoint_serves_what_was_counted_and_nothing_else() {
    let up = upstream("up").await;
    let (proxy, addresses) = reloadable_proxy(&everything_to(&[("web", up)], "0")).await;
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let scrape = socket.local_addr().unwrap();
    tokio::spawn(Arc::clone(&proxy).serve_metrics(socket));
    assert_eq!(send(get(addresses["web"], "/")).await.0, 200);

    let (status, headers, body) = send(get(scrape, "/metrics")).await;
    assert_eq!(status, 200);
    assert_eq!(
        headers["content-type"],
        "text/plain; version=0.0.4; charset=utf-8"
    );
    let web_2xx = "edgerush_listener_responses_total{listener=\"web\",class=\"2xx\"}";
    assert_eq!(sample(&body, web_2xx), 1);

    // A query is the scraper's business; any other path or method is not served.
    assert_eq!(send(get(scrape, "/metrics?name=x")).await.0, 200);
    let head = request(Method::HEAD, scrape, "/metrics");
    let (status, _, body) = send(head.body(Empty::new().boxed()).unwrap()).await;
    assert_eq!((status, body.as_str()), (StatusCode::OK, ""));
    assert_eq!(send(get(scrape, "/")).await.0, 404);
    assert_eq!(send(get(scrape, "/metrics/")).await.0, 404);
    let post = request(Method::POST, scrape, "/metrics");
    let (status, headers, _) = send(post.body(Empty::new().boxed()).unwrap()).await;
    assert_eq!(status, 405);
    assert_eq!(headers["allow"], "GET, HEAD");

    // Scrapes are not traffic: no listener counts them.
    assert_eq!(sample(&proxy.metrics(), web_2xx), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_connection_accepted_on_one_thread_is_served_on_another() {
    let up = upstream("up").await;
    let config = compiled(&everything_to(&[("web", up)], "0"));
    let proxy = Arc::new(Proxy::new(config, NonZeroUsize::MIN, upstream_under_test()).unwrap());
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();

    // A worker of its own: a thread with a runtime that is given connections, and serves
    // each to its end.
    let (hand_over, mut handed) = mpsc::channel::<std::net::TcpStream>(4);
    let (served, was_served) = std::sync::mpsc::channel();
    let shared = Arc::clone(&proxy);
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        runtime.block_on(local.run_until(async move {
            let worker = Worker::new(shared);
            while let Some(stream) = handed.recv().await {
                let stream = TcpStream::from_std(stream).unwrap();
                Rc::clone(&worker).serve_connection(0, stream).await;
                served.send(std::thread::current().id()).unwrap();
            }
        }));
    });

    // Accepted here, on the test's runtime, and handed over as a socket of the standard
    // library's, which belongs to no runtime.
    tokio::spawn(async move {
        loop {
            let (stream, _) = socket.accept().await.unwrap();
            hand_over.send(stream.into_std().unwrap()).await.unwrap();
        }
    });

    let (status, headers, _) = send(get(address, "/")).await;
    assert_eq!(status, 200);
    assert_eq!(headers["x-upstream"], "up");
    // The client is gone, so the connection came to its end, where it was served.
    let served_by = was_served.recv_timeout(Duration::from_secs(10)).unwrap();
    assert_ne!(served_by, std::thread::current().id());
    let accepted = "edgerush_listener_connections_accepted_total{listener=\"web\"}";
    assert_eq!(sample(&proxy.metrics(), accepted), 1);
}

#[test]
fn a_failure_to_accept_is_counted_and_only_some_are_waited_after() {
    let config = compiled(&everything_to(
        &[("web", "127.0.0.1:1".parse().unwrap())],
        "0",
    ));
    let proxy = Proxy::new(config, NonZeroUsize::MIN, upstream_under_test()).unwrap();
    let gone = std::io::Error::from(std::io::ErrorKind::ConnectionAborted);
    assert_eq!(proxy.accept_failed(0, &gone), None);
    // Out of file descriptors: accepting again at once would fail again at once.
    let exhausted = std::io::Error::other("too many open files");
    assert!(proxy.accept_failed(0, &exhausted).is_some());
    let errors = "edgerush_listener_accept_errors_total{listener=\"web\"}";
    assert_eq!(sample(&proxy.metrics(), errors), 2);
}
