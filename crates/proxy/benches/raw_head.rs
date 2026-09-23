//! Instruction counts for taking a request's head down and deciding on it, both ways (14 §6):
//! as a raw head, the way our own server reads it, and as the header map the engine's
//! server builds. The same bytes and the same requests as the `decide` benchmark; what is
//! measured is everything from the parser's reading of them to the decision.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench raw_head`, see the repository
//! README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use bytes::Bytes;
use edgerush_config::{Compiled, Config, compile};
use edgerush_proxy::decide;
use edgerush_proxy::fields::FieldLines;
use edgerush_proxy::head::Head;
use edgerush_proxy::raw::RawHead;
use edgerush_proxy::upstream::h1::H1Limits;
use edgerush_proxy::upstream::h1::codec::{Sending, write_head};
use http::header::{HeaderMap, HeaderName, HeaderValue};
use http::request::Parts;
use http::{Method, Request, Uri};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

const SHOP: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http }
routes:
  - name: shop
    listeners: [web]
    hostnames:
      - { name: shop.example.com, falls_through: true }
    rules:
      - matches:
          - path: { prefix: /cart }
        filters:
          - type: request_header_modifier
            set: [{ name: X-Gateway, value: edgerush }]
            remove: [x-debug]
        backends:
          - { upstream: cart, weight: 9 }
          - { upstream: cart-canary, weight: 1 }
      - matches:
          - path: { prefix: / }
        backends:
          - { upstream: pages, weight: 1 }
upstreams:
  cart: { endpoints: ["127.0.0.1:9002"] }
  cart-canary: { endpoints: ["127.0.0.1:9003"] }
  pages: { endpoints: ["127.0.0.1:9004"] }
"#;

fn shop() -> Compiled {
    let config: Config = serde_saphyr::from_str(SHOP).expect("valid YAML");
    compile(&config).expect("valid config")
}

/// A request with a browser's worth of headers.
fn sent(target: &str, host: Option<&str>) -> Bytes {
    sent_with(target, host, &[])
}

/// The same request as the `decide` benchmark's, as the bytes our server reads.
fn sent_with(target: &str, host: Option<&str>, more: &[(&'static str, &'static str)]) -> Bytes {
    let fields = [
        (
            "user-agent",
            "Mozilla/5.0 (X11; Linux x86_64; rv:140.0) Gecko/20100101 Firefox/140.0",
        ),
        ("accept", "application/json, text/plain, */*"),
        ("accept-language", "en-GB,en;q=0.5"),
        ("accept-encoding", "gzip, deflate, br, zstd"),
        ("origin", "https://app.example.com"),
        ("referer", "https://app.example.com/orders"),
        ("x-request-id", "0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f"),
        (
            "cookie",
            "session=8f14e45fceea167a5a36dedd4bea2543; theme=dark",
        ),
    ];
    let mut sent = format!("GET {target} HTTP/1.1\r\n").into_bytes();
    let fields = fields.into_iter().chain(more.iter().copied());
    for (name, value) in host.map(|host| ("host", host)).into_iter().chain(fields) {
        sent.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
    }
    sent.extend_from_slice(b"\r\n");
    Bytes::from(sent)
}

// Both are measured from the same bytes to the same decision: the parser's reading, then a
// header map built of it or its lines taken down, then the request core. The config and
// what was made are handed back so that dropping them is not measured.
#[library_benchmark]
#[benches::requests(
    args = [
        (shop(), sent("/pages/about?lang=en", Some("shop.example.com"))),
        (shop(), sent("/cart/items?page=3", Some("shop.example.com"))),
        (shop(), sent("/pages/./a/../about?lang=en", Some("shop.example.com"))),
        (shop(), sent("http://shop.example.com/pages/about?lang=en", None)),
        (shop(), sent_with(
            "/pages/about?lang=en",
            Some("shop.example.com"),
            &[("connection", "keep-alive"), ("keep-alive", "timeout=5"), ("te", "trailers")]
        )),
        (shop(), sent_with(
            "http://shop.example.com/pages.Pages/About",
            None,
            &[("te", "trailers"), ("content-type", "application/grpc")]
        )),
        (shop(), sent("/pages/about", Some("other.example.org"))),
    ]
)]
fn by_raw(snapshot: Compiled, head: Bytes) -> (Compiled, Option<RawHead>, bool) {
    let mut room = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut room);
    let parsed = request.parse(black_box(&head));
    let raw = match (parsed, request.method, request.path) {
        (Ok(httparse::Status::Complete(_)), Some(method), Some(target)) => {
            let method = Method::from_bytes(method.as_bytes()).ok();
            let uri = target.parse::<Uri>().ok();
            let lines = FieldLines::new(&head, request.headers).ok();
            match (method, uri, lines) {
                (Some(method), Some(uri), Some(lines)) => {
                    Some(RawHead::new(method, uri, head.clone(), lines))
                }
                _ => None,
            }
        }
        _ => None,
    };
    let mut raw = raw;
    let forwarded = match (snapshot.listeners.first(), raw.as_mut()) {
        (Some(listener), Some(head)) => {
            decide(black_box(&snapshot), listener, head, 0x9E37_79B9_7F4A_7C15).is_ok()
        }
        _ => false,
    };
    (snapshot, raw, forwarded)
}

#[library_benchmark]
#[benches::requests(
    args = [
        (shop(), sent("/pages/about?lang=en", Some("shop.example.com"))),
        (shop(), sent("/cart/items?page=3", Some("shop.example.com"))),
        (shop(), sent("/pages/./a/../about?lang=en", Some("shop.example.com"))),
        (shop(), sent("http://shop.example.com/pages/about?lang=en", None)),
        (shop(), sent_with(
            "/pages/about?lang=en",
            Some("shop.example.com"),
            &[("connection", "keep-alive"), ("keep-alive", "timeout=5"), ("te", "trailers")]
        )),
        (shop(), sent_with(
            "http://shop.example.com/pages.Pages/About",
            None,
            &[("te", "trailers"), ("content-type", "application/grpc")]
        )),
        (shop(), sent("/pages/about", Some("other.example.org"))),
    ]
)]
fn by_map(snapshot: Compiled, head: Bytes) -> (Compiled, Option<Parts>, bool) {
    let mut room = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut room);
    let parsed = request.parse(black_box(&head));
    let parts = match (parsed, request.method, request.path) {
        (Ok(httparse::Status::Complete(_)), Some(method), Some(target)) => {
            let method = Method::from_bytes(method.as_bytes()).ok();
            let uri = target.parse::<Uri>().ok();
            // As the downstream parser builds its map.
            let mut headers = HeaderMap::with_capacity(request.headers.len());
            for field in request.headers.iter() {
                if let (Ok(name), Ok(value)) = (
                    HeaderName::from_bytes(field.name.as_bytes()),
                    HeaderValue::from_bytes(field.value),
                ) {
                    headers.append(name, value);
                }
            }
            match (method, uri) {
                (Some(method), Some(uri)) => {
                    let (mut parts, ()) = Request::new(()).into_parts();
                    parts.method = method;
                    parts.uri = uri;
                    parts.headers = headers;
                    Some(parts)
                }
                _ => None,
            }
        }
        _ => None,
    };
    let mut parts = parts;
    let forwarded = match (snapshot.listeners.first(), parts.as_mut()) {
        (Some(listener), Some(head)) => {
            decide(black_box(&snapshot), listener, head, 0x9E37_79B9_7F4A_7C15).is_ok()
        }
        _ => false,
    };
    (snapshot, parts, forwarded)
}

/// A request decided on as a raw head, ready to be written upstream.
fn decided_raw(sent: &Bytes) -> RawHead {
    let mut room = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut room);
    request.parse(sent).expect("a request");
    let lines = FieldLines::new(sent, request.headers).expect("lines of the head");
    let uri: Uri = request.path.expect("a target").parse().expect("a target");
    let mut head = RawHead::new(Method::GET, uri, sent.clone(), lines);
    let snapshot = shop();
    let listener = snapshot.listeners.first().expect("a listener");
    decide(&snapshot, listener, &mut head, 0).expect("decided");
    head
}

/// The same, as a header map.
fn decided_map(sent: &Bytes) -> Parts {
    let mut room = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut room);
    request.parse(sent).expect("a request");
    let mut headers = HeaderMap::new();
    for field in request.headers.iter() {
        headers.append(
            HeaderName::from_bytes(field.name.as_bytes()).expect("a name"),
            HeaderValue::from_bytes(field.value).expect("a value"),
        );
    }
    let (mut parts, ()) = Request::new(()).into_parts();
    parts.uri = request.path.expect("a target").parse().expect("a target");
    parts.headers = headers;
    let snapshot = shop();
    let listener = snapshot.listeners.first().expect("a listener");
    decide(&snapshot, listener, &mut parts, 0).expect("decided");
    parts
}

// Writing a decided request's head for the upstream, both ways: the usual request, and one
// the rule changes. Into room made beforehand, as the exchange makes it.
#[library_benchmark]
#[bench::usual_form(decided_raw(&sent("/pages/about?lang=en", Some("shop.example.com"))))]
#[bench::with_header_changes(decided_raw(&sent("/cart/items?page=3", Some("shop.example.com"))))]
fn write_raw(head: RawHead) -> (RawHead, Vec<u8>) {
    let mut out = Vec::with_capacity(2048);
    let written = write_head(
        &mut out,
        head.method(),
        head.uri(),
        black_box(&head),
        Sending::None,
        &H1Limits::default(),
    );
    assert!(written.is_ok(), "written");
    (head, out)
}

#[library_benchmark]
#[bench::usual_form(decided_map(&sent("/pages/about?lang=en", Some("shop.example.com"))))]
#[bench::with_header_changes(decided_map(&sent("/cart/items?page=3", Some("shop.example.com"))))]
fn write_map(head: Parts) -> (Parts, Vec<u8>) {
    let mut out = Vec::with_capacity(2048);
    let written = write_head(
        &mut out,
        &head.method,
        &head.uri,
        black_box(&head.headers),
        Sending::None,
        &H1Limits::default(),
    );
    assert!(written.is_ok(), "written");
    (head, out)
}

library_benchmark_group!(name = raw; benchmarks = by_raw, by_map, write_raw, write_map);
main!(library_benchmark_groups = raw);
