//! Instruction counts for the request core: everything between a request's head and the
//! choice of its upstream — host, path normalisation, routing, hop-by-hop headers, header
//! changes, a rewrite, backend — or of a redirect.
//!
//! The head is a map with what the core adds kept beside it, as our HTTP/2 and HTTP/3 servers
//! hand heads over.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-proxy --features fuzzing --bench decide`,
//! see the repository README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_config::{Compiled, Config, compile};
use edgerush_proxy::map_head::MapHead;
use edgerush_proxy::storage::{LIMIT, Storage};
use edgerush_proxy::upstream::h1::blocks::{Blocks, Sizes};
use edgerush_proxy::{Client, decide};
use http::request::Parts;
use http::{HeaderValue, Request};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::cell::RefCell;
use std::hint::black_box;
use std::rc::Rc;

/// The ID a listener that generates them gives the request, made beforehand: making one is
/// the filters crate's `request_id` benchmark.
const ID: HeaderValue = HeaderValue::from_static("0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f");

const SHOP: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, forwarding: { trusted_proxies: [], trusted_only_headers: [Forwarded, X-Real-IP, "X-Forwarded-*"] }, request_id: generate }
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
        forward:
          backends:
            - { upstream: cart, weight: 9 }
            - { upstream: cart-canary, weight: 1 }
      - matches:
          - path: { prefix: /api }
        filters:
          - { type: url_rewrite, host: api.internal, path: { replace_prefix: /v2 } }
        forward:
          backends:
            - { upstream: pages, weight: 1 }
      - matches:
          - path: { prefix: /old }
        redirect: { status: 301, path: { replace_prefix: /new }, query: keep }
      - matches:
          - path: { prefix: / }
        forward:
          backends:
            - { upstream: pages, weight: 1 }
upstreams:
  cart: { endpoints: ["127.0.0.1:9002"] }
  cart-canary: { endpoints: ["127.0.0.1:9003"] }
  pages: { endpoints: ["127.0.0.1:9004"] }
"#;

/// A client that is no trusted proxy, as most are.
fn peer() -> Client {
    Client::new("203.0.113.7".parse().expect("an address"))
}

fn shop() -> Compiled {
    let config: Config = serde_saphyr::from_str(SHOP).expect("valid YAML");
    compile(&config).expect("valid config")
}

/// A request with a browser's worth of headers.
/// `parts` as our HTTP/2 and HTTP/3 servers hand a head over: what the core adds kept beside
/// the map, in room lent by the worker's blocks, which have some to lend.
fn mapped(parts: Parts) -> MapHead {
    let blocks = Rc::new(RefCell::new(Blocks::new(
        Sizes::default(),
        Storage::new(LIMIT),
    )));
    blocks.borrow_mut().give_edits(Vec::with_capacity(8));
    MapHead::lent(parts, &blocks)
}

fn head(target: &str, host: Option<&str>) -> Parts {
    head_with(target, host, &[])
}

/// The same over HTTP/2: the host in the target, as `:authority`, and no `Host` field. (An
/// HTTP/1.1 request without one is refused, whatever its target says: RFC 9112 §3.2.)
fn h2_head(target: &str, more: &[(&'static str, &'static str)]) -> Parts {
    let mut head = head_with(target, None, more);
    head.version = http::Version::HTTP_2;
    head
}

fn head_with(target: &str, host: Option<&str>, more: &[(&'static str, &'static str)]) -> Parts {
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
    let mut request = Request::builder().uri(target);
    let fields = fields.into_iter().chain(more.iter().copied());
    for (name, value) in host.map(|host| ("host", host)).into_iter().chain(fields) {
        request = request.header(name, value);
    }
    request.body(()).expect("valid request").into_parts().0
}

// The config and the head are handed back so that dropping them is not measured.
#[library_benchmark]
#[bench::usual_form(
    shop(),
    peer(),
    mapped(head("/pages/about?lang=en", Some("shop.example.com")))
)]
#[bench::with_header_changes(
    shop(),
    peer(),
    mapped(head("/cart/items?page=3", Some("shop.example.com")))
)]
#[bench::path_to_normalise(
    shop(),
    peer(),
    mapped(head("/pages/./a/../about?lang=en", Some("shop.example.com")))
)]
#[bench::host_in_the_target(shop(), peer(), mapped(h2_head("http://shop.example.com/pages/about?lang=en", &[])))]
#[bench::connection_header(
    shop(),
    peer(),
    mapped(head_with(
        "/pages/about?lang=en",
        Some("shop.example.com"),
        &[("connection", "keep-alive"), ("keep-alive", "timeout=5"), ("te", "trailers")]
    ))
)]
#[bench::grpc_says_te_trailers(
    shop(),
    peer(),
    mapped(h2_head(
        "http://shop.example.com/pages.Pages/About",
        &[("te", "trailers"), ("content-type", "application/grpc")]
    ))
)]
#[bench::no_route(
    shop(),
    peer(),
    mapped(head("/pages/about", Some("other.example.org")))
)]
#[bench::rewritten(
    shop(),
    peer(),
    mapped(head("/api/orders/42?expand=items", Some("shop.example.com")))
)]
#[bench::redirected(
    shop(),
    peer(),
    mapped(head("/old/orders/42?expand=items", Some("shop.example.com")))
)]
fn request_core(
    snapshot: Compiled,
    client: Client,
    mut head: MapHead,
) -> (Compiled, Client, MapHead, bool) {
    let forwarded = match snapshot.listeners.first() {
        Some(listener) => decide(
            black_box(&snapshot),
            listener,
            black_box(&mut head),
            black_box(&client),
            &mut || 0x9E37_79B9_7F4A_7C15,
            Some(&ID),
        )
        .is_ok(),
        None => false,
    };
    (snapshot, client, head, forwarded)
}

library_benchmark_group!(name = core; benchmarks = request_core);
main!(library_benchmark_groups = core);
