//! Instruction counts for what a passthrough listener does with a connection before it
//! knows where the connection goes: a TLS listener reads its ClientHello
//! ([17 §3](../../../docs/17-tcp-and-tls-passthrough.md)) — once a connection, when the whole
//! ClientHello has come; the reader reads everything again as more comes, so a ClientHello
//! that arrives in pieces costs about this for each — and either kind chooses its route.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench l4`, see the repository
//! README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_config::{Compiled, Config, compile};
use edgerush_proxy::l4::hello::{Hello, read};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;

/// BoringSSL's ClientHello with a post-quantum key share, asking for `api.example.com`,
/// in one record; the same cut into three records; and one asking for no name. The fuzz
/// target's seeds.
const NAMED: &[u8] = include_bytes!("../../../fuzz/seeds/tls_hello/named");
const ACROSS_RECORDS: &[u8] = include_bytes!("../../../fuzz/seeds/tls_hello/across_records");
const UNNAMED: &[u8] = include_bytes!("../../../fuzz/seeds/tls_hello/unnamed");

#[library_benchmark]
#[bench::one_record(NAMED)]
#[bench::three_records(ACROSS_RECORDS)]
#[bench::no_name(UNNAMED)]
fn read_hello(bytes: &[u8]) -> bool {
    black_box(matches!(read(black_box(bytes)), Hello::Whole(_)))
}

/// A tcp listener, and a tls one with an exact name, a wildcard and a catch-all among a
/// hundred other names.
fn passthrough() -> Compiled {
    let mut routes = String::new();
    for n in 0..100 {
        routes.push_str(&format!(
            "  - {{ name: host-{n}, listeners: [sni], hostnames: [{{ name: host-{n}.example.org, falls_through: true }}], backends: [{{ upstream: api, weight: 1 }}] }}
"
        ));
    }
    let yaml = format!(
        r#"
listeners:
  db: {{ address: "[::]:5432", protocol: tcp, proxy_protocol: off }}
  sni: {{ address: "[::]:443", protocol: tls, proxy_protocol: off }}
routes: []
tcp_routes:
  - {{ name: db, listeners: [db], backends: [{{ upstream: postgres, weight: 1 }}] }}
tls_routes:
{routes}  - {{ name: api, listeners: [sni], hostnames: [{{ name: api.example.com, falls_through: true }}], backends: [{{ upstream: api, weight: 1 }}] }}
  - {{ name: rest, listeners: [sni], hostnames: [{{ name: "*.example.com", wildcard: any_labels, falls_through: true }}], backends: [{{ upstream: api, weight: 1 }}] }}
upstreams:
  api: {{ load_balancer: p2c, endpoints: ["10.0.0.2:443"] }}
  postgres: {{ load_balancer: p2c, endpoints: ["10.0.0.1:5432"] }}
"#
    );
    let config: Config = serde_saphyr::from_str(&yaml).expect("valid YAML");
    compile(&config).expect("valid config")
}

/// The config, the position of the listener called `listener`, and the name a ClientHello
/// asks for: found before anything is measured.
fn asked(listener: &str, name: Option<&'static str>) -> (Compiled, usize, Option<&'static str>) {
    let compiled = passthrough();
    let at = compiled
        .listeners()
        .iter()
        .position(|found| found.name == listener)
        .expect("a listener");
    (compiled, at, name)
}

// The config is handed back so that dropping it is not measured. No closure is written
// here: one handed to generic code would put this function's path in that code's name,
// which iai-callgrind stops counting in (10 §3 in the docs).
#[library_benchmark]
#[bench::tls_exact(asked("sni", Some("api.example.com")))]
#[bench::tls_wildcard(asked("sni", Some("www.example.com")))]
#[bench::tls_no_name(asked("sni", None))]
#[bench::tcp(asked("db", None))]
fn route((compiled, at, name): (Compiled, usize, Option<&'static str>)) -> (Compiled, bool) {
    let mut found = false;
    if let Some(listener) = compiled.listeners().get(black_box(at))
        && let Some(l4) = &listener.l4
    {
        found = l4.route(black_box(name)).is_some();
    }
    (compiled, found)
}

library_benchmark_group!(name = l4; benchmarks = read_hello, route);

main!(library_benchmark_groups = l4);
