//! Fuzzes `edgerush test`'s file of tests: its reading, the checks on it, and the running of
//! what passes them ([22 §6](../../../docs/22-explain-and-test.md)) — any bytes at all, as a
//! file's, against one fixed config.
//!
//! Whatever it is given, nothing may fail. A file that cannot be read, or is not valid, is
//! refused with at least one mistake, each told without failing; one that is valid has every
//! test run, each to an explanation of its own request, and a test that passed has nothing
//! that differed.
//!
//! `cargo fuzz run test_file corpus/test_file seeds/test_file`.

#![no_main]

use edgerush_explain::{Snapshot, runner, test_file};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

/// What the tests run against: hosts exact, wildcard and every one, one that does not fall
/// through; paths exact, regex and prefix; predicates on method, header and query; a
/// redirect, a rewrite, a mirror, a rule with nowhere to send; listeners that generate
/// request IDs and that pass them, over HTTP/1.1 and 2; a tcp listener, and a tls one with
/// routes by exact name, by wildcard, one that does not fall through and one with nowhere to
/// send.
const CONFIG: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: ["10.0.0.0/8"], trusted_only_headers: [Forwarded, X-Real-IP, "X-Forwarded-*"] }, request_id: generate }
  passing: { address: "[::]:8081", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
  sni: { address: "[::]:443", protocol: tls, proxy_protocol: off }
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
          - { type: request_mirror, upstream: shadow, fraction: { numerator: 1, denominator: 10 } }
        forward:
          backends:
            - { upstream: cart, weight: 9 }
            - { upstream: cart-canary, weight: 1 }
      - matches:
          - path: { exact: /closed }
        redirect: { status: 301, path: { replace_full: /open }, query: keep }
      - matches:
          - path: { prefix: /search }
            query: [{ name: q, value: { exact: "a b" } }]
        forward: { backends: [{ upstream: api, weight: 1 }] }
      - matches:
          - path: { regex: "/v[0-9]+/.*" }
            headers: [{ name: X-Env, value: { regex: "canary|beta" } }]
          - path: { prefix: /admin }
            method: POST
        filters:
          - { type: url_rewrite, host: api.internal, path: { replace_full: /api } }
        forward: { backends: [{ upstream: api, weight: 1 }] }
  - name: wild
    listeners: [web]
    hostnames: [{ name: "*.example.com", wildcard: any_labels, falls_through: false }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: fallback, weight: 1 }] }
  - name: passed
    listeners: [passing]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: fallback, weight: 1 }] }
  - name: everything-else
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /static } }]
        forward: { backends: [{ upstream: fallback, weight: 0 }] }
upstreams:
  api: { load_balancer: p2c, endpoints: [] }
  cart: { load_balancer: p2c, endpoints: [] }
  cart-canary: { load_balancer: p2c, endpoints: [] }
  fallback: { load_balancer: p2c, endpoints: [] }
  shadow: { load_balancer: p2c, endpoints: [] }
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: fallback, weight: 1 }] }
tls_routes:
  - { name: tls-rest, listeners: [sni], hostnames: [{ name: "*.example.com", wildcard: any_labels, falls_through: true }], backends: [{ upstream: api, weight: 1 }] }
  - { name: tls-api, listeners: [sni], hostnames: [{ name: api.example.com, falls_through: true }], backends: [{ upstream: api, weight: 3 }, { upstream: cart, weight: 1 }] }
  - { name: tls-kept, listeners: [sni], hostnames: [{ name: "*.example.org", wildcard: one_label, falls_through: false }], backends: [{ upstream: api, weight: 0 }] }
  - { name: tls-org, listeners: [sni], hostnames: [{ name: www.example.org, falls_through: true }], backends: [{ upstream: api, weight: 1 }] }
"#;

/// The config, compiled once.
fn snapshot() -> &'static Snapshot {
    static SNAPSHOT: OnceLock<Snapshot> = OnceLock::new();
    SNAPSHOT.get_or_init(|| {
        let config = serde_saphyr::from_str(CONFIG).expect("the config is YAML");
        Snapshot::new(config).expect("the config compiles")
    })
}

fuzz_target!(|data: &[u8]| {
    let file = match test_file::read(data) {
        Ok(file) => file,
        Err(mistake) => {
            let _told = mistake.to_string();
            return;
        }
    };
    match file.prepare(snapshot()) {
        Err(mistakes) => {
            assert!(!mistakes.is_empty());
            for mistake in &mistakes {
                let _told = mistake.to_string();
            }
        }
        Ok(tests) => {
            for test in &tests {
                let ran = runner::run(snapshot(), test).expect("a valid test runs");
                assert!(ran.explanation.starts_with(&format!("{} (", test.listener)));
                assert_eq!(ran.passed(), ran.differences.is_empty());
                for difference in &ran.differences {
                    assert!(difference.contains(": expected "));
                }
            }
        }
    }
});
