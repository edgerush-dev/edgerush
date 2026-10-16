//! Fuzzes the reading of the rate-limiting engine's limits file and the compiling of what it
//! reads ([07 §2](../../../docs/07-config-and-dsl.md)): any bytes at all, as the file's,
//! read as `edgerush/src/config_file.rs` reads it, with the same options.
//!
//! What was read is compiled under a config whose one forwarding rule names every limit the
//! file holds. Whatever it is given, reading must not fail, nor compiling, and a refusal
//! must be told without failing. A config that compiles puts the rule under each limit
//! once, the smallest bucket first; one that does not is refused with at least one problem.
//!
//! `cargo fuzz run limits corpus/limits seeds/limits`.

#![no_main]

use edgerush_config::{Config, Limits, compile};
use libfuzzer_sys::fuzz_target;

const CONFIG: &str = r#"
listeners: { web: { address: "127.0.0.1:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate } }
routes:
  - name: api
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules: [{ matches: [{ path: { prefix: / } }], forward: { backends: [{ upstream: u, weight: 1 }] } }]
upstreams: { u: { load_balancer: p2c, endpoints: [] } }
rate_limit_service: { pods: ["rl-0:7000"] }
"#;

fuzz_target!(|data: &[u8]| {
    let options = serde_saphyr::options! { with_snippet: false };
    let limits: Limits = match serde_saphyr::from_slice_with_options(data, options) {
        Ok(limits) => limits,
        Err(error) => {
            let _told = error.to_string();
            return;
        }
    };
    let mut config: Config = serde_saphyr::from_str(CONFIG).expect("the config is one");
    let names: Vec<String> = limits
        .limits
        .iter()
        .map(|limit| limit.name.clone())
        .collect();
    config.routes[0].rules[0].rate_limits = names;
    config.limits = Some(limits);
    match compile(&config) {
        Ok(compiled) => {
            let id = edgerush_config::RuleId { route: 0, rule: 0 };
            let rule = compiled.rule(id).expect("the rule is compiled");
            let limits = &rule.limits;
            for (at, limit) in limits.iter().enumerate() {
                assert!(
                    limits[..at].iter().all(|before| before.name != limit.name),
                    "a limit twice"
                );
                assert!(
                    limits[..at]
                        .iter()
                        .all(|before| before.burst <= limit.burst),
                    "out of order"
                );
            }
        }
        Err(problems) => {
            assert!(!problems.is_empty());
            for problem in &problems {
                let _told = problem.to_string();
            }
        }
    }
});
