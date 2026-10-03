//! Configs the tests serve.

use super::*;

/// A config for `upstream` whose `web` listener is `https`, for `a.test`, with `http3`.
pub(super) fn h3_config(
    upstream: SocketAddr,
    http3: edgerush_config::Http3,
) -> edgerush_config::Config {
    let mut config = everything_config(upstream);
    secured(
        &mut config,
        vec![crate::tls::testing::certificate(&["a.test"])],
        None,
    );
    config.listeners.get_mut("web").unwrap().http3 = Some(http3);
    config
}

/// A `tcp` listener `db` whose one route goes to `backend`, with `extra` in the
/// listener's settings.
pub(super) fn tcp_to(backend: SocketAddr, extra: &str) -> String {
    format!(
        "listeners: {{ db: {{ address: \"127.0.0.1:0\", protocol: tcp, proxy_protocol: off{extra} }} }}\n\
             routes: []\n\
             tcp_routes: [{{ name: db, listeners: [db], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
             upstreams: {{ up: {{ load_balancer: p2c, endpoints: [\"{backend}\"] }} }}\n"
    )
}

/// `everything_config`, its `web` listener trusting `trusted` and taking off the control
/// plane's default trusted-only headers from anyone else.
pub(super) fn forwarding_config(upstream: SocketAddr, trusted: &[&str]) -> Config {
    let mut config = everything_config(upstream);
    config.listeners.get_mut("web").unwrap().forwarding = Some(edgerush_config::Forwarding {
        trusted_proxies: trusted.iter().map(|range| (*range).to_owned()).collect(),
        trusted_only_headers: ["Forwarded", "X-Real-IP", "X-Forwarded-*"]
            .map(str::to_owned)
            .to_vec(),
    });
    config
}

pub(super) fn trusting(
    server_name: &str,
    authority: &edgerush_config::Certificate,
) -> edgerush_config::UpstreamTls {
    edgerush_config::UpstreamTls {
        server_name: server_name.to_owned(),
        authorities: vec![authority.chain.clone()],
        client_certificate: None,
    }
}

/// A retry for `statuses` and `grpc` statuses, `attempts` times, waiting `backoff_ms`
/// at first and at most.
pub(super) fn retrying(
    attempts: u32,
    statuses: &[u16],
    grpc: &[&str],
    backoff_ms: u64,
) -> edgerush_config::Retry {
    edgerush_config::Retry {
        attempts,
        http_statuses: statuses.to_vec(),
        grpc_statuses: grpc.iter().map(|&name| name.to_owned()).collect(),
        on_timeout: false,
        backoff_base_ms: backoff_ms,
        backoff_max_ms: backoff_ms,
    }
}

pub(super) fn every_second_by(probe: edgerush_config::Probe) -> edgerush_config::HealthCheck {
    edgerush_config::HealthCheck {
        interval_seconds: 1,
        timeout_seconds: 1,
        healthy_threshold: 1,
        unhealthy_threshold: 1,
        probe,
    }
}

pub(super) fn healthz() -> edgerush_config::Probe {
    edgerush_config::Probe::Http {
        path: "/healthz".to_owned(),
    }
}

/// A config of one listener that sends everything to `upstream`.
pub(super) fn everything_to(upstream: SocketAddr) -> Compiled {
    compile(&everything_config(upstream)).unwrap()
}

/// The same, with `web` speaking TLS and presenting `certificates`.
pub(super) fn everything_secured_to(
    upstream: SocketAddr,
    certificates: Vec<edgerush_config::Certificate>,
) -> Compiled {
    let mut config = everything_config(upstream);
    secured(&mut config, certificates, None);
    compile(&config).unwrap()
}

/// `config`'s `web` listener made `https`, presenting `certificates`, each named for its
/// place (`web0`, `web1`, ...), and validating clients as `client_validation` says.
pub(super) fn secured(
    config: &mut Config,
    certificates: Vec<edgerush_config::Certificate>,
    client_validation: Option<edgerush_config::ClientValidation>,
) {
    let mut names = Vec::new();
    for (at, certificate) in certificates.into_iter().enumerate() {
        let name = format!("web{at}");
        config.certificates.insert(name.clone(), certificate);
        names.push(name);
    }
    let web = config.listeners.get_mut("web").unwrap();
    web.protocol = edgerush_config::Protocol::Https;
    web.tls = Some(edgerush_config::Tls {
        certificates: names,
        client_validation,
    });
}

pub(super) fn everything_config(upstream: SocketAddr) -> Config {
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        forward: {{ backends: [{{ upstream: up, weight: 1 }}] }}
upstreams:
  up: {{ load_balancer: p2c, endpoints: ["{upstream}"] }}
"#
    );
    serde_saphyr::from_str(&yaml).unwrap()
}

/// `yaml` with its upstream `up` sent a PROXY header of `version`.
pub(super) fn sending(yaml: &str, version: &str) -> String {
    yaml.replace(
        "up: { load_balancer: p2c,",
        &format!("up: {{ proxy_protocol: {version}, load_balancer: p2c,"),
    )
}
