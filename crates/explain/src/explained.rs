//! Where a request goes and why, worked out as the data plane would work it out, without
//! running one: what `edgerush explain` prints and `edgerush test` checks
//! ([22](../../../docs/22-explain-and-test.md)).
//!
//! The request is decided by the core the proxy runs, with its routing passed in: the
//! listener's router decides, and the router crate's walk over every match runs beside it on
//! the very request routed. The text is made from what that gives and from the config as
//! written.

use crate::asked::{Asked, Invalid};
use edgerush_config::{
    Compiled, CompiledListener, Config, ConfigError, Filter, HeaderChanges, MatchId, Matches,
    PathChange, Protocol, RequestId, Rule, compile_with_matches,
};
use edgerush_proxy::{Client, Decision, Rejection, decide_routed};
use edgerush_router::{Explanation, Failure, Key, Seen, Verdict, Wanted, explain};
use http::header::{HeaderMap, HeaderValue};
use http::{Method, Version};

/// A config as `explain` and `test` read it: compiled, with every listener's matches kept.
#[derive(Debug)]
pub struct Snapshot {
    pub(crate) config: Config,
    pub(crate) compiled: Compiled,
    pub(crate) matches: Matches,
}

/// Why a request could not be explained.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Unexplained {
    /// A listener the config does not have.
    #[error("there is no listener {0}")]
    NoListener(String),
    /// A request to a `tcp` or `tls` listener, which takes connections.
    #[error("listener {0} is a {1} listener: it takes connections, not requests")]
    NotHttp(String, &'static str),
    /// A connection to an `http` or `https` listener, which takes requests.
    #[error("listener {0} is an {1} listener: it takes requests, not bare connections")]
    NotPassthrough(String, &'static str),
    /// The request cannot be made into a head for the listener.
    #[error(transparent)]
    Invalid(#[from] Invalid),
    /// The walk named another winner than the router: a bug.
    #[error("explain and the router chose differently, which is a bug; please report it")]
    Disagreed,
}

impl Snapshot {
    /// Compiles `config` as the harness compiles it, keeping every listener's matches.
    ///
    /// # Errors
    ///
    /// Every problem the config has, as [`edgerush_config::compile`] gives them.
    pub fn new(config: Config) -> Result<Self, Vec<ConfigError>> {
        let (compiled, matches) = compile_with_matches(&config)?;
        Ok(Self {
            config,
            compiled,
            matches,
        })
    }

    /// The listener called `name`, if there is one.
    #[must_use]
    pub fn listener(&self, name: &str) -> Option<&CompiledListener> {
        self.compiled
            .listeners()
            .iter()
            .find(|listener| listener.name == name)
    }

    /// Decides `asked` as `listener` would, with the walk beside its routing.
    ///
    /// # Errors
    ///
    /// For a listener that is not `http` or `https`, a request that cannot be its head, or
    /// a walk that disagrees with the router.
    pub fn explain<'s>(
        &'s self,
        listener: &'s CompiledListener,
        asked: &Asked,
    ) -> Result<Explained<'s>, Unexplained> {
        if matches!(listener.protocol, Protocol::Tcp | Protocol::Tls) {
            return Err(Unexplained::NotHttp(
                listener.name.clone(),
                protocol_name(listener.protocol),
            ));
        }
        let Self {
            compiled, matches, ..
        } = self;
        let mut head = asked.head(listener)?;
        let before = head.headers.clone();
        let id = match listener.request_id {
            RequestId::Generate => Some(STAND_IN_ID),
            RequestId::Pass => None,
        };
        let mut walk = None;
        let mut routed = None;
        let decided = decide_routed(
            compiled,
            listener,
            &mut head,
            &Client::new(asked.client),
            &mut draw,
            id.as_ref(),
            |request| {
                routed = listener.router.route(request).copied();
                walk = Some(Walk {
                    explanation: explain(matches.of(&listener.name), request),
                    path: request.path.to_owned(),
                    method: request.method.clone(),
                });
                routed
            },
        );
        if let Some(walk) = &walk
            && walk.explanation.chosen().map(|chosen| chosen.value.rule) != routed
        {
            return Err(Unexplained::Disagreed);
        }
        Ok(Explained {
            snapshot: self,
            listener,
            asked: asked.clone(),
            walk,
            decided,
            before,
            after: head,
        })
    }
}

/// What the core made of a request, and the walk beside its routing.
#[derive(Debug)]
pub struct Explained<'s> {
    snapshot: &'s Snapshot,
    listener: &'s CompiledListener,
    asked: Asked,
    /// `None` if the request was refused before it was routed.
    walk: Option<Walk<'s>>,
    decided: Result<Decision<'s>, Rejection>,
    /// The fields as the request came with them, and as the upstream would be sent them.
    before: HeaderMap,
    after: http::request::Parts,
}

impl<'s> Explained<'s> {
    /// The explanation, as text.
    #[must_use]
    pub fn text(&self) -> String {
        let snapshot = self.snapshot;
        text(
            &snapshot.config,
            &snapshot.compiled,
            self.listener,
            &self.asked,
            self,
        )
    }

    /// The route the request was routed to, by name, and its rule as the config states it,
    /// with its position in the route; `None` for a request that was not routed.
    #[must_use]
    pub fn routed(&self) -> Option<(&'s str, &'s Rule, usize)> {
        let snapshot = self.snapshot;
        let id = self.walk.as_ref()?.explanation.chosen()?.value.rule;
        let route = snapshot.config.routes.get(id.route)?;
        Some((route.name.as_str(), route.rules.get(id.rule)?, id.rule))
    }

    /// What the core decided: where the request goes, or why the gateway answers it.
    pub fn decided(&self) -> Result<&Decision<'s>, Rejection> {
        self.decided.as_ref().map_err(|rejection| *rejection)
    }

    /// The head as the upstream would be sent it, for a request that goes to one.
    #[must_use]
    pub fn upstream(&self) -> &http::request::Parts {
        &self.after
    }
}

#[derive(Debug)]
struct Walk<'c> {
    explanation: Explanation<'c, MatchId>,
    /// The path routed on, normalised.
    path: String,
    method: Method,
}

/// The ID a listener that generates them gives the request explained: a UUIDv7 of time 0,
/// in place of a random one ([22 §3](../../../docs/22-explain-and-test.md)).
pub const STAND_IN_ID: HeaderValue =
    HeaderValue::from_static("00000000-0000-7000-8000-000000000000");

/// The core's random draw: always the first of what it draws among. What a rule draws
/// among is shown whole.
fn draw() -> u64 {
    0
}

/// The explanation, as text.
fn text(
    config: &Config,
    compiled: &Compiled,
    listener: &CompiledListener,
    asked: &Asked,
    outcome: &Explained<'_>,
) -> String {
    let mut lines = vec![
        format!(
            "{} ({})  {} {}  {}  from {}",
            listener.name,
            protocol_name(listener.protocol),
            asked.method,
            asked.url(),
            version_name(asked.protocol),
            asked.client
        ),
        String::new(),
    ];
    let label = |id: MatchId| {
        format!(
            "{} rule {} match {}",
            compiled.route_name(id.rule.route).unwrap_or("?"),
            id.rule.rule,
            id.position
        )
    };
    let chosen = outcome
        .walk
        .as_ref()
        .and_then(|walk| walk.explanation.chosen())
        .map(|chosen| chosen.value);
    match &outcome.walk {
        None => lines.push("  refused before it is routed".to_owned()),
        Some(walk) => {
            let considered = &walk.explanation.considered;
            let width = considered
                .iter()
                .map(|considered| label(considered.route_match.value).len())
                .max()
                .unwrap_or(0);
            for considered in considered {
                let marker = if matches!(considered.verdict, Verdict::Chosen) {
                    '→'
                } else {
                    ' '
                };
                let why = verdict(
                    &considered.verdict,
                    &walk.path,
                    &walk.method,
                    chosen.map(&label),
                    "match",
                );
                lines.push(format!(
                    "{marker} {:<width$}  {why}",
                    label(considered.route_match.value)
                ));
            }
            if considered.is_empty() {
                lines.push("  no match is for this host".to_owned());
            }
            match walk.explanation.other_hosts {
                0 => {}
                1 => lines.push("  1 match for other hosts".to_owned()),
                many => lines.push(format!("  {many} matches for other hosts")),
            }
        }
    }
    lines.push(String::new());

    let rule = chosen.and_then(|id| {
        let rule = config.routes.get(id.rule.route)?.rules.get(id.rule.rule)?;
        Some((id, rule))
    });
    if let Some((id, rule)) = rule {
        lines.push(format!("{:<10}{}", "rule", label(id)));
        field(&mut lines, "filters", filters(rule));
        if let Some(forward) = &rule.forward {
            if let Some(timeouts) = &forward.timeouts {
                let stated = [
                    ("request", timeouts.request_ms),
                    ("backend request", timeouts.backend_request_ms),
                    ("tunnel idle", timeouts.tunnel_idle_ms),
                ];
                let said: Vec<String> = stated
                    .iter()
                    .filter_map(|(what, ms)| Some(format!("{what} {}", millis((*ms)?))))
                    .collect();
                field(&mut lines, "timeouts", vec![said.join(", ")]);
            }
            if let Some(retry) = &forward.retry {
                let mut on: Vec<String> = retry.http_statuses.iter().map(u16::to_string).collect();
                on.extend(retry.grpc_statuses.iter().cloned());
                if retry.on_timeout {
                    on.push("timeout".to_owned());
                }
                let said = format!(
                    "{} more on {}, backoff {} to {} ms",
                    retry.attempts,
                    if on.is_empty() {
                        "nothing".to_owned()
                    } else {
                        on.join(", ")
                    },
                    retry.backoff_base_ms,
                    retry.backoff_max_ms
                );
                field(&mut lines, "retry", vec![said]);
            }
        }
    }
    match &outcome.decided {
        Ok(Decision::Forward(forward)) => {
            let target = outcome
                .after
                .uri
                .path_and_query()
                .map_or("/", http::uri::PathAndQuery::as_str);
            let mut upstream = vec![format!("{} {target}", outcome.after.method)];
            upstream.extend(changes(&outcome.before, &outcome.after.headers));
            field(&mut lines, "upstream", upstream);
            if let Some(opening) = &forward.websocket {
                let how = match opening {
                    edgerush_proxy::Opening::Upgrade(_) => "opening, by HTTP/1.1's upgrade",
                    edgerush_proxy::Opening::Connect => "opening, by an extended CONNECT",
                };
                field(&mut lines, "websocket", vec![how.to_owned()]);
            }
            if let Some((_, rule)) = rule {
                let backends = rule.forward.iter().flat_map(|forward| &forward.backends);
                let backends: Vec<String> = backends
                    .map(|backend| format!("{} (weight {})", backend.upstream, backend.weight))
                    .collect();
                field(&mut lines, "backends", vec![backends.join(", ")]);
                let mirrors: Vec<String> = rule
                    .filters
                    .iter()
                    .filter_map(|filter| match filter {
                        Filter::RequestMirror(mirror) => Some(format!(
                            "{} ({}/{})",
                            mirror.upstream, mirror.fraction.numerator, mirror.fraction.denominator
                        )),
                        _ => None,
                    })
                    .collect();
                let mirrors = if mirrors.is_empty() {
                    "none".to_owned()
                } else {
                    mirrors.join(", ")
                };
                field(&mut lines, "mirrors", vec![mirrors]);
            }
        }
        Ok(Decision::Redirect(redirected)) => {
            let location = String::from_utf8_lossy(redirected.location.as_bytes());
            let said = format!("{} to {location}", redirected.status.as_u16());
            field(&mut lines, "redirect", vec![said]);
        }
        Err(rejection) => {
            let said = format!("{} {}", rejection.status().as_u16(), rejection.reason());
            field(&mut lines, "answer", vec![said]);
        }
    }
    let mut text = lines.join("\n");
    text.push('\n');
    text
}

/// Adds a field of the rule's: its name, then its lines beside it.
pub(crate) fn field(lines: &mut Vec<String>, name: &str, said: Vec<String>) {
    for (at, line) in said.into_iter().enumerate() {
        let name = if at == 0 { name } else { "" };
        lines.push(format!("{name:<10}{line}").trim_end().to_owned());
    }
}

/// Why a match did or did not take the request.
/// Why something the walk ranked did or did not take what was asked, by the `path` and
/// `method` routed on; `what` it is (a match, a route), and the label of the one `chosen`.
pub(crate) fn verdict(
    verdict: &Verdict<'_>,
    path: &str,
    method: &Method,
    chosen: Option<String>,
    what: &str,
) -> String {
    match verdict {
        Verdict::Chosen => "chosen".to_owned(),
        Verdict::Outranked(key) => {
            let key = match key {
                Key::Host => "host",
                Key::Path => "path",
                Key::Method => "method",
                Key::Headers => "headers",
                Key::Query => "query",
                Key::Order => "order",
            };
            format!("outranked by {} ({key})", chosen.unwrap_or_default())
        }
        Verdict::Overshadowed { by } => {
            format!("{by} claims this host, and this {what}'s hostname does not fall through")
        }
        Verdict::Failed(failure) => match failure {
            Failure::Path(pattern) => {
                if pattern.is_regex() {
                    format!("path {path} does not match {}", pattern.as_str())
                } else if pattern.is_prefix() {
                    let prefix = if pattern.as_str().is_empty() {
                        "/"
                    } else {
                        pattern.as_str()
                    };
                    format!("path {path} is not under {prefix}")
                } else {
                    format!("path {path} is not {}", pattern.as_str())
                }
            }
            Failure::Method(wanted) => format!("method {method}, wanted {wanted}"),
            Failure::Header { predicate, seen } => {
                let name = predicate.name().as_str();
                format!(
                    "{}, wanted {}",
                    seen_as("header", name, seen),
                    wanted(predicate.wanted())
                )
            }
            Failure::Query { predicate, seen } => {
                let name = String::from_utf8_lossy(predicate.name());
                format!(
                    "{}, wanted {}",
                    seen_as("query", &name, seen),
                    wanted(predicate.wanted())
                )
            }
        },
    }
}

/// What a request has for a header or query parameter.
fn seen_as(kind: &str, name: &str, seen: &Seen) -> String {
    match seen {
        Seen::Absent => format!("{kind} {name} absent"),
        Seen::Value(value) => format!("{kind} {name}: {}", quoted(value)),
        Seen::Undecodable(raw) => format!("{kind} {name}: {} cannot be decoded", quoted(raw)),
    }
}

fn wanted(wanted: Wanted<'_>) -> String {
    match wanted {
        Wanted::Exact(value) => quoted(value),
        Wanted::Regex(pattern) => format!("a match for {}", quoted(pattern.as_bytes())),
    }
}

/// Bytes in quotes, anything but printable ASCII escaped.
fn quoted(bytes: &[u8]) -> String {
    format!("\"{}\"", bytes.escape_ascii())
}

/// Each header the core added, changed or took away: what it had taken away, then what it
/// has, in the order the upstream is sent them.
fn changes(before: &HeaderMap, after: &HeaderMap) -> Vec<String> {
    let line = |sign: char, name: &http::HeaderName, value: &HeaderValue| {
        format!("{sign} {name}: {}", value.as_bytes().escape_ascii())
    };
    let mut lines = Vec::new();
    for name in after.keys() {
        let had: Vec<&HeaderValue> = before.get_all(name).iter().collect();
        let has: Vec<&HeaderValue> = after.get_all(name).iter().collect();
        if had != has {
            lines.extend(had.into_iter().map(|value| line('-', name, value)));
            lines.extend(has.into_iter().map(|value| line('+', name, value)));
        }
    }
    for name in before.keys().filter(|name| !after.contains_key(*name)) {
        lines.extend(
            before
                .get_all(name)
                .iter()
                .map(|value| line('-', name, value)),
        );
    }
    lines
}

/// The rule's filters, a line each, in the order they are written.
fn filters(rule: &Rule) -> Vec<String> {
    if rule.filters.is_empty() {
        return vec!["none".to_owned()];
    }
    rule.filters
        .iter()
        .map(|filter| match filter {
            Filter::RequestHeaderModifier(changes) => {
                format!("request_header_modifier: {}", header_changes(changes))
            }
            Filter::ResponseHeaderModifier(changes) => {
                format!("response_header_modifier: {}", header_changes(changes))
            }
            Filter::RequestMirror(mirror) => format!(
                "request_mirror: {} ({}/{})",
                mirror.upstream, mirror.fraction.numerator, mirror.fraction.denominator
            ),
            Filter::UrlRewrite(rewrite) => {
                let mut said = Vec::new();
                if let Some(host) = &rewrite.host {
                    said.push(format!("host {host}"));
                }
                if let Some(path) = &rewrite.path {
                    said.push(path_change(path));
                }
                format!("url_rewrite: {}", said.join(", "))
            }
        })
        .collect()
}

fn header_changes(changes: &HeaderChanges) -> String {
    let pairs = |headers: &[edgerush_config::Header]| {
        headers
            .iter()
            .map(|header| format!("{}: {}", header.name, header.value))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let mut said = Vec::new();
    if !changes.set.is_empty() {
        said.push(format!("set {}", pairs(&changes.set)));
    }
    if !changes.add.is_empty() {
        said.push(format!("add {}", pairs(&changes.add)));
    }
    if !changes.remove.is_empty() {
        said.push(format!("remove {}", changes.remove.join(", ")));
    }
    said.join("; ")
}

fn path_change(change: &PathChange) -> String {
    match change {
        PathChange::ReplaceFull(path) => format!("path replace_full {path}"),
        PathChange::ReplacePrefix(path) => format!("path replace_prefix {path}"),
    }
}

fn millis(ms: u64) -> String {
    if ms == 0 {
        "no limit".to_owned()
    } else {
        format!("{ms} ms")
    }
}

/// What a listener of `protocol` is called in the config.
#[must_use]
pub fn protocol_name(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Http => "http",
        Protocol::Https => "https",
        Protocol::Tcp => "tcp",
        Protocol::Tls => "tls",
    }
}

fn version_name(version: Version) -> &'static str {
    match version {
        Version::HTTP_10 => "HTTP/1.0",
        Version::HTTP_11 => "HTTP/1.1",
        Version::HTTP_2 => "HTTP/2",
        _ => "HTTP/3",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::asked;

    /// Every kind of verdict and outcome: precedence by host, path and method; a header
    /// against a regex; a query decoded; a hostname that does not fall through; a redirect,
    /// a rewrite, a mirror, timeouts and a retry; a WebSocket; a rule with nowhere to send;
    /// a listener that passes request IDs on; and a `tcp` listener.
    const CONFIG: &str = r#"listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [Forwarded, X-Real-IP, "X-Forwarded-*"] }, request_id: generate }
  passing: { address: "[::]:8081", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
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
          timeouts: { request_ms: 5000, backend_request_ms: 1000 }
          retry: { attempts: 2, http_statuses: [502, 503], on_timeout: true, backoff_base_ms: 25, backoff_max_ms: 250 }
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
      - matches:
          - path: { prefix: /chat }
        forward: { backends: [{ upstream: chat, weight: 1 }] }
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
  chat: { load_balancer: p2c, endpoints: [] }
  fallback: { load_balancer: p2c, endpoints: [] }
  shadow: { load_balancer: p2c, endpoints: [] }
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: fallback, weight: 1 }] }
"#;

    fn snapshot() -> Snapshot {
        Snapshot::new(serde_saphyr::from_str(CONFIG).unwrap()).unwrap()
    }

    /// A request from 203.0.113.7.
    fn asked(protocol: &str, method: &str, url: &str, headers: &[&str]) -> Asked {
        let (scheme, authority, target) = asked::url(url).unwrap();
        Asked {
            client: "203.0.113.7".parse().unwrap(),
            protocol: asked::protocol(protocol).unwrap(),
            method: asked::method(method).unwrap(),
            scheme,
            authority,
            target,
            headers: headers
                .iter()
                .map(|line| asked::header_line(line).unwrap())
                .collect(),
            connect_protocol: None,
        }
    }

    /// Explains a request from 203.0.113.7 to `listener`; the text.
    fn explained(
        listener: &str,
        protocol: &str,
        method: &str,
        url: &str,
        headers: &[&str],
    ) -> String {
        let snapshot = snapshot();
        let listener = snapshot.listener(listener).unwrap();
        snapshot
            .explain(listener, &asked(protocol, method, url, headers))
            .unwrap()
            .text()
    }

    /// Explains a GET over HTTP/1.1 to `web`; the text.
    fn explained_get(url: &str, headers: &[&str]) -> String {
        explained("web", "1.1", "GET", url, headers)
    }

    #[test]
    fn every_match_for_the_host_says_why_it_did_not_take_a_request_nothing_takes() {
        let text = explained_get("http://shop.example.com/search?q=a+c", &[]);
        assert_eq!(
            text,
            "\
web (http)  GET http://shop.example.com/search?q=a+c  HTTP/1.1  from 203.0.113.7

  shop rule 1 match 0             path /search is not /closed
  shop rule 3 match 0             path /search does not match /v[0-9]+/.*
  shop rule 2 match 0             query q: \"a c\", wanted \"a b\"
  shop rule 3 match 1             path /search is not under /admin
  shop rule 0 match 0             path /search is not under /cart
  shop rule 4 match 0             path /search is not under /chat
  wild rule 0 match 0             shop.example.com claims this host, and this match's hostname does not fall through
  everything-else rule 0 match 0  path /search is not under /static

answer    404 no_route
"
        );
    }

    #[test]
    fn a_forwarded_request_shows_its_rule_and_what_goes_upstream() {
        let text = explained(
            "web",
            "2",
            "GET",
            "http://shop.example.com/cart?id=7",
            &["X-Debug: 1"],
        );
        assert_eq!(
            text,
            "\
web (http)  GET http://shop.example.com/cart?id=7  HTTP/2  from 203.0.113.7

  shop rule 1 match 0             path /cart is not /closed
  shop rule 3 match 0             path /cart does not match /v[0-9]+/.*
  shop rule 2 match 0             path /cart is not under /search
  shop rule 3 match 1             path /cart is not under /admin
→ shop rule 0 match 0             chosen
  shop rule 4 match 0             path /cart is not under /chat
  wild rule 0 match 0             shop.example.com claims this host, and this match's hostname does not fall through
  everything-else rule 0 match 0  path /cart is not under /static

rule      shop rule 0 match 0
filters   request_header_modifier: set X-Gateway: edgerush; remove x-debug
          request_mirror: shadow (1/10)
timeouts  request 5000 ms, backend request 1000 ms
retry     2 more on 502, 503, timeout, backoff 25 to 250 ms
upstream  GET /cart?id=7
          + via: 2 edgerush
          + host: shop.example.com
          + x-request-id: 00000000-0000-7000-8000-000000000000
          + x-forwarded-for: 203.0.113.7
          + x-forwarded-proto: http
          + x-forwarded-host: shop.example.com
          + x-gateway: edgerush
          - x-debug: 1
backends  cart (weight 9), cart-canary (weight 1)
mirrors   shadow (1/10)
"
        );
    }

    #[test]
    fn a_redirect_says_where_the_client_is_sent() {
        let text = explained_get("http://shop.example.com/closed?x=1", &[]);
        assert!(
            text.contains("\n→ shop rule 1 match 0             chosen\n"),
            "{text}"
        );
        assert!(
            text.ends_with(
                "\n\nrule      shop rule 1 match 0\nfilters   none\nredirect  301 to /open?x=1\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_request_refused_before_it_is_routed_says_the_answer() {
        let text = explained_get("http://shop.example.com/a/%2e%2e/b", &[]);
        assert_eq!(
            text,
            "\
web (http)  GET http://shop.example.com/a/%2e%2e/b  HTTP/1.1  from 203.0.113.7

  refused before it is routed

answer    400 bad_path
"
        );
    }

    #[test]
    fn a_failed_predicate_says_what_was_seen_and_what_was_wanted() {
        let text = explained_get("http://shop.example.com/v2/orders", &["X-Env: prod"]);
        assert!(
            text.contains(
                "\n  shop rule 3 match 0             header x-env: \"prod\", wanted a match for \"canary|beta\"\n"
            ),
            "{text}"
        );
        let text = explained_get("http://shop.example.com/v2/orders", &[]);
        assert!(
            text.contains(" header x-env absent, wanted a match for "),
            "{text}"
        );
        let text = explained("web", "1.1", "PUT", "http://shop.example.com/admin/x", &[]);
        assert!(
            text.contains("\n  shop rule 3 match 1             method PUT, wanted POST\n"),
            "{text}"
        );
        let text = explained_get("http://shop.example.com/search?q=%zz", &[]);
        assert!(
            text.contains(" query q: \"%zz\" cannot be decoded, wanted \"a b\"\n"),
            "{text}"
        );
    }

    #[test]
    fn a_more_specific_host_outranks_and_other_hosts_are_counted() {
        let text = explained_get("http://a.example.com/static/x", &[]);
        assert!(
            text.contains(
                "\n→ wild rule 0 match 0             chosen\n  everything-else rule 0 match 0  outranked by wild rule 0 match 0 (host)\n  6 matches for other hosts\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_rewrite_shows_the_host_and_target_the_upstream_is_sent() {
        let text = explained_get("http://shop.example.com/v2/x", &["X-Env: canary"]);
        assert!(
            text.contains(
                "\nfilters   url_rewrite: host api.internal, path replace_full /api\nupstream  GET /api\n          - host: shop.example.com\n          + host: api.internal\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_websocket_opening_is_said() {
        let opening = [
            "Connection: Upgrade",
            "Upgrade: websocket",
            "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==",
            "Sec-WebSocket-Version: 13",
        ];
        let text = explained_get("http://shop.example.com/chat", &opening);
        assert!(
            text.contains("\nwebsocket opening, by HTTP/1.1's upgrade\n"),
            "{text}"
        );
        assert!(
            text.contains("\n          - upgrade: websocket\n"),
            "{text}"
        );
    }

    #[test]
    fn a_rule_with_nowhere_to_send_is_answered_by_the_gateway() {
        let text = explained_get("http://shop.example.com/static/x", &[]);
        assert!(
            text.ends_with(
                "\n→ everything-else rule 0 match 0  chosen\n\nrule      everything-else rule 0 match 0\nfilters   none\nanswer    500 no_backend\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn the_stand_in_id_is_given_only_where_the_listener_generates_ids() {
        let generated = explained_get("http://shop.example.com/chat", &["X-Request-ID: abc"]);
        assert!(
            generated.contains(
                "\n          - x-request-id: abc\n          + x-request-id: 00000000-0000-7000-8000-000000000000\n"
            ),
            "{generated}"
        );
        let passed = explained(
            "passing",
            "1.1",
            "GET",
            "http://x.example.net/",
            &["X-Request-ID: abc"],
        );
        assert!(!passed.contains("x-request-id"), "{passed}");
        assert!(
            passed.contains("\n          + x-forwarded-for: 203.0.113.7\n"),
            "{passed}"
        );
    }

    #[test]
    fn a_listener_that_is_not_http_or_https_is_not_explained_yet() {
        let snapshot = snapshot();
        let db = snapshot.listener("db").unwrap();
        let asked = asked("1.1", "GET", "http://db.example.com/", &[]);
        assert_eq!(
            snapshot.explain(db, &asked).err(),
            Some(Unexplained::NotHttp("db".to_owned(), "tcp"))
        );
    }

    #[test]
    fn a_request_that_cannot_be_the_listeners_head_is_not_explained() {
        let snapshot = snapshot();
        let web = snapshot.listener("web").unwrap();
        let asked = asked("3", "GET", "http://shop.example.com/", &[]);
        assert_eq!(
            snapshot.explain(web, &asked).err(),
            Some(Unexplained::Invalid(Invalid::NoHttp3("web".to_owned())))
        );
        assert!(snapshot.listener("nowhere").is_none());
    }
}
