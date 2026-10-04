//! `edgerush explain`: where a request to a listener goes and why every other match did not
//! take it, as the data plane would decide it, without running one
//! ([22 §4](../../../docs/22-explain-and-test.md)).
//!
//! The request is decided by the core the proxy runs, with its routing passed in: the
//! listener's router decides, and the router crate's walk over every match runs beside it on
//! the very request routed. The text is made from what that gives and from the config as
//! written; everything but reading the config is pure.

use crate::asked::{self, Asked, Invalid};
use crate::config_file::{self, Rejected};
use edgerush_config::{
    Compiled, CompiledListener, Config, Filter, HeaderChanges, MatchId, Matches, PathChange,
    Protocol, RequestId, Rule,
};
use edgerush_proxy::{Client, Decision, Rejection, decide_routed};
use edgerush_router::{Explanation, Failure, Key, Seen, Verdict, Wanted, explain};
use http::header::{HeaderMap, HeaderValue};
use http::{Method, Version};
use std::io::Write;
use std::path::PathBuf;

pub(crate) const USAGE: &str = "\
Usage: edgerush explain --config <FILE> --listener <NAME> [REQUEST]

Says where a request to a listener goes and why every other match did not take it, as the
data plane would decide it, without running one. The config is read as the harness reads
it; the files of its certificates are not.

Request, every part of it required for an http or https listener:
      --client <ADDRESS>         The IP address it came from
      --protocol <VERSION>       The HTTP version it came by: 1.0, 1.1, 2 or 3
      --method <METHOD>          Its method, as sent
      --url <URL>                Its scheme (the listener's), host and port, path and query
      --header <'NAME: VALUE'>   A field line, in order: once for each, or not at all
      --connect-protocol <NAME>  An extended CONNECT's protocol, over HTTP/2 or HTTP/3

Options:
      --config <FILE>            The config, in YAML
      --listener <NAME>          The listener the request comes to
  -h, --help                     Print help
";

/// The ID a listener that generates them gives the request explained: a UUIDv7 of time 0,
/// in place of a random one ([22 §3](../../../docs/22-explain-and-test.md)).
pub(crate) const STAND_IN_ID: HeaderValue =
    HeaderValue::from_static("00000000-0000-7000-8000-000000000000");

/// The core's random draw: always the first of what it draws among. What a rule draws
/// among is shown whole.
fn draw() -> u64 {
    0
}

/// Runs `edgerush explain` with the arguments after its name. Returns the exit status: 0
/// for a request explained, whatever became of it; 2 for one that could not be.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match parse(args) {
        Ok(Parsed::Help) => write!(stdout, "{USAGE}").map(|()| 0),
        Ok(Parsed::Explain(options)) => match run(&options) {
            Ok(text) => write!(stdout, "{text}").map(|()| 0),
            Err(failure) => writeln!(stderr, "error: {failure}").map(|()| crate::EXIT_USAGE),
        },
        Err(error) => write!(stderr, "error: {error}\n\n{USAGE}").map(|()| crate::EXIT_USAGE),
    };
    written.unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Explain(Options),
}

/// The command line, as given.
#[derive(Debug, Default, PartialEq, Eq)]
struct Options {
    config: PathBuf,
    listener: String,
    client: Option<String>,
    protocol: Option<String>,
    method: Option<String>,
    url: Option<String>,
    headers: Vec<String>,
    connect_protocol: Option<String>,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'{0}' is required")]
    Required(&'static str),
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut config = None;
    let mut listener = None;
    let mut options = Options::default();
    while let Some(arg) = args.next() {
        let flag: &'static str = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--config" => "--config",
            "--listener" => "--listener",
            "--client" => "--client",
            "--protocol" => "--protocol",
            "--method" => "--method",
            "--url" => "--url",
            "--header" => "--header",
            "--connect-protocol" => "--connect-protocol",
            _ => return Err(UsageError::Unexpected(arg)),
        };
        let value = args.next().ok_or(UsageError::NoValue(flag))?;
        let once = match flag {
            "--config" => &mut config,
            "--listener" => &mut listener,
            "--client" => &mut options.client,
            "--protocol" => &mut options.protocol,
            "--method" => &mut options.method,
            "--url" => &mut options.url,
            "--connect-protocol" => &mut options.connect_protocol,
            _ => {
                options.headers.push(value);
                continue;
            }
        };
        if once.replace(value).is_some() {
            return Err(UsageError::Twice(flag));
        }
    }
    options.config = config
        .map(PathBuf::from)
        .ok_or(UsageError::Required("--config <FILE>"))?;
    options.listener = listener.ok_or(UsageError::Required("--listener <NAME>"))?;
    Ok(Parsed::Explain(options))
}

/// Why a request could not be explained.
#[derive(Debug, thiserror::Error)]
enum Unexplained {
    #[error("config {} cannot be read:\n{rejected}", path.display())]
    Config { path: PathBuf, rejected: Rejected },
    #[error("there is no listener {0}")]
    NoListener(String),
    #[error("listener {0} is a {1} listener: explain takes http and https listeners for now")]
    NotHttp(String, &'static str),
    #[error("'{0}' is required for an {1} listener")]
    Required(&'static str, &'static str),
    #[error(transparent)]
    Invalid(#[from] Invalid),
    #[error("explain and the router chose differently, which is a bug; please report it")]
    Disagreed,
}

fn run(options: &Options) -> Result<String, Unexplained> {
    let (config, compiled, matches) =
        config_file::offline(&options.config).map_err(|rejected| Unexplained::Config {
            path: options.config.clone(),
            rejected,
        })?;
    let listener = compiled
        .listeners()
        .iter()
        .find(|listener| listener.name == options.listener)
        .ok_or_else(|| Unexplained::NoListener(options.listener.clone()))?;
    let kind = protocol_name(listener.protocol);
    if matches!(listener.protocol, Protocol::Tcp | Protocol::Tls) {
        return Err(Unexplained::NotHttp(listener.name.clone(), kind));
    }
    let required =
        |given: &Option<String>, flag| given.clone().ok_or(Unexplained::Required(flag, kind));
    let (scheme, authority, target) = asked::url(&required(&options.url, "--url")?)?;
    let client = required(&options.client, "--client")?;
    let asked = Asked {
        client: client.parse().map_err(|_| Invalid::Client(client))?,
        protocol: asked::protocol(&required(&options.protocol, "--protocol")?)?,
        method: asked::method(&required(&options.method, "--method")?)?,
        scheme,
        authority,
        target,
        headers: options
            .headers
            .iter()
            .map(|line| asked::header_line(line))
            .collect::<Result<_, _>>()?,
        connect_protocol: options.connect_protocol.clone(),
    };
    explained(&config, &compiled, &matches, listener, &asked)
}

/// What the core made of the request, and the walk beside its routing.
struct Outcome<'c> {
    /// `None` if the request was refused before it was routed.
    walk: Option<Walk<'c>>,
    decided: Result<Decision<'c>, Rejection>,
    /// The fields as the request came with them, and as the upstream would be sent them.
    before: HeaderMap,
    after: http::request::Parts,
}

struct Walk<'c> {
    explanation: Explanation<'c, MatchId>,
    /// The path routed on, normalised.
    path: String,
    method: Method,
}

/// Decides the request and gives the text that explains it.
fn explained(
    config: &Config,
    compiled: &Compiled,
    matches: &Matches,
    listener: &CompiledListener,
    asked: &Asked,
) -> Result<String, Unexplained> {
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
    let outcome = Outcome {
        walk,
        decided,
        before,
        after: head,
    };
    Ok(text(config, compiled, listener, asked, &outcome))
}

/// The explanation, as text.
fn text(
    config: &Config,
    compiled: &Compiled,
    listener: &CompiledListener,
    asked: &Asked,
    outcome: &Outcome<'_>,
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
                let why = verdict(&considered.verdict, walk, chosen.map(&label));
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
fn field(lines: &mut Vec<String>, name: &str, said: Vec<String>) {
    for (at, line) in said.into_iter().enumerate() {
        let name = if at == 0 { name } else { "" };
        lines.push(format!("{name:<10}{line}").trim_end().to_owned());
    }
}

/// Why a match did or did not take the request.
fn verdict(verdict: &Verdict<'_>, walk: &Walk<'_>, chosen: Option<String>) -> String {
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
            format!("{by} claims this host, and this match's hostname does not fall through")
        }
        Verdict::Failed(failure) => match failure {
            Failure::Path(pattern) => {
                let path = &walk.path;
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
            Failure::Method(wanted) => format!("method {}, wanted {wanted}", walk.method),
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

fn protocol_name(protocol: Protocol) -> &'static str {
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
    use std::fs;

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

    /// A file of the test's own in the system's temporary directory, gone with the test.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(test: &str, content: &str) -> Self {
            let name = format!("edgerush-{}-explain-{test}.yaml", std::process::id());
            let path = std::env::temp_dir().join(name);
            fs::write(&path, content).unwrap();
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _gone_already = fs::remove_file(&self.0);
        }
    }

    /// Runs the command on `config`, saved under the test's name, with `args` after
    /// `--config`; its exit status, stdout and stderr.
    fn explain_on(config: &str, test: &str, args: &[&str]) -> (u8, String, String) {
        let file = Scratch::new(test, config);
        let path = file.0.to_str().unwrap().to_owned();
        let all = ["--config", path.as_str()]
            .into_iter()
            .chain(args.iter().copied());
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(all.map(str::to_owned), &mut stdout, &mut stderr);
        (
            status,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    /// The flags of a request from 203.0.113.7 to `listener`.
    fn request<'a>(
        listener: &'a str,
        protocol: &'a str,
        method: &'a str,
        url: &'a str,
    ) -> Vec<&'a str> {
        vec![
            "--listener",
            listener,
            "--client",
            "203.0.113.7",
            "--protocol",
            protocol,
            "--method",
            method,
            "--url",
            url,
        ]
    }

    /// Explains a GET over HTTP/1.1 to `web`, with these headers; the text.
    fn explained_get(test: &str, url: &str, headers: &[&str]) -> String {
        let mut args = request("web", "1.1", "GET", url);
        for header in headers {
            args.extend(["--header", header]);
        }
        let (status, stdout, stderr) = explain_on(CONFIG, test, &args);
        assert_eq!((status, stderr.as_str()), (0, ""), "{stdout}");
        stdout
    }

    #[test]
    fn every_match_for_the_host_says_why_it_did_not_take_a_request_nothing_takes() {
        let text = explained_get("no_route", "http://shop.example.com/search?q=a+c", &[]);
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
        let mut args = request("web", "2", "GET", "http://shop.example.com/cart?id=7");
        args.extend(["--header", "X-Debug: 1"]);
        let (status, text, _) = explain_on(CONFIG, "forwarded", &args);
        assert_eq!(status, 0);
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
        let text = explained_get("redirect", "http://shop.example.com/closed?x=1", &[]);
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
        let text = explained_get("refused", "http://shop.example.com/a/%2e%2e/b", &[]);
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
        let text = explained_get(
            "header",
            "http://shop.example.com/v2/orders",
            &["X-Env: prod"],
        );
        assert!(
            text.contains(
                "\n  shop rule 3 match 0             header x-env: \"prod\", wanted a match for \"canary|beta\"\n"
            ),
            "{text}"
        );
        let text = explained_get("absent", "http://shop.example.com/v2/orders", &[]);
        assert!(
            text.contains(" header x-env absent, wanted a match for "),
            "{text}"
        );
        let args = request("web", "1.1", "PUT", "http://shop.example.com/admin/x");
        let (_, text, _) = explain_on(CONFIG, "method", &args);
        assert!(
            text.contains("\n  shop rule 3 match 1             method PUT, wanted POST\n"),
            "{text}"
        );
        let text = explained_get("undecodable", "http://shop.example.com/search?q=%zz", &[]);
        assert!(
            text.contains(" query q: \"%zz\" cannot be decoded, wanted \"a b\"\n"),
            "{text}"
        );
    }

    #[test]
    fn a_more_specific_host_outranks_and_other_hosts_are_counted() {
        let text = explained_get("outranked", "http://a.example.com/static/x", &[]);
        assert!(
            text.contains(
                "\n→ wild rule 0 match 0             chosen\n  everything-else rule 0 match 0  outranked by wild rule 0 match 0 (host)\n  6 matches for other hosts\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn a_rewrite_shows_the_host_and_target_the_upstream_is_sent() {
        let text = explained_get(
            "rewrite",
            "http://shop.example.com/v2/x",
            &["X-Env: canary"],
        );
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
        let text = explained_get("websocket", "http://shop.example.com/chat", &opening);
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
        let text = explained_get("nowhere", "http://shop.example.com/static/x", &[]);
        assert!(
            text.ends_with(
                "\n→ everything-else rule 0 match 0  chosen\n\nrule      everything-else rule 0 match 0\nfilters   none\nanswer    500 no_backend\n"
            ),
            "{text}"
        );
    }

    #[test]
    fn the_stand_in_id_is_given_only_where_the_listener_generates_ids() {
        let generated = explained_get(
            "generated",
            "http://shop.example.com/chat",
            &["X-Request-ID: abc"],
        );
        assert!(
            generated.contains(
                "\n          - x-request-id: abc\n          + x-request-id: 00000000-0000-7000-8000-000000000000\n"
            ),
            "{generated}"
        );
        let mut args = request("passing", "1.1", "GET", "http://x.example.net/");
        args.extend(["--header", "X-Request-ID: abc"]);
        let (status, passed, _) = explain_on(CONFIG, "passed", &args);
        assert_eq!(status, 0);
        assert!(!passed.contains("x-request-id"), "{passed}");
        assert!(
            passed.contains("\n          + x-forwarded-for: 203.0.113.7\n"),
            "{passed}"
        );
    }

    #[test]
    fn certificate_files_are_never_opened() {
        let secure = "  secure: { address: \"[::]:8443\", protocol: https, proxy_protocol: off, tls: { certificates: [site] }, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }\n  db: {";
        let mut https = CONFIG.replace("  db: {", secure);
        https.push_str("certificates:\n  site: { chain_file: /nowhere/site.pem, key_file: /nowhere/site.key }\n");
        let args = request("secure", "1.1", "GET", "https://shop.example.com/");
        let (status, text, stderr) = explain_on(&https, "certificates", &args);
        assert_eq!((status, stderr.as_str()), (0, ""));
        assert!(
            text.starts_with("secure (https)  GET https://shop.example.com/"),
            "{text}"
        );
    }

    #[test]
    fn what_cannot_be_explained_exits_2_and_says_why() {
        let failed = |test: &str, config: &str, args: &[&str]| {
            let (status, stdout, stderr) = explain_on(config, test, args);
            assert_eq!(status, 2, "{test}: {stdout}");
            assert!(stdout.is_empty(), "{test}");
            stderr
        };
        let url = "http://shop.example.com/";
        assert_eq!(
            failed(
                "no_listener",
                CONFIG,
                &request("nowhere", "1.1", "GET", url)
            ),
            "error: there is no listener nowhere\n"
        );
        assert_eq!(
            failed("tcp", CONFIG, &request("db", "1.1", "GET", url)),
            "error: listener db is a tcp listener: explain takes http and https listeners for now\n"
        );
        let mut no_client = request("web", "1.1", "GET", url);
        no_client.drain(2..4);
        assert_eq!(
            failed("no_client", CONFIG, &no_client),
            "error: '--client' is required for an http listener\n"
        );
        assert_eq!(
            failed(
                "bad_url",
                CONFIG,
                &request("web", "1.1", "GET", "shop.example.com/")
            ),
            "error: 'shop.example.com/' is not a URL with a scheme, a host and a path\n"
        );
        let mut host = request("web", "1.1", "GET", url);
        host.extend(["--header", "Host: elsewhere"]);
        assert_eq!(
            failed("host", CONFIG, &host),
            "error: Host is the URL's to give: write it in the URL\n"
        );
        assert_eq!(
            failed("http3", CONFIG, &request("web", "3", "GET", url)),
            "error: listener web does not serve HTTP/3\n"
        );
        let invalid = failed(
            "invalid",
            "listeners: {}\n",
            &request("web", "1.1", "GET", url),
        );
        assert!(invalid.starts_with("error: config "), "{invalid}");
        assert!(invalid.contains("cannot be read"), "{invalid}");
    }

    #[test]
    fn a_config_file_that_is_not_there_exits_2() {
        let missing = std::env::temp_dir().join("edgerush-explain-no-such-file.yaml");
        let args = ["--config", missing.to_str().unwrap(), "--listener", "web"];
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(
            args.iter().map(ToString::to_string),
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(status, 2);
        assert!(stdout.is_empty());
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains("cannot be read")
        );
    }

    #[test]
    fn the_command_line_is_read_flag_by_flag() {
        let parsed = |args: &[&str]| parse(args.iter().map(ToString::to_string));
        assert_eq!(parsed(&["--listener", "web", "-h"]), Ok(Parsed::Help));
        assert_eq!(
            parsed(&["--listener", "web"]),
            Err(UsageError::Required("--config <FILE>"))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml"]),
            Err(UsageError::Required("--listener <NAME>"))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml", "--config", "b.yaml"]),
            Err(UsageError::Twice("--config"))
        );
        assert_eq!(parsed(&["--url"]), Err(UsageError::NoValue("--url")));
        assert_eq!(
            parsed(&["--cookie", "a=1"]),
            Err(UsageError::Unexpected("--cookie".to_owned()))
        );
        let headers = [
            "--config",
            "a.yaml",
            "--header",
            "A: 1",
            "--listener",
            "web",
            "--header",
            "B: 2",
            "--header",
            "A: 3",
        ];
        let Ok(Parsed::Explain(options)) = parsed(&headers) else {
            panic!("parsed");
        };
        assert_eq!(options.headers, ["A: 1", "B: 2", "A: 3"]);
        assert_eq!(options.listener, "web");
    }

    #[test]
    fn help_is_the_usage_on_stdout() {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(["--help".to_owned()].into_iter(), &mut stdout, &mut stderr);
        assert_eq!(
            (status, String::from_utf8(stdout).unwrap()),
            (0, USAGE.to_owned())
        );
        assert!(stderr.is_empty());
    }
}
