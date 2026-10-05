//! The file of tests `edgerush test` runs ([22 §3](../../../docs/22-explain-and-test.md)):
//! its model, read from YAML, and the checks that make it valid before anything is run —
//! first on its own, then against the config, where each request is made what it will be
//! run as.

use crate::asked::{self, Asked, Invalid};
use crate::explained::Snapshot;
use edgerush_config::Protocol;
use edgerush_proxy::Decision;
use http::StatusCode;
use serde::Deserialize;
use serde_saphyr::Spanned;
use std::collections::HashSet;
use std::fmt::{self, Display, Formatter};

/// A file of tests, as read.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestFile {
    /// The tests, in order, each with where it is in the file.
    pub tests: Vec<Spanned<Test>>,
}

/// One test: a request, and what is expected of it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Test {
    /// Its name, unique in the file.
    pub name: String,
    /// The request.
    pub request: Request,
    /// The answer its upstream gives, for a forwarded request whose `response` is checked.
    #[serde(default)]
    pub upstream_answer: Option<UpstreamAnswer>,
    /// What is expected of it.
    pub expect: Expect,
}

/// An upstream's answer as a test states it: a final status, and its field lines in order,
/// `[]` for none.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamAnswer {
    /// Its status.
    pub status: u16,
    /// Its field lines.
    pub headers: Vec<Header>,
}

/// A request as a test states it (22 §2): every field that changes what the core does,
/// none filled in by default.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    /// The listener it comes to, by name.
    pub listener: String,
    /// The address it came from.
    #[serde(default)]
    pub client: Option<String>,
    /// The HTTP version it came by: `"1.0"`, `"1.1"`, `"2"` or `"3"`.
    #[serde(default)]
    pub protocol: Option<String>,
    /// Its method, as sent.
    #[serde(default)]
    pub method: Option<String>,
    /// Its scheme (the listener's), host and port, path and query.
    #[serde(default)]
    pub url: Option<String>,
    /// Its field lines, in order; `[]` for none.
    #[serde(default)]
    pub headers: Option<Vec<Header>>,
    /// An extended CONNECT's `:protocol`.
    #[serde(default)]
    pub connect_protocol: Option<String>,
    /// For a `tls` listener, the name the ClientHello asks for, or `none`.
    #[serde(default)]
    pub sni: Option<Sni>,
}

/// What a ClientHello asks for: `none`, or `{ name }`.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(untagged)]
pub enum Sni {
    /// `none`: it asks for no name.
    Word(NoName),
    /// `{ name }`: it asks for this one.
    Name(SniName),
}

/// The word `none`, and nothing else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NoName {
    /// No name.
    None,
}

/// A name a ClientHello asks for.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SniName {
    /// The name.
    pub name: String,
}

/// A field line of a request.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Header {
    /// Its name.
    pub name: String,
    /// Its value.
    pub value: String,
}

/// What a test expects: one outcome, and what goes with it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Expect {
    /// The route the request belongs to, by name; stated whenever it has one.
    #[serde(default)]
    pub route: Option<String>,
    /// The rule's position in the route, from 0, as the access log numbers it.
    #[serde(default)]
    pub rule: Option<usize>,
    /// It goes to the rule's backends.
    #[serde(default)]
    pub forward: Option<Forward>,
    /// It is answered with a redirect.
    #[serde(default)]
    pub redirect: Option<Redirect>,
    /// The gateway answers it itself, for the reason the metrics name.
    #[serde(default)]
    pub answer: Option<String>,
    /// A `tcp` or `tls` connection is refused, for the reason the metrics name.
    #[serde(default)]
    pub refused: Option<String>,
    /// What the upstream is sent, as far as it is stated.
    #[serde(default)]
    pub upstream_request: Option<UpstreamRequest>,
    /// The answer the client gets, as far as it is stated.
    #[serde(default)]
    pub response: Option<Response>,
}

/// The rule's backends and mirrors, each list whole: what is drawn at random is asserted
/// as the list it is drawn from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Forward {
    /// The backends, in the rule's order.
    pub backends: Vec<Backend>,
    /// The mirrors, in the rule's order; `[]` for none. Stated for a request, and never for
    /// a connection, which has no mirrors.
    #[serde(default)]
    pub mirrors: Option<Vec<Mirror>>,
}

/// A backend and its weight.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Backend {
    /// The upstream.
    pub upstream: String,
    /// Its weight.
    pub weight: u32,
}

/// A mirror and the share of requests it is sent.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mirror {
    /// The upstream.
    pub upstream: String,
    /// The share.
    pub fraction: Fraction,
}

/// `numerator` out of every `denominator`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Fraction {
    /// How many.
    pub numerator: u32,
    /// Out of how many.
    pub denominator: u32,
}

/// A redirect: its status and where it sends the client.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Redirect {
    /// The status.
    pub status: u16,
    /// The `Location`.
    pub location: String,
}

/// The request as the upstream is sent it: only what is stated is checked.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpstreamRequest {
    /// Its target, path and query.
    #[serde(default)]
    pub target: Option<String>,
    /// Headers, each as it must be.
    #[serde(default)]
    pub headers: Vec<HeaderExpected>,
}

/// The answer the client gets, as the gateway hands it to its server, whose framing fields
/// are the server's to write: only what is stated is checked.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    /// Its status.
    #[serde(default)]
    pub status: Option<u16>,
    /// Headers, each as it must be.
    #[serde(default)]
    pub headers: Vec<HeaderExpected>,
}

/// A header as it must be: one field line with this value, two or more in order, or none.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HeaderExpected {
    /// Its name.
    pub name: String,
    /// The value of its one field line.
    #[serde(default)]
    pub value: Option<String>,
    /// The values of its field lines, two or more, in order.
    #[serde(default)]
    pub values: Option<Vec<String>>,
    /// That it is not there: only ever `true`.
    #[serde(default)]
    pub absent: Option<bool>,
}

/// The reasons the request core answers a request itself for, as the metrics name them.
const ANSWERS: [&str; 8] = [
    "bad_host",
    "bad_path",
    "bad_connection",
    "bad_target",
    "no_route",
    "no_backend",
    "edits",
    "unknown_protocol",
];

/// The reasons a `tcp` or `tls` connection is refused for, as the metrics name them.
const REFUSALS: [&str; 2] = ["no_route", "no_backend"];

/// What is wrong with a file of tests, and where.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mistake {
    /// The line of the test it is in; 0 for the file as a whole.
    pub line: u64,
    /// What is wrong.
    pub problem: Problem,
}

impl Display for Mistake {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "line {}: {}", self.line, self.problem)
    }
}

/// What can be wrong with a file of tests.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Problem {
    /// Not YAML, or not a file of tests: the parser's word, without the lines around it.
    #[error("{0}")]
    Unreadable(String),
    /// No tests in it.
    #[error("the file has no tests")]
    NoTests,
    /// A name used before.
    #[error("test name '{0}' is used twice")]
    NameTwice(String),
    /// No outcome, or more than one.
    #[error("a test expects one of forward, redirect, answer and refused, and only one")]
    Outcome,
    /// `route` without `rule`, or the other way round, for a request.
    #[error("route and rule are stated together")]
    RouteAndRule,
    /// A forward or redirect without its route and rule.
    #[error("a forwarded or redirected request has a route and rule: state them")]
    NoRoute,
    /// A forwarded connection without its route.
    #[error("a forwarded connection has a route: state it")]
    NoConnectionRoute,
    /// A forwarded request without its mirrors.
    #[error("a forwarded request states its mirrors: [] for none")]
    NoMirrors,
    /// An answer the gateway never gives itself.
    #[error("'{0}' is not an answer the gateway gives: one of {list}", list = ANSWERS.join(", "))]
    Answer(String),
    /// A refusal the gateway never gives a connection.
    #[error("'{0}' is not a refusal the gateway gives: one of {list}", list = REFUSALS.join(", "))]
    Refused(String),
    /// `upstream_request` beside something other than `forward`.
    #[error("upstream_request is for a forwarded request")]
    UpstreamRequest,
    /// `upstream_answer` beside something other than `forward`.
    #[error("upstream_answer is for a forwarded request")]
    UpstreamAnswer,
    /// `upstream_answer` with no `response` to check.
    #[error("an upstream_answer is stated for the response it makes: state the response")]
    NoResponse,
    /// A forwarded request's `response` without the answer it is made from.
    #[error("a forwarded request's response is made from its upstream_answer: state it")]
    NoUpstreamAnswer,
    /// A number that is no status.
    #[error("{0} is not a status")]
    Status(u16),
    /// An upstream's status that ends no exchange.
    #[error("{0} is not the status of a final answer")]
    NotFinal(u16),
    /// An upstream answer to a WebSocket handshake.
    #[error(
        "a WebSocket handshake's answer cannot be told offline: whether it switches rests on \
         the gateway's own key and on its backend's connection"
    )]
    WebSocket,
    /// A header expected in no one way.
    #[error("header {0} is expected by one of value, values and absent: true")]
    HeaderForm(String),
    /// `values` with fewer than two.
    #[error("header {0} has values for two field lines or more: write one as value")]
    TooFewValues(String),
    /// `absent: false`.
    #[error("header {0} has absent: true or no absent at all")]
    AbsentFalse(String),
    /// A listener the config does not have.
    #[error("there is no listener {0}")]
    NoListener(String),
    /// A field the listener's kind needs, missing.
    #[error("request needs {0} for {1} listener")]
    Missing(&'static str, &'static str),
    /// A field that means nothing for the listener's kind.
    #[error("{0} does not apply to {1} listener")]
    Unwanted(&'static str, &'static str),
    /// A request that cannot be what it says.
    #[error(transparent)]
    Invalid(#[from] Invalid),
}

/// Reads a file of tests from its bytes.
///
/// # Errors
///
/// If it is not YAML, or not a file of tests.
pub fn read(yaml: &[u8]) -> Result<TestFile, Mistake> {
    // Without the lines around a mistake, as the config is read.
    let options = serde_saphyr::options! { with_snippet: false };
    serde_saphyr::from_slice_with_options(yaml, options).map_err(|error| Mistake {
        line: 0,
        problem: Problem::Unreadable(error.to_string()),
    })
}

/// A test made ready to run against one config: its request as it is to be sent.
#[derive(Debug, Clone)]
pub struct Prepared {
    /// Its line in the file.
    pub line: u64,
    /// Its name.
    pub name: String,
    /// The listener it goes to.
    pub listener: String,
    /// The request, or the connection.
    pub asked: Asking,
    /// The answer its upstream gives, if the test states one.
    pub upstream_answer: Option<UpstreamAnswer>,
    /// What is expected of it.
    pub expect: Expect,
}

/// What a test sends its listener.
#[derive(Debug, Clone)]
pub enum Asking {
    /// A request, to an `http` or `https` listener.
    Request(Asked),
    /// A connection, to a `tcp` or `tls` listener, with the name its ClientHello asks for:
    /// always `None` for `tcp`.
    Connection(Option<String>),
}

impl TestFile {
    /// Checks the file, on its own and against `snapshot`, and makes every test ready to
    /// run.
    ///
    /// # Errors
    ///
    /// Every mistake found, in the order of the file.
    pub fn prepare(&self, snapshot: &Snapshot) -> Result<Vec<Prepared>, Vec<Mistake>> {
        let mut mistakes = Vec::new();
        if self.tests.is_empty() {
            mistakes.push(Mistake {
                line: 0,
                problem: Problem::NoTests,
            });
        }
        let mut names = HashSet::new();
        let mut prepared = Vec::new();
        for test in &self.tests {
            let line = test.referenced.line();
            let mut wrong = |problem| mistakes.push(Mistake { line, problem });
            let test = &test.value;
            if !names.insert(test.name.as_str()) {
                wrong(Problem::NameTwice(test.name.clone()));
            }
            for problem in expectation(&test.expect) {
                wrong(problem);
            }
            for problem in answers(test) {
                wrong(problem);
            }
            if let Some(listener) = snapshot.listener(&test.request.listener) {
                for problem in for_listener(test, listener.protocol) {
                    wrong(problem);
                }
            }
            match asked(&test.request, snapshot) {
                Ok(asked) => {
                    if test.upstream_answer.is_some() && handshake(snapshot, &test.request, &asked)
                    {
                        wrong(Problem::WebSocket);
                    }
                    prepared.push(Prepared {
                        line,
                        name: test.name.clone(),
                        listener: test.request.listener.clone(),
                        asked,
                        upstream_answer: test.upstream_answer.clone(),
                        expect: test.expect.clone(),
                    });
                }
                Err(problem) => wrong(problem),
            }
        }
        if mistakes.is_empty() {
            Ok(prepared)
        } else {
            Err(mistakes)
        }
    }
}

/// What is wrong with an expectation on its own, whatever its listener.
fn expectation(expect: &Expect) -> Vec<Problem> {
    let mut problems = Vec::new();
    let outcomes = [
        expect.forward.is_some(),
        expect.redirect.is_some(),
        expect.answer.is_some(),
        expect.refused.is_some(),
    ];
    if outcomes.iter().filter(|stated| **stated).count() != 1 {
        problems.push(Problem::Outcome);
    }
    if let Some(answer) = &expect.answer
        && !ANSWERS.contains(&answer.as_str())
    {
        problems.push(Problem::Answer(answer.clone()));
    }
    if let Some(refused) = &expect.refused
        && !REFUSALS.contains(&refused.as_str())
    {
        problems.push(Problem::Refused(refused.clone()));
    }
    if let Some(upstream) = &expect.upstream_request {
        if expect.forward.is_none() {
            problems.push(Problem::UpstreamRequest);
        }
        problems.extend(header_forms(&upstream.headers));
    }
    problems
}

/// What is wrong with how each header is expected: by one of `value`, `values` (two or more)
/// and `absent: true`.
fn header_forms(headers: &[HeaderExpected]) -> Vec<Problem> {
    headers
        .iter()
        .filter_map(|header| {
            let name = header.name.clone();
            match (&header.value, &header.values, header.absent) {
                (Some(_), None, None) | (None, None, Some(true)) => None,
                (None, Some(values), None) if values.len() >= 2 => None,
                (None, Some(_), None) => Some(Problem::TooFewValues(name)),
                (None, None, Some(false)) => Some(Problem::AbsentFalse(name)),
                _ => Some(Problem::HeaderForm(name)),
            }
        })
        .collect()
}

/// What is wrong with a test's upstream answer and the response it is checked by: an
/// answer goes with a forward and a response to check; a forwarded request's response is
/// made from one.
fn answers(test: &Test) -> Vec<Problem> {
    let mut problems = Vec::new();
    let expect = &test.expect;
    if let Some(answer) = &test.upstream_answer {
        if expect.forward.is_none() {
            problems.push(Problem::UpstreamAnswer);
        } else if expect.response.is_none() {
            problems.push(Problem::NoResponse);
        }
        match StatusCode::from_u16(answer.status) {
            Err(_) => problems.push(Problem::Status(answer.status)),
            Ok(status) if status.is_informational() => {
                problems.push(Problem::NotFinal(answer.status));
            }
            Ok(_) => {}
        }
        problems.extend(
            answer
                .headers
                .iter()
                .filter_map(|header| asked::answer_header(&header.name, &header.value).err())
                .map(Problem::from),
        );
    }
    if let Some(response) = &expect.response {
        if expect.forward.is_some() && test.upstream_answer.is_none() {
            problems.push(Problem::NoUpstreamAnswer);
        }
        if let Some(status) = response.status
            && StatusCode::from_u16(status).is_err()
        {
            problems.push(Problem::Status(status));
        }
        problems.extend(header_forms(&response.headers));
    }
    problems
}

/// Whether a request is a WebSocket handshake the gateway would carry, as the core decides
/// it: one it forwards as one.
fn handshake(snapshot: &Snapshot, request: &Request, asked: &Asking) -> bool {
    let (Some(listener), Asking::Request(asked)) = (snapshot.listener(&request.listener), asked)
    else {
        return false;
    };
    snapshot.explain(listener, asked).is_ok_and(|explained| {
        matches!(
            explained.decided(),
            Ok(Decision::Forward(forward)) if forward.websocket.is_some()
        )
    })
}

/// A listener of `protocol`, as the problems name it.
fn kind(protocol: Protocol) -> &'static str {
    match protocol {
        Protocol::Http => "an http",
        Protocol::Https => "an https",
        Protocol::Tcp => "a tcp",
        Protocol::Tls => "a tls",
    }
}

/// What is wrong with a test for a listener of `protocol`: a request's has a rule beside
/// its route and states its mirrors; a connection's has a route alone, is forwarded or
/// refused, and has no answer of HTTP's.
fn for_listener(test: &Test, protocol: Protocol) -> Vec<Problem> {
    let expect = &test.expect;
    let mut problems = Vec::new();
    let kind = kind(protocol);
    match protocol {
        Protocol::Http | Protocol::Https => {
            let routed = expect.forward.is_some() || expect.redirect.is_some();
            if expect.route.is_some() != expect.rule.is_some() {
                problems.push(Problem::RouteAndRule);
            } else if routed && expect.route.is_none() {
                problems.push(Problem::NoRoute);
            }
            if expect
                .forward
                .as_ref()
                .is_some_and(|forward| forward.mirrors.is_none())
            {
                problems.push(Problem::NoMirrors);
            }
            if expect.refused.is_some() {
                problems.push(Problem::Unwanted("refused", kind));
            }
        }
        Protocol::Tcp | Protocol::Tls => {
            let unwanted = [
                ("rule", expect.rule.is_some()),
                ("redirect", expect.redirect.is_some()),
                ("answer", expect.answer.is_some()),
                ("upstream_request", expect.upstream_request.is_some()),
                ("upstream_answer", test.upstream_answer.is_some()),
                ("response", expect.response.is_some()),
                (
                    "mirrors",
                    expect
                        .forward
                        .as_ref()
                        .is_some_and(|forward| forward.mirrors.is_some()),
                ),
            ];
            problems.extend(
                unwanted
                    .into_iter()
                    .filter(|(_, stated)| *stated)
                    .map(|(field, _)| Problem::Unwanted(field, kind)),
            );
            if expect.forward.is_some() && expect.route.is_none() {
                problems.push(Problem::NoConnectionRoute);
            }
        }
    }
    problems
}

/// What a test sends its listener, made for the listener: every field the listener's kind
/// needs, none it does not, and each one what it can be.
fn asked(request: &Request, snapshot: &Snapshot) -> Result<Asking, Problem> {
    let listener = snapshot
        .listener(&request.listener)
        .ok_or_else(|| Problem::NoListener(request.listener.clone()))?;
    let kind = kind(listener.protocol);
    let http = [
        ("client", request.client.is_some()),
        ("protocol", request.protocol.is_some()),
        ("method", request.method.is_some()),
        ("url", request.url.is_some()),
        ("headers", request.headers.is_some()),
        ("connect_protocol", request.connect_protocol.is_some()),
    ];
    let unwanted = |given: &[(&'static str, bool)]| {
        given
            .iter()
            .find(|(_, given)| *given)
            .map_or(Ok(()), |(field, _)| Err(Problem::Unwanted(field, kind)))
    };
    match listener.protocol {
        Protocol::Tcp => {
            unwanted(&http)?;
            unwanted(&[("sni", request.sni.is_some())])?;
            return Ok(Asking::Connection(None));
        }
        Protocol::Tls => {
            unwanted(&http)?;
            return match &request.sni {
                None => Err(Problem::Missing("sni (none for no name)", kind)),
                Some(Sni::Word(NoName::None)) => Ok(Asking::Connection(None)),
                Some(Sni::Name(SniName { name })) => Ok(Asking::Connection(Some(name.clone()))),
            };
        }
        Protocol::Http | Protocol::Https => unwanted(&[("sni", request.sni.is_some())])?,
    }
    let needed = |given: &Option<String>, field| given.clone().ok_or(Problem::Missing(field, kind));
    let (scheme, authority, target) = asked::url(&needed(&request.url, "url")?)?;
    let client = needed(&request.client, "client")?;
    let headers = request
        .headers
        .as_ref()
        .ok_or(Problem::Missing("headers ([] for none)", kind))?
        .iter()
        .map(|header| asked::header(&header.name, &header.value))
        .collect::<Result<_, _>>()?;
    let asked = Asked {
        client: client.parse().map_err(|_| Invalid::Client(client))?,
        protocol: asked::protocol(&needed(&request.protocol, "protocol")?)?,
        method: asked::method(&needed(&request.method, "method")?)?,
        scheme,
        authority,
        target,
        headers,
        connect_protocol: request.connect_protocol.clone(),
    };
    // What only the listener can say of the request: its scheme, HTTP/3, a connect
    // protocol.
    asked.head(listener)?;
    Ok(Asking::Request(asked))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Version};

    const CONFIG: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
  sni: { address: "[::]:443", protocol: tls, proxy_protocol: off }
routes:
  - name: shop
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: cart, weight: 1 }] }
upstreams:
  cart: { load_balancer: p2c, endpoints: [] }
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: cart, weight: 1 }] }
tls_routes:
  - { name: api, listeners: [sni], hostnames: [{ name: api.example.com, falls_through: true }], backends: [{ upstream: cart, weight: 1 }] }
"#;

    fn snapshot() -> Snapshot {
        Snapshot::new(serde_saphyr::from_str(CONFIG).unwrap()).unwrap()
    }

    /// A test of this request and expectation, both written as flow mappings.
    fn test(name: &str, request: &str, expect: &str) -> String {
        format!("  - name: {name}\n    request: {request}\n    expect: {expect}\n")
    }

    const REQUEST: &str = "{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"http://shop.example.com/cart\", headers: [] }";
    const FORWARD: &str = "{ route: shop, rule: 0, forward: { backends: [{ upstream: cart, weight: 1 }], mirrors: [] } }";

    /// The mistakes in a file of these tests, as line and message.
    fn mistakes(tests: &[String]) -> Vec<(u64, String)> {
        let yaml = format!("tests:\n{}", tests.concat());
        let file = read(yaml.as_bytes()).unwrap();
        let mistakes = file.prepare(&snapshot()).unwrap_err();
        mistakes
            .into_iter()
            .map(|mistake| (mistake.line, mistake.problem.to_string()))
            .collect()
    }

    #[test]
    fn a_valid_file_is_made_ready_to_run_each_test_with_its_line() {
        let second = "{ listener: web, client: \"2001:db8::1\", protocol: \"2\", method: POST, url: \"http://shop.example.com/x?q=1\", headers: [{ name: X-Env, value: canary }] }";
        let yaml = format!(
            "tests:\n{}{}",
            test("first", REQUEST, FORWARD),
            test("second", second, "{ answer: no_route }")
        );
        let prepared = read(yaml.as_bytes()).unwrap().prepare(&snapshot()).unwrap();
        let lines: Vec<(u64, &str)> = prepared
            .iter()
            .map(|test| (test.line, test.name.as_str()))
            .collect();
        assert_eq!(lines, [(2, "first"), (5, "second")]);
        let second = &prepared[1];
        assert_eq!(second.listener, "web");
        let Asking::Request(asked) = &second.asked else {
            panic!("a request");
        };
        assert_eq!(asked.client.to_string(), "2001:db8::1");
        assert_eq!(
            (asked.protocol, &asked.method),
            (Version::HTTP_2, &Method::POST)
        );
        assert_eq!(asked.target.as_str(), "/x?q=1");
        assert_eq!(asked.headers.len(), 1);
        assert_eq!(second.expect.answer.as_deref(), Some("no_route"));
    }

    #[test]
    fn what_is_wrong_with_an_expectation_is_said_with_its_line() {
        let one = "a test expects one of forward, redirect, answer and refused, and only one";
        let cases = [
            ("{ route: shop, rule: 0 }", one.to_owned()),
            (
                "{ route: shop, rule: 0, answer: no_backend, redirect: { status: 301, location: / } }",
                one.to_owned(),
            ),
            (
                "{ route: shop, answer: no_backend }",
                "route and rule are stated together".to_owned(),
            ),
            (
                "{ redirect: { status: 301, location: / } }",
                "a forwarded or redirected request has a route and rule: state them".to_owned(),
            ),
            (
                "{ answer: not_found }",
                format!(
                    "'not_found' is not an answer the gateway gives: one of {}",
                    ANSWERS.join(", ")
                ),
            ),
            (
                "{ answer: no_route, upstream_request: { target: / } }",
                "upstream_request is for a forwarded request".to_owned(),
            ),
        ];
        for (expect, said) in cases {
            assert_eq!(
                mistakes(&[test("a", REQUEST, expect)]),
                [(2, said)],
                "{expect}"
            );
        }
    }

    #[test]
    fn a_header_is_expected_one_way_only() {
        let with = |header: &str| {
            format!(
                "{{ route: shop, rule: 0, forward: {{ backends: [{{ upstream: cart, weight: 1 }}], mirrors: [] }}, upstream_request: {{ headers: [{header}] }} }}"
            )
        };
        let either = "header X-A is expected by one of value, values and absent: true";
        let cases = [
            ("{ name: X-A, value: a, absent: true }", either),
            ("{ name: X-A }", either),
            (
                "{ name: X-A, values: [a] }",
                "header X-A has values for two field lines or more: write one as value",
            ),
            (
                "{ name: X-A, absent: false }",
                "header X-A has absent: true or no absent at all",
            ),
        ];
        for (header, said) in cases {
            assert_eq!(
                mistakes(&[test("a", REQUEST, &with(header))]),
                [(2, said.to_owned())],
                "{header}"
            );
        }
        for header in [
            "{ name: X-A, value: a }",
            "{ name: X-A, values: [a, b] }",
            "{ name: X-A, absent: true }",
        ] {
            let yaml = format!("tests:\n{}", test("a", REQUEST, &with(header)));
            let file = read(yaml.as_bytes()).unwrap();
            assert!(file.prepare(&snapshot()).is_ok(), "{header}");
        }
    }

    #[test]
    fn what_is_wrong_with_a_request_is_said_with_its_line() {
        let cases = [
            ("{ listener: nowhere }", "there is no listener nowhere"),
            (
                "{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"http://a/\", headers: [], sni: none }",
                "sni does not apply to an http listener",
            ),
            (
                "{ listener: web, protocol: \"1.1\", method: GET, url: \"http://a/\", headers: [] }",
                "request needs client for an http listener",
            ),
            (
                "{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"http://a/\" }",
                "request needs headers ([] for none) for an http listener",
            ),
            (
                "{ listener: web, client: somewhere, protocol: \"1.1\", method: GET, url: \"http://a/\", headers: [] }",
                "'somewhere' is not an IP address",
            ),
            (
                "{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"https://a/\", headers: [] }",
                "the scheme of listener web is http, not https",
            ),
            (
                "{ listener: web, client: 203.0.113.7, protocol: \"3\", method: GET, url: \"http://a/\", headers: [] }",
                "listener web does not serve HTTP/3",
            ),
            (
                "{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"http://a/\", headers: [{ name: Host, value: b }] }",
                "Host is the URL's to give: write it in the URL",
            ),
        ];
        for (request, said) in cases {
            assert_eq!(
                mistakes(&[test("a", request, FORWARD)]),
                [(2, said.to_owned())],
                "{request}"
            );
        }
    }

    #[test]
    fn a_connection_is_its_listener_and_for_tls_its_name_or_none() {
        let forward = "{ route: postgres, forward: { backends: [{ upstream: cart, weight: 1 }] } }";
        let yaml = format!(
            "tests:\n{}{}{}",
            test("tcp", "{ listener: db }", forward),
            test(
                "named",
                "{ listener: sni, sni: { name: api.example.com } }",
                "{ refused: no_route }"
            ),
            test(
                "unnamed",
                "{ listener: sni, sni: none }",
                "{ refused: no_route }"
            )
        );
        let prepared = read(yaml.as_bytes()).unwrap().prepare(&snapshot()).unwrap();
        let asked: Vec<Option<String>> = prepared
            .iter()
            .map(|test| match &test.asked {
                Asking::Connection(sni) => sni.clone(),
                Asking::Request(_) => panic!("a connection"),
            })
            .collect();
        assert_eq!(asked, [None, Some("api.example.com".to_owned()), None]);
    }

    #[test]
    fn what_does_not_apply_to_a_connection_is_said() {
        let refused = "{ refused: no_route }";
        let cases = [
            (
                "{ listener: db, client: 203.0.113.7 }",
                refused,
                vec!["client does not apply to a tcp listener"],
            ),
            (
                "{ listener: db, sni: none }",
                refused,
                vec!["sni does not apply to a tcp listener"],
            ),
            (
                "{ listener: sni }",
                refused,
                vec!["request needs sni (none for no name) for a tls listener"],
            ),
            (
                "{ listener: sni, sni: none, headers: [] }",
                refused,
                vec!["headers does not apply to a tls listener"],
            ),
            (
                "{ listener: db }",
                "{ route: postgres, rule: 0, forward: { backends: [], mirrors: [] } }",
                vec![
                    "rule does not apply to a tcp listener",
                    "mirrors does not apply to a tcp listener",
                ],
            ),
            (
                "{ listener: db }",
                "{ forward: { backends: [] } }",
                vec!["a forwarded connection has a route: state it"],
            ),
            (
                "{ listener: sni, sni: none }",
                "{ answer: no_route }",
                vec!["answer does not apply to a tls listener"],
            ),
            (
                "{ listener: sni, sni: none }",
                "{ refused: no_way }",
                vec!["'no_way' is not a refusal the gateway gives: one of no_route, no_backend"],
            ),
        ];
        for (request, expect, said) in cases {
            let said: Vec<(u64, String)> =
                said.into_iter().map(|said| (2, said.to_owned())).collect();
            assert_eq!(
                mistakes(&[test("a", request, expect)]),
                said,
                "{request} {expect}"
            );
        }
        // A request is refused nothing: its listener answers.
        assert_eq!(
            mistakes(&[test("a", REQUEST, refused)]),
            [(2, "refused does not apply to an http listener".to_owned())]
        );
        // And a request forwarded states its mirrors.
        let no_mirrors =
            "{ route: shop, rule: 0, forward: { backends: [{ upstream: cart, weight: 1 }] } }";
        assert_eq!(
            mistakes(&[test("a", REQUEST, no_mirrors)]),
            [(
                2,
                "a forwarded request states its mirrors: [] for none".to_owned()
            )]
        );
        // An sni that is neither none nor a name cannot be read.
        let odd = format!(
            "tests:\n{}",
            test("a", "{ listener: sni, sni: off }", refused)
        );
        assert!(matches!(
            read(odd.as_bytes()).unwrap_err().problem,
            Problem::Unreadable(_)
        ));
    }

    #[test]
    fn what_is_wrong_with_the_file_as_a_whole_is_said() {
        let file = read(b"tests: []\n").unwrap();
        assert_eq!(
            file.prepare(&snapshot()).unwrap_err(),
            [Mistake {
                line: 0,
                problem: Problem::NoTests
            }]
        );
        let twice = [
            test("same", REQUEST, FORWARD),
            test("same", REQUEST, FORWARD),
        ];
        assert_eq!(
            mistakes(&twice),
            [(5, "test name 'same' is used twice".to_owned())]
        );
        // Every mistake is said, in the order of the file.
        let both = [
            test("one", "{ listener: nowhere }", FORWARD),
            test("two", REQUEST, "{ answer: not_found }"),
        ];
        let lines: Vec<u64> = mistakes(&both).into_iter().map(|(line, _)| line).collect();
        assert_eq!(lines, [2, 5]);
    }

    #[test]
    fn a_file_that_is_not_a_file_of_tests_cannot_be_read() {
        for yaml in [
            "tests: [",
            "test: []\n",
            "tests:\n  - name: a\n    request: { listener: web }\n    expect: { answer: no_route }\n    extra: 1\n",
            // A forward's backends are stated, whatever its listener.
            "tests:\n  - name: a\n    request: { listener: web }\n    expect: { route: shop, rule: 0, forward: { mirrors: [] } }\n",
        ] {
            let mistake = read(yaml.as_bytes()).unwrap_err();
            assert_eq!(mistake.line, 0);
            assert!(matches!(mistake.problem, Problem::Unreadable(_)), "{yaml}");
        }
    }

    /// A test of this request, upstream answer and expectation, each written as a flow
    /// mapping.
    fn answered(name: &str, request: &str, answer: &str, expect: &str) -> String {
        format!(
            "  - name: {name}\n    request: {request}\n    upstream_answer: {answer}\n    expect: {expect}\n"
        )
    }

    const ANSWER: &str = "{ status: 200, headers: [{ name: Server, value: gunicorn }] }";
    const RESPONSE: &str = "response: { status: 200, headers: [{ name: Server, absent: true }] }";

    #[test]
    fn an_upstream_answer_and_its_response_go_together() {
        let forward = |with: &str| {
            format!(
                "{{ route: shop, rule: 0, forward: {{ backends: [{{ upstream: cart, weight: 1 }}], mirrors: [] }}{with} }}"
            )
        };
        let with_response = forward(&format!(", {RESPONSE}"));
        // Valid: a forward's response from its answer; a redirect's or an answer's alone.
        let yaml = format!(
            "tests:\n{}{}",
            answered("forwarded", REQUEST, ANSWER, &with_response),
            test(
                "refused",
                REQUEST,
                "{ answer: no_route, response: { status: 404 } }"
            )
        );
        let prepared = read(yaml.as_bytes()).unwrap().prepare(&snapshot()).unwrap();
        let stated = prepared[0].upstream_answer.as_ref().unwrap();
        assert_eq!((stated.status, stated.headers.len()), (200, 1));
        assert!(prepared[1].upstream_answer.is_none());

        let cases = [
            (
                answered(
                    "a",
                    REQUEST,
                    ANSWER,
                    &format!("{{ answer: no_route, {RESPONSE} }}"),
                ),
                vec!["upstream_answer is for a forwarded request"],
            ),
            (
                answered("a", REQUEST, ANSWER, FORWARD),
                vec!["an upstream_answer is stated for the response it makes: state the response"],
            ),
            (
                test("a", REQUEST, &with_response),
                vec!["a forwarded request's response is made from its upstream_answer: state it"],
            ),
            (
                answered("a", REQUEST, "{ status: 103, headers: [] }", &with_response),
                vec!["103 is not the status of a final answer"],
            ),
            (
                answered(
                    "a",
                    REQUEST,
                    "{ status: 1000, headers: [] }",
                    &with_response,
                ),
                vec!["1000 is not a status"],
            ),
            (
                answered(
                    "a",
                    REQUEST,
                    "{ status: 200, headers: [{ name: \"X A\", value: b }] }",
                    &with_response,
                ),
                vec!["'X A' is not a header name"],
            ),
            (
                test(
                    "a",
                    REQUEST,
                    "{ answer: no_route, response: { status: 99 } }",
                ),
                vec!["99 is not a status"],
            ),
            (
                test(
                    "a",
                    REQUEST,
                    "{ answer: no_route, response: { headers: [{ name: X-A, values: [a] }] } }",
                ),
                vec!["header X-A has values for two field lines or more: write one as value"],
            ),
            (
                answered(
                    "a",
                    "{ listener: db }",
                    ANSWER,
                    &format!("{{ route: postgres, forward: {{ backends: [] }}, {RESPONSE} }}"),
                ),
                vec![
                    "upstream_answer does not apply to a tcp listener",
                    "response does not apply to a tcp listener",
                ],
            ),
        ];
        for (test, said) in cases {
            let said: Vec<(u64, String)> =
                said.into_iter().map(|said| (2, said.to_owned())).collect();
            assert_eq!(mistakes(std::slice::from_ref(&test)), said, "{test}");
        }
    }

    /// Whether a WebSocket handshake switches cannot be told offline, so its test states no
    /// upstream answer; one that is no handshake, by the core's reading, may.
    #[test]
    fn a_websocket_handshake_states_no_upstream_answer() {
        let handshake = "{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"http://shop.example.com/chat\", headers: [{ name: Connection, value: Upgrade }, { name: Upgrade, value: websocket }, { name: Sec-WebSocket-Key, value: dGhlIHNhbXBsZSBub25jZQ== }, { name: Sec-WebSocket-Version, value: \"13\" }] }";
        let expect = format!(
            "{{ route: shop, rule: 0, forward: {{ backends: [{{ upstream: cart, weight: 1 }}], mirrors: [] }}, {RESPONSE} }}"
        );
        assert_eq!(
            mistakes(&[answered("a", handshake, ANSWER, &expect)]),
            [(
                2,
                "a WebSocket handshake's answer cannot be told offline: whether it switches rests on the gateway's own key and on its backend's connection".to_owned()
            )]
        );
        // Without its key it is a plain GET, whose answer can be told.
        let plain = handshake.replace(
            ", { name: Sec-WebSocket-Key, value: dGhlIHNhbXBsZSBub25jZQ== }",
            "",
        );
        let yaml = format!("tests:\n{}", answered("a", &plain, ANSWER, &expect));
        assert!(read(yaml.as_bytes()).unwrap().prepare(&snapshot()).is_ok());
    }
}
