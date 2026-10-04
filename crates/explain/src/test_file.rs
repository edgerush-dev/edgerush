//! The file of tests `edgerush test` runs ([22 §3](../../../docs/22-explain-and-test.md)):
//! its model, read from YAML, and the checks that make it valid before anything is run —
//! first on its own, then against the config, where each request is made what it will be
//! run as.

use crate::asked::{self, Asked, Invalid};
use crate::explained::{Snapshot, protocol_name};
use edgerush_config::Protocol;
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
    /// What is expected of it.
    pub expect: Expect,
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
    /// What the upstream is sent, as far as it is stated.
    #[serde(default)]
    pub upstream_request: Option<UpstreamRequest>,
}

/// The rule's backends and mirrors, each list whole: what is drawn at random is asserted
/// as the list it is drawn from.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Forward {
    /// The backends, in the rule's order.
    pub backends: Vec<Backend>,
    /// The mirrors, in the rule's order; `[]` for none.
    pub mirrors: Vec<Mirror>,
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
    #[error("a test expects one of forward, redirect and answer, and only one")]
    Outcome,
    /// `route` without `rule`, or the other way round.
    #[error("route and rule are stated together")]
    RouteAndRule,
    /// A forward or redirect without its route and rule.
    #[error("a forwarded or redirected request has a route and rule: state them")]
    NoRoute,
    /// An answer the gateway never gives itself.
    #[error("'{0}' is not an answer the gateway gives: one of {list}", list = ANSWERS.join(", "))]
    Answer(String),
    /// `upstream_request` beside something other than `forward`.
    #[error("upstream_request is for a forwarded request")]
    UpstreamRequest,
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
    /// A listener that is not `http` or `https`.
    #[error("listener {0} is a {1} listener: test takes http and https listeners for now")]
    NotHttp(String, &'static str),
    /// A field the listener's kind needs, missing.
    #[error("request needs {0} for an {1} listener")]
    Missing(&'static str, &'static str),
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
    /// The request.
    pub asked: Asked,
    /// What is expected of it.
    pub expect: Expect,
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
            match asked(&test.request, snapshot) {
                Ok(asked) => prepared.push(Prepared {
                    line,
                    name: test.name.clone(),
                    listener: test.request.listener.clone(),
                    asked,
                    expect: test.expect.clone(),
                }),
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

/// What is wrong with an expectation on its own.
fn expectation(expect: &Expect) -> Vec<Problem> {
    let mut problems = Vec::new();
    let outcomes = [
        expect.forward.is_some(),
        expect.redirect.is_some(),
        expect.answer.is_some(),
    ];
    if outcomes.iter().filter(|stated| **stated).count() != 1 {
        problems.push(Problem::Outcome);
    }
    if expect.route.is_some() != expect.rule.is_some() {
        problems.push(Problem::RouteAndRule);
    } else if (expect.forward.is_some() || expect.redirect.is_some()) && expect.route.is_none() {
        problems.push(Problem::NoRoute);
    }
    if let Some(answer) = &expect.answer
        && !ANSWERS.contains(&answer.as_str())
    {
        problems.push(Problem::Answer(answer.clone()));
    }
    if let Some(upstream) = &expect.upstream_request {
        if expect.forward.is_none() {
            problems.push(Problem::UpstreamRequest);
        }
        for header in &upstream.headers {
            let name = header.name.clone();
            match (&header.value, &header.values, header.absent) {
                (Some(_), None, None) | (None, None, Some(true)) => {}
                (None, Some(values), None) if values.len() >= 2 => {}
                (None, Some(_), None) => problems.push(Problem::TooFewValues(name)),
                (None, None, Some(false)) => problems.push(Problem::AbsentFalse(name)),
                _ => problems.push(Problem::HeaderForm(name)),
            }
        }
    }
    problems
}

/// The request a test states, made for its listener: every field the listener's kind
/// needs, and each one what it can be.
fn asked(request: &Request, snapshot: &Snapshot) -> Result<Asked, Problem> {
    let listener = snapshot
        .listener(&request.listener)
        .ok_or_else(|| Problem::NoListener(request.listener.clone()))?;
    let kind = protocol_name(listener.protocol);
    if matches!(listener.protocol, Protocol::Tcp | Protocol::Tls) {
        return Err(Problem::NotHttp(listener.name.clone(), kind));
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
    Ok(asked)
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{Method, Version};

    const CONFIG: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
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
        assert_eq!(second.asked.client.to_string(), "2001:db8::1");
        assert_eq!(
            (second.asked.protocol, &second.asked.method),
            (Version::HTTP_2, &Method::POST)
        );
        assert_eq!(second.asked.target.as_str(), "/x?q=1");
        assert_eq!(second.asked.headers.len(), 1);
        assert_eq!(second.expect.answer.as_deref(), Some("no_route"));
    }

    #[test]
    fn what_is_wrong_with_an_expectation_is_said_with_its_line() {
        let one = "a test expects one of forward, redirect and answer, and only one";
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
                "{ listener: db }",
                "listener db is a tcp listener: test takes http and https listeners for now",
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
            "tests:\n  - name: a\n    request: { listener: web }\n    expect: { route: shop, rule: 0, forward: { backends: [] } }\n",
        ] {
            let mistake = read(yaml.as_bytes()).unwrap_err();
            assert_eq!(mistake.line, 0);
            assert!(matches!(mistake.problem, Problem::Unreadable(_)), "{yaml}");
        }
    }
}
