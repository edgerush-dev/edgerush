//! Running a test ([22 §3](../../../docs/22-explain-and-test.md)): its request or connection
//! explained, and what came of it held to what the test expects. Each difference is a line,
//! expected beside got; a test with none passed.

use crate::asked::{self, Asked};
use crate::explained::{STAND_IN_ID, Snapshot, Unexplained};
use crate::test_file::{
    Asking, Backend, Expect, Fraction, HeaderExpected, Mirror, Prepared, UpstreamAnswer,
};
use edgerush_config::{CompiledListener, Filter, RequestId, Rule};
use edgerush_proxy::way_back::{self, Way};
use edgerush_proxy::{Decision, Rejection};
use http::header::HeaderMap;
use http::{StatusCode, Version};

/// What a test came to.
#[derive(Debug)]
pub struct Ran {
    /// What the test expected that it did not get, a line each.
    pub differences: Vec<String>,
    /// Its request's explanation, as `explain` gives it.
    pub explanation: String,
}

impl Ran {
    /// Whether it got all it expected.
    #[must_use]
    pub fn passed(&self) -> bool {
        self.differences.is_empty()
    }
}

/// Runs `test` against `snapshot`.
///
/// # Errors
///
/// When the request cannot be explained, which one prepared against `snapshot` always can
/// unless the walk disagrees with the router.
pub fn run(snapshot: &Snapshot, test: &Prepared) -> Result<Ran, Unexplained> {
    let listener = snapshot
        .listener(&test.listener)
        .ok_or_else(|| Unexplained::NoListener(test.listener.clone()))?;
    match &test.asked {
        Asking::Request(asked) => request(
            snapshot,
            listener,
            asked,
            test.upstream_answer.as_ref(),
            &test.expect,
        ),
        Asking::Connection(sni) => connection(snapshot, listener, sni.as_deref(), &test.expect),
    }
}

/// What differs, a line each.
#[derive(Default)]
struct Differences(Vec<String>);

impl Differences {
    fn differ(&mut self, what: &str, expected: String, got: String) {
        if expected != got {
            self.0
                .push(format!("{what}: expected {expected}, got {got}"));
        }
    }
}

/// What was said, or `none`.
fn or_none(said: Option<String>) -> String {
    said.unwrap_or_else(|| "none".to_owned())
}

/// Runs a request's test.
fn request(
    snapshot: &Snapshot,
    listener: &CompiledListener,
    asked: &Asked,
    upstream_answer: Option<&UpstreamAnswer>,
    expect: &Expect,
) -> Result<Ran, Unexplained> {
    let explained = snapshot.explain(listener, asked)?;
    let mut differences = Differences::default();
    let mut differ = |what: &str, expected: String, got: String| {
        differences.differ(what, expected, got);
    };

    let routed = explained.routed();
    differ(
        "route",
        or_none(expect.route.clone()),
        or_none(routed.map(|(route, ..)| route.to_owned())),
    );
    differ(
        "rule",
        or_none(expect.rule.map(|rule| rule.to_string())),
        or_none(routed.map(|(.., rule)| rule.to_string())),
    );

    let decided = explained.decided();
    match (&expect.forward, &expect.redirect, &expect.answer, decided) {
        (Some(forward), _, _, Ok(Decision::Forward(_))) => {
            let rule = routed.map(|(_, rule, _)| rule);
            differ(
                "backends",
                backends(&forward.backends),
                backends(&rule.map_or_else(Vec::new, rule_backends)),
            );
            differ(
                "mirrors",
                mirrors(forward.mirrors.as_deref().unwrap_or_default()),
                mirrors(&rule.map_or_else(Vec::new, rule_mirrors)),
            );
            if let Some(upstream) = &expect.upstream_request {
                let head = explained.upstream();
                if let Some(target) = &upstream.target {
                    let got = head
                        .uri
                        .path_and_query()
                        .map_or("/", |target| target.as_str());
                    differ("target", target.clone(), got.to_owned());
                }
                for header in &upstream.headers {
                    let (expected, got) = header_values(header, &head.headers);
                    differ(&format!("header {}", header.name), expected, got);
                }
            }
        }
        (_, Some(redirect), _, Ok(Decision::Redirect(redirected))) => {
            let location = String::from_utf8_lossy(redirected.location.as_bytes());
            differ(
                "redirect",
                format!("{} to {}", redirect.status, redirect.location),
                format!("{} to {location}", redirected.status.as_u16()),
            );
        }
        (_, _, Some(answer), Err(rejection)) => {
            differ("answer", answer.clone(), rejection.reason().to_owned());
        }
        (forward, redirect, answer, decided) => {
            let expected = if forward.is_some() {
                "forward".to_owned()
            } else if let Some(redirect) = redirect {
                format!("redirect {} to {}", redirect.status, redirect.location)
            } else {
                format!("answer {}", answer.clone().unwrap_or_default())
            };
            let got = match decided {
                Ok(Decision::Forward(_)) => "forward".to_owned(),
                Ok(Decision::Redirect(redirected)) => format!(
                    "redirect {} to {}",
                    redirected.status.as_u16(),
                    String::from_utf8_lossy(redirected.location.as_bytes())
                ),
                Err(rejection) => format!("answer {}", rejection.reason()),
            };
            differ("outcome", expected, got);
        }
    }

    if let Some(response) = &expect.response
        && let Some(got) = answer(listener, asked, decided, upstream_answer)
    {
        if let Some(status) = response.status {
            differ(
                "response status",
                status.to_string(),
                got.status().as_u16().to_string(),
            );
        }
        for header in &response.headers {
            let (expected, got) = header_values(header, got.headers());
            differ(&format!("response header {}", header.name), expected, got);
        }
    }

    Ok(Ran {
        differences: differences.0,
        explanation: explained.text(),
    })
}

/// The answer the client gets, as the gateway hands it to its server (22 §5): the upstream's
/// answer edited on its way back, a redirect's, or the gateway's own, each with what every
/// answer says last. None for a request forwarded without an upstream answer stated, which
/// only a test that expected another outcome has, and which that outcome's line tells.
fn answer(
    listener: &CompiledListener,
    asked: &Asked,
    decided: Result<&Decision<'_>, Rejection>,
    stated: Option<&UpstreamAnswer>,
) -> Option<http::Response<()>> {
    // Read from the head as the client sent it, as `serve` reads it.
    let sent: HeaderMap = asked.headers.iter().cloned().collect();
    let call = way_back::is_grpc_call(asked.protocol, &sent);
    let mut answer = http::Response::new(());
    match decided {
        Ok(Decision::Forward(forward)) => {
            let stated = stated?;
            *answer.status_mut() = StatusCode::from_u16(stated.status).ok()?;
            for header in &stated.headers {
                let (name, value) = asked::answer_header(&header.name, &header.value).ok()?;
                answer.headers_mut().append(name, value);
            }
            let way = Way {
                changes: forward.rule.response_headers.as_ref(),
                upgradable: asked.protocol == Version::HTTP_11,
                websocket: None,
            };
            // A map takes every field it is given, and no WebSocket's answer is told here:
            // nothing fails.
            let _edited = way_back::upstream_answer(&mut answer, &way);
        }
        Ok(Decision::Redirect(_)) if call => way_back::call_redirected(&mut answer),
        Ok(Decision::Redirect(redirected)) => {
            // A map takes every field it is given: nothing fails.
            let _made = way_back::redirect_answer(
                &mut answer,
                redirected.status,
                redirected.location.clone(),
                redirected.rule.response_headers.as_ref(),
            );
        }
        Err(rejection) if call => way_back::call_rejected(&mut answer, rejection),
        Err(Rejection::MaxForwards { options }) => {
            // A map takes every field it is given: nothing fails.
            let _made = way_back::final_recipient_answer(&mut answer, options);
        }
        Err(rejection) => *answer.status_mut() = rejection.status(),
    }
    let alt_svc = way_back::alt_svc(listener, listener.address.port());
    let id = (listener.request_id == RequestId::Generate).then_some(STAND_IN_ID);
    way_back::every_answer(&mut answer, alt_svc.as_ref(), id);
    Some(answer)
}

/// Runs a connection's test: its route, and its backends or why it is refused.
fn connection(
    snapshot: &Snapshot,
    listener: &CompiledListener,
    sni: Option<&str>,
    expect: &Expect,
) -> Result<Ran, Unexplained> {
    let connected = snapshot.explain_connection(listener, sni)?;
    let mut differences = Differences::default();
    differences.differ(
        "route",
        or_none(expect.route.clone()),
        or_none(connected.route().map(str::to_owned)),
    );
    let got_backends: Vec<Backend> = connected
        .backends()
        .iter()
        .map(|backend| Backend {
            upstream: backend.upstream.clone(),
            weight: backend.weight,
        })
        .collect();
    match (&expect.forward, &expect.refused, connected.refused()) {
        (Some(forward), _, None) => {
            differences.differ(
                "backends",
                backends(&forward.backends),
                backends(&got_backends),
            );
        }
        (_, Some(refused), Some(got)) => {
            differences.differ("refused", refused.clone(), got.to_owned());
        }
        (_, refused, got) => {
            let said = |refused: Option<&str>| {
                refused.map_or_else(
                    || "forward".to_owned(),
                    |reason| format!("refused {reason}"),
                )
            };
            differences.differ("outcome", said(refused.as_deref()), said(got));
        }
    }
    Ok(Ran {
        differences: differences.0,
        explanation: connected.text(),
    })
}

/// A rule's backends as a test states them.
fn rule_backends(rule: &Rule) -> Vec<Backend> {
    rule.forward
        .iter()
        .flat_map(|forward| &forward.backends)
        .map(|backend| Backend {
            upstream: backend.upstream.clone(),
            weight: backend.weight,
        })
        .collect()
}

/// A rule's mirrors as a test states them.
fn rule_mirrors(rule: &Rule) -> Vec<Mirror> {
    rule.filters
        .iter()
        .filter_map(|filter| match filter {
            Filter::RequestMirror(mirror) => Some(Mirror {
                upstream: mirror.upstream.clone(),
                fraction: Fraction {
                    numerator: mirror.fraction.numerator,
                    denominator: mirror.fraction.denominator,
                },
            }),
            _ => None,
        })
        .collect()
}

fn backends(backends: &[Backend]) -> String {
    listed(
        backends
            .iter()
            .map(|backend| format!("{} (weight {})", backend.upstream, backend.weight)),
    )
}

fn mirrors(mirrors: &[Mirror]) -> String {
    listed(mirrors.iter().map(|mirror| {
        format!(
            "{} ({}/{})",
            mirror.upstream, mirror.fraction.numerator, mirror.fraction.denominator
        )
    }))
}

fn listed(items: impl Iterator<Item = String>) -> String {
    let items: Vec<String> = items.collect();
    if items.is_empty() {
        "none".to_owned()
    } else {
        items.join(", ")
    }
}

/// A header as expected and as the upstream would be sent it, each written the same way:
/// `absent`, or its field lines' values, quoted, in order.
fn header_values(header: &HeaderExpected, headers: &HeaderMap) -> (String, String) {
    let lines = |values: Vec<&[u8]>| {
        if values.is_empty() {
            "absent".to_owned()
        } else {
            values
                .iter()
                .map(|value| format!("\"{}\"", value.escape_ascii()))
                .collect::<Vec<_>>()
                .join(", ")
        }
    };
    let expected: Vec<&[u8]> = match (&header.value, &header.values) {
        (Some(value), _) => vec![value.as_bytes()],
        (None, Some(values)) => values.iter().map(String::as_bytes).collect(),
        (None, None) => Vec::new(),
    };
    // A name that is no header name is had by no header.
    let got: Vec<&[u8]> = http::HeaderName::from_bytes(header.name.as_bytes())
        .map(|name| {
            headers
                .get_all(name)
                .iter()
                .map(http::HeaderValue::as_bytes)
                .collect()
        })
        .unwrap_or_default();
    (lines(expected), lines(got))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_file;

    const CONFIG: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: shop
    listeners: [web]
    hostnames: [{ name: shop.example.com, falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /cart } }]
        filters:
          - { type: request_header_modifier, set: [{ name: X-Gateway, value: edgerush }], add: [{ name: X-Tag, value: b }] }
          - { type: request_mirror, upstream: shadow, fraction: { numerator: 1, denominator: 10 } }
          - { type: response_header_modifier, set: [{ name: X-Served-By, value: edgerush }], remove: [server] }
        forward: { backends: [{ upstream: cart, weight: 9 }, { upstream: canary, weight: 1 }] }
      - matches: [{ path: { exact: /closed } }]
        filters:
          - { type: response_header_modifier, set: [{ name: Cache-Control, value: no-store }] }
        redirect: { status: 301, path: { replace_full: /open }, query: keep }
upstreams:
  cart: { load_balancer: p2c, endpoints: [] }
  canary: { load_balancer: p2c, endpoints: [] }
  shadow: { load_balancer: p2c, endpoints: [] }
"#;

    /// Runs one test of this request and expectation; what differed.
    fn differences(url: &str, headers: &str, expect: &str) -> Vec<String> {
        let snapshot = Snapshot::new(serde_saphyr::from_str(CONFIG).unwrap()).unwrap();
        let yaml = format!(
            "tests:\n  - name: a\n    request: {{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"{url}\", headers: {headers} }}\n    expect: {expect}\n"
        );
        let tests = test_file::read(yaml.as_bytes())
            .unwrap()
            .prepare(&snapshot)
            .unwrap();
        let ran = run(&snapshot, &tests[0]).unwrap();
        assert!(ran.explanation.starts_with("web (http)  GET "));
        assert_eq!(ran.passed(), ran.differences.is_empty());
        ran.differences
    }

    const CART: &str = "http://shop.example.com/cart?id=7";
    const BACKENDS: &str =
        "backends: [{ upstream: cart, weight: 9 }, { upstream: canary, weight: 1 }]";
    const MIRRORS: &str =
        "mirrors: [{ upstream: shadow, fraction: { numerator: 1, denominator: 10 } }]";

    fn forward(route: &str, backends: &str, mirrors: &str, upstream: &str) -> String {
        format!("{{ {route}, forward: {{ {backends}, {mirrors} }}{upstream} }}")
    }

    const CONNECTIONS: &str = r#"
listeners:
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
  sni: { address: "[::]:443", protocol: tls, proxy_protocol: off }
routes: []
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: pg, weight: 1 }] }
tls_routes:
  - { name: api, listeners: [sni], hostnames: [{ name: api.example.com, falls_through: true }], backends: [{ upstream: api, weight: 3 }, { upstream: canary, weight: 1 }] }
  - { name: idle, listeners: [sni], hostnames: [{ name: idle.example.com, falls_through: true }], backends: [{ upstream: api, weight: 0 }] }
upstreams:
  api: { load_balancer: p2c, endpoints: [] }
  canary: { load_balancer: p2c, endpoints: [] }
  pg: { load_balancer: p2c, endpoints: [] }
"#;

    /// Runs one test of a connection; what differed.
    fn connection_differences(request: &str, expect: &str) -> Vec<String> {
        let snapshot = Snapshot::new(serde_saphyr::from_str(CONNECTIONS).unwrap()).unwrap();
        let yaml = format!("tests:\n  - name: a\n    request: {request}\n    expect: {expect}\n");
        let tests = test_file::read(yaml.as_bytes())
            .unwrap()
            .prepare(&snapshot)
            .unwrap();
        let ran = run(&snapshot, &tests[0]).unwrap();
        assert!(ran.explanation.contains(" (t"), "{}", ran.explanation);
        ran.differences
    }

    #[test]
    fn a_connections_test_holds_its_route_backends_and_refusal() {
        let tcp = "{ listener: db }";
        let api = "{ listener: sni, sni: { name: api.example.com } }";
        let idle = "{ listener: sni, sni: { name: idle.example.com } }";
        let none = "{ listener: sni, sni: none }";
        let pg = "{ route: postgres, forward: { backends: [{ upstream: pg, weight: 1 }] } }";
        let api_backends = "{ route: api, forward: { backends: [{ upstream: api, weight: 3 }, { upstream: canary, weight: 1 }] } }";
        assert!(connection_differences(tcp, pg).is_empty());
        assert!(connection_differences(api, api_backends).is_empty());
        assert!(connection_differences(none, "{ refused: no_route }").is_empty());
        assert!(connection_differences(idle, "{ route: idle, refused: no_backend }").is_empty());

        assert_eq!(
            connection_differences(
                api,
                "{ route: api, forward: { backends: [{ upstream: api, weight: 1 }] } }"
            ),
            ["backends: expected api (weight 1), got api (weight 3), canary (weight 1)"]
        );
        assert_eq!(
            connection_differences(idle, "{ refused: no_backend }"),
            ["route: expected none, got idle"]
        );
        assert_eq!(
            connection_differences(idle, "{ route: idle, refused: no_route }"),
            ["refused: expected no_route, got no_backend"]
        );
        assert_eq!(
            connection_differences(none, pg),
            [
                "route: expected postgres, got none",
                "outcome: expected forward, got refused no_route"
            ]
        );
        assert_eq!(
            connection_differences(api, "{ route: api, refused: no_backend }"),
            ["outcome: expected refused no_backend, got forward"]
        );
    }

    #[test]
    fn a_test_that_gets_all_it_expects_passes() {
        let upstream = ", upstream_request: { target: \"/cart?id=7\", headers: [{ name: X-Gateway, value: edgerush }, { name: X-Tag, values: [a, b] }, { name: X-Debug, absent: true }] }";
        let expect = forward("route: shop, rule: 0", BACKENDS, MIRRORS, upstream);
        assert_eq!(
            differences(CART, "[{ name: X-Tag, value: a }]", &expect),
            Vec::<String>::new()
        );
        let redirect =
            "{ route: shop, rule: 1, redirect: { status: 301, location: \"/open?x=1\" } }";
        assert!(differences("http://shop.example.com/closed?x=1", "[]", redirect).is_empty());
        let refused = "{ answer: bad_path }";
        assert!(differences("http://shop.example.com/a/%2e%2e/", "[]", refused).is_empty());
        assert!(differences("http://elsewhere.example/", "[]", "{ answer: no_route }").is_empty());
    }

    #[test]
    fn the_route_and_rule_are_held_to_what_was_routed_on_absence_included() {
        let expect = forward("route: other, rule: 1", BACKENDS, MIRRORS, "");
        assert_eq!(
            differences(CART, "[]", &expect),
            ["route: expected other, got shop", "rule: expected 1, got 0"]
        );
        // An answer for a request that was routed states its route and rule.
        let snapshot_answer = "{ answer: no_route }";
        assert_eq!(
            differences(CART, "[]", snapshot_answer),
            [
                "route: expected none, got shop",
                "rule: expected none, got 0",
                "outcome: expected answer no_route, got forward"
            ]
        );
    }

    #[test]
    fn the_rules_lists_are_held_whole() {
        let one = "backends: [{ upstream: cart, weight: 1 }]";
        let expect = forward("route: shop, rule: 0", one, "mirrors: []", "");
        assert_eq!(
            differences(CART, "[]", &expect),
            [
                "backends: expected cart (weight 1), got cart (weight 9), canary (weight 1)",
                "mirrors: expected none, got shadow (1/10)"
            ]
        );
    }

    #[test]
    fn the_upstream_request_is_held_to_what_is_stated_of_it() {
        let upstream = ", upstream_request: { target: /cart, headers: [{ name: X-Gateway, value: other }, { name: X-Tag, value: b }, { name: X-Forwarded-For, absent: true }, { name: X-None, values: [a, b] }] }";
        let expect = forward("route: shop, rule: 0", BACKENDS, MIRRORS, upstream);
        assert_eq!(
            differences(CART, "[{ name: X-Tag, value: a }]", &expect),
            [
                "target: expected /cart, got /cart?id=7",
                "header X-Gateway: expected \"other\", got \"edgerush\"",
                "header X-Tag: expected \"b\", got \"a\", \"b\"",
                "header X-Forwarded-For: expected absent, got \"203.0.113.7\"",
                "header X-None: expected \"a\", \"b\", got absent"
            ]
        );
    }

    #[test]
    fn another_outcome_is_said_as_the_outcome() {
        let redirect = "{ route: shop, rule: 1, redirect: { status: 302, location: /open } }";
        assert_eq!(
            differences("http://shop.example.com/closed", "[]", redirect),
            ["redirect: expected 302 to /open, got 301 to /open"]
        );
        assert_eq!(
            differences(
                "http://shop.example.com/a/%2e%2e/",
                "[]",
                "{ answer: bad_host }"
            ),
            ["answer: expected bad_host, got bad_path"]
        );
        let expect = forward("route: shop, rule: 1", BACKENDS, MIRRORS, "");
        assert_eq!(
            differences("http://shop.example.com/closed", "[]", &expect),
            ["outcome: expected forward, got redirect 301 to /open"]
        );
    }

    /// Runs one test against `snapshot`, its request, upstream answer and expectation written
    /// as flow mappings; what differed.
    fn ran(snapshot: &Snapshot, request: &str, answer: Option<&str>, expect: &str) -> Vec<String> {
        let answer = answer.map_or_else(String::new, |answer| {
            format!("    upstream_answer: {answer}\n")
        });
        let yaml =
            format!("tests:\n  - name: a\n    request: {request}\n{answer}    expect: {expect}\n");
        let tests = test_file::read(yaml.as_bytes())
            .unwrap()
            .prepare(snapshot)
            .unwrap();
        let ran = run(snapshot, &tests[0]).unwrap();
        assert_eq!(ran.passed(), ran.differences.is_empty());
        ran.differences
    }

    fn shop() -> Snapshot {
        Snapshot::new(serde_saphyr::from_str(CONFIG).unwrap()).unwrap()
    }

    /// An HTTP/1.1 GET of `url` to the shop's listener.
    fn get(url: &str) -> String {
        format!(
            "{{ listener: web, client: 203.0.113.7, protocol: \"1.1\", method: GET, url: \"{url}\", headers: [] }}"
        )
    }

    /// An upstream's answer with fields about its connection, one its `Connection` names, and
    /// one the rule takes off.
    const ANSWER: &str = "{ status: 200, headers: [{ name: Connection, value: x-debug }, { name: X-Debug, value: \"1\" }, { name: Keep-Alive, value: timeout=5 }, { name: Server, value: gunicorn }, { name: Content-Type, value: text/plain }] }";

    const ID: &str = "{ name: X-Request-ID, value: 00000000-0000-7000-8000-000000000000 }";

    #[test]
    fn the_response_is_the_upstreams_answer_as_the_way_back_leaves_it() {
        let response = format!(
            ", response: {{ status: 200, headers: [{{ name: X-Debug, absent: true }}, {{ name: Keep-Alive, absent: true }}, {{ name: Server, absent: true }}, {{ name: Content-Type, value: text/plain }}, {{ name: X-Served-By, value: edgerush }}, {ID}] }}"
        );
        let expect = forward("route: shop, rule: 0", BACKENDS, MIRRORS, &response);
        assert_eq!(
            ran(&shop(), &get(CART), Some(ANSWER), &expect),
            Vec::<String>::new()
        );
        // What differs is said, a line each.
        let response = ", response: { status: 201, headers: [{ name: X-Served-By, value: other }, { name: Server, value: gunicorn }] }";
        let expect = forward("route: shop, rule: 0", BACKENDS, MIRRORS, response);
        assert_eq!(
            ran(&shop(), &get(CART), Some(ANSWER), &expect),
            [
                "response status: expected 201, got 200",
                "response header X-Served-By: expected \"other\", got \"edgerush\"",
                "response header Server: expected \"gunicorn\", got absent"
            ]
        );
    }

    #[test]
    fn a_redirects_and_the_gateways_own_answers_are_told_without_one() {
        let redirect = format!(
            "{{ route: shop, rule: 1, redirect: {{ status: 301, location: \"/open?x=1\" }}, response: {{ status: 301, headers: [{{ name: Location, value: \"/open?x=1\" }}, {{ name: Cache-Control, value: no-store }}, {ID}] }} }}"
        );
        assert_eq!(
            ran(
                &shop(),
                &get("http://shop.example.com/closed?x=1"),
                None,
                &redirect
            ),
            Vec::<String>::new()
        );
        let missing = format!(
            "{{ answer: no_route, response: {{ status: 404, headers: [{ID}, {{ name: Content-Type, absent: true }}] }} }}"
        );
        assert_eq!(
            ran(&shop(), &get("http://elsewhere.example/"), None, &missing),
            Vec::<String>::new()
        );
        let wrong = "{ answer: no_route, response: { status: 500 } }";
        assert_eq!(
            ran(&shop(), &get("http://elsewhere.example/"), None, wrong),
            ["response status: expected 500, got 404"]
        );
    }

    /// A gRPC call the gateway answers itself is answered as gRPC answers: 200, with the
    /// status gRPC gives the cause.
    #[test]
    fn a_grpc_call_is_answered_as_grpc_answers() {
        let call = "{ listener: web, client: 203.0.113.7, protocol: \"2\", method: POST, url: \"http://elsewhere.example/bench.Echo/Call\", headers: [{ name: Content-Type, value: application/grpc }] }";
        let expect = "{ answer: no_route, response: { status: 200, headers: [{ name: Content-Type, value: application/grpc }, { name: grpc-status, value: \"12\" }, { name: grpc-message, value: no route serves this method }] } }";
        assert_eq!(ran(&shop(), call, None, expect), Vec::<String>::new());
        let redirect = call.replace(
            "elsewhere.example/bench.Echo/Call",
            "shop.example.com/closed",
        );
        let expect = "{ route: shop, rule: 1, redirect: { status: 301, location: /open }, response: { status: 200, headers: [{ name: grpc-status, value: \"12\" }, { name: Location, absent: true }] } }";
        assert_eq!(ran(&shop(), &redirect, None, expect), Vec::<String>::new());
        // The same request over HTTP/1.1 is no call.
        let plain = call.replace("\"2\"", "\"1.1\"");
        let expect = "{ answer: no_route, response: { status: 404, headers: [{ name: grpc-status, absent: true }] } }";
        assert_eq!(ran(&shop(), &plain, None, expect), Vec::<String>::new());
    }

    /// A listener that serves HTTP/3 says so on every answer, on the port of its address and
    /// for as long as its config says; one that passes request IDs tells the client none.
    #[test]
    fn a_listener_that_serves_http3_says_so_on_every_response() {
        let mut config: edgerush_config::Config = serde_saphyr::from_str(
            r#"
listeners:
  quic: { address: "[::]:9443", protocol: https, proxy_protocol: off, tls: { certificates: [site] }, http3: { alt_svc_max_age: 60 }, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
routes: []
upstreams: {}
"#,
        )
        .unwrap();
        let site = edgerush_config::Certificate {
            chain: String::new(),
            key: String::new(),
        };
        config.certificates.insert("site".to_owned(), site);
        let snapshot = Snapshot::new(config).unwrap();
        let request = "{ listener: quic, client: 203.0.113.7, protocol: \"3\", method: GET, url: \"https://a.example/\", headers: [] }";
        let expect = r#"{ answer: no_route, response: { status: 404, headers: [{ name: Alt-Svc, value: 'h3=":9443"; ma=60' }, { name: X-Request-ID, absent: true }] } }"#;
        assert_eq!(ran(&snapshot, request, None, expect), Vec::<String>::new());
    }

    /// A 426 that offers WebSocket says so to an HTTP/1.1 client, which alone can switch to
    /// it, and not to an HTTP/2 one.
    #[test]
    fn a_426_offers_websocket_to_an_http11_client_alone() {
        let refusal = "{ status: 426, headers: [{ name: Upgrade, value: \"h2c, websocket\" }, { name: Connection, value: upgrade }] }";
        let offered = forward(
            "route: shop, rule: 0",
            BACKENDS,
            MIRRORS,
            ", response: { status: 426, headers: [{ name: Upgrade, value: websocket }] }",
        );
        assert_eq!(
            ran(&shop(), &get(CART), Some(refusal), &offered),
            Vec::<String>::new()
        );
        let over_h2 = get(CART).replace("\"1.1\"", "\"2\"");
        let not_offered = forward(
            "route: shop, rule: 0",
            BACKENDS,
            MIRRORS,
            ", response: { status: 426, headers: [{ name: Upgrade, absent: true }] }",
        );
        assert_eq!(
            ran(&shop(), &over_h2, Some(refusal), &not_offered),
            Vec::<String>::new()
        );
    }
}
