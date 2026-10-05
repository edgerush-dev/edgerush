//! Running a test ([22 §3](../../../docs/22-explain-and-test.md)): its request explained, and
//! what came of it held to what the test expects. Each difference is a line, expected
//! beside got; a test with none passed.

use crate::explained::{Snapshot, Unexplained};
use crate::test_file::{Backend, Fraction, HeaderExpected, Mirror, Prepared};
use edgerush_config::{Filter, Rule};
use edgerush_proxy::Decision;
use http::header::HeaderMap;

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
    let explained = snapshot.explain(listener, &test.asked)?;
    let expect = &test.expect;
    let mut differences = Vec::new();
    let mut differ = |what: &str, expected: String, got: String| {
        if expected != got {
            differences.push(format!("{what}: expected {expected}, got {got}"));
        }
    };

    let routed = explained.routed();
    let or_none = |said: Option<String>| said.unwrap_or_else(|| "none".to_owned());
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
                mirrors(&forward.mirrors),
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

    Ok(Ran {
        differences,
        explanation: explained.text(),
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
        forward: { backends: [{ upstream: cart, weight: 9 }, { upstream: canary, weight: 1 }] }
      - matches: [{ path: { exact: /closed } }]
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
}
