//! The request core: from the head of a request to where it goes, whatever protocol it
//! came in by. It sees plain `http` types and a compiled config, never the HTTP engine;
//! HTTP/1.1, HTTP/2 and later HTTP/3 are adapters around it.
//!
//! The host is the one the request target names (HTTP/2's `:authority`, HTTP/1.1's
//! absolute form) and only without one the `Host` field, as RFC 9112 §3.2.2 has it. The
//! path is normalised once, and the same normal form is matched and forwarded. Whatever is
//! routed on is what the upstream gets to see: the head is rewritten to say it.

use crate::host::{HostError, bare_host, host_field};
use edgerush_config::{Compiled, CompiledListener, CompiledRule, CompiledUpstream};
use edgerush_router::{NormaliseError, RequestParts, normalise_path};
use http::header::{HOST, HeaderValue};
use http::request::Parts;
use http::uri::PathAndQuery;
use http::{StatusCode, Uri};
use std::borrow::Cow;

/// Where a request goes.
#[derive(Debug, Clone, Copy)]
pub struct Forward<'a> {
    /// The rule the request belongs to, for what is still to be done to its response.
    pub rule: &'a CompiledRule,
    /// The upstream chosen among the rule's backends.
    pub upstream: &'a CompiledUpstream,
}

/// Why a request goes nowhere, and is answered here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum Rejection {
    /// The host cannot be told.
    #[error(transparent)]
    Host(#[from] HostError),
    /// The path has no normal form.
    #[error(transparent)]
    Path(#[from] NormaliseError),
    /// The normalised target is not one the `http` crate takes. Not known to happen.
    #[error("request target cannot be rewritten")]
    Target,
    /// No rule is for this request.
    #[error("no route")]
    NoRoute,
    /// The rule has no backend with a share of its requests.
    #[error("no backend")]
    NoBackend,
}

impl Rejection {
    /// The status to answer with: 400 for a request that cannot be read in one way only,
    /// 404 for one that no rule is for, and 500 for a rule with nowhere to send it, as
    /// Gateway API asks.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Host(_) | Self::Path(_) | Self::Target => StatusCode::BAD_REQUEST,
            Self::NoRoute => StatusCode::NOT_FOUND,
            Self::NoBackend => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

/// Decides where a request that came in on `listener` goes, and makes its head what the
/// upstream is to see: the normalised path, a `Host` field that names the host that was
/// routed on, and the rule's changes to the headers.
///
/// `random` chooses among weighted backends and should be uniform over `u64`; passing it
/// in keeps the core deterministic. A request in the usual form — origin-form target, a
/// path already normal — is decided without allocating, except as the rule's header
/// changes do.
///
/// # Errors
///
/// Returns a [`Rejection`] for a request to answer locally; the head is then unchanged.
pub fn decide<'a>(
    snapshot: &'a Compiled,
    listener: &CompiledListener,
    head: &mut Parts,
    random: u64,
) -> Result<Forward<'a>, Rejection> {
    let host = match head.uri.authority() {
        Some(authority) => bare_host(authority.as_str())?,
        None => bare_host(host_field(&head.headers)?)?,
    };
    let path = normalise_path(head.uri.path())?;
    let request = RequestParts {
        host,
        path: &path,
        query: head.uri.query().unwrap_or_default(),
        method: &head.method,
        headers: &head.headers,
    };
    let id = *listener.router.route(&request).ok_or(Rejection::NoRoute)?;
    // A router only ever yields rules of the snapshot it was compiled into.
    let rule = snapshot.rule(id).ok_or(Rejection::NoBackend)?;
    let upstream = rule
        .backends
        .pick(random)
        .and_then(|id| snapshot.upstream(id))
        .ok_or(Rejection::NoBackend)?;

    // Everything that can fail comes before the first change to the head.
    let target = match path {
        Cow::Borrowed(_) => None,
        Cow::Owned(path) => Some(with_path(&head.uri, path)?),
    };
    let host_field = match head.uri.authority() {
        Some(authority) if !names_host(head, authority.as_str()) => {
            Some(HeaderValue::from_str(authority.as_str()).map_err(|_| HostError::Invalid)?)
        }
        _ => None,
    };

    if let Some(target) = target {
        head.uri = target;
    }
    if let Some(host_field) = host_field {
        head.headers.insert(HOST, host_field);
    }
    if let Some(changes) = &rule.request_headers {
        changes.apply(&mut head.headers);
    }
    Ok(Forward { rule, upstream })
}

/// Whether the `Host` field already says what the target's authority says.
fn names_host(head: &Parts, authority: &str) -> bool {
    let mut fields = head.headers.get_all(HOST).iter();
    fields.next().is_some_and(|field| field == authority) && fields.next().is_none()
}

/// The same target with another path.
fn with_path(uri: &Uri, path: String) -> Result<Uri, Rejection> {
    let mut target = path;
    if let Some(query) = uri.query() {
        target.push('?');
        target.push_str(query);
    }
    let mut parts = uri.clone().into_parts();
    parts.path_and_query = Some(PathAndQuery::try_from(target).map_err(|_| Rejection::Target)?);
    Uri::from_parts(parts).map_err(|_| Rejection::Target)
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Config, compile};
    use http::{Method, Request};
    use proptest::prelude::*;

    const SHOP: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http }
  admin: { address: "[::]:9090", protocol: http }
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
        backends:
          - { upstream: cart, weight: 9 }
          - { upstream: cart-canary, weight: 1 }
      - matches:
          - path: { exact: /closed }
        backends:
          - { upstream: cart, weight: 0 }
      - matches:
          - path: { prefix: /search }
            query: [{ name: q, value: { exact: "a b" } }]
        backends:
          - { upstream: search, weight: 1 }
  - name: everything-else
    listeners: [web]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: / }
        backends:
          - { upstream: fallback, weight: 1 }
  - name: admin
    listeners: [admin]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: /status }
        backends:
          - { upstream: admin, weight: 1 }
upstreams:
  admin: { endpoints: ["127.0.0.1:9001"] }
  cart: { endpoints: ["127.0.0.1:9002"] }
  cart-canary: { endpoints: ["127.0.0.1:9003"] }
  fallback: { endpoints: [] }
  search: { endpoints: ["127.0.0.1:9004"] }
"#;

    fn shop() -> Compiled {
        let config: Config = serde_saphyr::from_str(SHOP).unwrap();
        compile(&config).unwrap()
    }

    fn head(target: &str, fields: &[(&str, &str)]) -> Parts {
        let mut request = Request::builder().method(Method::GET).uri(target);
        for (name, value) in fields {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    /// Decides on the named listener; the name of the upstream, or the rejection.
    fn decide_on(listener: &str, head: &mut Parts, random: u64) -> Result<String, Rejection> {
        let shop = shop();
        let listener = shop.listeners.iter().find(|l| l.name == listener).unwrap();
        decide(&shop, listener, head, random).map(|forward| forward.upstream.name.clone())
    }

    fn upstream_for(target: &str, fields: &[(&str, &str)]) -> Result<String, Rejection> {
        decide_on("web", &mut head(target, fields), 0)
    }

    #[test]
    fn the_host_field_and_the_path_lead_to_a_rule_and_its_upstream() {
        let upstream = upstream_for("/cart/items", &[("host", "shop.example.com")]);
        assert_eq!(upstream.as_deref(), Ok("cart"));
        let upstream = upstream_for("/about", &[("host", "shop.example.com")]);
        assert_eq!(upstream.as_deref(), Ok("fallback"));
    }

    #[test]
    fn port_trailing_dot_and_letter_case_do_not_make_another_host() {
        for host in [
            "shop.example.com:8080",
            "shop.example.com.",
            "SHOP.Example.Com.:80",
        ] {
            let upstream = upstream_for("/cart", &[("host", host)]);
            assert_eq!(upstream.as_deref(), Ok("cart"), "{host}");
        }
    }

    #[test]
    fn the_random_number_chooses_among_weighted_backends() {
        for (random, upstream) in [(0, "cart"), (8, "cart"), (9, "cart-canary"), (10, "cart")] {
            let mut head = head("/cart", &[("host", "shop.example.com")]);
            assert_eq!(decide_on("web", &mut head, random).as_deref(), Ok(upstream));
        }
    }

    #[test]
    fn a_listener_has_only_the_routes_that_are_for_it() {
        let mut on_admin = head("/status", &[("host", "shop.example.com")]);
        assert_eq!(decide_on("admin", &mut on_admin, 0).as_deref(), Ok("admin"));
        let mut on_admin = head("/cart", &[("host", "shop.example.com")]);
        assert_eq!(
            decide_on("admin", &mut on_admin, 0),
            Err(Rejection::NoRoute)
        );
    }

    #[test]
    fn the_forward_carries_the_rule() {
        let shop = shop();
        let web = shop.listeners.iter().find(|l| l.name == "web").unwrap();
        let mut head = head("/cart", &[("host", "shop.example.com")]);
        let forward = decide(&shop, web, &mut head, 0).unwrap();
        assert!(forward.rule.request_headers.is_some());
        assert_eq!(
            forward.upstream.endpoints,
            ["127.0.0.1:9002".parse().unwrap()]
        );
    }

    #[test]
    fn the_host_of_the_target_is_used_and_the_host_field_made_to_agree() {
        // An HTTP/2 request: `:authority` and no `Host` field.
        let mut h2 = head("http://shop.example.com/cart", &[]);
        assert_eq!(decide_on("web", &mut h2, 0).as_deref(), Ok("cart"));
        assert_eq!(
            h2.headers.get_all(HOST).iter().collect::<Vec<_>>(),
            ["shop.example.com"]
        );

        // Absolute form with a `Host` field that says something else: RFC 9112 §3.2.2.
        let mut absolute = head(
            "http://shop.example.com:8080/cart",
            &[("host", "other.example.org")],
        );
        assert_eq!(decide_on("web", &mut absolute, 0).as_deref(), Ok("cart"));
        assert_eq!(
            absolute.headers.get_all(HOST).iter().collect::<Vec<_>>(),
            ["shop.example.com:8080"]
        );
        assert_eq!(absolute.uri, "http://shop.example.com:8080/cart");

        // Two fields, of which one agrees, are still made one.
        let mut twice = head(
            "http://shop.example.com/cart",
            &[("host", "shop.example.com"), ("host", "other.example.org")],
        );
        assert_eq!(decide_on("web", &mut twice, 0).as_deref(), Ok("cart"));
        assert_eq!(
            twice.headers.get_all(HOST).iter().collect::<Vec<_>>(),
            ["shop.example.com"]
        );
    }

    #[test]
    fn a_request_without_a_host_that_can_be_told_is_rejected() {
        assert_eq!(
            upstream_for("/cart", &[]),
            Err(Rejection::Host(HostError::Missing))
        );
        assert_eq!(
            upstream_for(
                "/cart",
                &[("host", "shop.example.com"), ("host", "shop.example.com")]
            ),
            Err(Rejection::Host(HostError::Repeated))
        );
        assert_eq!(
            upstream_for("/cart", &[("host", "user@shop.example.com")]),
            Err(Rejection::Host(HostError::Invalid))
        );
        assert_eq!(
            upstream_for(
                "http://user@shop.example.com/cart",
                &[("host", "shop.example.com")]
            ),
            Err(Rejection::Host(HostError::Invalid))
        );
    }

    #[test]
    fn a_path_without_a_normal_form_is_rejected() {
        let host = [("host", "shop.example.com")];
        assert_eq!(
            upstream_for("/cart/..%2Fadmin", &host),
            Err(Rejection::Path(NormaliseError::EncodedSeparator))
        );
        assert_eq!(
            upstream_for("/../cart", &host),
            Err(Rejection::Path(NormaliseError::AboveRoot))
        );
        // Asterisk form (`OPTIONS *`) and authority form (`CONNECT`) have no path.
        assert_eq!(
            upstream_for("*", &host),
            Err(Rejection::Path(NormaliseError::NotAbsolute))
        );
        let mut connect = head("shop.example.com:443", &[]);
        connect.method = Method::CONNECT;
        assert_eq!(
            decide_on("web", &mut connect, 0),
            Err(Rejection::Path(NormaliseError::NotAbsolute))
        );
    }

    #[test]
    fn the_normal_form_of_the_path_is_matched_and_forwarded() {
        let mut head = head(
            "/shop/../cart//items/%61?next=/a/../b&x=%61",
            &[("host", "shop.example.com")],
        );
        assert_eq!(decide_on("web", &mut head, 0).as_deref(), Ok("cart"));
        assert_eq!(head.uri, "/cart/items/a?next=/a/../b&x=%61");

        let mut absolute = self::head("http://shop.example.com/./cart", &[]);
        assert_eq!(decide_on("web", &mut absolute, 0).as_deref(), Ok("cart"));
        assert_eq!(absolute.uri, "http://shop.example.com/cart");
    }

    #[test]
    fn a_target_in_normal_form_is_left_as_it_came() {
        let mut head = head("/cart/items?x=%61&y", &[("host", "shop.example.com")]);
        assert_eq!(decide_on("web", &mut head, 0).as_deref(), Ok("cart"));
        assert_eq!(head.uri, "/cart/items?x=%61&y");
    }

    #[test]
    fn the_query_is_routed_on() {
        let host = [("host", "shop.example.com")];
        assert_eq!(
            upstream_for("/search?q=a+b", &host).as_deref(),
            Ok("search")
        );
        assert_eq!(
            upstream_for("/search?q=ab", &host).as_deref(),
            Ok("fallback")
        );
    }

    #[test]
    fn the_rule_changes_the_request_headers() {
        let mut head = head(
            "/cart",
            &[
                ("host", "shop.example.com"),
                ("x-debug", "1"),
                ("x-gateway", "spoofed"),
            ],
        );
        assert_eq!(decide_on("web", &mut head, 0).as_deref(), Ok("cart"));
        assert_eq!(head.headers.get("x-gateway").unwrap(), "edgerush");
        assert!(!head.headers.contains_key("x-debug"));
        assert_eq!(head.headers.get(HOST).unwrap(), "shop.example.com");
    }

    #[test]
    fn a_request_no_rule_is_for_is_rejected() {
        let mut request = head("/other", &[("host", "shop.example.com")]);
        assert_eq!(decide_on("admin", &mut request, 0), Err(Rejection::NoRoute));
        assert_eq!(
            upstream_for("/cart", &[("host", "[::1]:8080")]).as_deref(),
            Ok("fallback")
        );
    }

    #[test]
    fn a_rule_with_nowhere_to_go_is_rejected() {
        let upstream = upstream_for("/closed", &[("host", "shop.example.com")]);
        assert_eq!(upstream, Err(Rejection::NoBackend));
    }

    #[test]
    fn a_rejected_request_keeps_its_head() {
        let mut head = head(
            "http://shop.example.com/./closed",
            &[("host", "other.example.org")],
        );
        assert_eq!(decide_on("web", &mut head, 0), Err(Rejection::NoBackend));
        assert_eq!(head.uri, "http://shop.example.com/./closed");
        assert_eq!(head.headers.get(HOST).unwrap(), "other.example.org");
    }

    #[test]
    fn every_rejection_has_its_status() {
        assert_eq!(Rejection::Host(HostError::Missing).status(), 400);
        assert_eq!(Rejection::Path(NormaliseError::Backslash).status(), 400);
        assert_eq!(Rejection::Target.status(), 400);
        assert_eq!(Rejection::NoRoute.status(), 404);
        assert_eq!(Rejection::NoBackend.status(), 500);
    }

    /// Targets the `http` crate takes, with paths that often need normalising.
    fn target() -> impl Strategy<Value = Uri> {
        let piece = prop_oneof![
            4 => "[a-zA-Z0-9]{1,3}",
            4 => Just("/".to_owned()),
            2 => Just(".".to_owned()),
            2 => Just("..".to_owned()),
            1 => "%[0-7][0-9a-fA-F]",
            1 => "[!-~]",
        ];
        let path = prop::collection::vec(piece, 0..10).prop_map(|pieces| pieces.concat());
        let origin = prop_oneof![Just(""), Just("http://shop.example.com")];
        let query = prop_oneof![Just(""), Just("?"), Just("?a=/../%2f&b")];
        (origin, path, query).prop_filter_map("not a target", |(origin, path, query)| {
            Uri::try_from(format!("{origin}/{path}{query}")).ok()
        })
    }

    proptest! {
        /// The path that is forwarded is the normal form of the one that came, and nothing
        /// else about the target changes: rewriting never fails on what normalising yields.
        #[test]
        fn whatever_is_forwarded_has_the_normal_path_and_the_rest_as_it_came(target in target()) {
            let mut head = head("/", &[("host", "shop.example.com")]);
            head.uri = target.clone();
            match (decide_on("web", &mut head, 0), normalise_path(target.path())) {
                (Ok(_), Ok(normal)) => {
                    prop_assert_eq!(head.uri.path(), &*normal);
                    prop_assert_eq!(head.uri.query(), target.query());
                    prop_assert_eq!(head.uri.scheme(), target.scheme());
                    prop_assert_eq!(head.uri.authority(), target.authority());
                }
                (Err(Rejection::Path(theirs)), Err(ours)) => prop_assert_eq!(theirs, ours),
                (decided, normal) => prop_assert!(false, "{target}: {decided:?}, {normal:?}"),
            }
        }
    }
}
