//! The request core: from the head of a request to where it goes, whatever protocol it
//! came in by. It sees plain `http` types and a compiled config, never the HTTP engine;
//! HTTP/1.1, HTTP/2 and later HTTP/3 are adapters around it.
//!
//! The host is the one the request target names (HTTP/2's `:authority`, HTTP/1.1's
//! absolute form) and only without one the `Host` field, as RFC 9112 §3.2.2 has it; when
//! the target names it, the `Host` field is made to say the same before anything is
//! matched, so that a request has one host for every predicate and for the upstream. The
//! path is normalised once, and the same normal form is matched and forwarded. Whatever is
//! routed on is what the upstream gets to see — the head is rewritten to say it — unless the
//! rule rewrites it, after routing, to another path or host. A rule that redirects is
//! answered with a `Location` made from what was routed on. What the
//! request said about the connection it came in on is taken off ([`crate::hop_by_hop`])
//! before the rule's own changes to the headers. A cookie string that came in pieces, as
//! HTTP/2 allows, is put together before anything looks at it ([`crate::cookies`]). What
//! the upstream is told of the client — `X-Forwarded-*` and `Via` — is made before routing,
//! so that a rule reads what the upstream will ([`crate::forwarding`]).

use crate::forwarding::{Client, forward};
use crate::head::Head;
use crate::hop_by_hop::ConnectionError;
use crate::host::{HostError, bare_host};
use edgerush_config::{
    Compiled, CompiledListener, CompiledRule, Outcome, Protocol, Step, UpstreamId,
};
use edgerush_filters::{Requested, Scheme, UrlRewrite};
use edgerush_router::{NormaliseError, RequestParts, normalise_path};
use http::header::HeaderMap;
use http::uri::PathAndQuery;
use http::{HeaderValue, Method, StatusCode, Uri, Version};
use std::borrow::Cow;
use std::sync::Arc;

/// What becomes of a request.
#[derive(Debug, Clone)]
pub enum Decision<'a> {
    /// It goes to an upstream.
    Forward(Forward<'a>),
    /// It is answered with a redirect, and nothing goes upstream.
    Redirect(Redirected<'a>),
}

/// A request answered with a redirect.
#[derive(Debug, Clone)]
pub struct Redirected<'a> {
    /// The rule the request belongs to, for its changes to the answer's headers.
    pub rule: &'a Arc<CompiledRule>,
    /// The status to answer with.
    pub status: StatusCode,
    /// Where the client is sent.
    pub location: HeaderValue,
}

/// Where a request goes.
#[derive(Debug, Clone)]
pub struct Forward<'a> {
    /// The rule the request belongs to, for what is still to be done to its response. A
    /// request that is still under way when the config changes keeps its rule, and only
    /// its rule, alive.
    pub rule: &'a Arc<CompiledRule>,
    /// The position of the rule's route, as a [`RuleId`](edgerush_config::RuleId) gives it.
    pub route: usize,
    /// The upstream chosen among the rule's backends: its position in the snapshot's list.
    pub upstream: UpstreamId,
    /// The rule's mirrors that take a copy of this request, in the rule's order. None of
    /// them is sent one for a WebSocket handshake, which a mirror could only ever be sent
    /// the handshake of, never the messages; each is drawn all the same, so that what it
    /// did not get can be counted ([19 §5](../../../docs/19-websocket.md)).
    pub mirrors: Vec<Mirroring>,
    /// How a WebSocket's opening handshake came, for a request the gateway carries as one
    /// ([19 §2, §3](../../../docs/19-websocket.md)). An HTTP/1.1 one's `Upgrade` has been
    /// taken off with the other hop-by-hop fields all the same; what goes upstream in their
    /// place is for whoever sends it.
    pub websocket: Option<Opening>,
}

/// A WebSocket's opening handshake, as its client sent it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Opening {
    /// HTTP/1.1's upgrade (RFC 6455 §4), with the `Sec-WebSocket-Key` the client sent.
    Upgrade(HeaderValue),
    /// An extended CONNECT for `websocket` over HTTP/2 or HTTP/3 (RFC 8441, RFC 9220),
    /// which has no key.
    Connect,
}

/// A mirror that takes a copy of a request.
#[derive(Debug, Clone)]
pub struct Mirroring {
    /// Its position among the rule's mirrors.
    pub mirror: usize,
    /// The request as it stood at the mirror's place, before a change after it; none for a
    /// mirror with no change after it, which is sent the request as it goes upstream.
    pub own: Option<Copied>,
}

/// A request as it stood at a mirror's place among its rule's filters.
#[derive(Debug, Clone)]
pub struct Copied {
    /// Its target.
    pub target: Uri,
    /// Its fields.
    pub fields: HeaderMap,
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
    /// The `Connection` header is not one to act on.
    #[error(transparent)]
    Connection(#[from] ConnectionError),
    /// The normalised target is not one the `http` crate takes. Not known to happen.
    #[error("request target cannot be rewritten")]
    Target,
    /// No rule is for this request.
    #[error("no route")]
    NoRoute,
    /// The rule has no backend with a share of its requests.
    #[error("no backend")]
    NoBackend,
    /// The head could not take the changes asked of it: more fields added than its edits
    /// hold. Not reachable from a config the gateway accepts (14 §6).
    #[error("request head cannot take its changes")]
    Edits,
    /// An extended CONNECT for a protocol other than WebSocket, which is all the gateway
    /// carries (RFC 9220 §3: a server "SHOULD respond ... with a 501").
    #[error("extended CONNECT for a protocol not carried")]
    Protocol,
}

impl Rejection {
    /// The status to answer with: 400 for a request that cannot be read in one way only,
    /// 404 for one that no rule is for, and 500 for a rule with nowhere to send it, as
    /// Gateway API asks.
    #[must_use]
    pub fn status(&self) -> StatusCode {
        match self {
            Self::Host(_) | Self::Path(_) | Self::Connection(_) | Self::Target => {
                StatusCode::BAD_REQUEST
            }
            Self::NoRoute => StatusCode::NOT_FOUND,
            Self::NoBackend | Self::Edits => StatusCode::INTERNAL_SERVER_ERROR,
            Self::Protocol => StatusCode::NOT_IMPLEMENTED,
        }
    }
}

/// Decides where a request from `client` that came in on `listener` goes, and makes its
/// head what the upstream is to see: the normalised path, a `Host` field that names the host
/// that was routed on, the cookie string in one piece, what the upstream is told of the
/// client, no hop-by-hop headers, and the rule's changes to the headers, and the rule's rewrite of path and host, which is made from the
/// normalised path and never routes again. The rule's changes are made in the order its
/// filters are written, and a mirror of the rule's placed before one of them takes a copy
/// of the request as it stands there. A request its rule redirects is answered with
/// the `Location` made from
/// what was routed on — the listener's scheme, the host, the normalised path, the query —
/// and its head is left as routing left it.
///
/// `random` is drawn from once to choose among weighted backends, and once for each of
/// the rule's mirrors, whether it takes this request; it should be uniform over `u64`, and
/// passing it in keeps the core deterministic. A request in the usual form — origin-form
/// target, a path already normal — is decided without allocating, except as the rule's
/// header changes do, and a copy for a mirror placed before one.
///
/// `id` is the request's ID, if its listener gives one (08 §3): the upstream is told it in
/// place of any the request came with, and a `Connection` that names it is refused.
///
/// # Errors
///
/// Returns a [`Rejection`] for a request to answer locally. Its target is then as it came;
/// of its headers, a cookie string that came in pieces may be whole, the `Host` field may
/// have been made to agree with the target, and what the upstream is told of the client
/// may have been made, as all three are before anything is matched.
pub fn decide<'a, H: Head>(
    snapshot: &'a Compiled,
    listener: &CompiledListener,
    head: &mut H,
    client: &Client,
    random: &mut impl FnMut() -> u64,
    id: Option<&HeaderValue>,
) -> Result<Decision<'a>, Rejection> {
    let found = head.survey();
    // An extended CONNECT is for WebSocket or for nothing the gateway carries: it cannot be
    // served as a plain request, as an HTTP/1 upgrade can, since without its protocol it
    // asks for nothing (19 §4).
    let connect = match head.protocol() {
        Some(protocol) if head.method() == Method::CONNECT => {
            if !protocol.eq_ignore_ascii_case("websocket") {
                return Err(Rejection::Protocol);
            }
            true
        }
        _ => false,
    };
    if found.cookie_fields > 1 {
        // Before routing, so that rules and the upstream read the same cookie string.
        head.join_cookies()?;
    }
    // A request has one host, for everything that looks at it: the router's hostnames, a
    // rule's predicate on the `Host` header, and the upstream. When the target names it,
    // the `Host` field is made to say the same before anything is matched.
    // A bare host is the front of what names it, so its length is all that is kept of
    // it while the `Host` field is made to agree: the target does not change meanwhile.
    let named = match head.uri().authority() {
        Some(authority) => Some(bare_host(authority.as_str())?.len()),
        None => None,
    };
    // Before the target's host takes the `Host` field's place, the field an HTTP/1 request
    // came with is held to what RFC 9112 §3.2 asks of it: one field line, holding a host,
    // and one there at all from HTTP/1.1. Replaced first, a second field or an invalid one
    // would be gone before anything could refuse it. A request in origin-form has its field
    // read for routing below, and HTTP/2 carries its host as `:authority`.
    if named.is_some() && matches!(head.version(), Version::HTTP_10 | Version::HTTP_11) {
        match head.host_field() {
            Ok(field) => {
                bare_host(field)?;
            }
            Err(HostError::Missing) if head.version() == Version::HTTP_10 => {}
            Err(error) => return Err(error.into()),
        }
    }
    head.agree_host()?;
    // Before routing, so that a rule's predicates read what the upstream will be told, and
    // before the host is borrowed from the head for routing.
    forward(head, listener, client, id)?;
    let host = match (named, head.uri().authority()) {
        (Some(length), Some(authority)) => {
            authority.as_str().get(..length).ok_or(HostError::Invalid)?
        }
        _ => bare_host(head.host_field()?)?,
    };
    let path = normalise_path(head.uri().path())?;
    if found.hop_by_hop {
        head.check_connection(id.is_some())?;
    }
    // Read before the hop-by-hop fields come off, as they must for everything else: an
    // `Upgrade` is one, and the only WebSocket the gateway carries says it (19 §2).
    let websocket = if connect {
        Some(Opening::Connect)
    } else if found.hop_by_hop {
        crate::websocket::handshake(head.version(), head.method(), &head.fields())
            .map(|key| Opening::Upgrade(key.value()))
    } else {
        None
    };
    // What routing reads of the head is let go of before anything in it changes.
    let id = {
        let fields = head.fields();
        let request = RequestParts {
            host,
            path: &path,
            query: head.uri().query().unwrap_or_default(),
            method: head.method(),
            headers: &fields,
        };
        *listener.router.route(&request).ok_or(Rejection::NoRoute)?
    };
    // A router only ever yields rules of the snapshot it was compiled into.
    let rule = snapshot.rule(id).ok_or(Rejection::NoBackend)?;
    let backends = match &rule.outcome {
        Outcome::Forward { backends, .. } => backends,
        Outcome::Redirect(redirect) => {
            let scheme = match listener.protocol {
                Protocol::Https => Scheme::Https,
                Protocol::Http | Protocol::Tcp | Protocol::Tls => Scheme::Http,
            };
            let location = redirect
                .location(&Requested {
                    scheme,
                    host,
                    path: &path,
                    query: head.uri().query(),
                })
                .map_err(|_| Rejection::Target)?;
            return Ok(Decision::Redirect(Redirected {
                rule,
                status: redirect.status(),
                location,
            }));
        }
    };
    let upstream = backends.pick(random()).ok_or(Rejection::NoBackend)?;
    // Which of the rule's mirrors take this request, drawn here, where what each is sent
    // is made: only a mirror that takes it has a copy made. A rule without mirrors draws
    // nothing and holds nothing for them.
    let mut mirrors = Vec::new();
    if !rule.mirrors.is_empty() {
        for (at, mirror) in rule.mirrors.iter().enumerate() {
            if mirror.takes(random()) {
                mirrors.push(Mirroring {
                    mirror: at,
                    own: None,
                });
            }
        }
    }

    // Whatever can still fail comes before the target and the rest of the headers change.
    // A query with no path before it (`http://a?q=1`) is read as the path `/`, and is
    // given it: origin-form has no target without a path (RFC 9112 §3.2.1).
    let pathless = head
        .uri()
        .path_and_query()
        .is_some_and(|target| target.as_str().starts_with('?'));
    // A rewrite's path is made from the normalised one routed on, and is normal itself.
    let mut rewritten = rule
        .rewrite
        .as_ref()
        .and_then(UrlRewrite::path)
        .map(|change| {
            let mut rewritten = String::with_capacity(path.len());
            change.write(&path, &mut rewritten);
            with_path(head.uri(), rewritten)
        })
        .transpose()?;
    // The normalised target is what goes upstream unless a rewrite takes its place; with
    // one, it is made only for a copy that stands before the rewrite.
    let copied_before_rewrite = || {
        rule.steps
            .iter()
            .take_while(|step| **step != Step::Rewrite)
            .any(|step| {
                matches!(step, Step::Copy(at) if mirrors.iter().any(|taken| taken.mirror == *at))
            })
    };
    let target = if rewritten.is_none() || copied_before_rewrite() {
        match path {
            Cow::Owned(path) => Some(with_path(head.uri(), path)?),
            Cow::Borrowed(path) if pathless => Some(with_path(head.uri(), path.to_owned())?),
            Cow::Borrowed(_) => None,
        }
    } else {
        None
    };

    // What every copy has: the normal target, no hop-by-hop headers.
    if let Some(target) = target {
        head.set_uri(target);
    }
    if found.hop_by_hop {
        head.strip_request()?;
    }
    // A request's trailers go no further than the gateway (03 §11), and nor does the
    // declaration of what they would hold.
    if found.trailer {
        head.remove_where(|name| name.eq_ignore_ascii_case(b"trailer"))?;
    }
    // Then the rule's changes, in its order, and the copies made between them.
    for step in &rule.steps {
        match *step {
            Step::Rewrite => {
                if let Some(target) = rewritten.take() {
                    head.set_uri(target);
                }
                if let Some(host) = rule.rewrite.as_ref().and_then(UrlRewrite::host) {
                    head.set_host(host)?;
                }
            }
            Step::RequestHeaders => {
                if let Some(changes) = &rule.request_headers {
                    head.apply(changes)?;
                }
            }
            Step::Copy(at) if websocket.is_none() => {
                if let Some(taken) = mirrors.iter_mut().find(|taken| taken.mirror == at) {
                    taken.own = Some(Copied {
                        target: head.uri().clone(),
                        fields: head.to_map(),
                    });
                }
            }
            // A handshake's mirrors are sent nothing, so nothing is copied for them.
            Step::Copy(_) => {}
        }
    }
    Ok(Decision::Forward(Forward {
        rule,
        route: id.route,
        upstream,
        mirrors,
        websocket,
    }))
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
    use crate::head::survey;
    use crate::raw::RawHead;
    use edgerush_config::{Config, compile};
    use edgerush_router::Fields;
    use http::header::{HOST, HeaderMap, HeaderValue};
    use http::request::Parts;
    use http::{Method, Request};
    use proptest::prelude::*;

    /// A client that is no trusted proxy.
    fn peer() -> Client {
        Client::new("203.0.113.7".parse().unwrap())
    }

    const SHOP: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: ["10.0.0.0/8"], trusted_only_headers: [Forwarded, X-Real-IP, "X-Forwarded-*"] }, request_id: generate }
  admin: { address: "[::]:9090", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
  secure: { address: "[::]:8443", protocol: https, proxy_protocol: off, tls: { certificates: [secure] }, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: shop
    listeners: [web, secure]
    hostnames:
      - { name: shop.example.com, falls_through: true }
    rules:
      - matches:
          - path: { prefix: /cart }
        filters:
          - type: request_header_modifier
            set: [{ name: X-Gateway, value: edgerush }]
            remove: [x-debug]
        forward:
          backends:
            - { upstream: cart, weight: 9 }
            - { upstream: cart-canary, weight: 1 }
      - matches:
          - path: { exact: /closed }
        forward:
          backends:
            - { upstream: cart, weight: 0 }
      - matches:
          - path: { prefix: /search }
            query: [{ name: q, value: { exact: "a b" } }]
        forward:
          backends:
            - { upstream: search, weight: 1 }
      - matches:
          - path: { prefix: /account }
            headers: [{ name: Cookie, value: { exact: "a=1; b=2" } }]
        forward:
          backends:
            - { upstream: search, weight: 1 }
      - matches:
          - path: { prefix: /plain }
            headers: [{ name: X-Forwarded-Proto, value: { exact: http } }]
        forward:
          backends:
            - { upstream: search, weight: 1 }
      - matches:
          - path: { prefix: /told }
        filters:
          - type: request_header_modifier
            set: [{ name: X-Forwarded-For, value: 192.0.2.1 }]
            remove: [via]
        forward:
          backends:
            - { upstream: search, weight: 1 }
      - matches:
          - path: { prefix: /v1 }
        filters:
          - { type: url_rewrite, host: api.internal, path: { replace_prefix: /api } }
        forward:
          backends:
            - { upstream: search, weight: 1 }
  - name: everything-else
    listeners: [web]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: /tenant }
            headers: [{ name: Host, value: { exact: tenant.example.net } }]
        forward:
          backends:
            - { upstream: search, weight: 1 }
      - matches:
          - path: { prefix: / }
        forward:
          backends:
            - { upstream: fallback, weight: 1 }
  - name: admin
    listeners: [admin]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: /status }
        forward:
          backends:
            - { upstream: admin, weight: 1 }
upstreams:
  admin: { load_balancer: p2c, endpoints: ["127.0.0.1:9001"] }
  cart: { load_balancer: p2c, endpoints: ["127.0.0.1:9002"] }
  cart-canary: { load_balancer: p2c, endpoints: ["127.0.0.1:9003"] }
  fallback: { load_balancer: p2c, endpoints: [] }
  search: { load_balancer: p2c, endpoints: ["127.0.0.1:9004"] }
"#;

    fn shop() -> Compiled {
        compile(&shop_config(SHOP)).unwrap()
    }

    /// The config `yaml` states, with the certificate its `secure` listener names.
    fn shop_config(yaml: &str) -> Config {
        let mut config: Config = serde_saphyr::from_str(yaml).unwrap();
        let secure = edgerush_config::Certificate {
            chain: "C".to_owned(),
            key: "K".to_owned(),
        };
        config.certificates.insert("secure".to_owned(), secure);
        config
    }

    fn head(target: &str, fields: &[(&str, &str)]) -> Parts {
        let mut request = Request::builder().method(Method::GET).uri(target);
        for (name, value) in fields {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    /// Where a request forwarded goes.
    fn forwarding(decision: Decision<'_>) -> Forward<'_> {
        match decision {
            Decision::Forward(forward) => forward,
            Decision::Redirect(redirected) => panic!("redirected to {:?}", redirected.location),
        }
    }

    /// Decides on the named listener; the name of the upstream, or the rejection.
    fn decide_on<H: Head>(listener: &str, head: &mut H, random: u64) -> Result<String, Rejection> {
        decide_from(&peer(), listener, head, random)
    }

    /// The same, for a request from `client`.
    fn decide_from<H: Head>(
        client: &Client,
        listener: &str,
        head: &mut H,
        random: u64,
    ) -> Result<String, Rejection> {
        let shop = shop();
        let listener = shop.listeners.iter().find(|l| l.name == listener).unwrap();
        decide(&shop, listener, head, client, &mut || random, None).map(|decision| {
            let forward = forwarding(decision);
            shop.upstream(forward.upstream).unwrap().name.clone()
        })
    }

    fn upstream_for(target: &str, fields: &[(&str, &str)]) -> Result<String, Rejection> {
        decide_on("web", &mut head(target, fields), 0)
    }

    /// Every value of `name`, as text.
    fn values_of(fields: &impl Fields, name: &str) -> Vec<String> {
        fields
            .values(&http::HeaderName::from_bytes(name.as_bytes()).unwrap())
            .map(|value| String::from_utf8(value.to_vec()).unwrap())
            .collect()
    }

    /// A client the `web` listener trusts to say who its own clients are.
    fn load_balancer() -> Client {
        Client::new("10.1.0.5".parse().unwrap())
    }

    /// What a client says of forwarding, and what it may not.
    const SAID: &[u8] = b"GET /cart HTTP/1.1\r\nHost: shop.example.com\r\nX-Forwarded-For: 10.0.0.1\r\nX-Forwarded-Proto: https\r\nX-Forwarded-Host: admin.example.com\r\nForwarded: for=10.0.0.1\r\nX-Real-IP: 10.0.0.1\r\nX-Forwarded-Port: 443\r\nX-Forwarded-Prefix: /admin\r\nx_forwarded_for: 10.0.0.2\r\nVia: 1.0 cdn\r\nAccept: */*\r\n\r\n";

    /// A peer that is no trusted proxy is the client. What it said of forwarding is not
    /// passed on: `X-Forwarded-For`, `-Proto` and `-Host` say the gateway's own, and the
    /// listener's trusted-only headers go. `Via` has the gateway's entry after the client's.
    /// A name spelled otherwise (`x_forwarded_for`) is another header, left as it is. Both
    /// kinds of head come to the same.
    #[test]
    fn an_untrusted_peer_is_the_client_and_what_it_says_of_forwarding_is_not_passed_on() {
        let (mut map, mut raw) = both_heads(SAID).unwrap();
        assert_eq!(decide_on("web", &mut map, 0).as_deref(), Ok("cart"));
        assert_eq!(decide_on("web", &mut raw, 0).as_deref(), Ok("cart"));
        let edited = raw.fields();
        for (name, expected) in [
            ("x-forwarded-for", vec!["203.0.113.7"]),
            ("x-forwarded-proto", vec!["http"]),
            ("x-forwarded-host", vec!["shop.example.com"]),
            ("via", vec!["1.0 cdn", "1.1 edgerush"]),
            ("forwarded", vec![]),
            ("x-real-ip", vec![]),
            ("x-forwarded-port", vec![]),
            ("x-forwarded-prefix", vec![]),
            ("x_forwarded_for", vec!["10.0.0.2"]),
            ("accept", vec!["*/*"]),
        ] {
            assert_eq!(values_of(&map.headers, name), expected, "{name}, map");
            assert_eq!(values_of(&edited, name), expected, "{name}, raw");
        }

        // With no trusted-only headers, the gateway's own three are still its own.
        let mut request = head(
            "/status",
            &[
                ("host", "status.example.com:9090"),
                ("x-forwarded-for", "10.0.0.1"),
                ("x-forwarded-proto", "https"),
                ("x-forwarded-host", "admin.example.com"),
                ("x-forwarded-port", "443"),
            ],
        );
        assert_eq!(decide_on("admin", &mut request, 0).as_deref(), Ok("admin"));
        assert_eq!(
            values_of(&request.headers, "x-forwarded-for"),
            ["203.0.113.7"]
        );
        assert_eq!(values_of(&request.headers, "x-forwarded-proto"), ["http"]);
        // As the client wrote it, the port with it.
        assert_eq!(
            values_of(&request.headers, "x-forwarded-host"),
            ["status.example.com:9090"]
        );
        assert_eq!(values_of(&request.headers, "x-forwarded-port"), ["443"]);
    }

    /// An ID a listener that generates them gave a request.
    const ID_TEXT: &str = "0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f";
    /// The same, as a header's value.
    const ID: HeaderValue = HeaderValue::from_static(ID_TEXT);

    /// Decides on the named listener of `snapshot`, the request given `id`; the name of the
    /// upstream, or the rejection.
    fn decide_with_id<H: Head>(
        snapshot: &Compiled,
        client: &Client,
        listener: &str,
        head: &mut H,
        id: Option<&HeaderValue>,
    ) -> Result<String, Rejection> {
        let listener = snapshot
            .listeners
            .iter()
            .find(|l| l.name == listener)
            .unwrap();
        decide(snapshot, listener, head, client, &mut || 0, id).map(|decision| {
            let forward = forwarding(decision);
            snapshot.upstream(forward.upstream).unwrap().name.clone()
        })
    }

    /// A request given an ID carries it, and no other: whatever it came with is gone, from a
    /// trusted proxy too (08 §3). Both kinds of head come to the same.
    #[test]
    fn a_request_given_an_id_carries_it_and_no_other() {
        const SENT: &[u8] = b"GET /cart HTTP/1.1\r\nHost: shop.example.com\r\nX-Request-ID: mine\r\nAccept: */*\r\nx-request-id: also-mine\r\n\r\n";
        let shop = shop();
        for client in [peer(), load_balancer()] {
            let (mut map, mut raw) = both_heads(SENT).unwrap();
            for upstream in [
                decide_with_id(&shop, &client, "web", &mut map, Some(&ID)),
                decide_with_id(&shop, &client, "web", &mut raw, Some(&ID)),
            ] {
                assert_eq!(upstream.as_deref(), Ok("cart"));
            }
            let id = ID_TEXT;
            assert_eq!(values_of(&map.headers, "x-request-id"), [id]);
            assert_eq!(values_of(&raw.fields(), "x-request-id"), [id]);
            assert_eq!(values_of(&raw.fields(), "accept"), ["*/*"]);
        }
    }

    /// A request given no ID — its listener passes the header through — keeps whatever it
    /// came with, as it came.
    #[test]
    fn a_request_given_no_id_keeps_its_own() {
        const SENT: &[u8] = b"GET /cart HTTP/1.1\r\nHost: shop.example.com\r\nX-Request-ID: mine\r\nx-request-id: also-mine\r\n\r\n";
        let shop = shop();
        for client in [peer(), load_balancer()] {
            let (mut map, mut raw) = both_heads(SENT).unwrap();
            assert!(decide_with_id(&shop, &client, "web", &mut map, None).is_ok());
            assert!(decide_with_id(&shop, &client, "web", &mut raw, None).is_ok());
            assert_eq!(
                values_of(&map.headers, "x-request-id"),
                ["mine", "also-mine"]
            );
            assert_eq!(
                values_of(&raw.fields(), "x-request-id"),
                ["mine", "also-mine"]
            );
        }
    }

    /// A listener whose trusted-only headers take in `X-Request-ID` takes a client's off and
    /// still gives the request its own; a listener that passes IDs through then passes on
    /// only a trusted proxy's.
    #[test]
    fn trusted_only_headers_take_a_clients_id_and_never_the_gateways() {
        let snapshot = compile(
            &serde_saphyr::from_str::<Config>(
                r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: ["10.0.0.0/8"], trusted_only_headers: ["X-*"] }, request_id: generate }
routes:
  - name: shop
    listeners: [web]
    hostnames: [{ name: shop.example.com, falls_through: false }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: cart, weight: 1 }] }
upstreams:
  cart: { load_balancer: p2c, endpoints: ["10.0.0.1:80"] }
"#,
            )
            .unwrap(),
        )
        .unwrap();
        let sent = || {
            head(
                "/",
                &[("host", "shop.example.com"), ("x-request-id", "mine")],
            )
        };
        let id = ID_TEXT;
        for (client, given, expected) in [
            (peer(), Some(&ID), vec![id]),
            (load_balancer(), Some(&ID), vec![id]),
            (peer(), None, vec![]),
            (load_balancer(), None, vec!["mine"]),
        ] {
            let mut request = sent();
            assert!(decide_with_id(&snapshot, &client, "web", &mut request, given).is_ok());
            assert_eq!(
                values_of(&request.headers, "x-request-id"),
                expected,
                "{given:?}"
            );
        }
    }

    /// A client's `Connection` may not have the gateway take off the ID it gave the request,
    /// any more than what it says of forwarding; where the gateway gives none, the header
    /// is the client's, and goes as the client asked.
    #[test]
    fn a_connection_that_names_the_id_is_refused_only_where_the_gateway_gives_one() {
        let shop = shop();
        let sent = || {
            head(
                "/cart",
                &[
                    ("host", "shop.example.com"),
                    ("connection", "X-Request-ID"),
                    ("x-request-id", "mine"),
                ],
            )
        };
        let mut request = sent();
        assert_eq!(
            decide_with_id(&shop, &peer(), "web", &mut request, Some(&ID)),
            Err(Rejection::Connection(ConnectionError::Protected))
        );
        let mut request = sent();
        assert_eq!(
            decide_with_id(&shop, &peer(), "web", &mut request, None).as_deref(),
            Ok("cart")
        );
        assert!(values_of(&request.headers, "x-request-id").is_empty());
    }

    /// A trusted proxy's chain names the client: the first address from the right that is
    /// not a trusted proxy's, and only that is passed on. What it said of the scheme and the
    /// host is kept, and so is what only it may send.
    #[test]
    fn a_trusted_proxy_names_the_client_and_what_it_says_is_kept() {
        let (mut map, mut raw) = both_heads(SAID).unwrap();
        let client = load_balancer();
        assert_eq!(
            decide_from(&client, "web", &mut map, 0).as_deref(),
            Ok("cart")
        );
        assert_eq!(
            decide_from(&client, "web", &mut raw, 0).as_deref(),
            Ok("cart")
        );
        let edited = raw.fields();
        for (name, expected) in [
            // 10.0.0.1 is inside the trusted range: every entry trusted, the leftmost.
            ("x-forwarded-for", vec!["10.0.0.1"]),
            ("x-forwarded-proto", vec!["https"]),
            ("x-forwarded-host", vec!["admin.example.com"]),
            ("forwarded", vec!["for=10.0.0.1"]),
            ("x-real-ip", vec!["10.0.0.1"]),
            ("x-forwarded-prefix", vec!["/admin"]),
        ] {
            assert_eq!(values_of(&map.headers, name), expected, "{name}, map");
            assert_eq!(values_of(&edited, name), expected, "{name}, raw");
        }

        // The load balancer example: a forged entry, then the one the balancer added.
        let mut request = head(
            "/cart",
            &[
                ("host", "shop.example.com"),
                ("x-forwarded-for", "1.2.3.4, 198.51.100.9"),
            ],
        );
        assert_eq!(
            decide_from(&client, "web", &mut request, 0).as_deref(),
            Ok("cart")
        );
        assert_eq!(
            values_of(&request.headers, "x-forwarded-for"),
            ["198.51.100.9"]
        );
        // Saying nothing, it is the client; the scheme and host are then the gateway's.
        let mut request = head("/cart", &[("host", "shop.example.com")]);
        assert_eq!(
            decide_from(&client, "web", &mut request, 0).as_deref(),
            Ok("cart")
        );
        assert_eq!(values_of(&request.headers, "x-forwarded-for"), ["10.1.0.5"]);
        assert_eq!(values_of(&request.headers, "x-forwarded-proto"), ["http"]);
        assert_eq!(
            values_of(&request.headers, "x-forwarded-host"),
            ["shop.example.com"]
        );
    }

    /// An IPv4 client of a dual-stack socket is told as IPv4, and trusted as it.
    #[test]
    fn a_client_is_told_in_one_form() {
        let mapped = Client::new("::ffff:10.1.0.5".parse().unwrap());
        let mut request = head(
            "/cart",
            &[
                ("host", "shop.example.com"),
                ("x-forwarded-for", "198.51.100.9"),
            ],
        );
        assert_eq!(
            decide_from(&mapped, "web", &mut request, 0).as_deref(),
            Ok("cart")
        );
        assert_eq!(
            values_of(&request.headers, "x-forwarded-for"),
            ["198.51.100.9"]
        );
        let mapped = Client::new("::ffff:203.0.113.7".parse().unwrap());
        let mut request = head("/cart", &[("host", "shop.example.com")]);
        assert_eq!(
            decide_from(&mapped, "web", &mut request, 0).as_deref(),
            Ok("cart")
        );
        assert_eq!(
            values_of(&request.headers, "x-forwarded-for"),
            ["203.0.113.7"]
        );
    }

    /// A rule's predicates read what the upstream will be told, not what the client said;
    /// and a rule's own changes come after, and may change it.
    #[test]
    fn a_rule_reads_what_the_upstream_is_told_and_may_change_it() {
        let said = [("host", "shop.example.com"), ("x-forwarded-proto", "https")];
        assert_eq!(upstream_for("/plain", &said).as_deref(), Ok("search"));
        // From a trusted proxy that said `https`, the rule is not for it.
        let mut request = head("/plain", &said);
        assert_eq!(
            decide_from(&load_balancer(), "web", &mut request, 0).as_deref(),
            Ok("fallback")
        );

        let mut request = head("/told", &[("host", "shop.example.com")]);
        assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("search"));
        assert_eq!(
            values_of(&request.headers, "x-forwarded-for"),
            ["192.0.2.1"]
        );
        assert!(values_of(&request.headers, "via").is_empty());
    }

    /// A request's `Trailer` declaration goes with the trailers it declares, which go no
    /// further than the gateway; that it accepts trailers in its answer stays.
    #[test]
    fn a_requests_declaration_of_trailers_goes_no_further() {
        let sent = b"POST /cart HTTP/1.1\r\nHost: shop.example.com\r\nTrailer: x-sum\r\nTE: trailers\r\nTransfer-Encoding: chunked\r\n\r\n";
        let (mut map, mut raw) = both_heads(sent).unwrap();
        assert_eq!(decide_on("web", &mut map, 0).as_deref(), Ok("cart"));
        assert_eq!(decide_on("web", &mut raw, 0).as_deref(), Ok("cart"));
        let edited = raw.fields();
        assert!(values_of(&map.headers, "trailer").is_empty());
        assert!(values_of(&edited, "trailer").is_empty());
        assert_eq!(values_of(&map.headers, "te"), ["trailers"]);
        assert_eq!(values_of(&edited, "te"), ["trailers"]);
    }

    /// The scheme is the listener's, and `Via` names the version the request came in.
    #[test]
    fn the_scheme_and_version_are_those_the_request_came_by() {
        let mut request = head("/cart", &[("host", "shop.example.com")]);
        assert_eq!(decide_on("secure", &mut request, 0).as_deref(), Ok("cart"));
        assert_eq!(values_of(&request.headers, "x-forwarded-proto"), ["https"]);
        let mut request = Request::builder()
            .uri("https://shop.example.com/cart")
            .version(http::Version::HTTP_2)
            .body(())
            .unwrap()
            .into_parts()
            .0;
        assert_eq!(decide_on("secure", &mut request, 0).as_deref(), Ok("cart"));
        assert_eq!(values_of(&request.headers, "via"), ["2 edgerush"]);
        assert_eq!(
            values_of(&request.headers, "x-forwarded-host"),
            ["shop.example.com"]
        );
        let mut request = head("/cart", &[("host", "shop.example.com")]);
        request.version = http::Version::HTTP_10;
        assert_eq!(decide_on("admin", &mut request, 0), Err(Rejection::NoRoute));
        assert_eq!(values_of(&request.headers, "via"), ["1.0 edgerush"]);
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
        let forward = forwarding(decide(&shop, web, &mut head, &peer(), &mut || 0, None).unwrap());
        assert!(forward.rule.request_headers.is_some());
        assert_eq!(shop.upstream(forward.upstream).unwrap().name, "cart");
    }

    const MOVED: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: moved
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /old } }]
        redirect: { status: 301, path: { replace_prefix: /new }, query: keep }
      - matches: [{ path: { prefix: /secure } }]
        redirect: { status: 308, scheme: https, query: drop }
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: pages, weight: 1 }] }
upstreams:
  pages: { load_balancer: p2c, endpoints: ["127.0.0.1:9000"] }
"#;

    /// What a request to `web` of [`MOVED`] is redirected with: status and `Location`.
    fn redirected<H: Head>(head: &mut H) -> Option<(StatusCode, String)> {
        let config: Config = serde_saphyr::from_str(MOVED).unwrap();
        let moved = compile(&config).unwrap();
        let web = moved.listeners.iter().find(|l| l.name == "web").unwrap();
        match decide(&moved, web, head, &peer(), &mut || 0, None).unwrap() {
            Decision::Redirect(redirected) => Some((
                redirected.status,
                redirected.location.to_str().unwrap().to_owned(),
            )),
            Decision::Forward(_) => None,
        }
    }

    #[test]
    fn a_redirect_is_made_from_what_was_routed_on_and_the_head_is_left_alone() {
        // The normalised path, and the query as it came.
        let target = "/old/./a%61?x=%2f&y";
        let mut request = head(target, &[("host", "Shop.Example.com:8080")]);
        assert_eq!(
            redirected(&mut request),
            Some((StatusCode::MOVED_PERMANENTLY, "/new/aa?x=%2f&y".to_owned()))
        );
        assert_eq!(request.uri, target);
        assert!(request.headers.contains_key("host"));

        // A stated scheme: the host routed on, in lower case, and its default port.
        let mut request = head("/secure/p?q", &[("host", "Shop.Example.com:8080")]);
        assert_eq!(
            redirected(&mut request),
            Some((
                StatusCode::PERMANENT_REDIRECT,
                "https://shop.example.com/secure/p".to_owned()
            ))
        );
        // Over HTTP/2 the host is `:authority`'s.
        let mut h2 = head("http://h2.example.com/secure", &[]);
        h2.version = http::Version::HTTP_2;
        assert_eq!(
            redirected(&mut h2).map(|(_, location)| location).as_deref(),
            Some("https://h2.example.com/secure")
        );
        // Whatever the rule does not redirect is forwarded as ever.
        let mut request = head("/elsewhere", &[("host", "shop.example.com")]);
        assert_eq!(redirected(&mut request), None);
    }

    const REWRITTEN: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: rewritten
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /prefix/one } }]
        filters: [{ type: url_rewrite, path: { replace_prefix: /one } }]
        forward: { backends: [{ upstream: up, weight: 1 }] }
      - matches: [{ path: { prefix: /strip-prefix } }]
        filters: [{ type: url_rewrite, path: { replace_prefix: / } }]
        forward: { backends: [{ upstream: up, weight: 1 }] }
      - matches: [{ path: { prefix: /full/one } }]
        filters: [{ type: url_rewrite, path: { replace_full: /one } }]
        forward: { backends: [{ upstream: up, weight: 1 }] }
      - matches: [{ path: { prefix: /host } }]
        filters: [{ type: url_rewrite, host: one.example.org }]
        forward: { backends: [{ upstream: up, weight: 1 }] }
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: up, weight: 1 }] }
upstreams:
  up: { load_balancer: p2c, endpoints: ["127.0.0.1:9000"] }
"#;

    /// Decides on `web` of [`REWRITTEN`], and gives the head as it goes upstream.
    fn rewritten<H: Head>(mut head: H) -> H {
        let config: Config = serde_saphyr::from_str(REWRITTEN).unwrap();
        let rewritten = compile(&config).unwrap();
        let web = rewritten
            .listeners
            .iter()
            .find(|l| l.name == "web")
            .unwrap();
        forwarding(decide(&rewritten, web, &mut head, &peer(), &mut || 0, None).unwrap());
        head
    }

    /// Gateway API's `httproute-rewrite-path` cases, and what normalising and the query add.
    #[test]
    fn a_rewrite_changes_the_path_the_upstream_is_sent() {
        let cases = [
            ("/prefix/one/two", "/one/two"),
            ("/prefix/one/./tw%6f?x=%2f&y", "/one/two?x=%2f&y"),
            ("/strip-prefix/three", "/three"),
            ("/strip-prefix", "/"),
            ("/strip-prefix?q", "/?q"),
            ("/full/one/two", "/one"),
            ("/full/one/two?keep=1", "/one?keep=1"),
            ("/elsewhere/./a", "/elsewhere/a"),
        ];
        for (target, sent) in cases {
            let went = rewritten(head(target, &[("host", "shop.example.com")]));
            assert_eq!(went.uri, sent, "{target}");
            assert_eq!(went.headers["host"], "shop.example.com", "{target}");
        }
    }

    /// Gateway API's `httproute-rewrite-host`: the upstream is sent the new host, over
    /// HTTP/2 too, whose `:authority` the `Host` field was made to agree with.
    #[test]
    fn a_rewrite_changes_the_host_the_upstream_is_sent() {
        let sent = rewritten(head("/host/a?b", &[("host", "Shop.Example.com:8080")]));
        assert_eq!(sent.headers.get_all("host").iter().count(), 1);
        assert_eq!(sent.headers["host"], "one.example.org");
        assert_eq!(sent.uri, "/host/a?b");

        let mut h2 = head("http://shop.example.com/host/a", &[]);
        h2.version = http::Version::HTTP_2;
        let h2 = rewritten(h2);
        assert_eq!(h2.headers["host"], "one.example.org");
        // The target's authority is the endpoint's by the time it is sent (`at_endpoint`).
        assert_eq!(h2.uri.path(), "/host/a");
    }

    const MIRRORED: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: mirrored
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /a } }]
        filters:
          - { type: request_mirror, upstream: first, fraction: { numerator: 1, denominator: 1 } }
          - { type: request_header_modifier, set: [{ name: x-changed, value: "1" }] }
          - { type: request_mirror, upstream: second, fraction: { numerator: 1, denominator: 2 } }
          - { type: url_rewrite, host: one.example.org, path: { replace_prefix: /api } }
          - { type: request_mirror, upstream: third, fraction: { numerator: 1, denominator: 1 } }
        forward: { backends: [{ upstream: up, weight: 1 }] }
upstreams:
  up: { load_balancer: p2c, endpoints: ["127.0.0.1:9000"] }
  first: { load_balancer: p2c, endpoints: ["127.0.0.1:9001"] }
  second: { load_balancer: p2c, endpoints: ["127.0.0.1:9002"] }
  third: { load_balancer: p2c, endpoints: ["127.0.0.1:9003"] }
"#;

    /// What a copy says: its target, its `Host`, and whether it has `x-changed`.
    type Seen = (String, String, bool);

    /// The mirrors that take a request to `web` of [`MIRRORED`], drawn from `draws` after
    /// the backend's draw: each with what its copy says, if it has one.
    fn mirrored<H: Head>(mut head: H, draws: &[u64]) -> Vec<(usize, Option<Seen>)> {
        let config: Config = serde_saphyr::from_str(MIRRORED).unwrap();
        let mirrored = compile(&config).unwrap();
        let web = mirrored.listeners.iter().find(|l| l.name == "web").unwrap();
        let mut draws = std::iter::once(0).chain(draws.iter().copied());
        let forward = forwarding(
            decide(
                &mirrored,
                web,
                &mut head,
                &peer(),
                &mut || draws.next().unwrap(),
                None,
            )
            .unwrap(),
        );
        forward
            .mirrors
            .into_iter()
            .map(|mirroring| {
                let own = mirroring.own.map(|copied| {
                    (
                        copied.target.to_string(),
                        copied.fields["host"].to_str().unwrap().to_owned(),
                        copied.fields.contains_key("x-changed"),
                    )
                });
                (mirroring.mirror, own)
            })
            .collect()
    }

    #[test]
    fn a_mirror_is_sent_the_request_as_it_stands_at_its_place() {
        let copy =
            |target: &str, host: &str, changed| Some((target.to_owned(), host.to_owned(), changed));
        let target = "/a/./b?c=d";
        let fields = [
            ("host", "shop.example.com"),
            ("connection", "x-hop"),
            ("x-hop", "1"),
        ];
        // Every mirror takes it: the first before any change, the second after the header
        // change, the third after both, as the upstream.
        assert_eq!(
            mirrored(head(target, &fields), &[0, 0, 0]),
            [
                (0, copy("/a/b?c=d", "shop.example.com", false)),
                (1, copy("/a/b?c=d", "shop.example.com", true)),
                (2, None),
            ]
        );
        // The second's share is one in two, and this draw is not in it: no copy is made.
        assert_eq!(
            mirrored(head(target, &fields), &[0, 1, 0]),
            [(0, copy("/a/b?c=d", "shop.example.com", false)), (2, None)]
        );
        // A raw head's copies are its map's.
        let sent = format!(
            "GET {target} HTTP/1.1\r\nHost: shop.example.com\r\nConnection: x-hop\r\nx-hop: 1\r\n\r\n"
        );
        let (map, raw) = both_heads(sent.as_bytes()).unwrap();
        assert_eq!(mirrored(raw, &[0, 0, 0]), mirrored(map, &[0, 0, 0]));
        // What every copy has lost: what was about the client's connection.
        let config: Config = serde_saphyr::from_str(MIRRORED).unwrap();
        let compiled = compile(&config).unwrap();
        let web = compiled.listeners.iter().find(|l| l.name == "web").unwrap();
        let mut request = head(target, &fields);
        let forward =
            forwarding(decide(&compiled, web, &mut request, &peer(), &mut || 0, None).unwrap());
        let first = forward.mirrors[0].own.as_ref().unwrap();
        assert!(!first.fields.contains_key("x-hop"));
        assert!(!first.fields.contains_key("connection"));
        // And what every copy is told of the client, as the upstream is.
        assert_eq!(first.fields["x-forwarded-for"], "203.0.113.7");
        assert_eq!(first.fields["via"], "1.1 edgerush");
        assert_eq!(request.uri, "/api/b?c=d");
        assert_eq!(request.headers["host"], "one.example.org");
    }

    #[test]
    fn a_request_that_cannot_be_routed_is_rejected_before_any_redirect() {
        let config: Config = serde_saphyr::from_str(MOVED).unwrap();
        let moved = compile(&config).unwrap();
        let web = moved.listeners.iter().find(|l| l.name == "web").unwrap();
        let mut ambiguous = head("/old/%2e%2e/admin", &[("host", "shop.example.com")]);
        assert!(matches!(
            decide(&moved, web, &mut ambiguous, &peer(), &mut || 0, None),
            Err(Rejection::Path(_))
        ));
        let mut hostless = head("/old/a", &[]);
        assert!(matches!(
            decide(&moved, web, &mut hostless, &peer(), &mut || 0, None),
            Err(Rejection::Host(_))
        ));
    }

    #[test]
    fn the_host_of_the_target_is_used_and_the_host_field_made_to_agree() {
        // An HTTP/2 request: `:authority` and no `Host` field.
        let mut h2 = head("http://shop.example.com/cart", &[]);
        h2.version = http::Version::HTTP_2;
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
    }

    /// The target's host wins over the `Host` field only once the field has been found to
    /// be one, and one that could be a host: an HTTP/1 request with more than one `Host`
    /// field line or an invalid one is refused, whatever its target names, and so is an
    /// HTTP/1.1 request with none ([RFC 9112 §3.2]). HTTP/1.0 did not have to send one.
    ///
    /// [RFC 9112 §3.2]: https://www.rfc-editor.org/rfc/rfc9112.html#section-3.2
    #[test]
    fn the_host_fields_of_an_http_1_request_are_checked_before_the_target_wins() {
        let target = "http://shop.example.com/cart";
        assert_eq!(
            upstream_for(
                target,
                &[("host", "shop.example.com"), ("host", "other.example.org")]
            ),
            Err(Rejection::Host(HostError::Repeated))
        );
        assert_eq!(
            upstream_for(target, &[("host", "b"), ("host", "b")]),
            Err(Rejection::Host(HostError::Repeated))
        );
        assert_eq!(
            upstream_for(target, &[("host", "invalid/host")]),
            Err(Rejection::Host(HostError::Invalid))
        );
        assert_eq!(
            upstream_for(target, &[]),
            Err(Rejection::Host(HostError::Missing))
        );

        let mut from_1_0 = head(target, &[]);
        from_1_0.version = http::Version::HTTP_10;
        assert_eq!(decide_on("web", &mut from_1_0, 0).as_deref(), Ok("cart"));
        assert_eq!(
            from_1_0.headers.get_all(HOST).iter().collect::<Vec<_>>(),
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
        let mut connect = head("shop.example.com:443", &[("host", "shop.example.com:443")]);
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

        let mut absolute = self::head(
            "http://shop.example.com/./cart",
            &[("host", "shop.example.com")],
        );
        assert_eq!(decide_on("web", &mut absolute, 0).as_deref(), Ok("cart"));
        assert_eq!(absolute.uri, "http://shop.example.com/cart");
    }

    /// A target with a query and no path is read as the path `/`, and goes on with one:
    /// origin-form has no target without a path, and an upstream sent `?q=1` alone has
    /// been sent a request line it cannot read ([RFC 9112 §3.2.1]).
    ///
    /// [RFC 9112 §3.2.1]: https://www.rfc-editor.org/rfc/rfc9112.html#section-3.2.1
    #[test]
    fn a_query_with_no_path_is_given_the_root() {
        let mut head = head(
            "http://shop.example.com?q=1",
            &[("host", "shop.example.com")],
        );
        assert_eq!(decide_on("web", &mut head, 0).as_deref(), Ok("fallback"));
        assert_eq!(
            head.uri.path_and_query().map(PathAndQuery::as_str),
            Some("/?q=1")
        );
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
    fn a_cookie_string_that_came_in_pieces_is_routed_on_and_forwarded_whole() {
        // HTTP/2 lets a client split its cookie string into fields (RFC 9113 §8.2.3).
        let mut request = head(
            "/account",
            &[
                ("host", "shop.example.com"),
                ("cookie", "a=1"),
                ("accept", "*/*"),
                ("cookie", "b=2"),
            ],
        );
        assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("search"));
        let cookies: Vec<_> = request.headers.get_all("cookie").iter().collect();
        assert_eq!(cookies, ["a=1; b=2"]);
        assert_eq!(request.headers.get("accept").unwrap(), "*/*");

        // Whole already: left as it is, and routed on the same.
        let mut whole = head(
            "/account",
            &[("host", "shop.example.com"), ("cookie", "a=1; b=2")],
        );
        assert_eq!(decide_on("web", &mut whole, 0).as_deref(), Ok("search"));
        assert_eq!(whole.headers.get("cookie").unwrap(), "a=1; b=2");

        // Other cookies do not match, in pieces or whole.
        let mut other = head(
            "/account",
            &[
                ("host", "shop.example.com"),
                ("cookie", "a=1"),
                ("cookie", "c=3"),
            ],
        );
        assert_eq!(decide_on("web", &mut other, 0).as_deref(), Ok("fallback"));
        let cookies: Vec<_> = other.headers.get_all("cookie").iter().collect();
        assert_eq!(cookies, ["a=1; c=3"]);
    }

    #[test]
    fn three_pieces_and_a_sensitive_one_make_one_sensitive_cookie_string() {
        let mut request = head(
            "/cart",
            &[
                ("host", "shop.example.com"),
                ("cookie", "a=1"),
                ("cookie", "b=2"),
                ("cookie", "c=3"),
            ],
        );
        let mut secret = HeaderValue::from_static("session=s3cr3t");
        secret.set_sensitive(true);
        request.headers.append("cookie", secret);
        assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("cart"));
        let cookies: Vec<_> = request.headers.get_all("cookie").iter().collect();
        assert_eq!(cookies, ["a=1; b=2; c=3; session=s3cr3t"]);
        assert!(cookies[0].is_sensitive());
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
    fn a_clients_proxy_credentials_stay_behind_and_a_rules_own_go_out() {
        let fields = [
            ("host", "shop.example.com"),
            ("proxy-authorization", "Basic Y2xpZW50"),
            ("authorization", "Bearer for-the-origin"),
        ];
        let mut request = head("/cart", &fields);
        assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("cart"));
        assert!(!request.headers.contains_key("proxy-authorization"));
        assert_eq!(
            request.headers.get("authorization").unwrap(),
            "Bearer for-the-origin"
        );

        // An upstream that is itself a proxy wanting credentials is said so in the config:
        // the rule's changes come after the client's have been taken off.
        let chained = SHOP.replace(
            "set: [{ name: X-Gateway, value: edgerush }]",
            "set: [{ name: Proxy-Authorization, value: Basic Z2F0ZXdheQ== }]",
        );
        let chained = compile(&shop_config(&chained)).unwrap();
        let web = chained.listeners.iter().find(|l| l.name == "web").unwrap();
        let mut request = head("/cart", &fields);
        decide(&chained, web, &mut request, &peer(), &mut || 0, None).unwrap();
        let credentials: Vec<_> = request
            .headers
            .get_all("proxy-authorization")
            .iter()
            .collect();
        assert_eq!(credentials, ["Basic Z2F0ZXdheQ=="]);
    }

    #[test]
    fn a_request_that_says_te_trailers_and_nothing_more_goes_on_saying_it() {
        // What every gRPC client sends. It is already what would be forwarded, so there is
        // nothing to take off and put back.
        let mut grpc = head(
            "http://shop.example.com/cart",
            &[("te", "trailers"), ("content-type", "application/grpc")],
        );
        grpc.version = http::Version::HTTP_2;
        assert_eq!(decide_on("web", &mut grpc, 0).as_deref(), Ok("cart"));
        let te: Vec<_> = grpc.headers.get_all("te").iter().collect();
        assert_eq!(te, ["trailers"]);
        assert!(!hop_by_hop_to_take_off(&grpc.headers));

        // Said in any other way, it is still brought to that one form.
        for said in [
            &[("te", "Trailers")][..],
            &[("te", "trailers, gzip")],
            &[("te", "trailers"), ("te", "trailers")],
            &[("te", "trailers"), ("connection", "te")],
            &[("te", "trailers"), ("keep-alive", "timeout=5")],
        ] {
            let mut fields = vec![("host", "shop.example.com")];
            fields.extend_from_slice(said);
            let mut request = head("/cart", &fields);
            assert!(hop_by_hop_to_take_off(&request.headers), "{said:?}");
            assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("cart"));
            let te: Vec<_> = request.headers.get_all("te").iter().collect();
            assert_eq!(te, ["trailers"], "{said:?}");
            assert!(!request.headers.contains_key("connection"), "{said:?}");
            assert!(!request.headers.contains_key("keep-alive"), "{said:?}");
        }

        // And a `TE` that does not accept trailers goes, a weighted `trailers` among them:
        // the keyword takes no weight.
        for te in ["gzip", "trailers;q=1"] {
            let mut request = head("/cart", &[("host", "shop.example.com"), ("te", te)]);
            assert!(hop_by_hop_to_take_off(&request.headers), "{te}");
            assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("cart"));
            assert!(!request.headers.contains_key("te"), "{te}");
        }
    }

    /// Whether the pass over the headers finds hop-by-hop headers that need work.
    fn hop_by_hop_to_take_off(headers: &HeaderMap) -> bool {
        survey(headers).hop_by_hop
    }

    #[test]
    fn what_the_request_says_about_its_connection_stays_behind() {
        let mut request = head(
            "/cart",
            &[
                ("host", "shop.example.com"),
                ("connection", "keep-alive, x-hop"),
                ("keep-alive", "timeout=5"),
                ("x-hop", "1"),
                ("te", "trailers, gzip"),
                ("accept", "*/*"),
            ],
        );
        assert_eq!(decide_on("web", &mut request, 0).as_deref(), Ok("cart"));
        let mut names: Vec<&str> = request.headers.keys().map(|name| name.as_str()).collect();
        names.sort_unstable();
        assert_eq!(
            names,
            [
                "accept",
                "host",
                "te",
                "via",
                "x-forwarded-for",
                "x-forwarded-host",
                "x-forwarded-proto",
                "x-gateway"
            ]
        );
        assert_eq!(request.headers.get("te").unwrap(), "trailers");
    }

    #[test]
    fn a_connection_header_that_would_take_the_host_away_is_rejected() {
        let fields = [("host", "shop.example.com"), ("connection", "close, host")];
        assert_eq!(
            upstream_for("/cart", &fields),
            Err(Rejection::Connection(ConnectionError::Protected))
        );
        let fields = [("host", "shop.example.com"), ("connection", "x y")];
        assert_eq!(
            upstream_for("/cart", &fields),
            Err(Rejection::Connection(ConnectionError::Malformed))
        );
    }

    #[test]
    fn a_rule_with_nowhere_to_go_is_rejected() {
        let upstream = upstream_for("/closed", &[("host", "shop.example.com")]);
        assert_eq!(upstream, Err(Rejection::NoBackend));
    }

    #[test]
    fn a_rule_that_looks_at_the_host_header_sees_the_host_that_is_routed_on() {
        // The target names one host and the `Host` field another: the target's counts
        // (RFC 9112 §3.2.2), for every predicate, not only for the hostname.
        let mut spoofed = head(
            "http://shop.example.com/tenant",
            &[("host", "tenant.example.net")],
        );
        assert_eq!(decide_on("web", &mut spoofed, 0).as_deref(), Ok("fallback"));
        assert_eq!(spoofed.headers.get(HOST).unwrap(), "shop.example.com");

        // The other way round, the rule is for the request, whatever its `Host` field says.
        let mut tenant = head(
            "http://tenant.example.net/tenant",
            &[("host", "shop.example.com")],
        );
        assert_eq!(decide_on("web", &mut tenant, 0).as_deref(), Ok("search"));
        assert_eq!(tenant.headers.get(HOST).unwrap(), "tenant.example.net");

        // HTTP/2 has `:authority` and no `Host` field at all.
        let mut h2 = head("http://tenant.example.net/tenant", &[]);
        h2.version = http::Version::HTTP_2;
        assert_eq!(decide_on("web", &mut h2, 0).as_deref(), Ok("search"));

        // Without a host in the target, the `Host` field is the host.
        let mut origin_form = head("/tenant", &[("host", "tenant.example.net")]);
        assert_eq!(
            decide_on("web", &mut origin_form, 0).as_deref(),
            Ok("search")
        );
        let mut origin_form = head("/tenant", &[("host", "shop.example.com")]);
        assert_eq!(
            decide_on("web", &mut origin_form, 0).as_deref(),
            Ok("fallback")
        );
    }

    #[test]
    fn a_rejected_request_keeps_its_target_and_the_headers_nothing_was_matched_on() {
        let mut head = head(
            "http://shop.example.com/./closed",
            &[
                ("host", "other.example.org"),
                ("connection", "x-hop"),
                ("x-hop", "1"),
                ("x-debug", "1"),
            ],
        );
        assert_eq!(decide_on("web", &mut head, 0), Err(Rejection::NoBackend));
        assert_eq!(head.uri, "http://shop.example.com/./closed");
        assert_eq!(head.headers.get("x-hop").unwrap(), "1");
        assert_eq!(head.headers.get("x-debug").unwrap(), "1");
        // The host is settled before anything is matched, so it is settled here too.
        assert_eq!(head.headers.get(HOST).unwrap(), "shop.example.com");
    }

    #[test]
    fn every_rejection_has_its_status() {
        assert_eq!(Rejection::Host(HostError::Missing).status(), 400);
        assert_eq!(Rejection::Path(NormaliseError::Backslash).status(), 400);
        assert_eq!(
            Rejection::Connection(ConnectionError::Malformed).status(),
            400
        );
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
        /// The path that is forwarded is the normal form of the one that came — under `/v1`,
        /// the `shop` route's rule rewrites that to `/api` — and nothing else about the
        /// target changes: rewriting never fails on what normalising yields.
        #[test]
        fn whatever_is_forwarded_has_the_normal_path_and_the_rest_as_it_came(target in target()) {
            let mut head = head("/", &[("host", "shop.example.com")]);
            head.uri = target.clone();
            match (decide_on("web", &mut head, 0), normalise_path(target.path())) {
                (Ok(_), Ok(normal)) => {
                    let forwarded = match normal.strip_prefix("/v1") {
                        Some(rest) if rest.is_empty() || rest.starts_with('/') => {
                            format!("/api{rest}")
                        }
                        _ => normal.into_owned(),
                    };
                    prop_assert_eq!(head.uri.path(), forwarded);
                    prop_assert_eq!(head.uri.query(), target.query());
                    prop_assert_eq!(head.uri.scheme(), target.scheme());
                    prop_assert_eq!(head.uri.authority(), target.authority());
                }
                (Err(Rejection::Path(theirs)), Err(ours)) => prop_assert_eq!(theirs, ours),
                (decided, normal) => prop_assert!(false, "{target}: {decided:?}, {normal:?}"),
            }
        }
    }

    /// A request's head as our own server reads it — the bytes, where each field line lies
    /// in them — and the header map an engine's server would make of the same bytes; `None`
    /// for bytes that neither would take.
    fn both_heads(sent: &[u8]) -> Option<(Parts, RawHead)> {
        let mut room = [httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut room);
        if !matches!(request.parse(sent), Ok(httparse::Status::Complete(_))) {
            return None;
        }
        let method = Method::from_bytes(request.method?.as_bytes()).ok()?;
        let uri: http::Uri = request.path?.parse().ok()?;
        let mut map = HeaderMap::new();
        for field in request.headers.iter() {
            map.append(
                http::HeaderName::from_bytes(field.name.as_bytes()).ok()?,
                HeaderValue::from_bytes(field.value).ok()?,
            );
        }
        let lines = crate::fields::FieldLines::new(sent, request.headers).ok()?;
        let (mut parts, ()) = Request::new(()).into_parts();
        parts.method = method.clone();
        parts.uri = uri.clone();
        parts.headers = map;
        let raw = RawHead::new(
            method,
            uri,
            http::Version::HTTP_11,
            bytes::Bytes::copy_from_slice(sent),
            lines,
        );
        Some((parts, raw))
    }

    /// Names in any case, among them every one the core does something with, and values
    /// that make them mean something: hosts that route, cookies in pieces, `Connection`
    /// naming other fields, `TE` with and without trailers.
    fn raw_field() -> impl Strategy<Value = (&'static str, &'static str)> {
        (
            prop::sample::select(vec![
                "host",
                "Host",
                "cookie",
                "Cookie",
                "connection",
                "Connection",
                "te",
                "TE",
                "keep-alive",
                "x-hop",
                "X-Hop",
                "x-debug",
                "X-Debug",
                "x-gateway",
                "upgrade",
                "proxy-connection",
                "accept",
                "content-length",
                "Transfer-Encoding",
                "Trailer",
                "authorization",
                "Expect",
                "x-forwarded-for",
                "X-Forwarded-Proto",
                "x-forwarded-host",
                "X-Forwarded-Port",
                "forwarded",
                "X-Real-IP",
                "Via",
                "x_forwarded_for",
            ]),
            prop::sample::select(vec![
                "5",
                "chunked",
                "x-sum, x-hop",
                "NTLM TlRMTVNTUAABAAAA",
                "Bearer abc",
                "100-continue",
                "shop.example.com",
                "tenant.example.net",
                "other.example.org",
                "a=1",
                "b=2",
                "a=1; b=2",
                "keep-alive",
                "x-hop",
                "close",
                "te",
                "trailers",
                "gzip",
                "trailers, gzip",
                "1",
                "x-hop, keep-alive",
                "x-debug",
                "host",
                "x hop",
                "10.1.0.5",
                "1.2.3.4, 198.51.100.9",
                "198.51.100.9, unknown",
                "https",
                "for=1.2.3.4",
                "1.1 cdn",
            ]),
        )
    }

    fn every_name() -> Vec<http::HeaderName> {
        [
            "host",
            "cookie",
            "connection",
            "te",
            "keep-alive",
            "x-hop",
            "x-debug",
            "x-gateway",
            "upgrade",
            "proxy-connection",
            "accept",
            "trailer",
            "authorization",
            "expect",
            "content-length",
            "transfer-encoding",
            "x-forwarded-for",
            "x-forwarded-proto",
            "x-forwarded-host",
            "x-forwarded-port",
            "forwarded",
            "x-real-ip",
            "via",
            "x_forwarded_for",
        ]
        .into_iter()
        .map(http::HeaderName::from_static)
        .collect()
    }

    proptest! {
        /// A raw head is decided on as the header map of the same bytes is: the same rule
        /// or the same refusal, the same target, and every field the same after.
        #[test]
        fn a_raw_head_is_decided_on_as_its_header_map_is(
            target in prop::sample::select(vec![
                "/cart/items", "/account", "/tenant/x", "/search?q=a%20b", "/pages/./a/../b",
                "/status", "/closed", "http://shop.example.com/cart",
                "http://tenant.example.net/tenant/y", "http://Shop.Example.com:80/account",
                "/v1/./users?page=2", "http://shop.example.com/v1",
            ]),
            // Most requests name a host that routes, so that what is done after routing is
            // reached; the rest may name any, or none.
            host in prop::sample::select(vec![
                Some("shop.example.com"), Some("tenant.example.net"), Some("shop.example.com"),
                None,
            ]),
            fields in prop::collection::vec(raw_field(), 0..8),
            listener in prop::sample::select(vec!["web", "web", "admin"]),
            random in 0..10u64,
            from_proxy in any::<bool>(),
        ) {
            let client = if from_proxy { load_balancer() } else { peer() };
            let mut sent = format!("GET {target} HTTP/1.1\r\n").into_bytes();
            if let Some(host) = host {
                sent.extend_from_slice(format!("Host: {host}\r\n").as_bytes());
            }
            for (name, value) in &fields {
                sent.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
            }
            sent.extend_from_slice(b"\r\n");
            let (mut map, mut raw) = both_heads(&sent).unwrap();
            // What is read before anything is decided, for how the body goes and what may
            // not travel on.
            let nominated = crate::hop_by_hop::nominated(&map.headers);
            prop_assert_eq!(&crate::hop_by_hop::nominated(&raw), &nominated);
            prop_assert_eq!(
                crate::hop_by_hop::is_chunked_request(&raw),
                crate::hop_by_hop::is_chunked_request(&map.headers)
            );
            let by_map = decide_from(&client, listener, &mut map, random);
            let by_raw = decide_from(&client, listener, &mut raw, random);
            prop_assert_eq!(&by_raw, &by_map);
            prop_assert_eq!(raw.uri(), map.uri());
            // And what the rest of the way does to a head that is going.
            if by_map.is_ok() {
                use crate::head::Forwarded;
                use crate::upstream::auth::carries_credentials;
                prop_assert_eq!(carries_credentials(&raw), carries_credentials(&map.headers));
                if carries_credentials(&raw) {
                    prop_assert_eq!(raw.close_connection(), map.close_connection());
                }
            }
            let edited = raw.fields();
            for name in every_name() {
                let from_raw: Vec<&[u8]> = edited.values(&name).collect();
                let from_map: Vec<&[u8]> = Fields::values(&map.headers, &name).collect();
                prop_assert_eq!(from_raw, from_map, "{}", name);
            }

            // A map with what the core adds kept beside it, as our HTTP/2 and HTTP/3 servers'
            // heads are, is decided on as the map is, field for field.
            {
                use crate::head::Forwarded;
                use crate::map_head::MapHead;
                use crate::upstream::auth::carries_credentials;
                use crate::upstream::h1::codec::OutgoingFields;
                let (parts, _) = both_heads(&sent).unwrap();
                let mut beside = MapHead::new(parts);
                let by_beside = decide_from(&client, listener, &mut beside, random);
                prop_assert_eq!(&by_beside, &by_map);
                prop_assert_eq!(beside.uri(), map.uri());
                if by_map.is_ok() && carries_credentials(beside.outgoing()) {
                    prop_assert_eq!(beside.close_connection(), Ok(()));
                }
                let fields = beside.fields();
                for name in every_name() {
                    let from_beside: Vec<&[u8]> = fields.values(&name).collect();
                    let from_map: Vec<&[u8]> = Fields::values(&map.headers, &name).collect();
                    prop_assert_eq!(from_beside, from_map, "{}", name);
                }
                prop_assert_eq!(beside.to_map(), map.headers.clone());
                // What is written of it is what is written of the map, in whatever order.
                let lines = |written: Vec<u8>| {
                    let mut lines: Vec<Vec<u8>> = written
                        .split(|&byte| byte == b'\n')
                        .map(<[u8]>::to_vec)
                        .collect();
                    lines.sort();
                    lines
                };
                let mut from_beside = Vec::new();
                beside.outgoing().write_fields(&mut from_beside);
                let mut from_map = Vec::new();
                map.headers.write_fields(&mut from_map);
                prop_assert_eq!(beside.outgoing().written_len(), from_beside.len());
                prop_assert_eq!(lines(from_beside), lines(from_map));
            }

            // Made into `http`'s parts, for a client that takes those, it is the map.
            {
                use crate::head::Forwarded;
                let decided = {
                    let (_, mut fresh) = both_heads(&sent).unwrap();
                    let _ = decide_from(&client, listener, &mut fresh, random);
                    if by_map.is_ok() && crate::upstream::auth::carries_credentials(&fresh) {
                        let _ = fresh.close_connection();
                    }
                    fresh.into_parts()
                };
                for name in every_name() {
                    let from_parts: Vec<&[u8]> = Fields::values(&decided.headers, &name).collect();
                    let from_map: Vec<&[u8]> = Fields::values(&map.headers, &name).collect();
                    prop_assert_eq!(from_parts, from_map, "{}", name);
                }
                prop_assert_eq!(&decided.uri, &map.uri);
                prop_assert_eq!(&decided.method, &map.method);
            }

            // Written out as it will be, and read again, it is still the same.
            let written = written(&raw);
            let mut room = [httparse::EMPTY_HEADER; 64];
            let mut again = httparse::Request::new(&mut room);
            prop_assert!(matches!(again.parse(&written), Ok(httparse::Status::Complete(_))));
            for name in every_name() {
                let read: Vec<&[u8]> = again
                    .headers
                    .iter()
                    .filter(|field| field.name.eq_ignore_ascii_case(name.as_str()))
                    .map(|field| field.value)
                    .collect();
                let from_map: Vec<&[u8]> = Fields::values(&map.headers, &name).collect();
                prop_assert_eq!(read, from_map, "{}", name);
            }

            // And as the upstream's head, both ways: the same request, framed by what the
            // writer says and nothing the client said, in as many bytes as it said it would
            // take.
            if by_map.is_ok() {
                use crate::upstream::h1::codec::{Sending, head_len, write_head};
                let limits = crate::upstream::h1::H1Limits::default();
                let sending = Sending::Length(3);
                let mut by_map_head = Vec::new();
                let map_len = head_len(map.method(), map.uri(), &map.headers, sending);
                write_head(&mut by_map_head, map.method(), map.uri(), &map.headers, sending, map_len, &limits)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                let mut by_raw_head = Vec::new();
                let raw_len = head_len(raw.method(), raw.uri(), &raw, sending);
                write_head(&mut by_raw_head, raw.method(), raw.uri(), &raw, sending, raw_len, &limits)
                    .map_err(|error| TestCaseError::fail(error.to_string()))?;
                prop_assert_eq!(by_raw_head.len(), raw_len);
                prop_assert_eq!(by_map_head.len(), map_len);
                let read = |head: &[u8]| -> Vec<(String, Vec<u8>)> {
                    let mut room = [httparse::EMPTY_HEADER; 64];
                    let mut request = httparse::Request::new(&mut room);
                    assert!(request.parse(head).unwrap().is_complete());
                    let mut fields: Vec<(String, Vec<u8>)> = request
                        .headers
                        .iter()
                        .map(|field| (field.name.to_ascii_lowercase(), field.value.to_vec()))
                        .collect();
                    // Order matters within a name only.
                    fields.sort_by(|one, other| one.0.cmp(&other.0));
                    fields
                };
                let (from_map, from_raw) = (read(&by_map_head), read(&by_raw_head));
                prop_assert_eq!(&from_raw, &from_map);
                let framing: Vec<_> = from_raw
                    .iter()
                    .filter(|(name, _)| name == "content-length" || name == "transfer-encoding")
                    .collect();
                prop_assert_eq!(framing, [&("content-length".to_owned(), b"3".to_vec())]);
                let line = |head: &[u8]| head.split(|&byte| byte == b'\n').next().map(<[u8]>::to_vec);
                prop_assert_eq!(line(&by_raw_head), line(&by_map_head));
            }
        }
    }

    /// The head of a request line and a raw head's pieces: its kept lines copied, the
    /// fields added written out.
    fn written(raw: &RawHead) -> Vec<u8> {
        let mut out = b"GET / HTTP/1.1\r\n".to_vec();
        for piece in raw.pieces(&[]) {
            match piece {
                crate::fields::Piece::Copy(span) => out.extend_from_slice(&raw.bytes()[span]),
                crate::fields::Piece::Field(name, value) => {
                    out.extend_from_slice(name.as_str().as_bytes());
                    out.extend_from_slice(b": ");
                    out.extend_from_slice(value.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            }
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    /// A raw head carrying credentials that bind a connection is sent on saying so: its
    /// upstream connection is closed after it, as a map head's is.
    #[test]
    fn a_raw_head_with_credentials_closes_its_upstream_connection() {
        use crate::head::Forwarded;
        use crate::upstream::auth::carries_credentials;
        use crate::upstream::h1::codec::{Sending, head_len, write_head};
        let sent = b"GET /account HTTP/1.1\r\nHost: shop.example.com\r\nAuthorization: NTLM TlRMTVNTUAABAAAA\r\nAccept: */*\r\n\r\n";
        let (_, mut raw) = both_heads(sent).unwrap();
        assert_eq!(decide_on("web", &mut raw, 0).as_deref(), Ok("fallback"));
        assert!(carries_credentials(&raw));
        assert_eq!(raw.close_connection(), Ok(()));
        let close: Vec<&[u8]> = Fields::values(&raw, &http::header::CONNECTION).collect();
        assert_eq!(close, [b"close".as_slice()]);
        let mut out = Vec::new();
        let len = head_len(raw.method(), raw.uri(), &raw, Sending::None);
        write_head(
            &mut out,
            raw.method(),
            raw.uri(),
            &raw,
            Sending::None,
            len,
            &crate::upstream::h1::H1Limits::default(),
        )
        .unwrap();
        let written = String::from_utf8(out).unwrap();
        assert!(written.contains("\r\nconnection: close\r\n"), "{written}");
        assert!(
            written.contains("\r\nAuthorization: NTLM TlRMTVNTUAABAAAA\r\n"),
            "{written}"
        );
    }

    /// What the core does not change is left as it came: a head whose `Host` already says
    /// what its target names, with nothing else to do but tell the upstream of the client,
    /// is copied whole, and what it is told follows.
    #[test]
    fn a_raw_head_the_core_leaves_alone_is_copied_whole() {
        let sent = b"GET http://shop.example.com/account HTTP/1.1\r\nHost: shop.example.com\r\nCookie: a=1; b=2\r\nAccept: */*\r\n\r\n";
        let (_, mut raw) = both_heads(sent).unwrap();
        assert_eq!(decide_on("web", &mut raw, 0).as_deref(), Ok("search"));
        let pieces: Vec<_> = raw.pieces(&[]).collect();
        let section = b"GET http://shop.example.com/account HTTP/1.1\r\n".len()..sent.len() - 2;
        assert_eq!(pieces.first(), Some(&crate::fields::Piece::Copy(section)));
        let added: Vec<(&str, &[u8])> = pieces[1..]
            .iter()
            .map(|piece| match piece {
                crate::fields::Piece::Field(name, value) => (name.as_str(), value.as_bytes()),
                crate::fields::Piece::Copy(span) => panic!("a second run {span:?}"),
            })
            .collect();
        assert_eq!(
            added,
            [
                ("x-forwarded-for", b"203.0.113.7".as_slice()),
                ("x-forwarded-proto", b"http"),
                ("x-forwarded-host", b"shop.example.com"),
                ("via", b"1.1 edgerush"),
            ]
        );
    }
}
