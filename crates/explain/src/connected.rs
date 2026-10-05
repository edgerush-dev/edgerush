//! Where a connection to a `tcp` or `tls` listener goes and why
//! ([22 §4](../../../docs/22-explain-and-test.md)): a `tcp` listener's one route; a `tls`
//! listener's by the name its ClientHello asks for, chosen as the passthrough path chooses
//! it ([`L4::route`]), every route for the name listed as the router crate's walk ranks
//! them, which is held to that choice.

use crate::explained::{Snapshot, Unexplained, field, protocol_name, verdict};
use edgerush_config::{Backend, CompiledListener, L4, L4Route};
use edgerush_router::{Explanation, RequestParts, Verdict, explain};
use http::Method;
use http::header::HeaderMap;

/// What became of a connection, and the walk beside its routing.
#[derive(Debug)]
pub struct Connected<'s> {
    snapshot: &'s Snapshot,
    listener: &'s CompiledListener,
    l4: &'s L4,
    /// The name the ClientHello asked for, if it asked for one; always `None` for `tcp`.
    sni: Option<String>,
    /// The walk over a `tls` listener's routes, for a ClientHello that asks for a name.
    walk: Option<Explanation<'s, usize>>,
    route: Option<&'s L4Route>,
}

impl Snapshot {
    /// Decides a connection to `listener`, a `tcp` or `tls` one, whose ClientHello asks for
    /// `sni`: as the passthrough path decides it, with the walk beside.
    ///
    /// # Errors
    ///
    /// For a listener that is not `tcp` or `tls`, or a walk that disagrees with the route
    /// chosen.
    pub fn explain_connection<'s>(
        &'s self,
        listener: &'s CompiledListener,
        sni: Option<&str>,
    ) -> Result<Connected<'s>, Unexplained> {
        let Some(l4) = &listener.l4 else {
            return Err(Unexplained::NotPassthrough(
                listener.name.clone(),
                protocol_name(listener.protocol),
            ));
        };
        let sni = match l4 {
            L4::Tcp(_) => None,
            L4::Tls(_) => sni,
        };
        let none = HeaderMap::new();
        let walk = sni.map(|name| {
            let asked = RequestParts {
                host: name,
                path: "/",
                query: "",
                method: &Method::GET,
                headers: &none,
            };
            explain(self.matches.sni_of(&listener.name), &asked)
        });
        let routed = l4.route(sni);
        if let Some(walk) = &walk
            && walk.chosen().map(|chosen| chosen.value) != routed.and_then(|(at, _)| at)
        {
            return Err(Unexplained::Disagreed);
        }
        Ok(Connected {
            snapshot: self,
            listener,
            l4,
            sni: sni.map(str::to_owned),
            walk,
            route: routed.map(|(_, route)| route),
        })
    }
}

impl<'s> Connected<'s> {
    /// The route the connection goes by, by name.
    #[must_use]
    pub fn route(&self) -> Option<&'s str> {
        self.route.map(|route| route.name.as_str())
    }

    /// The route's backends as the config states them; none without a route.
    #[must_use]
    pub fn backends(&self) -> &'s [Backend] {
        let Some(name) = self.route() else {
            return &[];
        };
        let config = &self.snapshot.config;
        let tcp = config.tcp_routes.iter().find(|route| route.name == name);
        let tls = config.tls_routes.iter().find(|route| route.name == name);
        tcp.map(|route| route.backends.as_slice())
            .or_else(|| tls.map(|route| route.backends.as_slice()))
            .unwrap_or(&[])
    }

    /// Why the connection is refused, by the name the metrics give it: `no_route` for one
    /// no route is for, `no_backend` for a route with no backend that has a share of its
    /// connections; `None` for one that goes on.
    #[must_use]
    pub fn refused(&self) -> Option<&'static str> {
        match self.route {
            None => Some("no_route"),
            Some(route) if route.backends.pick(0).is_none() => Some("no_backend"),
            Some(_) => None,
        }
    }

    /// The explanation, as text.
    #[must_use]
    pub fn text(&self) -> String {
        let listener = self.listener;
        let kind = protocol_name(listener.protocol);
        let asked = match (self.l4, &self.sni) {
            (L4::Tcp(_), _) => String::new(),
            (L4::Tls(_), Some(name)) => format!("  SNI {name}"),
            (L4::Tls(_), None) => "  no SNI".to_owned(),
        };
        let mut lines = vec![format!("{} ({kind}){asked}", listener.name), String::new()];
        match (self.l4, &self.walk) {
            (L4::Tcp(route), _) => {
                lines.push(format!("→ {}  the listener's one route", route.name));
            }
            (L4::Tls(_), None) => {
                lines.push("  a ClientHello that asks for no name has no route".to_owned());
            }
            (L4::Tls(router), Some(walk)) => {
                let label = |at: usize| {
                    router
                        .routes()
                        .get(at)
                        .map_or("?", |route| route.name.as_str())
                };
                let chosen = walk.chosen().map(|chosen| label(chosen.value).to_owned());
                let width = walk
                    .considered
                    .iter()
                    .map(|considered| label(considered.route_match.value).len())
                    .max()
                    .unwrap_or(0);
                for considered in &walk.considered {
                    let marker = if matches!(considered.verdict, Verdict::Chosen) {
                        '→'
                    } else {
                        ' '
                    };
                    let why = verdict(
                        &considered.verdict,
                        "/",
                        &Method::GET,
                        chosen.clone(),
                        "route",
                    );
                    lines.push(format!(
                        "{marker} {:<width$}  {why}",
                        label(considered.route_match.value)
                    ));
                }
                if walk.considered.is_empty() {
                    lines.push("  no route is for this name".to_owned());
                }
                match walk.other_hosts {
                    0 => {}
                    1 => lines.push("  1 route for other hosts".to_owned()),
                    many => lines.push(format!("  {many} routes for other hosts")),
                }
            }
        }
        lines.push(String::new());
        if let Some(route) = self.route() {
            field(&mut lines, "route", vec![route.to_owned()]);
        }
        match self.refused() {
            Some(reason) => field(&mut lines, "refused", vec![reason.to_owned()]),
            None => {
                let backends: Vec<String> = self
                    .backends()
                    .iter()
                    .map(|backend| format!("{} (weight {})", backend.upstream, backend.weight))
                    .collect();
                field(&mut lines, "backends", vec![backends.join(", ")]);
            }
        }
        let mut text = lines.join("\n");
        text.push('\n');
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tcp listener; a tls one whose routes are chosen by exact name, by wildcard, by a
    /// wildcard that does not fall through, and one with nowhere to send; an http one.
    const CONFIG: &str = r#"
listeners:
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
  sni: { address: "[::]:443", protocol: tls, proxy_protocol: off }
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes: []
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: pg, weight: 1 }] }
tls_routes:
  - { name: rest, listeners: [sni], hostnames: [{ name: "*.example.com", wildcard: any_labels, falls_through: true }], backends: [{ upstream: api, weight: 1 }] }
  - { name: api, listeners: [sni], hostnames: [{ name: api.example.com, falls_through: true }], backends: [{ upstream: api, weight: 3 }, { upstream: api-canary, weight: 1 }] }
  - { name: kept, listeners: [sni], hostnames: [{ name: "*.example.org", wildcard: one_label, falls_through: false }], backends: [{ upstream: api, weight: 0 }] }
  - { name: org, listeners: [sni], hostnames: [{ name: www.example.org, falls_through: true }], backends: [{ upstream: api, weight: 1 }] }
upstreams:
  api: { load_balancer: p2c, endpoints: [] }
  api-canary: { load_balancer: p2c, endpoints: [] }
  pg: { load_balancer: p2c, endpoints: [] }
"#;

    fn snapshot() -> Snapshot {
        Snapshot::new(serde_saphyr::from_str(CONFIG).unwrap()).unwrap()
    }

    /// The text for a connection to `listener` asking for `sni`.
    fn explained(listener: &str, sni: Option<&str>) -> String {
        let snapshot = snapshot();
        let listener = snapshot.listener(listener).unwrap();
        snapshot.explain_connection(listener, sni).unwrap().text()
    }

    #[test]
    fn a_tcp_listeners_connection_goes_by_its_one_route() {
        let text = "\
db (tcp)

→ postgres  the listener's one route

route     postgres
backends  pg (weight 1)
";
        assert_eq!(explained("db", None), text);
        // A name means nothing to a tcp listener.
        assert_eq!(explained("db", Some("api.example.com")), text);
    }

    #[test]
    fn a_tls_listeners_routes_for_the_name_are_ranked_and_the_rest_counted() {
        assert_eq!(
            explained("sni", Some("api.example.com")),
            "\
sni (tls)  SNI api.example.com

→ api   chosen
  rest  outranked by api (host)
  2 routes for other hosts

route     api
backends  api (weight 3), api-canary (weight 1)
"
        );
        assert_eq!(
            explained("sni", Some("WWW.example.org")),
            "\
sni (tls)  SNI WWW.example.org

→ org   chosen
  kept  www.example.org claims this host, and this route's hostname does not fall through
  2 routes for other hosts

route     org
backends  api (weight 1)
"
        );
    }

    #[test]
    fn a_connection_with_no_route_or_no_backend_is_refused() {
        assert_eq!(
            explained("sni", Some("elsewhere.test")),
            "\
sni (tls)  SNI elsewhere.test

  no route is for this name
  4 routes for other hosts

refused   no_route
"
        );
        assert_eq!(
            explained("sni", None),
            "\
sni (tls)  no SNI

  a ClientHello that asks for no name has no route

refused   no_route
"
        );
        assert_eq!(
            explained("sni", Some("a.example.org")),
            "\
sni (tls)  SNI a.example.org

→ kept  chosen
  3 routes for other hosts

route     kept
refused   no_backend
"
        );
    }

    #[test]
    fn what_a_test_reads_of_a_connection_is_its_route_backends_and_refusal() {
        let snapshot = snapshot();
        let sni = snapshot.listener("sni").unwrap();
        let api = snapshot
            .explain_connection(sni, Some("api.example.com"))
            .unwrap();
        assert_eq!(api.route(), Some("api"));
        let backends: Vec<(&str, u32)> = api
            .backends()
            .iter()
            .map(|backend| (backend.upstream.as_str(), backend.weight))
            .collect();
        assert_eq!(backends, [("api", 3), ("api-canary", 1)]);
        assert_eq!(api.refused(), None);
        let none = snapshot.explain_connection(sni, None).unwrap();
        assert_eq!((none.route(), none.refused()), (None, Some("no_route")));
        assert!(none.backends().is_empty());
    }

    #[test]
    fn an_http_listener_takes_requests_not_bare_connections() {
        let snapshot = snapshot();
        let web = snapshot.listener("web").unwrap();
        assert_eq!(
            snapshot.explain_connection(web, None).err(),
            Some(Unexplained::NotPassthrough("web".to_owned(), "http"))
        );
    }
}
