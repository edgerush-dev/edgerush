//! Where a `udp` listener's sessions go and why
//! ([22 §4](../../../docs/22-explain-and-test.md)): the listener's one UDP route, whose
//! backends a new client's first datagram picks from by weight, as the proxy picks it
//! ([24 §3](../../../docs/24-udp-proxying.md)); or, where nothing can be carried, why every
//! datagram is dropped, by the name the metrics give it.

use crate::explained::{Snapshot, Unexplained, field};
use edgerush_config::{Backend, CompiledListener, L4Route, Protocol};

/// What becomes of the datagrams a `udp` listener is sent.
#[derive(Debug)]
pub struct Datagrams<'s> {
    snapshot: &'s Snapshot,
    listener: &'s CompiledListener,
    route: Option<&'s L4Route>,
}

impl Snapshot {
    /// Decides what becomes of the datagrams sent to `listener`, a `udp` one: its route
    /// alone decides, whoever the client.
    ///
    /// # Errors
    ///
    /// For a listener that is not `udp`.
    pub fn explain_datagrams<'s>(
        &'s self,
        listener: &'s CompiledListener,
    ) -> Result<Datagrams<'s>, Unexplained> {
        if listener.protocol != Protocol::Udp {
            return Err(Unexplained::NotUdp(listener.name.clone()));
        }
        Ok(Datagrams {
            snapshot: self,
            listener,
            route: listener.udp.as_ref(),
        })
    }
}

impl<'s> Datagrams<'s> {
    /// The route sessions go by, by name.
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
        self.snapshot
            .config
            .udp_routes
            .iter()
            .find(|route| route.name == name)
            .map_or(&[], |route| route.backends.as_slice())
    }

    /// Why every datagram is dropped, by the name the metrics give it: `no_route` for a
    /// listener no route serves, `invalid_backend` for a route with no valid backend that
    /// has a share; `None` where sessions are made, if only for the share an invalid
    /// backend drops.
    #[must_use]
    pub fn dropped(&self) -> Option<&'static str> {
        match self.route {
            None => Some("no_route"),
            Some(route) if route.backends.upstreams().next().is_none() => Some("invalid_backend"),
            Some(_) => None,
        }
    }

    /// The explanation, as text.
    #[must_use]
    pub fn text(&self) -> String {
        let mut lines = vec![format!("{} (udp)", self.listener.name), String::new()];
        match self.route() {
            Some(route) => lines.push(format!("→ {route}  the listener's one route")),
            None => lines.push("  no route serves this listener".to_owned()),
        }
        lines.push(String::new());
        if let Some(route) = self.route() {
            field(&mut lines, "route", vec![route.to_owned()]);
        }
        match self.dropped() {
            Some(reason) => field(&mut lines, "dropped", vec![reason.to_owned()]),
            None => {
                let backends: Vec<String> =
                    self.backends().iter().map(ToString::to_string).collect();
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

    /// `udp` listeners: one served by a weighted route, one by a route whose one backend is
    /// invalid, one by a route whose every weight is 0, one no route serves; a `tcp` one.
    const CONFIG: &str = r#"
listeners:
  dns: { address: "[::]:53", protocol: udp }
  gone: { address: "[::]:54", protocol: udp }
  idle: { address: "[::]:55", protocol: udp }
  bare: { address: "[::]:56", protocol: udp }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
routes: []
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: pg, weight: 1 }] }
udp_routes:
  - { name: resolvers, listeners: [dns], backends: [{ upstream: dns-a, weight: 3 }, { upstream: dns-b, weight: 1 }, { upstream: dns-old, weight: 0 }] }
  - { name: missing, listeners: [gone], backends: [{ invalid: "Service shop/dns does not exist", weight: 1 }] }
  - { name: kept, listeners: [idle], backends: [{ upstream: dns-a, weight: 0 }] }
upstreams:
  dns-a: { protocol: udp, load_balancer: p2c, endpoints: { listed: [] } }
  dns-b: { protocol: udp, load_balancer: p2c, endpoints: { listed: [] } }
  dns-old: { protocol: udp, load_balancer: p2c, endpoints: { listed: [] } }
  pg: { load_balancer: p2c, endpoints: { listed: [] } }
"#;

    fn snapshot() -> Snapshot {
        Snapshot::new(serde_saphyr::from_str(CONFIG).unwrap()).unwrap()
    }

    /// The text for datagrams to `listener`.
    fn explained(listener: &str) -> String {
        let snapshot = snapshot();
        let listener = snapshot.listener(listener).unwrap();
        snapshot.explain_datagrams(listener).unwrap().text()
    }

    #[test]
    fn a_udp_listeners_sessions_go_by_its_one_route() {
        assert_eq!(
            explained("dns"),
            "\
dns (udp)

→ resolvers  the listener's one route

route     resolvers
backends  dns-a (weight 3), dns-b (weight 1), dns-old (weight 0)
"
        );
    }

    #[test]
    fn datagrams_with_no_route_or_no_backend_are_dropped() {
        assert_eq!(
            explained("bare"),
            "\
bare (udp)

  no route serves this listener

dropped   no_route
"
        );
        // A route whose one backend is invalid, or whose every weight is 0, makes no
        // session: each datagram is dropped.
        assert_eq!(
            explained("gone"),
            "\
gone (udp)

→ missing  the listener's one route

route     missing
dropped   invalid_backend
"
        );
        assert_eq!(
            explained("idle"),
            "\
idle (udp)

→ kept  the listener's one route

route     kept
dropped   invalid_backend
"
        );
    }

    #[test]
    fn what_a_test_reads_of_datagrams_is_their_route_backends_and_drop() {
        let snapshot = snapshot();
        let dns = snapshot.listener("dns").unwrap();
        let sessions = snapshot.explain_datagrams(dns).unwrap();
        assert_eq!(sessions.route(), Some("resolvers"));
        let backends: Vec<(&str, u32)> = sessions
            .backends()
            .iter()
            .map(|backend| {
                (
                    backend.upstream.as_deref().unwrap_or_default(),
                    backend.weight,
                )
            })
            .collect();
        assert_eq!(backends, [("dns-a", 3), ("dns-b", 1), ("dns-old", 0)]);
        assert_eq!(sessions.dropped(), None);
        let bare = snapshot.listener("bare").unwrap();
        let none = snapshot.explain_datagrams(bare).unwrap();
        assert_eq!((none.route(), none.dropped()), (None, Some("no_route")));
        assert!(none.backends().is_empty());
    }

    #[test]
    fn only_a_udp_listener_takes_datagrams_and_takes_nothing_else() {
        let snapshot = snapshot();
        let db = snapshot.listener("db").unwrap();
        assert_eq!(
            snapshot.explain_datagrams(db).err(),
            Some(Unexplained::NotUdp("db".to_owned()))
        );
        let dns = snapshot.listener("dns").unwrap();
        assert_eq!(
            snapshot.explain_connection(dns, None).err(),
            Some(Unexplained::Udp("dns".to_owned()))
        );
    }
}
