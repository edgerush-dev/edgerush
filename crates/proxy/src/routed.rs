//! What a tunnel was routed by, known across reloads ([03 §10](../../docs/03-data-plane.md)).
//!
//! A tunnel — a passthrough connection, or a WebSocket once it has switched — is routed
//! once, when it opens, as a `udp` listener's session is when it is made (24 §5). A reload that takes away its route, its route's listing of the
//! upstream it went to, or its listener drains it; one that keeps all three, whatever else
//! it changes, does not. So every snapshot gives each listener, route and upstream it has a
//! key, kept from the snapshot before for as long as all three are there by name: a tunnel
//! finds its key by position when it opens, and a worker's sweep sees at a reload which
//! keys went.
//!
//! Pure: what it is made from is a compiled config, and what it hands out comes from the
//! process's [`Keys`].

use crate::upstream::destination::Keys;
use edgerush_config::{Compiled, CompiledListener, L4, Outcome, UpstreamId};
use std::collections::{HashMap, HashSet};

/// Which of its listener's routes a tunnel went by.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum Through {
    /// An HTTP route, at its position as a rule's ID gives it: a WebSocket's.
    Http(usize),
    /// A `tcp` listener's one route.
    Tcp,
    /// A `tls` listener's route at this position among its routes.
    Tls(usize),
    /// A `udp` listener's one route: a session's.
    Udp,
}

/// The keys of what a snapshot's tunnels go by.
#[derive(Debug, Default)]
pub(crate) struct Routed {
    /// By position of the listener, as the data plane numbers them: the key of each route
    /// of it, by [`Through`], and upstream of that route's, by position.
    by_position: Vec<HashMap<(Through, usize), u64>>,
    /// The same by name — listener, route, upstream — for the next snapshot to keep keys by.
    by_name: HashMap<(String, String, String), u64>,
    /// Every key there is.
    live: HashSet<u64>,
}

impl Routed {
    /// The keys of `config`, whose listeners the data plane numbers as `listeners` does:
    /// those of `previous` for what it has by name too, and new ones from `keys` for the
    /// rest, so that a key `previous` had and this has not is gone.
    pub(crate) fn reconcile(
        config: &Compiled,
        listeners: &[Option<String>],
        previous: &Self,
        keys: &Keys,
    ) -> Self {
        let mut routed = Self::default();
        for name in listeners {
            let mut here = HashMap::new();
            let found = name
                .as_ref()
                .and_then(|name| config.listeners().iter().find(|l| l.name == *name));
            if let (Some(name), Some(listener)) = (name, found) {
                for (through, route, upstream) in routes_of(config, listener) {
                    let Some(upstream_name) = config.upstream(upstream).map(|u| u.name.as_str())
                    else {
                        continue;
                    };
                    let named = (name.clone(), route.to_owned(), upstream_name.to_owned());
                    let key = *routed.by_name.entry(named).or_insert_with_key(|named| {
                        previous
                            .by_name
                            .get(named)
                            .copied()
                            .unwrap_or_else(|| keys.next())
                    });
                    here.insert((through, upstream.0), key);
                    routed.live.insert(key);
                }
            }
            routed.by_position.push(here);
        }
        routed
    }

    /// The key of a tunnel of the listener at `listener` that went by `through` to the
    /// upstream at `upstream`, positions all of this snapshot's.
    pub(crate) fn key(&self, listener: usize, through: Through, upstream: usize) -> Option<u64> {
        self.by_position
            .get(listener)?
            .get(&(through, upstream))
            .copied()
    }

    /// Whether `key` is one of this snapshot's.
    pub(crate) fn holds(&self, key: u64) -> bool {
        self.live.contains(&key)
    }
}

/// Every route of `listener`'s and upstream it lists, weight 0 among them, so that a
/// backend retired gently keeps what it carries (03 §10): for an HTTP route, every upstream
/// its rules forward to; a mirror is no route of a tunnel's.
fn routes_of<'a>(
    config: &'a Compiled,
    listener: &'a CompiledListener,
) -> Vec<(Through, &'a str, UpstreamId)> {
    let mut found = Vec::new();
    match &listener.l4 {
        Some(L4::Tcp(route)) => found.extend(
            route
                .backends
                .listed()
                .map(|upstream| (Through::Tcp, route.name.as_str(), upstream)),
        ),
        Some(L4::Tls(routes)) => {
            for (at, route) in routes.routes().iter().enumerate() {
                found.extend(
                    route
                        .backends
                        .listed()
                        .map(|upstream| (Through::Tls(at), route.name.as_str(), upstream)),
                );
            }
        }
        None => {}
    }
    if let Some(route) = &listener.udp {
        found.extend(
            route
                .backends
                .listed()
                .map(|upstream| (Through::Udp, route.name.as_str(), upstream)),
        );
    }
    // None for a `tcp`, `tls` or `udp` listener; an `https` one that passes some through has both.
    for &route in &listener.http_routes {
        let Some(name) = config.route_name(route) else {
            continue;
        };
        for rule in config.rules_of(route) {
            if let Outcome::Forward { backends, .. } = &rule.outcome {
                found.extend(
                    backends
                        .listed()
                        .map(|upstream| (Through::Http(route), name, upstream)),
                );
            }
        }
    }
    found
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Config, compile};

    const LISTENERS: &str = r#"
listeners:
  web: { address: "[::]:80", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
  sni: { address: "[::]:8443", protocol: tls, proxy_protocol: off }
"#;

    const UPSTREAMS: &str = r#"
upstreams:
  one: { load_balancer: p2c, endpoints: { listed: ["10.0.0.1:80"] } }
  two: { load_balancer: p2c, endpoints: { listed: ["10.0.0.2:80"] } }
"#;

    /// A config of the three listeners above with `routes`, `tcp_routes` and `tls_routes`
    /// as given, and the upstreams `one` and `two`.
    fn config(routes: &str, tcp: &str, tls: &str) -> Compiled {
        let yaml = format!(
            "{LISTENERS}routes: [{routes}]\ntcp_routes: [{tcp}]\ntls_routes: [{tls}]\n{UPSTREAMS}"
        );
        compile(&serde_saphyr::from_str::<Config>(&yaml).unwrap()).unwrap()
    }

    fn http(name: &str, path: &str, upstream: &str) -> String {
        format!(
            "{{ name: {name}, listeners: [web], hostnames: [{{ name: \"*\", falls_through: true }}], \
             rules: [{{ matches: [{{ path: {{ prefix: {path} }} }}], forward: {{ backends: [{{ upstream: {upstream}, weight: 1 }}] }} }}] }}"
        )
    }

    fn tcp(upstream: &str, weight: u32) -> String {
        format!(
            "{{ name: db, listeners: [db], backends: [{{ upstream: {upstream}, weight: {weight} }}] }}"
        )
    }

    fn tls(name: &str, host: &str, upstream: &str) -> String {
        format!(
            "{{ name: {name}, listeners: [sni], hostnames: [{{ name: {host}, falls_through: true }}], \
             backends: [{{ upstream: {upstream}, weight: 1 }}] }}"
        )
    }

    /// The listeners' slots as the data plane numbers them: the first config's names, in
    /// order, then a slot no listener has.
    fn listeners() -> Vec<Option<String>> {
        ["db", "sni", "web"]
            .map(|name| Some(name.to_owned()))
            .into_iter()
            .chain([None])
            .collect()
    }

    /// Positions of the configs below: `one` is upstream 0, `two` upstream 1.
    const ONE: usize = 0;
    const TWO: usize = 1;
    const DB: usize = 0;
    const SNI: usize = 1;
    const WEB: usize = 2;

    /// An `https` listener that passes some names through has a key for each of its HTTP
    /// routes and of its TLS routes: a reload that takes either away drains what it carries.
    #[test]
    fn an_https_listener_that_passes_some_through_has_both_kinds_of_route() {
        let yaml = format!(
            r#"
listeners:
  web: {{ address: "[::]:443", protocol: https, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate, tls: {{ certificates: [], without_sni: none, shared_by: [{{ hostnames: [{{ name: "*" }}] }}, {{ hostnames: [{{ name: api.test }}], passes_through: true }}] }} }}
routes: [{http}]
tls_routes: [{tls}]
{UPSTREAMS}"#,
            http = http("chat", "/chat", "one"),
            tls = tls("api", "api.test", "two").replace("[sni]", "[web]"),
        );
        let compiled = compile(&serde_saphyr::from_str::<Config>(&yaml).unwrap()).unwrap();
        let routed = Routed::reconcile(
            &compiled,
            &[Some("web".to_owned()), None],
            &Routed::default(),
            &Keys::default(),
        );
        assert!(routed.key(0, Through::Http(0), ONE).is_some());
        assert!(routed.key(0, Through::Tls(0), TWO).is_some());
    }

    /// A reload that keeps a listener, a route of it and an upstream that route lists keeps
    /// their key, whatever else it changes: weights, hostnames, other rules and routes, and
    /// the route's place among the routes.
    #[test]
    fn a_reload_that_keeps_listener_route_and_upstream_keeps_the_key() {
        let keys = Keys::default();
        let before = Routed::reconcile(
            &config(
                &http("chat", "/chat", "one"),
                &tcp("one", 1),
                &tls("api", "api.test", "two"),
            ),
            &listeners(),
            &Routed::default(),
            &keys,
        );
        let chat = before.key(WEB, Through::Http(0), ONE).unwrap();
        let db = before.key(DB, Through::Tcp, ONE).unwrap();
        let api = before.key(SNI, Through::Tls(0), TWO).unwrap();
        assert_eq!(before.key(WEB, Through::Http(0), TWO), None);

        let after = Routed::reconcile(
            &config(
                &format!(
                    "{}, {}",
                    http("other", "/other", "two"),
                    http("chat", "/talk", "one")
                ),
                &tcp("one", 7),
                &format!(
                    "{}, {}",
                    tls("web", "www.test", "one"),
                    tls("api", "v2.api.test", "two")
                ),
            ),
            &listeners(),
            &before,
            &keys,
        );
        assert_eq!(after.key(WEB, Through::Http(1), ONE), Some(chat));
        assert_eq!(after.key(DB, Through::Tcp, ONE), Some(db));
        assert_eq!(after.key(SNI, Through::Tls(1), TWO), Some(api));
        for key in [chat, db, api] {
            assert!(after.holds(key));
        }
    }

    /// A reload that takes away the route, its listing of the upstream, or the listener
    /// drops the key; and one that brings them back gives a new key, which no tunnel of the
    /// first can have.
    #[test]
    fn a_reload_that_takes_listener_route_or_upstream_away_drops_the_key() {
        let keys = Keys::default();
        let before = Routed::reconcile(
            &config(
                &http("chat", "/chat", "one"),
                &tcp("one", 1),
                &tls("api", "api.test", "two"),
            ),
            &listeners(),
            &Routed::default(),
            &keys,
        );
        let chat = before.key(WEB, Through::Http(0), ONE).unwrap();
        let db = before.key(DB, Through::Tcp, ONE).unwrap();
        let api = before.key(SNI, Through::Tls(0), TWO).unwrap();

        // The route gone, the upstream swapped, the route gone.
        let after = Routed::reconcile(
            &config(
                &http("talk", "/chat", "one"),
                &tcp("two", 1),
                &tls("rest", "api.test", "two"),
            ),
            &listeners(),
            &before,
            &keys,
        );
        for key in [chat, db, api] {
            assert!(!after.holds(key), "{key} kept");
        }
        // Back as they were: new keys.
        let again = Routed::reconcile(
            &config(
                &http("chat", "/chat", "one"),
                &tcp("one", 1),
                &tls("api", "api.test", "two"),
            ),
            &listeners(),
            &after,
            &keys,
        );
        let back = again.key(WEB, Through::Http(0), ONE).unwrap();
        assert!(back != chat && !after.holds(back));

        // The listener gone: none of its keys is kept.
        let without_web = format!(
            "listeners:\n  db: {{ address: \"[::]:5432\", protocol: tcp, proxy_protocol: off }}\n\
             routes: []\ntcp_routes: [{}]\n{UPSTREAMS}",
            tcp("one", 1)
        );
        let gone = Routed::reconcile(
            &compile(&serde_saphyr::from_str::<Config>(&without_web).unwrap()).unwrap(),
            &listeners(),
            &again,
            &keys,
        );
        assert!(!gone.holds(back));
        assert_eq!(gone.key(WEB, Through::Http(0), ONE), None);
        assert_eq!(
            gone.key(DB, Through::Tcp, ONE),
            again.key(DB, Through::Tcp, ONE)
        );
    }

    /// A backend a reload leaves listed at weight 0 keeps its key, on every kind of route:
    /// it takes no new tunnel, and those it has carry on (03 §10). Taken out of the route,
    /// it loses it.
    #[test]
    fn a_backend_left_listed_at_weight_zero_keeps_the_key() {
        let keys = Keys::default();
        let before = Routed::reconcile(
            &config(
                &http("chat", "/chat", "one"),
                &tcp("one", 1),
                &tls("api", "api.test", "two"),
            ),
            &listeners(),
            &Routed::default(),
            &keys,
        );
        let chat = before.key(WEB, Through::Http(0), ONE).unwrap();
        let db = before.key(DB, Through::Tcp, ONE).unwrap();
        let api = before.key(SNI, Through::Tls(0), TWO).unwrap();

        let unweighted = |route: String| route.replace("weight: 1", "weight: 0");
        let retired = Routed::reconcile(
            &config(
                &unweighted(http("chat", "/chat", "one")),
                &tcp("one", 0),
                &unweighted(tls("api", "api.test", "two")),
            ),
            &listeners(),
            &before,
            &keys,
        );
        assert_eq!(retired.key(WEB, Through::Http(0), ONE), Some(chat));
        assert_eq!(retired.key(DB, Through::Tcp, ONE), Some(db));
        assert_eq!(retired.key(SNI, Through::Tls(0), TWO), Some(api));
        for key in [chat, db, api] {
            assert!(retired.holds(key), "{key} dropped");
        }

        let taken_out = Routed::reconcile(
            &config(
                &http("chat", "/chat", "two"),
                &tcp("two", 1),
                &tls("api", "api.test", "one"),
            ),
            &listeners(),
            &retired,
            &keys,
        );
        for key in [chat, db, api] {
            assert!(!taken_out.holds(key), "{key} kept");
        }
    }

    /// A `udp` listener's route keys its sessions as a `tcp` one's keys its tunnels: kept
    /// while the listener, the route and its listing of the upstream are, whatever the
    /// weights; dropped once the route or the listing goes.
    #[test]
    fn a_udp_listeners_route_keys_its_sessions() {
        let udp = |route: &str, backends: &str| {
            let yaml = format!(
                "listeners:\n  dns: {{ address: \"[::]:53\", protocol: udp }}\n\
                 routes: []\n\
                 udp_routes: [{{ name: {route}, listeners: [dns], backends: [{backends}] }}]\n\
                 upstreams:\n  \
                 one: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"10.0.0.1:53\"] }} }}\n  \
                 two: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"10.0.0.2:53\"] }} }}\n"
            );
            compile(&serde_saphyr::from_str::<Config>(&yaml).unwrap()).unwrap()
        };
        let keys = Keys::default();
        let dns = [Some("dns".to_owned()), None];
        let before = Routed::reconcile(
            &udp("dns", "{ upstream: one, weight: 1 }"),
            &dns,
            &Routed::default(),
            &keys,
        );
        let one = before.key(0, Through::Udp, ONE).unwrap();
        assert_eq!(before.key(0, Through::Tcp, ONE), None);

        let reweighted = Routed::reconcile(
            &udp(
                "dns",
                "{ upstream: one, weight: 3 }, { upstream: two, weight: 1 }",
            ),
            &dns,
            &before,
            &keys,
        );
        assert_eq!(reweighted.key(0, Through::Udp, ONE), Some(one));
        assert!(reweighted.holds(one));

        let retired = Routed::reconcile(
            &udp(
                "dns",
                "{ upstream: one, weight: 0 }, { upstream: two, weight: 1 }",
            ),
            &dns,
            &reweighted,
            &keys,
        );
        assert_eq!(retired.key(0, Through::Udp, ONE), Some(one));

        let listing_gone = Routed::reconcile(
            &udp("dns", "{ upstream: two, weight: 1 }"),
            &dns,
            &reweighted,
            &keys,
        );
        assert!(!listing_gone.holds(one));
        let two = listing_gone.key(0, Through::Udp, TWO).unwrap();
        let renamed = Routed::reconcile(
            &udp("names", "{ upstream: two, weight: 1 }"),
            &dns,
            &listing_gone,
            &keys,
        );
        assert!(!renamed.holds(two));
    }
}
