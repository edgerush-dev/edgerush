use super::*;
use edgerush_config::{Config, compile};
use edgerush_dns::testing::{Reply, answer};
use edgerush_proxy::Proxy;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Mutex;
use tokio::net::TcpListener;

/// What a stand-in nameserver sends back to a query: the datagrams, or over TCP the
/// message, for its kind, A or AAAA, and whether it came over TCP.
pub(crate) type Replies = Arc<dyn Fn(&[u8], Kind, bool) -> Vec<Vec<u8>> + Send + Sync>;

/// A nameserver on a port of its own, over UDP and TCP, answering as `replies` says.
pub(crate) async fn nameserver(replies: Replies) -> SocketAddr {
    let (udp, tcp) = loop {
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        if let Ok(tcp) = TcpListener::bind(udp.local_addr().unwrap()).await {
            break (udp, tcp);
        }
    };
    let address = udp.local_addr().unwrap();
    let answering = Arc::clone(&replies);
    tokio::spawn(async move {
        let mut buffer = [0; 1500];
        while let Ok((length, peer)) = udp.recv_from(&mut buffer).await {
            let query = &buffer[..length];
            for datagram in answering(query, kind_of(query), false) {
                let _ = udp.send_to(&datagram, peer).await;
            }
        }
    });
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = tcp.accept().await {
            let replies = Arc::clone(&replies);
            tokio::spawn(async move {
                let mut length = [0; 2];
                stream.read_exact(&mut length).await.unwrap();
                let mut query = vec![0; usize::from(u16::from_be_bytes(length))];
                stream.read_exact(&mut query).await.unwrap();
                if let Some(message) = replies(&query, kind_of(&query), true).first() {
                    let length = u16::try_from(message.len()).unwrap().to_be_bytes();
                    stream.write_all(&length).await.unwrap();
                    stream.write_all(message).await.unwrap();
                }
            });
        }
    });
    address
}

/// Which records a query asks for: the kind before its class, before the OPT record of 11
/// bytes it ends with.
fn kind_of(query: &[u8]) -> Kind {
    let at = query.len() - 11 - 4;
    if u16::from_be_bytes([query[at], query[at + 1]]) == 28 {
        Kind::Aaaa
    } else {
        Kind::A
    }
}

pub(crate) fn replying(
    reply: impl Fn(Kind, bool) -> Option<Reply> + Send + Sync + 'static,
) -> Replies {
    Arc::new(move |query, kind, tcp| {
        reply(kind, tcp)
            .and_then(|reply| answer(query, &reply))
            .into_iter()
            .collect()
    })
}

fn ip(address: &str) -> IpAddr {
    address.parse().unwrap()
}

const PATIENCE: Duration = Duration::from_millis(300);

async fn looked(servers: &[SocketAddr]) -> Looked {
    let mut id = 0;
    look_up(servers, "web.shop.svc.cluster.local", PATIENCE, || {
        id += 1;
        id
    })
    .await
}

#[tokio::test]
async fn both_kinds_are_asked_and_their_addresses_kept_for_the_least_time_to_live() {
    let server = nameserver(replying(|kind, _| {
        Some(match kind {
            Kind::A => Reply::Addresses(vec![ip("10.0.0.2"), ip("10.0.0.1")], 30),
            Kind::Aaaa => Reply::Addresses(vec![ip("fd00::1")], 60),
        })
    }))
    .await;
    assert_eq!(
        looked(&[server]).await,
        Looked::Answered(
            vec![ip("10.0.0.1"), ip("10.0.0.2"), ip("fd00::1")],
            Duration::from_secs(30)
        )
    );
    // A name with A records alone: those, for their time to live, not the other's.
    let four = nameserver(replying(|kind, _| {
        Some(match kind {
            Kind::A => Reply::Addresses(vec![ip("10.0.0.1")], 30),
            Kind::Aaaa => Reply::NoSuchName(5),
        })
    }))
    .await;
    assert_eq!(
        looked(&[four]).await,
        Looked::Answered(vec![ip("10.0.0.1")], Duration::from_secs(30))
    );
    // No such name: none, for the zone's negative time to live.
    let gone = nameserver(replying(|_, _| Some(Reply::NoSuchName(20)))).await;
    assert_eq!(
        looked(&[gone]).await,
        Looked::Answered(Vec::new(), Duration::from_secs(20))
    );
}

#[tokio::test]
async fn an_answer_cut_short_is_asked_for_again_over_tcp() {
    let server = nameserver(replying(|_, tcp| {
        Some(if tcp {
            Reply::Addresses(vec![ip("10.0.0.7")], 30)
        } else {
            Reply::Truncated
        })
    }))
    .await;
    assert_eq!(
        looked(&[server]).await,
        Looked::Answered(vec![ip("10.0.0.7")], Duration::from_secs(30))
    );
}

#[tokio::test]
async fn a_datagram_that_is_no_answer_to_the_query_is_passed_over() {
    let server = nameserver(Arc::new(|query: &[u8], _, _| {
        let real = answer(query, &Reply::Addresses(vec![ip("10.0.0.1")], 30)).unwrap();
        let mut forged = answer(query, &Reply::Addresses(vec![ip("192.0.2.66")], 30)).unwrap();
        forged[0] ^= 0xff;
        vec![forged, real]
    }))
    .await;
    assert_eq!(
        looked(&[server]).await,
        Looked::Answered(vec![ip("10.0.0.1")], Duration::from_secs(30))
    );
}

#[tokio::test]
async fn a_nameserver_that_fails_or_is_silent_is_passed_for_the_next() {
    let failing = nameserver(replying(|_, _| Some(Reply::Failed))).await;
    let silent = nameserver(replying(|_, _| None)).await;
    let good = nameserver(replying(|_, _| {
        Some(Reply::Addresses(vec![ip("10.0.0.1")], 30))
    }))
    .await;
    assert_eq!(
        looked(&[failing, silent, good]).await,
        Looked::Answered(vec![ip("10.0.0.1")], Duration::from_secs(30))
    );
    let Looked::Failed(Failure::Unanswered(why)) = looked(&[failing, silent]).await else {
        panic!("answered by nameservers that fail")
    };
    assert_eq!(why, format!("{silent}: no answer within {PATIENCE:?}"));
    let Looked::Failed(Failure::Unanswered(why)) = looked(&[silent, failing]).await else {
        panic!("answered by nameservers that fail")
    };
    assert_eq!(why, format!("{failing} failed, response code 2"));
    assert_eq!(looked(&[]).await, Looked::Failed(Failure::NoNameserver));
}

/// What hands `proxy` what its names resolve to, as the data plane's harness does.
fn taking(
    proxy: &Arc<Proxy>,
) -> impl Fn(&str, Option<Vec<IpAddr>>) -> Result<(), String> + 'static {
    let proxy = Arc::clone(proxy);
    move |name, addresses| {
        proxy
            .reresolve([(name.to_owned(), addresses)])
            .map(|_| ())
            .map_err(|error| error.to_string())
    }
}

/// How many endpoints the proxy serves upstream `web` with, as its metrics say.
fn endpoints(proxy: &Proxy) -> Option<u64> {
    let scraped = proxy.metrics();
    let line = scraped
        .lines()
        .find(|line| line.starts_with("edgerush_upstream_healthy_endpoints{upstream=\"web\"}"))?;
    line.rsplit(' ').next()?.parse().ok()
}

async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never {what}"));
}

/// Each name the running config resolves is looked up, what it resolves to taken in, and
/// looked up again once its time to live has run out, never sooner than the floor; a lookup
/// that fails keeps what was taken in and is said once; and an answer that the name has no
/// addresses is taken in.
#[tokio::test(flavor = "current_thread")]
async fn each_name_is_resolved_again_as_it_runs_out_and_a_failure_keeps_the_last() {
    let reply: Arc<Mutex<Option<Reply>>> =
        Arc::new(Mutex::new(Some(Reply::Addresses(vec![ip("10.0.0.1")], 0))));
    let replies = Arc::clone(&reply);
    let asked = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::clone(&asked);
    let server = nameserver(replying(move |kind, _| {
        counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let reply = replies.lock().unwrap().clone()?;
        Some(match (kind, reply) {
            (Kind::Aaaa, Reply::Addresses(..)) => Reply::NoSuchName(0),
            (_, reply) => reply,
        })
    }))
    .await;
    let config: Config = serde_saphyr::from_str(
        "listeners: {}\nroutes: []\nupstreams:\n  web: { load_balancer: p2c, endpoints: { dns: \"web.shop.svc.cluster.local:8080\" } }\n",
    )
    .unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let said: Rc<RefCellLines> = Rc::default();
    let heard = Rc::clone(&said);
    let pace = Pace {
        floor: Duration::from_millis(50),
        patience: PATIENCE,
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let resolving = tokio::task::spawn_local(resolve_names(
                proxy.names(),
                vec![server],
                pace,
                taking(&proxy),
                move |name, what| heard.0.borrow_mut().push(format!("{name}: {what}")),
            ));
            until("resolved", || proxy.unresolved().is_empty()).await;
            until("served with its address", || endpoints(&proxy) == Some(1)).await;
            // Its time to live run out, it is looked up again.
            *reply.lock().unwrap() =
                Some(Reply::Addresses(vec![ip("10.0.0.1"), ip("10.0.0.2")], 0));
            until("served with both", || endpoints(&proxy) == Some(2)).await;
            // A time to live of none is kept for the floor: two queries, A and AAAA, every
            // 50 ms, not a lookup that spins.
            let before = asked.load(std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(500)).await;
            let during = asked.load(std::sync::atomic::Ordering::SeqCst) - before;
            assert!((4..=24).contains(&during), "{during} queries in 500 ms");
            // No answer: what was taken in is kept, and that is said once.
            *reply.lock().unwrap() = None;
            until("said to fail", || !said.0.borrow().is_empty()).await;
            tokio::time::sleep(Duration::from_millis(800)).await;
            assert_eq!(endpoints(&proxy), Some(2));
            assert_eq!(
                *said.0.borrow(),
                [format!(
                    "web.shop.svc.cluster.local: {server}: no answer within {PATIENCE:?}"
                )]
            );
            // The name gone: none.
            *reply.lock().unwrap() = Some(Reply::NoSuchName(0));
            until("served with none", || endpoints(&proxy) == Some(0)).await;
            resolving.abort();
        })
        .await;
}

/// A name whose first lookup fails is taken in as having none, so that nothing waits on it.
#[tokio::test(flavor = "current_thread")]
async fn a_name_whose_first_lookup_fails_is_taken_in_as_none() {
    let config: Config = serde_saphyr::from_str(
        "listeners: {}\nroutes: []\nupstreams:\n  web: { load_balancer: p2c, endpoints: { dns: \"web.shop.svc.cluster.local:8080\" } }\n",
    )
    .unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    assert!(!proxy.unresolved().is_empty());
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let resolving = tokio::task::spawn_local(resolve_names(
                proxy.names(),
                Vec::new(),
                Pace::default(),
                taking(&proxy),
                |_, _| {},
            ));
            until("taken in", || proxy.unresolved().is_empty()).await;
            assert_eq!(endpoints(&proxy), Some(0));
            resolving.abort();
        })
        .await;
}

#[derive(Default)]
struct RefCellLines(std::cell::RefCell<Vec<String>>);
