//! Instruction counts for a datagram's path through a `udp` listener
//! ([24 §3](../../../docs/24-udp-proxying.md)): a client's datagram read off the
//! listener's socket, carried on its session's socket to an echoing backend, and the
//! answer read off that socket and sent back from the listener's, over loopback on one
//! worker. The same round trips made straight to the backend are the baseline: what the
//! two differ by, a round trip, is the gateway's work for a datagram each way, the
//! kernel's not counted.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench udp_path`, see the repository
//! README.

#![allow(
    clippy::expect_used,
    reason = "benchmark set-up over fixed inputs, not production code"
)]
#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_config::{Config, compile};
use edgerush_proxy::{Proxy, Worker};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::rc::Rc;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::runtime::Runtime;
use tokio::task::LocalSet;

/// Round trips a benchmark makes.
const TRIPS: usize = 100;

/// A DNS query's size.
const DATAGRAM: [u8; 64] = [7; 64];

/// A runtime with a worker serving a `udp` listener whose one route goes to an echoing
/// backend, and clients of it: what is measured runs on it, set up beforehand so that none
/// of the setting up is counted.
struct Served {
    runtime: Runtime,
    local: LocalSet,
    /// Where the clients send: the listener, or the backend itself for the baseline.
    to: SocketAddr,
    clients: Vec<UdpSocket>,
}

/// `clients` clients, each of which has had one round trip made already, through the
/// listener or, `direct`, straight to the backend.
fn served(direct: bool, clients: usize) -> Served {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let local = LocalSet::new();
    let (to, clients) = local.block_on(&runtime, async move {
        let backend = UdpSocket::bind("127.0.0.1:0").await.expect("a backend");
        let at = backend.local_addr().expect("its address");
        tokio::task::spawn_local(async move {
            let mut buffer = [0; 2048];
            while let Ok((len, from)) = backend.recv_from(&mut buffer).await {
                let _sent = backend.send_to(&buffer[..len], from).await;
            }
        });
        let yaml = format!(
            "listeners: {{ dns: {{ address: \"127.0.0.1:0\", protocol: udp }} }}\n\
             routes: []\n\
             udp_routes: [{{ name: dns, listeners: [dns], backends: [{{ upstream: up, weight: 1 }}] }}]\n\
             upstreams: {{ up: {{ protocol: udp, load_balancer: p2c, endpoints: {{ listed: [\"{at}\"] }} }} }}\n"
        );
        let config: Config = serde_saphyr::from_str(&yaml).expect("a config");
        let proxy = Proxy::new(compile(&config).expect("compiled"), NonZeroUsize::MIN)
            .expect("a data plane");
        let worker = Worker::new(Arc::new(proxy));
        let socket = UdpSocket::bind("127.0.0.1:0").await.expect("a listener");
        let front = socket.local_addr().expect("its address");
        let serving = Rc::clone(&worker).serve_udp(0, socket).expect("served");
        tokio::task::spawn_local(serving);
        let to = if direct { at } else { front };
        let mut made = Vec::with_capacity(clients);
        for _ in 0..clients {
            let client = UdpSocket::bind("127.0.0.1:0").await.expect("a client");
            client.connect(to).await.expect("connected");
            round_trip(&client).await;
            made.push(client);
        }
        (to, made)
    });
    Served {
        runtime,
        local,
        to,
        clients,
    }
}

/// One datagram sent and its echo taken.
async fn round_trip(client: &UdpSocket) {
    client.send(&DATAGRAM).await.expect("sent");
    let mut answer = [0; 2048];
    let len = client.recv(&mut answer).await.expect("answered");
    assert_eq!(len, DATAGRAM.len());
}

/// [`TRIPS`] round trips of one client, which has its session.
fn trips_of_one(served: &Served) {
    let client = served.clients.first().expect("a client");
    served.local.block_on(&served.runtime, async {
        for _ in 0..TRIPS {
            round_trip(black_box(client)).await;
        }
    });
}

#[library_benchmark]
#[bench::through_the_listener(served(false, 1))]
#[bench::straight_to_the_backend(served(true, 1))]
fn known_client(served: Served) -> SocketAddr {
    trips_of_one(&served);
    black_box(served.to)
}

/// [`TRIPS`] clients that have sent nothing yet, through the listener, where each first
/// datagram makes a session, its socket opened and connected; or, `direct`, straight to
/// the backend.
fn fresh(direct: bool) -> Served {
    let mut served = served(direct, 0);
    let to = served.to;
    served.clients = served.local.block_on(&served.runtime, async move {
        let mut clients = Vec::with_capacity(TRIPS);
        for _ in 0..TRIPS {
            let client = UdpSocket::bind("127.0.0.1:0").await.expect("a client");
            client.connect(to).await.expect("connected");
            clients.push(client);
        }
        clients
    });
    served
}

#[library_benchmark]
#[bench::through_the_listener(fresh(false))]
#[bench::straight_to_the_backend(fresh(true))]
fn new_client(served: Served) -> SocketAddr {
    served.local.block_on(&served.runtime, async {
        for client in &served.clients {
            round_trip(black_box(client)).await;
        }
    });
    black_box(served.to)
}

library_benchmark_group!(
    name = udp_path;
    benchmarks = known_client, new_client
);

main!(library_benchmark_groups = udp_path);
