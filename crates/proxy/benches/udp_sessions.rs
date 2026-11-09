//! Instruction counts for a `udp` listener's session table on a datagram's path
//! ([24 §3](../../../docs/24-udp-proxying.md)): a datagram from a client that has a session,
//! which looks it up and moves its idle bound later, and one from a new client, which picks
//! its backend, takes room for it and makes it, each among ten thousand sessions.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench udp_sessions`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_config::{Destination, UpstreamId, WeightedBackends};
use edgerush_proxy::connections::{Loads, QUIC_MOST};
use edgerush_proxy::udp::sessions::{Client, Sessions};
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

/// Sessions on one worker's listener already.
const SESSIONS: u16 = 10_000;

/// The client on port `port` of one address, to the listener's one local address.
fn client(port: u16) -> Client {
    Client {
        peer: SocketAddr::new(
            IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1)),
            port,
        ),
        local: SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 53),
    }
}

/// What the benchmarks start from: a table of [`SESSIONS`] sessions over two backends,
/// the route's backends, and the time they were made.
type Table = (Sessions<u64>, WeightedBackends, Instant);

fn full() -> Table {
    let loads = Loads::new(1, usize::MAX, QUIC_MOST, 1);
    let mut sessions = Sessions::new(Duration::from_secs(60), loads, 0, 0);
    let backends = WeightedBackends::new([(UpstreamId(0), 1), (UpstreamId(1), 1)]);
    let start = Instant::now();
    for port in 0..SESSIONS {
        let _made = sessions.arrived(client(port), start, &backends, drawn(port), opened);
    }
    (sessions, backends, start)
}

/// The random number a new session is picked by, as the table asks for it. Made here, as
/// [`opened`] is, not in the benchmarks: a closure written there would put the benchmark's
/// path into the name of the generic it is handed to, which iai-callgrind stops counting in
/// (10 §3).
fn drawn(random: u16) -> impl FnOnce() -> u64 {
    move || u64::from(random)
}

/// What a session holds of its own: here a number, where the data plane opens a socket.
fn opened(backend: Destination) -> Result<u64, ()> {
    Ok(match backend {
        Destination::Upstream(UpstreamId(at)) | Destination::Invalid(at) => at as u64,
    })
}

// A datagram from a client in the middle of the table, a second after the last.
#[library_benchmark]
#[bench::known(full())]
fn known_client((mut sessions, backends, start): Table) -> Sessions<u64> {
    let now = start + Duration::from_secs(1);
    let found = sessions
        .arrived(
            black_box(client(SESSIONS / 2)),
            now,
            &backends,
            drawn(0),
            opened,
        )
        .is_ok();
    black_box(found);
    sessions
}

// A datagram from a client the table has not seen.
#[library_benchmark]
#[bench::new(full())]
fn new_client((mut sessions, backends, start): Table) -> Sessions<u64> {
    let now = start + Duration::from_secs(1);
    let made = sessions
        .arrived(
            black_box(client(SESSIONS)),
            now,
            &backends,
            drawn(SESSIONS),
            opened,
        )
        .is_ok();
    black_box(made);
    sessions
}

library_benchmark_group!(
    name = udp_sessions;
    benchmarks = known_client, new_client
);
main!(library_benchmark_groups = udp_sessions);
