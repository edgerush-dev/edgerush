//! Instruction counts for what an HTTP/2 connection's driver does after every turn to charge
//! what h2 holds of what its peer sent ([15 §3](../../../docs/15-http2-and-grpc.md)): ask
//! h2, under its streams' lock, and settle the connection's account — which, where nothing
//! has changed since the last turn, is all.
//!
//! Linux only (valgrind):
//! `cargo bench -p edgerush-proxy --features fuzzing --bench received`, see the repository
//! README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use bytes::Bytes;
use edgerush_proxy::received::{Account, Received};
use edgerush_proxy::storage::Storage;
use iai_callgrind::{library_benchmark, library_benchmark_group, main};
use std::hint::black_box;
use std::rc::Rc;
use tokio::io::DuplexStream;

/// Turns of a driver, one after another.
const TURNS: usize = 100;

/// A server connection, handshaken over a pipe, with its client and its runtime kept so
/// that nothing of them is dropped while it is measured; and its account in a worker's
/// table.
struct Driven {
    _runtime: tokio::runtime::Runtime,
    connection: h2::server::Connection<DuplexStream, Bytes>,
    _client: (
        h2::client::SendRequest<Bytes>,
        h2::client::Connection<DuplexStream, Bytes>,
    ),
    _table: Rc<Received>,
    account: Account,
}

fn driven(charged: usize) -> Driven {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .expect("a runtime");
    let (near, far) = tokio::io::duplex(1 << 16);
    let (client, server) = runtime
        .block_on(async { tokio::join!(h2::client::handshake(far), h2::server::handshake(near)) });
    let table = Received::new(Storage::new(1 << 30));
    let account = table.open();
    account.settle(charged);
    Driven {
        _runtime: runtime,
        connection: server.expect("the server's handshake"),
        _client: client.expect("the client's handshake"),
        _table: table,
        account,
    }
}

// Where nothing has changed since the last turn: the cost every turn pays.
#[library_benchmark]
#[bench::nothing_held(driven(0))]
fn ask_and_settle(driven: Driven) -> (Driven, usize) {
    let mut held = 0;
    for _ in 0..TURNS {
        held = black_box(&driven.connection).received_unreleased();
        driven.account.settle(black_box(held));
    }
    (driven, held)
}

// Where the charge moves every turn, between two amounts.
#[library_benchmark]
#[bench::moving(driven(0))]
fn settle_moving(driven: Driven) -> Driven {
    for turn in 0..TURNS {
        driven.account.settle(black_box(16_384 * (turn % 2)));
    }
    driven
}

library_benchmark_group!(
    name = received;
    benchmarks = ask_and_settle, settle_moving
);
main!(library_benchmark_groups = received);
