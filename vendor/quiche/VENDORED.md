# quiche 0.30.0, patched

This is [quiche](https://github.com/cloudflare/quiche) 0.30.0 as published on crates.io
(`quiche/` at `be47c5011215b9f13bad06bd7627d3ae49888a19`), copyright Cloudflare and its
contributors, under the BSD 2-Clause licence in `COPYING`. The workspace builds it through
`[patch.crates-io]`, in place of the published crate. Only what the package needs is kept:
the manifest, the lock file, the licence, the README, `src/` and `examples/` (quiche's own
tests read certificates from there).

## What is changed

Every change is marked `EdgeRush:` in the source.

- **STREAM frames share a packet.** When quiche builds a short-header packet, it writes a
  STREAM frame for the first flushable stream and stops there. Only its `fuzzing` feature
  goes on to the next streams while the packet has room. The change drops that
  `#[cfg(feature = "fuzzing")]`, so every build coalesces (`src/lib.rs`, in `send_single`).
- **Two of quiche's tests follow from it.** `stream_round_robin` and `stream_reprioritize`
  expect one stream per packet. They now check the same order, frame by frame, across the
  one packet that carries them all (`src/tests.rs`).
- **An ACK alone may wait, when asked to.** `Config::enable_delayed_ack(true)` (off by
  default) makes an ACK in the application space wait, once the handshake is confirmed.
  It goes out at once after a second ack-eliciting packet, after one that came out of order
  or past a gap, or after an ACK was lost; otherwise it goes with the next frames sent, or
  at the latest 5 ms short of the advertised `max_ack_delay` after the first packet it
  covers. `timeout_instant()` includes that time. This is RFC 9000 §13.2.1 and §13.2.2, as
  HAProxy and NGINX do it. The state is three fields of `PktNumSpace` (`src/packet.rs`),
  counted in `recv_single`, cleared where the ACK frame is written in `send_single`. The
  decision is `ack_due`, which `write_pkt_type` asks before it starts a packet for an ACK
  alone (`src/lib.rs`). Five tests at the end of `src/tests.rs` cover it.

## Why

**Coalescing.** A gateway answering many small requests on one connection otherwise sends
a packet for every answer. That is a UDP send, an encryption and a header protection for
each, and about half an ACK back from the client for each (16 §2 in the docs). On the benchmark
laptop, with one client connection and 256 requests in flight, it was 1.06 datagrams a
request both ways; coalescing brought it to 0.16, against NGINX's 0.06. NGINX, HAProxy,
quinn and ngtcp2 all fill a packet with as many streams' frames as fit, as RFC 9000 §12.4
allows. Priority order is kept: the frames go in the order quiche would have sent them one
packet at a time.

**Delayed ACKs.** quiche sends an ACK whenever the application asks it for a packet and
one is owed. A driver asks after every datagram it hands over, so a request answered from
upstream costs two packets back: the ACK at once, the answer after. With 256 connections
of one request each, that was 3.0 datagrams a request both ways for EdgeRush and for NGINX,
and 2.1 for HAProxy, which lets the ACK wait for the answer (16 §2 in the docs).

With both changes, quiche's own library tests pass (1,145 of 1,145, on Windows and Linux,
with `cargo test --no-default-features --features boringssl-boring-crate --lib`). The
proxy's probe `several_streams_share_a_packet` in `crates/proxy/tests/h3_library.rs` fails
against the published crate, and its test `an_answer_carries_the_ack_of_its_request` in
`crates/proxy/src/downstream/h3/tests.rs` fails without delayed ACKs.

## Moving to another version

Take the new version's published package (`cargo download`, or the registry's source
under `~/.cargo/registry/src/`) and copy the same files. Then re-apply every change marked
`EdgeRush:`, run quiche's tests as above, and run the proxy's probe and tests. If upstream
has both by then, delete this directory and the `[patch.crates-io]` entries in
`Cargo.toml` and `fuzz/Cargo.toml`.
