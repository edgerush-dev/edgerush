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

## Why

A gateway answering many small requests on one connection otherwise sends a packet for
every answer. That is a UDP send, an encryption and a header protection for each, and
about half an ACK back from the client for each (16 §2 in the docs). On the benchmark
laptop, with one client connection and 256 requests in flight, it was 1.06 datagrams a
request both ways; coalescing brought it to 0.16, against NGINX's 0.06. NGINX, HAProxy,
quinn and ngtcp2 all fill a packet with as many streams' frames as fit, as RFC 9000 §12.4
allows. Priority order is kept: the frames go in the order quiche would have sent them one
packet at a time.

With the change, quiche's own library tests pass (1,140 of 1,140, run on Linux with
`cargo test --release --no-default-features --features boringssl-boring-crate --lib`).
The proxy's probe `several_streams_share_a_packet` in
`crates/proxy/tests/h3_library.rs` fails against the published crate and passes against
this one.

## Moving to another version

Take the new version's published package (`cargo download`, or the registry's source
under `~/.cargo/registry/src/`) and copy the same files. Then re-apply the change to
`send_single` and the two tests, run quiche's tests as above, and run the probe. If
upstream coalesces by then, delete this directory and the `[patch.crates-io]` entries in
`Cargo.toml` and `fuzz/Cargo.toml`.
