# quiche 0.30.1, patched

This is [quiche](https://github.com/cloudflare/quiche) 0.30.1 as published on crates.io
(`quiche/` at `b364fdfee3799d8c469d5cd8f5693fafb420a466`), copyright Cloudflare and its
contributors, under the BSD 2-Clause licence in `COPYING`. The workspace builds it through
`[patch.crates-io]`, in place of the published crate. Only what the package needs is kept:
the manifest, the lock file, the licence, the README, `src/` and `examples/` (quiche's own
tests read certificates from there).

## What is changed

Every change is marked `EdgeRush:` in the source.

- **STREAM frames share a packet.** When quiche builds a short-header packet, it writes a
  STREAM frame for the first flushable stream and stops there. Only its `fuzzing` feature
  goes on to the next streams while the packet has room. The change drops that
  `#[cfg(feature = "fuzzing")]`, so every build coalesces (`src/lib.rs`, in `send_single`),
  and starts no further stream's frame once 12 bytes or fewer are left
  (`frame::MAX_STREAM_OVERHEAD`, kept here), so that no stream is sent a sliver.
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
- **A server follows every NAT rebinding.** A server's paths are capped at its
  `active_connection_id_limit`, two by default, and a new path found room only by dropping
  one that held no Destination Connection ID. A path the peer has left keeps its ID, and
  with a peer's zero-length IDs every path keeps sequence 0, so a client's second
  rebinding found no room and everything from its new address was dropped. Now, when
  there is no such path, a server drops one the peer migrated away from (`Path::left`,
  set in `on_peer_migrated`), once the path it migrated to is validated
  (`PathMap::remove_for_new_path`, called from `get_or_create_recv_path_id`). The dropped
  path's own ID is retired, so the peer issues a fresh one; a zero-length ID, or one the
  active path shares after a migration without a spare, stays.
- **What was in flight to the address a peer left is sent again at once.** Loss recovery
  is kept per path, and an ACK on the new path does not count towards loss on the old one,
  so packets sent to the old address just before the move were found lost only by the old
  path's PTO, a probe or two at each doubling: the gap in a download never closed. Now
  `on_peer_migrated` also has the old path's recovery declare everything in flight on it
  lost (`RecoveryOps::on_peer_left`, with a `lose_all` for each of the two recoveries,
  `recovery/congestion` and `recovery/gcongestion`), and its frames go on the new path.
  The losses are not taken for congestion: the old path keeps its window, as the
  gcongestion recovery already left it and as Google's QUIC keeps the old path's, so a
  packet copied from another address by an attacker (RFC 9000 §9.3.2), which moves the
  server until the client's next packet moves it back, costs the client a resend, not its
  window. `a_spurious_migration_leaves_the_clients_own_path_its_window` in
  `crates/proxy/tests/h3_library.rs` covers it.
- Three tests (five cases) after `connection_migration_zero_length_cid` in
  `src/tests.rs` cover both, beside quiche's own `path_probing_dos` and
  `path_event_queue_bounded_on_port_rotation`, which still pass. quiche's
  `max_outstanding_sent_packets_multiple_paths` checks that neither of two paths holds the
  outstanding packets alone; under gcongestion, whose recovery drops a lost packet's record
  at once, the path the client left holds none, so that check is made for cubic only.
- **A server's answer goes again at once when the ClientHello comes again.** A client's
  Initial CRYPTO data that the server has already read means the client did not have the
  answer. The first time, and only then, a server marks its unacknowledged Initial and
  Handshake CRYPTO data to be sent again at once, rather than at its PTO (RFC 9002 §6.2.3,
  as HAProxy does it; `Connection::handshake_sped_up`, in the CRYPTO frame's handling in
  `src/lib.rs`). `a_repeated_client_hello_brings_the_server_hello_again` and
  `the_server_hello_goes_again_early_once_only` in `src/tests.rs` cover it. What sets it
  off, a repeat and nothing else, is not reached by quiche's client, whose ClientHello
  fits one packet.
- **A stream holds at most 1,024 runs of data.** A stream's receive buffer keeps what came
  out of order as runs with gaps between them, each piece in a buffer and a tree node of
  its own, and flow control bounds only their bytes. A `RangeSet` of what is held now
  counts the runs (`RecvBuf::runs` in `src/stream/recv_buf.rs`, kept as data is stored,
  read and cleared); past 1,024 a write fails with the new `Error::TooManyGaps`, which
  closes the connection with PROTOCOL_VIOLATION. Data in order is one run however many
  pieces it came in. Three tests at the end of `src/stream/recv_buf.rs` and
  `a_stream_sent_in_gapped_pieces_closes_the_connection` in `src/tests.rs` cover it.
- **The HTTP/3 layer lets go of a stream the transport has let go of.** Its state for a
  request stream went only when both sides finished through its own calls; for one reset
  either way, stopped, or answered before the request was whole, it stayed until the
  connection ended, with the capacity of its header buffer, up to the largest HEADERS
  frame the stream had. The transport now keeps the streams it collects, once something
  has asked it to (`StreamMap::tell_collected` and `collected_next`, `src/stream/mod.rs`),
  and `h3::Connection::poll` asks, and forgets its state for each first thing
  (`src/h3/mod.rs`). A connection with no HTTP/3 keeps no such list.
  `a_request_stream_the_transport_lets_go_is_let_go_of` in `src/h3/mod.rs` and
  `streams_let_go_of_are_told_only_once_asked` in `src/tests.rs` cover it.
- **What a connection holds for its peer is told.** `Connection::received_held()` says
  what the streams hold of what they received and the application has not read: the bytes,
  and the pieces they are held in (`Held`; a byte count kept by each stream's receive
  buffer as data is stored, read and cleared, `src/stream/recv_buf.rs`, and the pieces
  its tree's length). `h3::Connection::frame_buffers()` says the capacity of the buffers
  its streams read frames into, a HEADERS frame held whole until it has all come. Both are
  for the application's account of the memory a peer makes it hold; quiche keeps no
  account of its own. `what_is_held_is_counted` in `src/stream/recv_buf.rs`,
  `what_a_connection_holds_unread_is_counted` in `src/tests.rs` and
  `a_head_that_has_come_in_part_is_counted` in `src/h3/mod.rs` cover it.
- **A field from QPACK's static table is not copied.** `h3::Header` held its name and
  value as two `Vec`s, and the decoder copied into them even what it took from the static
  table: a request's `:method GET` and `:scheme https`, and the names of the `:authority`,
  `:path` and other fields it sends as references to the table, each an allocation of a
  few bytes. `Header` now holds each as a `Cow<'static, [u8]>`, borrowed where the decoder
  takes it from the table and owned otherwise (`src/h3/mod.rs`, `Decoder::decode` in
  `src/h3/qpack/decoder.rs`). Its API is as it was: `Header::new` still copies, and
  `name()` and `value()` still give slices. `static_fields_are_borrowed_from_the_table`
  at the end of `src/h3/qpack/decoder.rs` covers it.
- **An invalid Retry token can be told without a connection.** `invalid_token_close()`
  writes a CONNECTION_CLOSE with INVALID_TOKEN in an Initial packet under the keys of the
  client's destination ID, as `retry()` writes a Retry (`packet::invalid_token_close` in
  `src/packet.rs`). `an_invalid_retry_token_is_told_with_a_close_the_client_reads` in
  `crates/proxy/tests/h3_library.rs` covers it.
- **A field sent never indexed keeps the mark.** QPACK's N bit says a field is never to be
  indexed, and an intermediary keeps it when it sends the field on (RFC 9204 §7.1.3);
  quiche's decoder dropped it. `Header` now carries it, read by `NameValue::is_never_indexed`
  (a provided method, false for anything else) and made by `Header::never_indexed`; the
  decoder sets it from either literal representation, and the encoder sends such a field
  as a literal with the bit set, even one the static table holds whole
  (`src/h3/qpack/`). `never_indexed_fields_keep_the_mark` in `src/h3/qpack/mod.rs` covers
  it.
- **Whether the peer's certificate verified can be asked.** A server that lets a client
  through whatever its certificate's verification says (Gateway API's
  `AllowInsecureFallback`) needs the verdict to forward only a certificate that verified
  (03 §3 in the docs), and quiche gave the certificate alone.
  `Connection::peer_cert_verified()` reads BoringSSL's `SSL_get_verify_result` for a peer
  that showed one (`src/tls/boringssl.rs`, `src/lib.rs`).
  `a_listener_validating_clients_in_the_insecure_fallback_lets_them_all_through` in
  `crates/proxy/src/tls.rs` covers it.
- **Tests of faults quiche has fixed itself are kept.** A stream whose STREAM frame header
  does not fit what is left of a packet stays queued for the next one; a
  RETIRE_CONNECTION_ID repeated for an ID already retired is ignored; a stream the peer
  stops is reported even after `stream_writable_next()` has returned it; and a stream the
  peer completes while the application no longer reads it is collected, one the peer
  stopped only once the stop is heard. Each was a change of ours until quiche made the
  same fix, and its test stays: `a_stream_left_out_of_a_full_packet_is_sent_in_a_later_one`
  in `crates/proxy/tests/h3_library.rs`, and `a_repeated_retire_connection_id_is_ignored`,
  `a_stream_stopped_after_it_was_taken_as_writable_is_reported`,
  `a_stream_answered_before_its_request_ended_is_collected` and
  `a_stopped_stream_ended_unread_is_kept_until_the_stop_is_heard` in `src/tests.rs`.

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

**Rebinding.** A client's NAT may give it a new port at any time, and does again and
again on a long connection. quic-interop-runner's rebinding cases move the client every
five seconds, and every client's download stalled: for good at the first move, the data in
flight to the old address never sent again, and past the second, no room for its path.
quinn and NGINX keep the current path and the one before it, and drop the older; HAProxy
moves its one path. Only paths the peer left are dropped, and only once the path it moved
to is validated, so what quiche's DoS tests guard still holds: a path the peer is probing
is kept, and an attacker rotating source addresses cannot validate the paths it makes.
quinn keeps one loss recovery for the connection, so an ACK on the new path finds the old
path's packets lost within a round trip; here they are declared lost when the peer moves,
which RFC 9000 §9.4 leaves to the endpoint. A peer that moved on purpose, its old address
still working, may have a few packets sent twice.

**The ServerHello sent again early.** quiche answered a client's repeated ClientHello with
an ACK alone and sent its own ServerHello again only at its PTO, which doubles each time. In
quic-interop-runner's cases of 30% loss and corruption, a ServerHello spoiled three times
over took a handshake past ten seconds. RFC 9002 §6.2.3 lets an endpoint send unacknowledged
CRYPTO data early "for a limited number of times per connection"; HAProxy does it once, on
a duplicate CRYPTO frame, which is what this does. What goes stays within the
anti-amplification limit: a client's Initial is 1,200 bytes at least.

**The runs.** A peer that sends a stream's data a byte at a time, a gap after each, makes
a byte cost a buffer and a tree node, 100 bytes and more: every other byte of a 16 MiB
connection window is some eight million of them, on the order of a gigabyte for one
connection. quinn closes a connection past 1,024 chunks a stream, Google's QUICHE past
10,000 ranges, with PROTOCOL_VIOLATION as here; NGINX bounds the frames a connection
holds. A gap is a packet lost and not yet sent again, and no sender's congestion control
keeps the 2,048 packets in flight that 1,024 gaps take on a path losing that much.

**The HTTP/3 state kept.** Upstream's own source says so where it would be freed ("TODO:
check if stream is completed so it can be freed"). A client that asks and cancels, again
and again on one connection, grew the state without end: fifty such streams left fifty
entries, each with the header buffer its HEADERS frame had grown.

**What is held, told.** A worker's connections together can hold far more than it has:
16 MiB of window and 19 MiB of staged heads each, and 32,768 of them. EdgeRush charges
what quiche holds for them to the worker's storage ledger, as it charges what HTTP/1 and
HTTP/2 stage, and needs to know it. The pieces are told apart from the bytes because each
costs a buffer and a node past its bytes: a peer that sends its data a byte a frame, in
order, costs a hundred times what flow control counts.

**The invalid token.** A client whose Retry token fails will take no other Retry, and RFC
9000 §8.1.3 has the server close at once with INVALID_TOKEN, as NGINX and quinn do, so the
client can start again rather than wait out its handshake timer. Published quiche can
write that close only from a connection, and a connection writes nothing before it has
processed the client's Initial, which runs the TLS handshake a Retry is there to spare.

With all twelve changes, quiche's own library tests pass (1,309 of 1,309, on Windows and
Linux, with `cargo test --no-default-features --features boringssl-boring-crate --lib`),
and `check.sh` runs them, in a target directory of their own (`target/vendored`): the
workspace's tests never reach this crate, which is not a member of it.
The proxy's probe `several_streams_share_a_packet` in `crates/proxy/tests/h3_library.rs`
fails against the published crate, as do its tests `an_answer_carries_the_ack_of_its_request`
(without delayed ACKs), `a_client_rebound_again_and_again_is_followed`,
`a_client_rebound_during_a_large_answer_gets_it_whole` and
`a_client_that_asks_and_stops_again_and_again_holds_no_more_than_its_streams` in
`crates/proxy/src/downstream/h3/tests.rs`.

## Advisories

`cargo deny` looks advisories up only for crates with a registry source, and a path has
none. So `vendor/bases` names quiche 0.30.1 as published, which `check.sh` checks against
the RustSec database as it checks the workspace, and whose lock file GitHub's dependency
graph reads for GitHub's own advisories. An advisory fixed in this copy is ignored in
`deny.toml`, with the reason. quiche marks its security fixes in its release notes
(<https://github.com/cloudflare/quiche/releases>), and not every one reaches either
database: the notes are read by hand when quiche moves to another version, which is to be
one with no security fix it lacks.

## Moving to another version

Read quiche's advisories first (above). Take the new version's published package
(`cargo download`, or the registry's source under `~/.cargo/registry/src/`) and copy the
same files. Then re-apply every change marked `EdgeRush:`, move the version in
`vendor/bases/Cargo.toml` and run `cargo generate-lockfile` there, run quiche's tests as
above, and run the proxy's probe and tests. If upstream has them all by then, delete this directory and
the `[patch.crates-io]` entries in `Cargo.toml` and `fuzz/Cargo.toml`.
