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
- **A stream whose frame header does not fit waits for the next packet.** When what was
  left of a packet could not take the next stream's STREAM frame header, quiche took the
  stream off its send queue with its data unsent, and nothing put it back: a stream is
  queued again only if it was not flushable before, and it still was. Now the packet ends
  there and the stream stays queued, as quinn does (`src/lib.rs`, in `send_single`). With
  coalescing this is any stream after the first whose header needs 15 bytes or more (an
  8-byte varint for its ID or offset) where 13 or 14 are left. `a_stream_left_out_of_a_full_packet_is_sent_in_a_later_one`
  in `crates/proxy/tests/h3_library.rs` covers it.
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
  `recovery/congestion` and `recovery/gcongestion`, accounted as their loss detection
  accounts), and its frames go on the new path.
- Three tests (five cases) after `connection_migration_zero_length_cid` in
  `src/tests.rs` cover both, beside quiche's own `path_probing_dos` and
  `path_event_queue_bounded_on_port_rotation`, which still pass.
- **A RETIRE_CONNECTION_ID repeated for an ID already retired is ignored.** Removing a
  connection ID refused when one ID was left before it looked for the ID, so the same
  frame arriving again, once the ID was retired and before the application issued
  another, closed the connection with PROTOCOL_VIOLATION (`src/cid.rs`, `remove`). It now
  refuses only to remove the last ID, as it meant to. `a_repeated_retire_connection_id_is_ignored`
  in `src/tests.rs` covers it.
- **A server's answer goes again at once when the ClientHello comes again.** A client's
  Initial CRYPTO data that the server has already read means the client did not have the
  answer. The first time, and only then, a server marks its unacknowledged Initial and
  Handshake CRYPTO data to be sent again at once, rather than at its PTO (RFC 9002 §6.2.3,
  as HAProxy does it; `Connection::handshake_sped_up`, in the CRYPTO frame's handling in
  `src/lib.rs`). `a_repeated_client_hello_brings_the_server_hello_again` and
  `the_server_hello_goes_again_early_once_only` in `src/tests.rs` cover it. What sets it
  off, a repeat and nothing else, is not reached by quiche's client, whose ClientHello
  fits one packet.
- **A stream the peer stops is always reported.** On a STOP_SENDING, quiche resets the
  stream's sending side and marks the stream writable, so that `stream_writable_next()`
  hands it to the application, which learns of the stop by writing (`StreamStopped`); the
  stream is not collected while so marked. It marked the stream only if it had not been
  writable before, but a stream `stream_writable_next()` has already returned is off that
  set however writable it is. So the stop was never reported, the stream was collected
  once its RESET_STREAM was acknowledged, and its credit went back to the peer. Now it is
  marked either way (`src/lib.rs`, the STOP_SENDING frame's handling);
  `a_stream_stopped_after_it_was_taken_as_writable_is_reported` in `src/tests.rs` covers
  it.
- **A stream holds at most 1,024 runs of data.** A stream's receive buffer keeps what came
  out of order as runs with gaps between them, each piece in a buffer and a tree node of
  its own, and flow control bounds only their bytes. A `RangeSet` of what is held now
  counts the runs (`RecvBuf::runs` in `src/stream/recv_buf.rs`, kept as data is stored,
  read and cleared); past 1,024 a write fails with the new `Error::TooManyGaps`, which
  closes the connection with PROTOCOL_VIOLATION. Data in order is one run however many
  pieces it came in. Three tests at the end of `src/stream/recv_buf.rs` and
  `a_stream_sent_in_gapped_pieces_closes_the_connection` in `src/tests.rs` cover it.
- **A stream completed by the peer while no longer read is collected.** quiche collects a
  stream once both its sides are done, but looked for one only as an ACK came or the
  application read. When a stream's sending side was done and acknowledged before the peer
  ended its side, by a RESET_STREAM or a FIN, on a stream the application had stopped
  reading, nothing looked again: the stream stayed, and its credit with it, until the
  connection closed. Both frames' handling now collects a stream they complete with nothing
  left to read (`completed_unread` in `src/lib.rs`), keeping one the peer stopped until the
  application has heard of it, as the ACK's handling does.
  `a_stream_answered_before_its_request_ended_is_collected` and
  `a_stopped_stream_ended_unread_is_kept_until_the_stop_is_heard` in `src/tests.rs` cover
  it.
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

## Why

**Coalescing.** A gateway answering many small requests on one connection otherwise sends
a packet for every answer. That is a UDP send, an encryption and a header protection for
each, and about half an ACK back from the client for each (16 §2 in the docs). On the benchmark
laptop, with one client connection and 256 requests in flight, it was 1.06 datagrams a
request both ways; coalescing brought it to 0.16, against NGINX's 0.06. NGINX, HAProxy,
quinn and ngtcp2 all fill a packet with as many streams' frames as fit, as RFC 9000 §12.4
allows. Priority order is kept: the frames go in the order quiche would have sent them one
packet at a time.

**The stream left out.** Published quiche reaches that arm only for the first stream of a
packet other frames have nearly filled; coalescing reaches it for every stream after the
first. The rest of such an answer was never sent: a large answer on a long connection
stalled for good, holding one of the client's stream credits until the client gave up on
it. Stopping is one arm of a loop that only ever looks at the head of the queue; going on to
a later stream that fits, as HAProxy does, would need the loop to walk the queue.

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

**The repeated RETIRE_CONNECTION_ID.** Frames are sent again after a loss, and a peer may
send one more than once anyway: quic-go's client, in quic-interop-runner's case of a lossy
handshake, sent its first RETIRE_CONNECTION_ID in two packets that arrived together. The
first retired the ID and the second closed the connection, on the second of fifty
connections, before EdgeRush's driver had issued the replacement it issues once a turn.
RFC 9000 §19.16 makes an error of retiring an ID never issued, or the one a packet was
sent to, neither of which this was.

**The ServerHello sent again early.** quiche answered a client's repeated ClientHello with
an ACK alone and sent its own ServerHello again only at its PTO, which doubles each time. In
quic-interop-runner's cases of 30% loss and corruption, a ServerHello spoiled three times
over took a handshake past ten seconds. RFC 9002 §6.2.3 lets an endpoint send unacknowledged
CRYPTO data early "for a limited number of times per connection"; HAProxy does it once, on
a duplicate CRYPTO frame, which is what this does. What goes stays within the
anti-amplification limit: a client's Initial is 1,200 bytes at least.

**The stopped stream.** A client that stops reading an answer no longer wants it, and the
gateway gives up the exchange behind it (RFC 9114 §4.1.1). EdgeRush's driver takes every
stream quiche reports writable, so a stream the client stopped while its answer waited on
an upstream was never reported: the exchange went on, and the stream's credit went back to
the client, which could ask and stop again without end, past the stream bound that is
meant to bound its exchanges.

**The runs.** A peer that sends a stream's data a byte at a time, a gap after each, makes
a byte cost a buffer and a tree node, 100 bytes and more: every other byte of a 16 MiB
connection window is some eight million of them, on the order of a gigabyte for one
connection. quinn closes a connection past 1,024 chunks a stream, Google's QUICHE past
10,000 ranges, with PROTOCOL_VIOLATION as here; NGINX bounds the frames a connection
holds. A gap is a packet lost and not yet sent again, and no sender's congestion control
keeps the 2,048 packets in flight that 1,024 gaps take on a path losing that much.

**The stream left uncollected.** A gateway answers before a request is whole when it need
not read the rest, a 413 or a 401 to an upload, and stops reading (RFC 9114 §4.1). Whether
the stream was then collected depended on the order the client's frames came in: its
acknowledgement of the answer's end first, and the stream stayed. A client with a hundred
streams that met a hundred such answers had none left.

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

With all fourteen changes, quiche's own library tests pass (1,176 of 1,176, on Windows and
Linux, with `cargo test --no-default-features --features boringssl-boring-crate --lib`),
and `check.sh` runs them, in a target directory of their own (`target/vendored`): the
workspace's tests never reach this crate, which is not a member of it.
The proxy's probe `several_streams_share_a_packet` in `crates/proxy/tests/h3_library.rs`
fails against the published crate, as do its tests `an_answer_carries_the_ack_of_its_request`
(without delayed ACKs), `a_client_rebound_again_and_again_is_followed`,
`a_client_rebound_during_a_large_answer_gets_it_whole`,
`an_answer_the_client_stops_reading_is_given_up` and
`a_client_that_asks_and_stops_again_and_again_holds_no_more_than_its_streams` in
`crates/proxy/src/downstream/h3/tests.rs`.

## Advisories

`cargo deny` looks advisories up only for crates with a registry source, and a path has
none. So `vendor/bases` names quiche 0.30.0 as published, which `check.sh` checks against
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
