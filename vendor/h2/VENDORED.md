# h2 0.4.19, patched

This is [h2](https://github.com/hyperium/h2) 0.4.19 as published on crates.io (at
`d57d1b852fec9dda6d42d3454502006d52104da8`), copyright the h2 authors, under the MIT
licence in `LICENSE`. The workspace builds it through `[patch.crates-io]`, in place of the
published crate, and so does `fuzz/Cargo.toml`. Only what the package needs is kept: the
manifest, the licence, the README and `src/`. The manifest still names h2's examples and
benchmarks, which are not here; nothing builds them.

## What is changed

Every change is marked `EdgeRush:` in the source. Additions for idle connections (14 §3
of the design docs), for charging what a connection holds and allocates and bounding what
it decodes a poll (15 §3), checks RFC 9113 asks for that published h2 does not make
(15 §3), the never-indexed mark RFC 7541 asks an intermediary to keep (15 §3), and one
allowance, for fuzz builds:

- **A server connection can give back its buffers.** h2 makes three buffers for every
  connection when it is handshaken, and keeps them for the connection's life: the 16 KiB
  it writes frames from (`codec/framed_write.rs`), the 8 KiB tokio-util reads frames into
  (`codec/framed_read.rs`), and the 4 KiB its HPACK decoder decodes Huffman-coded strings
  in (`hpack/decoder.rs`). `server::Connection::release_buffers` drops each of them that
  holds nothing — the read buffer and the decoder's only while no header block is half
  assembled, the write buffer only while no frame is queued or half written — and
  `server::Connection::buffer_capacity` says what they hold room for.
- **They are made again when next needed.** The write buffer, when a frame is next
  buffered (`Encoder::buffer`; `has_capacity` counts an empty one as room). The read
  buffer, before the next read, with room for a frame's header and no more
  (`FramedRead::poll_next`): a connection is polled, and reads nothing, as soon as it has
  given its buffers back, and would otherwise make the whole buffer again at once. Once a
  header is read the codec makes room for the rest of its frame, so the first frame after
  a release costs one more read; left to itself, tokio-util reserves a single byte before
  each read and would read a byte at a time. The decoder's, by `huffman::decode`, which
  reserves what each string needs.
- **A connection says what it holds of what its peer sent.**
  `server::Connection::received_unreleased` and `client::Connection::received_unreleased`
  return the connection's `in_flight_data` (`proto/streams/recv.rs`): the DATA received
  over all its streams and not yet given back as credit — what h2 holds until it is read,
  and what readers hold until they release it — through `proto::Connection` and
  `Streams`, under the streams' lock. Nothing h2 does changes.
- **A server connection says what it holds of everything else its peer sent.**
  `server::Connection::held` counts a header block still being put together (what is left
  of it undecoded and its fields so far), the HPACK decoder's buffer and dynamic table
  (`hpack/decoder.rs`), the room the stream slab, the id map, the event queue and the send
  queue hold (`proto/streams/store.rs`, `buffer.rs`; none ever shrinks), and the heads and
  trailers queued and not yet taken (`recv.rs`, counted as each is queued and taken). Sizes
  are by allocation, not by RFC 9113's measure of a header list: a map's capacity, and an
  allocation for each name and value (`proto/grant.rs`, `held`).
- **It allocates for its peer only what its owner has reserved.**
  `server::Connection::set_grant` sets an amount (`proto/grant.rs`) the codec and the
  streams share; it is unlimited until set, so a client, and a server whose owner never
  sets it, behave as published h2 does. Each allocation for what the peer sent is taken
  from it first: before a header block's frame is decoded, headroom for the most decoding
  it may allocate, given back after (`codec/framed_read.rs`); for a new stream, its decoded
  head and the room its place adds to the slabs (`streams.rs`, `Recv::open`); for a block
  left unfinished, its remainder and fields, as they grow; for trailers, theirs
  (`recv_trailers`). Without headroom a block is decoded with none of its fields kept
  (`keep_no_fields`, `frame/headers.rs`), to keep the HPACK state, and its stream refused
  with REFUSED_STREAM, as one past the stream limit is; a stream whose head and place the
  grant cannot cover is refused the same way; trailers past it reset their stream with
  ENHANCE_YOUR_CALM; an unfinished block it cannot cover closes the connection with
  ENHANCE_YOUR_CALM, as a block cannot be refused part way. `server::Connection::wanted`
  says the largest allocation refused for want of the grant since it was last asked, for
  the owner to judge whether it was short of room; a stream refused for its stream limit
  adds nothing.
- **It decodes only as much a poll as its owner allows.** Published h2 reads and decodes
  frames until its socket has nothing more, so a peer with many frames waiting keeps its
  task decoding while the worker's other tasks wait; a bound on the bytes read would not
  help, as what is already buffered is decoded all the same. `server::Connection::set_work`
  sets an allowance (`codec/framed_read.rs`), unlimited until set: each frame read spends
  one unit, whatever becomes of it, and a frame of a header block one more for each whole
  KiB it carries. Once it is spent no further frame is read: `FramedRead::poll_next` wakes
  the task and returns `Pending`, as Tokio's own budget does, and a header block left part
  way waits for its next frame as it does for a slow peer. `work_left` says what is left.
- **What an accepted head held can be asked.** `server::head_held` measures a request's
  head as `held` counted it, so that as `accept` hands it over and the count falls by that
  much, the owner passes that much of its charge on with it.
- **Nothing decoded pins a buffer.** The pseudo-field values kept as they came —
  `:authority`, `:path`, `:protocol`, and a `:scheme` other than `http` or `https`, which
  are static — are copied out of the buffer they were decoded in (`hpack/header.rs`), and
  an unfinished block's remainder out of the read buffer (`framed_read.rs`); each would
  otherwise keep the whole buffer. Other field values were copied already.
- **A field value with whitespace at either end is malformed.** RFC 9113 §8.2.1: "A field
  value MUST NOT start or end with an ASCII whitespace character". `HeaderValue` allows SP
  and HTAB anywhere, so published h2 takes such a value. `HeaderBlock::load`
  (`frame/headers.rs`) marks the block malformed, as it does a field only a connection has:
  the stream is reset with PROTOCOL_ERROR, in a head or trailers, a request's or an
  answer's, and on a server the reset counts towards `max_local_error_reset_streams` as
  every malformed request does.
- **A `:path` with a fragment is malformed.** RFC 9113 §8.3.1: `:path` is the target's path
  and query. Published h2 builds the request's URI with `http`'s `PathAndQuery`, which cuts
  a fragment off without an error, so `/a#b` was handed over as `/a`. The server's
  `convert_poll_message` (`server.rs`) now refuses a `:path` with a `#`, as it refuses an
  empty one: the stream is reset with PROTOCOL_ERROR, and counted.
- **An answer without `:status` is malformed** (RFC 9113 §8.3.2). Published h2 took it for
  a 200. The client's `convert_poll_message` (`client.rs`) resets the stream with
  PROTOCOL_ERROR: h2's own fix, `3c5f61c` (#959), not in a release yet, carried as it was
  made; drop it when a release has it.
- **An answer with a request's pseudo-field is malformed** (§8.3). Published h2 set
  `:method`, `:path` and the like aside; `convert_poll_message` resets the stream.
- **Trailers with a pseudo-field are malformed** (§8.1). Published h2 dropped them and passed
  the rest of the section on. `recv_trailers` (`proto/streams/recv.rs`) resets the stream,
  on a client and a server alike; a server's reset counts as any malformed request's.
- **A GOAWAY on a stream is a connection error** (RFC 9113 §6.8). Published h2 took one
  with a stream identifier other than 0 for a graceful GOAWAY. `GoAway::load`
  (`frame/go_away.rs`) is given the frame's head and refuses it, and the connection is
  closed with PROTOCOL_ERROR: h2's own fix, `d4a37fc` (#951), not in a release yet, carried
  as it was made; drop it when a release has it.
- **A field sent never indexed keeps the mark** (RFC 7541 §6.2.3: an intermediary
  re-encodes it the same way). Published h2 decoded it as any literal (a `TODO` in
  `hpack/decoder.rs`). The decoder now marks its value sensitive, which h2's encoder already
  never indexes, so a value passed on to the next hop as it was handed over goes as it came.
- **Its fuzzing module may go undocumented in every build.** cargo-fuzz builds with
  `--cfg fuzzing`, which brings in h2's `fuzz_bridge` module, and h2 denies `missing_docs`
  but allows it there only with its `unstable` feature. The lints of a registry dependency
  are capped at warnings; a path dependency's are not, so the vendored copy stopped every
  fuzz build of the workspace's crates. The allowance is now unconditional (`lib.rs`).

h2's own tests are not carried: they need dependencies the workspace does not have. The
buffers' release is covered by `a_server_connection_gives_back_its_buffers_and_serves_on`
and `a_server_connection_keeps_what_waits_in_its_buffers` in
`crates/proxy/tests/h2_library.rs`, which fail with the checks for an empty buffer taken
out, or with the release doing nothing; the count of what is held by
`a_server_connection_says_what_it_holds_of_what_was_sent` there, and through the proxy's
charges by `an_upload_nobody_reads_is_charged_to_the_worker` and
`an_answer_nobody_reads_from_an_http2_upstream_is_charged_to_the_worker`.
What else a server holds, and the grant, are covered by the probes after "What a server
holds of what its peer sent" there: `a_head_waiting_to_be_accepted_is_held_and_handed_over_with_its_count`,
`a_head_with_no_grant_to_decode_is_refused_and_its_table_entries_kept`,
`an_unfinished_header_block_is_held_across_polls`,
`an_unfinished_header_block_past_the_grant_closes_the_connection`,
`an_unfinished_header_blocks_first_frame_past_the_grant_closes_the_connection`,
`trailers_past_the_grant_reset_their_stream`,
`a_header_blocks_last_frame_with_no_grant_to_decode_refuses_its_stream`,
`a_finished_blocks_charge_goes_back_to_pay_for_its_stream`,
`the_room_answers_waited_in_to_be_sent_is_held`,
`decoding_needs_headroom_for_every_fields_place_not_just_the_lists_bytes`,
`a_streams_place_is_charged_by_the_room_it_adds` and
`a_stream_is_taken_up_only_if_the_grant_covers_the_room_its_place_adds`,
`what_was_refused_for_want_of_grant_is_said_once` and
`a_stream_past_its_limit_wants_nothing_of_the_grant`; through the
proxy by `a_stream_is_charged_the_head_h2_hands_over_with_it` and
`unfinished_header_blocks_from_many_connections_stay_within_the_workers_storage`. The
copies that unpin buffers change nothing the public API shows, and no test sees them.
The allowance is covered by the probes after "How much a server decodes" there:
`a_server_reads_no_frame_past_its_work_and_wakes_itself_to_go_on`,
`a_header_blocks_frame_counts_one_more_for_each_kib_it_carries`,
`streams_refused_for_their_limit_spend_the_work_as_others_do` and
`a_header_block_spent_part_way_is_finished_with_the_next_allowance`; through the proxy by
`frames_waiting_in_a_connections_socket_are_decoded_a_turn_at_a_time` and
`frames_sent_to_a_connection_told_to_go_are_decoded_a_turn_at_a_time`.
The refusal of whitespace at either end is covered by
`a_request_field_value_with_whitespace_at_either_end_is_reset`,
`resets_for_whitespace_at_either_end_are_bounded`,
`request_trailers_with_whitespace_at_either_end_are_reset` and
`an_answer_with_whitespace_at_either_end_of_a_field_value_is_reset` there, the refusal of
a fragment by `a_path_with_a_fragment_is_reset`, of an answer without `:status` by
`an_answer_without_a_status_is_reset`, of a request's pseudo-field in an answer by
`an_answer_with_a_requests_pseudo_field_is_reset`, and of pseudo-fields in trailers by
`an_answers_trailers_with_a_pseudo_field_are_refused` and
`a_requests_trailers_with_a_pseudo_field_are_reset`, and of a GOAWAY on a stream by
`a_goaway_on_a_stream_is_a_connection_error_to_a_server` and `…_to_a_client`; the
never-indexed mark by `a_requests_field_sent_never_indexed_is_handed_over_sensitive` and
`an_answers_field_sent_never_indexed_is_handed_over_sensitive`, and through the proxy by
`a_field_sent_never_indexed_stays_never_indexed_both_ways_through_an_http2_upstream`.
The allowance is covered by building any fuzz target with cargo-fuzz (the repository
README).

## Advisories

`cargo deny` looks advisories up only for crates with a registry source, and a path has
none. So `vendor/bases` names h2 0.4.19 as published, which `check.sh` checks against the
RustSec database (<https://rustsec.org/packages/h2.html>) as it checks the workspace, and
whose lock file GitHub's dependency graph reads for GitHub's own advisories. An advisory
fixed in this copy is ignored in `deny.toml`, with the reason. h2 is carried only to a
version none of them affects.

## Carrying it to another version

Read h2's advisories first (above). Copy the new version's `Cargo.toml`, `LICENSE`,
`README.md` and `src/` over these, then make the changes marked `EdgeRush:` again
(`grep -rl EdgeRush src` lists the eighteen files); a refusal the new version makes itself is
dropped, with its probe kept. The buffers' changes change nothing h2 does unless a
connection's buffers are given back, and the grant and the allowance nothing unless they are set. Update the version here, in the workspace's `Cargo.toml`, in `fuzz/Cargo.toml` and in
`vendor/bases/Cargo.toml` (then `cargo generate-lockfile` there), and run the tests above
and a fuzz target's build.
