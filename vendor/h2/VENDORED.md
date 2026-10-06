# h2 0.4.19, patched

This is [h2](https://github.com/hyperium/h2) 0.4.19 as published on crates.io (at
`d57d1b852fec9dda6d42d3454502006d52104da8`), copyright the h2 authors, under the MIT
licence in `LICENSE`. The workspace builds it through `[patch.crates-io]`, in place of the
published crate, and so does `fuzz/Cargo.toml`. Only what the package needs is kept: the
manifest, the licence, the README and `src/`. The manifest still names h2's examples and
benchmarks, which are not here; nothing builds them.

## What is changed

Every change is marked `EdgeRush:` in the source. Two additions, for idle connections
(14 §3 of the design docs) and for charging what a connection holds (15 §3), checks RFC
9113 asks for that published h2 does not make (15 §3), and one allowance, for fuzz builds:

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
The refusal of whitespace at either end is covered by
`a_request_field_value_with_whitespace_at_either_end_is_reset`,
`resets_for_whitespace_at_either_end_are_bounded`,
`request_trailers_with_whitespace_at_either_end_are_reset` and
`an_answer_with_whitespace_at_either_end_of_a_field_value_is_reset` there, the refusal of
a fragment by `a_path_with_a_fragment_is_reset`, of an answer without `:status` by
`an_answer_without_a_status_is_reset`, of a request's pseudo-field in an answer by
`an_answer_with_a_requests_pseudo_field_is_reset`, and of pseudo-fields in trailers by
`an_answers_trailers_with_a_pseudo_field_are_refused` and
`a_requests_trailers_with_a_pseudo_field_are_reset`.
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
(`grep -rl EdgeRush src` lists the eleven files); a refusal the new version makes itself is
dropped, with its probe kept. The buffers' changes change nothing h2 does unless a
connection's buffers are given back. Update the version here, in the workspace's `Cargo.toml`, in `fuzz/Cargo.toml` and in
`vendor/bases/Cargo.toml` (then `cargo generate-lockfile` there), and run the tests above
and a fuzz target's build.
