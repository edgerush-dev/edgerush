# h2 0.4.19, patched

This is [h2](https://github.com/hyperium/h2) 0.4.19 as published on crates.io (at
`d57d1b852fec9dda6d42d3454502006d52104da8`), copyright the h2 authors, under the MIT
licence in `LICENSE`. The workspace builds it through `[patch.crates-io]`, in place of the
published crate, and so does `fuzz/Cargo.toml`. Only what the package needs is kept: the
manifest, the licence, the README and `src/`. The manifest still names h2's examples and
benchmarks, which are not here; nothing builds them.

## What is changed

Every change is marked `EdgeRush:` in the source. Two additions, for idle connections
(14 §3 of the design docs) and for charging what a connection holds (15 §3), and one
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
(`grep -rl EdgeRush src` lists the ten files).
They change nothing h2 does unless a connection's buffers are given back. Update the version here, in the workspace's `Cargo.toml`, in `fuzz/Cargo.toml` and in
`vendor/bases/Cargo.toml` (then `cargo generate-lockfile` there), and run the tests above
and a fuzz target's build.
