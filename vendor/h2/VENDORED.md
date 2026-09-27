# h2 0.4.19, patched

This is [h2](https://github.com/hyperium/h2) 0.4.19 as published on crates.io (at
`d57d1b852fec9dda6d42d3454502006d52104da8`), copyright the h2 authors, under the MIT
licence in `LICENSE`. The workspace builds it through `[patch.crates-io]`, in place of the
published crate, and so does `fuzz/Cargo.toml`. Only what the package needs is kept: the
manifest, the licence, the README and `src/`. The manifest still names h2's examples and
benchmarks, which are not here; nothing builds them.

## What is changed

Every change is marked `EdgeRush:` in the source. One addition, for idle connections
(14 §3 of the design docs):

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

h2's own tests are not carried: they need dependencies the workspace does not have. The
change is covered by `a_server_connection_gives_back_its_buffers_and_serves_on` and
`a_server_connection_keeps_what_waits_in_its_buffers` in `crates/proxy/tests/h2_library.rs`,
which fail with the checks for an empty buffer taken out, or with the release doing nothing.

## Carrying it to another version

Copy the new version's `Cargo.toml`, `LICENSE`, `README.md` and `src/` over these, then
make the changes marked `EdgeRush:` again (`grep -rl EdgeRush src` lists the six files).
They are additions, and change nothing h2 does unless a connection's buffers are given
back. Update the version here, in the workspace's `Cargo.toml` and in `fuzz/Cargo.toml`,
and run the two tests above.
