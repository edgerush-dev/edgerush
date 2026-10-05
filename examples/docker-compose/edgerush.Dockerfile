# EdgeRush for the compose demo: the release binary built from the repository as it is
# checked out (uncommitted changes included), on Debian. The build context is the
# repository; compose.yaml says so.
FROM rust:1-bookworm AS build
# BoringSSL is built from source by boring-sys: cmake for it, libclang for its bindings.
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
# The pinned toolchain in a layer of its own, so a change to the code does not fetch it again.
COPY rust-toolchain.toml .
RUN rustup toolchain install
COPY . .
# Few jobs: the machine this is built on is shared, and fat LTO needs its memory at the end.
ARG JOBS=2
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    CARGO_BUILD_JOBS=${JOBS} cargo build --release --locked -p edgerush \
    && cp target/release/edgerush /usr/local/bin/edgerush

FROM debian:bookworm-slim
COPY --from=build /usr/local/bin/edgerush /usr/local/bin/edgerush
USER nobody
ENTRYPOINT ["edgerush"]
CMD ["--help"]
