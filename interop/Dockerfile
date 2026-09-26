# EdgeRush as a server of quic-interop-runner (docs/16 §8): the gateway built with its
# `interop` feature, which speaks HTTP/0.9 over QUIC beside HTTP/3, and NGINX behind it
# serving the files the runner puts in /www. Built from the repository's root:
#
#     docker build -f interop/Dockerfile -t edgerush-qns:latest .
FROM rust:1-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang libclang-dev \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
RUN cargo build --release -p edgerush --features interop \
    && cp target/release/edgerush /edgerush

FROM martenseemann/quic-network-simulator-endpoint:latest
RUN apt-get update && apt-get install -y --no-install-recommends nginx-light \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /edgerush /usr/local/bin/edgerush
COPY interop/run_endpoint.sh /run_endpoint.sh
RUN chmod +x /run_endpoint.sh
ENTRYPOINT ["/run_endpoint.sh"]
