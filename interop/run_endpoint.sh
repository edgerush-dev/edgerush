#!/bin/bash
# quic-interop-runner's entry point (docs/16 §8): the network simulator's routes, then
# EdgeRush on port 443 over TCP and QUIC, serving /www through a local NGINX.
#
# A case EdgeRush does not take is answered with 127, which the runner reads as
# unsupported: zero-RTT is off, quiche has neither QUIC v2, nor migration to a preferred
# address, nor ECN, and a case the runner adds later is not known here.
set -e
/setup.sh
echo "role $ROLE, test case $TESTCASE"
if [ "$ROLE" != server ]; then
    exit 127
fi
case "$TESTCASE" in
    # HTTP/3, and HTTP/0.9 over QUIC for the transport's cases, several of which the
    # runner sends under one of these names (loss and corruption as `transfer` and
    # `multiconnect`, for one).
    http3 | handshake | transfer | multiconnect | chacha20 | keyupdate | resumption | versionnegotiation) ;;
    *) exit 127 ;;
esac

cat >/tmp/nginx.conf <<'NGINX'
# The runner's files in /www are readable by root only.
user root;
worker_processes 1;
pid /tmp/nginx.pid;
error_log /logs/nginx.log warn;
events {}
http {
    access_log off;
    keepalive_requests 1000000;
    server {
        listen 127.0.0.1:9000;
        root /www;
    }
}
NGINX
mkdir -p /tmp/nginx-temp
nginx -c /tmp/nginx.conf -e /logs/nginx.log -p /tmp/nginx-temp/

{
    echo 'listeners:'
    echo '  web:'
    echo '    address: "[::]:443"'
    echo '    protocol: https'
    echo '    http3: {}'
    echo '    tls:'
    echo '      certificates:'
    echo '        - chain: |'
    sed 's/^/            /' /certs/cert.pem
    echo '          key: |'
    sed 's/^/            /' /certs/priv.key
    echo 'routes:'
    echo '  - name: www'
    echo '    listeners: [web]'
    echo '    hostnames: [{ name: "*", falls_through: true }]'
    echo '    rules:'
    echo '      - matches: [{ path: { prefix: / } }]'
    echo '        backends: [{ upstream: origin, weight: 1 }]'
    echo 'upstreams:'
    echo '  origin: { endpoints: ["127.0.0.1:9000"] }'
} >/tmp/config.yaml

# Two workers, so that a client that rebinds may land on the other one's socket and be
# forwarded to its own (16 §3).
exec edgerush proxy --config /tmp/config.yaml --workers 2 2>>/logs/edgerush.log
