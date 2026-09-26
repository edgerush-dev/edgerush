# quic-interop-runner endpoint

EdgeRush as a server of [quic-interop-runner](https://github.com/quic-interop/quic-interop-runner),
which runs every QUIC client it knows against it over a simulated network
([docs/16 §8](../../docs/16-http3.md) has what it found). The image is the gateway built
with its `interop` feature, which speaks HTTP/0.9 over QUIC (`hq-interop`) beside HTTP/3
for the runner's transport cases, with NGINX behind it serving the runner's files. A
release build has none of that.

## Building

From the repository's root, on Linux with Docker:

```sh
docker build -f interop/Dockerfile -t edgerush-qns:latest .
```

## Running

The runner wants Python 3.13 at most (pyshark breaks on 3.14) and tshark 4.5 or later.
In its checkout, add EdgeRush to `implementations_quic.json`:

```json
"edgerush": { "image": "edgerush-qns:latest", "url": "https://github.com/lstyles/edgerush", "role": "server" }
```

then run clients against it, all cases or some:

```sh
python run.py -s edgerush -c quic-go,ngtcp2,quiche -j results.json -l logs
python run.py -s edgerush -c chrome -t http3
```

A case the endpoint does not take exits with 127, which the runner shows as unsupported
(`run_endpoint.sh` says which and why).
