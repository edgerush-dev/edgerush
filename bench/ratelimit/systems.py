"""Each system under test as it ships, configured for the contract (100 a second, burst
100 where it has one) on `n` instances, as a Docker Compose project.

Clients reach the instances on a front network; instances reach their store or peers on a
back network, the one faults touch. Addresses are fixed so that faults can name them.
"""

import json
import os

PROJECT = "rlh"
FRONT, BACK = "172.31.10", "172.31.20"
UPSTREAM_IP = f"{FRONT}.5"
# Where the load comes from as instances see it: the host, the front network's gateway.
CLIENT_IP = f"{FRONT}.1"
REDIS_IP, RATELIMIT_IP = f"{BACK}.2", f"{BACK}.3"
# EdgeRush's rate-limiting service: three pods, and where the binaries built on the host are.
SERVICE_IPS = [f"{BACK}.{4 + pod}" for pod in range(3)]
BINARIES = os.environ.get("RLH_BIN", "/home/claude/rlh-bin")

IMAGES = {
    "envoy": "envoyproxy/envoy:v1.38.7",
    "ratelimit": "envoyproxy/ratelimit:8fe6ea42",
    "gubernator": "ghcr.io/gubernator-io/gubernator:v2.20.3",
    "apisix": "apache/apisix:3.19.0-debian",
    "kong": "kong:3.9.4",
    "traefik": "traefik:v3.7.14",
    "tyk": "tykio/tyk-gateway:v5.15.1",
    "haproxy": "haproxy:3.2",
    "redis": "redis:8.6.7",
    "upstream": "nginx:1.29",
    # EdgeRush runs from binaries built on the host, in an image with the host's glibc.
    "edgerush": "ubuntu:26.04",
}


def front_ip(i):
    """Instance `i` (0-based) on the front network."""
    return f"{FRONT}.{11 + i}"


def back_ip(i):
    """Instance `i` (0-based) on the back network."""
    return f"{BACK}.{11 + i}"


def name(i):
    return f"{PROJECT}-gw{i + 1}"


UPSTREAM_CONF = """server {
    listen 80;
    keepalive_requests 10000000;
    access_log off;
    location / { return 200 "ok\\n"; }
}
"""

ENVOY = """static_resources:
  listeners:
  - name: web
    address: {socket_address: {address: 0.0.0.0, port_value: 8080}}
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: web
          route_config:
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: {prefix: "/"}
                route:
                  cluster: upstream
                  rate_limits:
                  - actions:
                    - request_headers: {header_name: x-key, descriptor_key: key}
          http_filters:
          - name: envoy.filters.http.ratelimit
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.ratelimit.v3.RateLimit
              domain: rl%(timeout)s
              rate_limit_service:
                transport_api_version: V3
                grpc_service: {envoy_grpc: {cluster_name: ratelimit}}
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: upstream
    type: STATIC
    load_assignment:
      cluster_name: upstream
      endpoints: [{lb_endpoints: [{endpoint: {address: {socket_address: {address: %(upstream)s, port_value: 80}}}}]}]
  - name: ratelimit
    type: STATIC
    typed_extension_protocol_options:
      envoy.extensions.upstreams.http.v3.HttpProtocolOptions:
        "@type": type.googleapis.com/envoy.extensions.upstreams.http.v3.HttpProtocolOptions
        explicit_http_config: {http2_protocol_options: {}}
    load_assignment:
      cluster_name: ratelimit
      endpoints: [{lb_endpoints: [{endpoint: {address: {socket_address: {address: %(ratelimit)s, port_value: 8081}}}}]}]
"""

ENVOY_LOCAL = """cluster_manager:
  local_cluster_name: local
static_resources:
  listeners:
  - name: web
    address: {socket_address: {address: 0.0.0.0, port_value: 8080}}
    filter_chains:
    - filters:
      - name: envoy.filters.network.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          stat_prefix: web
          route_config:
            virtual_hosts:
            - name: all
              domains: ["*"]
              routes:
              - match: {prefix: "/"}
                route: {cluster: upstream}
          http_filters:
          - name: envoy.filters.http.local_ratelimit
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.local_ratelimit.v3.LocalRateLimit
              stat_prefix: rl
              token_bucket: {max_tokens: 100, tokens_per_fill: 100, fill_interval: 1s}
              filter_enabled: {default_value: {numerator: 100, denominator: HUNDRED}}
              filter_enforced: {default_value: {numerator: 100, denominator: HUNDRED}}
              local_cluster_rate_limit: {}
          - name: envoy.filters.http.router
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.filters.http.router.v3.Router
  clusters:
  - name: upstream
    type: STATIC
    load_assignment:
      cluster_name: upstream
      endpoints: [{lb_endpoints: [{endpoint: {address: {socket_address: {address: %(upstream)s, port_value: 80}}}}]}]
  - name: local
    type: STATIC
    load_assignment:
      cluster_name: local
      endpoints: [{lb_endpoints: [%(instances)s]}]
"""

RATELIMIT = """domain: rl
descriptors:
  - key: key
    rate_limit:
      unit: second
      requests_per_unit: 100
"""

APISIX_CONFIG = """deployment:
  role: data_plane
  role_data_plane:
    config_provider: yaml
apisix:
  node_listen: 9080
  enable_admin: false
nginx_config:
  worker_processes: 1
  error_log_level: warn
"""

APISIX_ROUTES = """upstreams:
  - id: 1
    type: roundrobin
    nodes:
      "%(upstream)s:80": 1
routes:
  - id: rl
    uri: /*
    upstream_id: 1
    plugins:
      limit-count:
        count: 100
        time_window: 1
        key_type: var
        key: http_x_key
        policy: redis
        redis_host: %(redis)s
        rejected_code: 429%(extra)s
#END
"""

KONG = """_format_version: "3.0"
services:
  - name: up
    url: http://%(upstream)s:80
    routes:
      - name: all
        paths: ["/"]
    plugins:
      - name: rate-limiting
        config:
          second: 100
          policy: redis
          limit_by: header
          header_name: x-key
          redis:
            host: %(redis)s
            port: 6379
"""

TRAEFIK = """http:
  routers:
    all:
      rule: "PathPrefix(`/`)"
      service: up
      middlewares: [rl]
      entryPoints: [web]
  middlewares:
    rl:
      rateLimit:
        average: 100
        period: 1s
        burst: 100
        sourceCriterion:
          requestHeaderName: X-Key
        redis:
          endpoints: ["%(redis)s:6379"]
  services:
    up:
      loadBalancer:
        servers:
          - url: "http://%(upstream)s:80"
"""

TYK_CONF = {
    "listen_port": 8080,
    "secret": "harness",
    "template_path": "/opt/tyk-gateway/templates",
    "app_path": "/opt/tyk-gateway/apps",
    "middleware_path": "/opt/tyk-gateway/middleware",
    "use_db_app_configs": False,
    "storage": {"type": "redis", "host": REDIS_IP, "port": 6379},
    "enable_analytics": False,
    "hash_keys": True,
    "log_level": "warn",
}

TYK_API = {
    "name": "rl",
    "api_id": "rl",
    "org_id": "default",
    "use_keyless": True,
    "active": True,
    "version_data": {
        "not_versioned": True,
        "versions": {"Default": {"name": "Default", "use_extended_paths": True}},
    },
    "proxy": {
        "listen_path": "/",
        "target_url": f"http://{UPSTREAM_IP}:80",
        "strip_listen_path": False,
    },
    "global_rate_limit": {"rate": 100, "per": 1},
}

HAPROXY = """global
    nbthread 1
defaults
    mode http
    timeout connect 5s
    timeout client 30s
    timeout server 30s
peers mesh
%(peers)s
frontend web
    bind :8080
    stick-table type string len 64 size 100k expire 10s store http_req_rate(1s) peers mesh
    http-request track-sc0 req.hdr(x-key)
    http-request deny deny_status 429 if { sc_http_req_rate(0) gt 100 }
    default_backend up
backend up
    server u %(upstream)s:80
"""


class System:
    """What the harness needs to know of a system: how to build it, where its instances
    listen, how to ask them, and what its 'owner' is for faults."""

    def __init__(self, key, port, ask, owner, stores, build):
        self.key = key
        self.port = port
        # 'http': a GET with the key in X-Key, 200 admitted and 429 refused;
        # 'gubernator': a POST to its API, the answer's status deciding.
        self.ask = ask
        # 'store': the store goes or restarts; 'instance': the instance owning the key does;
        # 'first': no owner, so the first instance.
        self.owner = owner
        # Containers on the back network beside the instances: the store side of a partition.
        self.stores = stores
        self._build = build

    def project(self, n):
        """(compose document, {relative path: content})."""
        services, files = self._build(n)
        services["upstream"] = {
            "image": IMAGES["upstream"],
            "container_name": f"{PROJECT}-upstream",
            "volumes": ["./upstream.conf:/etc/nginx/conf.d/default.conf:ro"],
            "networks": {"front": {"ipv4_address": UPSTREAM_IP}},
        }
        files["upstream.conf"] = UPSTREAM_CONF
        compose = {
            "name": PROJECT,
            "services": services,
            "networks": {
                "front": {"ipam": {"config": [{"subnet": f"{FRONT}.0/24"}]}},
                "back": {"ipam": {"config": [{"subnet": f"{BACK}.0/24"}]}},
            },
        }
        return compose, files


def gateway(i, image, **more):
    """An instance on both networks, held to one CPU as every system's is."""
    service = {
        "image": image,
        "container_name": name(i),
        "hostname": f"gw{i + 1}",
        "cpus": 1,
        "ulimits": {"nofile": {"soft": 65536, "hard": 65536}},
        "networks": {
            "front": {"ipv4_address": front_ip(i)},
            "back": {"ipv4_address": back_ip(i)},
        },
    }
    service.update(more)
    return service


def redis():
    return {
        "image": IMAGES["redis"],
        "container_name": f"{PROJECT}-redis",
        "command": ["redis-server", "--save", "", "--appendonly", "no"],
        "networks": {"back": {"ipv4_address": REDIS_IP}},
    }


def build_envoy(n, timeout=""):
    services = {
        "redis": redis(),
        "ratelimit": {
            "image": IMAGES["ratelimit"],
            "container_name": f"{PROJECT}-ratelimit",
            "command": ["/bin/ratelimit"],
            "environment": {
                "USE_STATSD": "false",
                "LOG_LEVEL": "warn",
                "REDIS_SOCKET_TYPE": "tcp",
                "REDIS_URL": f"{REDIS_IP}:6379",
                "RUNTIME_ROOT": "/data",
                "RUNTIME_SUBDIRECTORY": "ratelimit",
                "RUNTIME_WATCH_ROOT": "false",
            },
            "volumes": ["./ratelimit.yaml:/data/ratelimit/config/config.yaml:ro"],
            "networks": {"back": {"ipv4_address": RATELIMIT_IP}},
        },
    }
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["envoy"],
            command=["envoy", "-c", "/etc/envoy.yaml", "--concurrency", "1", "-l", "warn"],
            volumes=["./envoy.yaml:/etc/envoy.yaml:ro"],
        )
    files = {
        "envoy.yaml": ENVOY
        % {"upstream": UPSTREAM_IP, "ratelimit": RATELIMIT_IP, "timeout": timeout},
        "ratelimit.yaml": RATELIMIT,
    }
    return services, files


def build_envoy_local(n):
    """Envoy's own token bucket, the limit divided among the instances of its local cluster
    (`local_cluster_rate_limit`): each request costs N tokens of a bucket of 100."""
    instances = ", ".join(
        f"{{endpoint: {{address: {{socket_address: {{address: {front_ip(i)}, port_value: 8080}}}}}}}}"
        for i in range(n)
    )
    services = {}
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["envoy"],
            command=["envoy", "-c", "/etc/envoy.yaml", "--concurrency", "1", "-l", "warn"],
            volumes=["./envoy.yaml:/etc/envoy.yaml:ro"],
        )
    files = {"envoy.yaml": ENVOY_LOCAL % {"upstream": UPSTREAM_IP, "instances": instances}}
    return services, files


def build_gubernator(n):
    services = {}
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["gubernator"],
            environment={
                "GUBER_GRPC_ADDRESS": "0.0.0.0:1051",
                "GUBER_HTTP_ADDRESS": "0.0.0.0:1050",
                "GUBER_ADVERTISE_ADDRESS": f"{back_ip(i)}:1051",
                "GUBER_MEMBERLIST_KNOWN_NODES": f"{back_ip(0)}",
                "GOMAXPROCS": "1",
            },
        )
    return services, {}


def build_apisix(extra):
    def build(n):
        services = {"redis": redis()}
        for i in range(n):
            services[f"gw{i + 1}"] = gateway(
                i,
                IMAGES["apisix"],
                volumes=[
                    "./config.yaml:/usr/local/apisix/conf/config.yaml:ro",
                    "./apisix.yaml:/usr/local/apisix/conf/apisix.yaml:ro",
                ],
            )
        files = {
            "config.yaml": APISIX_CONFIG,
            "apisix.yaml": APISIX_ROUTES
            % {"upstream": UPSTREAM_IP, "redis": REDIS_IP, "extra": extra},
        }
        return services, files

    return build


def build_kong(n):
    services = {"redis": redis()}
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["kong"],
            environment={
                "KONG_DATABASE": "off",
                "KONG_DECLARATIVE_CONFIG": "/kong/kong.yml",
                "KONG_PROXY_LISTEN": "0.0.0.0:8000",
                "KONG_ADMIN_LISTEN": "127.0.0.1:8001",
                "KONG_NGINX_WORKER_PROCESSES": "1",
                "KONG_LOG_LEVEL": "warn",
                "KONG_PROXY_ACCESS_LOG": "off",
            },
            volumes=["./kong.yml:/kong/kong.yml:ro"],
        )
    return services, {"kong.yml": KONG % {"upstream": UPSTREAM_IP, "redis": REDIS_IP}}


def build_traefik(n):
    services = {"redis": redis()}
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["traefik"],
            command=[
                "--entryPoints.web.address=:8000",
                "--providers.file.filename=/etc/traefik/dynamic.yml",
                "--log.level=WARN",
            ],
            environment={"GOMAXPROCS": "1"},
            volumes=["./dynamic.yml:/etc/traefik/dynamic.yml:ro"],
        )
    files = {"dynamic.yml": TRAEFIK % {"upstream": UPSTREAM_IP, "redis": REDIS_IP}}
    return services, files


def build_tyk(n):
    services = {"redis": redis()}
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["tyk"],
            environment={"GOMAXPROCS": "1"},
            volumes=[
                "./tyk.conf:/opt/tyk-gateway/tyk.conf:ro",
                "./apps:/opt/tyk-gateway/apps:ro",
            ],
        )
    files = {
        "tyk.conf": json.dumps(TYK_CONF, indent=2) + "\n",
        "apps/rl.json": json.dumps(TYK_API, indent=2) + "\n",
    }
    return services, files


def build_haproxy(n):
    peers = "\n".join(f"    peer gw{i + 1} {back_ip(i)}:10000" for i in range(n))
    services = {}
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["haproxy"],
            command=["haproxy", "-f", "/usr/local/etc/haproxy/haproxy.cfg", "-L", f"gw{i + 1}"],
            volumes=["./haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg:ro"],
        )
    return services, {"haproxy.cfg": HAPROXY % {"peers": peers, "upstream": UPSTREAM_IP}}


EDGERUSH = """listeners:
  web: { address: "0.0.0.0:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: rl
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: up, weight: 1 }] }
        rate_limits: [per-client]
upstreams:
  up: { load_balancer: p2c, endpoints: ["%(upstream)s:80"] }
rate_limit_service: { pods: [%(pods)s], cycle_ms: 10 }
limits_file: limits.yaml
"""

# Keyed by the client's address: until authentication, the only key EdgeRush takes. Every
# run's load comes from the host, so every run is the one key; a bucket of 100 at 100 a
# second is full again between runs.
EDGERUSH_LIMITS = """limits:
  - { name: per-client, key: client, model: { bucket: { rate: 100/s, burst: 100 } } }
"""


def build_edgerush(n):
    """EdgeRush with the rate-limiting service: three service pods, and proxies of one
    worker each, every worker a data plane of the service."""
    pods = [f"{pod}={SERVICE_IPS[pod]}:7000" for pod in range(3)]
    services = {}
    for pod in range(3):
        command = ["/opt/bin/edgerush-ratelimit-service", "--id", str(pod),
                   "--listen", "0.0.0.0:7000", "--cycle-ms", "10"]
        for named in pods:
            command += ["--pod", named]
        services[f"rl{pod}"] = {
            "image": IMAGES["edgerush"],
            "container_name": f"{PROJECT}-rl{pod}",
            "command": command,
            "volumes": [f"{BINARIES}:/opt/bin:ro"],
            "networks": {"back": {"ipv4_address": SERVICE_IPS[pod]}},
        }
    for i in range(n):
        services[f"gw{i + 1}"] = gateway(
            i,
            IMAGES["edgerush"],
            command=["/opt/bin/edgerush", "proxy", "--config", "/etc/edgerush.yaml",
                     "--workers", "1"],
            volumes=[f"{BINARIES}:/opt/bin:ro", "./edgerush.yaml:/etc/edgerush.yaml:ro",
                     "./limits.yaml:/etc/limits.yaml:ro"],
        )
    files = {
        "edgerush.yaml": EDGERUSH % {
            "upstream": UPSTREAM_IP,
            "pods": ", ".join(f'"{ip}:7000"' for ip in SERVICE_IPS),
        },
        "limits.yaml": EDGERUSH_LIMITS,
    }
    return services, files


def fnv1a(data):
    h = 0xcbf29ce484222325
    for byte in data:
        h ^= byte
        h = (h * 0x100000001b3) & 0xFFFFFFFFFFFFFFFF
    return h


def mix(x):
    x = ((x ^ (x >> 30)) * 0xbf58476d1ce4e5b9) & 0xFFFFFFFFFFFFFFFF
    x = ((x ^ (x >> 27)) * 0x94d049bb133111eb) & 0xFFFFFFFFFFFFFFFF
    return x ^ (x >> 31)


def service_owner(pods=(0, 1, 2)):
    """The service pod owning the load's key — the limit's name, a zero, and the client's
    address, the host's — as `edgerush_ratelimit::service::View::owners` ranks them."""
    address = bytes(int(octet) for octet in CLIENT_IP.split("."))
    h = fnv1a(b"per-client" + bytes([0]) + address)
    return max(pods, key=lambda pod: (mix(h ^ mix(pod + 1)), pod))


SYSTEMS = {
    s.key: s
    for s in [
        System("envoy", 8080, "http", "store", ["redis", "ratelimit"], build_envoy),
        System(
            "envoy-wait",
            8080,
            "http",
            "store",
            ["redis", "ratelimit"],
            lambda n: build_envoy(n, "\n              timeout: 1s"),
        ),
        System("envoy-local", 8080, "http", "first", [], build_envoy_local),
        System("gubernator", 1050, "gubernator", "instance", [], build_gubernator),
        System("apisix", 9080, "http", "store", ["redis"], build_apisix("")),
        System(
            "apisix-delayed",
            9080,
            "http",
            "store",
            ["redis"],
            build_apisix("\n        sync_interval: 0.1"),
        ),
        System("kong", 8000, "http", "store", ["redis"], build_kong),
        System("traefik", 8000, "http", "store", ["redis"], build_traefik),
        System("tyk", 8080, "http", "first", ["redis"], build_tyk),
        System("haproxy", 8080, "http", "first", [], build_haproxy),
        System("edgerush", 8080, "http", "service", ["rl0", "rl1", "rl2"], build_edgerush),
    ]
}

STORE_IPS = {"redis": REDIS_IP, "ratelimit": RATELIMIT_IP} | {
    f"rl{pod}": ip for pod, ip in enumerate(SERVICE_IPS)
}
