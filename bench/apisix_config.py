"""APISIX's config.yaml and apisix.yaml for one variant, written into a directory.

Arguments: the apisix.yaml template, the directory, the variant (apisix, apisix-local or
apisix-rl), the rate a second, Redis's wait in milliseconds, whether a failed count
refuses the request (true or false), worker processes, whether over TLS (1 or 0), and the
directory holding cert.pem and key.pem.
"""
import sys

template, out, variant, rate, timeout_ms, deny, workers, tls, tls_dir = sys.argv[1:]
routes = open(template).read()
if variant != "apisix":
    limit = (
        "      limit-count:\n"
        f"        count: {rate}\n"
        "        time_window: 1\n"
        "        key_type: var\n"
        "        key: remote_addr\n"
        "        rejected_code: 429\n"
        "        show_limit_quota_header: false\n"
    )
    if variant == "apisix-local":
        limit += "        policy: local\n"
    else:
        degrade = "false" if deny == "true" else "true"
        limit += (
            "        policy: redis\n"
            "        redis_host: 127.0.0.1\n"
            "        redis_port: 6379\n"
            f"        redis_timeout: {timeout_ms}\n"
            f"        allow_degradation: {degrade}\n"
        )
    plugins = "    upstream_id: backend\n    plugins:\n"
    assert routes.count(plugins) == 1, "the measured route moved"
    routes = routes.replace(plugins, plugins + limit)
if tls == "1":
    def pem(name):
        return "".join("      " + line for line in open(f"{tls_dir}/{name}"))
    routes += (
        "ssls:\n  - id: bench\n    snis: [bench.example.com]\n"
        f"    cert: |\n{pem('cert.pem')}    key: |\n{pem('key.pem')}"
    )
open(f"{out}/apisix.yaml", "w").write(routes + "#END\n")

plain, secure = ("18080", "true") if tls == "1" else ("8080", "false")
open(f"{out}/config.yaml", "w").write(f"""deployment:
  role: data_plane
  role_data_plane:
    config_provider: yaml
apisix:
  enable_admin: false
  enable_control: false
  enable_server_tokens: false
  enable_ipv6: false
  enable_http2: true
  node_listen:
    - ip: 127.0.0.1
      port: {plain}
  ssl:
    enable: {secure}
    # A client that names no server, as the readiness check does, gets the bench's
    # certificate, as from the others' one certificate.
    fallback_sni: bench.example.com
    listen:
      - ip: 127.0.0.1
        port: 8080
nginx_config:
  worker_processes: {workers}
  error_log_level: warn
  worker_rlimit_nofile: 200000
  event:
    worker_connections: 65536
  http:
    enable_access_log: false
    keepalive_timeout: 3600s
  http_configuration_snippet: |
    keepalive_requests 1000000000;
plugins:
  - proxy-rewrite
  - response-rewrite
  - fault-injection
  - limit-count
""")
