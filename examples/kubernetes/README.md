# Kubernetes designs — not implemented

These manifests illustrate the planned APIs. EdgeRush does not yet provide the operator,
control plane, CRDs, rate limiter or ingress-nginx translator they need. Do not apply
them expecting a working gateway; fields may change before implementation.

| File | Design |
|---|---|
| [01-planes.yaml](01-planes.yaml) | A team's control plane and separate data planes for API and WebSocket traffic |
| [02-routes.yaml](02-routes.yaml) | Ingress and HTTPRoute sharing a data plane |
| [03-rate-limits.yaml](03-rate-limits.yaml) | Tables, hierarchical rate limits and signal-driven shedding |
| [04-ingress-nginx.yaml](04-ingress-nginx.yaml) | Planned translation of ingress-nginx annotations |

For a working local example, use [Docker Compose](../docker-compose/README.md).
