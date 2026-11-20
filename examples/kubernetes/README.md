# Kubernetes designs — mostly not implemented

These manifests, but `rate-limits/`, illustrate planned APIs. Do not apply them expecting
a working gateway: the ingress-nginx annotation translator they use is not built, and the
`ControlPlane` and `DataPlane` here are early designs whose fields differ from the CRDs the
operator now serves.

The operator and control plane run today. The README's
[In a cluster: the operator](../../README.md#in-a-cluster-the-operator) shows the two
resources that make a working gateway, and [kind/operator.sh](../../kind/operator.sh) does
it end to end; the Ingress and HTTPRoute in [02-routes.yaml](02-routes.yaml) are the kind
of objects it then serves.

| File | Design |
|---|---|
| [01-planes.yaml](01-planes.yaml) | A team's control plane and separate data planes for API and WebSocket traffic |
| [02-routes.yaml](02-routes.yaml) | Ingress and HTTPRoute sharing a data plane |
| [04-ingress-nginx.yaml](04-ingress-nginx.yaml) | Planned translation of ingress-nginx annotations |

[rate-limits/](rate-limits/README.md) is built: a data plane shared by several teams, its
RateLimitPolicies at every level, and common mistakes with what `edgerush validate`, `kubectl`
and status show of each.

For a working local example, use [Docker Compose](../docker-compose/README.md).
