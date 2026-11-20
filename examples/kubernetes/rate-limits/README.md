# Rate limits on a shared data plane

One data plane shared by five teams, with `RateLimitPolicy` at every level it has, an
`ignore` and an ignore rule, and seven common mistakes with what each one shows. The
design is in the docs (04 §4 and §6, 06 §2); this is how an operator meets it.

## The files

| Path | What |
|---|---|
| [manifests/](manifests/) | What the teams apply, one file each |
| [mistakes.yaml](mistakes.yaml) | Seven mistakes, applied on top |
| [validate/](validate/) | What `edgerush validate` needs besides: the data plane's settings, and the Gateway and classes the operator makes |

One data plane, `edge/public`, serves everyone: Gateway API routes through the Gateway
`edge/public` the operator makes of it, and Ingresses through its IngressClass
`edge.public`.

| Team | Namespace | Brings | File |
|---|---|---|---|
| Platform | `edge` | the ControlPlane and DataPlane (listeners `http`, `https`, `internal`), an HTTP→HTTPS redirect, the Gateway's policies | [10-platform.yaml](manifests/10-platform.yaml), [20-platform-policies.yaml](manifests/20-platform-policies.yaml) |
| Shop | `shop` | a ListenerSet for `shop.example.com`, the `storefront` route with four named rules | [30-shop.yaml](manifests/30-shop.yaml) |
| Search | `search` | the `search` route on `search.example.com`, a dry run | [40-search.yaml](manifests/40-search.yaml) |
| Partners, Inventory | `partners`, `inventory` | inbound webhooks; a gRPC service on the internal listener | [50-partners-and-inventory.yaml](manifests/50-partners-and-inventory.yaml) |
| Billing | `legacy` | an old app on Ingress, on `billing.example.net` | [60-legacy-ingress.yaml](manifests/60-legacy-ingress.yaml) |

Every limit's name starts with its team's name. Nothing enforces that, but names are
unique across the data plane, and prefixes are how teams keep out of each other's way.

## How to read it

A request is under every limit at every level it passes through, least specific first:

```
DataPlane global        platform-per-ip                         every request, Ingress too
  Gateway public        platform-api-per-ip, platform-writes-per-ip
    listener internal   platform-internal-per-ip   ignores platform-per-ip, platform-api-per-ip
    ListenerSet shop    shop-per-ip
      route storefront  shop-storefront-per-ip
        rule checkout   shop-checkout-per-ip       ignores shop-storefront-per-ip
        rule healthz    (none)                     ignores *
      route search      search-per-ip, search-per-ip-strict (dry run)
      rule inbound      partners-webhooks-per-ip   ignores platform-per-ip, platform-writes-per-ip
      route inventory   inventory-per-ip
  Ingress billing       legacy-billing-per-ip      (under the DataPlane only, not the Gateway)
```

- **Everything stacks.** A request is charged to every limit it's under, or to none.
- **`ignore` only reaches up the tree.** A policy can remove limits from less specific
  levels, by name. It can never remove limits further down, or another policy's limits
  on the same target.
- **A limit with a `match`** (`platform-api-per-ip`: paths under `/api`;
  `platform-writes-per-ip`: methods) only applies to the requests it matches.
- **One name, one count.** `platform-api-per-ip` counts a client across every team's
  `/api` routes together.

## What each request is under

Client `203.0.113.7` on the internet, unless the row says otherwise.

| Request | Served by | Limits | Why |
|---|---|---|---|
| `GET https://shop.example.com/` | storefront / browse | `platform-per-ip`, `shop-per-ip`, `shop-storefront-per-ip` | not under `/api`, not a write |
| `GET https://shop.example.com/api/products` | storefront / products-api | `platform-per-ip`, `platform-api-per-ip`, `shop-per-ip`, `shop-storefront-per-ip` | under `/api` |
| `POST https://shop.example.com/checkout` | storefront / checkout | `platform-per-ip`, `platform-writes-per-ip`, `shop-per-ip`, `shop-checkout-per-ip` | a write; checkout ignores the storefront's limit and counts apart |
| `GET https://shop.example.com/healthz` | storefront / healthz | none | `ignore: ["*"]` removes everything above the rule, `platform-per-ip` included |
| `GET https://search.example.com/api/search` | search / query | `platform-per-ip`, `platform-api-per-ip`, `search-per-ip`, `search-per-ip-strict` (dry run) | the same `platform-api-per-ip` count as the shop's `/api` |
| `POST https://hooks.example.com/webhooks` from partner `198.51.100.20` | webhooks / inbound | `partners-webhooks-per-ip` | the rule removed both platform limits a webhook would hit |
| gRPC `inventory.v1.Inventory/GetStock` on `inventory.internal.example.com:8443` from the office NAT `10.20.0.1` | inventory / stock | `platform-writes-per-ip`, `platform-internal-per-ip`, `inventory-per-ip` | **gotcha**: see below |
| `GET https://billing.example.net/invoices` | Ingress billing | `platform-per-ip`, `legacy-billing-per-ip` | an Ingress isn't under the Gateway |
| `GET http://shop.example.com/` | to-https / redirect | none | a redirect is under no limits |

### Counting across routes

A scraper hits twelve teams' `/api` routes at 90/s each. Each team's own limit admits
it, but `platform-api-per-ip` sees 1080/s against 1000/s and refuses about 80/s, spread
across all twelve. That's the cross-route limit doing its job, and the 429s name it:
`RateLimit: "platform-api-per-ip";r=0;t=1`.

### Checkout counted apart

A shopper browses at 190/s and checks out at 5/s. Browsing stays under
`shop-storefront-per-ip` (200/s), because checkout ignores that limit and counts against
`shop-checkout-per-ip` instead. Without the `ignore`, checkouts would also count
against the storefront limit: a shopper browsing at 198/s would push it over 200/s, and
their checkouts would be refused by `shop-storefront-per-ip`. Counted apart, browsing
can't use up checkout's room, and checkout can't use up browsing's.

### Gotchas this setup walks into

1. **gRPC is POST.** `platform-writes-per-ip` (100/s for POST, PUT, PATCH, DELETE)
   catches every gRPC call. On the internal listener, the office's whole NAT address
   gets 100/s of gRPC between all its callers. `platform-internal` ignores
   `platform-per-ip` and `platform-api-per-ip` but not the writes limit. Two fixes: the
   platform adds the writes limit to `platform-internal`'s `ignore`, or the inventory
   team ignores it on its own route, which is allowed because teams may remove any
   limit above them. The 429s say `RateLimit: "platform-writes-per-ip";…`, so it's
   found quickly.
2. **The Gateway's limits don't reach Ingress.** `billing.example.net` isn't under
   `platform-api-per-ip` or `platform-writes-per-ip`. Only the DataPlane's `global`
   covers both kinds of traffic.
3. **A host the Gateway's listeners cover is the Gateway's.** Ingress shares ports 80
   and 443 with the Gateway's listeners there. Had billing stayed on
   `billing.example.com`, its Ingress would take that host from the listeners'
   `*.example.com`, and `validate` would say so and exit 1:
   `Ingress legacy/billing claims billing.example.com, taking it from Gateway edge/public listener https's *.example.com, which does not fall through`.
4. **`ignore: ["*"]` on `/healthz` is for good.** If the platform adds a limit next
   month, health checks are still exempt. That's intended here; on a real endpoint it
   would mean a lasting exemption.
5. **`Local` mode counts per pod.** With `mode: Local`, each data-plane pod counts on its
   own, so a client spread over 3 pods gets up to 3 times every number in these files.
6. **A misspelt `ignore` removes nothing, silently.** See mistake 5 below.

## Before applying: `validate`

`edgerush validate` translates the manifests as the control plane would and compiles each
policy as it does, with no cluster. It reads no DataPlane, so it is given what the
operator makes of one besides:

```
$ edgerush validate --from-manifests manifests \
    --from-manifests validate/made-by-the-operator.yaml --data-plane validate/data-plane.yaml
11 routes and 8 upstreams translated; 0 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 0 Ingresses of other classes or none passed over
TLS Secrets were taken as they will be in the cluster, not checked: --check-secrets checks them
```

and it exits 0. With the mistakes:

```
$ edgerush validate --from-manifests manifests --from-manifests mistakes.yaml \
    --from-manifests validate/made-by-the-operator.yaml --data-plane validate/data-plane.yaml
RateLimitPolicy search/search-bad-rate: the data plane would refuse it: limit `search-suggest-per-ip`: `model.bucket.rate`: `100/sec` is not a rate: a number of at least 1, `/`, and `s`, `min`, `h` or `d`
RateLimitPolicy shop/shop-products: refused: its limit `search-per-ip` has the name of RateLimitPolicy search/search's, which holds it
note: RateLimitPolicy edge/platform-redirects: applies nothing to HTTPRoute edge/to-https section redirect, which forwards nothing: a rule that redirects is under no limits
note: RateLimitPolicy legacy/legacy-billing-backend: applies nothing to Service legacy/billing: it is not there
RateLimitPolicy legacy/legacy-billing-backend: none of its targets is there, so it applies nothing and has no status
note: RateLimitPolicy shop/shop-checkout-typo: applies nothing to HTTPRoute shop/storefront section checkuot: it has no rule `checkuot`
11 routes and 8 upstreams translated; 3 left out; 0 paths without their backend; 0 TLS hosts or entries not served as listed; 0 paths shadowed; 0 Ingresses of other classes or none passed over; 3 notes
TLS Secrets were taken as they will be in the cluster, not checked: --check-secrets checks them
```

and it exits 1. Notes fail nothing: a target not there yet, or a rule that redirects, is
what files applied in any order look like. Two mistakes it does not show:

- **Mistake 3**, targets at two levels, is refused only by the CRD's rules, which
  `validate` leaves to the API server: `kubectl apply --dry-run=server` checks them.
- **Mistake 5**, an `ignore` of a name not there, is no fault anywhere.

Of two policies with a limit of one name, the older keeps it. Manifests in a repository
carry no creation time, so `validate` takes them by namespace and name instead, as the
control plane does for two policies of one age.

## What the operator sees

### Refused at `kubectl apply`

Nothing is stored; the team fixes the file and applies it again.

```
$ kubectl apply -f mistakes.yaml
ratelimitpolicy.gateway.edgerush.dev/shop-products created
ratelimitpolicy.gateway.edgerush.dev/shop-checkout-typo created
The RateLimitPolicy "shop-mixed" is invalid: spec.targetRefs: Invalid value: "array":
  targets must all name a section, or none, as a listener is not its Gateway, nor a rule
  its route
The RateLimitPolicy "search-bad-rate" is invalid: spec.limits[0].model.bucket.rate:
  Invalid value: "100/sec": spec.limits[0].model.bucket.rate in body should match
  '^[1-9][0-9]*/(s|min|h|d)$'
ratelimitpolicy.gateway.edgerush.dev/inventory-writes created
The RateLimitPolicy "legacy-billing-backend" is invalid: spec.targetRefs[0]: Invalid
  value: "object": a target is a Gateway, ListenerSet, HTTPRoute or GRPCRoute of
  gateway.networking.k8s.io, or an Ingress of networking.k8s.io
ratelimitpolicy.gateway.edgerush.dev/platform-redirects created
```

The admission webhook goes further than the schema and compiles the policy alone. It
can't see other policies, targets or the DataPlane, so a taken name, a missing target
and a redirecting rule all get through to status.

### Stored, with the reason in status

A policy's status has an entry for each Gateway or Ingress its targets reach, and its
`Accepted` condition says why it applies nothing (`kubectl get ratelimitpolicy -o yaml`).

**Mistake 1, a taken name.** The whole policy is out, `shop-products-burst` included.
The message names the limit, not the search team's policy, which is in another
namespace.

```yaml
status:
  ancestors:
    - ancestorRef: { group: gateway.networking.k8s.io, kind: Gateway, namespace: edge, name: public }
      controllerName: edgerush.dev/gateway-controller
      conditions:
        - type: Accepted
          status: "False"
          reason: Conflicted
          message: "the name of its limit `search-per-ip` is held by another RateLimitPolicy"
          observedGeneration: 1
```

**Mistake 2, a `sectionName` typo.** It's kept, and starts working by itself if a rule
named `checkuot` ever appears.

```yaml
        - type: Accepted
          status: "False"
          reason: TargetNotFound
          message: "applies nothing to HTTPRoute shop/storefront section checkuot: it has no rule `checkuot`"
```

**Mistake 7, a limit on a redirect.**

```yaml
        - type: Accepted
          status: "False"
          reason: NotForwarding
          message: "applies nothing to HTTPRoute edge/to-https section redirect, which forwards nothing: a rule that redirects is under no limits"
```

**An update that breaks a working policy.** Say the shop team edits `shop-all` so that
generation 4 takes the name `search-per-ip`. Generation 3 keeps applying:

```yaml
        - type: Accepted
          status: "False"
          reason: Conflicted
          message: "the name of its limit `search-per-ip` is held by another RateLimitPolicy; generation 3 applies instead"
          observedGeneration: 4
```

**On the targets**, a route's status shows which policies reach it. The condition sits
in the route's parent entry, here the ListenerSet:

```
    Type:     gateway.edgerush.dev/RateLimitPolicyAffected
    Status:   True
    Reason:   Accepted
    Message:  affected by RateLimitPolicy shop/shop-checkout, RateLimitPolicy shop/shop-healthz, RateLimitPolicy shop/shop-storefront
```

### Mistake 5, silently doing less

`inventory-writes` ignores `platform-write-per-ip`, a name that isn't there. The policy
is accepted, because ignoring a name that isn't there is not an error: a limit renamed
later never breaks a policy. The office's gRPC calls are still refused past 100/s,
answered `RESOURCE_EXHAUSTED` as a gRPC call refused by a limit is, and the metrics name
the limit refusing them:

```
edgerush_rate_limit_decisions_total{limit="platform-writes-per-ip", outcome="refused"}  18234
```

The fix is one letter in the `ignore`.

### A dry run

`search-per-ip-strict` never refuses. Before enforcing it, the search team checks
whether 20/s would have hurt anyone:

```
edgerush_rate_limit_decisions_total{limit="search-per-ip-strict", outcome="admitted"}         9120455
edgerush_rate_limit_decisions_total{limit="search-per-ip-strict", outcome="dry_run_refused"}     1032
```

Enforcing it means deleting `dryRun: true`. Its counts carry on under its name.
