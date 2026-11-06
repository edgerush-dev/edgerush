//! What the types read: an object of the experimental channel, with a value and fields the
//! standard channel lacks, and one without a field its schema requires.

use edgerush_gateway_api::backend_tls_policy::BackendTLSPolicy;
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::grpc_route::GRPCRoute;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::listener_set::ListenerSet;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
use edgerush_gateway_api::tcp_route::TCPRoute;
use edgerush_gateway_api::tls_route::TLSRoute;
use k8s_openapi::{ListableResource, Resource};

#[test]
fn an_experimental_route_is_read_its_unknown_parts_as_they_are() {
    // `ExternalAuth` and `sessionPersistence` are the experimental channel's (v1.7.0).
    let route: HTTPRoute = serde_saphyr::from_str(
        r#"
        apiVersion: gateway.networking.k8s.io/v1
        kind: HTTPRoute
        metadata: { name: web, namespace: shop }
        spec:
          parentRefs: [{ name: edge }]
          rules:
          - filters:
            - type: ExternalAuth
              externalAuth: { protocol: HTTP, backendRef: { name: auth, port: 8080 } }
            sessionPersistence: { type: Cookie }
            backendRefs: [{ name: web, port: 80 }]
        "#,
    )
    .unwrap();
    let rules = route.spec.rules;
    let filters = rules[0].filters.as_ref().unwrap();
    assert_eq!(filters[0].r#type, "ExternalAuth");
    assert_eq!(route.metadata.name.as_deref(), Some("web"));
}

#[test]
fn a_required_field_missing_is_read_empty() {
    // A backendRef's `name` is required.
    let route: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: web }\nspec: { rules: [{ backendRefs: [{ port: 80 }] }] }",
    )
    .unwrap();
    let rules = route.spec.rules;
    assert_eq!(rules[0].backend_refs.as_ref().unwrap()[0].name, "");
}

#[test]
fn what_a_file_leaves_out_reads_as_the_api_server_would_have_stored_it() {
    let route: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: web }
spec: { rules: [{ backendRefs: [{ name: web, port: 80 }] }] }",
    )
    .unwrap();
    let rule = &route.spec.rules[0];
    // A rule of no match matches the path prefix `/`, the default's own default in it.
    assert_eq!(rule.matches.len(), 1);
    assert_eq!(rule.matches[0].path.r#type, "PathPrefix");
    assert_eq!(rule.matches[0].path.value, "/");
    // A backend's weight is 1, its group the core's and its kind Service.
    let backend = &rule.backend_refs.as_ref().unwrap()[0];
    assert_eq!(
        (
            backend.weight,
            backend.group.as_str(),
            backend.kind.as_str()
        ),
        (1, "", "Service")
    );
    // A route of no rule has the one rule matching every path.
    let route: HTTPRoute = serde_saphyr::from_str(
        "metadata: { name: web }
spec: {}",
    )
    .unwrap();
    assert_eq!(route.spec.rules.len(), 1);
    assert_eq!(route.spec.rules[0].matches[0].path.value, "/");
    // A listener that says nothing of its routes takes them from its own namespace.
    let gateway: Gateway = serde_saphyr::from_str(
        "metadata: { name: web }
spec: { gatewayClassName: x, listeners: [{ name: http, port: 80, protocol: HTTP }] }",
    )
    .unwrap();
    assert_eq!(
        gateway.spec.listeners[0].allowed_routes.namespaces.from,
        "Same"
    );
    // A ListenerSet's parent is a Gateway of the Gateway API's group unless it says
    // otherwise, and its listeners take routes from their own namespace.
    let set: ListenerSet = serde_saphyr::from_str(
        "metadata: { name: team, namespace: shop }
spec:
  parentRef: { name: web, namespace: edge }
  listeners: [{ name: http, port: 8080, protocol: HTTP }]",
    )
    .unwrap();
    assert_eq!(
        (
            set.spec.parent_ref.group.as_str(),
            set.spec.parent_ref.kind.as_str()
        ),
        ("gateway.networking.k8s.io", "Gateway")
    );
    assert_eq!(set.spec.listeners[0].allowed_routes.namespaces.from, "Same");
    // A GRPCRoute's method and header matches are exact, its backends as an HTTPRoute's;
    // unlike an HTTPRoute's, its CRD gives a rule no match and a route no rule.
    let route: GRPCRoute = serde_saphyr::from_str(
        "metadata: { name: calls }
spec:
  rules:
  - matches: [{ method: { service: shop.Orders }, headers: [{ name: tier, value: gold }] }]
    backendRefs: [{ name: calls, port: 50051 }]
  - backendRefs: [{ name: calls, port: 50051 }]",
    )
    .unwrap();
    let rules = route.spec.rules.as_ref().unwrap();
    let matched = &rules[0].matches.as_ref().unwrap()[0];
    assert_eq!(matched.method.as_ref().unwrap().r#type, "Exact");
    assert_eq!(matched.headers.as_ref().unwrap()[0].r#type, "Exact");
    let backend = &rules[0].backend_refs.as_ref().unwrap()[0];
    assert_eq!(
        (
            backend.weight,
            backend.group.as_str(),
            backend.kind.as_str()
        ),
        (1, "", "Service")
    );
    assert!(rules[1].matches.is_none());
    let route: GRPCRoute = serde_saphyr::from_str(
        "metadata: { name: calls }
spec: {}",
    )
    .unwrap();
    assert!(route.spec.rules.is_none());
    // A TLSRoute's one rule holds backends alone, defaulted as an HTTPRoute's; its
    // hostnames, which its CRD requires, read as none where a manifest leaves them out.
    let route: TLSRoute = serde_saphyr::from_str(
        "metadata: { name: api }
spec:
  hostnames: [api.example.com]
  rules: [{ backendRefs: [{ name: api, port: 8443 }] }]",
    )
    .unwrap();
    assert_eq!(route.spec.hostnames, ["api.example.com"]);
    let backend = &route.spec.rules[0].backend_refs[0];
    assert_eq!(
        (
            backend.weight,
            backend.group.as_str(),
            backend.kind.as_str(),
            backend.port
        ),
        (1, "", "Service", Some(8443))
    );
    let route: TLSRoute = serde_saphyr::from_str(
        "metadata: { name: api }
spec: {}",
    )
    .unwrap();
    assert!(route.spec.hostnames.is_empty() && route.spec.rules.is_empty());
    // A TCPRoute's one rule holds backends alone, defaulted as a TLSRoute's; it has no
    // hostnames.
    let route: TCPRoute = serde_saphyr::from_str(
        "metadata: { name: db }
spec:
  rules: [{ backendRefs: [{ name: db, port: 5432 }] }]",
    )
    .unwrap();
    let backend = &route.spec.rules[0].backend_refs[0];
    assert_eq!(
        (
            backend.weight,
            backend.group.as_str(),
            backend.kind.as_str(),
            backend.port
        ),
        (1, "", "Service", Some(5432))
    );
    let route: TCPRoute = serde_saphyr::from_str(
        "metadata: { name: db }
spec: {}",
    )
    .unwrap();
    assert!(route.spec.rules.is_empty());
    // A BackendTLSPolicy's target and validation, its CA references defaulted as the spec
    // has them; its options a map.
    let policy: BackendTLSPolicy = serde_saphyr::from_str(
        "metadata: { name: tls }
spec:
  targetRefs: [{ group: '', kind: Service, name: web, sectionName: https }]
  validation:
    caCertificateRefs: [{ group: '', kind: ConfigMap, name: ca }]
    hostname: web.example
    subjectAltNames: [{ type: URI, uri: 'spiffe://cluster/web' }]
  options: { example.com/x: y }",
    )
    .unwrap();
    let target = &policy.spec.target_refs[0];
    assert_eq!(
        (target.kind.as_str(), target.section_name.as_deref()),
        ("Service", Some("https"))
    );
    let validation = &policy.spec.validation;
    assert_eq!(validation.hostname, "web.example");
    assert_eq!(
        validation.ca_certificate_refs.as_ref().unwrap()[0].name,
        "ca"
    );
    let san = &validation.subject_alt_names.as_ref().unwrap()[0];
    assert_eq!(
        (san.r#type.as_str(), san.uri.as_deref()),
        ("URI", Some("spiffe://cluster/web"))
    );
    assert_eq!(policy.spec.options.unwrap()["example.com/x"], "y");
}

#[test]
fn each_kind_is_listed_where_the_api_server_serves_it() {
    let at = |group: &str, version: &str, plural: &str, list: &str| {
        (
            group.to_owned(),
            version.to_owned(),
            plural.to_owned(),
            list.to_owned(),
        )
    };
    fn of<K: ListableResource>() -> (String, String, String, String) {
        (
            K::GROUP.to_owned(),
            K::VERSION.to_owned(),
            K::URL_PATH_SEGMENT.to_owned(),
            K::LIST_KIND.to_owned(),
        )
    }
    let group = "gateway.networking.k8s.io";
    assert_eq!(
        of::<GatewayClass>(),
        at(group, "v1", "gatewayclasses", "GatewayClassList")
    );
    assert_eq!(of::<Gateway>(), at(group, "v1", "gateways", "GatewayList"));
    assert_eq!(
        of::<GRPCRoute>(),
        at(group, "v1", "grpcroutes", "GRPCRouteList")
    );
    assert_eq!(
        of::<HTTPRoute>(),
        at(group, "v1", "httproutes", "HTTPRouteList")
    );
    assert_eq!(
        of::<ListenerSet>(),
        at(group, "v1", "listenersets", "ListenerSetList")
    );
    assert_eq!(
        of::<ReferenceGrant>(),
        at(group, "v1", "referencegrants", "ReferenceGrantList")
    );
    assert_eq!(
        of::<TLSRoute>(),
        at(group, "v1", "tlsroutes", "TLSRouteList")
    );
    assert_eq!(
        of::<TCPRoute>(),
        at(group, "v1", "tcproutes", "TCPRouteList")
    );
    assert_eq!(
        of::<BackendTLSPolicy>(),
        at(group, "v1", "backendtlspolicies", "BackendTLSPolicyList")
    );
    assert_eq!(HTTPRoute::API_VERSION, "gateway.networking.k8s.io/v1");
}
