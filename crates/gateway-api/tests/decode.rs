//! What the types read: an object of the experimental channel, with a value and fields the
//! standard channel lacks, and one without a field its schema requires.

use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
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
        of::<HTTPRoute>(),
        at(group, "v1", "httproutes", "HTTPRouteList")
    );
    assert_eq!(
        of::<ReferenceGrant>(),
        at(group, "v1", "referencegrants", "ReferenceGrantList")
    );
    assert_eq!(HTTPRoute::API_VERSION, "gateway.networking.k8s.io/v1");
}
