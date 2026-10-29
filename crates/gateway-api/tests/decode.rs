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
    let rules = route.spec.rules.unwrap();
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
    let rules = route.spec.rules.unwrap();
    assert_eq!(rules[0].backend_refs.as_ref().unwrap()[0].name, "");
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
