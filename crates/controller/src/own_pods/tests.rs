#![allow(
    clippy::unwrap_used,
    reason = "tests: an object that cannot be read is a broken test"
)]

use super::*;
use crate::data_plane::{Own, settings};
use edgerush_planes::data_plane::DataPlane;

/// An EndpointSlice of namespace `edge` of Service `service` (none: unlabelled) holding
/// `endpoints`, YAML each.
fn slice(name: &str, service: Option<&str>, endpoints: &[&str]) -> EndpointSlice {
    slice_in("edge", name, service, endpoints)
}

/// [`slice`], of namespace `namespace`.
fn slice_in(
    namespace: &str,
    name: &str,
    service: Option<&str>,
    endpoints: &[&str],
) -> EndpointSlice {
    let labels = service.map_or_else(String::new, |service| {
        format!(", labels: {{ kubernetes.io/service-name: {service} }}")
    });
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: {namespace}{labels} }}\naddressType: IPv4\nendpoints: [{}]\n",
        endpoints.join(", ")
    ))
    .unwrap()
}

fn named(names: &[&str]) -> BTreeSet<Named> {
    names
        .iter()
        .map(|&name| ("edge".to_owned(), name.to_owned()))
        .collect()
}

fn ips(addresses: &[&str]) -> Arc<[IpAddr]> {
    addresses.iter().map(|a| a.parse().unwrap()).collect()
}

const OURS: [&str; 2] = ["edgerush-controller-edge", "edgerush-proxy-web"];

#[test]
fn every_endpoint_of_both_services_is_the_gateways_ready_or_not() {
    let slices = [
        slice(
            "edgerush-controller-edge-a1",
            Some("edgerush-controller-edge"),
            &[
                "{ addresses: [10.0.0.2], conditions: { ready: true } }",
                "{ addresses: [10.0.0.3], conditions: { ready: false } }",
            ],
        ),
        slice(
            "edgerush-proxy-web-b2",
            Some("edgerush-proxy-web"),
            &[
                "{ addresses: [10.0.1.2], conditions: { ready: false, serving: true, terminating: true } }",
                "{ addresses: [10.0.1.3] }",
            ],
        ),
    ];
    assert_eq!(
        addresses(&slices, &named(&OURS)),
        ips(&["10.0.0.2", "10.0.0.3", "10.0.1.2", "10.0.1.3"])
    );
}

#[test]
fn another_services_endpoints_and_an_unlabelled_slice_are_not_the_gateways() {
    let slices = [
        slice(
            "web-c3",
            Some("web"),
            &["{ addresses: [10.0.2.2], conditions: { ready: true } }"],
        ),
        slice("loose", None, &["{ addresses: [10.0.2.3] }"]),
        slice(
            "edgerush-proxy-web-b2",
            Some("edgerush-proxy-web"),
            &["{ addresses: [10.0.1.2] }"],
        ),
    ];
    assert_eq!(addresses(&slices, &named(&OURS)), ips(&["10.0.1.2"]));
}

/// A Service of a data plane's name in another namespace is not its: only the one of its
/// DataPlane's namespace is.
#[test]
fn a_service_of_the_same_name_in_another_namespace_is_not_the_gateways() {
    let slices = [
        slice_in(
            "blog",
            "edgerush-proxy-web-b2",
            Some("edgerush-proxy-web"),
            &["{ addresses: [10.0.3.2] }"],
        ),
        slice(
            "edgerush-proxy-web-b2",
            Some("edgerush-proxy-web"),
            &["{ addresses: [10.0.1.2] }"],
        ),
    ];
    assert_eq!(addresses(&slices, &named(&OURS)), ips(&["10.0.1.2"]));
}

#[test]
fn an_address_in_several_slices_is_said_once_and_in_order() {
    let slices = [
        slice(
            "edgerush-proxy-web-b2",
            Some("edgerush-proxy-web"),
            &["{ addresses: [10.0.1.9] }", "{ addresses: [10.0.1.2] }"],
        ),
        slice(
            "edgerush-proxy-web-b3",
            Some("edgerush-proxy-web"),
            &["{ addresses: [10.0.1.2] }"],
        ),
        slice(
            "edgerush-proxy-web-v6",
            Some("edgerush-proxy-web"),
            &["{ addresses: [\"fd00::2\"] }"],
        ),
    ];
    assert_eq!(
        addresses(&slices, &named(&OURS)),
        ips(&["10.0.1.2", "10.0.1.9", "fd00::2"])
    );
}

#[test]
fn a_slice_with_no_endpoints_gives_none() {
    let slices = [slice(
        "edgerush-controller-edge-a1",
        Some("edgerush-controller-edge"),
        &[],
    )];
    assert!(addresses(&slices, &named(&OURS)).is_empty());
}

/// DataPlane `name` of ControlPlane `edge`.
fn data_plane(name: &str) -> DataPlane {
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: edge, generation: 1, uid: u-{name} }}\nspec:\n  controlPlaneRef: {{ name: edge }}\n  ingress: {{ httpPort: 80, httpsPort: 443 }}\n",
    ))
    .unwrap()
}

fn own() -> Own {
    Own {
        control_plane: "edge".to_owned(),
        namespace: "edge".to_owned(),
        cluster_domain: "cluster.local".to_owned(),
        system_authorities: Vec::new(),
    }
}

/// The control plane's Service, and every data plane's it serves: each data plane refuses
/// the others' pods as its own (03 §6).
#[test]
fn the_services_are_the_control_planes_and_every_data_planes() {
    assert_eq!(
        services("edge", "edge", &Planes::new()),
        named(&["edgerush-controller-edge"])
    );
    let web = Planes::from([(
        "DataPlane edge/web".to_owned(),
        settings_of(&data_plane("web"), &own()),
    )]);
    assert_eq!(services("edge", "edge", &web), named(&OURS));
    let mut both = web;
    both.insert(
        "DataPlane edge/api".to_owned(),
        settings_of(&data_plane("api"), &own()),
    );
    assert_eq!(
        services("edge", "edge", &both),
        named(&[
            "edgerush-controller-edge",
            "edgerush-proxy-api",
            "edgerush-proxy-web"
        ])
    );
}

/// A DataPlane generated for Gateway `web` of namespace `shop` (05 §3), of ControlPlane
/// `edge` of namespace `edge`.
fn generated() -> DataPlane {
    serde_saphyr::from_str(
        "metadata: { name: web-edge, namespace: shop, generation: 1, uid: u-web-edge }\nspec:\n  controlPlaneRef: { name: edge, namespace: edge }\n  gatewayRef: { name: web }\n",
    )
    .unwrap()
}

/// A generated DataPlane's Service is of its own namespace, the Gateway's.
#[test]
fn a_generated_data_planes_service_is_of_its_gateways_namespace() {
    let planes = Planes::from([(
        "DataPlane shop/web-edge".to_owned(),
        settings_of(&generated(), &own()),
    )]);
    assert_eq!(
        services("edge", "edge", &planes),
        BTreeSet::from([
            ("edge".to_owned(), "edgerush-controller-edge".to_owned()),
            ("shop".to_owned(), "edgerush-proxy-web-edge".to_owned()),
        ])
    );
}

/// The follow: the control plane's pods once its namespace is listed, and the data plane's
/// too once the settings name its Service, with no restart; a generated one's in its own
/// namespace, followed once it is named.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_planes_service_named_later_is_followed_with_no_restart() {
    use edgerush_kubernetes::Timing;
    use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
    use std::time::Duration;

    let slices = serde_json::json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSliceList",
        "metadata": { "resourceVersion": "5" },
        "items": [
            slice("edgerush-controller-edge-a1", Some("edgerush-controller-edge"), &["{ addresses: [10.0.0.2], targetRef: { kind: Pod, name: controller-a } }"]),
            slice("edgerush-proxy-web-b2", Some("edgerush-proxy-web"), &["{ addresses: [10.0.1.2] }"]),
        ],
    })
    .to_string();
    let shop = serde_json::json!({
        "apiVersion": "discovery.k8s.io/v1",
        "kind": "EndpointSliceList",
        "metadata": { "resourceVersion": "5" },
        "items": [
            slice_in("shop", "edgerush-proxy-web-edge-c3", Some("edgerush-proxy-web-edge"), &["{ addresses: [10.0.4.2] }"]),
            slice_in("shop", "edgerush-proxy-web-d4", Some("edgerush-proxy-web"), &["{ addresses: [10.0.4.3] }"]),
        ],
    })
    .to_string();
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            return Answer {
                status: 200,
                chunks: vec![bytes::Bytes::from_static(b"\n")],
                pause: Duration::from_secs(60),
            };
        }
        if path.starts_with("/apis/discovery.k8s.io/v1/namespaces/edge/endpointslices?") {
            answer(200, &slices)
        } else if path.starts_with("/apis/discovery.k8s.io/v1/namespaces/shop/endpointslices?") {
            answer(200, &shop)
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(&token("controller-own-pods", "secret"), Timing::default());
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(Planes::new());
            let (out, found) = watch::channel::<Arc<[IpAddr]>>(Arc::from([]));
            let (replicas_out, mut replicas_found) =
                watch::channel::<Arc<[Replica]>>(Arc::from([]));
            let _following = tokio::task::spawn_local(follow_own_pods(
                api,
                "edge".to_owned(),
                "edge".to_owned(),
                planes,
                out,
                Some(replicas_out),
                |_| {},
            ));
            let replicas = tokio::time::timeout(
                Duration::from_secs(5),
                replicas_found.wait_for(|now| !now.is_empty()),
            )
            .await
            .expect("no replica found")
            .unwrap()
            .clone();
            assert_eq!(
                *replicas,
                [Replica {
                    pod: "controller-a".to_owned(),
                    address: "10.0.0.2".parse().unwrap(),
                    ready: true,
                }]
            );
            let until = |found: &watch::Receiver<Arc<[IpAddr]>>, want: Arc<[IpAddr]>| {
                let mut found = found.clone();
                async move {
                    tokio::time::timeout(Duration::from_secs(5), found.wait_for(|now| *now == want))
                        .await
                        .is_ok()
                }
            };
            assert!(
                until(&found, ips(&["10.0.0.2"])).await,
                "{:?}",
                *found.borrow()
            );
            settings.send_replace(Planes::from([(
                "DataPlane edge/web".to_owned(),
                settings_of(&data_plane("web"), &own()),
            )]));
            assert!(
                until(&found, ips(&["10.0.0.2", "10.0.1.2"])).await,
                "{:?}",
                *found.borrow()
            );
            settings.send_modify(|planes| {
                planes.insert(
                    "DataPlane shop/web-edge".to_owned(),
                    settings_of(&generated(), &own()),
                );
            });
            assert!(
                until(&found, ips(&["10.0.0.2", "10.0.1.2", "10.0.4.2"])).await,
                "{:?}",
                *found.borrow()
            );
        })
        .await;
}

fn settings_of(data_plane: &DataPlane, own: &Own) -> crate::Settings {
    settings(data_plane, own).unwrap()
}

/// The control plane's replicas are the endpoints of its own Service that name their pod,
/// each by that pod, with whether it is ready: one that says nothing of it taken as ready.
/// A data plane's endpoints, and one of another namespace's Service of the same name, are
/// none of them.
#[test]
fn the_replicas_are_the_control_planes_endpoints_by_pod() {
    let slices = [
        slice(
            "edgerush-controller-edge-a1",
            Some("edgerush-controller-edge"),
            &[
                "{ addresses: [10.0.0.3], conditions: { ready: false }, targetRef: { kind: Pod, name: controller-b } }",
                "{ addresses: [10.0.0.2, 10.0.0.9], conditions: { ready: true }, targetRef: { kind: Pod, name: controller-a } }",
                "{ addresses: [10.0.0.4], targetRef: { kind: Pod, name: controller-c } }",
                "{ addresses: [10.0.0.5] }",
                "{ addresses: [10.0.0.6], targetRef: { kind: Node, name: node-a } }",
            ],
        ),
        slice(
            "edgerush-controller-edge-a2",
            Some("edgerush-controller-edge"),
            &[
                "{ addresses: [10.0.0.2], conditions: { ready: true }, targetRef: { kind: Pod, name: controller-a } }",
            ],
        ),
        slice(
            "edgerush-proxy-web-b2",
            Some("edgerush-proxy-web"),
            &["{ addresses: [10.0.1.2], targetRef: { kind: Pod, name: proxy-a } }"],
        ),
        slice_in(
            "shop",
            "edgerush-controller-edge-c3",
            Some("edgerush-controller-edge"),
            &["{ addresses: [10.0.4.2], targetRef: { kind: Pod, name: elsewhere } }"],
        ),
    ];
    let replica = |pod: &str, address: &str, ready: bool| Replica {
        pod: pod.to_owned(),
        address: address.parse().unwrap(),
        ready,
    };
    assert_eq!(
        replicas(&slices, "edge", "edge"),
        [
            replica("controller-a", "10.0.0.2", true),
            replica("controller-b", "10.0.0.3", false),
            replica("controller-c", "10.0.0.4", true),
        ]
    );
}
