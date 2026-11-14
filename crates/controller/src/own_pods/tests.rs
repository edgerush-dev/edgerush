#![allow(
    clippy::unwrap_used,
    reason = "tests: an object that cannot be read is a broken test"
)]

use super::*;
use crate::data_plane::{Own, settings};
use edgerush_planes::data_plane::DataPlane;

/// An EndpointSlice of Service `service` (none: unlabelled) holding `endpoints`, YAML each.
fn slice(name: &str, service: Option<&str>, endpoints: &[&str]) -> EndpointSlice {
    let labels = service.map_or_else(String::new, |service| {
        format!(", labels: {{ kubernetes.io/service-name: {service} }}")
    });
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: edge{labels} }}\naddressType: IPv4\nendpoints: [{}]\n",
        endpoints.join(", ")
    ))
    .unwrap()
}

fn named(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|&name| name.to_owned()).collect()
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

#[test]
fn the_services_are_the_control_planes_and_its_data_planes_where_it_has_one() {
    assert_eq!(services("edge", None), named(&["edgerush-controller-edge"]));
    let web: DataPlane = serde_saphyr::from_str(
        "metadata: { name: web, namespace: edge, generation: 1, uid: u1 }\nspec:\n  controlPlaneRef: { name: edge }\n  ingress: { httpPort: 80, httpsPort: 443 }\n",
    )
    .unwrap();
    let own = Own {
        control_plane: "edge".to_owned(),
        cluster_domain: "cluster.local".to_owned(),
        system_authorities: Vec::new(),
    };
    let settings = settings(&web, &own).unwrap();
    assert_eq!(services("edge", Some(&settings)), named(&OURS));
}

/// The follow: the control plane's pods once its namespace is listed, and the data plane's
/// too once the settings name its Service, with no restart.
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
            slice("edgerush-controller-edge-a1", Some("edgerush-controller-edge"), &["{ addresses: [10.0.0.2] }"]),
            slice("edgerush-proxy-web-b2", Some("edgerush-proxy-web"), &["{ addresses: [10.0.1.2] }"]),
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
        } else {
            answer(500, "")
        }
    })
    .await;
    let api = fake.client(&token("controller-own-pods", "secret"), Timing::default());
    let web: DataPlane = serde_saphyr::from_str(
        "metadata: { name: web, namespace: edge, generation: 1, uid: u1 }\nspec:\n  controlPlaneRef: { name: edge }\n  ingress: { httpPort: 80, httpsPort: 443 }\n",
    )
    .unwrap();
    let own = Own {
        control_plane: "edge".to_owned(),
        cluster_domain: "cluster.local".to_owned(),
        system_authorities: Vec::new(),
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (settings, planes) = watch::channel(None);
            let (out, found) = watch::channel::<Arc<[IpAddr]>>(Arc::from([]));
            let _following = tokio::task::spawn_local(follow_own_pods(
                api,
                Scope::Namespace("edge".to_owned()),
                "edge".to_owned(),
                planes,
                out,
                |_| {},
            ));
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
            settings.send_replace(Some(settings_of(&web, &own)));
            assert!(
                until(&found, ips(&["10.0.0.2", "10.0.1.2"])).await,
                "{:?}",
                *found.borrow()
            );
        })
        .await;
}

fn settings_of(data_plane: &DataPlane, own: &Own) -> Settings {
    settings(data_plane, own).unwrap()
}
