//! Golden tests: manifests in, the config and report expected out.
//!
//! Each directory under `tests/golden/` is a case: `manifests.yaml`, the objects as
//! `kubectl apply` would take them; `config.yaml`, the config expected, written as the
//! development harness's file is, without the certificates' contents, which a config file
//! does not hold; `report.txt`, the report expected, a finding a line, what was left out
//! first, then the paths whose backend is not there, then what of a `tls` is not served as
//! it lists it, then the paths shadowed; where the case says, `certificates.txt`, the names of the
//! certificates the config carries, a line each (none otherwise); and, where the case says,
//! `passed-over.txt`, how many Ingresses were another data plane's; and, where the case
//! says, `gateway.txt`, what was made of the data plane's Gateway's listeners, a line each
//! (none otherwise: the data plane is no Gateway, or names one not there or not ours).
//! The data plane's settings are `tests/golden/plane.yaml`, unless the case has its own, and
//! the objects of `tests/golden/shared.yaml` go with every case's.
//! Every config translated must also pass the data plane's own `compile`, and, written as
//! YAML as the control plane writes it, read back the same; and the Ingresses it says it
//! served must be those its config's routes are of.

use edgerush_config::{Config, compile};
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
use edgerush_translate::{Cluster, Plane, translate};
use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use serde::Deserialize;
use std::fs;
use std::path::Path;

/// A manifest of a kind translation reads.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Manifest {
    IngressClass(Box<IngressClass>),
    Ingress(Box<Ingress>),
    Service(Box<Service>),
    Secret(Box<Secret>),
    GatewayClass(Box<GatewayClass>),
    Gateway(Box<Gateway>),
    HTTPRoute(Box<HTTPRoute>),
    ReferenceGrant(Box<ReferenceGrant>),
    Namespace(Box<Namespace>),
}

#[test]
fn every_case_translates_to_its_config_and_report() {
    let read = |path: &Path| {
        fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
            .replace("\r\n", "\n")
    };
    let cluster = |manifests: &str| {
        let mut cluster = Cluster::default();
        for manifest in serde_saphyr::from_multiple::<Manifest>(manifests).unwrap() {
            match manifest {
                Manifest::IngressClass(class) => cluster.ingress_classes.push(*class),
                Manifest::Ingress(ingress) => cluster.ingresses.push(*ingress),
                Manifest::Service(service) => cluster.services.push(*service),
                Manifest::Secret(secret) => cluster.secrets.push(*secret),
                Manifest::GatewayClass(class) => cluster.gateway_classes.push(*class),
                Manifest::Gateway(gateway) => cluster.gateways.push(*gateway),
                Manifest::HTTPRoute(route) => cluster.http_routes.push(*route),
                Manifest::ReferenceGrant(grant) => cluster.reference_grants.push(*grant),
                Manifest::Namespace(namespace) => cluster.namespaces.push(*namespace),
            }
        }
        cluster
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let shared: Plane = serde_saphyr::from_str(&read(&root.join("plane.yaml"))).unwrap();
    let mut cases: Vec<_> = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    cases.sort();
    assert!(!cases.is_empty(), "no cases under {}", root.display());
    for case in cases {
        let name = case.file_name().unwrap().to_string_lossy().into_owned();
        let own = case.join("plane.yaml");
        let plane = if own.exists() {
            serde_saphyr::from_str(&read(&own)).unwrap()
        } else {
            shared.clone()
        };
        // Every case has the objects every cluster of the tests has: an IngressClass `edge`
        // that names this controller.
        let manifests = format!(
            "{}---
{}",
            read(&root.join("shared.yaml")),
            read(&case.join("manifests.yaml"))
        );
        let translation = translate(&plane, &cluster(&manifests));
        // How many Ingresses were another data plane's, where the case says.
        let passed_over = case.join("passed-over.txt");
        if passed_over.exists() {
            let expected: usize = read(&passed_over).trim().parse().unwrap();
            assert_eq!(translation.passed_over, expected, "{name}: passed over");
        }
        let expected: Config = serde_saphyr::from_str(&read(&case.join("config.yaml")))
            .unwrap_or_else(|error| panic!("{name}: config.yaml: {error}"));
        let mut config = translation.config.clone();
        let certificates: Vec<String> = std::mem::take(&mut config.certificates)
            .into_keys()
            .collect();
        assert_eq!(config, expected, "{name}: config");
        let written = serde_saphyr::to_string(&config).unwrap();
        let read_back: Config = serde_saphyr::from_str(&written)
            .unwrap_or_else(|error| panic!("{name}: the config written: {error}\n{written}"));
        assert_eq!(read_back, config, "{name}: the config written, read back");
        let listed = case.join("certificates.txt");
        let expected: Vec<String> = if listed.exists() {
            read(&listed).lines().map(str::to_owned).collect()
        } else {
            Vec::new()
        };
        assert_eq!(certificates, expected, "{name}: certificates");
        let report: Vec<String> = translation
            .report
            .iter()
            .map(ToString::to_string)
            .chain(translation.unresolved.iter().map(ToString::to_string))
            .chain(translation.tls.iter().map(ToString::to_string))
            .chain(translation.shadowed.iter().map(ToString::to_string))
            .collect();
        let expected: Vec<String> = read(&case.join("report.txt"))
            .lines()
            .map(str::to_owned)
            .collect();
        assert_eq!(report, expected, "{name}: report");
        let listed = case.join("gateway.txt");
        let listeners: Option<Vec<String>> = translation
            .gateway
            .as_ref()
            .map(|gateway| gateway.listeners.iter().map(ToString::to_string).collect());
        let expected: Option<Vec<String>> = listed
            .exists()
            .then(|| read(&listed).lines().map(str::to_owned).collect());
        assert_eq!(listeners, expected, "{name}: the Gateway's listeners");
        // The Ingresses served are those the config's routes are of, each route named
        // `ingress/<namespace>/<name>/...`.
        let mut of_routes: Vec<String> = translation
            .config
            .routes
            .iter()
            .filter_map(|route| {
                let mut parts = route.name.strip_prefix("ingress/")?.splitn(3, '/');
                Some(format!("{}/{}", parts.next()?, parts.next()?))
            })
            .collect();
        of_routes.sort();
        of_routes.dedup();
        let mut served: Vec<String> = translation
            .served
            .iter()
            .map(|object| {
                assert_eq!(object.kind, "Ingress", "{name}: served");
                format!("{}/{}", object.namespace, object.name)
            })
            .collect();
        served.sort();
        assert_eq!(served, of_routes, "{name}: the Ingresses served");
        // Every Ingress is claimed or passed over, and every one served is claimed.
        let ingresses = serde_saphyr::from_multiple::<Manifest>(&manifests)
            .unwrap()
            .into_iter()
            .filter(|manifest| matches!(manifest, Manifest::Ingress(_)))
            .count();
        assert_eq!(
            translation.claimed.len() + translation.passed_over,
            ingresses,
            "{name}: the Ingresses claimed and passed over"
        );
        for object in &translation.served {
            assert!(
                translation.claimed.contains(object),
                "{name}: {object} served, not claimed"
            );
        }
        if let Err(problems) = compile(&translation.config) {
            let problems: Vec<String> = problems.iter().map(ToString::to_string).collect();
            panic!("{name}: the data plane refuses its config: {problems:?}");
        }
    }
}
