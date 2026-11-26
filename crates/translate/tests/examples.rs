//! The worked example of rate limits, `examples/kubernetes/rate-limits`, translated as its
//! README tells it: what the teams apply, with what the operator makes of their DataPlane
//! (`tests/rate-limits-example`), translates with nothing said; with the mistakes beside
//! them, each RateLimitPolicy refused is said with why, and a target not there or
//! forwarding nothing is a note.

use edgerush_config::compile;
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::grpc_route::GRPCRoute;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::listener_set::ListenerSet;
use edgerush_planes::rate_limit_policy::RateLimitPolicy;
use edgerush_translate::{Cluster, Plane, Translation, translate};
use k8s_openapi::api::core::v1::{Namespace, Service};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

/// A manifest of a kind the example holds that translation reads, or another, passed over:
/// the ControlPlane and DataPlane are the operator's to read.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Manifest {
    IngressClass(Box<IngressClass>),
    Ingress(Box<Ingress>),
    Service(Box<Service>),
    GatewayClass(Box<GatewayClass>),
    Gateway(Box<Gateway>),
    ListenerSet(Box<ListenerSet>),
    HTTPRoute(Box<HTTPRoute>),
    GRPCRoute(Box<GRPCRoute>),
    RateLimitPolicy(Box<RateLimitPolicy>),
    Namespace(Box<Namespace>),
    #[serde(other)]
    Other,
}

/// Every finding, a line each, a note marked as one, in the order the control plane logs
/// them.
fn said(translation: &Translation) -> Vec<String> {
    translation
        .report
        .iter()
        .map(|finding| {
            if finding.problem.is_note() {
                format!("note: {finding}")
            } else {
                finding.to_string()
            }
        })
        .chain(translation.unresolved.iter().map(ToString::to_string))
        .chain(translation.tls.iter().map(ToString::to_string))
        .chain(translation.shadowed.iter().map(ToString::to_string))
        .chain(translation.taken.iter().map(ToString::to_string))
        .collect()
}

#[test]
fn the_rate_limits_example_translates_and_its_mistakes_are_said() {
    let read = |path: &Path| {
        fs::read_to_string(path)
            .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
            .replace("\r\n", "\n")
    };
    // The example's translation, of its manifests, what the operator makes, and `more`.
    let translated = |more: &[PathBuf]| -> Translation {
        let example =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/kubernetes/rate-limits");
        let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/rate-limits-example");
        let mut files: Vec<PathBuf> = fs::read_dir(example.join("manifests"))
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect();
        files.sort();
        files.push(fixtures.join("made-by-the-operator.yaml"));
        files.extend(more.iter().map(|relative| example.join(relative)));
        let mut cluster = Cluster::default();
        for file in &files {
            for manifest in serde_saphyr::from_multiple::<Manifest>(&read(file))
                .unwrap_or_else(|error| panic!("{}: {error}", file.display()))
            {
                match manifest {
                    Manifest::IngressClass(class) => cluster.ingress_classes.push(*class),
                    Manifest::Ingress(ingress) => cluster.ingresses.push(*ingress),
                    Manifest::Service(service) => cluster.services.push(*service),
                    Manifest::GatewayClass(class) => cluster.gateway_classes.push(*class),
                    Manifest::Gateway(gateway) => cluster.gateways.push(*gateway),
                    Manifest::ListenerSet(set) => cluster.listener_sets.push(*set),
                    Manifest::HTTPRoute(route) => cluster.http_routes.push(*route),
                    Manifest::GRPCRoute(route) => cluster.grpc_routes.push(*route),
                    Manifest::RateLimitPolicy(policy) => {
                        cluster.rate_limit_policies.push(*policy);
                    }
                    Manifest::Namespace(namespace) => cluster.namespaces.push(*namespace),
                    Manifest::Other => {}
                }
            }
        }
        // The Secrets the listeners name are cert-manager's, made in the cluster.
        cluster.secrets_assumed = true;
        let plane: Plane = serde_saphyr::from_str(&read(&fixtures.join("plane.yaml"))).unwrap();
        translate(&plane, &cluster)
    };
    let clean = translated(&[]);
    assert_eq!(said(&clean), Vec::<String>::new());
    assert_eq!(
        (
            clean.config.routes.len(),
            clean.config.upstreams.len(),
            clean.passed_over
        ),
        (11, 8, 0)
    );
    assert!(compile(&clean.config).is_ok());

    let mistaken = translated(&[PathBuf::from("mistakes.yaml")]);
    assert_eq!(
        said(&mistaken),
        [
            "RateLimitPolicy search/search-bad-rate: the data plane would refuse it: limit `search-suggest-per-ip`: `model.bucket.rate`: `100/sec` is not a rate: a number of at least 1, `/`, and `s`, `min`, `h` or `d`",
            "RateLimitPolicy shop/shop-products: refused: its limit `search-per-ip` has the name of RateLimitPolicy search/search's, which holds it",
            "note: RateLimitPolicy edge/platform-redirects: applies nothing to HTTPRoute edge/to-https section redirect, which forwards nothing: a rule that redirects is under no limits",
            "note: RateLimitPolicy legacy/legacy-billing-backend: applies nothing to Service legacy/billing: it is not there",
            "RateLimitPolicy legacy/legacy-billing-backend: none of its targets is there, so it applies nothing and has no status",
            "note: RateLimitPolicy shop/shop-checkout-typo: applies nothing to HTTPRoute shop/storefront section checkuot: it has no rule `checkuot`",
        ]
    );
    assert_eq!(
        (
            mistaken.config.routes.len(),
            mistaken.config.upstreams.len()
        ),
        (11, 8)
    );
    assert!(compile(&mistaken.config).is_ok());
}
