//! How many Services each data plane serves unprotected as their backend protection
//! annotations do not read ([25 §2](../../../docs/25-backend-protection.md),
//! [08 §1](../../../docs/08-observability.md)): a typo that leaves a backend unguarded is
//! said in the report, and this gauge is what an alert watches. Every replica keeps it, of
//! the translations it makes.

use edgerush_telemetry::{Exposition, Kind};
use edgerush_translate::{Problem, Translation};
use std::collections::BTreeMap;

/// The gauge of the Services each data plane serves unprotected.
pub const GAUGE: &str = "edgerush_services_unprotected";

/// By data plane, as the control plane names it, how many Services its last translation
/// serves unprotected.
#[derive(Debug, Default)]
pub struct Unprotected {
    by_plane: BTreeMap<String, usize>,
}

impl Unprotected {
    /// None translated yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes `translation`, the last of `data_plane`'s.
    pub fn take(&mut self, data_plane: &str, translation: &Translation) {
        let unprotected = translation
            .report
            .iter()
            .filter(|finding| matches!(finding.problem, Problem::Unprotected(_)))
            .count();
        self.by_plane.insert(data_plane.to_owned(), unprotected);
    }

    /// The gauge, in the Prometheus text format: a sample for each data plane translated.
    #[must_use]
    pub fn exposition(&self) -> String {
        let mut exposition = Exposition::new();
        exposition.family(
            GAUGE,
            Kind::Gauge,
            "Services a data plane's HTTP and gRPC routes reach that it serves unprotected, their backend protection annotations not read.",
        );
        for (data_plane, unprotected) in &self.by_plane {
            exposition.sample(GAUGE, &[("data_plane", data_plane)], unprotected);
        }
        exposition.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_translate::{Cluster, Plane, translate};
    use k8s_openapi::api::core::v1::Service;
    use k8s_openapi::api::networking::v1::{Ingress, IngressClass};

    /// A translation of an Ingress reaching `unread` Services whose annotations do not
    /// read, and one whose do.
    fn reaching(unread: usize) -> Translation {
        let plane: Plane = serde_saphyr::from_str(
            "ingress_class: edge\ncluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\n",
        )
        .unwrap();
        let mut cluster = Cluster::default();
        cluster.ingress_classes.push(
            serde_saphyr::from_str::<IngressClass>(
                "metadata: { name: edge }\nspec: { controller: edgerush.dev/ingress-controller }",
            )
            .unwrap(),
        );
        let mut paths = String::new();
        for n in 0..=unread {
            let cap = if n == 0 { "'20'" } else { "none" };
            cluster.services.push(
                serde_saphyr::from_str::<Service>(&format!(
                    "metadata: {{ name: s{n}, namespace: shop, annotations: \
                     {{ gateway.edgerush.dev/max-in-flight-per-pod: {cap} }} }}\n\
                     spec: {{ ports: [{{ name: http, port: 80 }}] }}"
                ))
                .unwrap(),
            );
            paths.push_str(&format!(
                "{{ path: /{n}, pathType: Prefix, backend: {{ service: {{ name: s{n}, port: {{ number: 80 }} }} }} }}, "
            ));
        }
        cluster.ingresses.push(
            serde_saphyr::from_str::<Ingress>(&format!(
                "metadata: {{ name: shop, namespace: shop }}\n\
                 spec: {{ ingressClassName: edge, rules: [{{ host: shop.example.com, http: {{ paths: [{paths}] }} }}] }}"
            ))
            .unwrap(),
        );
        translate(&plane, &cluster)
    }

    /// Each data plane's count of its last translation, a data plane serving every Service
    /// as asked counted as none.
    #[test]
    fn each_data_plane_says_how_many_its_last_translation_serves_unprotected() {
        let mut gauge = Unprotected::new();
        gauge.take("edge/web", &reaching(1));
        gauge.take("edge/api", &reaching(0));
        gauge.take("edge/web", &reaching(2));
        let samples: Vec<String> = gauge
            .exposition()
            .lines()
            .filter(|line| !line.starts_with('#'))
            .map(str::to_owned)
            .collect();
        assert_eq!(
            samples,
            [
                format!("{GAUGE}{{data_plane=\"edge/api\"}} 0"),
                format!("{GAUGE}{{data_plane=\"edge/web\"}} 2"),
            ]
        );
    }
}
