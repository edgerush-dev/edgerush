//! How many RateLimitPolicies each data plane leaves out as they cannot be implemented
//! there ([04 §6](../../../docs/04-control-plane.md), [08 §1](../../../docs/08-observability.md)):
//! a policy whose status holds as many ancestors as it can, none of them one the data plane
//! writes, has no room to say why, so the report and this gauge do. Every replica keeps it,
//! of the translations it makes.

use edgerush_telemetry::{Exposition, Kind};
use edgerush_translate::{Problem, Translation};
use std::collections::BTreeMap;

/// The gauge of the policies each data plane leaves out.
pub const GAUGE: &str = "edgerush_rate_limit_policies_unimplementable";

/// By data plane, as the control plane names it, how many policies its last translation
/// left out.
#[derive(Debug, Default)]
pub struct Unimplementable {
    by_plane: BTreeMap<String, usize>,
}

impl Unimplementable {
    /// None translated yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes `translation`, the last of `data_plane`'s.
    pub fn take(&mut self, data_plane: &str, translation: &Translation) {
        let left = translation
            .report
            .iter()
            .filter(|finding| matches!(finding.problem, Problem::LimitPolicyUnimplementable(_)))
            .count();
        self.by_plane.insert(data_plane.to_owned(), left);
    }

    /// The gauge, in the Prometheus text format: a sample for each data plane translated.
    #[must_use]
    pub fn exposition(&self) -> String {
        let mut exposition = Exposition::new();
        exposition.family(
            GAUGE,
            Kind::Gauge,
            "RateLimitPolicies a data plane leaves out, their status holding as many ancestors as it can, none of them its own.",
        );
        for (data_plane, left) in &self.by_plane {
            exposition.sample(GAUGE, &[("data_plane", data_plane)], left);
        }
        exposition.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_translate::{Finding, Object, Plane, translate};

    /// A translation whose report says `left` policies are left out.
    fn leaving(left: usize) -> Translation {
        let plane: Plane = serde_saphyr::from_str(
            "cluster_domain: cluster.local\ningress_hosts_fall_through: false\ningress_https_redirect: true\ningress_https_redirect_max_age: 30\n",
        )
        .unwrap();
        let mut translation = translate(&plane, &edgerush_translate::Cluster::default());
        for n in 0..left {
            translation.report.push(Finding {
                object: Object {
                    kind: "RateLimitPolicy",
                    namespace: "shop".to_owned(),
                    name: format!("full-{n}"),
                },
                problem: Problem::LimitPolicyUnimplementable("full".to_owned()),
            });
        }
        translation
    }

    /// Each data plane's count of its last translation, a data plane leaving none out
    /// counted as none.
    #[test]
    fn each_data_plane_says_how_many_its_last_translation_left_out() {
        let mut gauge = Unimplementable::new();
        gauge.take("edge/web", &leaving(1));
        gauge.take("edge/api", &leaving(0));
        gauge.take("edge/web", &leaving(2));
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
