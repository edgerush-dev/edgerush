//! What `install/` ships beside the CRDs: the four ClusterRoles the operator binds and the
//! admission policy on a control plane's Services, as the code names them.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a file that cannot be read fails the test"
)]

use edgerush_planes::objects::{GATEWAY_CONTROLLER, INGRESS_CONTROLLER, ROLES, controller_name};
use k8s_openapi::api::admissionregistration::v1::{
    ValidatingAdmissionPolicy, ValidatingAdmissionPolicyBinding,
};
use k8s_openapi::api::rbac::v1::ClusterRole;
use serde::Deserialize;

fn read(file: &str) -> String {
    std::fs::read_to_string(format!("{}/install/{file}", env!("CARGO_MANIFEST_DIR")))
        .expect("the file is there")
        .replace("\r\n", "\n")
}

#[test]
fn the_roles_shipped_are_the_four_the_operator_binds() {
    let roles: Vec<ClusterRole> = serde_saphyr::from_multiple(&read("roles.yaml")).unwrap();
    let names: Vec<_> = roles
        .iter()
        .map(|role| role.metadata.name.as_deref().unwrap_or_default())
        .collect();
    assert_eq!(names, ROLES);
}

#[test]
fn only_the_control_plane_cluster_role_reads_cluster_scoped_kinds() {
    // Bound by RoleBindings, a role grants nothing of a cluster-scoped kind; and the one
    // bound by a ClusterRoleBinding whatever the scope must grant nothing namespaced, or it
    // would grant it in every namespace (05 §2).
    let cluster_scoped = [
        "ingressclasses",
        "gatewayclasses",
        "gatewayclasses/status",
        "namespaces",
        "customresourcedefinitions",
    ];
    let roles: Vec<ClusterRole> = serde_saphyr::from_multiple(&read("roles.yaml")).unwrap();
    for role in roles {
        let name = role.metadata.name.unwrap_or_default();
        for rule in role.rules.unwrap_or_default() {
            for resource in rule.resources.unwrap_or_default() {
                let is_cluster_scoped = cluster_scoped.contains(&resource.as_str());
                assert_eq!(
                    is_cluster_scoped,
                    name == "edgerush-control-plane-cluster",
                    "{name} holds {resource}"
                );
            }
        }
    }
}

/// The admission policy and its binding, as `admission.yaml` holds them.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Admission {
    ValidatingAdmissionPolicy(Box<ValidatingAdmissionPolicy>),
    ValidatingAdmissionPolicyBinding(Box<ValidatingAdmissionPolicyBinding>),
}

#[test]
fn the_admission_policy_matches_the_control_planes_service_accounts_and_is_bound() {
    let shipped: Vec<Admission> = serde_saphyr::from_multiple(&read("admission.yaml")).unwrap();
    let [
        Admission::ValidatingAdmissionPolicy(policy),
        Admission::ValidatingAdmissionPolicyBinding(binding),
    ] = shipped.as_slice()
    else {
        panic!("admission.yaml is a policy, then its binding");
    };
    let policy_name = policy.metadata.name.as_deref();
    let spec = &binding.spec;
    assert_eq!(Some(spec.policy_name.as_str()), policy_name);
    assert_eq!(spec.validation_actions, ["Deny"]);
    let conditions = policy
        .spec
        .as_ref()
        .and_then(|spec| spec.match_conditions.as_ref())
        .expect("the policy matches by condition");
    // The control plane's ServiceAccount is named as the policy expects.
    let prefix = controller_name("");
    assert!(
        conditions
            .iter()
            .any(|c| c.expression.contains(&format!(":{prefix}[^:]+$"))),
        "no condition matches {prefix}<controlplane>"
    );
}

#[test]
fn the_classes_name_the_controllers_translation_serves() {
    assert_eq!(INGRESS_CONTROLLER, edgerush_translate::CONTROLLER);
    assert_eq!(GATEWAY_CONTROLLER, edgerush_translate::GATEWAY_CONTROLLER);
}
