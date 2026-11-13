//! What `install/` ships beside the CRDs: the four ClusterRoles the operator binds, the
//! admission policy on a control plane's Services, as the code names them, and the
//! operator itself, which grants by binding those four alone.

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

/// The operator's install, as `operator.yaml` holds it.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum OperatorInstall {
    Namespace(Box<k8s_openapi::api::core::v1::Namespace>),
    ServiceAccount(Box<k8s_openapi::api::core::v1::ServiceAccount>),
    ClusterRole(Box<ClusterRole>),
    ClusterRoleBinding(Box<k8s_openapi::api::rbac::v1::ClusterRoleBinding>),
    Role(Box<k8s_openapi::api::rbac::v1::Role>),
    RoleBinding(Box<k8s_openapi::api::rbac::v1::RoleBinding>),
    Deployment(Box<k8s_openapi::api::apps::v1::Deployment>),
}

#[test]
fn the_operator_binds_the_four_roles_alone_and_reads_no_secret() {
    let shipped: Vec<OperatorInstall> =
        serde_saphyr::from_multiple(&read("operator.yaml")).unwrap();
    let rules: Vec<_> = shipped
        .iter()
        .filter_map(|object| match object {
            OperatorInstall::ClusterRole(role) => role.rules.clone(),
            OperatorInstall::Role(role) => role.rules.clone(),
            _ => None,
        })
        .flatten()
        .collect();
    let binds: Vec<_> = rules
        .iter()
        .filter(|rule| {
            rule.verbs
                .iter()
                .any(|verb| verb == "bind" || verb == "escalate")
        })
        .collect();
    let [bind] = binds.as_slice() else {
        panic!("one rule binds: {binds:?}");
    };
    assert_eq!(bind.verbs, ["bind"]);
    assert_eq!(
        bind.resources.as_deref(),
        Some(&["clusterroles".to_owned()][..])
    );
    assert_eq!(
        bind.resource_names.as_deref(),
        Some(&ROLES.map(str::to_owned)[..])
    );
    // It grants rights by binding alone: it makes no role, and holds none of what the
    // planes read.
    for rule in &rules {
        for resource in rule.resources.iter().flatten() {
            assert!(
                ![
                    "secrets",
                    "roles",
                    "clusterroles",
                    "ingresses",
                    "httproutes",
                    "configmaps"
                ]
                .contains(&resource.as_str())
                    || rule.verbs == ["bind"],
                "the operator holds {:?} on {resource}",
                rule.verbs
            );
            assert!(!resource.contains('*'), "a wildcard: {resource}");
        }
    }
}

#[test]
fn the_operator_reads_the_crds_of_the_two_kinds_it_makes_alone() {
    let shipped: Vec<OperatorInstall> =
        serde_saphyr::from_multiple(&read("operator.yaml")).unwrap();
    let crds: Vec<_> = shipped
        .iter()
        .filter_map(|object| match object {
            OperatorInstall::ClusterRole(role) => role.rules.clone(),
            _ => None,
        })
        .flatten()
        .filter(|rule| {
            rule.resources
                .iter()
                .flatten()
                .any(|resource| resource == "customresourcedefinitions")
        })
        .collect();
    let [crds] = crds.as_slice() else {
        panic!("one rule reads CRDs: {crds:?}");
    };
    assert_eq!(
        crds.resources.as_deref(),
        Some(&["customresourcedefinitions".to_owned()][..])
    );
    assert_eq!(crds.verbs, ["get", "list", "watch"]);
    assert_eq!(
        crds.resource_names.as_deref(),
        Some(
            &[
                "gatewayclasses.gateway.networking.k8s.io".to_owned(),
                "gateways.gateway.networking.k8s.io".to_owned(),
            ][..]
        )
    );
}

#[test]
fn the_operator_runs_as_the_planes_do_on_the_image_it_gives_them() {
    let shipped: Vec<OperatorInstall> =
        serde_saphyr::from_multiple(&read("operator.yaml")).unwrap();
    let deployment = shipped
        .iter()
        .find_map(|object| match object {
            OperatorInstall::Deployment(deployment) => Some(deployment),
            _ => None,
        })
        .expect("a Deployment");
    let pod = deployment
        .spec
        .as_ref()
        .and_then(|spec| spec.template.spec.as_ref())
        .unwrap();
    let container = &pod.containers[0];
    let args = container.args.clone().unwrap();
    assert_eq!(args[0], "operator");
    let image = args
        .iter()
        .position(|arg| arg == "--image")
        .map(|at| args[at + 1].clone());
    assert_eq!(image, container.image, "the planes run another image");
    let security = pod.security_context.as_ref().unwrap();
    assert_eq!(security.run_as_non_root, Some(true));
    let own = container.security_context.as_ref().unwrap();
    assert_eq!(own.read_only_root_filesystem, Some(true));
    assert_eq!(own.allow_privilege_escalation, Some(false));
}

#[test]
fn the_operator_runs_as_its_service_account_which_its_bindings_name() {
    let shipped: Vec<OperatorInstall> =
        serde_saphyr::from_multiple(&read("operator.yaml")).unwrap();
    let mut namespace = None;
    let mut account = None;
    let mut subjects = Vec::new();
    let mut runs_as = None;
    for object in &shipped {
        match object {
            OperatorInstall::Namespace(made) => namespace = made.metadata.name.clone(),
            OperatorInstall::ServiceAccount(made) => {
                account = Some((made.metadata.namespace.clone(), made.metadata.name.clone()));
            }
            OperatorInstall::ClusterRoleBinding(binding) => {
                subjects.extend(binding.subjects.clone().unwrap_or_default());
            }
            OperatorInstall::RoleBinding(binding) => {
                assert_eq!(binding.metadata.namespace, namespace, "its Lease's binding");
                subjects.extend(binding.subjects.clone().unwrap_or_default());
            }
            OperatorInstall::Deployment(deployment) => {
                assert_eq!(deployment.metadata.namespace, namespace);
                runs_as = deployment
                    .spec
                    .as_ref()
                    .and_then(|spec| spec.template.spec.as_ref())
                    .and_then(|pod| pod.service_account_name.clone());
            }
            OperatorInstall::ClusterRole(_) | OperatorInstall::Role(_) => {}
        }
    }
    let (account_namespace, account_name) = account.expect("a ServiceAccount");
    assert_eq!(account_namespace, namespace);
    assert_eq!(runs_as, account_name);
    assert_eq!(subjects.len(), 2);
    for subject in subjects {
        assert_eq!(subject.kind, "ServiceAccount");
        assert_eq!(Some(subject.name), account_name);
        assert_eq!(subject.namespace, namespace);
    }
}
