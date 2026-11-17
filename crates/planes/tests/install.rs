//! What `install/` ships: the CRDs' bound on a name, as the code names Services by it; the
//! five ClusterRoles the operator binds, the admission policies on a control plane's
//! Services, as the code names them, and on generated DataPlanes, as `operator.yaml` names
//! the operator's ServiceAccount; and the operator itself, which grants by binding those
//! five alone.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "a file that cannot be read fails the test"
)]

use edgerush_planes::objects::{
    CONTROL_PLANE_GENERATED_ROLE, CONTROL_PLANE_LABEL, CONTROL_PLANE_NAMESPACE_LABEL,
    DATA_PLANE_LABEL, GATEWAY_CONTROLLER, INGRESS_CONTROLLER, NAMESPACE_LABEL, ROLES,
    controller_name, identity_secret, proxy_name,
};
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

/// The longest name the CRD in `file` admits, by its rule on `metadata.name`.
fn longest_name(file: &str) -> usize {
    read(file)
        .lines()
        .find_map(|line| {
            line.trim()
                .strip_prefix("rule: size(self.metadata.name) <= ")
        })
        .and_then(|rule| rule.split_once(' '))
        .and_then(|(longest, _)| longest.parse().ok())
        .expect("the CRD bounds the name")
}

/// A Service's name is a DNS-1035 label: what each kind's Service is named must fit, for
/// the longest name its CRD admits, and the CRD must admit every name that fits, or a
/// resource is admitted whose Service the API server then refuses (05 §1).
#[test]
fn the_longest_names_admitted_name_services_of_63_characters() {
    let control_plane = longest_name("gateway.edgerush.dev_controlplanes.yaml");
    assert_eq!(
        controller_name(&"a".repeat(control_plane)).len(),
        63,
        "a ControlPlane's controller's Service"
    );
    let data_plane = longest_name("gateway.edgerush.dev_dataplanes.yaml");
    assert_eq!(
        proxy_name(&"a".repeat(data_plane)).len(),
        63,
        "a DataPlane's Service"
    );
}

#[test]
fn the_roles_shipped_are_the_five_the_operator_binds() {
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

/// The policies `admission.yaml` ships, each by its name with the expressions of its
/// match conditions, every one bound to deny.
fn admission_policies() -> Vec<(String, Vec<String>)> {
    let shipped: Vec<Admission> = serde_saphyr::from_multiple(&read("admission.yaml")).unwrap();
    let mut policies = Vec::new();
    let mut bound = Vec::new();
    for object in shipped {
        match object {
            Admission::ValidatingAdmissionPolicy(policy) => {
                let conditions = policy
                    .spec
                    .and_then(|spec| spec.match_conditions)
                    .expect("each policy matches by condition");
                policies.push((
                    policy.metadata.name.expect("a policy is named"),
                    conditions.into_iter().map(|c| c.expression).collect(),
                ));
            }
            Admission::ValidatingAdmissionPolicyBinding(binding) => {
                assert_eq!(binding.spec.validation_actions, ["Deny"]);
                bound.push(binding.spec.policy_name);
            }
        }
    }
    let mut names: Vec<String> = policies.iter().map(|(name, _)| name.clone()).collect();
    names.sort();
    bound.sort();
    assert_eq!(names, bound, "each policy is bound, once");
    policies
}

#[test]
fn the_service_policy_matches_the_control_planes_service_accounts_and_is_bound() {
    let policies = admission_policies();
    let (_, conditions) = policies
        .iter()
        .find(|(name, _)| name == "edgerush-control-plane-services")
        .expect("the policy on a control plane's Services");
    // The control plane's ServiceAccount is named as the policy expects.
    let prefix = controller_name("");
    assert!(
        conditions
            .iter()
            .any(|c| c.contains(&format!(":{prefix}[^:]+$"))),
        "no condition matches {prefix}<controlplane>"
    );
}

#[test]
fn the_generated_data_plane_policy_lets_through_the_operator_alone_and_is_bound() {
    let policies = admission_policies();
    let (_, conditions) = policies
        .iter()
        .find(|(name, _)| name == "edgerush-generated-data-planes")
        .expect("the policy on generated DataPlanes");
    // The operator's ServiceAccount, by its namespace and name as operator.yaml installs
    // it: one of its name in another namespace is let through nothing.
    let shipped: Vec<OperatorInstall> =
        serde_saphyr::from_multiple(&read("operator.yaml")).unwrap();
    let (namespace, name) = shipped
        .iter()
        .find_map(|object| match object {
            OperatorInstall::ServiceAccount(account) => Some((
                account.metadata.namespace.clone().unwrap(),
                account.metadata.name.clone().unwrap(),
            )),
            _ => None,
        })
        .expect("the operator's ServiceAccount");
    let operator = format!("'system:serviceaccount:{namespace}:{name}'");
    assert_eq!(
        conditions,
        &[format!("request.userInfo.username != {operator}")],
        "the policy is to judge everyone but {operator}"
    );
}

#[test]
fn the_policy_beyond_its_namespace_reads_the_control_planes_labels_as_the_code_writes_them() {
    let shipped: Vec<Admission> = serde_saphyr::from_multiple(&read("admission.yaml")).unwrap();
    let spec = shipped
        .into_iter()
        .find_map(|object| match object {
            Admission::ValidatingAdmissionPolicy(policy)
                if policy.metadata.name.as_deref()
                    == Some("edgerush-control-plane-beyond-its-namespace") =>
            {
                policy.spec
            }
            _ => None,
        })
        .expect("the policy on what a control plane writes beyond its namespace");
    // Services and Secrets, made, changed and deleted.
    let rules = spec
        .match_constraints
        .and_then(|constraints| constraints.resource_rules)
        .unwrap();
    let [rule] = rules.as_slice() else {
        panic!("one rule: {rules:?}");
    };
    assert_eq!(
        rule.resources.as_deref(),
        Some(&["services".to_owned(), "secrets".to_owned()][..])
    );
    assert_eq!(
        rule.operations.as_deref(),
        Some(
            &[
                "CREATE".to_owned(),
                "UPDATE".to_owned(),
                "DELETE".to_owned()
            ][..]
        )
    );
    // The control plane's ServiceAccount, as the operator names it, outside its namespace.
    let prefix = controller_name("");
    let conditions: Vec<String> = spec
        .match_conditions
        .unwrap()
        .into_iter()
        .map(|condition| condition.expression)
        .collect();
    assert!(
        conditions
            .iter()
            .any(|c| c.contains(&format!(":{prefix}[^:]+$"))),
        "no condition matches {prefix}<controlplane>"
    );
    assert!(
        conditions
            .iter()
            .any(|c| c == "request.namespace != request.userInfo.username.split(':')[2]"),
        "no condition leaves its own namespace out: {conditions:?}"
    );
    let variables: Vec<(String, String)> = spec
        .variables
        .unwrap()
        .into_iter()
        .map(|variable| (variable.name, variable.expression))
        .collect();
    let variable = |name: &str| {
        variables
            .iter()
            .find(|(each, _)| each == name)
            .map(|(_, expression)| expression.clone())
            .unwrap_or_else(|| panic!("no variable {name}"))
    };
    // Its ControlPlane's name is what follows the prefix.
    assert!(
        variable("controlPlane").ends_with(&format!(".substring({})", prefix.len())),
        "{}",
        variable("controlPlane")
    );
    // Both labels, on the object and on the one it replaces, as the code writes them.
    for name in ["newIsOurs", "oldIsOurs"] {
        let expression = variable(name);
        for label in [CONTROL_PLANE_LABEL, CONTROL_PLANE_NAMESPACE_LABEL] {
            assert!(
                expression.contains(&format!("'{label}' in ")),
                "{name} does not check {label}: {expression}"
            );
        }
    }
    // And shaped as the code writes them: a data plane's identity Secret, its Service
    // selecting its pods by the labels the operator gives them.
    let shaped = variable("shapedAsItsOwn");
    let identity = identity_secret("");
    let (prefix, suffix) = identity.split_at(proxy_name("").len());
    for written in [
        format!("startsWith('{prefix}')"),
        format!("endsWith('{suffix}')"),
        "'kubernetes.io/tls'".to_owned(),
        "'ExternalName'".to_owned(),
        format!(
            "'{}' + object.spec.selector['{DATA_PLANE_LABEL}']",
            proxy_name("")
        ),
        format!("object.spec.selector['{CONTROL_PLANE_LABEL}'] == variables.controlPlane"),
        format!("object.spec.selector['{NAMESPACE_LABEL}'] == request.namespace"),
    ] {
        assert!(
            shaped.contains(&written),
            "shapedAsItsOwn does not hold {written}: {shaped}"
        );
    }
    let validations: Vec<String> = spec
        .validations
        .unwrap()
        .into_iter()
        .map(|validation| validation.expression)
        .collect();
    assert_eq!(
        validations,
        [
            "variables.newIsOurs && variables.oldIsOurs",
            "variables.shapedAsItsOwn"
        ]
    );
}

#[test]
fn the_generated_role_writes_services_and_secrets_alone() {
    // What the control plane writes for a DataPlane generated beyond its namespace, and
    // nothing it reads there, which edgerush-control-plane holds over the watch scope.
    let roles: Vec<ClusterRole> = serde_saphyr::from_multiple(&read("roles.yaml")).unwrap();
    let role = roles
        .iter()
        .find(|role| role.metadata.name.as_deref() == Some(CONTROL_PLANE_GENERATED_ROLE))
        .expect("the role");
    let rules: Vec<(Vec<String>, Vec<String>, Vec<String>)> = role
        .rules
        .iter()
        .flatten()
        .map(|rule| {
            (
                rule.api_groups.clone().unwrap_or_default(),
                rule.resources.clone().unwrap_or_default(),
                rule.verbs.clone(),
            )
        })
        .collect();
    let owned = |items: &[&str]| items.iter().map(|&item| item.to_owned()).collect();
    assert_eq!(
        rules,
        [
            (
                owned(&[""]),
                owned(&["services"]),
                owned(&["create", "patch"])
            ),
            (
                owned(&[""]),
                owned(&["secrets"]),
                owned(&["create", "update"])
            ),
        ]
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
fn the_operator_binds_the_five_roles_alone_and_reads_no_secret() {
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
fn the_operator_writes_the_status_of_gateway_classes_and_gateways_by_patch_alone() {
    // A class naming EdgeRush's controller that no ControlPlane made is refused in its
    // status, and so is a Gateway written alone that sets parameters (05 §3); no other
    // Gateway API status is the operator's.
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
        .filter(|rule| {
            rule.resources
                .iter()
                .flatten()
                .any(|resource| resource.ends_with("/status") && !resource.contains("planes"))
        })
        .collect();
    let mut resources: Vec<&str> = rules
        .iter()
        .flat_map(|rule| {
            assert_eq!(
                rule.api_groups.as_deref(),
                Some(&["gateway.networking.k8s.io".to_owned()][..])
            );
            assert_eq!(rule.verbs, ["patch"]);
            rule.resources.iter().flatten().map(String::as_str)
        })
        .collect();
    resources.sort_unstable();
    assert_eq!(resources, ["gatewayclasses/status", "gateways/status"]);
}

#[test]
fn the_operator_reads_platform_defaults_and_never_writes_them() {
    // What the platform allows is the platform's to write (05 §4).
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
        .filter(|rule| {
            rule.resources
                .iter()
                .flatten()
                .any(|resource| resource.starts_with("platformdefaults"))
        })
        .collect();
    let [read_only] = rules.as_slice() else {
        panic!("one rule holds PlatformDefaults: {rules:?}");
    };
    assert_eq!(
        read_only.resources.as_deref(),
        Some(&["platformdefaults".to_owned()][..])
    );
    assert_eq!(read_only.verbs, ["get", "list", "watch"]);
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
