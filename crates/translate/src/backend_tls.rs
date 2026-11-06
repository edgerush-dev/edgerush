//! BackendTLSPolicies ([04 §2](../../../docs/04-control-plane.md)): which one covers each
//! Service port, and the TLS it asks for there.
//!
//! A policy covers the Services its targets name, a whole Service or one port of it by name.
//! Where two cover the same, the older, then the first by name, takes it, and the other is
//! conflicted there; one that names a port takes that port from one that names the whole
//! Service, both taking what they cover. A policy covers what it takes whether what it asks
//! can be had or not: one that cannot fails its backends' connections closed, never leaving
//! them spoken to in the clear.

use crate::{Cluster, Object, namespace};
use edgerush_config::{UpstreamTls, VerifiedName, is_dns_name, is_host_name, is_uri};
use edgerush_gateway_api::backend_tls_policy::BackendTLSPolicy;
use std::collections::BTreeMap;
use std::collections::btree_map::Entry;

/// The key of a policy's CA certificates in a ConfigMap it names.
const CA_KEY: &str = "ca.crt";

/// The policies of a cluster, each chosen where it covers a Service port.
#[derive(Debug, Default)]
pub(crate) struct Policies<'a> {
    /// By the namespace and name of a Service, and the name of its port (none for the whole
    /// Service), the outcome of the policy that covers it.
    covering: BTreeMap<(&'a str, &'a str, Option<&'a str>), usize>,
    /// What each policy came to, oldest first, then by namespace and name.
    pub(crate) outcomes: Vec<Outcome<'a>>,
}

/// What a policy came to.
#[derive(Debug)]
pub(crate) struct Outcome<'a> {
    /// The policy.
    pub(crate) policy: &'a BackendTLSPolicy,
    /// The TLS it asks for, or why that cannot be had, which fails the connections to what
    /// it covers.
    pub(crate) asked: Result<UpstreamTls, String>,
    /// Why it is invalid as a whole, if it is: what it asks is not something to ask.
    pub(crate) invalid: Option<String>,
    /// Its targets, as each is written, and what each came to.
    pub(crate) targets: Vec<(String, Target)>,
}

/// What a policy's target came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Target {
    /// It covers it.
    Covers,
    /// Another policy, which comes first, covers it.
    Conflicted(Object),
    /// It is not there, or not a Service.
    NotFound(String),
}

impl<'a> Policies<'a> {
    /// The policies of `cluster`, each chosen where it covers.
    pub(crate) fn of(cluster: &'a Cluster) -> Self {
        let mut policies: Vec<&BackendTLSPolicy> = cluster.backend_tls_policies.iter().collect();
        policies.sort_by(|a, b| {
            (
                &a.metadata.creation_timestamp,
                namespace(&a.metadata),
                &a.metadata.name,
            )
                .cmp(&(
                    &b.metadata.creation_timestamp,
                    namespace(&b.metadata),
                    &b.metadata.name,
                ))
        });
        let mut chosen = Self::default();
        for policy in policies {
            let at = chosen.outcomes.len();
            let in_namespace = namespace(&policy.metadata);
            let mut targets = Vec::new();
            for target in &policy.spec.target_refs {
                let section = target.section_name.as_deref();
                let written = match section {
                    Some(section) => {
                        format!(
                            "{} {in_namespace}/{} port {section}",
                            target.kind, target.name
                        )
                    }
                    None => format!("{} {in_namespace}/{}", target.kind, target.name),
                };
                let came_to = match covered(cluster, in_namespace, target) {
                    Err(why) => Target::NotFound(why),
                    Ok(()) => {
                        match chosen
                            .covering
                            .entry((in_namespace, target.name.as_str(), section))
                        {
                            Entry::Vacant(free) => {
                                free.insert(at);
                                Target::Covers
                            }
                            Entry::Occupied(taken) => Target::Conflicted(Object::of(
                                "BackendTLSPolicy",
                                &chosen.outcomes[*taken.get()].policy.metadata,
                            )),
                        }
                    }
                };
                targets.push((written, came_to));
            }
            let (asked, invalid) = asked(cluster, policy);
            chosen.outcomes.push(Outcome {
                policy,
                asked,
                invalid,
                targets,
            });
        }
        chosen
    }

    /// The policy that covers the port of the Service `service` of `namespace` named `port`
    /// (none, for a port of no name), if one does: one that names the port, or else one
    /// that names the whole Service.
    pub(crate) fn covering(
        &self,
        namespace: &str,
        service: &str,
        port: Option<&str>,
    ) -> Option<&Outcome<'a>> {
        let sectioned = port.and_then(|port| self.covering.get(&(namespace, service, Some(port))));
        sectioned
            .or_else(|| self.covering.get(&(namespace, service, None)))
            .map(|&at| &self.outcomes[at])
    }
}

/// Whether `target`, a target of a policy of `namespace`, is a Service there, and a port of
/// it if it names one; or why not.
fn covered(
    cluster: &Cluster,
    namespace: &str,
    target: &edgerush_gateway_api::backend_tls_policy::BackendTLSPolicyTargetRefs,
) -> Result<(), String> {
    if !(target.group.is_empty() && target.kind == "Service") {
        return Err(format!(
            "it is of group {:?} and kind {}, where a policy covers core Services alone",
            target.group, target.kind
        ));
    }
    let Some(service) = cluster.services.iter().find(|service| {
        service.metadata.name.as_deref() == Some(target.name.as_str())
            && crate::namespace(&service.metadata) == namespace
    }) else {
        return Err("it does not exist".to_owned());
    };
    let Some(section) = &target.section_name else {
        return Ok(());
    };
    let ports = service
        .spec
        .as_ref()
        .and_then(|spec| spec.ports.as_deref())
        .unwrap_or_default();
    if ports.iter().any(|port| port.name.as_ref() == Some(section)) {
        Ok(())
    } else {
        Err("the Service has no port of that name".to_owned())
    }
}

/// The TLS `policy` asks for, or why it cannot be had; and why it is invalid, if it is.
fn asked(
    cluster: &Cluster,
    policy: &BackendTLSPolicy,
) -> (Result<UpstreamTls, String>, Option<String>) {
    let validation = &policy.spec.validation;
    let in_namespace = namespace(&policy.metadata);
    let refs = validation
        .ca_certificate_refs
        .as_deref()
        .unwrap_or_default();
    let mut unresolved = Vec::new();
    let mut authorities = Vec::new();
    for reference in refs {
        match authority(cluster, in_namespace, reference) {
            Ok(pem) => authorities.push(pem),
            Err(problem) => unresolved.push(problem),
        }
    }
    let invalid = invalid(policy, refs.is_empty());
    let asked = if let Some(why) = &invalid {
        Err(why.clone())
    } else if !unresolved.is_empty() {
        Err(unresolved.join("; "))
    } else if validation.well_known_ca_certificates.is_some() {
        Err("the system's CA certificates are not supported yet".to_owned())
    } else {
        Ok(UpstreamTls {
            server_name: validation.hostname.clone(),
            verified_names: verified_names(policy),
            authorities,
            well_known_authorities: None,
            client_certificate: None,
        })
    };
    (asked, invalid)
}

/// The CA certificates, in PEM, of `reference`, a CA certificate ref of a policy of
/// `namespace`, read as the data plane will read them; or why they cannot be used.
fn authority(
    cluster: &Cluster,
    namespace: &str,
    reference: &edgerush_gateway_api::backend_tls_policy::BackendTLSPolicyValidationCaCertificateRefs,
) -> Result<String, String> {
    if !(reference.group.is_empty() && reference.kind == "ConfigMap") {
        let kind = if reference.group.is_empty() {
            reference.kind.clone()
        } else {
            format!("{}/{}", reference.group, reference.kind)
        };
        return Err(format!(
            "a CA certificate ref to a {kind}, where a ConfigMap is read"
        ));
    }
    let name = &reference.name;
    let Some(config_map) = cluster.config_maps.iter().find(|config_map| {
        config_map.metadata.name.as_deref() == Some(name.as_str())
            && crate::namespace(&config_map.metadata) == namespace
    }) else {
        return Err(format!("ConfigMap {namespace}/{name} does not exist"));
    };
    let Some(pem) = config_map.data.as_ref().and_then(|data| data.get(CA_KEY)) else {
        return Err(format!("ConfigMap {namespace}/{name} has no {CA_KEY}"));
    };
    // BoringSSL's own reason is not said: it names its parser's state, not the ConfigMap's.
    edgerush_certificates::authorities(std::slice::from_ref(pem))
        .map_err(|_| format!("ConfigMap {namespace}/{name}'s {CA_KEY} is not PEM certificates"))?;
    Ok(pem.clone())
}

/// Why `policy` is invalid, if it is: CA certificates both named and well known, or
/// neither; well-known ones of a name the spec does not have; a hostname that is not a
/// host name; or a subject alternative name that is not one of its type.
fn invalid(policy: &BackendTLSPolicy, no_refs: bool) -> Option<String> {
    let validation = &policy.spec.validation;
    match (&validation.well_known_ca_certificates, no_refs) {
        (Some(_), false) => {
            return Some(
                "it names CA certificates and well-known ones both, where it takes one of the two"
                    .to_owned(),
            );
        }
        (None, true) => return Some("it names no CA certificates".to_owned()),
        (Some(name), true) if name != "System" => {
            return Some(format!(
                "wellKnownCACertificates {name} is not one the gateway knows"
            ));
        }
        _ => {}
    }
    if !is_host_name(&validation.hostname) {
        return Some(format!(
            "hostname {} is not a host name",
            validation.hostname
        ));
    }
    for name in validation.subject_alt_names.iter().flatten() {
        let fit = match name.r#type.as_str() {
            "Hostname" => name.hostname.as_deref().is_some_and(is_dns_name),
            "URI" => name.uri.as_deref().is_some_and(is_uri),
            other => {
                return Some(format!(
                    "a subject alternative name of type {other}, where it is Hostname or URI"
                ));
            }
        };
        if !fit {
            let written = name
                .hostname
                .as_deref()
                .or(name.uri.as_deref())
                .unwrap_or("");
            return Some(format!(
                "subject alternative name {written:?} is not a {}",
                name.r#type
            ));
        }
    }
    None
}

/// The names `policy` has an endpoint's certificate checked for in place of its hostname:
/// its subject alternative names, each of its type; none, and the hostname is checked.
fn verified_names(policy: &BackendTLSPolicy) -> Vec<VerifiedName> {
    policy
        .spec
        .validation
        .subject_alt_names
        .iter()
        .flatten()
        .filter_map(|name| match name.r#type.as_str() {
            "Hostname" => name.hostname.clone().map(VerifiedName::Dns),
            "URI" => name.uri.clone().map(VerifiedName::Uri),
            _ => None,
        })
        .collect()
}
