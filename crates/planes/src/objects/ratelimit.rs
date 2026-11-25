//! The rate-limiting service a ControlPlane declares (`rateLimiting.service`), the objects
//! the operator makes for it ([05 §2](../../../../docs/05-operator.md),
//! [06 §4](../../../../docs/06-rate-limiting.md)): its ServiceAccount, its config file as a
//! ConfigMap, its Deployment, rolled one pod at a time, its PodDisruptionBudget, and two
//! Services, a headless one listing its pods ready or not, which they find each other
//! through, and a ClusterIP one of its ready pods, which data planes first ask.
//!
//! Every object carries [`rate_limiting_labels`], its ControlPlane's but for its component,
//! so that its pods are none of the controller's, and each is owned by the ControlPlane.
//! The pods' certificate is a Secret the control plane writes, [`ratelimit_identity_secret`],
//! as a data plane's is; until it is there, the pods wait for it.

use std::collections::BTreeMap;

use k8s_openapi::api::apps::v1::{
    Deployment, DeploymentSpec, DeploymentStrategy, RollingUpdateDeployment,
};
use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapVolumeSource, Container, HTTPGetAction, PodSpec, PodTemplateSpec, Probe,
    SecretVolumeSource, Service, ServiceAccount, ServicePort, ServiceSpec, Toleration,
    TopologySpreadConstraint, Volume, VolumeMount,
};
use k8s_openapi::api::policy::v1::{PodDisruptionBudget, PodDisruptionBudgetSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{LabelSelector, ObjectMeta, OwnerReference};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::Serialize;

use super::{
    Error, Identity, Install, Object, from_field, labels, locked_down, metadata, pod_security,
    port, requirements,
};
use crate::control_plane::ControlPlaneRateLimitingService;
use crate::workers::millis;

#[cfg(test)]
mod tests;

/// The component the rate-limiting service's objects are labelled with, where the
/// controller's say `controller`: what tells its pods from the controller's.
pub const RATE_LIMITING_COMPONENT: &str = "ratelimit";

/// The port its pods serve data planes and each other on, and its Services' port.
pub const RATELIMIT_PORT: i32 = 7000;
/// The port its pods show `/metrics` and answer `/ready` on.
const METRICS_PORT: i32 = 9090;

/// The binary its pods run, in the planes' image beside `edgerush` (05 §7).
const BINARY: &str = "edgerush-ratelimit-service";
/// Where its pods mount their config file, and the file's name in its ConfigMap.
const CONFIG_MOUNT: &str = "/etc/edgerush/ratelimit";
const CONFIG_FILE: &str = "service.yaml";
/// Where its pods mount their certificate.
const IDENTITY_MOUNT: &str = "/etc/edgerush/identity";

/// The annotation on its pods holding a digest of their config file, which they read once,
/// as they start: a file changed changes the pods' template, and so rolls them.
pub const CONFIG_DIGEST_ANNOTATION: &str = "gateway.edgerush.dev/config-digest";

/// How long its pods stay on a node not ready or unreachable before they are evicted,
/// where Kubernetes' default is 300: so that a majority lost with its nodes is taken for
/// gone, and formed again, in a minute or two (06 §4).
const NODE_GONE_SECONDS: i64 = 30;

/// The name of ControlPlane `control_plane`'s rate-limiting service: its ServiceAccount,
/// ConfigMap, Deployment, PodDisruptionBudget and the ClusterIP Service data planes first
/// ask (05 §2).
#[must_use]
pub fn ratelimit_name(control_plane: &str) -> String {
    format!("edgerush-ratelimit-{control_plane}")
}

/// The headless Service its pods find each other through, listing them ready or not: their
/// `store.seeds` (05 §2).
#[must_use]
pub fn ratelimit_peers(control_plane: &str) -> String {
    format!("{}-peers", ratelimit_name(control_plane))
}

/// The Secret its pods' certificate is kept in, which the control plane writes and they
/// mount (05 §2).
#[must_use]
pub fn ratelimit_identity_secret(control_plane: &str) -> String {
    format!("{}-identity", ratelimit_name(control_plane))
}

/// What every object of ControlPlane `control_plane`'s rate-limiting service, in
/// `namespace`, is labelled with; its pods' selector is these.
#[must_use]
pub fn rate_limiting_labels(namespace: &str, control_plane: &str) -> BTreeMap<String, String> {
    labels(RATE_LIMITING_COMPONENT, namespace, control_plane, None)
}

/// A service pod's config file, as `edgerush-ratelimit-service --config` reads it
/// (`edgerush_config::ServiceFile`): what the operator writes of it, the rest left to its
/// defaults. No `pod.address`: each pod takes its own `POD_IP` (06 §4).
#[derive(Serialize)]
struct File {
    pod: FilePod,
    store: FileStore,
    #[serde(skip_serializing_if = "Option::is_none")]
    keys: Option<FileKeys>,
    tls: FileTls,
}

#[derive(Serialize)]
struct FilePod {
    listen: String,
    metrics: String,
}

#[derive(Serialize)]
struct FileStore {
    seeds: Vec<String>,
    shards: i32,
    replicas: i32,
    forwarder_decides: bool,
}

#[derive(Serialize)]
struct FileKeys {
    max_bytes: i64,
}

#[derive(Serialize)]
struct FileTls {
    certificate: String,
    key: String,
    authorities: String,
    trust_domain: String,
}

/// FNV-1a, 64 bits, of `bytes`: a digest that is the same from one build to the next, as
/// the standard library's hashers are not promised to be, so that an operator upgraded
/// rolls no pod whose file is unchanged.
fn digest(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    bytes.iter().fold(OFFSET, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(PRIME)
    })
}

/// The cap of each pod's keys: as declared, else half the pods' memory limit, else none,
/// leaving the file's own (06 §8). Half, not more: what copying to the other pods holds
/// beside the keys is not counted by the cap, and a pod paused or replaced has been seen to
/// hold up to about as much again (05 §1).
fn max_key_bytes(service: &ControlPlaneRateLimitingService) -> Result<Option<i64>, Error> {
    if let Some(declared) = service.max_key_bytes {
        return Ok(Some(declared));
    }
    let limit = service
        .resources
        .as_ref()
        .and_then(|resources| resources.limits.as_ref())
        .and_then(|limits| limits.get("memory"));
    let bytes = match limit {
        None => return Ok(None),
        Some(IntOrString::Int(bytes)) => {
            u64::try_from(*bytes).map_err(|_| Error::Memory(bytes.to_string()))?
        }
        Some(IntOrString::String(written)) => millis(written)
            .map(|millis| millis / 1000)
            .ok_or_else(|| Error::Memory(written.clone()))?,
    };
    Ok(Some(i64::try_from(bytes / 2).unwrap_or(i64::MAX).max(1)))
}

/// The config file of `control_plane`'s service, `service` as it declares it, in
/// `namespace`.
fn file(
    control_plane: &str,
    namespace: &str,
    service: &ControlPlaneRateLimitingService,
    install: &Install,
) -> Result<String, Error> {
    let file = File {
        pod: FilePod {
            listen: format!("0.0.0.0:{RATELIMIT_PORT}"),
            metrics: format!("0.0.0.0:{METRICS_PORT}"),
        },
        store: FileStore {
            seeds: vec![format!(
                "{}.{namespace}.svc:{RATELIMIT_PORT}",
                ratelimit_peers(control_plane)
            )],
            shards: service.shards,
            replicas: service.copies,
            forwarder_decides: service.forwarder_decides,
        },
        keys: max_key_bytes(service)?.map(|max_bytes| FileKeys { max_bytes }),
        tls: FileTls {
            certificate: format!("{IDENTITY_MOUNT}/tls.crt"),
            key: format!("{IDENTITY_MOUNT}/tls.key"),
            authorities: format!("{IDENTITY_MOUNT}/ca.crt"),
            // Its ControlPlane's, as every workload's ID is (12 item 22).
            trust_domain: format!("{control_plane}.{}", install.cluster_domain),
        },
    };
    serde_saphyr::to_string(&file).map_err(|error| Error::ServiceFile(error.to_string()))
}

/// A Service of the service's pods, `selector`, on its port.
fn store_service(meta: ObjectMeta, selector: &BTreeMap<String, String>, headless: bool) -> Object {
    Object::Service(Box::new(Service {
        metadata: meta,
        spec: Some(ServiceSpec {
            selector: Some(selector.clone()),
            ports: Some(vec![ServicePort {
                name: Some("store".to_owned()),
                port: RATELIMIT_PORT,
                target_port: Some(IntOrString::String("store".to_owned())),
                ..ServicePort::default()
            }]),
            // Headless, a name for each pod's address; and every pod, ready or not, as a
            // pod is ready only once it serves, which it cannot before it has found the
            // others (05 §2).
            cluster_ip: headless.then(|| "None".to_owned()),
            publish_not_ready_addresses: headless.then_some(true),
            ..ServiceSpec::default()
        }),
        ..Service::default()
    }))
}

/// A toleration of `taint` for [`NODE_GONE_SECONDS`].
fn node_gone(taint: &str) -> Toleration {
    Toleration {
        key: Some(taint.to_owned()),
        operator: Some("Exists".to_owned()),
        effect: Some("NoExecute".to_owned()),
        toleration_seconds: Some(NODE_GONE_SECONDS),
        ..Toleration::default()
    }
}

/// Spread over `topology_key`'s domains, preferred and not required: a zone or a node
/// short of room holds up no rollout.
fn spread(topology_key: &str, selector: &BTreeMap<String, String>) -> TopologySpreadConstraint {
    TopologySpreadConstraint {
        max_skew: 1,
        topology_key: topology_key.to_owned(),
        when_unsatisfiable: "ScheduleAnyway".to_owned(),
        label_selector: Some(LabelSelector {
            match_labels: Some(selector.clone()),
            ..LabelSelector::default()
        }),
        ..TopologySpreadConstraint::default()
    }
}

/// Every object of the rate-limiting service `service` that ControlPlane `cp` declares,
/// owned by it, `owner`.
pub(super) fn objects(
    cp: &Identity<'_>,
    owner: &OwnerReference,
    service: &ControlPlaneRateLimitingService,
    install: &Install,
) -> Result<Vec<Object>, Error> {
    let labels = rate_limiting_labels(cp.namespace, cp.name);
    let name = ratelimit_name(cp.name);
    let meta = |name: String| metadata(name, Some(cp.namespace), &labels, Some(owner));
    let file = file(cp.name, cp.namespace, service, install)?;
    let config_digest = format!("{:016x}", digest(file.as_bytes()));

    let mut objects = vec![
        Object::ServiceAccount(ServiceAccount {
            metadata: meta(name.clone()),
            ..ServiceAccount::default()
        }),
        Object::ConfigMap(ConfigMap {
            metadata: meta(name.clone()),
            data: Some(BTreeMap::from([(CONFIG_FILE.to_owned(), file)])),
            ..ConfigMap::default()
        }),
    ];

    let resources = service.resources.as_ref();
    let container = Container {
        name: "ratelimit".to_owned(),
        image: Some(install.image.clone()),
        command: Some(vec![BINARY.to_owned()]),
        args: Some(vec![
            "--config".to_owned(),
            format!("{CONFIG_MOUNT}/{CONFIG_FILE}"),
        ]),
        // Its own address, as every pod reads one file (06 §4).
        env: Some(vec![from_field("POD_IP", "status.podIP")]),
        ports: Some(vec![
            port("store", RATELIMIT_PORT),
            port("metrics", METRICS_PORT),
        ]),
        // Ready once it serves: out of the data planes' Service until then, and the next
        // pod of a rollout not started before (06 §4).
        readiness_probe: Some(Probe {
            http_get: Some(HTTPGetAction {
                path: Some("/ready".to_owned()),
                port: IntOrString::String("metrics".to_owned()),
                ..HTTPGetAction::default()
            }),
            ..Probe::default()
        }),
        resources: resources.map(|r| requirements(r.limits.as_ref(), r.requests.as_ref())),
        volume_mounts: Some(vec![
            VolumeMount {
                name: "config".to_owned(),
                mount_path: CONFIG_MOUNT.to_owned(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
            VolumeMount {
                name: "identity".to_owned(),
                mount_path: IDENTITY_MOUNT.to_owned(),
                read_only: Some(true),
                ..VolumeMount::default()
            },
        ]),
        security_context: Some(locked_down()),
        ..Container::default()
    };
    objects.push(Object::Deployment(Box::new(Deployment {
        metadata: meta(name.clone()),
        spec: DeploymentSpec {
            replicas: Some(service.replicas),
            selector: LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            },
            // One pod at a time: a new one serves before an old one goes (06 §4).
            strategy: Some(DeploymentStrategy {
                type_: Some("RollingUpdate".to_owned()),
                rolling_update: Some(RollingUpdateDeployment {
                    max_surge: Some(IntOrString::Int(1)),
                    max_unavailable: Some(IntOrString::Int(0)),
                }),
            }),
            template: PodTemplateSpec {
                metadata: Some(ObjectMeta {
                    labels: Some(labels.clone()),
                    annotations: Some(BTreeMap::from([(
                        CONFIG_DIGEST_ANNOTATION.to_owned(),
                        config_digest,
                    )])),
                    ..ObjectMeta::default()
                }),
                spec: Some(PodSpec {
                    service_account_name: Some(name.clone()),
                    // It asks nothing of the API server.
                    automount_service_account_token: Some(false),
                    security_context: Some(pod_security(None)),
                    topology_spread_constraints: Some(vec![
                        spread("topology.kubernetes.io/zone", &labels),
                        spread("kubernetes.io/hostname", &labels),
                    ]),
                    tolerations: Some(vec![
                        node_gone("node.kubernetes.io/not-ready"),
                        node_gone("node.kubernetes.io/unreachable"),
                    ]),
                    containers: vec![container],
                    volumes: Some(vec![
                        Volume {
                            name: "config".to_owned(),
                            config_map: Some(ConfigMapVolumeSource {
                                name: name.clone(),
                                ..ConfigMapVolumeSource::default()
                            }),
                            ..Volume::default()
                        },
                        // Its certificate, which the control plane writes: a pod waits for
                        // it (05 §2).
                        Volume {
                            name: "identity".to_owned(),
                            secret: Some(SecretVolumeSource {
                                secret_name: Some(ratelimit_identity_secret(cp.name)),
                                ..SecretVolumeSource::default()
                            }),
                            ..Volume::default()
                        },
                    ]),
                    ..PodSpec::default()
                }),
            },
            ..DeploymentSpec::default()
        },
        ..Deployment::default()
    })));
    // A node's drain takes one pod at a time, as a rollout does (06 §4).
    objects.push(Object::PodDisruptionBudget(PodDisruptionBudget {
        metadata: meta(name.clone()),
        spec: Some(PodDisruptionBudgetSpec {
            max_unavailable: Some(IntOrString::Int(1)),
            selector: Some(LabelSelector {
                match_labels: Some(labels.clone()),
                ..LabelSelector::default()
            }),
            ..PodDisruptionBudgetSpec::default()
        }),
        ..PodDisruptionBudget::default()
    }));
    objects.push(store_service(meta(ratelimit_peers(cp.name)), &labels, true));
    objects.push(store_service(meta(name), &labels, false));
    Ok(objects)
}
