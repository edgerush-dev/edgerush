//! `PlatformDefaults`, `gateway.edgerush.dev/v1alpha1`. Written by `edgerush-crdgen` from
//! `install/gateway.edgerush.dev_platformdefaults.yaml`: not to be edited (the crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// PlatformDefaults is what the platform allows the cluster's ControlPlanes and
/// DataPlanes. There is one, named "default"; without it, each ControlPlane may
/// watch its own namespace alone.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct PlatformDefaults {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec is what the platform allows.
    #[serde(default = "default_platform_defaults_spec")]
    pub spec: PlatformDefaultsSpec,
}

/// Spec is what the platform allows.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsSpec {
    /// GatewayClasses is the classes Gateway-first is on for, each with what a
    /// DataPlane generated for a Gateway of it is: a Gateway of a ControlPlane's
    /// class with an entry here, in that ControlPlane's watch scope, gets a DataPlane
    /// in its own namespace, named `<gateway>-<class>`, from the entry. A class with
    /// no entry has none. An entry edited reaches every DataPlane generated from it.
    #[serde(rename = "gatewayClasses")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway_classes: Option<Vec<PlatformDefaultsGatewayClasses>>,
    /// WatchScopes is which namespaces ControlPlanes may watch beyond their own,
    /// each an allowance for the ControlPlanes it picks. A ControlPlane may use any
    /// allowance that picks it; one asking for more than every one of them allows
    /// is not accepted, and nothing is made for it. An allowance narrowed takes back
    /// what it granted.
    #[serde(rename = "watchScopes")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub watch_scopes: Option<Vec<PlatformDefaultsWatchScopes>>,
}

/// GatewayClass is one class's entry.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClasses {
    /// DataPlane is the fields of a DataPlane generated for a Gateway of the
    /// class, as a DataPlane has them, but for controlPlaneRef and gatewayRef,
    /// which come from the class and the Gateway, and gateway and ingress: a
    /// generated DataPlane serves its Gateway alone.
    #[serde(rename = "dataPlane")]
    #[serde(default = "default_platform_defaults_gateway_classes_data_plane")]
    pub data_plane: PlatformDefaultsGatewayClassesDataPlane,
    /// Name is the GatewayClass's name.
    #[serde(default)]
    pub name: String,
}

/// DataPlane is the fields of a DataPlane generated for a Gateway of the
/// class, as a DataPlane has them, but for controlPlaneRef and gatewayRef,
/// which come from the class and the Gateway, and gateway and ingress: a
/// generated DataPlane serves its Gateway alone.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlane {
    /// IPFamily is what each port is bound on, and the families of the data plane's
    /// Service:
    ///
    /// * IPv4: `0.0.0.0`, which binds wherever there is a network.
    /// * IPv6: `[::]`, which takes IPv4 as well wherever the host has it.
    /// * Dual: the same socket, the Service reached over both.
    ///
    /// The schema's values: `IPv4`, `IPv6`, `Dual`. Another is read as it is.
    #[serde(rename = "ipFamily")]
    #[serde(default = "default_platform_defaults_gateway_classes_data_plane_ip_family")]
    pub ip_family: String,
    /// Ports is what Gateway API has no field for, by port: each applied to the
    /// listener bound to that port. A port not here has the control plane's
    /// defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<PlatformDefaultsGatewayClassesDataPlanePorts>>,
    /// Replicas is how many proxies run. With max above min, a
    /// HorizontalPodAutoscaler runs the fleet between the two.
    #[serde(default = "default_platform_defaults_gateway_classes_data_plane_replicas")]
    pub replicas: PlatformDefaultsGatewayClassesDataPlaneReplicas,
    /// Resources is each proxy's compute resources. The proxy runs a request
    /// worker for each CPU requested, rounded up; with a CPU limit, no more than the
    /// limit rounded down, and at least one. A CPU limit is not recommended:
    /// throttling hurts tail latency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<PlatformDefaultsGatewayClassesDataPlaneResources>,
    /// Service is the data plane's Service, its address, which its control plane
    /// writes: its ports are those the data plane binds. It is made with the
    /// DataPlane and lasts as long.
    #[serde(default = "default_platform_defaults_gateway_classes_data_plane_service")]
    pub service: PlatformDefaultsGatewayClassesDataPlaneService,
}

/// PortSettings is one port's.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlanePorts {
    /// AccessLog is where a record of each of its requests, or of its UDP
    /// listener's sessions, is written: nowhere unless said.
    #[serde(rename = "accessLog")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_log: Option<PlatformDefaultsGatewayClassesDataPlanePortsAccessLog>,
    /// Forwarding is whom its requests' forwarding headers are believed from.
    /// Left out, no one's are, and the headers only a trusted proxy may send are
    /// Forwarded, X-Real-IP and X-Forwarded-*.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forwarding: Option<PlatformDefaultsGatewayClassesDataPlanePortsForwarding>,
    /// HTTP3, present, serves HTTP/3 as well, on the same port over UDP: an
    /// HTTPS listener's alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http3: Option<PlatformDefaultsGatewayClassesDataPlanePortsHttp3>,
    /// Port is the port these settings are for.
    #[serde(default)]
    pub port: i32,
    /// ProxyProtocol, present, has every connection start with a PROXY protocol
    /// header, v1 or v2.
    #[serde(rename = "proxyProtocol")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_protocol: Option<PlatformDefaultsGatewayClassesDataPlanePortsProxyProtocol>,
    /// RequestID is what an HTTP listener does with X-Request-ID, Generate unless
    /// said:
    ///
    /// * Generate: every request gets an ID the gateway makes, in place of any it
    ///   came with; the answer tells the client the same ID.
    /// * Pass: the header is left as it is, both ways.
    ///
    /// The schema's values: `Generate`, `Pass`. Another is read as it is.
    #[serde(rename = "requestId")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// SessionAnswers is how many answers a session of the UDP listener on it
    /// expects for each datagram its client sends. Left out, a session ends when
    /// idle alone.
    #[serde(rename = "sessionAnswers")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_answers: Option<i64>,
    /// SessionIdleSeconds is how long a session of the UDP listener on it may
    /// carry nothing before it is ended: the data plane's 60 unless said.
    #[serde(rename = "sessionIdleSeconds")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_idle_seconds: Option<i64>,
    /// TunnelIdleSeconds is how long a tunnel of the TCP or TLS listener on it,
    /// or of an HTTPS one for the connections it passes through, may carry
    /// nothing either way before both its ends are closed: the data plane's
    /// hour unless said.
    #[serde(rename = "tunnelIdleSeconds")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tunnel_idle_seconds: Option<i64>,
}

/// AccessLog is where a record of each of its requests, or of its UDP
/// listener's sessions, is written: nowhere unless said.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlanePortsAccessLog {
    /// Path is the file's, where Type is "File", and must be left out otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Type is where:
    ///
    /// * Stdout: the process's standard output, which the container runtime collects.
    /// * File: a file, appended to; listeners that name the same path share it.
    ///
    /// The schema's values: `Stdout`, `File`. Another is read as it is.
    #[serde(default)]
    pub r#type: String,
}

/// Forwarding is whom its requests' forwarding headers are believed from.
/// Left out, no one's are, and the headers only a trusted proxy may send are
/// Forwarded, X-Real-IP and X-Forwarded-*.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlanePortsForwarding {
    /// TrustedOnlyHeaders are taken off a request that did not come from a
    /// trusted proxy: names, or the front of one followed by `*`
    /// (`X-Forwarded-*`), matched whatever their case.
    #[serde(rename = "trustedOnlyHeaders")]
    #[serde(default)]
    pub trusted_only_headers: Vec<String>,
    /// TrustedProxies are the proxies whose X-Forwarded-For is believed, as
    /// ranges of their addresses: 10.0.4.0/24, 192.0.2.1/32 for one. Empty,
    /// no one's is.
    #[serde(rename = "trustedProxies")]
    #[serde(default)]
    pub trusted_proxies: Vec<String>,
}

/// HTTP3, present, serves HTTP/3 as well, on the same port over UDP: an
/// HTTPS listener's alone.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlanePortsHttp3 {
    /// AltSvcMaxAge is how long, in seconds, a client told over TCP that the
    /// listener serves HTTP/3 may remember it: a day unless said.
    #[serde(rename = "altSvcMaxAge")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub alt_svc_max_age: Option<i64>,
    /// ForceRetry has every client prove its address with a Retry before its
    /// handshake; otherwise only past a threshold of handshakes under way.
    /// Off unless said.
    #[serde(rename = "forceRetry")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_retry: Option<bool>,
}

/// ProxyProtocol, present, has every connection start with a PROXY protocol
/// header, v1 or v2.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlanePortsProxyProtocol {
    /// Senders are the ranges, the load balancer's, whose headers' addresses
    /// are believed (10.0.0.0/16, 192.0.2.1/32 for one). From anywhere else
    /// the header is read and dropped, and whoever connected is the client.
    #[serde(default)]
    pub senders: Vec<String>,
}

/// Replicas is how many proxies run. With max above min, a
/// HorizontalPodAutoscaler runs the fleet between the two.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlaneReplicas {
    /// Max is the most an autoscaler may run. Left out, min run, always.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i32>,
    /// Min is how many run, or the fewest an autoscaler may.
    #[serde(default = "default_platform_defaults_gateway_classes_data_plane_replicas_min")]
    pub min: i32,
}

/// Resources is each proxy's compute resources. The proxy runs a request
/// worker for each CPU requested, rounded up; with a CPU limit, no more than the
/// limit rounded down, and at least one. A CPU limit is not recommended:
/// throttling hurts tail latency.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlaneResources {
    /// Limits is the most of each resource a proxy may use.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limits: Option<BTreeMap<String, IntOrString>>,
    /// Requests is how much of each resource a proxy is given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requests: Option<BTreeMap<String, IntOrString>>,
}

/// Service is the data plane's Service, its address, which its control plane
/// writes: its ports are those the data plane binds. It is made with the
/// DataPlane and lasts as long.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsGatewayClassesDataPlaneService {
    /// Annotations are the Service's, such as a cloud load balancer's settings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub annotations: Option<BTreeMap<String, String>>,
    /// Labels are the Service's, beside those of EdgeRush's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub labels: Option<BTreeMap<String, String>>,
    /// LoadBalancerClass is the Service's: a LoadBalancer's alone, and fixed while
    /// it is one, as Kubernetes has it.
    #[serde(rename = "loadBalancerClass")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_balancer_class: Option<String>,
    /// LoadBalancerSourceRanges are the ranges a LoadBalancer's clients may come
    /// from, where its cloud honours them.
    #[serde(rename = "loadBalancerSourceRanges")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load_balancer_source_ranges: Option<Vec<String>>,
    /// Type is the Service's.
    ///
    /// The schema's values: `ClusterIP`, `NodePort`, `LoadBalancer`. Another is read as it is.
    #[serde(default = "default_platform_defaults_gateway_classes_data_plane_service_type")]
    pub r#type: String,
}

/// WatchScope is one allowance.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopes {
    /// ControlPlanes is which ControlPlanes the allowance is for.
    #[serde(rename = "controlPlanes")]
    #[serde(default)]
    pub control_planes: PlatformDefaultsWatchScopesControlPlanes,
    /// Namespaces is which namespaces they may watch.
    #[serde(default)]
    pub namespaces: PlatformDefaultsWatchScopesNamespaces,
}

/// ControlPlanes is which ControlPlanes the allowance is for.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesControlPlanes {
    /// NamespaceSelector picks the ControlPlanes of the namespaces whose
    /// labels it matches. Every namespace carries its name in the label
    /// kubernetes.io/metadata.name, so a list of namespaces is a
    /// selector of that label. It must have at least one requirement.
    #[serde(rename = "namespaceSelector")]
    #[serde(default)]
    pub namespace_selector: PlatformDefaultsWatchScopesControlPlanesNamespaceSelector,
}

/// NamespaceSelector picks the ControlPlanes of the namespaces whose
/// labels it matches. Every namespace carries its name in the label
/// kubernetes.io/metadata.name, so a list of namespaces is a
/// selector of that label. It must have at least one requirement.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesControlPlanesNamespaceSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions:
        Option<Vec<PlatformDefaultsWatchScopesControlPlanesNamespaceSelectorMatchExpressions>>,
    /// matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels
    /// map is equivalent to an element of matchExpressions, whose key field is "key", the
    /// operator is "In", and the values array contains only "value". The requirements are ANDed.
    #[serde(rename = "matchLabels")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<BTreeMap<String, String>>,
}

/// A label selector requirement is a selector that contains values, a key, and an operator that
/// relates the key and values.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesControlPlanesNamespaceSelectorMatchExpressions {
    /// key is the label key that the selector applies to.
    #[serde(default)]
    pub key: String,
    /// operator represents a key's relationship to a set of values.
    /// Valid operators are In, NotIn, Exists and DoesNotExist.
    #[serde(default)]
    pub operator: String,
    /// values is an array of string values. If the operator is In or NotIn,
    /// the values array must be non-empty. If the operator is Exists or DoesNotExist,
    /// the values array must be empty. This array is replaced during a strategic
    /// merge patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// Namespaces is which namespaces they may watch.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesNamespaces {
    /// From is where the namespaces they may watch are:
    ///
    /// * All: every namespace in the cluster, so any watch scope.
    /// * Selector: those whose labels the selector matches, so a
    ///   Selector scope whose selector carries every requirement of
    ///   this one's, and so selects no namespace beyond it.
    ///
    /// The schema's values: `All`, `Selector`. Another is read as it is.
    #[serde(default)]
    pub from: String,
    /// Selector is the namespaces they may watch where From is "Selector",
    /// and must be left out otherwise. It must have at least one requirement:
    /// every namespace is From "All".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<PlatformDefaultsWatchScopesNamespacesSelector>,
}

/// Selector is the namespaces they may watch where From is "Selector",
/// and must be left out otherwise. It must have at least one requirement:
/// every namespace is From "All".
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesNamespacesSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions:
        Option<Vec<PlatformDefaultsWatchScopesNamespacesSelectorMatchExpressions>>,
    /// matchLabels is a map of {key,value} pairs. A single {key,value} in the matchLabels
    /// map is equivalent to an element of matchExpressions, whose key field is "key", the
    /// operator is "In", and the values array contains only "value". The requirements are ANDed.
    #[serde(rename = "matchLabels")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_labels: Option<BTreeMap<String, String>>,
}

/// A label selector requirement is a selector that contains values, a key, and an operator that
/// relates the key and values.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct PlatformDefaultsWatchScopesNamespacesSelectorMatchExpressions {
    /// key is the label key that the selector applies to.
    #[serde(default)]
    pub key: String,
    /// operator represents a key's relationship to a set of values.
    /// Valid operators are In, NotIn, Exists and DoesNotExist.
    #[serde(default)]
    pub operator: String,
    /// values is an array of string values. If the operator is In or NotIn,
    /// the values array must be non-empty. If the operator is Exists or DoesNotExist,
    /// the values array must be empty. This array is replaced during a strategic
    /// merge patch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub values: Option<Vec<String>>,
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_spec() -> PlatformDefaultsSpec {
    PlatformDefaultsSpec { ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_gateway_classes_data_plane() -> PlatformDefaultsGatewayClassesDataPlane {
    PlatformDefaultsGatewayClassesDataPlane { ip_family: default_platform_defaults_gateway_classes_data_plane_ip_family(), replicas: default_platform_defaults_gateway_classes_data_plane_replicas(), service: default_platform_defaults_gateway_classes_data_plane_service(), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_gateway_classes_data_plane_ip_family() -> String {
    String::from("IPv4")
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_gateway_classes_data_plane_replicas() -> PlatformDefaultsGatewayClassesDataPlaneReplicas {
    PlatformDefaultsGatewayClassesDataPlaneReplicas { min: 1, ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_gateway_classes_data_plane_service() -> PlatformDefaultsGatewayClassesDataPlaneService {
    PlatformDefaultsGatewayClassesDataPlaneService { r#type: String::from("LoadBalancer"), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_gateway_classes_data_plane_replicas_min() -> i32 {
    1
}

/// The schema's default.
#[rustfmt::skip]
fn default_platform_defaults_gateway_classes_data_plane_service_type() -> String {
    String::from("LoadBalancer")
}

impl k8s_openapi::Resource for PlatformDefaults {
    const API_VERSION: &'static str = "gateway.edgerush.dev/v1alpha1";
    const GROUP: &'static str = "gateway.edgerush.dev";
    const KIND: &'static str = "PlatformDefaults";
    const VERSION: &'static str = "v1alpha1";
    const URL_PATH_SEGMENT: &'static str = "platformdefaults";
    type Scope = k8s_openapi::ClusterResourceScope;
}

impl k8s_openapi::ListableResource for PlatformDefaults {
    const LIST_KIND: &'static str = "PlatformDefaultsList";
}

impl k8s_openapi::Metadata for PlatformDefaults {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
