//! `DataPlane`, `gateway.edgerush.dev/v1alpha1`. Written by `edgerush-crdgen` from
//! `install/gateway.edgerush.dev_dataplanes.yaml`: not to be edited (the crate's docs say how it is written again).

use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta};
use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// DataPlane is an EdgeRush data plane: the proxies that serve what its ControlPlane
/// translates, behind one address. It serves Ingress, Gateway API or both: with
/// `ingress`, the operator makes its IngressClass, named `<controlplane>.<dataplane>`;
/// with `gateway`, its Gateway, named as it is, of the GatewayClass of its
/// ControlPlane.
///
/// Its name is a DNS label, as it is a label's value on what is made for it.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct DataPlane {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// Spec is what the data plane is to be.
    #[serde(default)]
    pub spec: DataPlaneSpec,
    /// Status is what the operator and the control plane report of it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<DataPlaneStatus>,
}

/// Spec is what the data plane is to be.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneSpec {
    /// ControlPlaneRef names the ControlPlane, in the DataPlane's own namespace, that serves it.
    #[serde(rename = "controlPlaneRef")]
    #[serde(default)]
    pub control_plane_ref: DataPlaneControlPlaneRef,
    /// Gateway, present, has the data plane serve Gateway API as a Gateway the
    /// operator makes, of the DataPlane's name and in its namespace, and keeps as
    /// stated here: an edit made to the Gateway itself is undone. Its fields are the
    /// Gateway's own, as Gateway API defines them. Teams own listeners of their own
    /// through ListenerSets attached to it, where allowedListeners lets them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gateway: Option<DataPlaneGateway>,
    /// Ingress, present, has the data plane serve the Ingresses of its IngressClass,
    /// which the operator makes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingress: Option<DataPlaneIngress>,
    /// IPFamily is what each port is bound on, and the families of the data plane's
    /// Service:
    ///
    /// * IPv4: `0.0.0.0`, which binds wherever there is a network.
    /// * IPv6: `[::]`, which takes IPv4 as well wherever the host has it.
    /// * Dual: the same socket, the Service reached over both.
    ///
    /// The schema's values: `IPv4`, `IPv6`, `Dual`. Another is read as it is.
    #[serde(rename = "ipFamily")]
    #[serde(default = "default_data_plane_spec_ip_family")]
    pub ip_family: String,
    /// Ports is what Gateway API has no field for, by port: each applied to the
    /// listener bound to that port. A port not here has the control plane's
    /// defaults.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ports: Option<Vec<DataPlanePorts>>,
    /// Replicas is how many proxies run. With max above min, a
    /// HorizontalPodAutoscaler runs the fleet between the two.
    #[serde(default = "default_data_plane_spec_replicas")]
    pub replicas: DataPlaneReplicas,
    /// Resources is each proxy's compute resources. The proxy runs a request
    /// worker for each CPU requested, rounded up; with a CPU limit, no more than the
    /// limit rounded down, and at least one. A CPU limit is not recommended:
    /// throttling hurts tail latency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resources: Option<DataPlaneResources>,
    /// Service is the data plane's Service, its address, which its control plane
    /// writes: its ports are those the data plane binds. It is made with the
    /// DataPlane and lasts as long.
    #[serde(default = "default_data_plane_spec_service")]
    pub service: DataPlaneService,
}

/// ControlPlaneRef names the ControlPlane, in the DataPlane's own namespace, that serves it.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneControlPlaneRef {
    /// Name is the ControlPlane's name.
    #[serde(default)]
    pub name: String,
}

/// Gateway, present, has the data plane serve Gateway API as a Gateway the
/// operator makes, of the DataPlane's name and in its namespace, and keeps as
/// stated here: an edit made to the Gateway itself is undone. Its fields are the
/// Gateway's own, as Gateway API defines them. Teams own listeners of their own
/// through ListenerSets attached to it, where allowedListeners lets them.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGateway {
    /// AllowedListeners defines which ListenerSets can be attached to this Gateway.
    /// The default value is to allow no ListenerSets.
    #[serde(rename = "allowedListeners")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allowed_listeners: Option<DataPlaneGatewayAllowedListeners>,
    /// Listeners associated with this Gateway. Listeners define
    /// logical endpoints that are bound on this Gateway's addresses.
    /// At least one Listener MUST be specified.
    ///
    /// ## Distinct Listeners
    ///
    /// Each Listener in a set of Listeners (for example, in a single Gateway)
    /// MUST be _distinct_, in that a traffic flow MUST be able to be assigned to
    /// exactly one listener. (This section uses "set of Listeners" rather than
    /// "Listeners in a single Gateway" because implementations MAY merge configuration
    /// from multiple Gateways onto a single data plane, and these rules _also_
    /// apply in that case).
    ///
    /// Practically, this means that each listener in a set MUST have a unique
    /// combination of Port, Protocol, and, if supported by the protocol, Hostname.
    ///
    /// Some combinations of port, protocol, and TLS settings are considered
    /// Core support and MUST be supported by implementations based on the objects
    /// they support:
    ///
    /// HTTPRoute
    ///
    /// 1. HTTPRoute, Port: 80, Protocol: HTTP
    /// 2. HTTPRoute, Port: 443, Protocol: HTTPS, TLS Mode: Terminate, TLS keypair provided
    ///
    /// TLSRoute
    ///
    /// 1. TLSRoute, Port: 443, Protocol: TLS, TLS Mode: Passthrough
    ///
    /// "Distinct" Listeners have the following property:
    ///
    /// **The implementation can match inbound requests to a single distinct
    /// Listener**.
    ///
    /// When multiple Listeners share values for fields (for
    /// example, two Listeners with the same Port value), the implementation
    /// can match requests to only one of the Listeners using other
    /// Listener fields.
    ///
    /// When multiple listeners have the same value for the Protocol field, then
    /// each of the Listeners with matching Protocol values MUST have different
    /// values for other fields.
    ///
    /// The set of fields that MUST be different for a Listener differs per protocol.
    /// The following rules define the rules for what fields MUST be considered for
    /// Listeners to be distinct with each protocol currently defined in the
    /// Gateway API spec.
    ///
    /// The set of listeners that all share a protocol value MUST have _different_
    /// values for _at least one_ of these fields to be distinct:
    ///
    /// * **HTTP, HTTPS, TLS**: Port, Hostname
    /// * **TCP, UDP**: Port
    ///
    /// One **very** important rule to call out involves what happens when an
    /// implementation:
    ///
    /// * Supports TCP protocol Listeners, as well as HTTP, HTTPS, or TLS protocol
    ///   Listeners, and
    /// * sees HTTP, HTTPS, or TLS protocols with the same `port` as one with TCP
    ///   Protocol.
    ///
    /// In this case all the Listeners that share a port with the
    /// TCP Listener are not distinct and so MUST NOT be accepted.
    ///
    /// If an implementation does not support TCP Protocol Listeners, then the
    /// previous rule does not apply, and the TCP Listeners SHOULD NOT be
    /// accepted.
    ///
    /// Note that the `tls` field is not used for determining if a listener is distinct, because
    /// Listeners that _only_ differ on TLS config will still conflict in all cases.
    ///
    /// ### Listeners that are distinct only by Hostname
    ///
    /// When the Listeners are distinct based only on Hostname, inbound request
    /// hostnames MUST match from the most specific to least specific Hostname
    /// values to choose the correct Listener and its associated set of Routes.
    ///
    /// Exact matches MUST be processed before wildcard matches, and wildcard
    /// matches MUST be processed before fallback (empty Hostname value)
    /// matches. For example, `"foo.example.com"` takes precedence over
    /// `"*.example.com"`, and `"*.example.com"` takes precedence over `""`.
    ///
    /// Additionally, if there are multiple wildcard entries, more specific
    /// wildcard entries must be processed before less specific wildcard entries.
    /// For example, `"*.foo.example.com"` takes precedence over `"*.example.com"`.
    ///
    /// The precise definition here is that the higher the number of dots in the
    /// hostname to the right of the wildcard character, the higher the precedence.
    ///
    /// The wildcard character will match any number of characters _and dots_ to
    /// the left, however, so `"*.example.com"` will match both
    /// `"foo.bar.example.com"` _and_ `"bar.example.com"`.
    ///
    /// ## Handling indistinct Listeners
    ///
    /// If a set of Listeners contains Listeners that are not distinct, then those
    /// Listeners are _Conflicted_, and the implementation MUST set the "Conflicted"
    /// condition in the Listener Status to "True".
    ///
    /// The words "indistinct" and "conflicted" are considered equivalent for the
    /// purpose of this documentation.
    ///
    /// Implementations MAY choose to accept a Gateway with some Conflicted
    /// Listeners only if they only accept the partial Listener set that contains
    /// no Conflicted Listeners.
    ///
    /// Specifically, an implementation MAY accept a partial Listener set subject to
    /// the following rules:
    ///
    /// * The implementation MUST NOT pick one conflicting Listener as the winner.
    ///   ALL indistinct Listeners must not be accepted for processing.
    /// * At least one distinct Listener MUST be present, or else the Gateway effectively
    ///   contains _no_ Listeners, and must be rejected from processing as a whole.
    ///
    /// The implementation MUST set a "ListenersNotValid" condition on the
    /// Gateway Status when the Gateway contains Conflicted Listeners whether or
    /// not they accept the Gateway. That Condition SHOULD clearly
    /// indicate in the Message which Listeners are conflicted, and which are
    /// Accepted. Additionally, the Listener status for those listeners SHOULD
    /// indicate which Listeners are conflicted and not Accepted.
    ///
    /// ## General Listener behavior
    ///
    /// Note that, for all distinct Listeners, requests SHOULD match at most one Listener.
    /// For example, if Listeners are defined for "foo.example.com" and "*.example.com", a
    /// request to "foo.example.com" SHOULD only be routed using routes attached
    /// to the "foo.example.com" Listener (and not the "*.example.com" Listener).
    ///
    /// If traffic to a Gateway does not match any Listener's hostname (or if
    /// the Listener does not specify a hostname and the request does not match
    /// any attached Route), the request MUST be rejected. The specific mechanism
    /// for rejection depends on the protocol: HTTP returns a 404 status code,
    /// while gRPC returns an Unimplemented status code.
    ///
    /// This concept is known as "Listener Isolation", and it is an Extended feature
    /// of Gateway API. Implementations that do not support Listener Isolation MUST
    /// clearly document this, and MUST NOT claim support for the
    /// `GatewayHTTPListenerIsolation` feature.
    ///
    /// Implementations that _do_ support Listener Isolation SHOULD claim support
    /// for the Extended `GatewayHTTPListenerIsolation` feature and pass the associated
    /// conformance tests.
    ///
    /// ## Compatible Listeners
    ///
    /// A Gateway's Listeners are considered _compatible_ if:
    ///
    /// 1. They are distinct.
    /// 2. The implementation can serve them in compliance with the Addresses
    ///    requirement that all Listeners are available on all assigned
    ///    addresses.
    ///
    /// Compatible combinations in Extended support are expected to vary across
    /// implementations. A combination that is compatible for one implementation
    /// may not be compatible for another.
    ///
    /// For example, an implementation that cannot serve both TCP and UDP listeners
    /// on the same address, or cannot mix HTTPS and generic TLS listens on the same port
    /// would not consider those cases compatible, even though they are distinct.
    ///
    /// Implementations MAY merge separate Gateways onto a single set of
    /// Addresses if all Listeners across all Gateways are compatible.
    ///
    /// In a future release the MinItems=1 requirement MAY be dropped.
    ///
    /// Support: Core
    #[serde(default)]
    pub listeners: Vec<DataPlaneGatewayListeners>,
    /// TLS specifies frontend and backend tls configuration for entire gateway.
    ///
    /// Support: Extended
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<DataPlaneGatewayTls>,
}

/// AllowedListeners defines which ListenerSets can be attached to this Gateway.
/// The default value is to allow no ListenerSets.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayAllowedListeners {
    /// Namespaces defines which namespaces ListenerSets can be attached to this Gateway.
    /// The default value is to allow no ListenerSets.
    #[serde(default = "default_data_plane_gateway_allowed_listeners_namespaces")]
    pub namespaces: DataPlaneGatewayAllowedListenersNamespaces,
}

/// Namespaces defines which namespaces ListenerSets can be attached to this Gateway.
/// The default value is to allow no ListenerSets.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayAllowedListenersNamespaces {
    /// From indicates where ListenerSets can attach to this Gateway. Possible
    /// values are:
    ///
    /// * Same: Only ListenerSets in the same namespace may be attached to this Gateway.
    /// * Selector: ListenerSets in namespaces selected by the selector may be attached to this Gateway.
    /// * All: ListenerSets in all namespaces may be attached to this Gateway.
    /// * None: Only listeners defined in the Gateway's spec are allowed
    ///
    /// The default value None
    ///
    /// The schema's values: `All`, `Selector`, `Same`, `None`. Another is read as it is.
    #[serde(default = "default_data_plane_gateway_allowed_listeners_namespaces_from")]
    pub from: String,
    /// Selector must be specified when From is set to "Selector". In that case,
    /// only ListenerSets in Namespaces matching this Selector will be selected by this
    /// Gateway. This field is ignored for other values of "From".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<DataPlaneGatewayAllowedListenersNamespacesSelector>,
}

/// Selector must be specified when From is set to "Selector". In that case,
/// only ListenerSets in Namespaces matching this Selector will be selected by this
/// Gateway. This field is ignored for other values of "From".
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayAllowedListenersNamespacesSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions:
        Option<Vec<DataPlaneGatewayAllowedListenersNamespacesSelectorMatchExpressions>>,
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
pub struct DataPlaneGatewayAllowedListenersNamespacesSelectorMatchExpressions {
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

/// Listener embodies the concept of a logical endpoint where a Gateway accepts
/// network connections.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListeners {
    /// AllowedRoutes defines the types of routes that MAY be attached to a
    /// Listener and the trusted namespaces where those Route resources MAY be
    /// present.
    ///
    /// Although a client request may match multiple route rules, only one rule
    /// may ultimately receive the request. Matching precedence MUST be
    /// determined in order of the following criteria:
    ///
    /// * The most specific match as defined by the Route type.
    /// * The oldest Route based on creation timestamp. For example, a Route with
    ///   a creation timestamp of "2020-09-08 01:02:03" is given precedence over
    ///   a Route with a creation timestamp of "2020-09-08 01:02:04".
    /// * If everything else is equivalent, the Route appearing first in
    ///   alphabetical order (namespace/name) should be given precedence. For
    ///   example, foo/bar is given precedence over foo/baz.
    ///
    /// All valid rules within a Route attached to this Listener should be
    /// implemented. Invalid Route rules can be ignored (sometimes that will mean
    /// the full Route). If a Route rule transitions from valid to invalid,
    /// support for that Route rule should be dropped to ensure consistency. For
    /// example, even if a filter specified by a Route rule is invalid, the rest
    /// of the rules within that Route should still be supported.
    ///
    /// Support: Core
    #[serde(rename = "allowedRoutes")]
    #[serde(default = "default_data_plane_gateway_listeners_allowed_routes")]
    pub allowed_routes: DataPlaneGatewayListenersAllowedRoutes,
    /// Hostname specifies the virtual hostname to match for protocol types that
    /// define this concept. When unspecified, all hostnames are matched. This
    /// field is ignored for protocols that don't require hostname based
    /// matching.
    ///
    /// Implementations MUST apply Hostname matching appropriately for each of
    /// the following protocols:
    ///
    /// * TLS: The Listener Hostname MUST match the SNI.
    /// * HTTP: The Listener Hostname MUST match the Host header of the request.
    /// * HTTPS: The Listener Hostname SHOULD match both the SNI and Host header.
    ///   Note that this does not require the SNI and Host header to be the same.
    ///   The semantics of this are described in more detail below.
    ///
    /// To ensure security, Section 11.1 of RFC-6066 emphasizes that server
    /// implementations that rely on SNI hostname matching MUST also verify
    /// hostnames within the application protocol.
    ///
    /// Section 9.1.2 of RFC-7540 provides a mechanism for servers to reject the
    /// reuse of a connection by responding with the HTTP 421 Misdirected Request
    /// status code. This indicates that the origin server has rejected the
    /// request because it appears to have been misdirected.
    ///
    /// To detect misdirected requests, Gateways SHOULD match the authority of
    /// the requests with all the SNI hostname(s) configured across all the
    /// Gateway Listeners on the same port and protocol:
    ///
    /// * If another Listener has an exact match or more specific wildcard entry,
    ///   the Gateway SHOULD return a 421.
    /// * If the current Listener (selected by SNI matching during ClientHello)
    ///   does not match the Host:
    ///     * If another Listener does match the Host, the Gateway SHOULD return a
    ///       421.
    ///     * If no other Listener matches the Host, the Gateway MUST return a
    ///       404.
    ///
    /// For HTTPRoute and TLSRoute resources, there is an interaction with the
    /// `spec.hostnames` array. When both listener and route specify hostnames,
    /// there MUST be an intersection between the values for a Route to be
    /// accepted. For more information, refer to the Route specific Hostnames
    /// documentation.
    ///
    /// Hostnames that are prefixed with a wildcard label (`*.`) are interpreted
    /// as a suffix match. That means that a match for `*.example.com` would match
    /// both `test.example.com`, and `foo.test.example.com`, but not `example.com`.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,
    /// Name is the name of the Listener. This name MUST be unique within a
    /// Gateway.
    ///
    /// Support: Core
    #[serde(default)]
    pub name: String,
    /// Port is the network port. Multiple listeners may use the
    /// same port, subject to the Listener compatibility rules.
    ///
    /// Support: Core
    #[serde(default)]
    pub port: i32,
    /// Protocol specifies the network protocol this listener expects to receive.
    ///
    /// Support: Core
    #[serde(default)]
    pub protocol: String,
    /// TLS is the TLS configuration for the Listener. This field is required if
    /// the Protocol field is "HTTPS" or "TLS". It is invalid to set this field
    /// if the Protocol field is "HTTP", "TCP", or "UDP".
    ///
    /// The association of SNIs to Certificate defined in ListenerTLSConfig is
    /// defined based on the Hostname field for this listener.
    ///
    /// The GatewayClass MUST use the longest matching SNI out of all
    /// available certificates for any TLS handshake.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tls: Option<DataPlaneGatewayListenersTls>,
}

/// AllowedRoutes defines the types of routes that MAY be attached to a
/// Listener and the trusted namespaces where those Route resources MAY be
/// present.
///
/// Although a client request may match multiple route rules, only one rule
/// may ultimately receive the request. Matching precedence MUST be
/// determined in order of the following criteria:
///
/// * The most specific match as defined by the Route type.
/// * The oldest Route based on creation timestamp. For example, a Route with
///   a creation timestamp of "2020-09-08 01:02:03" is given precedence over
///   a Route with a creation timestamp of "2020-09-08 01:02:04".
/// * If everything else is equivalent, the Route appearing first in
///   alphabetical order (namespace/name) should be given precedence. For
///   example, foo/bar is given precedence over foo/baz.
///
/// All valid rules within a Route attached to this Listener should be
/// implemented. Invalid Route rules can be ignored (sometimes that will mean
/// the full Route). If a Route rule transitions from valid to invalid,
/// support for that Route rule should be dropped to ensure consistency. For
/// example, even if a filter specified by a Route rule is invalid, the rest
/// of the rules within that Route should still be supported.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListenersAllowedRoutes {
    /// Kinds specifies the groups and kinds of Routes that are allowed to bind
    /// to this Gateway Listener. When unspecified or empty, the kinds of Routes
    /// selected are determined using the Listener protocol.
    ///
    /// A RouteGroupKind MUST correspond to kinds of Routes that are compatible
    /// with the application protocol specified in the Listener's Protocol field.
    /// If an implementation does not support or recognize this resource type, it
    /// MUST set the "ResolvedRefs" condition to False for this Listener with the
    /// "InvalidRouteKinds" reason.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kinds: Option<Vec<DataPlaneGatewayListenersAllowedRoutesKinds>>,
    /// Namespaces indicates namespaces from which Routes may be attached to this
    /// Listener. This is restricted to the namespace of this Gateway by default.
    ///
    /// Support: Core
    #[serde(default = "default_data_plane_gateway_listeners_allowed_routes_namespaces")]
    pub namespaces: DataPlaneGatewayListenersAllowedRoutesNamespaces,
}

/// RouteGroupKind indicates the group and kind of a Route resource.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListenersAllowedRoutesKinds {
    /// Group is the group of the Route.
    #[serde(default = "default_data_plane_gateway_listeners_allowed_routes_kinds_group")]
    pub group: String,
    /// Kind is the kind of the Route.
    #[serde(default)]
    pub kind: String,
}

/// Namespaces indicates namespaces from which Routes may be attached to this
/// Listener. This is restricted to the namespace of this Gateway by default.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListenersAllowedRoutesNamespaces {
    /// From indicates where Routes will be selected for this Gateway. Possible
    /// values are:
    ///
    /// * All: Routes in all namespaces may be used by this Gateway.
    /// * Selector: Routes in namespaces selected by the selector may be used by
    ///   this Gateway.
    /// * Same: Only Routes in the same namespace may be used by this Gateway.
    ///
    /// Support: Core
    ///
    /// The schema's values: `All`, `Selector`, `Same`. Another is read as it is.
    #[serde(default = "default_data_plane_gateway_listeners_allowed_routes_namespaces_from")]
    pub from: String,
    /// Selector must be specified when From is set to "Selector". In that case,
    /// only Routes in Namespaces matching this Selector will be selected by this
    /// Gateway. This field is ignored for other values of "From".
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selector: Option<DataPlaneGatewayListenersAllowedRoutesNamespacesSelector>,
}

/// Selector must be specified when From is set to "Selector". In that case,
/// only Routes in Namespaces matching this Selector will be selected by this
/// Gateway. This field is ignored for other values of "From".
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListenersAllowedRoutesNamespacesSelector {
    /// matchExpressions is a list of label selector requirements. The requirements are ANDed.
    #[serde(rename = "matchExpressions")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub match_expressions:
        Option<Vec<DataPlaneGatewayListenersAllowedRoutesNamespacesSelectorMatchExpressions>>,
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
pub struct DataPlaneGatewayListenersAllowedRoutesNamespacesSelectorMatchExpressions {
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

/// TLS is the TLS configuration for the Listener. This field is required if
/// the Protocol field is "HTTPS" or "TLS". It is invalid to set this field
/// if the Protocol field is "HTTP", "TCP", or "UDP".
///
/// The association of SNIs to Certificate defined in ListenerTLSConfig is
/// defined based on the Hostname field for this listener.
///
/// The GatewayClass MUST use the longest matching SNI out of all
/// available certificates for any TLS handshake.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListenersTls {
    /// CertificateRefs contains a series of references to Kubernetes objects that
    /// contains TLS certificates and private keys. These certificates are used to
    /// establish a TLS handshake for requests that match the hostname of the
    /// associated listener.
    ///
    /// A single CertificateRef to a Kubernetes Secret has "Core" support.
    /// Implementations MAY choose to support attaching multiple certificates to
    /// a Listener, but this behavior is implementation-specific.
    ///
    /// References to a resource in different namespace are invalid UNLESS there
    /// is a ReferenceGrant in the target namespace that allows the certificate
    /// to be attached. If a ReferenceGrant does not allow this reference, the
    /// "ResolvedRefs" condition MUST be set to False for this listener with the
    /// "RefNotPermitted" reason.
    ///
    /// This field is required to have at least one element when the mode is set
    /// to "Terminate" (default) and is optional otherwise.
    ///
    /// CertificateRefs can reference to standard Kubernetes resources, i.e.
    /// Secret, or implementation-specific custom resources.
    ///
    /// Support: Core - A single reference to a Kubernetes Secret of type kubernetes.io/tls
    ///
    /// Support: Implementation-specific (More than one reference or other resource types)
    #[serde(rename = "certificateRefs")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_refs: Option<Vec<DataPlaneGatewayListenersTlsCertificateRefs>>,
    /// Mode defines the TLS behavior for the TLS session initiated by the client.
    /// There are two possible modes:
    ///
    /// - Terminate: The TLS session between the downstream client and the
    ///   Gateway is terminated at the Gateway. This mode requires certificates
    ///   to be specified in some way, such as populating the certificateRefs
    ///   field.
    /// - Passthrough: The TLS session is NOT terminated by the Gateway. This
    ///   implies that the Gateway can't decipher the TLS stream except for
    ///   the ClientHello message of the TLS protocol. The certificateRefs field
    ///   is ignored in this mode.
    ///
    /// Support: Core
    ///
    /// The schema's values: `Terminate`, `Passthrough`. Another is read as it is.
    #[serde(default = "default_data_plane_gateway_listeners_tls_mode")]
    pub mode: String,
    /// Options are a list of key/value pairs to enable extended TLS
    /// configuration for each implementation. For example, configuring the
    /// minimum TLS version or supported cipher suites.
    ///
    /// A set of common keys MAY be defined by the API in the future. To avoid
    /// any ambiguity, implementation-specific definitions MUST use
    /// domain-prefixed names, such as `example.com/my-custom-option`.
    /// Un-prefixed names are reserved for key names defined by Gateway API.
    ///
    /// Support: Implementation-specific
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<BTreeMap<String, String>>,
}

/// SecretObjectReference identifies an API object including its namespace,
/// defaulting to Secret.
///
/// The API object must be valid in the cluster; the Group and Kind must
/// be registered in the cluster for this reference to be valid.
///
/// References to objects with invalid Group and Kind are not valid, and must
/// be rejected by the implementation, with appropriate Conditions set
/// on the containing object.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayListenersTlsCertificateRefs {
    /// Group is the group of the referent. For example, "gateway.networking.k8s.io".
    /// When unspecified or empty string, core API group is inferred.
    #[serde(default = "default_data_plane_gateway_listeners_tls_certificate_refs_group")]
    pub group: String,
    /// Kind is kind of the referent. For example "Secret".
    #[serde(default = "default_data_plane_gateway_listeners_tls_certificate_refs_kind")]
    pub kind: String,
    /// Name is the name of the referent.
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of the referenced object. When unspecified, the local
    /// namespace is inferred.
    ///
    /// Note that when a namespace different than the local namespace is specified,
    /// a ReferenceGrant object is required in the referent namespace to allow that
    /// namespace's owner to accept the reference. See the ReferenceGrant
    /// documentation for details.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// TLS specifies frontend and backend tls configuration for entire gateway.
///
/// Support: Extended
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTls {
    /// Backend describes TLS configuration for gateway when connecting
    /// to backends.
    ///
    /// Note that this contains only details for the Gateway as a TLS client,
    /// and does _not_ imply behavior about how to choose which backend should
    /// get a TLS connection. That is determined by the presence of a BackendTLSPolicy.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<DataPlaneGatewayTlsBackend>,
    /// Frontend describes TLS config when client connects to Gateway.
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub frontend: Option<DataPlaneGatewayTlsFrontend>,
}

/// Backend describes TLS configuration for gateway when connecting
/// to backends.
///
/// Note that this contains only details for the Gateway as a TLS client,
/// and does _not_ imply behavior about how to choose which backend should
/// get a TLS connection. That is determined by the presence of a BackendTLSPolicy.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsBackend {
    /// ClientCertificateRef references an object that contains a client certificate
    /// and its associated private key. It can reference standard Kubernetes resources,
    /// i.e., Secret, or implementation-specific custom resources.
    ///
    /// A ClientCertificateRef is considered invalid if:
    ///
    /// * It refers to a resource that cannot be resolved (e.g., the referenced resource
    ///   does not exist) or is misconfigured (e.g., a Secret does not contain the keys
    ///   named `tls.crt` and `tls.key`). In this case, the `ResolvedRefs` condition
    ///   on the Gateway MUST be set to False with the Reason `InvalidClientCertificateRef`
    ///   and the Message of the Condition MUST indicate why the reference is invalid.
    ///
    /// * It refers to a resource in another namespace UNLESS there is a ReferenceGrant
    ///   in the target namespace that allows the certificate to be attached.
    ///   If a ReferenceGrant does not allow this reference, the `ResolvedRefs` condition
    ///   on the Gateway MUST be set to False with the Reason `RefNotPermitted`.
    ///
    /// Implementations MAY choose to perform further validation of the certificate
    /// content (e.g., checking expiry or enforcing specific formats). In such cases,
    /// an implementation-specific Reason and Message MUST be set.
    ///
    /// Support: Core - Reference to a Kubernetes TLS Secret (with the type `kubernetes.io/tls`).
    /// Support: Implementation-specific - Other resource kinds or Secrets with a
    /// different type (e.g., `Opaque`).
    #[serde(rename = "clientCertificateRef")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_certificate_ref: Option<DataPlaneGatewayTlsBackendClientCertificateRef>,
}

/// ClientCertificateRef references an object that contains a client certificate
/// and its associated private key. It can reference standard Kubernetes resources,
/// i.e., Secret, or implementation-specific custom resources.
///
/// A ClientCertificateRef is considered invalid if:
///
/// * It refers to a resource that cannot be resolved (e.g., the referenced resource
///   does not exist) or is misconfigured (e.g., a Secret does not contain the keys
///   named `tls.crt` and `tls.key`). In this case, the `ResolvedRefs` condition
///   on the Gateway MUST be set to False with the Reason `InvalidClientCertificateRef`
///   and the Message of the Condition MUST indicate why the reference is invalid.
///
/// * It refers to a resource in another namespace UNLESS there is a ReferenceGrant
///   in the target namespace that allows the certificate to be attached.
///   If a ReferenceGrant does not allow this reference, the `ResolvedRefs` condition
///   on the Gateway MUST be set to False with the Reason `RefNotPermitted`.
///
/// Implementations MAY choose to perform further validation of the certificate
/// content (e.g., checking expiry or enforcing specific formats). In such cases,
/// an implementation-specific Reason and Message MUST be set.
///
/// Support: Core - Reference to a Kubernetes TLS Secret (with the type `kubernetes.io/tls`).
/// Support: Implementation-specific - Other resource kinds or Secrets with a
/// different type (e.g., `Opaque`).
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsBackendClientCertificateRef {
    /// Group is the group of the referent. For example, "gateway.networking.k8s.io".
    /// When unspecified or empty string, core API group is inferred.
    #[serde(default = "default_data_plane_gateway_tls_backend_client_certificate_ref_group")]
    pub group: String,
    /// Kind is kind of the referent. For example "Secret".
    #[serde(default = "default_data_plane_gateway_tls_backend_client_certificate_ref_kind")]
    pub kind: String,
    /// Name is the name of the referent.
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of the referenced object. When unspecified, the local
    /// namespace is inferred.
    ///
    /// Note that when a namespace different than the local namespace is specified,
    /// a ReferenceGrant object is required in the referent namespace to allow that
    /// namespace's owner to accept the reference. See the ReferenceGrant
    /// documentation for details.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Frontend describes TLS config when client connects to Gateway.
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontend {
    /// Default specifies the default client certificate validation configuration
    /// for all Listeners handling HTTPS traffic, unless a per-port configuration
    /// is defined.
    ///
    /// support: Core
    #[serde(default)]
    pub default: DataPlaneGatewayTlsFrontendDefault,
    /// PerPort specifies tls configuration assigned per port.
    /// Per port configuration is optional. Once set this configuration overrides
    /// the default configuration for all Listeners handling HTTPS traffic
    /// that match this port.
    /// Each override port requires a unique TLS configuration.
    ///
    /// support: Core
    #[serde(rename = "perPort")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub per_port: Option<Vec<DataPlaneGatewayTlsFrontendPerPort>>,
}

/// Default specifies the default client certificate validation configuration
/// for all Listeners handling HTTPS traffic, unless a per-port configuration
/// is defined.
///
/// support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendDefault {
    /// Validation holds configuration information for validating the frontend (client).
    /// Setting this field will result in mutual authentication when connecting to the gateway.
    /// In browsers this may result in a dialog appearing
    /// that requests a user to specify the client certificate.
    /// The maximum depth of a certificate chain accepted in verification is Implementation specific.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<DataPlaneGatewayTlsFrontendDefaultValidation>,
}

/// Validation holds configuration information for validating the frontend (client).
/// Setting this field will result in mutual authentication when connecting to the gateway.
/// In browsers this may result in a dialog appearing
/// that requests a user to specify the client certificate.
/// The maximum depth of a certificate chain accepted in verification is Implementation specific.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendDefaultValidation {
    /// CACertificateRefs contains one or more references to Kubernetes
    /// objects that contain a PEM-encoded TLS CA certificate bundle, which
    /// is used as a trust anchor to validate the certificates presented by
    /// the client.
    ///
    /// A CACertificateRef is invalid if:
    ///
    /// * It refers to a resource that cannot be resolved (e.g., the
    ///   referenced resource does not exist) or is misconfigured (e.g., a
    ///   ConfigMap does not contain a key named `ca.crt`). In this case, the
    ///   Reason on all matching HTTPS listeners must be set to `InvalidCACertificateRef`
    ///   and the Message of the Condition must indicate which reference is invalid and why.
    ///
    /// * It refers to an unknown or unsupported kind of resource. In this
    ///   case, the Reason on all matching HTTPS listeners must be set to
    ///   `InvalidCACertificateKind` and the Message of the Condition must explain
    ///   which kind of resource is unknown or unsupported.
    ///
    /// * It refers to a resource in another namespace UNLESS there is a
    ///   ReferenceGrant in the target namespace that allows the CA
    ///   certificate to be attached. If a ReferenceGrant does not allow this
    ///   reference, the `ResolvedRefs` on all matching HTTPS listeners condition
    ///   MUST be set with the Reason `RefNotPermitted`.
    ///
    /// Implementations MAY choose to perform further validation of the
    /// certificate content (e.g., checking expiry or enforcing specific formats).
    /// In such cases, an implementation-specific Reason and Message MUST be set.
    ///
    /// In all cases, the implementation MUST ensure that the `ResolvedRefs`
    /// condition is set to `status: False` on all targeted listeners (i.e.,
    /// listeners serving HTTPS on a matching port). The condition MUST
    /// include a Reason and Message that indicate the cause of the error. If
    /// ALL CACertificateRefs are invalid, the implementation MUST also ensure
    /// the `Accepted` condition on the listener is set to `status: False`, with
    /// the Reason `NoValidCACertificate`.
    /// Implementations MAY choose to support attaching multiple CA certificates
    /// to a listener, but this behavior is implementation-specific.
    ///
    /// Support: Core - A single reference to a Kubernetes ConfigMap, with the
    /// CA certificate in a key named `ca.crt`.
    ///
    /// Support: Implementation-specific - More than one reference, other kinds
    /// of resources, or a single reference that includes multiple certificates.
    #[serde(rename = "caCertificateRefs")]
    #[serde(default)]
    pub ca_certificate_refs: Vec<DataPlaneGatewayTlsFrontendDefaultValidationCaCertificateRefs>,
    /// FrontendValidationMode defines the mode for validating the client certificate.
    /// There are two possible modes:
    ///
    /// - AllowValidOnly: In this mode, the gateway will accept connections only if
    ///   the client presents a valid certificate. This certificate must successfully
    ///   pass validation against the CA certificates specified in `CACertificateRefs`.
    /// - AllowInsecureFallback: In this mode, the gateway will accept connections
    ///   even if the client certificate is not presented or fails verification.
    ///
    ///   This approach delegates client authorization to the backend and introduce
    ///   a significant security risk. It should be used in testing environments or
    ///   on a temporary basis in non-testing environments.
    ///
    /// Defaults to AllowValidOnly.
    ///
    /// Support: Core
    ///
    /// The schema's values: `AllowValidOnly`, `AllowInsecureFallback`. Another is read as it is.
    #[serde(default = "default_data_plane_gateway_tls_frontend_default_validation_mode")]
    pub mode: String,
}

/// ObjectReference identifies an API object including its namespace.
///
/// The API object must be valid in the cluster; the Group and Kind must
/// be registered in the cluster for this reference to be valid.
///
/// References to objects with invalid Group and Kind are not valid, and must
/// be rejected by the implementation, with appropriate Conditions set
/// on the containing object.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendDefaultValidationCaCertificateRefs {
    /// Group is the group of the referent. For example, "gateway.networking.k8s.io".
    /// When set to the empty string, core API group is inferred.
    #[serde(default)]
    pub group: String,
    /// Kind is kind of the referent. For example "ConfigMap" or "Service".
    #[serde(default)]
    pub kind: String,
    /// Name is the name of the referent.
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of the referenced object. When unspecified, the local
    /// namespace is inferred.
    ///
    /// Note that when a namespace different than the local namespace is specified,
    /// a ReferenceGrant object is required in the referent namespace to allow that
    /// namespace's owner to accept the reference. See the ReferenceGrant
    /// documentation for details.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// `DataPlaneGatewayTlsFrontendPerPort`, which the schema does not describe.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendPerPort {
    /// The Port indicates the Port Number to which the TLS configuration will be
    /// applied. This configuration will be applied to all Listeners handling HTTPS
    /// traffic that match this port.
    ///
    /// Support: Core
    #[serde(default)]
    pub port: i32,
    /// TLS store the configuration that will be applied to all Listeners handling
    /// HTTPS traffic and matching given port.
    ///
    /// Support: Core
    #[serde(default)]
    pub tls: DataPlaneGatewayTlsFrontendPerPortTls,
}

/// TLS store the configuration that will be applied to all Listeners handling
/// HTTPS traffic and matching given port.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendPerPortTls {
    /// Validation holds configuration information for validating the frontend (client).
    /// Setting this field will result in mutual authentication when connecting to the gateway.
    /// In browsers this may result in a dialog appearing
    /// that requests a user to specify the client certificate.
    /// The maximum depth of a certificate chain accepted in verification is Implementation specific.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub validation: Option<DataPlaneGatewayTlsFrontendPerPortTlsValidation>,
}

/// Validation holds configuration information for validating the frontend (client).
/// Setting this field will result in mutual authentication when connecting to the gateway.
/// In browsers this may result in a dialog appearing
/// that requests a user to specify the client certificate.
/// The maximum depth of a certificate chain accepted in verification is Implementation specific.
///
/// Support: Core
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendPerPortTlsValidation {
    /// CACertificateRefs contains one or more references to Kubernetes
    /// objects that contain a PEM-encoded TLS CA certificate bundle, which
    /// is used as a trust anchor to validate the certificates presented by
    /// the client.
    ///
    /// A CACertificateRef is invalid if:
    ///
    /// * It refers to a resource that cannot be resolved (e.g., the
    ///   referenced resource does not exist) or is misconfigured (e.g., a
    ///   ConfigMap does not contain a key named `ca.crt`). In this case, the
    ///   Reason on all matching HTTPS listeners must be set to `InvalidCACertificateRef`
    ///   and the Message of the Condition must indicate which reference is invalid and why.
    ///
    /// * It refers to an unknown or unsupported kind of resource. In this
    ///   case, the Reason on all matching HTTPS listeners must be set to
    ///   `InvalidCACertificateKind` and the Message of the Condition must explain
    ///   which kind of resource is unknown or unsupported.
    ///
    /// * It refers to a resource in another namespace UNLESS there is a
    ///   ReferenceGrant in the target namespace that allows the CA
    ///   certificate to be attached. If a ReferenceGrant does not allow this
    ///   reference, the `ResolvedRefs` on all matching HTTPS listeners condition
    ///   MUST be set with the Reason `RefNotPermitted`.
    ///
    /// Implementations MAY choose to perform further validation of the
    /// certificate content (e.g., checking expiry or enforcing specific formats).
    /// In such cases, an implementation-specific Reason and Message MUST be set.
    ///
    /// In all cases, the implementation MUST ensure that the `ResolvedRefs`
    /// condition is set to `status: False` on all targeted listeners (i.e.,
    /// listeners serving HTTPS on a matching port). The condition MUST
    /// include a Reason and Message that indicate the cause of the error. If
    /// ALL CACertificateRefs are invalid, the implementation MUST also ensure
    /// the `Accepted` condition on the listener is set to `status: False`, with
    /// the Reason `NoValidCACertificate`.
    /// Implementations MAY choose to support attaching multiple CA certificates
    /// to a listener, but this behavior is implementation-specific.
    ///
    /// Support: Core - A single reference to a Kubernetes ConfigMap, with the
    /// CA certificate in a key named `ca.crt`.
    ///
    /// Support: Implementation-specific - More than one reference, other kinds
    /// of resources, or a single reference that includes multiple certificates.
    #[serde(rename = "caCertificateRefs")]
    #[serde(default)]
    pub ca_certificate_refs: Vec<DataPlaneGatewayTlsFrontendPerPortTlsValidationCaCertificateRefs>,
    /// FrontendValidationMode defines the mode for validating the client certificate.
    /// There are two possible modes:
    ///
    /// - AllowValidOnly: In this mode, the gateway will accept connections only if
    ///   the client presents a valid certificate. This certificate must successfully
    ///   pass validation against the CA certificates specified in `CACertificateRefs`.
    /// - AllowInsecureFallback: In this mode, the gateway will accept connections
    ///   even if the client certificate is not presented or fails verification.
    ///
    ///   This approach delegates client authorization to the backend and introduce
    ///   a significant security risk. It should be used in testing environments or
    ///   on a temporary basis in non-testing environments.
    ///
    /// Defaults to AllowValidOnly.
    ///
    /// Support: Core
    ///
    /// The schema's values: `AllowValidOnly`, `AllowInsecureFallback`. Another is read as it is.
    #[serde(default = "default_data_plane_gateway_tls_frontend_per_port_tls_validation_mode")]
    pub mode: String,
}

/// ObjectReference identifies an API object including its namespace.
///
/// The API object must be valid in the cluster; the Group and Kind must
/// be registered in the cluster for this reference to be valid.
///
/// References to objects with invalid Group and Kind are not valid, and must
/// be rejected by the implementation, with appropriate Conditions set
/// on the containing object.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneGatewayTlsFrontendPerPortTlsValidationCaCertificateRefs {
    /// Group is the group of the referent. For example, "gateway.networking.k8s.io".
    /// When set to the empty string, core API group is inferred.
    #[serde(default)]
    pub group: String,
    /// Kind is kind of the referent. For example "ConfigMap" or "Service".
    #[serde(default)]
    pub kind: String,
    /// Name is the name of the referent.
    #[serde(default)]
    pub name: String,
    /// Namespace is the namespace of the referenced object. When unspecified, the local
    /// namespace is inferred.
    ///
    /// Note that when a namespace different than the local namespace is specified,
    /// a ReferenceGrant object is required in the referent namespace to allow that
    /// namespace's owner to accept the reference. See the ReferenceGrant
    /// documentation for details.
    ///
    /// Support: Core
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub namespace: Option<String>,
}

/// Ingress, present, has the data plane serve the Ingresses of its IngressClass,
/// which the operator makes.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneIngress {
    /// HostFallThrough keeps a wildcard host's paths candidates on the more
    /// specific hosts it covers, where false, as nginx has it, leaves a request to
    /// the most specific host alone.
    #[serde(rename = "hostFallThrough")]
    #[serde(default = "default_data_plane_ingress_host_fall_through")]
    pub host_fall_through: bool,
    /// HTTPPort is where an Ingress's hosts are served over plain HTTP, where no
    /// listener of the Gateway names them. Bound only while one is, and always on
    /// the data plane's Service.
    #[serde(rename = "httpPort")]
    #[serde(default)]
    pub http_port: i32,
    /// HTTPSPort is where an Ingress's TLS hosts are served, where no listener of
    /// the Gateway names them. Bound only while one is, and always on the data
    /// plane's Service.
    #[serde(rename = "httpsPort")]
    #[serde(default)]
    pub https_port: i32,
    /// HTTPSRedirect redirects a request over plain HTTP to a host an Ingress
    /// secures to HTTPS, as ingress-nginx's ssl-redirect does. An Ingress may say
    /// otherwise for its own routes.
    #[serde(rename = "httpsRedirect")]
    #[serde(default = "default_data_plane_ingress_https_redirect")]
    pub https_redirect: bool,
    /// HTTPSRedirectMaxAge is how long, in seconds, a client may keep that
    /// redirect: stated, as a browser may otherwise keep a permanent one for ever.
    #[serde(rename = "httpsRedirectMaxAge")]
    #[serde(default = "default_data_plane_ingress_https_redirect_max_age")]
    pub https_redirect_max_age: i64,
}

/// PortSettings is one port's.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlanePorts {
    /// AccessLog is where a record of each of its requests, or of its UDP
    /// listener's sessions, is written: nowhere unless said.
    #[serde(rename = "accessLog")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub access_log: Option<DataPlanePortsAccessLog>,
    /// Forwarding is whom its requests' forwarding headers are believed from.
    /// Left out, no one's are, and the headers only a trusted proxy may send are
    /// Forwarded, X-Real-IP and X-Forwarded-*.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forwarding: Option<DataPlanePortsForwarding>,
    /// HTTP3, present, serves HTTP/3 as well, on the same port over UDP: an
    /// HTTPS listener's alone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http3: Option<DataPlanePortsHttp3>,
    /// Port is the port these settings are for.
    #[serde(default)]
    pub port: i32,
    /// ProxyProtocol, present, has every connection start with a PROXY protocol
    /// header, v1 or v2.
    #[serde(rename = "proxyProtocol")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub proxy_protocol: Option<DataPlanePortsProxyProtocol>,
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
pub struct DataPlanePortsAccessLog {
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
pub struct DataPlanePortsForwarding {
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
pub struct DataPlanePortsHttp3 {
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
pub struct DataPlanePortsProxyProtocol {
    /// Senders are the ranges, the load balancer's, whose headers' addresses
    /// are believed (10.0.0.0/16, 192.0.2.1/32 for one). From anywhere else
    /// the header is read and dropped, and whoever connected is the client.
    #[serde(default)]
    pub senders: Vec<String>,
}

/// Replicas is how many proxies run. With max above min, a
/// HorizontalPodAutoscaler runs the fleet between the two.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneReplicas {
    /// Max is the most an autoscaler may run. Left out, min run, always.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max: Option<i32>,
    /// Min is how many run, or the fewest an autoscaler may.
    #[serde(default = "default_data_plane_replicas_min")]
    pub min: i32,
}

/// Resources is each proxy's compute resources. The proxy runs a request
/// worker for each CPU requested, rounded up; with a CPU limit, no more than the
/// limit rounded down, and at least one. A CPU limit is not recommended:
/// throttling hurts tail latency.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneResources {
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
pub struct DataPlaneService {
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
    #[serde(default = "default_data_plane_service_type")]
    pub r#type: String,
}

/// Status is what the operator and the control plane report of it.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct DataPlaneStatus {
    /// Conditions describe the DataPlane's state. Known condition types are:
    ///
    /// * "Accepted": whether the operator accepted it, by the checks only the
    ///   cluster can make; what the control plane acts on.
    /// * "Programmed": whether what it asks for is made, and its workload has a
    ///   replica available.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_spec_ip_family() -> String {
    String::from("IPv4")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_spec_replicas() -> DataPlaneReplicas {
    DataPlaneReplicas { min: 1, ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_spec_service() -> DataPlaneService {
    DataPlaneService { r#type: String::from("LoadBalancer"), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_allowed_listeners_namespaces() -> DataPlaneGatewayAllowedListenersNamespaces {
    DataPlaneGatewayAllowedListenersNamespaces { from: String::from("None"), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_allowed_listeners_namespaces_from() -> String {
    String::from("None")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_allowed_routes() -> DataPlaneGatewayListenersAllowedRoutes {
    DataPlaneGatewayListenersAllowedRoutes { namespaces: DataPlaneGatewayListenersAllowedRoutesNamespaces { from: String::from("Same"), ..Default::default() }, ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_allowed_routes_namespaces() -> DataPlaneGatewayListenersAllowedRoutesNamespaces {
    DataPlaneGatewayListenersAllowedRoutesNamespaces { from: String::from("Same"), ..Default::default() }
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_allowed_routes_kinds_group() -> String {
    String::from("gateway.networking.k8s.io")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_allowed_routes_namespaces_from() -> String {
    String::from("Same")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_tls_mode() -> String {
    String::from("Terminate")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_tls_certificate_refs_group() -> String {
    String::from("")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_listeners_tls_certificate_refs_kind() -> String {
    String::from("Secret")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_tls_backend_client_certificate_ref_group() -> String {
    String::from("")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_tls_backend_client_certificate_ref_kind() -> String {
    String::from("Secret")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_tls_frontend_default_validation_mode() -> String {
    String::from("AllowValidOnly")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_gateway_tls_frontend_per_port_tls_validation_mode() -> String {
    String::from("AllowValidOnly")
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_ingress_host_fall_through() -> bool {
    false
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_ingress_https_redirect() -> bool {
    true
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_ingress_https_redirect_max_age() -> i64 {
    30
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_replicas_min() -> i32 {
    1
}

/// The schema's default.
#[rustfmt::skip]
fn default_data_plane_service_type() -> String {
    String::from("LoadBalancer")
}

impl k8s_openapi::Resource for DataPlane {
    const API_VERSION: &'static str = "gateway.edgerush.dev/v1alpha1";
    const GROUP: &'static str = "gateway.edgerush.dev";
    const KIND: &'static str = "DataPlane";
    const VERSION: &'static str = "v1alpha1";
    const URL_PATH_SEGMENT: &'static str = "dataplanes";
    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::ListableResource for DataPlane {
    const LIST_KIND: &'static str = "DataPlaneList";
}

impl k8s_openapi::Metadata for DataPlane {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
