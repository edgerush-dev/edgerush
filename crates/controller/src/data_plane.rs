//! The data planes' settings as their `DataPlane`s state them
//! ([05 §3](../../../docs/05-operator.md)): which of the DataPlanes naming the control
//! plane's ControlPlane are served, each on its own, and what translation is given of each:
//! those of its own namespace, and those the operator generates for Gateways of its class
//! across its watch scope, each in its Gateway's namespace. The choice is made of the
//! DataPlanes as they are handed to it, without I/O; [`follow_data_planes`] is the follow
//! that hands them on.
//!
//! **Every change goes through the operator** (05 §3): a generation of a DataPlane is acted
//! on only once the operator's `Accepted` condition is of it. Until then its last accepted
//! is kept; with none, as after a restart, it is not served until it is. What a DataPlane has no field
//! for stays at its default: the authorities trusted for `wss` backends none, and the data
//! plane's own settings the config's defaults, but for the Service of its own pods, which
//! says its zone: the data plane's Service, of the name the control plane gives it, and
//! writes as the DataPlane shapes it (05 §2).

use edgerush_config::{AccessLog, Forwarding, Http3, ListenerProxyProtocol, RequestId};
use edgerush_kubernetes::{ApiServer, Scope};
use edgerush_planes::data_plane::{DataPlane, DataPlanePorts, DataPlaneRateLimiting};
use edgerush_planes::objects::{ProxyService, data_plane_id, proxy_name};
use edgerush_translate::{
    GatewayName, IpFamily, Plane, PlaneRateLimitMode, PlaneRateLimiting, PortSettings,
    data_plane_limit,
};

use crate::across::{Across, follow_across, watch_parts};
use crate::{DataPlaneOf, Planes, Settings, WatchScope};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::watch;

/// The condition the operator says a DataPlane is accepted by.
const ACCEPTED: &str = "Accepted";

/// What the control plane knows of itself, which a DataPlane does not say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Own {
    /// The name of its ControlPlane, which the DataPlanes it serves name.
    pub control_plane: String,
    /// Its ControlPlane's namespace, its own: what a DataPlane of another namespace names
    /// beside the name (05 §3).
    pub namespace: String,
    /// The cluster's DNS domain, the operator's install setting.
    pub cluster_domain: String,
    /// The certificates, in PEM, of the authorities its own system trusts.
    pub system_authorities: Vec<String>,
}

/// What the operator says of a DataPlane's generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    /// Accepted, as of this generation.
    Accepted,
    /// Refused, as of this generation.
    Refused,
    /// Not looked at yet, as of this generation.
    Pending,
}

/// Whether the operator accepted `data_plane` as it now is.
#[must_use]
pub fn verdict(data_plane: &DataPlane) -> Verdict {
    let generation = data_plane.metadata.generation;
    let condition = data_plane
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .and_then(|conditions| conditions.iter().find(|c| c.type_ == ACCEPTED));
    match condition {
        Some(condition) if generation.is_some() && condition.observed_generation == generation => {
            match condition.status.as_str() {
                "True" => Verdict::Accepted,
                "False" => Verdict::Refused,
                _ => Verdict::Pending,
            }
        }
        _ => Verdict::Pending,
    }
}

/// Whether the operator has accepted `data_plane` as of any generation: one served, if
/// not yet as it now is.
#[must_use]
pub fn accepted_ever(data_plane: &DataPlane) -> bool {
    data_plane
        .status
        .as_ref()
        .and_then(|status| status.conditions.as_ref())
        .is_some_and(|conditions| {
            conditions
                .iter()
                .any(|condition| condition.type_ == ACCEPTED && condition.status == "True")
        })
}

/// Why a DataPlane is not served as it now is, or why none is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Waiting {
    /// No DataPlane names this control plane.
    None,
    /// The operator has yet to accept this DataPlane as it now is.
    Pending(String),
    /// The operator refused this DataPlane as it now is.
    Refused(String),
    /// Accepted, but of a value the data plane cannot take.
    Unusable(Unusable),
}

impl Waiting {
    /// The DataPlane it is of, as it is said, where it is of one.
    #[must_use]
    pub fn data_plane(&self) -> Option<&str> {
        match self {
            Self::None => None,
            Self::Pending(name) | Self::Refused(name) => Some(name),
            Self::Unusable(unusable) => Some(&unusable.data_plane),
        }
    }
}

impl fmt::Display for Waiting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "no DataPlane names this control plane"),
            Self::Pending(name) => write!(f, "{name}: the operator has yet to accept it"),
            Self::Refused(name) => write!(f, "{name}: the operator did not accept it"),
            Self::Unusable(unusable) => write!(f, "{unusable}"),
        }
    }
}

/// What is served of the DataPlanes, and why any is not as it now is.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Choice {
    /// The settings of each DataPlane served, by the DataPlane as it is said.
    pub served: Planes,
    /// Why each DataPlane naming this control plane that is not served as it now is is
    /// not, in the operator's order; [`Waiting::None`] alone where none names it.
    pub waiting: Vec<Waiting>,
}

impl Choice {
    /// What is said of each DataPlane not served as it now is: whether its last accepted
    /// generation is served meanwhile.
    #[must_use]
    pub fn told(&self) -> Vec<String> {
        self.waiting
            .iter()
            .map(|waiting| match waiting.data_plane() {
                Some(name) if self.served.contains_key(name) => {
                    format!("{waiting}; the DataPlane last accepted is served")
                }
                Some(_) => format!("{waiting}; it is not served until it is accepted"),
                None => format!("{waiting}; nothing is served until one is accepted"),
            })
            .collect()
    }
}

/// Which DataPlanes are served, as they change: each on its own, by its own last accepted
/// generation (05 §3).
#[derive(Debug, Default)]
pub struct Chooser {
    /// The settings last served of each DataPlane served, by the DataPlane as it is said.
    pub last: Planes,
}

impl Chooser {
    /// What to serve of `data_planes`, those the control plane reads, as they are now.
    /// Each naming this control plane's ControlPlane is served as its generation the operator
    /// accepted; one it has yet to look at, or accepted of a value the data plane cannot
    /// take, as its last accepted, where there is one, and else not; one it refused, or
    /// gone, not at all, nothing kept of it for a later edit.
    pub fn next(&mut self, data_planes: &[DataPlane], own: &Own) -> Choice {
        let mut ours: Vec<&DataPlane> = data_planes
            .iter()
            .filter(|data_plane| names_us(data_plane, own))
            .collect();
        // Oldest first, then by namespace and name: the operator's own order (05 §3).
        ours.sort_by(|a, b| {
            let key = |d: &&DataPlane| {
                (
                    d.metadata.creation_timestamp.clone(),
                    d.metadata.namespace.clone(),
                    d.metadata.name.clone(),
                )
            };
            key(a).cmp(&key(b))
        });
        let mut choice = Choice::default();
        if ours.is_empty() {
            choice.waiting.push(Waiting::None);
        }
        for data_plane in ours {
            let name = named(data_plane);
            let kept = || self.last.get(&name).cloned();
            let (served, waiting) = match verdict(data_plane) {
                Verdict::Accepted => match settings(data_plane, own) {
                    Ok(settings) => (Some(settings), None),
                    Err(unusable) => (kept(), Some(Waiting::Unusable(unusable))),
                },
                Verdict::Pending => (kept(), Some(Waiting::Pending(name.clone()))),
                Verdict::Refused => (None, Some(Waiting::Refused(name.clone()))),
            };
            if let Some(settings) = served {
                choice.served.insert(name, settings);
            }
            choice.waiting.extend(waiting);
        }
        self.last.clone_from(&choice.served);
        choice
    }
}

/// Whether `data_plane` names the control plane's ControlPlane, of its name and namespace,
/// a `controlPlaneRef` of no namespace naming one of the DataPlane's own.
#[must_use]
pub fn names_us(data_plane: &DataPlane, own: &Own) -> bool {
    let of = &data_plane.spec.control_plane_ref;
    let namespace = of
        .namespace
        .as_deref()
        .or(data_plane.metadata.namespace.as_deref());
    of.name == own.control_plane && namespace == Some(own.namespace.as_str())
}

/// The IngressClasses of the control plane's own, `<controlplane>.<dataplane>`, that a
/// DataPlane of `data_planes` may still serve (04 §6), `served` as [`Chooser::next`] chose:
/// each served one's, where it serves Ingress, and that of each naming it and not served,
/// one waiting on the operator or refused, whose pods serve their last config. Only a
/// DataPlane of the ControlPlane's own namespace has a class (05 §3).
#[must_use]
pub fn kept(data_planes: &[DataPlane], served: &Planes, own: &Own) -> BTreeSet<String> {
    data_planes
        .iter()
        .filter(|data_plane| names_us(data_plane, own))
        .filter_map(|data_plane| match served.get(&named(data_plane)) {
            Some(settings) => settings.plane.ingress_class.clone(),
            None if data_plane.metadata.namespace.as_deref() != Some(own.namespace.as_str()) => {
                None
            }
            None => Some(format!(
                "{}.{}",
                own.control_plane,
                data_plane.metadata.name.as_deref().unwrap_or_default()
            )),
        })
        .collect()
}

/// Follows through `api` the DataPlanes of the control plane's own namespace and of its
/// watch scope `scope`, where the operator generates them for Gateways of its class
/// (05 §3), handing `out` the settings of each served each time they change, none while
/// none is accepted, and `keeping` the classes [`kept`] of them, once they are listed; and
/// saying to `say` why one is not served as it now is, once each time that changes, and
/// each failure of the follow. A namespace that joins the scope is not waited on: its
/// DataPlanes are taken once listed. Runs until no one holds a receiver of `out`. For a
/// `LocalSet`.
pub async fn follow_data_planes(
    api: ApiServer,
    scope: WatchScope,
    own: Own,
    out: watch::Sender<Planes>,
    keeping: watch::Sender<Option<BTreeSet<String>>>,
    say: impl Fn(&str) + 'static,
) {
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let mut listed = follow_ours(Rc::new(api), scope, &own.namespace, &say);
    let mut chooser = Chooser::default();
    let mut said: Vec<String> = Vec::new();
    loop {
        tokio::select! {
            changed = listed.changed() => if changed.is_err() { return },
            () = out.closed() => return,
        }
        let now = Arc::clone(&listed.borrow_and_update());
        if !now.listed {
            continue;
        }
        let objects = now.objects();
        let choice = chooser.next(&objects, &own);
        let told = choice.told();
        // Handed on beside the settings, in either order: a class is let go of only once
        // neither serves nor keeps it (04 §6).
        let classes = Some(kept(&objects, &choice.served, &own));
        keeping.send_if_modified(|kept| {
            let changed = *kept != classes;
            if changed {
                *kept = classes;
            }
            changed
        });
        out.send_if_modified(|served| {
            let changed = *served != choice.served;
            if changed {
                *served = choice.served;
            }
            changed
        });
        // Each once, until it changes.
        for line in told.iter().filter(|line| !said.contains(line)) {
            say(line);
        }
        said = told;
    }
}

/// Follows through `api` the DataPlanes the control plane reads, of its own namespace
/// `namespace` and its watch scope `scope`, on tasks of the `LocalSet` it is called on,
/// which end once no one holds the receiver it returns; each failure said to `say`.
#[must_use]
pub fn follow_ours(
    api: Rc<ApiServer>,
    scope: WatchScope,
    namespace: &str,
    say: &Rc<dyn Fn(&str)>,
) -> watch::Receiver<Arc<Across<DataPlane>>> {
    let (parts_out, parts) = watch::channel(None);
    let (out, listed) = watch::channel(Arc::new(Across::default()));
    let (client, failing) = (Rc::clone(&api), Rc::clone(say));
    tokio::task::spawn_local(watch_parts(
        client,
        scope,
        Scope::Namespace(namespace.to_owned()),
        parts_out,
        move |failure| failing(&format!("namespaces of the watch scope: {failure}")),
    ));
    let failing = Rc::clone(say);
    tokio::task::spawn_local(follow_across(api, parts, out, move |part, failure| {
        failing(&format!("dataplanes {}: {failure}", in_part(part)));
    }));
    listed
}

/// Where `part` is, as said: `in shop`, `in the cluster`.
pub(crate) fn in_part(part: &Scope) -> String {
    match part {
        Scope::Namespace(namespace) => format!("in {namespace}"),
        Scope::Cluster => "in the cluster".to_owned(),
        Scope::Named(name) => format!("named {name}"),
        Scope::Labelled(labels) => format!("labelled {labels}"),
    }
}

/// `DataPlane <namespace>/<name>`, as it is said.
fn named(data_plane: &DataPlane) -> String {
    format!(
        "DataPlane {}/{}",
        data_plane.metadata.namespace.as_deref().unwrap_or_default(),
        data_plane.metadata.name.as_deref().unwrap_or_default()
    )
}

/// A DataPlane's value the data plane cannot take, which its schema let through.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{data_plane}: {field}: {why}")]
pub struct Unusable {
    /// The DataPlane, as it is said.
    pub data_plane: String,
    /// The field, as the DataPlane writes it.
    pub field: String,
    /// What is wrong with its value.
    pub why: String,
}

/// What is served of `data_plane`, as it now is: the settings translation is given, and
/// the data plane's Service as it shapes it (05 §2).
///
/// # Errors
///
/// [`Unusable`] for a value its schema let through that the data plane cannot take.
pub fn settings(data_plane: &DataPlane, own: &Own) -> Result<Settings, Unusable> {
    let plane = plane(data_plane, own)?;
    let service = ProxyService::of(data_plane).map_err(|error| Unusable {
        data_plane: named(data_plane),
        field: "service".to_owned(),
        why: error.to_string(),
    })?;
    Ok(Settings {
        plane,
        service: Some(service),
        generated: data_plane.spec.gateway_ref.is_some(),
        data_plane: Some(DataPlaneOf {
            namespace: data_plane.metadata.namespace.clone().unwrap_or_default(),
            name: data_plane.metadata.name.clone().unwrap_or_default(),
            generation: data_plane.metadata.generation.unwrap_or_default(),
        }),
    })
}

/// The settings translation is given of `data_plane`, as it now is (05 §3): the ID its
/// certificate carries, which the stream knows its data plane by; its IngressClass,
/// `<controlplane>.<dataplane>`, with `ingress` alone; its Gateway, of its own name and
/// namespace with `gateway`, or the one `gatewayRef` names in its namespace, where it was
/// generated for that Gateway; and the Service of its own pods, which says its zone, its own
/// Service's name (05 §3).
///
/// # Errors
///
/// [`Unusable`] for a value its schema let through that the data plane cannot take.
pub fn plane(data_plane: &DataPlane, own: &Own) -> Result<Plane, Unusable> {
    let name = data_plane.metadata.name.clone().unwrap_or_default();
    let namespace = data_plane.metadata.namespace.clone().unwrap_or_default();
    let unusable = |field: String, why: String| Unusable {
        data_plane: named(data_plane),
        field,
        why,
    };
    let spec = &data_plane.spec;
    let ip_family = match spec.ip_family.as_str() {
        "IPv4" => IpFamily::Ipv4,
        "IPv6" => IpFamily::Ipv6,
        "Dual" => IpFamily::Dual,
        other => {
            return Err(unusable(
                "ipFamily".to_owned(),
                format!("`{other}` is not IPv4, IPv6 or Dual"),
            ));
        }
    };
    let mut ports = BTreeMap::new();
    for settings in spec.ports.iter().flatten() {
        let at = |field: &str| format!("ports[{}].{field}", settings.port);
        let port = number(settings.port, 1)
            .ok_or_else(|| unusable(at("port"), format!("{} is out of range", settings.port)))?;
        ports.insert(
            port,
            port_settings(settings).map_err(|(field, why)| unusable(at(&field), why))?,
        );
    }
    let rate_limiting = spec
        .rate_limiting
        .as_ref()
        .map(rate_limiting)
        .transpose()
        .map_err(|(field, why)| unusable(format!("rateLimiting.{field}"), why))?;
    let ingress = spec.ingress.as_ref();
    let (http_port, https_port) = match ingress {
        Some(ingress) => {
            let port = |value: i32, field: &str| {
                number(value, 1).ok_or_else(|| {
                    unusable(
                        format!("ingress.{field}"),
                        format!("{value} is out of range"),
                    )
                })
            };
            (
                port(ingress.http_port, "httpPort")?,
                port(ingress.https_port, "httpsPort")?,
            )
        }
        // Never bound, as no Ingress is served.
        None => (80, 443),
    };
    let max_age = match ingress {
        Some(ingress) => number(ingress.https_redirect_max_age, 0).ok_or_else(|| {
            unusable(
                "ingress.httpsRedirectMaxAge".to_owned(),
                format!("{} is out of range", ingress.https_redirect_max_age),
            )
        })?,
        None => 30,
    };
    let own_service = format!(
        "{}.{namespace}.svc.{}",
        proxy_name(&name),
        own.cluster_domain
    );
    Ok(Plane {
        id: Some(data_plane_id(
            &own.control_plane,
            &own.cluster_domain,
            &namespace,
            &name,
        )),
        ingress_class: ingress.map(|_| format!("{}.{name}", own.control_plane)),
        gateway: match (&spec.gateway, &spec.gateway_ref) {
            (_, Some(gateway)) => Some(GatewayName {
                namespace,
                name: gateway.name.clone(),
            }),
            (Some(_), None) => Some(GatewayName { namespace, name }),
            (None, None) => None,
        },
        ports,
        ip_family,
        ingress_http_port: http_port,
        ingress_https_port: https_port,
        cluster_domain: own.cluster_domain.clone(),
        ingress_hosts_fall_through: ingress.is_some_and(|ingress| ingress.host_fall_through),
        ingress_https_redirect: ingress.is_none_or(|ingress| ingress.https_redirect),
        ingress_https_redirect_max_age: max_age,
        backend_authorities: Vec::new(),
        system_authorities: own.system_authorities.clone(),
        data_plane: edgerush_config::DataPlane {
            own_service: Some(own_service),
            ..edgerush_config::DataPlane::default()
        },
        rate_limiting,
    })
}

/// How the data plane enforces its rate limits, as `limiting` states it, and its global
/// limits, each the limits file's own (05 §1); or the field, below `rateLimiting`, and why it
/// cannot be.
fn rate_limiting(limiting: &DataPlaneRateLimiting) -> Result<PlaneRateLimiting, (String, String)> {
    let mode = match limiting.mode.as_str() {
        "Disabled" => PlaneRateLimitMode::Off,
        "Local" => PlaneRateLimitMode::Local,
        other => {
            return Err((
                "mode".to_owned(),
                format!("`{other}` is not Disabled or Local"),
            ));
        }
    };
    let max_key_bytes = limiting
        .max_key_bytes
        .map(|bytes| {
            number(bytes, 1)
                .ok_or_else(|| ("maxKeyBytes".to_owned(), format!("{bytes} is out of range")))
        })
        .transpose()?;
    let global = limiting
        .global
        .iter()
        .flatten()
        .map(|limit| {
            data_plane_limit(limit)
                .map_err(|(field, why)| (format!("global[{}].{field}", limit.name), why))
        })
        .collect::<Result<_, _>>()?;
    Ok(PlaneRateLimiting {
        mode,
        max_key_bytes,
        global,
    })
}

/// `value` as a number of `T`, if it is one and no less than `least`.
fn number<V: Copy + Into<i64>, T: TryFrom<i64>>(value: V, least: i64) -> Option<T> {
    let value: i64 = value.into();
    if value < least {
        return None;
    }
    T::try_from(value).ok()
}

/// What a port's listener is set up with, as `settings` states it; or the field, below
/// `ports[<port>]`, and why it cannot be.
fn port_settings(settings: &DataPlanePorts) -> Result<PortSettings, (String, String)> {
    let range = |field: &str, value: i64| (field.to_owned(), format!("{value} is out of range"));
    let http3 = match &settings.http3 {
        Some(http3) => {
            // Built through serde, so that what is left out takes the config's own default.
            let mut stated = serde_json::Map::new();
            if let Some(age) = http3.alt_svc_max_age {
                let age: u32 = number(age, 0).ok_or_else(|| range("http3.altSvcMaxAge", age))?;
                stated.insert("alt_svc_max_age".to_owned(), age.into());
            }
            if let Some(force) = http3.force_retry {
                stated.insert("force_retry".to_owned(), force.into());
            }
            Some(
                serde_json::from_value::<Http3>(serde_json::Value::Object(stated))
                    .map_err(|error| ("http3".to_owned(), error.to_string()))?,
            )
        }
        None => None,
    };
    let request_id = match settings.request_id.as_deref() {
        None => None,
        Some("Generate") => Some(RequestId::Generate),
        Some("Pass") => Some(RequestId::Pass),
        Some(other) => {
            return Err((
                "requestId".to_owned(),
                format!("`{other}` is not Generate or Pass"),
            ));
        }
    };
    let access_log = match &settings.access_log {
        None => None,
        Some(log) => Some(match (log.r#type.as_str(), &log.path) {
            ("Stdout", _) => AccessLog::Stdout,
            ("File", Some(path)) => AccessLog::File(path.into()),
            ("File", None) => {
                return Err(("accessLog.path".to_owned(), "a File needs one".to_owned()));
            }
            (other, _) => {
                return Err((
                    "accessLog.type".to_owned(),
                    format!("`{other}` is not Stdout or File"),
                ));
            }
        }),
    };
    let seconds = |value: Option<i64>, field: &str| -> Result<Option<u64>, (String, String)> {
        value
            .map(|value| number(value, 0).ok_or_else(|| range(field, value)))
            .transpose()
    };
    Ok(PortSettings {
        http3,
        proxy_protocol: settings
            .proxy_protocol
            .as_ref()
            .map(|proxy| ListenerProxyProtocol::Senders(proxy.senders.clone())),
        forwarding: settings.forwarding.as_ref().map(|forwarding| Forwarding {
            trusted_proxies: forwarding.trusted_proxies.clone(),
            trusted_only_headers: forwarding.trusted_only_headers.clone(),
        }),
        request_id,
        access_log,
        tunnel_idle_seconds: seconds(settings.tunnel_idle_seconds, "tunnelIdleSeconds")?,
        session_idle_seconds: seconds(settings.session_idle_seconds, "sessionIdleSeconds")?,
        session_answers: settings
            .session_answers
            .map(|value| number(value, 0).ok_or_else(|| range("sessionAnswers", value)))
            .transpose()?,
    })
}

#[cfg(test)]
mod tests;
