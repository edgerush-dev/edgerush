//! The data plane's settings as its `DataPlane` states them
//! ([05 §3](../../../docs/05-operator.md)): which of the DataPlanes of the control plane's
//! namespace is served, and what translation is given of it. The choice is made of the
//! DataPlanes as they are handed to it, without I/O; [`follow_data_planes`] is the follow
//! that hands them on.
//!
//! **Every change goes through the operator** (05 §3): a generation is acted on only once
//! the operator's `Accepted` condition is of it. Until then the last accepted is kept; with
//! none, as after a restart, nothing is served until it is. What a DataPlane has no field
//! for stays at its default: the authorities trusted for `wss` backends none, and the data
//! plane's own settings the config's defaults, but for the Service of its own pods, which
//! says its zone: the data plane's Service, of the name the control plane gives it.

use edgerush_config::{AccessLog, Forwarding, Http3, ListenerProxyProtocol, RequestId};
use edgerush_kubernetes::{ApiServer, Listed, Retry, Scope, follow};
use edgerush_planes::data_plane::{DataPlane, DataPlanePorts};
use edgerush_planes::objects::proxy_name;
use edgerush_translate::{GatewayName, IpFamily, Plane, PortSettings};
use std::collections::BTreeMap;
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::task::JoinSet;

/// The condition the operator says a DataPlane is accepted by.
const ACCEPTED: &str = "Accepted";

/// What the control plane knows of itself, which a DataPlane does not say.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Own {
    /// The name of its ControlPlane, which the DataPlanes it serves name.
    pub control_plane: String,
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

/// Why nothing new is served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Waiting {
    /// No DataPlane names this control plane.
    None,
    /// The operator has yet to accept this DataPlane as it now is.
    Pending(String),
    /// The operator refused this DataPlane as it now is.
    Refused(String),
    /// Accepted, but of a value the data plane cannot take, as said.
    Unusable(String),
}

impl fmt::Display for Waiting {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "no DataPlane names this control plane"),
            Self::Pending(name) => write!(f, "{name}: the operator has yet to accept it"),
            Self::Refused(name) => write!(f, "{name}: the operator did not accept it"),
            Self::Unusable(why) => write!(f, "{why}"),
        }
    }
}

/// What is to be served next.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Next {
    /// These settings, a new generation accepted.
    Serve(Plane),
    /// Those last served, kept, and why nothing new.
    Keep(Waiting),
    /// Nothing, and why.
    Wait(Waiting),
}

/// Which DataPlane is served, as they change.
#[derive(Debug, Default)]
pub struct Chooser {
    /// The settings last served, if any are.
    pub last: Option<Plane>,
}

impl Chooser {
    /// What to serve of `data_planes`, those of the control plane's own namespace, as they
    /// are now. Of those naming this control plane, one accepted, the oldest if the
    /// operator has left two so; else the oldest waiting on the operator, the last served
    /// kept meanwhile; else one refused. None is served while none is accepted.
    pub fn next(&mut self, data_planes: &[DataPlane], own: &Own) -> Next {
        let mut ours: Vec<&DataPlane> = data_planes
            .iter()
            .filter(|data_plane| data_plane.spec.control_plane_ref.name == own.control_plane)
            .collect();
        // Oldest first, then by name: the operator's own order (05 §3).
        ours.sort_by(|a, b| {
            let key = |d: &&DataPlane| {
                (
                    d.metadata.creation_timestamp.clone(),
                    d.metadata.name.clone(),
                )
            };
            key(a).cmp(&key(b))
        });
        let first = |wanted: Verdict| ours.iter().copied().find(|d| verdict(d) == wanted);
        let next = if let Some(accepted) = first(Verdict::Accepted) {
            match plane(accepted, own) {
                Ok(plane) => Next::Serve(plane),
                Err(unusable) => Next::Keep(Waiting::Unusable(unusable.to_string())),
            }
        } else if let Some(pending) = first(Verdict::Pending) {
            Next::Keep(Waiting::Pending(named(pending)))
        } else if let Some(refused) = first(Verdict::Refused) {
            Next::Wait(Waiting::Refused(named(refused)))
        } else {
            Next::Wait(Waiting::None)
        };
        match next {
            Next::Serve(plane) => {
                self.last = Some(plane.clone());
                Next::Serve(plane)
            }
            Next::Keep(waiting) if self.last.is_none() => Next::Wait(waiting),
            Next::Keep(waiting) => Next::Keep(waiting),
            Next::Wait(waiting) => {
                self.last = None;
                Next::Wait(waiting)
            }
        }
    }
}

/// Follows the DataPlanes of `namespace` through `api`, handing `out` the settings to serve
/// each time they change, none while none is accepted, and saying to `say` why nothing new
/// is served, once each time that changes, and each failure of the follow. Runs until no
/// one holds a receiver of `out`. For a `LocalSet`.
pub async fn follow_data_planes(
    api: ApiServer,
    namespace: Scope,
    own: Own,
    out: watch::Sender<Option<Plane>>,
    say: impl Fn(&str) + 'static,
) {
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let (listing, mut listed) = watch::channel(Arc::new(Listed::<DataPlane>::default()));
    let mut tasks = JoinSet::new();
    let failing = Rc::clone(&say);
    tasks.spawn_local(async move {
        follow(&api, &namespace, Retry::default(), listing, |failure| {
            failing(&format!("dataplanes: {failure}"));
        })
        .await;
    });
    let mut chooser = Chooser::default();
    let mut said: Option<Waiting> = None;
    loop {
        tokio::select! {
            changed = listed.changed() => if changed.is_err() { return },
            () = out.closed() => return,
        }
        let now = Arc::clone(&listed.borrow_and_update());
        if !now.listed {
            continue;
        }
        let waiting = match chooser.next(&now.objects, &own) {
            Next::Serve(plane) => {
                out.send_if_modified(|served| {
                    let changed = served.as_ref() != Some(&plane);
                    *served = Some(plane);
                    changed
                });
                None
            }
            Next::Keep(waiting) => Some(waiting),
            Next::Wait(waiting) => {
                out.send_if_modified(|served| served.take().is_some());
                Some(waiting)
            }
        };
        if let Some(waiting) = &waiting
            && said.as_ref() != Some(waiting)
        {
            say(&match chooser.last {
                Some(_) => format!("{waiting}; the DataPlane last accepted is served"),
                None => format!("{waiting}; nothing is served until one is accepted"),
            });
        }
        said = waiting;
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

/// The settings translation is given of `data_plane`, as it now is (05 §3): its
/// IngressClass, `<controlplane>.<dataplane>`, with `ingress` alone; its Gateway, of its
/// own name and namespace, with `gateway` alone; and the Service of its own pods, which
/// says its zone, its own Service's name (05 §3).
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
        ingress_class: ingress.map(|_| format!("{}.{name}", own.control_plane)),
        gateway: spec
            .gateway
            .as_ref()
            .map(|_| GatewayName { namespace, name }),
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
