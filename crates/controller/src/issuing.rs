//! The control plane as its planes' certificate authority
//! ([05 §2](../../../docs/05-operator.md), [12](../../../docs/12-open-questions.md) item 22).
//!
//! Its authority is a Secret of its own namespace: one its leader makes on first start,
//! and signs again when a fifth of its life is left, or the platform's, which it is given
//! and never writes. Every replica reads it and signs its own certificate for the stream
//! in memory, again at half its life, as istiod does: on first start there is no Secret
//! for it to mount. The leader writes each DataPlane's certificate into a Secret its pods
//! mount, owned by the DataPlane, and writes it again at half its life, or once the
//! authority's bundle changes. Each workload is known by the SPIFFE ID of its
//! ServiceAccount, in its ControlPlane's own trust domain.
//!
//! What to write is decided without I/O, of the Secrets and DataPlanes as they are and a
//! time passed in; [`issue`] is the loop that follows them and writes.

use crate::data_plane::accepted_ever;
use edgerush_certificates::authority::{Authority, Kind, Leaf, Written, renewal_due};
use edgerush_kubernetes::{ApiServer, Listed, Retry, Scope, follow};
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::objects::{controller_name, identity_secret, proxy_name, spiffe_id};
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::Secret;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use std::collections::BTreeMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::{Notify, watch};
use tokio::task::JoinSet;

/// How often what is due is looked at, besides each change.
const LOOK: Duration = Duration::from_secs(60);

/// The type of every Secret it writes, as cert-manager writes one.
const TLS: &str = "kubernetes.io/tls";

/// How long, once a provided authority is replaced, the control plane keeps its own
/// certificate of the one before: time for the kubelet to put each workload's new bundle,
/// which trusts both, into its pods, so that none refuses the control plane meanwhile
/// (05 §2). The kubelet takes some two minutes at most.
pub const SWITCH_AFTER: i64 = 300;

/// What is to be done of the control plane's own certificate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Own {
    /// Kept as it is.
    Keep,
    /// Kept, trusting this bundle now.
    Rebundle(Written),
    /// Signed anew.
    Issue,
}

/// What is to be done of the control plane's own certificate `current` at `now`, by
/// `authority`, where its predecessor's is kept until `switch_at`: none, one it does not
/// trust, or one past half its life is signed anew; one of its predecessor's is kept until
/// then; one whose bundle is no longer what it trusts is given the new.
#[must_use]
pub fn own_step(
    current: Option<&Written>,
    authority: &Authority,
    now: i64,
    switch_at: Option<i64>,
) -> Own {
    let Some(current) = current else {
        return Own::Issue;
    };
    let due = current
        .validity()
        .is_none_or(|dates| renewal_due(dates, now));
    if due || !authority.trusts(current, now) {
        return Own::Issue;
    }
    if !authority.signed(current, now) && switch_at.is_none_or(|at| now >= at) {
        return Own::Issue;
    }
    let trusted = authority.trusted_pem();
    if current.bundle == trusted {
        Own::Keep
    } else {
        Own::Rebundle(Written {
            bundle: trusted,
            ..current.clone()
        })
    }
}

/// Who it is, and the authority it signs with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issuer {
    /// Its own namespace, where its authority and its DataPlanes are.
    pub namespace: String,
    /// Its ControlPlane's name.
    pub control_plane: String,
    /// The cluster's DNS domain, which its trust domain ends in.
    pub cluster_domain: String,
    /// The Secret its authority is kept in.
    pub authority: String,
    /// Whether that is its own or the platform's.
    pub kind: Kind,
}

impl Issuer {
    /// What its own certificate is for: the names data planes dial it by, its Service's,
    /// and its ServiceAccount's ID.
    #[must_use]
    pub fn own_leaf(&self) -> Leaf {
        let name = controller_name(&self.control_plane);
        let service = format!("{name}.{}.svc", self.namespace);
        Leaf {
            common_name: name.clone(),
            dns: vec![
                service.clone(),
                format!("{service}.{}", self.cluster_domain),
            ],
            spiffe_id: spiffe_id(
                &self.control_plane,
                &self.cluster_domain,
                &self.namespace,
                &name,
            ),
        }
    }

    /// What DataPlane `data_plane`'s certificate is for: its ServiceAccount's ID.
    #[must_use]
    pub fn data_plane_leaf(&self, data_plane: &str) -> Leaf {
        let name = proxy_name(data_plane);
        Leaf {
            common_name: name.clone(),
            dns: Vec::new(),
            spiffe_id: spiffe_id(
                &self.control_plane,
                &self.cluster_domain,
                &self.namespace,
                &name,
            ),
        }
    }
}

/// What `secret` holds, if it holds all three of a certificate, its key and a bundle.
#[must_use]
pub fn written(secret: &Secret) -> Option<Written> {
    let data = secret.data.as_ref()?;
    let text = |key: &str| {
        data.get(key)
            .and_then(|bytes| String::from_utf8(bytes.0.clone()).ok())
    };
    Some(Written {
        certificate: text("tls.crt")?,
        key: text("tls.key")?,
        bundle: text("ca.crt")?,
    })
}

/// Secret `name` in `namespace`, of `written`, owned by `owner` if any, as of `version` if
/// it is there already.
#[must_use]
pub fn secret(
    name: &str,
    namespace: &str,
    written: &Written,
    owner: Option<OwnerReference>,
    version: Option<String>,
) -> Secret {
    let bytes = |text: &str| ByteString(text.as_bytes().to_vec());
    Secret {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some(namespace.to_owned()),
            resource_version: version,
            labels: Some(BTreeMap::from([(
                "app.kubernetes.io/managed-by".to_owned(),
                "edgerush-controller".to_owned(),
            )])),
            owner_references: owner.map(|owner| vec![owner]),
            ..ObjectMeta::default()
        },
        type_: Some(TLS.to_owned()),
        data: Some(BTreeMap::from([
            ("tls.crt".to_owned(), bytes(&written.certificate)),
            ("tls.key".to_owned(), bytes(&written.key)),
            ("ca.crt".to_owned(), bytes(&written.bundle)),
        ])),
        ..Secret::default()
    }
}

/// Whether a certificate written as `current` is to be issued again by `authority` at
/// `now`: there is none, or it cannot be read; its authority is not trusted; it names
/// another bundle; or it is past half its life.
#[must_use]
pub fn reissue(current: Option<&Written>, authority: &Authority, now: i64) -> bool {
    let Some(current) = current else {
        return true;
    };
    if current.bundle != authority.trusted_pem() || !authority.trusts(current, now) {
        return true;
    }
    current
        .validity()
        .is_none_or(|dates| renewal_due(dates, now))
}

/// What is to be done of the authority, its Secret as it is.
#[derive(Debug)]
pub enum Step {
    /// There to sign with.
    Ready(Authority),
    /// Made now, its Secret to be created.
    Make(Authority),
    /// Signed again now, its Secret, at this version, to be replaced.
    Resign(Authority, Option<String>),
    /// Nothing to sign with, and why.
    Wait(String),
}

/// What is to be done of `issuer`'s authority at `now`, its Secret `found`, by a replica
/// that leads or not: only a leader makes it or signs it again.
#[must_use]
pub fn step(issuer: &Issuer, found: Option<&Secret>, now: i64, leads: bool) -> Step {
    let Some(found) = found else {
        return match (issuer.kind, leads) {
            (Kind::Made, true) => match Authority::make(&issuer.control_plane, now) {
                Ok(authority) => Step::Make(authority),
                Err(error) => Step::Wait(format!("the authority cannot be made: {error}")),
            },
            (Kind::Made, false) => Step::Wait(format!(
                "Secret {}/{}: the authority, which the leader makes, is not there yet",
                issuer.namespace, issuer.authority
            )),
            (Kind::Provided, _) => Step::Wait(format!(
                "Secret {}/{}: the authority provided is not there",
                issuer.namespace, issuer.authority
            )),
        };
    };
    let read = written(found)
        .ok_or_else(|| "it holds no tls.crt, tls.key and ca.crt".to_owned())
        .and_then(|held| {
            Authority::read(&held, issuer.kind, now).map_err(|error| error.to_string())
        });
    match read {
        Ok(authority) if leads && authority.due(now) => match authority.resign(now) {
            Ok(again) => Step::Resign(again, found.metadata.resource_version.clone()),
            Err(error) => Step::Wait(format!("the authority cannot be signed again: {error}")),
        },
        Ok(authority) => Step::Ready(authority),
        Err(why) => Step::Wait(format!(
            "Secret {}/{}: the authority cannot be read: {why}",
            issuer.namespace, issuer.authority
        )),
    }
}

/// The DataPlanes of `data_planes` whose certificates `issuer` keeps: those that name its
/// ControlPlane and that the operator has ever accepted.
#[must_use]
pub fn served<'a>(issuer: &Issuer, data_planes: &'a [DataPlane]) -> Vec<&'a DataPlane> {
    data_planes
        .iter()
        .filter(|data_plane| data_plane.spec.control_plane_ref.name == issuer.control_plane)
        .filter(|data_plane| accepted_ever(data_plane))
        .collect()
}

/// An owner reference to `data_plane`, which its certificate's Secret goes with.
fn owner(data_plane: &DataPlane) -> Option<OwnerReference> {
    Some(OwnerReference {
        api_version: "gateway.edgerush.dev/v1alpha1".to_owned(),
        kind: "DataPlane".to_owned(),
        name: data_plane.metadata.name.clone()?,
        uid: data_plane.metadata.uid.clone()?,
        ..OwnerReference::default()
    })
}

/// Now, in seconds since the Unix epoch.
fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_secs()).unwrap_or(i64::MAX)
        })
}

/// Keeps `issuer`'s authority, its own certificate and its DataPlanes' through `api`: the
/// Secrets and DataPlanes of its namespace followed, its own certificate handed to `own`
/// each time it is signed, the leader alone writing, where `leading` says this replica
/// leads (always, without it). What it does and what fails is said to `say`. Runs until
/// no one holds a receiver of `own`. For a `LocalSet`.
pub async fn issue(
    api: ApiServer,
    issuer: Issuer,
    leading: Option<watch::Receiver<bool>>,
    own: watch::Sender<Option<Written>>,
    say: impl Fn(&str) + 'static,
) {
    let api = Rc::new(api);
    let say: Rc<dyn Fn(&str)> = Rc::new(say);
    let changed = Rc::new(Notify::new());
    let mut tasks = JoinSet::new();
    let Ok(namespace) = Scope::namespace(&issuer.namespace) else {
        say(&format!("`{}` is not a namespace", issuer.namespace));
        return;
    };
    let (secrets_out, secrets) = watch::channel(Arc::new(Listed::<Secret>::default()));
    let (planes_out, planes) = watch::channel(Arc::new(Listed::<DataPlane>::default()));
    {
        let (client, failing, part) = (Rc::clone(&api), Rc::clone(&say), namespace.clone());
        tasks.spawn_local(async move {
            follow(&client, &part, Retry::default(), secrets_out, |failure| {
                failing(&format!("secrets, for the authority: {failure}"));
            })
            .await;
        });
        let (client, failing) = (Rc::clone(&api), Rc::clone(&say));
        tasks.spawn_local(async move {
            follow(
                &client,
                &namespace,
                Retry::default(),
                planes_out,
                |failure| {
                    failing(&format!("dataplanes, for their certificates: {failure}"));
                },
            )
            .await;
        });
    }
    // Each change to either is a change to look at.
    {
        let (mut told, notify) = (secrets.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                notify.notify_one();
            }
        });
        let (mut told, notify) = (planes.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                notify.notify_one();
            }
        });
    }
    if let Some(leading) = &leading {
        let (mut told, changed) = (leading.clone(), Rc::clone(&changed));
        tasks.spawn_local(async move {
            while told.changed().await.is_ok() {
                changed.notify_one();
            }
        });
    }
    let mut said: Option<String> = None;
    // Until when its own certificate of a provided authority's predecessor is kept.
    let mut switch_at: Option<i64> = None;
    loop {
        if own.is_closed() {
            return;
        }
        let (secrets_now, planes_now) =
            (Arc::clone(&secrets.borrow()), Arc::clone(&planes.borrow()));
        if secrets_now.listed && planes_now.listed {
            let now = unix_now();
            let leads = leading.as_ref().is_none_or(|leads| *leads.borrow());
            let find = |name: &str| {
                secrets_now
                    .objects
                    .iter()
                    .find(|secret| secret.metadata.name.as_deref() == Some(name))
            };
            let waiting = match step(&issuer, find(&issuer.authority), now, leads) {
                Step::Wait(why) => Some(why),
                Step::Make(authority) => {
                    let made = secret(
                        &issuer.authority,
                        &issuer.namespace,
                        &authority.written(),
                        None,
                        None,
                    );
                    match api.create(&issuer.namespace, &made).await {
                        Ok(_) => say(&format!(
                            "authority made, Secret {}/{}",
                            issuer.namespace, issuer.authority
                        )),
                        // Another leader's, a moment before: read on the next change.
                        Err(error) if error.is_conflict() => {}
                        Err(error) => say(&format!(
                            "Secret {}/{}: the authority cannot be written: {error}",
                            issuer.namespace, issuer.authority
                        )),
                    }
                    None
                }
                Step::Resign(authority, version) => {
                    let again = secret(
                        &issuer.authority,
                        &issuer.namespace,
                        &authority.written(),
                        None,
                        version,
                    );
                    match api
                        .replace(&issuer.namespace, &issuer.authority, &again)
                        .await
                    {
                        Ok(_) => say(&format!(
                            "authority signed again, Secret {}/{}",
                            issuer.namespace, issuer.authority
                        )),
                        Err(error) if error.is_conflict() => {}
                        Err(error) => say(&format!(
                            "Secret {}/{}: the authority cannot be written: {error}",
                            issuer.namespace, issuer.authority
                        )),
                    }
                    None
                }
                Step::Ready(authority) => {
                    // A provided authority's predecessors, as the Secrets it wrote and its
                    // own certificate remember them, trusted until they expire (05 §2).
                    let remembered: Vec<String> = served(&issuer, &planes_now.objects)
                        .iter()
                        .filter_map(|data_plane| data_plane.metadata.name.as_deref())
                        .filter_map(|name| find(&identity_secret(name)).and_then(written))
                        .map(|held| held.bundle)
                        .chain(own.borrow().as_ref().map(|held| held.bundle.clone()))
                        .collect();
                    let remembered: Vec<&str> = remembered.iter().map(String::as_str).collect();
                    let authority = authority.trusting(&remembered, now);
                    // Its own, every replica: one of a predecessor's is kept a while, for
                    // its workloads to be given the new bundle first.
                    let current = own.borrow().clone();
                    if let Some(current) = &current {
                        if authority.signed(current, now) {
                            switch_at = None;
                        } else if switch_at.is_none() {
                            switch_at = Some(now + SWITCH_AFTER);
                        }
                    }
                    match own_step(current.as_ref(), &authority, now, switch_at) {
                        Own::Keep => {}
                        Own::Rebundle(kept) => {
                            own.send_replace(Some(kept));
                            say("own certificate kept, trusting the authority's new bundle");
                        }
                        Own::Issue => match authority.issue(&issuer.own_leaf(), now) {
                            Ok(issued) => {
                                own.send_replace(Some(issued));
                                switch_at = None;
                                say("own certificate signed");
                            }
                            Err(error) => {
                                say(&format!("own certificate cannot be signed: {error}"))
                            }
                        },
                    }
                    // Each DataPlane's, the leader's alone to write.
                    if leads {
                        for data_plane in served(&issuer, &planes_now.objects) {
                            let Some(name) = data_plane.metadata.name.as_deref() else {
                                continue;
                            };
                            let secret_name = identity_secret(name);
                            let found = find(&secret_name);
                            if !reissue(found.and_then(written).as_ref(), &authority, now) {
                                continue;
                            }
                            let issued = match authority.issue(&issuer.data_plane_leaf(name), now) {
                                Ok(issued) => issued,
                                Err(error) => {
                                    say(&format!(
                                        "certificate of DataPlane {}/{name} cannot be signed: {error}",
                                        issuer.namespace
                                    ));
                                    continue;
                                }
                            };
                            let version =
                                found.and_then(|found| found.metadata.resource_version.clone());
                            let object = secret(
                                &secret_name,
                                &issuer.namespace,
                                &issued,
                                owner(data_plane),
                                version.clone(),
                            );
                            let written = match version {
                                Some(_) => api
                                    .replace(&issuer.namespace, &secret_name, &object)
                                    .await
                                    .map(|_| ()),
                                None => api.create(&issuer.namespace, &object).await.map(|_| ()),
                            };
                            match written {
                                Ok(()) => say(&format!(
                                    "certificate of DataPlane {}/{name} written, Secret {secret_name}",
                                    issuer.namespace
                                )),
                                Err(error) if error.is_conflict() => {}
                                Err(error) => say(&format!(
                                    "Secret {}/{secret_name}: the certificate of DataPlane {name} cannot be written: {error}",
                                    issuer.namespace
                                )),
                            }
                        }
                    }
                    None
                }
            };
            if waiting != said {
                if let Some(why) = &waiting {
                    say(why);
                }
                said = waiting;
            }
        }
        tokio::select! {
            () = changed.notified() => {}
            () = tokio::time::sleep(LOOK) => {}
            () = own.closed() => return,
        }
    }
}

#[cfg(test)]
mod tests;
