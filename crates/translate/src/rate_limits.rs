//! Rate limits ([04 §4](../../../docs/04-control-plane.md)): the DataPlane's `global`, and
//! RateLimitPolicies, gathered into each rule's whole list. The control plane resolves
//! `ignore`, so the config has none and its `global` is empty: every limit a rule's requests
//! are under is named on the rule ([07 §2](../../../docs/07-config-and-dsl.md)).
//!
//! The levels, least specific first: the DataPlane's `global`, a Gateway, a listener of it,
//! a ListenerSet, a listener of that, a route, a rule; an Ingress's, the DataPlane's and the
//! Ingress's own alone, as the Gateway is not its parent. A policy's `ignore` removes, by
//! name, limits of the levels less specific than its own on the way to the rule, never one
//! of its level nor of a more specific one; `["*"]` removes them all.
//!
//! **A policy the data plane cannot serve is refused, and what was there before stays**
//! (04 §4): its spec is judged alone, compiled in the data plane's mode, so that a limit the
//! data plane would refuse, or whose model its mode does not run, refuses it; and so does a
//! limit of a name another limit holds, the DataPlane's or another policy's, which would
//! make two counts one. Refused, a policy applies its version last accepted, judged
//! likewise, or nothing; a refusal is said.
//!
//! **The incumbent keeps a name.** A policy holds the names of the version it applied in the
//! config last served, so the change that brings a clash is the one refused, whichever
//! policy is older. Each policy is judged against what the others hold, then the others
//! again against what it now applies, until nothing changes: a name one policy lets go of
//! is free for another in the same translation, whichever is judged first. Where neither
//! held it, as after a restart, the older takes it, then by namespace and name.

use crate::gateway::GatewayOutcome;
use crate::http_route::Attachment;
use crate::{Cluster, Finding, Object, Plane, PlaneRateLimiting, Problem, config_of, namespace};
use edgerush_config::{
    Bucket, Config, Global, Limit, LimitMatch, Limits, MOST_LIMITS, Model, Rolling, Window, compile,
};
use edgerush_planes::data_plane::DataPlaneRateLimitingGlobal;
use edgerush_planes::rate_limit_policy::{
    RateLimitPolicy, RateLimitPolicyLimits, RateLimitPolicySpec,
};
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// The API group of the Gateway API's kinds.
const GATEWAY_GROUP: &str = "gateway.networking.k8s.io";

/// The API group of Ingress.
const INGRESS_GROUP: &str = "networking.k8s.io";

/// A limit as a DataPlane or a RateLimitPolicy writes one, which are copies of one schema
/// generated apart, as the limits file writes it; or the field, below the limit, and why it
/// cannot be.
macro_rules! limit_of {
    ($limit:expr) => {{
        let limit = $limit;
        let key = match limit.key.as_str() {
            "Client" => "client",
            other => return Err(("key".to_owned(), format!("`{other}` is not Client"))),
        };
        let count = |field: &str, value: i64| {
            number(value).ok_or_else(|| (field.to_owned(), format!("{value} is out of range")))
        };
        let stated = &limit.model;
        let model = match (&stated.bucket, &stated.fixed_window, &stated.rolling_window) {
            (Some(bucket), None, None) => Model::Bucket(Bucket {
                rate: bucket.rate.clone(),
                burst: count("model.bucket.burst", bucket.burst)?,
            }),
            (None, Some(window), None) => Model::FixedWindow(Window {
                limit: count("model.fixedWindow.limit", window.limit)?,
                window: window.window.clone(),
            }),
            (None, None, Some(window)) => Model::RollingWindow(Rolling {
                limit: count("model.rollingWindow.limit", window.limit)?,
                window: window.window.clone(),
                slots: window
                    .slots
                    .map(|slots| count("model.rollingWindow.slots", slots))
                    .transpose()?,
            }),
            _ => {
                return Err((
                    "model".to_owned(),
                    "one of bucket, fixedWindow and rollingWindow is needed, and only one"
                        .to_owned(),
                ));
            }
        };
        Ok(Limit {
            name: limit.name.clone(),
            matches: limit.r#match.as_ref().map(|matches| LimitMatch {
                methods: matches.methods.clone().unwrap_or_default(),
                path_prefix: matches.path_prefix.clone(),
            }),
            key: key.to_owned(),
            model,
            dry_run: limit.dry_run.unwrap_or(false),
        })
    }};
}

/// One of a DataPlane's global limits, written as the limits file writes one; or the field,
/// below the limit, and why it cannot be.
///
/// # Errors
///
/// The field, and why, where the limit states what the limits file cannot: a key other than
/// `Client`, a number below 1 or too large, or not exactly one model.
pub fn data_plane_limit(limit: &DataPlaneRateLimitingGlobal) -> Result<Limit, (String, String)> {
    limit_of!(limit)
}

/// One of a RateLimitPolicy's limits, as [`data_plane_limit`] reads a DataPlane's.
fn policy_limit(limit: &RateLimitPolicyLimits) -> Result<Limit, (String, String)> {
    limit_of!(limit)
}

/// `value` as a number of `T`, if it is one and at least 1.
fn number<T: TryFrom<i64>>(value: i64) -> Option<T> {
    if value < 1 {
        return None;
    }
    T::try_from(value).ok()
}

/// A version of a RateLimitPolicy that applied: what a later translation needs of it, to
/// know the names it holds and to apply it where the policy's spec is refused (04 §4).
#[derive(Debug, Clone, PartialEq)]
pub struct AcceptedPolicy {
    /// The policy's namespace.
    pub namespace: String,
    /// The policy's name.
    pub name: String,
    /// The policy's UID: a policy made again under the same name is another, and holds
    /// nothing of the one before.
    pub uid: Option<String>,
    /// The generation of the spec.
    pub generation: Option<i64>,
    /// The spec.
    pub spec: RateLimitPolicySpec,
}

// A spec holds strings, integers and booleans alone, each equal to itself.
impl Eq for AcceptedPolicy {}

impl AcceptedPolicy {
    /// `policy`'s spec as it is now.
    #[must_use]
    pub fn of(policy: &RateLimitPolicy) -> Self {
        let metadata = &policy.metadata;
        Self {
            namespace: namespace(metadata).to_owned(),
            name: metadata.name.clone().unwrap_or_default(),
            uid: metadata.uid.clone(),
            generation: metadata.generation,
            spec: policy.spec.clone(),
        }
    }

    /// Whether this is a version of `policy`.
    fn is_of(&self, policy: &RateLimitPolicy) -> bool {
        let metadata = &policy.metadata;
        self.namespace == namespace(metadata)
            && Some(&self.name) == metadata.name.as_ref()
            && self.uid == metadata.uid
    }
}

/// The limits of a policy's spec, as the limits file writes them, if the data plane, in its
/// mode, would take them; or why not, as compiling them says it.
fn limits_of(
    limiting: &PlaneRateLimiting,
    spec: &RateLimitPolicySpec,
) -> Result<Vec<Limit>, String> {
    let limits = spec
        .limits
        .iter()
        .flatten()
        .map(|limit| {
            policy_limit(limit)
                .map_err(|(field, why)| format!("limits[{}].{field}: {why}", limit.name))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let alone = Config {
        rate_limiting: Some(limiting.written()),
        limits: Some(Limits {
            global: Global::default(),
            limits: limits.clone(),
        }),
        ..config_of(
            BTreeMap::new(),
            Vec::new(),
            BTreeMap::new(),
            &[],
            &[],
            &[],
            &[],
        )
    };
    compile(&alone).map_err(|problems| {
        problems
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; ")
    })?;
    Ok(limits)
}

/// Which version of a policy applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Applied {
    /// Its spec.
    Spec,
    /// Its version last accepted.
    Last,
    /// None: it applies nothing.
    Nothing,
}

/// A policy, its spec and its version last accepted, where that is another, each with its
/// limits or why the data plane would refuse them.
struct Judged<'a> {
    policy: &'a RateLimitPolicy,
    object: Object,
    spec: Result<Vec<Limit>, Problem>,
    last: Option<(&'a AcceptedPolicy, Result<Vec<Limit>, Problem>)>,
}

impl Judged<'_> {
    /// The limits of the version `applied`: none where it applies none.
    fn limits(&self, applied: Applied) -> &[Limit] {
        let limits = match applied {
            Applied::Spec => Some(&self.spec),
            Applied::Last => self.last.as_ref().map(|(_, limits)| limits),
            Applied::Nothing => None,
        };
        limits
            .and_then(|limits| limits.as_deref().ok())
            .unwrap_or_default()
    }

    /// The spec of the version `applied`, where it applies one.
    fn spec(&self, applied: Applied) -> Option<&RateLimitPolicySpec> {
        match applied {
            Applied::Spec => Some(&self.policy.spec),
            Applied::Last => self.last.as_ref().map(|(last, _)| &last.spec),
            Applied::Nothing => None,
        }
    }
}

/// Which version of each of `judged` applies, given that each held what `before` says, and
/// what keeps a spec from applying: each judged in turn against the DataPlane's names,
/// `global`, and the versions the others hold, those judged before it in this round by what
/// they now apply.
fn round(
    judged: &[Judged<'_>],
    global: &[String],
    before: &[Applied],
) -> (Vec<Applied>, Vec<Finding>) {
    let mut now: Vec<Applied> = Vec::with_capacity(judged.len());
    let mut findings = Vec::new();
    for (at, policy) in judged.iter().enumerate() {
        let applied = {
            let holder = |name: &str| -> Option<Option<Object>> {
                if global.iter().any(|held| held == name) {
                    return Some(None);
                }
                judged.iter().enumerate().find_map(|(other, judged)| {
                    let applied = match other.cmp(&at) {
                        Ordering::Less => now[other],
                        Ordering::Equal => return None,
                        Ordering::Greater => before[other],
                    };
                    let holds = judged
                        .limits(applied)
                        .iter()
                        .any(|limit| limit.name == name);
                    holds.then(|| Some(judged.object.clone()))
                })
            };
            let takes = |limits: &Result<Vec<Limit>, Problem>| -> Result<(), Problem> {
                let limits = limits.as_ref().map_err(Clone::clone)?;
                for limit in limits {
                    if let Some(by) = holder(&limit.name) {
                        return Err(Problem::LimitNameHeld {
                            name: limit.name.clone(),
                            by,
                        });
                    }
                }
                Ok(())
            };
            match takes(&policy.spec) {
                Ok(()) => Applied::Spec,
                Err(problem) => {
                    findings.push(Finding {
                        object: policy.object.clone(),
                        problem,
                    });
                    match &policy.last {
                        Some((version, last)) if takes(last).is_ok() => {
                            findings.push(Finding {
                                object: policy.object.clone(),
                                problem: Problem::LastAcceptedApplies(version.generation),
                            });
                            Applied::Last
                        }
                        _ => Applied::Nothing,
                    }
                }
            }
        };
        now.push(applied);
    }
    (now, findings)
}

/// A policy refused for what its judging alone cannot see: a rule it reaches under more
/// limits than the data plane takes (04 §4), and why.
#[derive(Debug, Clone)]
pub(crate) enum Refusal {
    /// Its spec: its version last accepted applies, where it has one.
    Spec(Problem),
    /// Its spec and its version last accepted: it applies nothing.
    Whole(Problem),
}

/// The policies refused for what their judging alone cannot see.
pub(crate) type Forced = BTreeMap<Object, Refusal>;

/// Refuses in `forced`, for each rule of `config` under more limits than the data plane
/// takes, versions of the policies of `applied` whose limits it is under, until it would
/// not be: the changes first, those not applying their version in `accepted` as it is, then
/// the rest, the youngest first in each; a spec refused before, its version last accepted.
/// Whether it refused any.
pub(crate) fn refuse_overfull(
    config: &Config,
    applied: &[AcceptedPolicy],
    accepted: &[AcceptedPolicy],
    forced: &mut Forced,
) -> bool {
    let unchanged = |policy: &AcceptedPolicy| {
        accepted.iter().any(|last| {
            last.namespace == policy.namespace
                && last.name == policy.name
                && last.uid == policy.uid
                && last.spec == policy.spec
        })
    };
    let mut now: BTreeSet<Object> = BTreeSet::new();
    for route in &config.routes {
        for (at, rule) in route.rules.iter().enumerate() {
            let count = rule.rate_limits.len();
            let Some(mut over) = count.checked_sub(MOST_LIMITS).filter(|over| *over > 0) else {
                continue;
            };
            let why = Problem::LimitsTooMany {
                route: route.name.clone(),
                rule: at,
                count,
            };
            // Youngest first, then the changes before the rest, each kept in that order.
            let mut owners: Vec<(&AcceptedPolicy, usize)> = applied
                .iter()
                .rev()
                .filter_map(|policy| {
                    let on = policy
                        .spec
                        .limits
                        .iter()
                        .flatten()
                        .filter(|limit| rule.rate_limits.contains(&limit.name))
                        .count();
                    (on > 0).then_some((policy, on))
                })
                .collect();
            owners.sort_by_key(|(policy, _)| unchanged(policy));
            for (policy, on) in owners {
                if over == 0 {
                    break;
                }
                let object = Object {
                    kind: "RateLimitPolicy",
                    namespace: policy.namespace.clone(),
                    name: policy.name.clone(),
                };
                if now.insert(object.clone()) {
                    let refusal = match forced.get(&object) {
                        None => Refusal::Spec(why.clone()),
                        Some(_) => Refusal::Whole(why.clone()),
                    };
                    forced.insert(object, refusal);
                }
                over = over.saturating_sub(on);
            }
        }
    }
    !now.is_empty()
}

/// What a policy's `ignore` removes.
#[derive(Debug)]
enum Ignore {
    /// These names.
    Names(BTreeSet<String>),
    /// Every limit less specific than its target: `["*"]`.
    All,
}

/// One of a policy's targets, in the policy's own namespace.
#[derive(Debug)]
struct Target {
    group: String,
    kind: String,
    name: String,
    section: Option<String>,
}

/// A policy taken: its targets, the names of its limits and what it ignores.
#[derive(Debug)]
struct Taken {
    namespace: String,
    targets: Vec<Target>,
    limits: Vec<String>,
    ignore: Ignore,
}

impl Taken {
    /// Whether the policy targets `kind` of `group` named `name` in `namespace`, with the
    /// section `section` or, for `None`, the whole of it.
    fn targets(
        &self,
        (group, kind): (&str, &str),
        namespace: &str,
        name: &str,
        section: Option<&str>,
    ) -> bool {
        self.namespace == namespace
            && self.targets.iter().any(|target| {
                target.group == group
                    && target.kind == kind
                    && target.name == name
                    && target.section.as_deref() == section
            })
    }
}

/// Where a rule of a route of the Gateway API is served: the route's attachment to one
/// listener, the route, and the rule's name, if it has one.
pub(crate) struct Place<'a> {
    /// The attachment: its listener, and the ListenerSet stating it, if one does.
    pub(crate) attachment: &'a Attachment,
    /// The route: an HTTPRoute or a GRPCRoute.
    pub(crate) route: &'a Object,
    /// The rule's name, which a policy's `sectionName` names it by.
    pub(crate) rule: Option<&'a str>,
}

/// The data plane's limits, and the policies taken, in the order they rank: none of either
/// where it does not limit, so that no rule is under a limit and the config carries none.
#[derive(Debug, Default)]
pub(crate) struct RateLimits {
    /// The names of the DataPlane's `global` limits.
    global: Vec<String>,
    /// The data plane's Gateway, where it is one.
    gateway: Option<Object>,
    /// The policies taken, oldest first, then by namespace and name.
    taken: Vec<Taken>,
    /// Every limit: the DataPlane's, then each policy's taken, in the order they rank.
    pub(crate) limits: Vec<Limit>,
    /// The version of each policy that applies, in the order they rank.
    pub(crate) accepted: Vec<AcceptedPolicy>,
}

impl RateLimits {
    /// The DataPlane's limits and every RateLimitPolicy's, oldest first, then by namespace
    /// and name: each policy's spec unless it is refused, and then its version in
    /// `accepted`, the last accepted, unless that is refused too; each refusal said in
    /// `report`.
    pub(crate) fn of(
        plane: &Plane,
        cluster: &Cluster,
        accepted: &[AcceptedPolicy],
        forced: &Forced,
        gateway: Option<&GatewayOutcome>,
        report: &mut Vec<Finding>,
    ) -> Self {
        let Some(limiting) = &plane.rate_limiting else {
            // Nothing is judged where nothing limits: each policy keeps what it held.
            return Self {
                accepted: accepted
                    .iter()
                    .filter(|last| {
                        cluster
                            .rate_limit_policies
                            .iter()
                            .any(|policy| last.is_of(policy))
                    })
                    .cloned()
                    .collect(),
                ..Self::default()
            };
        };
        let global: Vec<String> = limiting
            .global
            .iter()
            .map(|limit| limit.name.clone())
            .collect();
        let mut policies: Vec<&RateLimitPolicy> = cluster.rate_limit_policies.iter().collect();
        policies.sort_by_key(|policy| {
            let metadata = &policy.metadata;
            (
                metadata.creation_timestamp.clone(),
                namespace(metadata).to_owned(),
                metadata.name.clone(),
            )
        });
        let mut before = Vec::with_capacity(policies.len());
        let judged: Vec<Judged<'_>> = policies
            .into_iter()
            .map(|policy| {
                let last = accepted.iter().find(|last| last.is_of(policy));
                before.push(match last {
                    None => Applied::Nothing,
                    Some(last) if last.spec == policy.spec => Applied::Spec,
                    Some(_) => Applied::Last,
                });
                let object = Object::of("RateLimitPolicy", &policy.metadata);
                let refused = forced.get(&object);
                let judged = |spec: &RateLimitPolicySpec, whole: bool| match refused {
                    Some(Refusal::Whole(why)) => Err(why.clone()),
                    Some(Refusal::Spec(why)) if !whole => Err(why.clone()),
                    _ => limits_of(limiting, spec).map_err(Problem::Refused),
                };
                Judged {
                    policy,
                    spec: judged(&policy.spec, false),
                    last: last
                        .filter(|last| last.spec != policy.spec)
                        .map(|last| (last, judged(&last.spec, true))),
                    object,
                }
            })
            .collect();
        // A policy that applies its spec in one round does in every later one, so the rounds
        // settle within one a policy; the bound is for certainty.
        let mut applied = before;
        let mut rounds = 0;
        let findings = loop {
            let (now, findings) = round(&judged, &global, &applied);
            rounds += 1;
            let settled = now == applied;
            applied = now;
            if settled || rounds > judged.len() {
                break findings;
            }
        };
        report.extend(findings);
        let mut limits = limiting.global.clone();
        let mut taken = Vec::new();
        let mut kept = Vec::new();
        for (policy, applied) in judged.iter().zip(applied) {
            let Some(spec) = policy.spec(applied) else {
                continue;
            };
            kept.push(match (applied, &policy.last) {
                (Applied::Last, Some((last, _))) => (*last).clone(),
                _ => AcceptedPolicy::of(policy.policy),
            });
            let own = policy.limits(applied);
            let ignore = spec.ignore.as_deref().unwrap_or_default();
            taken.push(Taken {
                namespace: policy.object.namespace.clone(),
                targets: spec
                    .target_refs
                    .iter()
                    .map(|target| Target {
                        group: target.group.clone(),
                        kind: target.kind.clone(),
                        name: target.name.clone(),
                        section: target.section_name.clone(),
                    })
                    .collect(),
                limits: own.iter().map(|limit| limit.name.clone()).collect(),
                ignore: if ignore.iter().any(|name| name == "*") {
                    Ignore::All
                } else {
                    Ignore::Names(ignore.iter().cloned().collect())
                },
            });
            limits.extend(own.iter().cloned());
        }
        Self {
            global,
            gateway: gateway.map(|gateway| gateway.gateway.clone()),
            taken,
            limits,
            accepted: kept,
        }
    }

    /// The limits a rule of an Ingress `ingress` is under: the DataPlane's and the
    /// Ingress's own policies'.
    pub(crate) fn of_ingress(&self, ingress: &Object) -> Vec<String> {
        let own = self.on_level(|taken| {
            taken.targets(
                (INGRESS_GROUP, "Ingress"),
                &ingress.namespace,
                &ingress.name,
                None,
            )
        });
        self.under(vec![own])
    }

    /// The limits a rule of a route of the Gateway API, served at `place`, is under: the
    /// DataPlane's, then those of the policies on the Gateway, on the listener or the
    /// ListenerSet and its listener, on the route and on the rule.
    pub(crate) fn of_rule(&self, place: &Place<'_>) -> Vec<String> {
        let mut levels = Vec::new();
        let listener = place.attachment.listener.as_str();
        if let Some(gateway) = &self.gateway {
            let on_gateway = |section: Option<&str>| {
                self.on_level(|taken| {
                    taken.targets(
                        (GATEWAY_GROUP, "Gateway"),
                        &gateway.namespace,
                        &gateway.name,
                        section,
                    )
                })
            };
            levels.push(on_gateway(None));
            if place.attachment.set.is_none() {
                levels.push(on_gateway(Some(listener)));
            }
        }
        if let Some(set) = &place.attachment.set {
            let on_set = |section: Option<&str>| {
                self.on_level(|taken| {
                    taken.targets(
                        (GATEWAY_GROUP, "ListenerSet"),
                        &set.namespace,
                        &set.name,
                        section,
                    )
                })
            };
            levels.push(on_set(None));
            levels.push(on_set(Some(listener)));
        }
        let route = place.route;
        let on_route = |section: Option<&str>| {
            self.on_level(|taken| {
                taken.targets(
                    (GATEWAY_GROUP, route.kind),
                    &route.namespace,
                    &route.name,
                    section,
                )
            })
        };
        levels.push(on_route(None));
        if let Some(rule) = place.rule {
            levels.push(on_route(Some(rule)));
        }
        self.under(levels)
    }

    /// The policies taken that `on` says are on a level.
    fn on_level(&self, on: impl Fn(&Taken) -> bool) -> Vec<&Taken> {
        self.taken.iter().filter(|taken| on(taken)).collect()
    }

    /// The names of the limits a rule is under, of the DataPlane and of the policies of
    /// `levels`, least specific first: each level's less those a more specific level's
    /// policies ignore.
    fn under(&self, levels: Vec<Vec<&Taken>>) -> Vec<String> {
        let mut ignored: BTreeSet<&str> = BTreeSet::new();
        let mut all = false;
        let mut kept: Vec<Vec<&str>> = Vec::new();
        for level in levels.iter().rev() {
            kept.push(
                level
                    .iter()
                    .flat_map(|taken| &taken.limits)
                    .map(String::as_str)
                    .filter(|name| !all && !ignored.contains(name))
                    .collect(),
            );
            for taken in level {
                match &taken.ignore {
                    Ignore::All => all = true,
                    Ignore::Names(names) => ignored.extend(names.iter().map(String::as_str)),
                }
            }
        }
        let global = self
            .global
            .iter()
            .map(String::as_str)
            .filter(|name| !all && !ignored.contains(name));
        global
            .chain(kept.into_iter().rev().flatten())
            .map(str::to_owned)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A data plane limiting locally under one global limit, `platform`.
    const PLANE: &str = "cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: true
ingress_https_redirect_max_age: 30
rate_limiting:
  mode: local
  global: [{ name: platform, key: client, model: { bucket: { rate: 100/s, burst: 100 } } }]
";

    /// The policy `name` of namespace `shop`, made on day `day` of 2026, at `generation`,
    /// with a limit of each of `limits`' names.
    fn policy(name: &str, day: u8, generation: i64, limits: &[&str]) -> RateLimitPolicy {
        let limits: Vec<String> = limits
            .iter()
            .map(|limit| {
                format!(
                    "{{ name: {limit}, key: Client, model: {{ bucket: {{ rate: 1/s, burst: 1 }} }} }}"
                )
            })
            .collect();
        serde_saphyr::from_str(&format!(
            "metadata:
  name: {name}
  namespace: shop
  uid: {name}-uid
  generation: {generation}
  creationTimestamp: \"2026-01-{day:02}T00:00:00Z\"
spec:
  targetRefs: [{{ group: gateway.networking.k8s.io, kind: HTTPRoute, name: a }}]
  limits: [{}]
",
            limits.join(", ")
        ))
        .unwrap()
    }

    /// What applies of `policies` translated for `plane`, `accepted` being the versions
    /// last accepted: each policy's name and generation applied, in the order they rank,
    /// every limit's name, and the report.
    fn judged(
        plane: &str,
        policies: &[RateLimitPolicy],
        accepted: &[&RateLimitPolicy],
    ) -> (Vec<(String, i64)>, Vec<String>, Vec<String>) {
        let plane: Plane = serde_saphyr::from_str(plane).unwrap();
        let cluster = Cluster {
            rate_limit_policies: policies.to_vec(),
            ..Cluster::default()
        };
        let accepted: Vec<AcceptedPolicy> = accepted
            .iter()
            .map(|policy| AcceptedPolicy::of(policy))
            .collect();
        let mut report = Vec::new();
        let limits = RateLimits::of(
            &plane,
            &cluster,
            &accepted,
            &Forced::new(),
            None,
            &mut report,
        );
        (
            limits
                .accepted
                .iter()
                .map(|kept| (kept.name.clone(), kept.generation.unwrap()))
                .collect(),
            limits
                .limits
                .iter()
                .map(|limit| limit.name.clone())
                .collect(),
            report.iter().map(ToString::to_string).collect(),
        )
    }

    /// Names moved along a chain in one change, each to a policy judged before the one
    /// letting it go: `y` from `r` to `q`, `x` from `q` to `p`. Each is free once the policy
    /// holding it applies its new spec, so all three apply theirs in the same translation,
    /// and nothing is refused.
    #[test]
    fn names_moved_along_a_chain_settle_in_one_translation() {
        let before = [
            policy("p", 1, 1, &["p-own"]),
            policy("q", 2, 1, &["x"]),
            policy("r", 3, 1, &["y", "r-own"]),
        ];
        let now = [
            policy("p", 1, 2, &["p-own", "x"]),
            policy("q", 2, 2, &["y"]),
            policy("r", 3, 2, &["r-own"]),
        ];
        let (applied, limits, report) = judged(PLANE, &now, &before.iter().collect::<Vec<_>>());
        assert_eq!(report, Vec::<String>::new());
        assert_eq!(
            applied,
            [
                ("p".to_owned(), 2),
                ("q".to_owned(), 2),
                ("r".to_owned(), 2)
            ]
        );
        assert_eq!(limits, ["platform", "p-own", "x", "y", "r-own"]);
    }

    /// An older policy edited to take a name a younger one holds is refused, and applies its
    /// version last accepted; the younger keeps the name, untouched. Where neither held it,
    /// as after a restart, the older takes it.
    #[test]
    fn the_incumbent_keeps_its_name_and_age_decides_only_where_none_held_it() {
        let older = policy("older", 1, 1, &["older-own"]);
        let younger = policy("younger", 2, 1, &["shared"]);
        let edited = policy("older", 1, 2, &["older-own", "shared"]);
        let now = [edited, younger.clone()];
        let (applied, limits, report) = judged(PLANE, &now, &[&older, &younger]);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/older: refused: its limit `shared` has the name of \
                 RateLimitPolicy shop/younger's, which holds it",
                "RateLimitPolicy shop/older: its version last accepted applies instead \
                 (generation 1)"
            ]
        );
        assert_eq!(
            applied,
            [("older".to_owned(), 1), ("younger".to_owned(), 1)]
        );
        assert_eq!(limits, ["platform", "older-own", "shared"]);
        // Afresh, nothing accepted before: the older's spec takes the name, the younger
        // applies nothing.
        let (applied, _, report) = judged(PLANE, &now, &[]);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/younger: refused: its limit `shared` has the name of \
              RateLimitPolicy shop/older's, which holds it"
            ]
        );
        assert_eq!(applied, [("older".to_owned(), 2)]);
    }

    /// A policy deleted and made again under its name is another, of another UID: it holds
    /// nothing of the one before, nor applies its version where its own spec is refused.
    #[test]
    fn a_policy_made_again_holds_nothing_of_the_one_before() {
        let gone = policy("remade", 1, 1, &["kept"]);
        let mut remade = policy("remade", 1, 1, &["kept", "platform"]);
        remade.metadata.uid = Some("another-uid".to_owned());
        let (applied, limits, report) = judged(PLANE, &[remade], &[&gone]);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/remade: refused: its limit `platform` has the name of \
              the DataPlane's, which holds it"
            ]
        );
        assert_eq!(applied, []);
        assert_eq!(limits, ["platform"]);
    }

    /// Where the data plane does not limit, nothing is judged and no policy applies, but
    /// each keeps the version it held, so that what holds a name is the same once it
    /// limits again; one no longer there is let go of.
    #[test]
    fn a_data_plane_not_limiting_keeps_what_each_policy_held() {
        let plane = PLANE.split("rate_limiting").next().unwrap();
        let held = policy("held", 1, 1, &["held"]);
        let gone = policy("gone", 2, 1, &["gone"]);
        let edited = policy("held", 1, 2, &["held", "more"]);
        let (applied, limits, report) = judged(plane, &[edited], &[&held, &gone]);
        assert_eq!(report, Vec::<String>::new());
        assert_eq!(applied, [("held".to_owned(), 1)]);
        assert_eq!(limits, Vec::<String>::new());
    }

    /// The policy `name` of `shop` on the Ingress `shop/web`, made on day `day` of 2026, at
    /// `generation`, with `count` limits, `<name>-0` and on.
    fn on_ingress(name: &str, day: u8, generation: i64, count: usize) -> RateLimitPolicy {
        let names: Vec<String> = (0..count).map(|n| format!("{name}-{n}")).collect();
        let names: Vec<&str> = names.iter().map(String::as_str).collect();
        let mut policy = policy(name, day, generation, &names);
        policy.spec.target_refs[0].group = "networking.k8s.io".to_owned();
        policy.spec.target_refs[0].kind = "Ingress".to_owned();
        policy.spec.target_refs[0].name = "web".to_owned();
        policy
    }

    /// `policies` translated for a data plane serving the Ingress `shop/web`, `accepted`
    /// being the versions last accepted: each policy's name and generation applied, how many
    /// limits the Ingress's rule is under, and the report.
    fn on_the_rule(
        policies: &[RateLimitPolicy],
        accepted: &[&RateLimitPolicy],
    ) -> (Vec<(String, i64)>, usize, Vec<String>) {
        let plane: Plane =
            serde_saphyr::from_str(&format!("ingress_class: edge\n{PLANE}")).unwrap();
        let cluster = Cluster {
            ingress_classes: vec![
                serde_saphyr::from_str(
                    "{ metadata: { name: edge }, spec: { controller: edgerush.dev/ingress-controller } }",
                )
                .unwrap(),
            ],
            ingresses: vec![
                serde_saphyr::from_str(
                    "metadata: { name: web, namespace: shop }
spec:
  ingressClassName: edge
  rules:
    - host: shop.example
      http:
        paths:
          - { path: /, pathType: Prefix, backend: { service: { name: web, port: { number: 80 } } } }
",
                )
                .unwrap(),
            ],
            services: vec![
                serde_saphyr::from_str(
                    "{ metadata: { name: web, namespace: shop }, spec: { ports: [{ name: http, port: 80 }] } }",
                )
                .unwrap(),
            ],
            rate_limit_policies: policies.to_vec(),
            ..Cluster::default()
        };
        let accepted: Vec<AcceptedPolicy> = accepted
            .iter()
            .map(|policy| AcceptedPolicy::of(policy))
            .collect();
        let translation = crate::translate_from(&plane, &cluster, &accepted);
        let rules: Vec<usize> = translation
            .config
            .routes
            .iter()
            .flat_map(|route| &route.rules)
            .map(|rule| rule.rate_limits.len())
            .collect();
        assert_eq!(rules.len(), 1, "one rule");
        assert!(compile(&translation.config).is_ok(), "the config refused");
        (
            translation
                .rate_limit_policies
                .iter()
                .map(|kept| (kept.name.clone(), kept.generation.unwrap()))
                .collect(),
            rules[0],
            translation.report.iter().map(ToString::to_string).collect(),
        )
    }

    /// A new policy that would put a rule over the data plane's most limits is refused,
    /// applying nothing; the policy there before keeps applying, and the config is served.
    #[test]
    fn a_new_policy_putting_a_rule_over_the_most_limits_is_refused() {
        let big = on_ingress("big", 1, 1, 60);
        let (applied, under, report) =
            on_the_rule(&[big.clone(), on_ingress("extra", 2, 1, 5)], &[&big]);
        assert_eq!(applied, [("big".to_owned(), 1)]);
        assert_eq!(under, 61);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/extra: the data plane would refuse it: rule 0 of route \
              ingress/shop/web/shop.example would be under 66 limits, more than the 64 a rule takes"
            ]
        );
    }

    /// An edit that would put a rule over is refused, its version last accepted applying;
    /// so is an older policy's, against a younger one that applied as it is.
    #[test]
    fn an_edit_putting_a_rule_over_the_most_limits_applies_its_last_accepted() {
        let older = on_ingress("older", 1, 1, 2);
        let younger = on_ingress("younger", 2, 1, 60);
        let now = [on_ingress("older", 1, 2, 5), younger.clone()];
        let (applied, under, report) = on_the_rule(&now, &[&older, &younger]);
        assert_eq!(
            applied,
            [("older".to_owned(), 1), ("younger".to_owned(), 1)]
        );
        assert_eq!(under, 63);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/older: the data plane would refuse it: rule 0 of route \
                 ingress/shop/web/shop.example would be under 66 limits, more than the 64 a rule \
                 takes",
                "RateLimitPolicy shop/older: its version last accepted applies instead \
                 (generation 1)"
            ]
        );
    }

    /// Policies that all applied as they are, and put a rule over together, as a change to
    /// a route can have them: the youngest is refused, applying nothing. So is a change
    /// whose version last accepted puts the rule over as well.
    #[test]
    fn of_policies_putting_a_rule_over_unchanged_the_youngest_is_refused() {
        let first = on_ingress("first", 1, 1, 40);
        let second = on_ingress("second", 2, 1, 40);
        let (applied, under, report) =
            on_the_rule(&[first.clone(), second.clone()], &[&first, &second]);
        assert_eq!(applied, [("first".to_owned(), 1)]);
        assert_eq!(under, 41);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/second: the data plane would refuse it: rule 0 of route \
              ingress/shop/web/shop.example would be under 81 limits, more than the 64 a rule takes"
            ]
        );
        // Its edit refused, its version last accepted is too.
        let edited = on_ingress("second", 2, 2, 45);
        let (applied, under, report) = on_the_rule(&[first.clone(), edited], &[&first, &second]);
        assert_eq!(applied, [("first".to_owned(), 1)]);
        assert_eq!(under, 41);
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/second: the data plane would refuse it: rule 0 of route \
              ingress/shop/web/shop.example would be under 81 limits, more than the 64 a rule takes"
            ]
        );
    }
}
