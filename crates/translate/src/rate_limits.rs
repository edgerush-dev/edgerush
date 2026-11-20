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
use crate::http_route::{Attachment, RouteOutcome};
use crate::status::{Condition, GatewayStatus, LimitPolicyStatus, RouteStatus};
use crate::{
    Cluster, Finding, Object, Plane, PlaneRateLimitMode, PlaneRateLimiting, Problem, config_of,
    namespace,
};
use edgerush_config::{
    Bucket, Config, Global, Limit, LimitMatch, Limits, MOST_LIMITS, Model, Rolling, Window, compile,
};
use edgerush_planes::data_plane::DataPlaneRateLimitingGlobal;
use edgerush_planes::rate_limit_policy::{
    RateLimitPolicy, RateLimitPolicyLimits, RateLimitPolicySpec,
};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

/// The API group of the Gateway API's kinds.
const GATEWAY_GROUP: &str = "gateway.networking.k8s.io";

/// The API group of Ingress.
const INGRESS_GROUP: &str = "networking.k8s.io";

/// The condition on an object a RateLimitPolicy that applies targets, as GEP-713 has a
/// policy say so on its target, so that `kubectl describe` shows it (04 §6).
pub const AFFECTED: &str = "gateway.edgerush.dev/RateLimitPolicyAffected";

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

/// Why a RateLimitPolicy's `spec` is refused judged alone, as the admission webhook judges
/// it ([04 §3](../../../docs/04-control-plane.md)), if it is: its limits do not compile.
/// Alone means with no other policy, no target and no DataPlane, so a name another limit
/// holds, a target not there and a model the data plane's mode does not run are not said
/// here: they hang on what else is in the cluster, and its status says them. Said as the
/// translation says it, so the webhook and the status agree.
#[must_use]
pub fn policy_refused_alone(spec: &RateLimitPolicySpec) -> Option<Problem> {
    // Off adds nothing to what compiling the limits checks: only another mode refuses a
    // model, or a bucket of too short a refill, for what it runs.
    let alone = PlaneRateLimiting {
        mode: PlaneRateLimitMode::Off,
        max_key_bytes: None,
        global: Vec::new(),
    };
    limits_of(&alone, spec).err().map(Problem::Refused)
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
            // Every name another holds, so that its status names each (04 §6).
            let takes = |limits: &Result<Vec<Limit>, Problem>| -> Result<(), Vec<Problem>> {
                let limits = limits.as_ref().map_err(|problem| vec![problem.clone()])?;
                let held: Vec<Problem> = limits
                    .iter()
                    .filter_map(|limit| {
                        holder(&limit.name).map(|by| Problem::LimitNameHeld {
                            name: limit.name.clone(),
                            by,
                        })
                    })
                    .collect();
                if held.is_empty() { Ok(()) } else { Err(held) }
            };
            match takes(&policy.spec) {
                Ok(()) => Applied::Spec,
                Err(problems) => {
                    findings.extend(problems.into_iter().map(|problem| Finding {
                        object: policy.object.clone(),
                        problem,
                    }));
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

/// The policies whose spec is refused for what judging it alone cannot see, and why: a rule
/// it reaches would be under more limits than the data plane takes (04 §4). Each applies
/// its version last accepted, or nothing.
pub(crate) type Forced = BTreeMap<Object, Problem>;

/// Refuses in `forced`, for each rule of `config` under more limits than the data plane
/// takes, the specs of the policies of `applied` whose limits it is under that are changes,
/// not their version in `accepted` as it is, the youngest first, until it would not be.
/// Only a change is refused: a rule still over with none left, as a route's change brings
/// it under more policies, is left so, and the config is refused whole (Lucas). Whether it
/// refused any.
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
    let mut refused = false;
    for route in &config.routes {
        for (at, rule) in route.rules.iter().enumerate() {
            let count = rule.rate_limits.len();
            let Some(mut over) = count.checked_sub(MOST_LIMITS).filter(|over| *over > 0) else {
                continue;
            };
            // The changes whose limits the rule is under, the youngest first.
            for policy in applied.iter().rev().filter(|policy| !unchanged(policy)) {
                if over == 0 {
                    break;
                }
                let on = policy
                    .spec
                    .limits
                    .iter()
                    .flatten()
                    .filter(|limit| rule.rate_limits.contains(&limit.name))
                    .count();
                if on == 0 {
                    continue;
                }
                let object = Object {
                    kind: "RateLimitPolicy",
                    namespace: policy.namespace.clone(),
                    name: policy.name.clone(),
                };
                forced.entry(object).or_insert_with(|| {
                    refused = true;
                    Problem::LimitsTooMany {
                        route: route.name.clone(),
                        rule: at,
                        count,
                    }
                });
                over = over.saturating_sub(on);
            }
        }
    }
    refused
}

/// Why `target`, of a policy of `namespace`, is not in `cluster`, if it is not: the object
/// it names, or its `sectionName`, a listener of a Gateway or a ListenerSet, or a rule of a
/// route, by name.
fn not_there(cluster: &Cluster, namespace: &str, target: &Target) -> Option<String> {
    let Some(sections) = sections_of(cluster, namespace, target) else {
        return Some("it is not there".to_owned());
    };
    let section = target.section.as_deref()?;
    let of = if matches!(target.kind.as_str(), "HTTPRoute" | "GRPCRoute") {
        "rule"
    } else {
        "listener"
    };
    (!sections.contains(&Some(section))).then(|| format!("it has no {of} `{section}`"))
}

/// The names of the sections of the object `target`, of a policy of `namespace`, names in
/// `cluster`, a Gateway's or a ListenerSet's listeners or a route's rules (none for a rule
/// of no name, and none at all for an Ingress), if it is there.
fn sections_of<'a>(
    cluster: &'a Cluster,
    namespace: &str,
    target: &Target,
) -> Option<Vec<Option<&'a str>>> {
    let is = |metadata: &ObjectMeta| {
        crate::namespace(metadata) == namespace && metadata.name.as_deref() == Some(&target.name)
    };
    match (target.group.as_str(), target.kind.as_str()) {
        (GATEWAY_GROUP, "Gateway") => cluster
            .gateways
            .iter()
            .find(|gateway| is(&gateway.metadata))
            .map(|gateway| {
                let listeners = &gateway.spec.listeners;
                listeners.iter().map(|l| Some(l.name.as_str())).collect()
            }),
        (GATEWAY_GROUP, "ListenerSet") => cluster
            .listener_sets
            .iter()
            .find(|set| is(&set.metadata))
            .map(|set| {
                let listeners = &set.spec.listeners;
                listeners.iter().map(|l| Some(l.name.as_str())).collect()
            }),
        (GATEWAY_GROUP, "HTTPRoute") => cluster
            .http_routes
            .iter()
            .find(|route| is(&route.metadata))
            .map(|route| route.spec.rules.iter().map(|r| r.name.as_deref()).collect()),
        (GATEWAY_GROUP, "GRPCRoute") => cluster
            .grpc_routes
            .iter()
            .find(|route| is(&route.metadata))
            .map(|route| {
                let rules = route.spec.rules.iter().flatten();
                rules.map(|r| r.name.as_deref()).collect()
            }),
        (INGRESS_GROUP, "Ingress") => cluster
            .ingresses
            .iter()
            .find(|ingress| is(&ingress.metadata))
            .map(|_| Vec::new()),
        _ => None,
    }
}

/// Whether `target`, of a policy of `namespace`, forwards nothing, as the routes translated
/// say, `redirecting` the rules that redirect and `forwarding` the routes a rule of which
/// forwards: a rule of a route that redirects, or a route some rule of which redirects and
/// none forwards.
fn forwards_nothing(
    target: &Target,
    namespace: &str,
    redirecting: &BTreeSet<(Object, Option<String>)>,
    forwarding: &BTreeSet<Object>,
) -> bool {
    if !matches!(target.kind.as_str(), "HTTPRoute" | "GRPCRoute") {
        return false;
    }
    let is = |route: &Object| {
        route.kind == target.kind && route.namespace == namespace && route.name == target.name
    };
    match &target.section {
        Some(named) => redirecting
            .iter()
            .any(|(route, rule)| is(route) && rule.as_ref() == Some(named)),
        None => redirecting.iter().any(|(route, _)| is(route)) && !forwarding.iter().any(is),
    }
}

/// The most ancestors a policy's status holds, as its CRD has it.
const MOST_ANCESTORS: usize = 16;

/// Leaves out in `forced` each policy whose status, as `cluster` holds it, holds as many
/// ancestors as it can, none of them this controller's of an ancestor `translation` says the
/// policy reaches here: it may not add its own, so cannot be implemented here (GEP-713).
/// Whether it left out any.
pub(crate) fn leave_out_unimplementable(
    translation: &crate::Translation,
    cluster: &Cluster,
    forced: &mut Forced,
) -> bool {
    let mut left = false;
    for status in &translation.limit_policy_statuses {
        let Some(policy) = cluster
            .rate_limit_policies
            .iter()
            .find(|policy| Object::of("RateLimitPolicy", &policy.metadata) == status.policy)
        else {
            continue;
        };
        let held = policy
            .status
            .as_ref()
            .map(|status| status.ancestors.as_slice())
            .unwrap_or_default();
        let ours = held.iter().any(|ancestor| {
            let to = &ancestor.ancestor_ref;
            ancestor.controller_name == crate::GATEWAY_CONTROLLER
                && status.ancestors.iter().any(|reached| {
                    to.kind.as_deref().unwrap_or("Gateway") == reached.kind
                        && to.namespace.as_deref().unwrap_or(&status.policy.namespace)
                            == reached.namespace
                        && to.name == reached.name
                })
        });
        if ours || held.len() < MOST_ANCESTORS {
            continue;
        }
        let named: Vec<String> = status.ancestors.iter().map(ToString::to_string).collect();
        forced.entry(status.policy.clone()).or_insert_with(|| {
            left = true;
            Problem::LimitPolicyUnimplementable(format!(
                "its status holds {MOST_ANCESTORS} ancestors already, none of them {}, so it \
                 cannot be implemented there",
                named.join(" or ")
            ))
        });
    }
    left
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
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    group: String,
    kind: String,
    name: String,
    section: Option<String>,
}

/// A policy taken: its targets, the names of its limits and what it ignores.
#[derive(Debug)]
struct Taken {
    /// The policy.
    object: Object,
    namespace: String,
    targets: Vec<Target>,
    limits: Vec<String>,
    ignore: Ignore,
}

impl Target {
    /// Each of `spec`'s targets.
    fn of(spec: &RateLimitPolicySpec) -> Vec<Self> {
        spec.target_refs
            .iter()
            .map(|target| Self {
                group: target.group.clone(),
                kind: target.kind.clone(),
                name: target.name.clone(),
                section: target.section_name.clone(),
            })
            .collect()
    }

    /// The target as a finding says it, of a policy of `namespace`.
    fn said(&self, namespace: &str) -> String {
        match &self.section {
            Some(section) => format!("{} {namespace}/{} section {section}", self.kind, self.name),
            None => format!("{} {namespace}/{}", self.kind, self.name),
        }
    }
}

/// A policy as its status needs it: what it is, and the targets of its spec and of the
/// version that applies, where that is another.
#[derive(Debug)]
struct Stated {
    object: Object,
    generation: i64,
    /// Its spec's targets.
    targets: Vec<Target>,
    /// The targets of its version last accepted, where that applies in its spec's place.
    applied: Vec<Target>,
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
    /// Whether the data plane limits: where it does not, no policy is judged.
    limiting: bool,
    /// Every policy, in the order they rank, as its status needs it.
    stated: Vec<Stated>,
    /// The rules that redirect, by route and name (none for a rule of no name), and the
    /// routes a rule of which forwards, as their routes are translated: a rule that
    /// redirects is under no limits, so that a policy targeting one, or a route none of whose
    /// rules forwards, says so.
    redirecting: RefCell<BTreeSet<(Object, Option<String>)>>,
    forwarding: RefCell<BTreeSet<Object>>,
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
        let mut policies: Vec<&RateLimitPolicy> = cluster.rate_limit_policies.iter().collect();
        policies.sort_by_key(|policy| {
            let metadata = &policy.metadata;
            (
                metadata.creation_timestamp.clone(),
                namespace(metadata).to_owned(),
                metadata.name.clone(),
            )
        });
        let Some(limiting) = &plane.rate_limiting else {
            // Nothing is judged where nothing limits: each policy keeps what it held.
            return Self {
                stated: policies
                    .iter()
                    .map(|policy| Stated {
                        object: Object::of("RateLimitPolicy", &policy.metadata),
                        generation: policy.metadata.generation.unwrap_or_default(),
                        targets: Target::of(&policy.spec),
                        applied: Vec::new(),
                    })
                    .collect(),
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
                let judged = |spec: &RateLimitPolicySpec| {
                    limits_of(limiting, spec).map_err(Problem::Refused)
                };
                Judged {
                    policy,
                    spec: forced
                        .get(&object)
                        .map_or_else(|| judged(&policy.spec), |why| Err(why.clone())),
                    // A policy left out applies no version of itself.
                    last: last
                        .filter(|last| last.spec != policy.spec)
                        .filter(|_| {
                            !matches!(
                                forced.get(&object),
                                Some(Problem::LimitPolicyUnimplementable(_))
                            )
                        })
                        .map(|last| (last, judged(&last.spec))),
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
        let mut stated = Vec::with_capacity(judged.len());
        for (policy, applied) in judged.iter().zip(applied) {
            stated.push(Stated {
                object: policy.object.clone(),
                generation: policy.policy.metadata.generation.unwrap_or_default(),
                targets: Target::of(&policy.policy.spec),
                applied: match (applied, &policy.last) {
                    (Applied::Last, Some((last, _))) => Target::of(&last.spec),
                    _ => Vec::new(),
                },
            });
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
                object: policy.object.clone(),
                namespace: policy.object.namespace.clone(),
                targets: Target::of(spec),
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
            limiting: true,
            stated,
            redirecting: RefCell::default(),
            forwarding: RefCell::default(),
        }
    }

    /// Says on each object of the Gateway API a policy that applies targets, as GEP-713
    /// has it, that it does: `RateLimitPolicyAffected` on the Gateway and on its listeners,
    /// `gateway`, on each ListenerSet and its listeners, and on each parent of the data
    /// plane's of each route of `routes` a policy targets, or a rule of (04 §6): only
    /// where it limits something, its rule there in `cluster` and forwarding, and its route
    /// forwarding. The policies named, oldest first. Once every route is translated.
    pub(crate) fn mark_affected(
        &self,
        cluster: &Cluster,
        gateway: Option<&mut GatewayStatus>,
        routes: &mut [RouteStatus],
    ) {
        let redirecting = self.redirecting.borrow();
        let forwarding = self.forwarding.borrow();
        let by = |(group, kind): (&str, &str), object: &Object, section: Option<&str>| {
            let names: Vec<String> = self
                .taken
                .iter()
                .filter(|taken| {
                    taken.targets((group, kind), &object.namespace, &object.name, section)
                })
                .map(|taken| taken.object.to_string())
                .collect();
            (!names.is_empty()).then(|| {
                Condition::new(
                    AFFECTED,
                    true,
                    "Accepted",
                    format!("affected by {}", names.join(", ")),
                )
            })
        };
        if let (Some(status), Some(object)) = (gateway, &self.gateway) {
            let of = (GATEWAY_GROUP, "Gateway");
            status.conditions.extend(by(of, object, None));
            for listener in &mut status.listeners {
                listener
                    .conditions
                    .extend(by(of, object, Some(&listener.name)));
            }
            for set in &mut status.listener_sets {
                let of = (GATEWAY_GROUP, "ListenerSet");
                set.conditions.extend(by(of, &set.set, None));
                for listener in &mut set.listeners {
                    listener
                        .conditions
                        .extend(by(of, &set.set, Some(&listener.name)));
                }
            }
        }
        for route in routes {
            if !matches!(route.route.kind, "HTTPRoute" | "GRPCRoute") {
                continue;
            }
            // A rule has no status of its own: a policy on one says so on its route.
            let names: Vec<String> = self
                .taken
                .iter()
                .filter(|taken| {
                    taken.namespace == route.route.namespace
                        && taken.targets.iter().any(|target| {
                            target.group == GATEWAY_GROUP
                                && target.kind == route.route.kind
                                && target.name == route.route.name
                                && not_there(cluster, &taken.namespace, target).is_none()
                                && !forwards_nothing(
                                    target,
                                    &taken.namespace,
                                    &redirecting,
                                    &forwarding,
                                )
                        })
                })
                .map(|taken| taken.object.to_string())
                .collect();
            if names.is_empty() {
                continue;
            }
            let affected = Condition::new(
                AFFECTED,
                true,
                "Accepted",
                format!("affected by {}", names.join(", ")),
            );
            for parent in &mut route.parents {
                let accepted = parent
                    .conditions
                    .iter()
                    .any(|c| c.kind == "Accepted" && c.status);
                if accepted {
                    parent.conditions.push(affected.clone());
                }
            }
        }
    }

    /// Notes that a rule of `route`, named `rule`, redirects, as its route is translated.
    pub(crate) fn redirects(&self, route: &Object, rule: Option<&str>) {
        self.redirecting
            .borrow_mut()
            .insert((route.clone(), rule.map(str::to_owned)));
    }

    /// Notes that a rule of `route` forwards, as its route is translated.
    pub(crate) fn forwards(&self, route: &Object) {
        self.forwarding.borrow_mut().insert(route.clone());
    }

    /// Says in `report`, of each policy judged, each target of its spec, or its
    /// `sectionName`, that `cluster` does not hold, and each rule it targets that redirects,
    /// or route none of whose rules forwards: the policy applies nothing there (04 §4); and
    /// a policy none of whose targets is there. Once every route is translated.
    pub(crate) fn say_targets(&self, cluster: &Cluster, report: &mut Vec<Finding>) {
        if !self.limiting {
            return;
        }
        let redirecting = self.redirecting.borrow();
        let forwarding = self.forwarding.borrow();
        for stated in &self.stated {
            let namespace = stated.object.namespace.as_str();
            for target in &stated.targets {
                let said = target.said(namespace);
                let problem = match not_there(cluster, namespace, target) {
                    Some(why) => Problem::LimitTargetNotFound { target: said, why },
                    None if forwards_nothing(target, namespace, &redirecting, &forwarding) => {
                        Problem::LimitNotForwarding(said)
                    }
                    None => continue,
                };
                report.push(Finding {
                    object: stated.object.clone(),
                    problem,
                });
            }
            // No object it names there at all is likely a mistake, and no status can say it:
            // reaching nothing, it has none (Lucas). A section not there is said on the
            // ancestor its object reaches.
            let none_there = stated
                .targets
                .iter()
                .all(|target| sections_of(cluster, namespace, target).is_none());
            if !stated.targets.is_empty() && none_there {
                report.push(Finding {
                    object: stated.object.clone(),
                    problem: Problem::LimitTargetsNoneThere,
                });
            }
        }
    }

    /// What each policy's status is to say of the ancestors this data plane serves, as
    /// `report` says of it: those its targets reach, of its spec and of the version that
    /// applies, its Gateway, `gateway`, where a target is that Gateway, a listener of it, a
    /// ListenerSet attached to it or a route of `routes` attached to it, or a rule of one;
    /// and each Ingress of `claimed`, those of the data plane's class, that it targets.
    /// None for a policy that reaches none.
    pub(crate) fn statuses(
        &self,
        gateway: Option<&GatewayOutcome>,
        routes: &[RouteOutcome],
        claimed: &[Object],
        report: &[Finding],
    ) -> Vec<LimitPolicyStatus> {
        self.stated
            .iter()
            .filter_map(|stated| {
                let namespace = stated.object.namespace.as_str();
                let mut ancestors: Vec<Object> = Vec::new();
                for target in stated.targets.iter().chain(&stated.applied) {
                    let is = |object: &Object| {
                        object.namespace == namespace
                            && object.name == target.name
                            && object.kind == target.kind
                    };
                    let reached = match (target.group.as_str(), target.kind.as_str()) {
                        (GATEWAY_GROUP, "Gateway") => gateway
                            .filter(|gateway| is(&gateway.gateway))
                            .map(|gateway| gateway.gateway.clone()),
                        (GATEWAY_GROUP, "ListenerSet") => gateway
                            .filter(|gateway| {
                                gateway.sets.iter().any(|set| set.attached && is(&set.set))
                            })
                            .map(|gateway| gateway.gateway.clone()),
                        (GATEWAY_GROUP, "HTTPRoute" | "GRPCRoute") => gateway
                            .filter(|_| {
                                routes.iter().any(|route| {
                                    is(&route.route)
                                        && route.parents.iter().any(|p| p.attached.is_ok())
                                })
                            })
                            .map(|gateway| gateway.gateway.clone()),
                        (INGRESS_GROUP, "Ingress") => claimed.iter().find(|i| is(i)).cloned(),
                        _ => None,
                    };
                    if let Some(reached) = reached
                        && !ancestors.contains(&reached)
                    {
                        ancestors.push(reached);
                    }
                }
                if ancestors.is_empty() {
                    return None;
                }
                let said: Vec<&Problem> = report
                    .iter()
                    .filter(|finding| finding.object == stated.object)
                    .map(|finding| &finding.problem)
                    .collect();
                // One left out has no room in its status to say so: the report says it.
                if said
                    .iter()
                    .any(|problem| matches!(problem, Problem::LimitPolicyUnimplementable(_)))
                {
                    return None;
                }
                Some(LimitPolicyStatus {
                    policy: stated.object.clone(),
                    generation: stated.generation,
                    ancestors,
                    conditions: vec![self.accepted(&said)],
                })
            })
            .collect()
    }

    /// A policy's `Accepted`, as `said`, the report's findings of it, has it: false
    /// `NotLimiting` where the data plane does not limit, else `Invalid`, else `Conflicted`, naming each limit whose name another holds but never
    /// what holds it, else `TooManyLimits`, else `TargetNotFound`, else `NotForwarding`, the
    /// refusals saying which generation applies in the spec's place; else true (04 §6).
    fn accepted(&self, said: &[&Problem]) -> Condition {
        if !self.limiting {
            return Condition::new(
                "Accepted",
                false,
                "NotLimiting",
                "its DataPlane has no rateLimiting".to_owned(),
            );
        }
        let instead = said.iter().find_map(|problem| match problem {
            Problem::LastAcceptedApplies(generation) => Some(*generation),
            _ => None,
        });
        let refused = |reason: &'static str, whys: Vec<String>| {
            let mut message = whys.join("; ");
            if let Some(generation) = instead {
                let generation = generation.map_or_else(|| "unknown".to_owned(), |g| g.to_string());
                message.push_str(&format!("; generation {generation} applies instead"));
            }
            Condition::new("Accepted", false, reason, message)
        };
        let of = |pick: fn(&Problem) -> Option<String>| -> Vec<String> {
            said.iter().filter_map(|problem| pick(problem)).collect()
        };
        let invalid = of(|problem| match problem {
            Problem::Refused(_) => Some(problem.to_string()),
            _ => None,
        });
        if !invalid.is_empty() {
            return refused("Invalid", invalid);
        }
        let held = of(|problem| match problem {
            Problem::LimitNameHeld { name, by: None } => Some(format!(
                "the name of its limit `{name}` is held by the DataPlane's global limits"
            )),
            Problem::LimitNameHeld { name, by: Some(_) } => Some(format!(
                "the name of its limit `{name}` is held by another RateLimitPolicy"
            )),
            _ => None,
        });
        if !held.is_empty() {
            return refused("Conflicted", held);
        }
        let too_many = of(|problem| match problem {
            Problem::LimitsTooMany { .. } => Some(problem.to_string()),
            _ => None,
        });
        if !too_many.is_empty() {
            return refused("TooManyLimits", too_many);
        }
        let not_found = of(|problem| match problem {
            Problem::LimitTargetNotFound { .. } => Some(problem.to_string()),
            _ => None,
        });
        if !not_found.is_empty() {
            return Condition::new("Accepted", false, "TargetNotFound", not_found.join("; "));
        }
        let redirects = of(|problem| match problem {
            Problem::LimitNotForwarding(_) => Some(problem.to_string()),
            _ => None,
        });
        if !redirects.is_empty() {
            return Condition::new("Accepted", false, "NotForwarding", redirects.join("; "));
        }
        Condition::new("Accepted", true, "Accepted", String::new())
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
    /// limits the Ingress's rule is under, the report, and whether the data plane takes the
    /// config.
    fn on_the_rule(
        policies: &[RateLimitPolicy],
        accepted: &[&RateLimitPolicy],
    ) -> (Vec<(String, i64)>, usize, Vec<String>, bool) {
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
        (
            translation
                .rate_limit_policies
                .iter()
                .map(|kept| (kept.name.clone(), kept.generation.unwrap()))
                .collect(),
            rules[0],
            translation.report.iter().map(ToString::to_string).collect(),
            compile(&translation.config).is_ok(),
        )
    }

    /// The note on a rule of `count` limits on the Ingress `shop/web`.
    fn noted(count: usize) -> String {
        format!(
            "Ingress shop/web: rule 0 of route ingress/shop/web/shop.example is under {count} \
             limits; past 64 the config is refused"
        )
    }

    /// The refusal of `policy`'s change, as it would put the Ingress's rule under `count`.
    fn too_many(policy: &str, count: usize) -> String {
        format!(
            "RateLimitPolicy shop/{policy}: the data plane would refuse it: rule 0 of route \
             ingress/shop/web/shop.example would be under {count} limits, more than the 64 a \
             rule takes"
        )
    }

    /// A new policy that would put a rule over the data plane's most limits is refused,
    /// applying nothing; the policy there before keeps applying, and the config is served.
    #[test]
    fn a_new_policy_putting_a_rule_over_the_most_limits_is_refused() {
        let big = on_ingress("big", 1, 1, 60);
        let (applied, under, report, taken) =
            on_the_rule(&[big.clone(), on_ingress("extra", 2, 1, 5)], &[&big]);
        assert_eq!(applied, [("big".to_owned(), 1)]);
        assert_eq!(under, 61);
        assert_eq!(report, [too_many("extra", 66), noted(61)]);
        assert!(taken);
    }

    /// An edit that would put a rule over is refused, its version last accepted applying;
    /// so is an older policy's, against a younger one that applied as it is.
    #[test]
    fn an_edit_putting_a_rule_over_the_most_limits_applies_its_last_accepted() {
        let older = on_ingress("older", 1, 1, 2);
        let younger = on_ingress("younger", 2, 1, 60);
        let now = [on_ingress("older", 1, 2, 5), younger.clone()];
        let (applied, under, report, taken) = on_the_rule(&now, &[&older, &younger]);
        assert_eq!(
            applied,
            [("older".to_owned(), 1), ("younger".to_owned(), 1)]
        );
        assert_eq!(under, 63);
        assert_eq!(
            report,
            [
                too_many("older", 66),
                "RateLimitPolicy shop/older: its version last accepted applies instead \
                 (generation 1)"
                    .to_owned(),
                noted(63)
            ]
        );
        assert!(taken);
    }

    /// Policies that all apply as they were accepted, and put a rule over together, as a
    /// change to a route can have them: none is refused, and the data plane refuses the
    /// config whole, its last staying (Lucas). So too where a change refused leaves its
    /// version last accepted putting the rule over.
    #[test]
    fn policies_unchanged_putting_a_rule_over_have_the_config_refused() {
        let first = on_ingress("first", 1, 1, 40);
        let second = on_ingress("second", 2, 1, 40);
        let (applied, under, report, taken) =
            on_the_rule(&[first.clone(), second.clone()], &[&first, &second]);
        assert_eq!(applied, [("first".to_owned(), 1), ("second".to_owned(), 1)]);
        assert_eq!(under, 81);
        assert_eq!(report, Vec::<String>::new());
        assert!(!taken, "the config taken");
        let edited = on_ingress("second", 2, 2, 45);
        let (applied, under, report, taken) =
            on_the_rule(&[first.clone(), edited], &[&first, &second]);
        assert_eq!(applied, [("first".to_owned(), 1), ("second".to_owned(), 1)]);
        assert_eq!(under, 81);
        assert_eq!(
            report,
            [
                too_many("second", 86),
                "RateLimitPolicy shop/second: its version last accepted applies instead \
                 (generation 1)"
                    .to_owned()
            ]
        );
        assert!(!taken, "the config taken");
    }

    /// A change refused for the most limits says `TooManyLimits`, naming the rule and which
    /// generation applies in its place, of the Ingress it targets; the policy it leaves
    /// applying is accepted.
    #[test]
    fn a_change_refused_for_the_most_limits_says_too_many_limits_of_its_ingress() {
        let older = on_ingress("older", 1, 1, 2);
        let younger = on_ingress("younger", 2, 1, 60);
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
            rate_limit_policies: vec![on_ingress("older", 1, 2, 5), younger.clone()],
            ..Cluster::default()
        };
        let accepted = [AcceptedPolicy::of(&older), AcceptedPolicy::of(&younger)];
        let translation = crate::translate_from(&plane, &cluster, &accepted);
        let said: Vec<String> = translation
            .limit_policy_statuses
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            said,
            [
                format!(
                    "RateLimitPolicy shop/older, generation 2, of Ingress shop/web: \
                     Accepted=False/TooManyLimits ({}; generation 1 applies instead)",
                    too_many("older", 66).trim_start_matches("RateLimitPolicy shop/older: ")
                ),
                "RateLimitPolicy shop/younger, generation 1, of Ingress shop/web: \
                 Accepted=True/Accepted"
                    .to_owned()
            ]
        );
    }

    /// A refused edit that moved the policy to a target not there says of the ancestor its
    /// version last accepted still applies to, the generation applying named.
    #[test]
    fn a_refused_edit_says_of_the_ancestors_its_last_accepted_reaches() {
        let last = on_ingress("moved", 1, 1, 1);
        let mut edited = on_ingress("moved", 1, 2, 1);
        edited.spec.target_refs[0].name = "elsewhere".to_owned();
        edited.spec.limits.as_mut().unwrap()[0].key = "Header".to_owned();
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
            rate_limit_policies: vec![edited],
            ..Cluster::default()
        };
        let translation = crate::translate_from(&plane, &cluster, &[AcceptedPolicy::of(&last)]);
        let said: Vec<String> = translation
            .limit_policy_statuses
            .iter()
            .map(ToString::to_string)
            .collect();
        assert_eq!(
            said,
            [
                "RateLimitPolicy shop/moved, generation 2, of Ingress shop/web: \
                 Accepted=False/Invalid (the data plane would refuse it: limits[moved-0].key: \
                 `Header` is not Client; generation 1 applies instead)"
            ]
        );
    }

    /// A policy whose status holds as many ancestors as it can, none of them this data
    /// plane's, is left out whole: its version last accepted applies no more than its spec,
    /// and it holds no name; one with room for its own entry applies, and so does one whose
    /// full list holds this data plane's entry already.
    #[test]
    fn a_policy_with_a_full_ancestor_list_is_left_out_whole() {
        let accepted = on_ingress("full", 1, 1, 1);
        let mut edited = on_ingress("full", 1, 2, 2);
        let ancestors: Vec<String> = (0..16)
            .map(|n| {
                format!(
                    "{{ ancestorRef: {{ name: gateway-{n}, namespace: edge }}, controllerName: example.com/other, conditions: [] }}"
                )
            })
            .collect();
        edited.status = Some(
            serde_saphyr::from_str(&format!("ancestors: [{}]", ancestors.join(", "))).unwrap(),
        );
        let (applied, under, report, _) = on_the_rule(&[edited.clone()], &[&accepted]);
        assert_eq!(applied, []);
        assert_eq!(under, 1, "the DataPlane's alone");
        assert_eq!(
            report,
            [
                "RateLimitPolicy shop/full: left out: its status holds 16 ancestors already, \
                 none of them Ingress shop/web, so it cannot be implemented there"
            ]
        );
        // With room for one more, its spec applies.
        if let Some(status) = edited.status.as_mut() {
            status.ancestors.pop();
        }
        let (applied, under, _, _) = on_the_rule(&[edited.clone()], &[&accepted]);
        assert_eq!(applied, [("full".to_owned(), 2)]);
        assert_eq!(under, 3);
        // Full again, one of them this controller's of the Ingress.
        if let Some(status) = edited.status.as_mut() {
            status.ancestors.push(
                serde_saphyr::from_str(
                    "{ ancestorRef: { group: networking.k8s.io, kind: Ingress, name: web },                      controllerName: edgerush.dev/gateway-controller, conditions: [] }",
                )
                .unwrap(),
            );
        }
        let (applied, _, _, _) = on_the_rule(&[edited], &[&accepted]);
        assert_eq!(applied, [("full".to_owned(), 2)], "its own entry is there");
    }

    /// A target not there, and one forwarding nothing, are notes, not faults: the policy
    /// applies to it once it is there and forwards, so files applied in any order validate
    /// (Lucas); none there at all is a fault.
    #[test]
    fn a_target_not_there_or_forwarding_nothing_is_a_note() {
        let not_there = Problem::LimitTargetNotFound {
            target: "HTTPRoute shop/gone".to_owned(),
            why: "it is not there".to_owned(),
        };
        assert!(not_there.is_note());
        assert!(Problem::LimitNotForwarding("HTTPRoute shop/moved".to_owned()).is_note());
        // But none of a policy's targets there is a fault, likely a mistake (Lucas).
        assert!(!Problem::LimitTargetsNoneThere.is_note());
    }

    /// A rule past half the most limits is noted, so that one coming to the most is seen;
    /// one at half is not.
    #[test]
    fn a_rule_past_half_the_most_limits_is_noted() {
        let half = on_ingress("half", 1, 1, 31);
        let (_, under, report, taken) = on_the_rule(std::slice::from_ref(&half), &[&half]);
        assert_eq!(under, 32);
        assert_eq!(report, Vec::<String>::new());
        assert!(taken);
        let past = on_ingress("past", 1, 1, 32);
        let (_, under, report, taken) = on_the_rule(std::slice::from_ref(&past), &[&past]);
        assert_eq!(under, 33);
        assert_eq!(report, [noted(33)]);
        assert!(taken);
    }

    /// Policy `alone` of namespace `shop`, on route `a`, of the spec's fields `fields`.
    fn alone(fields: &str) -> RateLimitPolicy {
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: alone, namespace: shop, uid: alone-uid, generation: 1 }}
spec:
  targetRefs: [{{ group: gateway.networking.k8s.io, kind: HTTPRoute, name: a }}]
  {fields}
"
        ))
        .unwrap()
    }

    /// A policy alone is admitted where its limits compile, whatever its limits' models,
    /// whatever it ignores, and whatever names it takes: a name the DataPlane's `global`
    /// holds, as `platform`, is for the translation to say, as the cluster has it.
    #[test]
    fn a_policy_alone_whose_limits_compile_is_not_refused() {
        for fields in [
            "limits: [{ name: b, key: Client, model: { bucket: { rate: 10/s, burst: 20 } } }]",
            "limits: [{ name: f, key: Client, model: { fixedWindow: { limit: 10, window: 1m } } }]",
            "limits: [{ name: r, key: Client, model: { rollingWindow: { limit: 10, window: 1h } } }]",
            "limits: [{ name: m, key: Client, match: { methods: [POST] }, \
             model: { bucket: { rate: 1/s, burst: 1 } } }]",
            "limits: [{ name: platform, key: Client, model: { bucket: { rate: 1/s, burst: 1 } } }]",
            "ignore: [platform]",
            "ignore: [\"*\"]",
        ] {
            assert_eq!(policy_refused_alone(&alone(fields).spec), None, "{fields}");
        }
    }

    /// A policy alone whose limits do not compile is refused, saying why as its status
    /// says it, whichever field it is.
    #[test]
    fn a_policy_alone_is_refused_as_its_status_says() {
        for (fields, why) in [
            (
                "limits: [{ name: bad, key: Client, model: { bucket: { rate: 1/s, burst: 0 } } }]",
                "the data plane would refuse it: limits[bad].model.bucket.burst: 0 is out of range",
            ),
            (
                "limits: [{ name: bad, key: Client, model: { bucket: { rate: 10/fortnight, burst: 1 } } }]",
                "the data plane would refuse it: limit `bad`: `model.bucket.rate`: `10/fortnight` is \
                 not a rate: a number of at least 1, `/`, and `s`, `min`, `h` or `d`",
            ),
            (
                "limits: [{ name: bad, key: Header, model: { bucket: { rate: 1/s, burst: 1 } } }]",
                "the data plane would refuse it: limits[bad].key: `Header` is not Client",
            ),
            // What the CRD's schema lets by.
            (
                "limits: [{ name: bad, key: Client, model: { fixedWindow: { limit: 1, window: 300000d } } }]",
                "the data plane would refuse it: limit `bad`: `model.fixed_window.window`: \
                 `300000d` is not a window: a whole number of at least 1 and `ms`, `s`, `m`, `h` \
                 or `d`, under 213,503 days",
            ),
        ] {
            let policy = alone(fields);
            let refused = policy_refused_alone(&policy.spec).map(|problem| problem.to_string());
            assert_eq!(refused.as_deref(), Some(why), "{fields}");
            // As the translation says it of the policy in a data plane limiting.
            let (_, _, report) = judged(PLANE, &[policy], &[]);
            assert_eq!(report, [format!("RateLimitPolicy shop/alone: {why}")]);
        }
    }
}
