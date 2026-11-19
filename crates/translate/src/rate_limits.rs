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
//! Every policy is taken as valid but for what the data plane could not run: a limit it
//! would refuse, or a name another limit already holds, the DataPlane's or an older
//! policy's, which would make two counts one. Such a policy is left out whole, and said.

use crate::gateway::GatewayOutcome;
use crate::http_route::Attachment;
use crate::{Cluster, Finding, Object, Plane, Problem, namespace};
use edgerush_config::{Bucket, Limit, LimitMatch, Model, Rolling, Window};
use edgerush_planes::data_plane::DataPlaneRateLimitingGlobal;
use edgerush_planes::rate_limit_policy::{RateLimitPolicy, RateLimitPolicyLimits};
use std::collections::BTreeSet;

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
}

impl RateLimits {
    /// The DataPlane's limits and every RateLimitPolicy's, oldest first, then by namespace
    /// and name, each taken unless the data plane could not run it, which is said in
    /// `report`.
    pub(crate) fn of(
        plane: &Plane,
        cluster: &Cluster,
        gateway: Option<&GatewayOutcome>,
        report: &mut Vec<Finding>,
    ) -> Self {
        let Some(limiting) = &plane.rate_limiting else {
            return Self::default();
        };
        let mut limits = limiting.global.clone();
        let mut held: Vec<(String, Option<Object>)> = limits
            .iter()
            .map(|limit| (limit.name.clone(), None))
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
        let mut taken = Vec::new();
        'policies: for policy in policies {
            let object = Object::of("RateLimitPolicy", &policy.metadata);
            let spec = &policy.spec;
            let mut own = Vec::new();
            for limit in spec.limits.iter().flatten() {
                let read = match policy_limit(limit) {
                    Ok(read) => read,
                    Err((field, why)) => {
                        report.push(Finding {
                            object,
                            problem: Problem::Refused(format!(
                                "limits[{}].{field}: {why}",
                                limit.name
                            )),
                        });
                        continue 'policies;
                    }
                };
                if let Some((_, by)) = held.iter().find(|(name, _)| *name == read.name) {
                    report.push(Finding {
                        problem: Problem::LimitNameHeld {
                            name: read.name,
                            by: by.clone(),
                        },
                        object,
                    });
                    continue 'policies;
                }
                own.push(read);
            }
            held.extend(
                own.iter()
                    .map(|limit| (limit.name.clone(), Some(object.clone()))),
            );
            let ignore = spec.ignore.as_deref().unwrap_or_default();
            taken.push(Taken {
                namespace: object.namespace.clone(),
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
            limits.extend(own);
        }
        Self {
            global: limiting
                .global
                .iter()
                .map(|limit| limit.name.clone())
                .collect(),
            gateway: gateway.map(|gateway| gateway.gateway.clone()),
            taken,
            limits,
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
