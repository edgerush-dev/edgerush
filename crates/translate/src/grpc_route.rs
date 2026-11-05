//! GRPCRoutes attached to the data plane's Gateway ([04 §2](../../../docs/04-control-plane.md)),
//! attached as HTTPRoutes are (`http_route`): each rule's matches, the method a call names
//! and the headers it carries, every call where it has none; its filters, as an HTTPRoute's
//! of their type; its backends, spoken to in HTTP/2; a rule with what translation cannot
//! take answering `UNAVAILABLE` for every call it matches, one whose matches cannot be built
//! left out.

use crate::gateway::GatewayOutcome;
use crate::http_route::{
    Filtered, Reference, RouteOutcome, Translated, Written, answering, backends, config_routes,
    filtered, header_changes, mirrored, no_extension, no_settings, not_read, predicate,
};
use crate::{Cluster, Plane, namespace};
use edgerush_config::{Filter, Forward, GrpcMethod, Match, PathMatch, Rule, Upstream};
use edgerush_gateway_api::grpc_route::{
    GRPCRoute, GRPCRouteRules, GRPCRouteRulesFilters, GRPCRouteRulesMatchesMethod,
};
use std::collections::BTreeMap;

/// The rules of `route`, attached as `outcome` says, translated for the listeners of
/// `gateway` that serve each attachment, as an HTTPRoute's are: `refused` says why the data
/// plane would refuse a rule compiled alone with its upstreams, if it would, so that such a
/// rule is left out by itself; a rule with what translation cannot take answers
/// `UNAVAILABLE` for every call it matches, never served with part of it ignored; one whose
/// matches cannot be built is left out (04 §2).
pub(crate) fn translated(
    route: &GRPCRoute,
    outcome: &RouteOutcome,
    gateway: &GatewayOutcome,
    plane: &Plane,
    cluster: &Cluster,
    refused: impl Fn(&Rule, &BTreeMap<String, Upstream>) -> Option<String>,
) -> Translated {
    let mut translated = Translated::default();
    let route_namespace = namespace(&route.metadata);
    let mut rules = Vec::new();
    for (at, rule) in route.spec.rules.iter().flatten().enumerate() {
        let matches = match matches(rule) {
            Ok(matches) => matches,
            Err(why) => {
                translated.left_out.push((at, why));
                continue;
            }
        };
        let mut upstreams = BTreeMap::new();
        let mut invalid = Vec::new();
        let mut left_off = Vec::new();
        let taken = match unsupported(rule) {
            Some(why) => Err(why),
            None => filtered(
                rule.filters.iter().flatten().map(written),
                ("GRPCRoute", route_namespace),
                plane,
                cluster,
                &mut upstreams,
                &mut left_off,
            ),
        };
        if taken.is_err() {
            upstreams.clear();
            left_off.clear();
        }
        // A GRPCRoute has no redirect, so its filters are all done to the call.
        let compiled = match &taken {
            Err(why) => answering(matches.clone(), why),
            Ok(Filtered { filters, .. }) => Rule {
                matches: matches.clone(),
                filters: filters.clone(),
                forward: Some(Forward {
                    backends: backends(
                        rule.backend_refs.iter().flatten().map(|written| {
                            let reference = Reference {
                                group: &written.group,
                                kind: &written.kind,
                                name: &written.name,
                                namespace: written.namespace.as_deref(),
                                port: written.port,
                            };
                            (written.weight, reference)
                        }),
                        ("GRPCRoute", route_namespace),
                        plane,
                        cluster,
                        (&mut upstreams, &mut invalid),
                    ),
                    timeouts: None,
                    retry: None,
                }),
                redirect: None,
                paths: None,
                rate_limits: Vec::new(),
            },
        };
        if let Some(why) = refused(&compiled, &upstreams) {
            // As an HTTPRoute's: refused for what it does, its matches answer in its place;
            // refused for its matches, it is left out.
            let standing = answering(matches, &why);
            if refused(&standing, &BTreeMap::new()).is_some() {
                translated.left_out.push((at, why));
            } else {
                translated.answered.push((at, why));
                rules.push(standing);
            }
            continue;
        }
        match taken {
            Err(why) => translated.answered.push((at, why)),
            Ok(_) => {
                translated
                    .invalid
                    .extend(invalid.into_iter().map(|why| (at, why)));
                translated
                    .mirrors_left_off
                    .extend(left_off.into_iter().map(|why| (at, why)));
            }
        }
        translated.upstreams.extend(upstreams);
        rules.push(compiled);
    }
    if !rules.is_empty() {
        translated.routes = config_routes("grpcroute", outcome, gateway, &rules);
    }
    translated
}

/// A rule's matches as the config has them, or why they cannot be built: a match of a type
/// translation does not read, or a method match that names neither service nor method. A
/// rule of no matches matches every call, as a match of no method matches every method.
fn matches(rule: &GRPCRouteRules) -> Result<Vec<Match>, String> {
    let every = || Some(PathMatch::Prefix("/".to_owned()));
    let written = rule.matches.as_deref().unwrap_or_default();
    if written.is_empty() {
        return Ok(vec![Match {
            path: every(),
            grpc: None,
            method: None,
            headers: Vec::new(),
            query: Vec::new(),
        }]);
    }
    written
        .iter()
        .map(|written| {
            let (path, grpc) = match &written.method {
                None => (every(), None),
                Some(method) => method_match(method)?,
            };
            let headers = written
                .headers
                .iter()
                .flatten()
                .map(|h| predicate(&h.name, &h.r#type, &h.value, "header"))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Match {
                path,
                grpc,
                method: None,
                headers,
                query: Vec::new(),
            })
        })
        .collect()
}

/// A method match as the config has it: `Exact`, the config's gRPC match of either or both,
/// a service's leading `.` dropped, as a call's path never has it; `RegularExpression`, a
/// regex path `/<service>/<method>`, `[^/]+` for a part left out (04 §2). Or why not.
fn method_match(
    written: &GRPCRouteRulesMatchesMethod,
) -> Result<(Option<PathMatch>, Option<GrpcMethod>), String> {
    // Empty is left out, as the spec has it.
    let named = |part: &Option<String>| part.clone().filter(|part| !part.is_empty());
    let (service, method) = (named(&written.service), named(&written.method));
    if service.is_none() && method.is_none() {
        return Err("a method match names neither a service nor a method".to_owned());
    }
    match written.r#type.as_str() {
        "Exact" => Ok((
            None,
            Some(GrpcMethod {
                service: service.map(|service| match service.strip_prefix('.') {
                    Some(stripped) => stripped.to_owned(),
                    None => service,
                }),
                method,
            }),
        )),
        "RegularExpression" => {
            let part = |part: Option<String>| part.unwrap_or_else(|| "[^/]+".to_owned());
            let pattern = format!("/(?:{})/(?:{})", part(service), part(method));
            Ok((Some(PathMatch::Regex(pattern)), None))
        }
        other => Err(format!(
            "a method match of type `{other}` is not one translation reads"
        )),
    }
}

/// A GRPCRoute's filter as translation reads it, as an HTTPRoute's of its type is, or why
/// translation cannot take it.
fn written(filter: &GRPCRouteRulesFilters) -> Result<Written<'_>, String> {
    let missing = || no_settings(&filter.r#type);
    match filter.r#type.as_str() {
        "RequestHeaderModifier" => {
            let changes = filter
                .request_header_modifier
                .as_ref()
                .ok_or_else(missing)?;
            Ok(Written::Done(Filter::RequestHeaderModifier(
                header_changes(
                    changes.set.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.add.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.remove.iter().flatten(),
                ),
            )))
        }
        "ResponseHeaderModifier" => {
            let changes = filter
                .response_header_modifier
                .as_ref()
                .ok_or_else(missing)?;
            Ok(Written::Done(Filter::ResponseHeaderModifier(
                header_changes(
                    changes.set.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.add.iter().flatten().map(|h| (&h.name, &h.value)),
                    changes.remove.iter().flatten(),
                ),
            )))
        }
        "RequestMirror" => {
            let written = filter.request_mirror.as_ref().ok_or_else(missing)?;
            let fraction = mirrored(
                written.percent,
                written
                    .fraction
                    .as_ref()
                    .map(|fraction| (fraction.numerator, fraction.denominator)),
            )?;
            let to = &written.backend_ref;
            let reference = Reference {
                group: &to.group,
                kind: &to.kind,
                name: &to.name,
                namespace: to.namespace.as_deref(),
                port: to.port,
            };
            Ok(Written::Mirror(reference, fraction))
        }
        "ExtensionRef" => {
            let extension = filter.extension_ref.as_ref().ok_or_else(missing)?;
            Err(no_extension(
                &extension.group,
                &extension.kind,
                &extension.name,
            ))
        }
        other => Err(not_read(other)),
    }
}

/// What of a rule, beyond its matches, filters and backends, translation cannot take yet, if
/// anything.
fn unsupported(rule: &GRPCRouteRules) -> Option<String> {
    rule.backend_refs
        .iter()
        .flatten()
        .any(|written| {
            written
                .filters
                .as_ref()
                .is_some_and(|filters| !filters.is_empty())
        })
        .then(|| "not supported yet: a backend's filters".to_owned())
}

#[cfg(test)]
mod tests {
    use super::method_match;
    use edgerush_config::{GrpcMethod, PathMatch};
    use edgerush_gateway_api::grpc_route::GRPCRouteRulesMatchesMethod;

    fn method(
        kind: &str,
        service: Option<&str>,
        method: Option<&str>,
    ) -> GRPCRouteRulesMatchesMethod {
        GRPCRouteRulesMatchesMethod {
            method: method.map(str::to_owned),
            service: service.map(str::to_owned),
            r#type: kind.to_owned(),
        }
    }

    fn grpc(service: Option<&str>, method: Option<&str>) -> GrpcMethod {
        GrpcMethod {
            service: service.map(str::to_owned),
            method: method.map(str::to_owned),
        }
    }

    #[test]
    fn an_exact_method_match_is_the_configs_grpc_match_of_what_it_names() {
        for (service, name) in [
            (Some("pkg.Svc"), Some("Do")),
            (Some("pkg.Svc"), None),
            (None, Some("Do")),
        ] {
            assert_eq!(
                method_match(&method("Exact", service, name)),
                Ok((None, Some(grpc(service, name))))
            );
        }
    }

    #[test]
    fn a_services_leading_dot_is_dropped_and_an_empty_part_left_out() {
        assert_eq!(
            method_match(&method("Exact", Some(".pkg.Svc"), Some(""))),
            Ok((None, Some(grpc(Some("pkg.Svc"), None))))
        );
        assert_eq!(
            method_match(&method("Exact", Some(""), None)),
            Err("a method match names neither a service nor a method".to_owned())
        );
    }

    #[test]
    fn a_regular_expression_is_a_regex_path_of_both_parts() {
        let regex = |pattern: &str| Ok((Some(PathMatch::Regex(pattern.to_owned())), None));
        assert_eq!(
            method_match(&method(
                "RegularExpression",
                Some("pkg\\.v[0-9]+\\.Svc"),
                Some("Get.*")
            )),
            regex("/(?:pkg\\.v[0-9]+\\.Svc)/(?:Get.*)")
        );
        assert_eq!(
            method_match(&method("RegularExpression", None, Some("Get|List"))),
            regex("/(?:[^/]+)/(?:Get|List)")
        );
        assert_eq!(
            method_match(&method("RegularExpression", Some("pkg\\..*"), None)),
            regex("/(?:pkg\\..*)/(?:[^/]+)")
        );
    }

    #[test]
    fn a_method_match_of_another_type_is_not_read() {
        assert_eq!(
            method_match(&method("Prefix", Some("pkg"), None)),
            Err("a method match of type `Prefix` is not one translation reads".to_owned())
        );
    }
}
