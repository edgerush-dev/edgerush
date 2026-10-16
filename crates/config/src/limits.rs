//! The rate-limiting engine's configuration ([07 §2](../../../docs/07-config-and-dsl.md)):
//! its limits by name, and those that apply to every request through the data plane.
//! Routes and route rules name the limits that apply to them; compilation gathers each route
//! rule's into one list, the order a decision takes them in.
//!
//! A resource of its own, as certificates are: it reaches a data plane apart from the rest
//! of its config, and with rate limiting off nothing of it is read. Backend protection, to
//! come, is a section of its own beside the limits, sharing nothing with them.

use crate::compile::{ConfigError, Object, Place, Problem};
use edgerush_rules::plain::{Key, Limit as Count, PlainRule};
use edgerush_rules::{Rule, Schema};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::num::NonZeroU64;
use std::sync::Arc;

/// The rate-limiting engine's configuration: its limits, and those that apply to every
/// request.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "schema", schemars(title = "EdgeRush rate limits"))]
#[serde(deny_unknown_fields)]
pub struct Limits {
    /// What applies to every request through the data plane that a route rule forwards.
    #[serde(default)]
    pub global: Global,
    /// The limits, each by its name; routes and route rules name those that apply to them.
    pub limits: Vec<Limit>,
}

/// What applies to every request through the data plane.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Global {
    /// The limits every request is under, by name.
    #[serde(default)]
    pub rate_limits: Vec<String>,
}

/// A limit: whose requests it counts together, and how many it allows.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Limit {
    /// Its name, unique among the limits: what routes name it by, what its counts are kept
    /// under, so that they go on through a reload that leaves the limit as it was, and what
    /// the `RateLimit` header of a request it refuses calls it. Printable ASCII, which is
    /// all the header can hold.
    #[cfg_attr(feature = "schema", schemars(regex(pattern = NAME_PATTERN)))]
    pub name: String,
    /// The requests it applies to, among those of the route rules it is named for; all of
    /// them when left out.
    #[serde(default, rename = "match")]
    pub matches: Option<LimitMatch>,
    /// Whose requests are counted together: `client`, the address the data plane sees — the
    /// peer, or the client a trusted PROXY protocol header names — an IPv6 client by its
    /// /64, which one client holds whole. Never what a client says.
    pub key: String,
    /// How the requests are counted.
    pub model: Model,
    /// Counted and reported, never refusing: a request it has no room for goes on, counted
    /// as `dry_run_refused`, and it is not charged, as it would not be had it refused.
    /// Decided after the limits that refuse, and only when they admit; never waited for.
    #[serde(default)]
    pub dry_run: bool,
}

/// The requests a limit applies to: each of what it states holds.
#[derive(Debug, Clone, PartialEq, Eq, Default, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct LimitMatch {
    /// One of these methods, in capitals; any when left out.
    #[serde(default)]
    pub methods: Vec<String>,
    /// A path beginning with this.
    #[serde(default)]
    #[cfg_attr(feature = "schema", schemars(regex(pattern = "^/")))]
    pub path_prefix: Option<String>,
}

/// How a limit counts requests: its numbers inside it, as each model has its own.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum Model {
    /// A token bucket: requests at `rate` on average, at most `burst` at once.
    Bucket(Bucket),
}

/// A token bucket's numbers.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Bucket {
    /// Requests a second, minute, hour or day, on average: `100/s`, `1000/min`, `10/h`,
    /// `500/d`.
    #[cfg_attr(feature = "schema", schemars(regex(pattern = RATE_PATTERN)))]
    pub rate: String,
    /// How many requests a client can send at once, from idle, before its traffic is
    /// spread out at the rate: the whole bucket.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub burst: u64,
}

/// What a rate looks like.
#[cfg(feature = "schema")]
const RATE_PATTERN: &str = "^[1-9][0-9]*/(s|min|h|d)$";

/// What a limit's name looks like: printable ASCII, at least one character.
#[cfg(feature = "schema")]
const NAME_PATTERN: &str = "^[ -~]+$";

/// A limit, compiled: when it applies to a request and what the request is counted by, and
/// its contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledLimit {
    /// Its name, which its keys are counted under.
    pub name: String,
    /// Its name as a header's quoted string, `"writes"`: what the `RateLimit` header calls
    /// it.
    pub quoted: String,
    /// Its place among the engine's limits, in the file's order: what the data plane finds
    /// its series by.
    pub position: usize,
    /// A digest of what defines it apart from its name — its condition, its key, its model
    /// as written — which every key of its carries: a limit changed counts apart from the
    /// one it replaces, afresh, and never mixed with it while pods take the change (06 §2).
    pub definition: [u8; 8],
    /// When it applies, its key and its rate: requests a window.
    pub rule: Rule,
    /// Its rate as it was written, `100/s`: for telling a person.
    pub rate: String,
    /// The window its rate is a number of requests in, in nanoseconds.
    pub window: NonZeroU64,
    /// The whole bucket.
    pub burst: NonZeroU64,
    /// Whether it only counts and reports, never refusing (06 §2).
    pub dry_run: bool,
}

/// The engine's limits, compiled: by name, and those over every request.
#[derive(Debug, Default)]
pub(crate) struct CompiledLimits {
    by_name: BTreeMap<String, Arc<CompiledLimit>>,
    global: Vec<Arc<CompiledLimit>>,
}

/// Where a limit is named: how specific it is, which decides between limits of the same
/// bucket.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Level {
    Rule,
    Route,
    Global,
}

/// The window of a rate's unit, in nanoseconds.
fn window(unit: &str) -> Option<u64> {
    const SECOND: u64 = 1_000_000_000;
    match unit {
        "s" => Some(SECOND),
        "min" => Some(60 * SECOND),
        "h" => Some(3_600 * SECOND),
        "d" => Some(86_400 * SECOND),
        _ => None,
    }
}

/// A rate as `count/unit`: so many requests a window, both at least one.
fn rate(text: &str) -> Option<(NonZeroU64, NonZeroU64)> {
    let (count, unit) = text.split_once('/')?;
    if !count.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let count = NonZeroU64::new(count.parse().ok()?)?;
    Some((count, NonZeroU64::new(window(unit)?)?))
}

/// Compiles the engine's limits, if there are any, the problems found pushed to `errors`.
pub(crate) fn compile_limits(
    limits: Option<&Limits>,
    errors: &mut Vec<ConfigError>,
) -> CompiledLimits {
    let Some(limits) = limits else {
        return CompiledLimits::default();
    };
    let mut by_name = BTreeMap::new();
    for limit in &limits.limits {
        let place = Place::of(Object::Limit(limit.name.clone()));
        let before = errors.len();
        if limit.name.is_empty() || !limit.name.bytes().all(|byte| matches!(byte, b' '..=b'~')) {
            errors.push(place.problem(Problem::LimitName));
        }
        if by_name.contains_key(&limit.name) {
            errors.push(place.problem(Problem::LimitTwice));
        }
        if limit.key != "client" {
            errors.push(place.problem(Problem::LimitKey(limit.key.clone())));
        }
        let Model::Bucket(bucket) = &limit.model;
        let rate = rate(&bucket.rate);
        if rate.is_none() {
            errors.push(place.problem(Problem::LimitRate(bucket.rate.clone())));
        }
        let burst = NonZeroU64::new(bucket.burst);
        if burst.is_none() {
            errors.push(place.problem(Problem::LimitBurstZero));
        }
        let matches = limit.matches.clone().unwrap_or_default();
        let plain = PlainRule {
            name: limit.name.clone(),
            methods: matches.methods,
            path_prefix: matches.path_prefix,
            key: Key::Field(limit.key.clone()),
            limit: Count::Count(rate.map_or(1, |(count, _)| count.get())),
        };
        let rule = plain
            .compile(&Schema::default())
            .map_err(|error| {
                errors.push(place.problem(Problem::LimitRule {
                    field: error.field,
                    message: error.message,
                }));
            })
            .ok();
        if let (Some(rule), Some((_, window)), Some(burst)) = (rule, rate, burst)
            && errors.len() == before
        {
            let compiled = CompiledLimit {
                name: limit.name.clone(),
                quoted: quoted(&limit.name),
                position: by_name.len(),
                definition: definition(limit),
                rule,
                rate: bucket.rate.clone(),
                window,
                burst,
                dry_run: limit.dry_run,
            };
            by_name.insert(limit.name.clone(), Arc::new(compiled));
        }
    }
    let place = Place::of(Object::Limits);
    let global = named(&limits.global.rate_limits, &by_name, &place, errors);
    CompiledLimits { by_name, global }
}

/// The digest of what defines `limit` apart from its name, the same on every data plane
/// that reads it: FNV-1a over each field, each after its length, so that no two
/// definitions run together into the same bytes.
fn definition(limit: &Limit) -> [u8; 8] {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    let mut feed = |bytes: &[u8]| {
        for byte in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    let matches = limit.matches.clone().unwrap_or_default();
    feed(&(matches.methods.len() as u64).to_le_bytes());
    for method in &matches.methods {
        feed(method.as_bytes());
    }
    match &matches.path_prefix {
        Some(prefix) => {
            feed(b"prefix");
            feed(prefix.as_bytes());
        }
        None => feed(b""),
    }
    feed(limit.key.as_bytes());
    let Model::Bucket(bucket) = &limit.model;
    feed(b"bucket");
    feed(bucket.rate.as_bytes());
    feed(&bucket.burst.to_le_bytes());
    hash.to_le_bytes()
}

/// `name` as a structured field's string (RFC 9651 §3.3.3): in quotes, with a backslash
/// before each quote or backslash in it.
fn quoted(name: &str) -> String {
    let mut quoted = String::with_capacity(name.len() + 2);
    quoted.push('"');
    for character in name.chars() {
        if matches!(character, '"' | '\\') {
            quoted.push('\\');
        }
        quoted.push(character);
    }
    quoted.push('"');
    quoted
}

/// The limits `names` name, each looked up in `by_name`; a name with no limit a problem at
/// `place`.
fn named(
    names: &[String],
    by_name: &BTreeMap<String, Arc<CompiledLimit>>,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> Vec<Arc<CompiledLimit>> {
    names
        .iter()
        .filter_map(|name| {
            let found = by_name.get(name).cloned();
            if found.is_none() {
                errors.push(place.problem(Problem::LimitUnknown(name.clone())));
            }
            found
        })
        .collect()
}

impl CompiledLimits {
    /// Every limit, in the order of its position.
    pub(crate) fn all(&self) -> Vec<Arc<CompiledLimit>> {
        let mut all: Vec<Arc<CompiledLimit>> = self.by_name.values().cloned().collect();
        all.sort_by_key(|limit| limit.position);
        all
    }

    /// The limits a route rule's requests are under — the data plane's, its route's, its
    /// own — each once, in the order a decision takes them: the smallest bucket first, the
    /// limit most likely at its edge, ties going to the most specific (06 §4). Names with no
    /// limit are problems at `route_place` and `rule_place`.
    pub(crate) fn for_rule(
        &self,
        route: &[String],
        rule: &[String],
        route_place: &Place,
        rule_place: &Place,
        errors: &mut Vec<ConfigError>,
    ) -> Box<[Arc<CompiledLimit>]> {
        let mut levelled: Vec<(Level, Arc<CompiledLimit>)> = Vec::new();
        let rule = named(rule, &self.by_name, rule_place, errors);
        let route = named(route, &self.by_name, route_place, errors);
        let all = [
            (Level::Rule, rule),
            (Level::Route, route),
            (Level::Global, self.global.clone()),
        ];
        for (level, limits) in all {
            for limit in limits {
                if !levelled.iter().any(|(_, held)| held.name == limit.name) {
                    levelled.push((level, limit));
                }
            }
        }
        levelled.sort_by_key(|(level, limit)| (limit.burst, *level));
        levelled.into_iter().map(|(_, limit)| limit).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two limits defined alike have one digest; any change to the condition, the key or
    /// the model gives another. The name is not in it: a key carries the name apart.
    #[test]
    fn a_limit_is_digested_by_all_that_defines_it() {
        let limit = |yaml: &str| -> Limit { serde_saphyr::from_str(yaml).unwrap() };
        let base = "{ name: a, key: client, model: { bucket: { rate: 10/s, burst: 20 } } }";
        let digest = |yaml: &str| definition(&limit(yaml));
        assert_eq!(digest(base), digest(&base.replace("name: a", "name: b")));
        for changed in [
            base.replace("10/s", "11/s"),
            base.replace("burst: 20", "burst: 21"),
            base.replace("key: client", "key: auth.api_key"),
            base.replace("key: client,", "match: { methods: [GET] }, key: client,"),
            base.replace("key: client,", "match: { path_prefix: /v1 }, key: client,"),
        ] {
            assert_ne!(digest(base), digest(&changed), "{changed}");
        }
        let methods = |methods: &str| {
            digest(&base.replace(
                "key: client,",
                &format!("match: {{ methods: [{methods}] }}, key: client,"),
            ))
        };
        // As many methods, other ones: another condition.
        assert_ne!(methods("GET"), methods("POST"));
        // Fields kept apart: the same bytes split otherwise are not one condition.
        assert_ne!(methods("GET, POST"), methods("GETP, OST"));
    }

    #[test]
    fn a_name_is_quoted_as_a_header_string() {
        assert_eq!(quoted("writes"), r#""writes""#);
        assert_eq!(quoted(r#"say "hi" \o/"#), r#""say \"hi\" \\o/""#);
    }

    #[test]
    fn a_rate_is_a_count_of_at_least_one_in_a_unit_of_time() {
        let n = |n| NonZeroU64::new(n).unwrap();
        assert_eq!(rate("100/s"), Some((n(100), n(1_000_000_000))));
        assert_eq!(rate("1000/min"), Some((n(1_000), n(60_000_000_000))));
        assert_eq!(rate("10/h"), Some((n(10), n(3_600_000_000_000))));
        assert_eq!(rate("500/d"), Some((n(500), n(86_400_000_000_000))));
        for wrong in [
            "0/s", "100", "100/m", "/s", "-1/s", "+1/s", "1.5/s", "100/ s", "1/s/s",
        ] {
            assert_eq!(rate(wrong), None, "{wrong}");
        }
    }
}
