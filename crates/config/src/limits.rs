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
    /// Whose requests are counted together: `client`, the client's address as forwarding
    /// works it out for each request — the peer, or the client a trusted PROXY protocol
    /// header names, or the client a trusted proxy's `X-Forwarded-For` names — an IPv6
    /// client by its /64, which one client holds whole. Never what an untrusted client says.
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
    /// At most `limit` requests in each `window`, the count starting again as each ends:
    /// a whole window's worth may come at once, and two windows' worth across a boundary.
    /// Each key's windows are shifted by an offset of its own, so that keys' boundaries
    /// spread. Not in the asynchronous mode, which cannot hold a window against a client
    /// bursting on every data plane at each window's start: a quota there is a bucket, and a
    /// window held exactly takes the synchronous mode.
    FixedWindow(Window),
    /// At most `limit` requests in any span of `window`, counted to a thousandth of it. In
    /// no mode yet; never in local mode, nor with `when_unreachable: local`.
    RollingWindow(Window),
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
    /// spread out at the rate: the whole bucket. At least two of the rate-limiting service's
    /// cycles of the rate — 20 ms of it at the 10 ms cycle, 20 at `1000/s` — since a bucket
    /// that refills faster than its counts are shared refuses clients within their rate.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub burst: u64,
}

/// A window's numbers.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[cfg_attr(feature = "schema", derive(schemars::JsonSchema))]
#[serde(deny_unknown_fields)]
pub struct Window {
    /// Requests a window.
    #[cfg_attr(feature = "schema", schemars(range(min = 1)))]
    pub limit: u64,
    /// The window: a whole number and `ms`, `s`, `m`, `h` or `d`, as `500ms`, `1m`, `1d`.
    #[cfg_attr(feature = "schema", schemars(regex(pattern = WINDOW_PATTERN)))]
    pub window: String,
}

/// What a rate looks like.
#[cfg(feature = "schema")]
const RATE_PATTERN: &str = "^[1-9][0-9]*/(s|min|h|d)$";

/// What a window looks like.
#[cfg(feature = "schema")]
const WINDOW_PATTERN: &str = "^[1-9][0-9]*(ms|s|m|h|d)$";

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
    /// When it applies, its key and its count: requests a window.
    pub rule: Rule,
    /// Its count: the requests its window holds.
    pub count: NonZeroU64,
    /// The window its count is of, in nanoseconds: a bucket's rate's unit, or a window.
    pub window: NonZeroU64,
    /// Its model, with what is the model's alone.
    pub model: CompiledModel,
    /// Whether it only counts and reports, never refusing (06 §2).
    pub dry_run: bool,
}

/// A limit's model, compiled: what it holds beyond its count and window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CompiledModel {
    /// A token bucket.
    Bucket {
        /// Its rate as it was written, `100/s`: for telling a person.
        rate: String,
        /// The whole bucket.
        burst: NonZeroU64,
    },
    /// A fixed window.
    FixedWindow {
        /// Its window as it was written, `1m`: for telling a person.
        window: String,
    },
    /// A rolling window, which no mode decides yet.
    RollingWindow {
        /// Its window as it was written, `1m`: for telling a person.
        window: String,
    },
}

impl CompiledModel {
    /// Its name in the file.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Bucket { .. } => "bucket",
            Self::FixedWindow { .. } => "fixed_window",
            Self::RollingWindow { .. } => "rolling_window",
        }
    }
}

impl CompiledLimit {
    /// The most a key of it admits at once: its bucket, or its window's count.
    #[must_use]
    pub fn room(&self) -> NonZeroU64 {
        match &self.model {
            CompiledModel::Bucket { burst, .. } => *burst,
            CompiledModel::FixedWindow { .. } | CompiledModel::RollingWindow { .. } => self.count,
        }
    }
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

/// A window as a whole number and a unit, in nanoseconds: at least one, and within what
/// the clock holds.
fn window_length(text: &str) -> Option<NonZeroU64> {
    let digits = text.bytes().take_while(u8::is_ascii_digit).count();
    let (count, unit) = text.split_at(digits);
    let unit = match unit {
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        "m" => 60_000_000_000,
        "h" => 3_600_000_000_000,
        "d" => 86_400_000_000_000,
        _ => return None,
    };
    let count: u64 = count.parse().ok()?;
    NonZeroU64::new(count.checked_mul(unit)?)
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
    header_keys: bool,
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
        let header = limit.key.starts_with("headers[");
        let key_refused = if header && !header_keys {
            Some(Problem::LimitHeaderKeyOff(limit.key.clone()))
        } else if !header && limit.key != "client" {
            Some(Problem::LimitKey(limit.key.clone()))
        } else {
            None
        };
        // A key refused here is said once: the rule is not compiled to say it again.
        let key_ok = key_refused.is_none();
        if let Some(problem) = key_refused {
            errors.push(place.problem(problem));
        }
        let numbers = model(&limit.model, &place, errors);
        let matches = limit.matches.clone().unwrap_or_default();
        let plain = PlainRule {
            name: limit.name.clone(),
            methods: matches.methods,
            path_prefix: matches.path_prefix,
            key: Key::Field(limit.key.clone()),
            limit: Count::Count(numbers.as_ref().map_or(1, |(count, _, _)| count.get())),
        };
        let rule = if key_ok {
            plain
                .compile(&Schema {
                    said_keys: header_keys,
                    ..Schema::default()
                })
                .map_err(|error| {
                    errors.push(place.problem(Problem::LimitRule {
                        field: error.field,
                        message: error.message,
                    }));
                })
                .ok()
        } else {
            None
        };
        if let (Some(rule), Some((count, window, model))) = (rule, numbers)
            && errors.len() == before
        {
            let compiled = CompiledLimit {
                name: limit.name.clone(),
                quoted: quoted(&limit.name),
                position: by_name.len(),
                definition: definition(limit),
                rule,
                count,
                window,
                model,
                dry_run: limit.dry_run,
            };
            by_name.insert(limit.name.clone(), Arc::new(compiled));
        }
    }
    let place = Place::of(Object::Limits);
    let global = named(&limits.global.rate_limits, &by_name, &place, errors);
    CompiledLimits { by_name, global }
}

/// The name a window model has in the file.
fn window_name(model: &Model) -> &'static str {
    if matches!(model, Model::FixedWindow(_)) {
        "fixed_window"
    } else {
        "rolling_window"
    }
}

/// `model`'s count, window and what is its alone, if they are as they should be; the
/// problems found pushed to `errors` at `place`.
fn model(
    model: &Model,
    place: &Place,
    errors: &mut Vec<ConfigError>,
) -> Option<(NonZeroU64, NonZeroU64, CompiledModel)> {
    match model {
        Model::Bucket(bucket) => {
            let rate = rate(&bucket.rate);
            if rate.is_none() {
                errors.push(place.problem(Problem::LimitRate(bucket.rate.clone())));
            }
            let burst = NonZeroU64::new(bucket.burst);
            if burst.is_none() {
                errors.push(place.problem(Problem::LimitBurstZero));
            }
            let ((count, window), burst) = (rate?, burst?);
            let rate = bucket.rate.clone();
            Some((count, window, CompiledModel::Bucket { rate, burst }))
        }
        Model::FixedWindow(numbers) | Model::RollingWindow(numbers) => {
            let name = window_name(model);
            let count = NonZeroU64::new(numbers.limit);
            if count.is_none() {
                errors.push(place.problem(Problem::LimitCountZero(name)));
            }
            let length = window_length(&numbers.window);
            if length.is_none() {
                errors.push(place.problem(Problem::LimitWindow {
                    model: name,
                    window: numbers.window.clone(),
                }));
            }
            let window = numbers.window.clone();
            let compiled = if matches!(model, Model::FixedWindow(_)) {
                CompiledModel::FixedWindow { window }
            } else {
                CompiledModel::RollingWindow { window }
            };
            Some((count?, length?, compiled))
        }
    }
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
    match &limit.model {
        Model::Bucket(bucket) => {
            feed(b"bucket");
            feed(bucket.rate.as_bytes());
            feed(&bucket.burst.to_le_bytes());
        }
        Model::FixedWindow(window) | Model::RollingWindow(window) => {
            feed(window_name(&limit.model).as_bytes());
            feed(&window.limit.to_le_bytes());
            feed(window.window.as_bytes());
        }
    }
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
    /// own — each once, in the order a decision takes them: the smallest room first, a
    /// bucket's or a window's count, the limit most likely at its edge, ties going to the
    /// most specific (06 §4). Names with no
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
        levelled.sort_by_key(|(level, limit)| (limit.room(), *level));
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

    /// A window's model is in its digest, its count and its window as written: a change of
    /// model, even to the same numbers, counts afresh.
    #[test]
    fn a_windows_model_and_numbers_are_digested() {
        let limit = |yaml: &str| -> Limit { serde_saphyr::from_str(yaml).unwrap() };
        let digest = |model: &str| {
            definition(&limit(&format!(
                "{{ name: a, key: client, model: {{ {model} }} }}"
            )))
        };
        let fixed = digest("fixed_window: { limit: 10, window: 1s }");
        for other in [
            "fixed_window: { limit: 11, window: 1s }",
            "fixed_window: { limit: 10, window: 2s }",
            "fixed_window: { limit: 10, window: 1000ms }",
            "rolling_window: { limit: 10, window: 1s }",
            "bucket: { rate: 10/s, burst: 10 }",
        ] {
            assert_ne!(fixed, digest(other), "{other}");
        }
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
