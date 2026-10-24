//! What a limit rule's decision costs in time, on the machine it runs on: each of the
//! spike's rules compiled and written by hand in Rust, the rate limiter's decision for a
//! key it holds, and the two together as a request pays them — the rule's key put
//! together as the proxy does (the rule's name, a zero, the value) and decided (03 §2: at
//! most 150 ns for an existing key). Release builds only:
//! `cargo run --release -p edgerush-rules --example timing`.

use edgerush_ratelimit::service::{Contract, DataPlane, Decision, Keyed, ServicePod, View};
use edgerush_rules::{Address, Context, KeyValue, Named, Outcome, Rule, Schema, Tables, Text};
use std::collections::HashMap;
use std::hint::black_box;
use std::net::IpAddr;
use std::num::NonZeroU64;
use std::time::Instant;

const MS: u64 = 1_000_000;
const ROUNDS: usize = 9;
const PER_ROUND: u32 = 2_000_000;

struct Request {
    method: String,
    path: String,
    api_key: Option<String>,
    account: Option<String>,
    client: IpAddr,
    headers: Vec<(String, String)>,
}

impl Context for Request {
    fn text(&self, field: Text) -> Option<&str> {
        match field {
            Text::Method => Some(&self.method),
            Text::Path => Some(&self.path),
            Text::ApiKey => self.api_key.as_deref(),
            Text::Account => self.account.as_deref(),
            _ => None,
        }
    }

    fn address(&self, field: Address) -> Option<IpAddr> {
        (field == Address::Client).then_some(self.client)
    }

    fn named(&self, field: Named, name: &str) -> Option<&str> {
        if field != Named::Header {
            return None;
        }
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

struct Accounts(HashMap<String, i64>);

impl Tables for Accounts {
    fn number(&self, _table: usize, _column: usize, key: &str) -> Option<i64> {
        self.0.get(key).copied()
    }
}

const RULES: [(&str, Option<&str>, &str, &str); 6] = [
    ("constant", None, r#""orders-backend""#, "1000"),
    ("client", None, "client", "100"),
    (
        "writes",
        Some(r#"method == "POST" && path.startsWith("/v1/orders/")"#),
        "client",
        "10",
    ),
    ("api_key", None, "auth.api_key", "100"),
    (
        "account",
        None,
        "auth.account",
        r#"table("accounts")[auth.account].rps ?? 10"#,
    ),
    (
        "wide",
        Some(
            r#"!(client in [cidr("10.0.0.0/8"), cidr("172.16.0.0/12"), cidr("192.168.0.0/16")]) && method in ["POST", "PUT", "PATCH"] && path.startsWith("/v1/") && headers["x-env"] != "test""#,
        ),
        "client",
        "50",
    ),
];

/// The same rules as Rust.
fn by_hand<'a>(index: usize, r: &'a Request, tables: &Accounts) -> Outcome<'a> {
    let applies = |key, limit| Outcome::Applies { key, limit };
    match index {
        0 => applies(KeyValue::Text("orders-backend"), 1000),
        1 => applies(KeyValue::Address(r.client), 100),
        2 if r.method == "POST" && r.path.starts_with("/v1/orders/") => {
            applies(KeyValue::Address(r.client), 10)
        }
        3 => r
            .api_key
            .as_deref()
            .map_or(Outcome::Unkeyed, |key| applies(KeyValue::Text(key), 100)),
        4 => match r.account.as_deref() {
            None => Outcome::Unkeyed,
            Some(account) => {
                let limit = tables.number(0, 0, account).unwrap_or(10);
                u64::try_from(limit)
                    .ok()
                    .filter(|l| *l >= 1)
                    .map_or(Outcome::Malformed, |l| applies(KeyValue::Text(account), l))
            }
        },
        5 => {
            let private = match r.client {
                IpAddr::V4(v4) => {
                    let o = v4.octets();
                    o[0] == 10
                        || (o[0] == 172 && (16..32).contains(&o[1]))
                        || (o[0] == 192 && o[1] == 168)
                }
                IpAddr::V6(_) => false,
            };
            let test = r
                .headers
                .iter()
                .find(|(n, _)| n == "x-env")
                .is_some_and(|(_, v)| v == "test");
            if !private
                && matches!(r.method.as_str(), "POST" | "PUT" | "PATCH")
                && r.path.starts_with("/v1/")
                && !test
            {
                applies(KeyValue::Address(r.client), 50)
            } else {
                Outcome::NotApplicable
            }
        }
        _ => Outcome::NotApplicable,
    }
}

/// Nanoseconds a call of `f`, the median of `ROUNDS` rounds.
fn time(mut f: impl FnMut()) -> f64 {
    let mut rounds: Vec<f64> = (0..ROUNDS)
        .map(|_| {
            let start = Instant::now();
            for _ in 0..PER_ROUND {
                f();
            }
            start.elapsed().as_nanos() as f64 / f64::from(PER_ROUND)
        })
        .collect();
    rounds.sort_by(f64::total_cmp);
    rounds[ROUNDS / 2]
}

fn contract() -> Contract {
    let n = |n| NonZeroU64::new(n).unwrap_or(NonZeroU64::MIN);
    // Large enough that every decision admits: the limiter's common path.
    Contract {
        limit: n(1_000_000_000),
        window: n(1_000 * MS),
        burst: n(1_000_000_000),
    }
}

/// A data plane holding a thousand keys and each rule's key warm, as the proxy's worker
/// would.
fn warm(keys: &[Vec<u8>]) -> DataPlane<Keyed> {
    let view = View {
        epoch: 1,
        pods: vec![0],
    };
    let mut pod: ServicePod = ServicePod::new(0, 10 * MS, view.clone());
    let hasher = Keyed::new([0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210]);
    let mut plane = DataPlane::with_hasher(1, 10 * MS, view, hasher);
    let others = (0..1_000).map(|k| format!("other\0key-{k}").into_bytes());
    for key in others.chain(keys.iter().cloned()) {
        plane.decide(&key, contract(), 0);
    }
    for (_, batch) in plane.take_outgoing() {
        let answers = pod.on_batch(batch, 0);
        plane.on_answers(answers, 0);
    }
    plane.take_decided();
    plane.on_cycle(MS);
    plane.take_outgoing();
    plane
}

/// The rule's key as the proxy puts it together, in a buffer kept for it.
fn key_of(buffer: &mut Vec<u8>, name: &str, key: KeyValue<'_>) {
    buffer.clear();
    buffer.extend_from_slice(name.as_bytes());
    buffer.push(0);
    match key {
        KeyValue::Text(text) => buffer.extend_from_slice(text.as_bytes()),
        KeyValue::Address(IpAddr::V4(v4)) => buffer.extend_from_slice(&v4.octets()),
        KeyValue::Address(IpAddr::V6(v6)) => buffer.extend_from_slice(&v6.octets()),
    }
}

fn main() {
    let schema = Schema {
        tables: vec![("accounts".to_owned(), vec!["rps".to_owned()])],
        said_keys: false,
    };
    let mut rows: HashMap<String, i64> = (0..1_000).map(|n| (format!("acct-{n}"), 100)).collect();
    rows.insert("acme".to_owned(), 500);
    let tables = Accounts(rows);
    let request = Request {
        method: "POST".to_owned(),
        path: "/v1/orders/42".to_owned(),
        api_key: Some("c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9".to_owned()),
        account: Some("acme".to_owned()),
        client: IpAddr::from([192, 0, 2, 10]),
        headers: vec![
            ("accept".to_owned(), "*/*".to_owned()),
            ("user-agent".to_owned(), "curl/8".to_owned()),
            ("x-env".to_owned(), "prod".to_owned()),
        ],
    };
    let mut rules = Vec::new();
    for (name, when, key, limit) in RULES {
        match Rule::compile(name, when, key, limit, &schema) {
            Ok(rule) => rules.push(rule),
            Err(err) => {
                eprintln!("{err}");
                return;
            }
        }
    }
    let mut buffer = Vec::new();
    let keys: Vec<Vec<u8>> = rules
        .iter()
        .filter_map(|rule| match rule.evaluate(&request, &tables) {
            Outcome::Applies { key, .. } => {
                key_of(&mut buffer, rule.name(), key);
                Some(buffer.clone())
            }
            _ => None,
        })
        .collect();
    let mut plane = warm(&keys);
    let contract = contract();

    println!("ns a decision, median of {ROUNDS} rounds of {PER_ROUND}");
    println!("| Rule | Compiled | By hand | Limiter alone | Compiled + key + limiter |");
    println!("|---|---:|---:|---:|---:|");
    for (index, rule) in rules.iter().enumerate() {
        let compiled = time(|| {
            black_box(rule.evaluate(black_box(&request), black_box(&tables)));
        });
        let hand = time(|| {
            black_box(by_hand(
                black_box(index),
                black_box(&request),
                black_box(&tables),
            ));
        });
        let Outcome::Applies { key, .. } = rule.evaluate(&request, &tables) else {
            continue;
        };
        key_of(&mut buffer, rule.name(), key);
        let fixed = buffer.clone();
        let limiter = time(|| {
            let decision = plane.decide(black_box(&fixed), black_box(contract), black_box(2 * MS));
            black_box(decision == Decision::Admit);
        });
        let both = time(|| {
            if let Outcome::Applies { key, .. } =
                rule.evaluate(black_box(&request), black_box(&tables))
            {
                key_of(&mut buffer, rule.name(), key);
                let decision =
                    plane.decide(black_box(&buffer), black_box(contract), black_box(2 * MS));
                black_box(decision == Decision::Admit);
            }
        });
        println!(
            "| {} | {compiled:.1} | {hand:.1} | {limiter:.1} | {both:.1} |",
            rule.name()
        );
    }
}
