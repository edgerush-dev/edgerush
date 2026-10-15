//! Instruction counts for a limit rule's decision on the request path, compiled and written
//! by hand in Rust: the spike's rules (12 item 2), each against the code it stands for, so
//! that what the expressions cost over the work itself is the difference. DHAT counts what
//! each allocates.
//!
//! Linux only (valgrind): `cargo bench -p edgerush-rules --bench evaluate`, see the
//! repository README.

#![allow(
    missing_docs,
    reason = "the iai-callgrind macros generate public items without docs"
)]

use edgerush_rules::{Address, Context, KeyValue, Named, Outcome, Rule, Schema, Tables, Text};
use iai_callgrind::{
    Dhat, LibraryBenchmarkConfig, library_benchmark, library_benchmark_group, main,
};
use std::collections::HashMap;
use std::hint::black_box;
use std::net::IpAddr;

/// A request, its fields held as the proxy holds them: owned once, read by reference.
struct Request {
    method: String,
    path: String,
    host: String,
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
            Text::Host => Some(&self.host),
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

/// A table of a thousand accounts, so that a lookup is not a map of one.
struct Accounts(HashMap<String, i64>);

impl Tables for Accounts {
    fn number(&self, _table: usize, _column: usize, key: &str) -> Option<i64> {
        self.0.get(key).copied()
    }
}

fn accounts() -> Accounts {
    let mut rows: HashMap<String, i64> = (0..1_000).map(|n| (format!("acct-{n}"), 100)).collect();
    rows.insert("acme".to_owned(), 500);
    Accounts(rows)
}

fn request() -> Request {
    Request {
        method: "POST".to_owned(),
        path: "/v1/orders/42".to_owned(),
        host: "api.example.com".to_owned(),
        api_key: Some("c3b2a1f0-9e8d-4c7b-a6f5-e4d3c2b1a0f9".to_owned()),
        account: Some("acme".to_owned()),
        client: IpAddr::from([192, 0, 2, 10]),
        headers: vec![
            ("accept".to_owned(), "*/*".to_owned()),
            ("user-agent".to_owned(), "curl/8".to_owned()),
            ("x-env".to_owned(), "prod".to_owned()),
        ],
    }
}

fn schema() -> Schema {
    Schema {
        tables: vec![("accounts".to_owned(), vec!["rps".to_owned()])],
    }
}

/// The rules measured: a constant key; the client's address; a method and path
/// condition; an API key; an account's limit looked up, with a default; and a wider
/// condition, ranges and methods and a header.
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

#[allow(
    clippy::panic,
    reason = "a benchmark of a rule that does not compile would measure nothing"
)]
fn compiled(index: usize) -> (Rule, Request, Accounts) {
    let (name, when, key, limit) = RULES[index];
    let rule = Rule::compile(name, when, key, limit, &schema()).unwrap_or_else(|e| panic!("{e}"));
    (rule, request(), accounts())
}

fn by_hand(index: usize) -> (usize, Request, Accounts) {
    (index, request(), accounts())
}

/// The same rules as Rust, as a gateway with no language would write them.
fn decide_by_hand<'a>(index: usize, r: &'a Request, tables: &Accounts) -> Outcome<'a> {
    let applies = |key, limit| Outcome::Applies { key, limit };
    match index {
        0 => applies(KeyValue::Text("orders-backend"), 1000),
        1 => applies(KeyValue::Address(r.client), 100),
        2 => {
            if r.method == "POST" && r.path.starts_with("/v1/orders/") {
                applies(KeyValue::Address(r.client), 10)
            } else {
                Outcome::NotApplicable
            }
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
        _ => {
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
    }
}

#[library_benchmark]
#[bench::constant(compiled(0))]
#[bench::client(compiled(1))]
#[bench::writes(compiled(2))]
#[bench::api_key(compiled(3))]
#[bench::account(compiled(4))]
#[bench::wide(compiled(5))]
fn rule(setup: (Rule, Request, Accounts)) -> (bool, (Rule, Request, Accounts)) {
    let (rule, request, tables) = &setup;
    let applies = matches!(
        rule.evaluate(black_box(request), black_box(tables)),
        Outcome::Applies { .. }
    );
    // Handed back, so that what was set up is freed outside what is measured.
    (applies, setup)
}

#[library_benchmark]
#[bench::constant(by_hand(0))]
#[bench::client(by_hand(1))]
#[bench::writes(by_hand(2))]
#[bench::api_key(by_hand(3))]
#[bench::account(by_hand(4))]
#[bench::wide(by_hand(5))]
fn rust(setup: (usize, Request, Accounts)) -> (bool, (usize, Request, Accounts)) {
    let (index, request, tables) = &setup;
    let applies = matches!(
        decide_by_hand(black_box(*index), black_box(request), black_box(tables)),
        Outcome::Applies { .. }
    );
    (applies, setup)
}

// A thousand decisions: DHAT counts the whole run, setup and all, so a rule that
// allocated to decide would show here a thousand blocks more than under `rule`.
#[library_benchmark]
#[bench::constant(compiled(0))]
#[bench::client(compiled(1))]
#[bench::writes(compiled(2))]
#[bench::api_key(compiled(3))]
#[bench::account(compiled(4))]
#[bench::wide(compiled(5))]
fn thousand(setup: (Rule, Request, Accounts)) -> (usize, (Rule, Request, Accounts)) {
    let (rule, request, tables) = &setup;
    let applied = (0..1_000)
        .filter(|_| {
            matches!(
                rule.evaluate(black_box(request), black_box(tables)),
                Outcome::Applies { .. }
            )
        })
        .count();
    (applied, setup)
}

library_benchmark_group!(name = rules; benchmarks = rule, rust, thousand);
main!(
    config = LibraryBenchmarkConfig::default().tool(Dhat::default());
    library_benchmark_groups = rules
);
