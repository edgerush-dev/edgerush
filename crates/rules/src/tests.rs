use super::*;
use crate::plain::{self, PlainRule};
use crate::syntax::{self, Expr, Node, Op};
use proptest::prelude::*;
use std::collections::HashMap;
use std::net::IpAddr;

/// A request as the tests make them.
#[derive(Clone, Debug)]
struct Request {
    listener: String,
    route: String,
    protocol: String,
    id: Option<String>,
    method: String,
    host: String,
    path: String,
    sni: Option<String>,
    api_key: Option<String>,
    account: Option<String>,
    client: IpAddr,
    peer: IpAddr,
    headers: Vec<(String, String)>,
    query: Vec<(String, String)>,
    cookies: Vec<(String, String)>,
    claims: Vec<(String, String)>,
}

impl Default for Request {
    fn default() -> Self {
        Self {
            listener: "public".into(),
            route: "orders".into(),
            protocol: "2".into(),
            id: None,
            method: "GET".into(),
            host: "api.example.com".into(),
            path: "/".into(),
            sni: None,
            api_key: None,
            account: None,
            client: "192.0.2.10".parse().unwrap(),
            peer: "10.1.2.3".parse().unwrap(),
            headers: Vec::new(),
            query: Vec::new(),
            cookies: Vec::new(),
            claims: Vec::new(),
        }
    }
}

impl Context for Request {
    fn text(&self, field: Text) -> Option<&str> {
        match field {
            Text::Listener => Some(&self.listener),
            Text::Route => Some(&self.route),
            Text::Protocol => Some(&self.protocol),
            Text::Id => self.id.as_deref(),
            Text::Method => Some(&self.method),
            Text::Host => Some(&self.host),
            Text::Path => Some(&self.path),
            Text::TlsSni => self.sni.as_deref(),
            Text::TlsVersion | Text::TlsPeerSubject => None,
            Text::ApiKey => self.api_key.as_deref(),
            Text::Account => self.account.as_deref(),
        }
    }

    fn address(&self, field: Address) -> Option<IpAddr> {
        Some(match field {
            Address::Client => self.client,
            Address::Peer => self.peer,
        })
    }

    fn named(&self, field: Named, name: &str) -> Option<&str> {
        let values = match field {
            Named::Header => &self.headers,
            Named::Query => &self.query,
            Named::Cookie => &self.cookies,
            Named::Claim => &self.claims,
        };
        values
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, v)| v.as_str())
    }
}

/// Tables as the tests fill them: `(table, column, key)` to a number.
#[derive(Default)]
struct Data(HashMap<(usize, usize, String), i64>);

impl Tables for Data {
    fn number(&self, table: usize, column: usize, key: &str) -> Option<i64> {
        self.0.get(&(table, column, key.to_owned())).copied()
    }
}

fn schema() -> Schema {
    Schema {
        tables: vec![("accounts".into(), vec!["rps".into(), "burst".into()])],
        said_keys: false,
    }
}

fn accounts(rows: &[(&str, i64)]) -> Data {
    Data(
        rows.iter()
            .map(|(k, v)| ((0, 0, (*k).to_owned()), *v))
            .collect(),
    )
}

fn rule(when: Option<&str>, key: &str, limit: &str) -> Rule {
    Rule::compile("r", when, key, limit, &schema()).unwrap()
}

fn refused(when: Option<&str>, key: &str, limit: &str) -> RuleError {
    Rule::compile("r", when, key, limit, &schema()).unwrap_err()
}

fn applies(key: KeyValue<'_>, limit: u64) -> Outcome<'_> {
    Outcome::Applies { key, limit }
}

/// The spike's six rules (rate-limiting-plan.md step 1), each as plain fields and as
/// expressions: the two compile to the same and decide every request alike.
#[test]
fn the_six_rules_decide_alike_in_both_forms() {
    let schema = schema();
    let plain = |name: &str, methods: &[&str], prefix: Option<&str>, key, limit| PlainRule {
        name: name.into(),
        methods: methods.iter().map(|m| (*m).to_owned()).collect(),
        path_prefix: prefix.map(Into::into),
        key,
        limit,
    };
    let field = |name: &str| plain::Key::Field(name.into());
    let pairs = [
        (
            plain(
                "orders-backend",
                &[],
                None,
                plain::Key::Constant("orders-backend".into()),
                plain::Limit::Count(1000),
            ),
            rule(None, r#""orders-backend""#, "1000"),
        ),
        (
            plain(
                "per-client",
                &[],
                None,
                field("client"),
                plain::Limit::Count(100),
            ),
            rule(None, "client", "100"),
        ),
        (
            plain(
                "order-writes",
                &["POST"],
                Some("/v1/orders/"),
                field("client"),
                plain::Limit::Count(10),
            ),
            rule(
                Some(r#"method in ["POST"] && path.startsWith("/v1/orders/")"#),
                "client",
                "10",
            ),
        ),
        (
            plain(
                "per-api-key",
                &[],
                None,
                field("auth.api_key"),
                plain::Limit::Count(100),
            ),
            rule(None, "auth.api_key", "100"),
        ),
        (
            plain(
                "by-account",
                &[],
                None,
                field("auth.account"),
                plain::Limit::Table {
                    table: "accounts".into(),
                    column: "rps".into(),
                    default: 10,
                },
            ),
            rule(
                None,
                "auth.account",
                r#"table("accounts")[auth.account].rps ?? 10"#,
            ),
        ),
    ];
    let data = accounts(&[("acme", 500), ("broken", 0)]);
    let requests = [
        Request::default(),
        Request {
            method: "POST".into(),
            path: "/v1/orders/42".into(),
            ..Request::default()
        },
        Request {
            method: "POST".into(),
            path: "/v1/order".into(),
            api_key: Some("k1".into()),
            ..Request::default()
        },
        Request {
            account: Some("acme".into()),
            ..Request::default()
        },
        Request {
            account: Some("unknown".into()),
            ..Request::default()
        },
        Request {
            account: Some("broken".into()),
            ..Request::default()
        },
    ];
    for (plain, expression) in &pairs {
        let compiled = plain.compile(&schema).unwrap();
        for request in &requests {
            assert_eq!(
                compiled.evaluate(request, &data),
                expression.evaluate(request, &data),
                "{} on {request:?}",
                plain.name
            );
        }
    }
    let [constant, client, writes, api_key, account] = pairs.map(|(_, rule)| rule);
    let [plain_get, post_orders, post_other, acme, unknown, broken] = &requests;
    let ip = |r: &Request| KeyValue::Address(r.client);
    assert_eq!(
        constant.evaluate(plain_get, &data),
        applies(KeyValue::Text("orders-backend"), 1000)
    );
    assert_eq!(
        client.evaluate(plain_get, &data),
        applies(ip(plain_get), 100)
    );
    assert_eq!(writes.evaluate(plain_get, &data), Outcome::NotApplicable);
    assert_eq!(
        writes.evaluate(post_orders, &data),
        applies(ip(post_orders), 10)
    );
    assert_eq!(writes.evaluate(post_other, &data), Outcome::NotApplicable);
    assert_eq!(
        api_key.evaluate(plain_get, &data),
        Outcome::Unkeyed,
        "no API key"
    );
    assert_eq!(
        api_key.evaluate(post_other, &data),
        applies(KeyValue::Text("k1"), 100)
    );
    assert_eq!(
        account.evaluate(acme, &data),
        applies(KeyValue::Text("acme"), 500)
    );
    assert_eq!(
        account.evaluate(unknown, &data),
        applies(KeyValue::Text("unknown"), 10)
    );
    assert_eq!(
        account.evaluate(broken, &data),
        Outcome::Malformed,
        "a value of 0"
    );
}

/// Case 5, a malformed value: every mistake a config can make in a rule is refused when it
/// is compiled, where it goes wrong.
/// Whether a rule applies and what it counts by are apart: the first read while routing,
/// before a rewrite changes the path, the second after; together they are `evaluate`.
#[test]
fn whether_a_rule_applies_and_what_it_counts_by_are_apart() {
    let rule = rule(Some(r#"method == "POST""#), "client", "10");
    let get = Request::default();
    let post = Request {
        method: "POST".into(),
        ..Request::default()
    };
    let data = accounts(&[]);
    assert!(!rule.applies(&get, &data));
    assert!(rule.applies(&post, &data));
    let client = KeyValue::Address(get.client);
    assert_eq!(
        rule.counted(&get, &data),
        applies(client, 10),
        "its when not asked"
    );
    assert_eq!(rule.evaluate(&get, &data), Outcome::NotApplicable);
    assert_eq!(rule.evaluate(&post, &data), applies(client, 10));
}

#[test]
fn malformed_rules_are_refused_when_compiled() {
    for (when, key, limit, part, at, message) in [
        (
            Some("method == POST"),
            "client",
            "1",
            Part::When,
            10..14,
            "no field `POST`",
        ),
        (
            Some("path.startsWith(1)"),
            "client",
            "1",
            Part::When,
            16..17,
            "expected a text",
        ),
        (
            Some(r#"auth.acount == "a""#),
            "client",
            "1",
            Part::When,
            0..11,
            "no field",
        ),
        (
            Some(r#"method"#),
            "client",
            "1",
            Part::When,
            0..6,
            "expected a condition, not a text",
        ),
        (
            Some(r#"path > "/a""#),
            "client",
            "1",
            Part::When,
            0..11,
            "a text has no order",
        ),
        (
            Some(r#"client in cidr("10.0.0.0/33")"#),
            "client",
            "1",
            Part::When,
            15..28,
            "not an address range",
        ),
        (
            Some(r#"has(path)"#),
            "client",
            "1",
            Part::When,
            4..8,
            "never missing",
        ),
        (
            Some(r#"headers == "a""#),
            "client",
            "1",
            Part::When,
            0..7,
            "values by name",
        ),
        (
            Some(r#"headers[path] == "a""#),
            "client",
            "1",
            Part::When,
            8..12,
            "a name in quotes",
        ),
        (
            Some(r#"method == 1"#),
            "client",
            "1",
            Part::When,
            0..11,
            "a text compared with a number",
        ),
        (
            Some(r#"(method == "GET") == true"#),
            "client",
            "1",
            Part::When,
            0..25,
            "not compared",
        ),
        (
            None,
            r#"headers["x-api-key"]"#,
            "1",
            Part::Key,
            0..20,
            "what the client said",
        ),
        (None, "path", "1", Part::Key, 0..4, "what the client said"),
        (
            None,
            r#"auth.api_key ?? headers["x"]"#,
            "1",
            Part::Key,
            0..28,
            "what the client said",
        ),
        (
            None,
            "client == peer",
            "1",
            Part::Key,
            0..14,
            "a text or an address",
        ),
        (None, "client", "fast", Part::Limit, 0..4, "no field `fast`"),
        (None, "client", "0", Part::Limit, 0..1, "at least 1"),
        (None, "client", r#""100""#, Part::Limit, 0..5, "a number"),
        (
            None,
            "auth.account",
            r#"table("accounts")[auth.account].rps"#,
            Part::Limit,
            0..35,
            "default",
        ),
        (
            None,
            "auth.account",
            r#"table("acounts")[auth.account].rps ?? 1"#,
            Part::Limit,
            6..15,
            "no table",
        ),
        (
            None,
            "auth.account",
            r#"table("accounts")[auth.account].rpm ?? 1"#,
            Part::Limit,
            0..35,
            "no column",
        ),
        (
            None,
            "client",
            r#"10 ?? 5"#,
            Part::Limit,
            0..2,
            "never be used",
        ),
        (
            None,
            "auth.account",
            r#"table("accounts")[auth.account].rps ?? table("accounts")["x"].rps"#,
            Part::Limit,
            0..65,
            "default",
        ),
    ] {
        let err = refused(when, key, limit);
        assert_eq!(err.part, part, "{when:?} {key} {limit}: {err}");
        assert_eq!(err.error.at, at, "{when:?} {key} {limit}: {err}");
        assert!(
            err.error.message.contains(message),
            "{when:?} {key} {limit}: {err}"
        );
    }
}

#[test]
fn plain_rules_report_the_field_that_is_wrong() {
    let base = PlainRule {
        name: "p".into(),
        methods: Vec::new(),
        path_prefix: None,
        key: plain::Key::Field("client".into()),
        limit: plain::Limit::Count(1),
    };
    for (rule, field, message) in [
        (
            PlainRule {
                methods: vec!["post".into()],
                ..base.clone()
            },
            "methods",
            "not a method",
        ),
        (
            PlainRule {
                path_prefix: Some("v1/".into()),
                ..base.clone()
            },
            "path_prefix",
            "does not start",
        ),
        (
            PlainRule {
                key: plain::Key::Field("path".into()),
                ..base.clone()
            },
            "key",
            "what the client said",
        ),
        (
            PlainRule {
                key: plain::Key::Field("headers[\"x\"]".into()),
                ..base.clone()
            },
            "key",
            "what the client said",
        ),
        (
            PlainRule {
                key: plain::Key::Field("headers[\"X-Api-Key\"]".into()),
                ..base.clone()
            },
            "key",
            "not a field's name",
        ),
        (
            PlainRule {
                key: plain::Key::Field("headers[x]".into()),
                ..base.clone()
            },
            "key",
            "not a field's name",
        ),
        (
            PlainRule {
                limit: plain::Limit::Count(0),
                ..base.clone()
            },
            "limit",
            "at least 1",
        ),
        (
            PlainRule {
                limit: plain::Limit::Table {
                    table: "nope".into(),
                    column: "rps".into(),
                    default: 1,
                },
                ..base.clone()
            },
            "limit",
            "no table",
        ),
    ] {
        let err = rule.compile(&schema()).unwrap_err();
        assert_eq!(err.field, field, "{err}");
        assert!(err.message.contains(message), "{err}");
    }
    let quoted = PlainRule {
        path_prefix: Some(r#"/a"b\c"#.into()),
        ..base
    };
    let compiled = quoted.compile(&schema()).unwrap();
    let request = Request {
        path: r#"/a"b\c/d"#.into(),
        ..Request::default()
    };
    assert_ne!(
        compiled.evaluate(&request, &Data::default()),
        Outcome::NotApplicable
    );
}

/// With the schema allowing keys the client says, a plain rule may be keyed by a header,
/// written as the language writes one: counted by its value, and, without the header,
/// nothing to count by.
#[test]
fn a_plain_rule_keyed_by_a_header_counts_by_its_value() {
    let rule = PlainRule {
        name: "p".into(),
        methods: Vec::new(),
        path_prefix: None,
        key: plain::Key::Field("headers[\"x-api-key\"]".into()),
        limit: plain::Limit::Count(5),
    };
    let allowing = Schema {
        said_keys: true,
        ..schema()
    };
    let compiled = rule.compile(&allowing).unwrap();
    let with = Request {
        headers: vec![("x-api-key".into(), "k-123".into())],
        ..Request::default()
    };
    assert_eq!(
        compiled.evaluate(&with, &Data::default()),
        Outcome::Applies {
            key: KeyValue::Text("k-123"),
            limit: 5
        }
    );
    assert_eq!(
        compiled.evaluate(&Request::default(), &Data::default()),
        Outcome::Unkeyed
    );
}

/// What a missing value does: a comparison with it is false and `!=` true, a test of it
/// false, `has` says whether it is there and `??` stands in for it.
#[test]
fn missing_values_compare_false_and_take_defaults() {
    let data = Data::default();
    let bare = Request::default();
    let with = Request {
        headers: vec![("x-env".into(), "test".into())],
        id: Some("abc".into()),
        ..Request::default()
    };
    for (when, on_bare, on_with) in [
        (r#"headers["x-env"] == "test""#, false, true),
        (r#"headers["X-Env"] == "test""#, false, true),
        (r#"headers["x-env"] != "test""#, true, false),
        (r#"headers["x-env"].startsWith("t")"#, false, true),
        (r#"has(headers["x-env"])"#, false, true),
        (r#"(headers["x-env"] ?? "none") == "none""#, true, false),
        (r#"id == "abc" || !has(id)"#, true, true),
        (
            r#"(table("accounts")[id ?? "-"].rps ?? 3) == 3"#,
            true,
            true,
        ),
        (
            r#"client in cidr("192.0.2.0/24") && !(peer in cidr("192.0.2.0/24"))"#,
            true,
            true,
        ),
        (
            r#"client in [cidr("10.0.0.0/8"), cidr("192.0.2.10/32")]"#,
            true,
            true,
        ),
        (
            r#"client in cidr("0.0.0.0/0") && !(client in cidr("::/0"))"#,
            true,
            true,
        ),
        (
            r#"method in ["GET", "HEAD"] && protocol == "2""#,
            true,
            true,
        ),
    ] {
        let rule = Rule::compile("r", Some(when), "client", "1", &schema());
        let rule = match rule {
            Ok(rule) => rule,
            Err(err) => panic!("{when}: {err}"),
        };
        let applies = |r: &Request| rule.evaluate(r, &data) != Outcome::NotApplicable;
        assert_eq!(applies(&bare), on_bare, "{when} without");
        assert_eq!(applies(&with), on_with, "{when} with");
    }
}

#[test]
fn an_ipv4_client_seen_over_ipv6_is_matched_as_ipv4() {
    let rule = rule(Some(r#"client in cidr("192.0.2.0/24")"#), "client", "1");
    let mapped = Request {
        client: "::ffff:192.0.2.7".parse().unwrap(),
        ..Request::default()
    };
    assert_ne!(
        rule.evaluate(&mapped, &Data::default()),
        Outcome::NotApplicable
    );
}

// --- Differential: compiled against a naive interpreter of the syntax tree ----------------

/// A value as the naive interpreter holds it.
#[derive(Clone, Debug, PartialEq)]
enum Value {
    Missing,
    Text(String),
    Int(i64),
    Bool(bool),
    Address(IpAddr),
    Texts(Vec<String>),
    Ranges(Vec<(IpAddr, u8)>),
}

/// Whether `ip` is in `net/prefix`, bit by bit.
fn in_range(ip: IpAddr, (net, prefix): (IpAddr, u8)) -> bool {
    let bits = |ip: IpAddr| -> Vec<bool> {
        let bytes = match ip.to_canonical() {
            IpAddr::V4(v4) => v4.octets().to_vec(),
            IpAddr::V6(v6) => v6.octets().to_vec(),
        };
        bytes
            .iter()
            .flat_map(|b| (0..8).rev().map(move |i| b >> i & 1 == 1))
            .collect()
    };
    let (a, b) = (bits(ip), bits(net));
    a.len() == b.len() && a[..usize::from(prefix)] == b[..usize::from(prefix)]
}

fn dotted(node: &Node) -> Option<String> {
    match &node.expr {
        Expr::Ident(name) => Some(name.clone()),
        Expr::Member(base, name) => Some(format!("{}.{name}", dotted(base)?)),
        _ => None,
    }
}

/// Evaluates a tree as written, looking everything up by name.
fn naive(node: &Node, r: &Request, data: &Data) -> Value {
    let text = |v: Option<&String>| v.map_or(Value::Missing, |v| Value::Text(v.clone()));
    let pairs = |list: &[(String, String)], name: &str| {
        text(list.iter().find(|(n, _)| n == name).map(|(_, v)| v))
    };
    match &node.expr {
        Expr::Str(s) => Value::Text(s.clone()),
        Expr::Int(i) => Value::Int(*i),
        Expr::Bool(b) => Value::Bool(*b),
        Expr::Ident(_) | Expr::Member(..) => {
            if let Expr::Member(base, column) = &node.expr
                && let Expr::Index(table, key) = &base.expr
                && let Expr::Call(_, _) = &table.expr
            {
                let column = ["rps", "burst"].iter().position(|c| c == column).unwrap();
                return match naive(key, r, data) {
                    Value::Text(key) => data
                        .0
                        .get(&(0, column, key))
                        .map_or(Value::Missing, |v| Value::Int(*v)),
                    _ => Value::Missing,
                };
            }
            match dotted(node).unwrap().as_str() {
                "listener" => Value::Text(r.listener.clone()),
                "route" => Value::Text(r.route.clone()),
                "protocol" => Value::Text(r.protocol.clone()),
                "id" => text(r.id.as_ref()),
                "method" => Value::Text(r.method.clone()),
                "host" => Value::Text(r.host.clone()),
                "path" => Value::Text(r.path.clone()),
                "tls.sni" => text(r.sni.as_ref()),
                "auth.api_key" => text(r.api_key.as_ref()),
                "auth.account" => text(r.account.as_ref()),
                "client" => Value::Address(r.client),
                "peer" => Value::Address(r.peer),
                other => panic!("no field {other}"),
            }
        }
        Expr::Index(base, index) => {
            let Expr::Str(name) = &index.expr else {
                panic!()
            };
            match dotted(base).unwrap().as_str() {
                "headers" => pairs(&r.headers, &name.to_ascii_lowercase()),
                "query" => pairs(&r.query, name),
                "cookies" => pairs(&r.cookies, name),
                _ => pairs(&r.claims, name),
            }
        }
        Expr::Call(name, args) if name == "has" => {
            Value::Bool(naive(&args[0], r, data) != Value::Missing)
        }
        Expr::Call(_, args) => {
            let Expr::Str(range) = &args[0].expr else {
                panic!()
            };
            let (ip, prefix) = range.split_once('/').unwrap();
            Value::Ranges(vec![(ip.parse().unwrap(), prefix.parse().unwrap())])
        }
        Expr::Method(base, name, args) => match (naive(base, r, data), naive(&args[0], r, data)) {
            (Value::Text(t), Value::Text(p)) => Value::Bool(match name.as_str() {
                "startsWith" => t.starts_with(&p),
                "endsWith" => t.ends_with(&p),
                _ => t.contains(&p),
            }),
            _ => Value::Bool(false),
        },
        Expr::List(items) => {
            let values: Vec<Value> = items.iter().map(|i| naive(i, r, data)).collect();
            if values.iter().all(|v| matches!(v, Value::Text(_))) {
                Value::Texts(
                    values
                        .into_iter()
                        .map(|v| match v {
                            Value::Text(t) => t,
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            } else {
                Value::Ranges(
                    values
                        .into_iter()
                        .flat_map(|v| match v {
                            Value::Ranges(r) => r,
                            _ => unreachable!(),
                        })
                        .collect(),
                )
            }
        }
        Expr::Not(operand) => Value::Bool(naive(operand, r, data) == Value::Bool(false)),
        Expr::Binary(op, left, right) => {
            let (l, rv) = (naive(left, r, data), naive(right, r, data));
            match op {
                Op::And => Value::Bool(l == Value::Bool(true) && rv == Value::Bool(true)),
                Op::Or => Value::Bool(l == Value::Bool(true) || rv == Value::Bool(true)),
                Op::Coalesce => {
                    if l == Value::Missing {
                        rv
                    } else {
                        l
                    }
                }
                Op::In => Value::Bool(match (l, rv) {
                    (Value::Text(t), Value::Texts(list)) => list.contains(&t),
                    (Value::Address(ip), Value::Ranges(ranges)) => {
                        ranges.iter().any(|range| in_range(ip, *range))
                    }
                    _ => false,
                }),
                _ if l == Value::Missing || rv == Value::Missing => Value::Bool(*op == Op::Ne),
                Op::Eq => Value::Bool(l == rv),
                Op::Ne => Value::Bool(l != rv),
                _ => {
                    let (Value::Int(a), Value::Int(b)) = (l, rv) else {
                        panic!("ordered non-numbers")
                    };
                    Value::Bool(match op {
                        Op::Lt => a < b,
                        Op::Le => a <= b,
                        Op::Gt => a > b,
                        _ => a >= b,
                    })
                }
            }
        }
    }
}

/// Conditions over the fields, from small sets of values so that they often hold.
fn condition() -> impl Strategy<Value = String> {
    let leaf = prop_oneof![
        (
            prop::sample::select(vec!["GET", "POST", "PUT"]),
            prop::sample::select(vec!["==", "!="])
        )
            .prop_map(|(m, op)| format!(r#"method {op} "{m}""#)),
        prop::sample::select(vec!["a", "b"])
            .prop_map(|v| format!(r#"(headers["X-T"] ?? "a") == (query["q"] ?? "{v}")"#)),
        prop::sample::select(vec!["a", "rs", "/"])
            .prop_map(|p| format!(r#"(headers["x-t"] ?? path).startsWith("{p}")"#)),
        prop::sample::select(vec!["a", "rs", "/"])
            .prop_map(|p| format!(r#"path.endsWith(query["q"] ?? "{p}")"#)),
        prop::sample::select(vec!["a", "b"])
            .prop_map(|p| format!(r#"(query["q"] ?? path).contains("{p}")"#)),
        prop::sample::select(vec!["/", "/v1/", "/v1/orders"])
            .prop_map(|p| format!(r#"path.startsWith("{p}")"#)),
        prop::sample::select(vec!["s", "/", "rs"]).prop_map(|p| format!(r#"path.endsWith("{p}")"#)),
        prop::sample::select(vec!["a", "b"]).prop_map(|v| format!(r#"headers["X-T"] == "{v}""#)),
        prop::sample::select(vec!["a", "b"]).prop_map(|v| format!(r#"query["q"] != "{v}""#)),
        Just("has(auth.api_key)".to_owned()),
        Just(r#"auth.account in ["acme", "beta"]"#.to_owned()),
        prop::sample::select(vec![
            "192.0.2.0/24",
            "10.0.0.0/8",
            "0.0.0.0/0",
            "192.0.2.10/32",
            "192.0.2.11/32",
            "2001:db8::1/128"
        ])
        .prop_map(|n| format!(r#"client in cidr("{n}")"#)),
        Just(r#"peer in [cidr("10.0.0.0/8"), cidr("2001:db8::/32")]"#.to_owned()),
        (
            prop::sample::select(vec!["==", "!=", "<", "<=", ">", ">="]),
            0i64..8
        )
            .prop_map(|(op, n)| format!(
                r#"(table("accounts")[auth.account ?? "acme"].rps ?? 4) {op} {n}"#
            )),
        (0i64..8).prop_map(|n| format!(r#"table("accounts")[auth.account ?? "-"].burst < {n}"#)),
        Just(r#"(id ?? "none") == "none""#.to_owned()),
        Just("client == peer".to_owned()),
        prop::bool::ANY.prop_map(|b| b.to_string()),
    ];
    leaf.prop_recursive(4, 24, 2, |inner| {
        prop_oneof![
            inner.clone().prop_map(|c| format!("!({c})")),
            (inner.clone(), inner.clone()).prop_map(|(a, b)| format!("({a}) && ({b})")),
            (inner.clone(), inner).prop_map(|(a, b)| format!("({a}) || ({b})")),
        ]
    })
}

fn request() -> impl Strategy<Value = Request> {
    let maybe = |values: Vec<&'static str>| {
        prop::option::of(prop::sample::select(values).prop_map(str::to_owned))
    };
    (
        prop::sample::select(vec!["GET", "POST", "PUT"]),
        prop::sample::select(vec![
            "/",
            "/v1/orders",
            "/v1/orders/7s",
            "/v2/rs",
            "/api/v1/orders",
        ]),
        maybe(vec!["a", "b"]),
        maybe(vec!["a", "b", "c"]),
        maybe(vec!["k1"]),
        maybe(vec!["acme", "beta", "gamma"]),
        maybe(vec!["r1"]),
        prop::sample::select(vec![
            "192.0.2.10",
            "192.0.2.11",
            "192.0.2.99",
            "10.0.0.1",
            "2001:db8::1",
            "2001:db8::",
            "::ffff:192.0.2.10",
        ]),
        prop::sample::select(vec!["10.1.2.3", "2001:db8::5", "192.0.2.10"]),
    )
        .prop_map(
            |(method, path, header, query, api_key, account, id, client, peer)| Request {
                method: method.into(),
                path: path.into(),
                headers: header.map(|v| vec![("x-t".into(), v)]).unwrap_or_default(),
                query: query.map(|v| vec![("q".into(), v)]).unwrap_or_default(),
                api_key,
                account,
                id,
                client: client.parse().unwrap(),
                peer: peer.parse().unwrap(),
                ..Request::default()
            },
        )
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2_000))]

    /// Every condition decides as the naive interpreter of what was written does.
    #[test]
    fn compiled_conditions_decide_as_written(when in condition(), requests in prop::collection::vec(request(), 8)) {
        let data = Data([
            ((0, 0, "acme".to_owned()), 5),
            ((0, 1, "acme".to_owned()), 2),
            ((0, 0, "beta".to_owned()), 0),
        ].into_iter().collect());
        let rule = Rule::compile("r", Some(&when), "client", "1", &schema()).map_err(|e| TestCaseError::fail(format!("{when}: {e}")))?;
        let tree = syntax::parse(&when).unwrap();
        for request in &requests {
            let expected = naive(&tree, request, &data) == Value::Bool(true);
            let decided = rule.evaluate(request, &data) != Outcome::NotApplicable;
            prop_assert_eq!(decided, expected, "{} on {:?}", when, request);
        }
    }

    /// Whatever the text, compiling it ends: in a rule, or in an error inside the text.
    #[test]
    fn any_text_compiles_or_is_refused_within_it(source in "[ -~]{0,64}") {
        if let Err(err) = Rule::compile("r", Some(&source), "client", "1", &schema()) {
            prop_assert!(err.error.at.end <= source.len().max(err.error.at.start), "{}", err);
        }
    }
}
