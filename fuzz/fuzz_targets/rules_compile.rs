//! Fuzzes limit rules' expressions: any text offered as a rule's `when`, `key` and
//! `limit` — the three lines of the input — compiles or is refused with an error inside
//! the part it is about, and a rule that compiles decides a request without failing:
//! `cargo fuzz run rules_compile corpus/rules_compile fuzz/seeds/rules_compile`.

#![no_main]

use edgerush_rules::{Address, Context, Named, Part, Rule, Schema, Tables, Text};
use libfuzzer_sys::fuzz_target;
use std::net::IpAddr;

struct Request;

impl Context for Request {
    fn text(&self, field: Text) -> Option<&str> {
        match field {
            Text::Method => Some("POST"),
            Text::Path => Some("/v1/orders/7"),
            Text::Account => Some("acme"),
            Text::Id => None,
            _ => Some("x"),
        }
    }

    fn address(&self, field: Address) -> Option<IpAddr> {
        (field == Address::Client).then(|| IpAddr::from([192, 0, 2, 10]))
    }

    fn named(&self, _field: Named, name: &str) -> Option<&str> {
        (name == "x-env").then_some("prod")
    }
}

struct Accounts;

impl Tables for Accounts {
    fn number(&self, _table: usize, _column: usize, key: &str) -> Option<i64> {
        (key == "acme").then_some(-5)
    }
}

fuzz_target!(|bytes: &[u8]| {
    let Ok(text) = std::str::from_utf8(bytes) else {
        return;
    };
    let mut parts = text.splitn(3, '\n');
    let when = parts.next().filter(|w| !w.is_empty());
    let key = parts.next().unwrap_or("client");
    let limit = parts.next().unwrap_or("1");
    let schema = Schema {
        tables: vec![("accounts".to_owned(), vec!["rps".to_owned()])],
        said_keys: false,
    };
    match Rule::compile("fuzzed", when, key, limit, &schema) {
        Ok(rule) => {
            let _ = rule.evaluate(&Request, &Accounts);
        }
        Err(err) => {
            let source = match err.part {
                Part::When => when.unwrap_or(""),
                Part::Key => key,
                Part::Limit => limit,
            };
            assert!(err.error.at.start <= err.error.at.end, "{err}");
            assert!(err.error.at.end <= source.len(), "{err}");
            assert!(source.is_char_boundary(err.error.at.start), "{err}");
            assert!(source.is_char_boundary(err.error.at.end), "{err}");
        }
    }
});
