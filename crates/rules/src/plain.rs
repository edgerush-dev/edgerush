//! Rules written as plain fields: a fixed vocabulary of matches, key sources and limits,
//! no expressions. Each compiles to the expressions it stands for, so that both forms are
//! checked by one checker and decided by one evaluator, and cost the same.

use crate::{Part, Rule, RuleError, Schema};

/// What a plain rule counts by.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Key {
    /// The same for every request: the rule's own count.
    Constant(String),
    /// A field of the context, by its name: `client`, `auth.api_key`, …
    Field(String),
}

/// How much a plain rule allows.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Limit {
    /// A number.
    Count(u64),
    /// A table's value for the rule's key, or `default` without one.
    Table {
        /// The table.
        table: String,
        /// Its column.
        column: String,
        /// The limit for a key the table does not list.
        default: u64,
    },
}

/// A rule as plain fields.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlainRule {
    /// Its name.
    pub name: String,
    /// The methods it applies to; all of them when empty.
    pub methods: Vec<String>,
    /// The path it applies under, if not all.
    pub path_prefix: Option<String>,
    /// What it counts by.
    pub key: Key,
    /// How much it allows.
    pub limit: Limit,
}

/// What is wrong with a plain rule.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("rule `{rule}`, {field}: {message}")]
pub struct PlainError {
    /// The rule's name.
    pub rule: String,
    /// The field it is about.
    pub field: &'static str,
    /// What is wrong, for a person.
    pub message: String,
}

/// `text` as an expression's string.
fn quoted(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        if c == '"' || c == '\\' {
            out.push('\\');
        }
        out.push(c);
    }
    out.push('"');
    out
}

impl PlainRule {
    /// Compiles it against `schema`.
    pub fn compile(&self, schema: &Schema) -> Result<Rule, PlainError> {
        let fail = |field, message: String| PlainError {
            rule: self.name.clone(),
            field,
            message,
        };
        let mut conditions = Vec::new();
        if !self.methods.is_empty() {
            for method in &self.methods {
                if method.is_empty() || !method.bytes().all(|b| b.is_ascii_uppercase()) {
                    return Err(fail("methods", format!("`{method}` is not a method")));
                }
            }
            let list: Vec<String> = self.methods.iter().map(|m| quoted(m)).collect();
            conditions.push(format!("method in [{}]", list.join(", ")));
        }
        if let Some(prefix) = &self.path_prefix {
            if !prefix.starts_with('/') {
                return Err(fail(
                    "path_prefix",
                    format!("`{prefix}` does not start with `/`"),
                ));
            }
            conditions.push(format!("path.startsWith({})", quoted(prefix)));
        }
        let when = (!conditions.is_empty()).then(|| conditions.join(" && "));
        let key = match &self.key {
            Key::Constant(text) => quoted(text),
            Key::Field(name) => {
                let plain_name = !name.is_empty()
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b == b'_' || b == b'.');
                if !plain_name {
                    return Err(fail("key", format!("`{name}` is not a field's name")));
                }
                name.clone()
            }
        };
        let limit = match &self.limit {
            Limit::Count(count) => count.to_string(),
            Limit::Table {
                table,
                column,
                default,
            } => {
                let column_ok = !column.is_empty()
                    && column
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || b == b'_');
                if !column_ok {
                    return Err(fail("limit", format!("`{column}` is not a column's name")));
                }
                format!("table({})[{key}].{column} ?? {default}", quoted(table))
            }
        };
        Rule::compile(&self.name, when.as_deref(), &key, &limit, schema).map_err(
            |RuleError { part, error, .. }: RuleError| {
                let field = match part {
                    Part::When => "methods or path_prefix",
                    Part::Key => "key",
                    Part::Limit => "limit",
                };
                fail(field, error.message)
            },
        )
    }
}
