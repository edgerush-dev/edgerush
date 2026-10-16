//! Limit rules for the rate-limiting engine (12 item 2, 07 §2): when a rule applies, what
//! it counts by and how much it allows, written over the request context and checked
//! against it when the config is compiled.
//!
//! A spike of the DSL question: CEL's syntax for the part of it rules need — fields,
//! comparisons, `&&` `||` `!`, `in`, `has`, `startsWith` `endsWith` `contains`, `cidr`,
//! `table("t")[key].column` — and `??` for a default, which CEL lacks. It is not CEL and
//! does not claim to be. Rules written as plain fields ([`plain`]) compile to the same.
//!
//! Compiled, a rule reads the context's fields by position, never by name, and allocates
//! nothing to decide a request; its size is bounded, so is its cost. A key may only be what
//! the gateway established — an authentication result or the client's address — never
//! what the client said (23 §4.1).
//!
//! Pure: no I/O; the request and the tables are the caller's ([`Context`], [`Tables`]).

mod compile;
pub mod context;
pub mod plain;
mod syntax;

pub use context::{Address, Context, Named, Schema, Tables, Text};

use compile::{AddressNode, BoolNode, Checker, IntNode, TextNode, Typed};
use std::net::IpAddr;
use std::ops::Range;

/// What is wrong with an expression, and where in it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("at {}..{}: {message}", at.start, at.end)]
pub struct Error {
    /// The bytes of the source it is about.
    pub at: Range<usize>,
    /// What is wrong, for a person.
    pub message: String,
}

/// Which part of a rule an error is in.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Part {
    /// `when`
    #[error("when")]
    When,
    /// `key`
    #[error("key")]
    Key,
    /// `limit`
    #[error("limit")]
    Limit,
}

/// A rule that does not compile: which part, and what is wrong with it.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("rule `{rule}`, {part} {error}")]
pub struct RuleError {
    /// The rule's name.
    pub rule: String,
    /// The part.
    pub part: Part,
    /// The error, its place counted in that part's source.
    pub error: Error,
}

/// What a rule counts by, compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Key {
    Constant(Box<str>),
    Text(TextNode),
    Address(AddressNode),
}

/// A request's key under a rule, borrowed from the rule or the request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyValue<'a> {
    /// A text: a constant, an API key, an account.
    Text(&'a str),
    /// An address: the client's.
    Address(IpAddr),
}

/// What a rule makes of a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome<'a> {
    /// Its `when` does not hold: the rule does not apply.
    NotApplicable,
    /// It applies, but the request has no key for it — no API key, say: let through,
    /// counted as passing without one (23 §4.1).
    Unkeyed,
    /// It applies, but its limit came out below one, from a table's value: let through,
    /// counted as malformed.
    Malformed,
    /// It applies: count `key` against `limit`.
    Applies {
        /// What the request is counted by.
        key: KeyValue<'a>,
        /// How many the rule allows.
        limit: u64,
    },
}

/// A limit rule, compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rule {
    name: String,
    when: Option<BoolNode>,
    key: Key,
    limit: IntNode,
}

impl Rule {
    /// Compiles a rule from its parts' sources against `schema`: `when` a condition, `key`
    /// an established text or address (or a text in quotes, the same for every request),
    /// `limit` a number that is never missing.
    pub fn compile(
        name: &str,
        when: Option<&str>,
        key: &str,
        limit: &str,
        schema: &Schema,
    ) -> Result<Self, RuleError> {
        let fail = |part, error| RuleError {
            rule: name.to_owned(),
            part,
            error,
        };
        let checker = Checker { schema };
        let when = when
            .map(|source| {
                let node = syntax::parse(source)?;
                checker.condition(&node).map(|(node, _)| node)
            })
            .transpose()
            .map_err(|e| fail(Part::When, e))?;
        let key = compile_key(&checker, key).map_err(|e| fail(Part::Key, e))?;
        let limit = compile_limit(&checker, limit).map_err(|e| fail(Part::Limit, e))?;
        Ok(Self {
            name: name.to_owned(),
            when,
            key,
            limit,
        })
    }

    /// Its name.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// What it makes of a request: whether it applies, and if so its key and limit.
    pub fn evaluate<'a, C: Context, T: Tables>(
        &'a self,
        context: &'a C,
        tables: &T,
    ) -> Outcome<'a> {
        if !self.applies(context, tables) {
            return Outcome::NotApplicable;
        }
        self.counted(context, tables)
    }

    /// Whether it applies to a request: its `when` holds, or it has none. Apart from
    /// [`counted`](Self::counted), for a caller that knows the request's routed facts — its
    /// path before a rewrite — only while it routes.
    pub fn applies<C: Context, T: Tables>(&self, context: &C, tables: &T) -> bool {
        self.when
            .as_ref()
            .is_none_or(|when| when.evaluate(context, tables))
    }

    /// What a request it applies to is counted by and against, its `when` not asked.
    pub fn counted<'a, C: Context, T: Tables>(&'a self, context: &'a C, tables: &T) -> Outcome<'a> {
        let key = match &self.key {
            Key::Constant(text) => Some(KeyValue::Text(text)),
            Key::Text(node) => node.evaluate(context).map(KeyValue::Text),
            Key::Address(node) => node.evaluate(context).map(KeyValue::Address),
        };
        let Some(key) = key else {
            return Outcome::Unkeyed;
        };
        // The checker has made sure a limit is never missing; a table's value may still be
        // below one.
        match self.limit.evaluate(context, tables).map(u64::try_from) {
            Some(Ok(limit)) if limit >= 1 => Outcome::Applies { key, limit },
            _ => Outcome::Malformed,
        }
    }
}

fn compile_key(checker: &Checker<'_>, source: &str) -> Result<Key, Error> {
    let node = syntax::parse(source)?;
    let (typed, ty) = checker.check(&node)?;
    if ty.said {
        return Err(Error {
            at: node.at,
            message: "a key must be what the gateway established — an authentication result or \
                      the client's address — not what the client said"
                .to_owned(),
        });
    }
    match typed {
        Typed::Text(TextNode::Literal(text)) => Ok(Key::Constant(text)),
        Typed::Text(text) => Ok(Key::Text(text)),
        Typed::Address(address) => Ok(Key::Address(address)),
        _ => Err(Error {
            at: node.at,
            message: "a key is a text or an address".to_owned(),
        }),
    }
}

fn compile_limit(checker: &Checker<'_>, source: &str) -> Result<IntNode, Error> {
    let node = syntax::parse(source)?;
    let (typed, ty) = checker.check(&node)?;
    let Typed::Int(limit) = typed else {
        return Err(Error {
            at: node.at,
            message: "a limit is a number".to_owned(),
        });
    };
    if ty.optional {
        return Err(Error {
            at: node.at,
            message: "may be missing: give it a default with `??`".to_owned(),
        });
    }
    if let IntNode::Literal(value) = limit
        && value < 1
    {
        return Err(Error {
            at: node.at,
            message: "a limit is at least 1".to_owned(),
        });
    }
    Ok(limit)
}

#[cfg(test)]
mod tests;
