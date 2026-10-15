//! A syntax tree checked against the context and compiled to typed nodes, which read the
//! context's fields by position and allocate nothing: what a request costs is a walk of a
//! tree whose size the parser has already bounded.
//!
//! A field that may be missing is *optional*. A comparison with a missing value is false
//! (`!=` true), as is a test of a missing text (`startsWith`); `has(x)` asks, and `x ?? y`
//! gives `y` in its place. A limit may not be missing: it needs its default.

use crate::Error;
use crate::context::{Address, Context, Named, Schema, Tables, Text};
use crate::syntax::{Expr, Node, Op};
use std::net::IpAddr;
use std::ops::Range;

/// What a value is, as the checker knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Kind {
    Text,
    Int,
    Bool,
    Address,
    List,
}

/// A value's type: its kind, whether it may be missing, and, for a text or an address,
/// whether any of it is what the client said rather than what the gateway established —
/// which only keys care about, and only those two kinds can be keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Type {
    kind: Kind,
    pub(crate) optional: bool,
    pub(crate) said: bool,
}

/// An address range, from `cidr("…")`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Network {
    address: IpAddr,
    prefix: u8,
}

impl Network {
    fn parse(text: &str) -> Option<Self> {
        let (address, prefix) = text.split_once('/')?;
        let address: IpAddr = address.parse().ok()?;
        let prefix: u8 = prefix.parse().ok()?;
        let most = if address.is_ipv4() { 32 } else { 128 };
        (prefix <= most).then_some(Self { address, prefix })
    }

    fn contains(&self, address: IpAddr) -> bool {
        // The leading `prefix` bits of each, as numbers of `width` bits.
        let lead = |bits: u128, width: u32| {
            let prefix = u32::from(self.prefix);
            if prefix == 0 {
                0
            } else {
                bits >> (width - prefix)
            }
        };
        match (self.address, address.to_canonical()) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                lead(u32::from(net).into(), 32) == lead(u32::from(ip).into(), 32)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => lead(net.into(), 128) == lead(ip.into(), 128),
            _ => false,
        }
    }
}

/// A text, compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TextNode {
    Literal(Box<str>),
    Field(Text),
    Named(Named, Box<str>),
    Default(Box<TextNode>, Box<TextNode>),
}

/// A number, compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum IntNode {
    Literal(i64),
    Lookup {
        table: usize,
        column: usize,
        key: TextNode,
    },
    Default(Box<IntNode>, Box<IntNode>),
}

/// An address, compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AddressNode {
    Field(Address),
    Default(Box<AddressNode>, Box<AddressNode>),
}

/// How two values compare.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Comparison {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

/// A test on a text.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TextTest {
    StartsWith,
    EndsWith,
    Contains,
}

/// A condition, compiled. The usual shapes — a field against a text in quotes, a chain of
/// `&&` or of `||` — have nodes of their own, so that deciding them is one step rather than
/// a walk.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum BoolNode {
    Literal(bool),
    Not(Box<BoolNode>),
    /// Every one holds: a chain of `&&`.
    All(Box<[BoolNode]>),
    /// One of them holds: a chain of `||`.
    Any(Box<[BoolNode]>),
    /// A text field is (or, not `equal`, is not) a text in quotes.
    FieldIs {
        field: Text,
        value: Box<str>,
        equal: bool,
    },
    /// A text field against a text in quotes.
    FieldTest(TextTest, Text, Box<str>),
    /// A text field is one of these.
    FieldIn(Text, Box<[Box<str>]>),
    Text(Comparison, TextNode, TextNode),
    Int(Comparison, IntNode, IntNode),
    Address(Comparison, AddressNode, AddressNode),
    TextIn(TextNode, Box<[Box<str>]>),
    AddressIn(AddressNode, Box<[Network]>),
    TextTest(TextTest, TextNode, TextNode),
    HasText(TextNode),
    HasInt(IntNode),
    HasAddress(AddressNode),
}

impl TextNode {
    pub(crate) fn evaluate<'a, C: Context>(&'a self, context: &'a C) -> Option<&'a str> {
        match self {
            Self::Literal(text) => Some(text),
            Self::Field(field) => context.text(*field),
            Self::Named(field, name) => context.named(*field, name),
            Self::Default(value, default) => value
                .evaluate(context)
                .or_else(|| default.evaluate(context)),
        }
    }
}

impl IntNode {
    pub(crate) fn evaluate<C: Context, T: Tables>(&self, context: &C, tables: &T) -> Option<i64> {
        match self {
            Self::Literal(value) => Some(*value),
            Self::Lookup { table, column, key } => {
                tables.number(*table, *column, key.evaluate(context)?)
            }
            Self::Default(value, default) => value
                .evaluate(context, tables)
                .or_else(|| default.evaluate(context, tables)),
        }
    }
}

impl AddressNode {
    pub(crate) fn evaluate<C: Context>(&self, context: &C) -> Option<IpAddr> {
        match self {
            Self::Field(field) => context.address(*field),
            Self::Default(value, default) => value
                .evaluate(context)
                .or_else(|| default.evaluate(context)),
        }
    }
}

fn compare<V: PartialOrd>(comparison: Comparison, left: Option<V>, right: Option<V>) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return comparison == Comparison::Ne;
    };
    match comparison {
        Comparison::Eq => left == right,
        Comparison::Ne => left != right,
        Comparison::Lt => left < right,
        Comparison::Le => left <= right,
        Comparison::Gt => left > right,
        Comparison::Ge => left >= right,
    }
}

impl BoolNode {
    pub(crate) fn evaluate<C: Context, T: Tables>(&self, context: &C, tables: &T) -> bool {
        match self {
            Self::Literal(value) => *value,
            Self::Not(operand) => !operand.evaluate(context, tables),
            Self::All(nodes) => nodes.iter().all(|node| node.evaluate(context, tables)),
            Self::Any(nodes) => nodes.iter().any(|node| node.evaluate(context, tables)),
            Self::FieldIs {
                field,
                value,
                equal,
            } => (context.text(*field) == Some(value)) == *equal,
            Self::FieldTest(test, field, part) => {
                context.text(*field).is_some_and(|text| match test {
                    TextTest::StartsWith => text.starts_with(&**part),
                    TextTest::EndsWith => text.ends_with(&**part),
                    TextTest::Contains => text.contains(&**part),
                })
            }
            Self::FieldIn(field, list) => context
                .text(*field)
                .is_some_and(|text| list.iter().any(|item| **item == *text)),
            Self::Text(comparison, left, right) => {
                compare(*comparison, left.evaluate(context), right.evaluate(context))
            }
            Self::Int(comparison, left, right) => compare(
                *comparison,
                left.evaluate(context, tables),
                right.evaluate(context, tables),
            ),
            Self::Address(comparison, left, right) => {
                compare(*comparison, left.evaluate(context), right.evaluate(context))
            }
            Self::TextIn(text, list) => text
                .evaluate(context)
                .is_some_and(|text| list.iter().any(|item| **item == *text)),
            Self::AddressIn(address, networks) => address
                .evaluate(context)
                .is_some_and(|address| networks.iter().any(|net| net.contains(address))),
            Self::TextTest(test, text, part) => {
                match (text.evaluate(context), part.evaluate(context)) {
                    (Some(text), Some(part)) => match test {
                        TextTest::StartsWith => text.starts_with(part),
                        TextTest::EndsWith => text.ends_with(part),
                        TextTest::Contains => text.contains(part),
                    },
                    _ => false,
                }
            }
            Self::HasText(text) => text.evaluate(context).is_some(),
            Self::HasInt(int) => int.evaluate(context, tables).is_some(),
            Self::HasAddress(address) => address.evaluate(context).is_some(),
        }
    }
}

/// A checked, compiled expression of some kind.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Typed {
    Text(TextNode),
    Int(IntNode),
    Bool(BoolNode),
    Address(AddressNode),
    TextList(Box<[Box<str>]>),
    Networks(Box<[Network]>),
}

fn error(at: Range<usize>, message: impl Into<String>) -> Error {
    Error {
        at,
        message: message.into(),
    }
}

/// The context's fields by the names expressions use, with whether each may be missing and
/// whether it is what the client said.
fn field(path: &str) -> Option<(Typed, Type)> {
    let text = |field, optional, said| {
        Some((
            Typed::Text(TextNode::Field(field)),
            Type {
                kind: Kind::Text,
                optional,
                said,
            },
        ))
    };
    let address = |field| {
        Some((
            Typed::Address(AddressNode::Field(field)),
            Type {
                kind: Kind::Address,
                optional: false,
                said: false,
            },
        ))
    };
    match path {
        "listener" => text(Text::Listener, false, false),
        "route" => text(Text::Route, false, false),
        "protocol" => text(Text::Protocol, false, false),
        "id" => text(Text::Id, true, false),
        "method" => text(Text::Method, false, true),
        "host" => text(Text::Host, false, true),
        "path" => text(Text::Path, false, true),
        "tls.sni" => text(Text::TlsSni, true, true),
        "tls.version" => text(Text::TlsVersion, true, false),
        "tls.peer.subject" => text(Text::TlsPeerSubject, true, false),
        "auth.api_key" => text(Text::ApiKey, true, false),
        "auth.account" => text(Text::Account, true, false),
        "client" => address(Address::Client),
        "peer" => address(Address::Peer),
        _ => None,
    }
}

/// The fields of the context holding values by name, by the names expressions use.
fn named(path: &str) -> Option<(Named, bool)> {
    match path {
        "headers" => Some((Named::Header, true)),
        "query" => Some((Named::Query, true)),
        "cookies" => Some((Named::Cookie, true)),
        "auth.claims" => Some((Named::Claim, false)),
        _ => None,
    }
}

/// `a.b.c` as the dotted name it spells, if it is one.
fn dotted(node: &Node) -> Option<String> {
    match &node.expr {
        Expr::Ident(name) => Some(name.clone()),
        Expr::Member(base, name) => Some(format!("{}.{name}", dotted(base)?)),
        _ => None,
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Text => "a text",
        Kind::Int => "a number",
        Kind::Bool => "a condition",
        Kind::Address => "an address",
        Kind::List => "a list",
    }
}

/// Checks and compiles one expression against the context and `schema`.
pub(crate) struct Checker<'s> {
    pub(crate) schema: &'s Schema,
}

impl Checker<'_> {
    pub(crate) fn check(&self, node: &Node) -> Result<(Typed, Type), Error> {
        let at = node.at.clone();
        match &node.expr {
            Expr::Str(text) => Ok((
                Typed::Text(TextNode::Literal(text.as_str().into())),
                Type {
                    kind: Kind::Text,
                    optional: false,
                    said: false,
                },
            )),
            Expr::Int(value) => Ok((
                Typed::Int(IntNode::Literal(*value)),
                Type {
                    kind: Kind::Int,
                    optional: false,
                    said: false,
                },
            )),
            Expr::Bool(value) => Ok((Typed::Bool(BoolNode::Literal(*value)), boolean())),
            Expr::Ident(_) | Expr::Member(..) => self.field(node),
            Expr::Index(base, index) => self.index(base, index, at),
            Expr::Call(name, args) => self.call(name, args, at),
            Expr::Method(base, name, args) => self.method(base, name, args, at),
            Expr::List(items) => self.list(items, at),
            Expr::Not(operand) => {
                let (operand, ty) = self.condition(operand)?;
                Ok((Typed::Bool(BoolNode::Not(Box::new(operand))), ty))
            }
            Expr::Binary(op, left, right) => self.binary(*op, left, right, at),
        }
    }

    /// A condition, or an error saying what it is instead.
    pub(crate) fn condition(&self, node: &Node) -> Result<(BoolNode, Type), Error> {
        match self.check(node)? {
            (Typed::Bool(node), ty) => Ok((node, ty)),
            (_, ty) => Err(error(
                node.at.clone(),
                format!("expected a condition, not {}", kind_name(ty.kind)),
            )),
        }
    }

    fn field(&self, node: &Node) -> Result<(Typed, Type), Error> {
        let at = node.at.clone();
        // `table("t")[key].column`
        if let Expr::Member(base, column) = &node.expr
            && let Expr::Index(table, key) = &base.expr
            && let Expr::Call(function, args) = &table.expr
            && function == "table"
        {
            return self.lookup(args, key, column, at);
        }
        let Some(path) = dotted(node) else {
            return Err(error(at, "expected a field of the request"));
        };
        if let Some(found) = field(&path) {
            return Ok(found);
        }
        if named(&path).is_some() {
            return Err(error(
                at,
                format!("`{path}` holds values by name: `{path}[\"name\"]`"),
            ));
        }
        Err(error(at, format!("the request has no field `{path}`")))
    }

    fn index(&self, base: &Node, index: &Node, at: Range<usize>) -> Result<(Typed, Type), Error> {
        let Some(path) = dotted(base) else {
            return Err(error(
                at,
                "only `headers`, `query`, `cookies` and `auth.claims` take `[…]`",
            ));
        };
        let Some((field, said)) = named(&path) else {
            return Err(error(
                base.at.clone(),
                format!("`{path}` does not hold values by name"),
            ));
        };
        let Expr::Str(name) = &index.expr else {
            return Err(error(
                index.at.clone(),
                "a name in quotes: values are looked up by a name the rule gives",
            ));
        };
        let name = if field == Named::Header {
            name.to_ascii_lowercase()
        } else {
            name.clone()
        };
        Ok((
            Typed::Text(TextNode::Named(field, name.into())),
            Type {
                kind: Kind::Text,
                optional: true,
                said,
            },
        ))
    }

    fn lookup(
        &self,
        args: &[Node],
        key: &Node,
        column: &str,
        at: Range<usize>,
    ) -> Result<(Typed, Type), Error> {
        let [table_name] = args else {
            return Err(error(at, "`table` takes one table's name"));
        };
        let Expr::Str(name) = &table_name.expr else {
            return Err(error(table_name.at.clone(), "a table's name in quotes"));
        };
        let Some(table) = self.schema.tables.iter().position(|(t, _)| t == name) else {
            return Err(error(table_name.at.clone(), format!("no table `{name}`")));
        };
        let Some(column_at) = self.schema.tables[table].1.iter().position(|c| c == column) else {
            return Err(error(
                at,
                format!("table `{name}` has no column `{column}`"),
            ));
        };
        let (Typed::Text(key_node), _) = self.check(key)? else {
            return Err(error(key.at.clone(), "a table is looked up by a text"));
        };
        Ok((
            Typed::Int(IntNode::Lookup {
                table,
                column: column_at,
                key: key_node,
            }),
            Type {
                kind: Kind::Int,
                optional: true,
                said: false,
            },
        ))
    }

    fn call(&self, name: &str, args: &[Node], at: Range<usize>) -> Result<(Typed, Type), Error> {
        match name {
            "has" => {
                let [arg] = args else {
                    return Err(error(at, "`has` takes one value"));
                };
                let (typed, ty) = self.check(arg)?;
                if !ty.optional {
                    return Err(error(
                        arg.at.clone(),
                        "is never missing: `has` would always be true",
                    ));
                }
                let node = match typed {
                    Typed::Text(node) => BoolNode::HasText(node),
                    Typed::Int(node) => BoolNode::HasInt(node),
                    Typed::Address(node) => BoolNode::HasAddress(node),
                    _ => {
                        return Err(error(
                            arg.at.clone(),
                            "expected a value that may be missing",
                        ));
                    }
                };
                Ok((Typed::Bool(node), boolean()))
            }
            "cidr" => {
                let network = self.network(name, args, at)?;
                Ok((Typed::Networks(Box::new([network])), list()))
            }
            "table" => Err(error(at, "a table's value: `table(\"name\")[key].column`")),
            _ => Err(error(at, format!("no function `{name}`"))),
        }
    }

    fn network(&self, name: &str, args: &[Node], at: Range<usize>) -> Result<Network, Error> {
        let [arg] = args else {
            return Err(error(at, format!("`{name}` takes one range in quotes")));
        };
        let Expr::Str(text) = &arg.expr else {
            return Err(error(
                arg.at.clone(),
                "a range in quotes, as \"10.0.0.0/8\"",
            ));
        };
        Network::parse(text)
            .ok_or_else(|| error(arg.at.clone(), format!("`{text}` is not an address range")))
    }

    fn method(
        &self,
        base: &Node,
        name: &str,
        args: &[Node],
        at: Range<usize>,
    ) -> Result<(Typed, Type), Error> {
        let test = match name {
            "startsWith" => TextTest::StartsWith,
            "endsWith" => TextTest::EndsWith,
            "contains" => TextTest::Contains,
            _ => return Err(error(at, format!("no method `{name}`"))),
        };
        let [arg] = args else {
            return Err(error(at, format!("`{name}` takes one text")));
        };
        let (Typed::Text(text), _) = self.check(base)? else {
            return Err(error(base.at.clone(), format!("`{name}` is a text's")));
        };
        let (Typed::Text(part), _) = self.check(arg)? else {
            return Err(error(arg.at.clone(), "expected a text"));
        };
        let node = match (text, part) {
            (TextNode::Field(field), TextNode::Literal(part)) => {
                BoolNode::FieldTest(test, field, part)
            }
            (text, part) => BoolNode::TextTest(test, text, part),
        };
        Ok((Typed::Bool(node), boolean()))
    }

    fn list(&self, items: &[Node], at: Range<usize>) -> Result<(Typed, Type), Error> {
        let mut texts = Vec::new();
        let mut networks = Vec::new();
        for item in items {
            match &item.expr {
                Expr::Str(text) => texts.push(text.as_str().into()),
                Expr::Call(name, args) if name == "cidr" => {
                    networks.push(self.network(name, args, item.at.clone())?);
                }
                _ => {
                    return Err(error(
                        item.at.clone(),
                        "a list holds texts in quotes, or `cidr(…)` ranges",
                    ));
                }
            }
        }
        match (texts.is_empty(), networks.is_empty()) {
            (_, true) => Ok((Typed::TextList(texts.into()), list())),
            (true, false) => Ok((Typed::Networks(networks.into()), list())),
            (false, false) => Err(error(at, "a list of texts and ranges both")),
        }
    }

    fn binary(
        &self,
        op: Op,
        left: &Node,
        right: &Node,
        at: Range<usize>,
    ) -> Result<(Typed, Type), Error> {
        if matches!(op, Op::And | Op::Or) {
            let (l, _) = self.condition(left)?;
            let (r, _) = self.condition(right)?;
            // `a && b && c` is one chain of three, not two of two.
            let all = op == Op::And;
            let mut nodes = Vec::new();
            for side in [l, r] {
                match side {
                    BoolNode::All(chain) if all => nodes.extend(chain),
                    BoolNode::Any(chain) if !all => nodes.extend(chain),
                    other => nodes.push(other),
                }
            }
            let node = if all {
                BoolNode::All(nodes.into())
            } else {
                BoolNode::Any(nodes.into())
            };
            return Ok((Typed::Bool(node), boolean()));
        }
        let (l, lt) = self.check(left)?;
        let (r, rt) = self.check(right)?;
        let said = lt.said || rt.said;
        if op == Op::In {
            let node = match (l, r) {
                (Typed::Text(TextNode::Field(field)), Typed::TextList(list)) => {
                    BoolNode::FieldIn(field, list)
                }
                (Typed::Text(text), Typed::TextList(list)) => BoolNode::TextIn(text, list),
                (Typed::Address(address), Typed::Networks(networks)) => {
                    BoolNode::AddressIn(address, networks)
                }
                _ => {
                    return Err(error(
                        at,
                        "`in` takes a text and a list of texts, or an address and ranges",
                    ));
                }
            };
            return Ok((Typed::Bool(node), boolean()));
        }
        if op == Op::Coalesce {
            if !lt.optional {
                return Err(error(
                    left.at.clone(),
                    "is never missing: `??` would never be used",
                ));
            }
            let ty = Type {
                kind: lt.kind,
                optional: rt.optional,
                said,
            };
            let typed = match (l, r) {
                (Typed::Text(a), Typed::Text(b)) => {
                    Typed::Text(TextNode::Default(Box::new(a), Box::new(b)))
                }
                (Typed::Int(a), Typed::Int(b)) => {
                    Typed::Int(IntNode::Default(Box::new(a), Box::new(b)))
                }
                (Typed::Address(a), Typed::Address(b)) => {
                    Typed::Address(AddressNode::Default(Box::new(a), Box::new(b)))
                }
                _ => {
                    return Err(error(
                        right.at.clone(),
                        format!("a default for {} must be one too", kind_name(lt.kind)),
                    ));
                }
            };
            return Ok((typed, ty));
        }
        let comparison = match op {
            Op::Eq => Comparison::Eq,
            Op::Ne => Comparison::Ne,
            Op::Lt => Comparison::Lt,
            Op::Le => Comparison::Le,
            Op::Gt => Comparison::Gt,
            _ => Comparison::Ge,
        };
        let ordered = !matches!(comparison, Comparison::Eq | Comparison::Ne);
        let node = match (l, r) {
            (Typed::Text(TextNode::Field(field)), Typed::Text(TextNode::Literal(value)))
            | (Typed::Text(TextNode::Literal(value)), Typed::Text(TextNode::Field(field)))
                if !ordered =>
            {
                BoolNode::FieldIs {
                    field,
                    value,
                    equal: comparison == Comparison::Eq,
                }
            }
            (Typed::Text(a), Typed::Text(b)) if !ordered => BoolNode::Text(comparison, a, b),
            (Typed::Int(a), Typed::Int(b)) => BoolNode::Int(comparison, a, b),
            (Typed::Address(a), Typed::Address(b)) if !ordered => {
                BoolNode::Address(comparison, a, b)
            }
            (Typed::Bool(_), _) | (_, Typed::Bool(_)) => {
                return Err(error(
                    at,
                    "conditions are combined with `&&`, `||` and `!`, not compared",
                ));
            }
            _ if ordered && lt.kind == rt.kind => {
                return Err(error(at, format!("{} has no order", kind_name(lt.kind))));
            }
            _ => {
                return Err(error(
                    at,
                    format!(
                        "{} compared with {}",
                        kind_name(lt.kind),
                        kind_name(rt.kind)
                    ),
                ));
            }
        };
        Ok((Typed::Bool(node), boolean()))
    }
}

fn list() -> Type {
    Type {
        kind: Kind::List,
        optional: false,
        said: false,
    }
}

fn boolean() -> Type {
    Type {
        kind: Kind::Bool,
        optional: false,
        said: false,
    }
}
