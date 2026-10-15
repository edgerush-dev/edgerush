//! An expression's text to its syntax tree: CEL's syntax for the part of it limit rules
//! need, with `??` for a default. Bounded: a source, its nesting and its nodes are capped,
//! so that what a config can ask to be compiled is too.

use crate::Error;
use std::ops::Range;

/// The longest source accepted, in bytes.
pub(crate) const MAX_SOURCE: usize = 4096;
/// The deepest nesting accepted.
pub(crate) const MAX_DEPTH: usize = 32;
/// The most nodes an expression may have.
pub(crate) const MAX_NODES: usize = 256;

/// A binary operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Op {
    Or,
    And,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    In,
    Coalesce,
}

/// A node of the tree, with the source it came from.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Node {
    pub(crate) expr: Expr,
    pub(crate) at: Range<usize>,
}

/// What a node is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Expr {
    Str(String),
    Int(i64),
    Bool(bool),
    Ident(String),
    /// `base.name`
    Member(Box<Node>, String),
    /// `base[index]`
    Index(Box<Node>, Box<Node>),
    /// `name(args)`
    Call(String, Vec<Node>),
    /// `base.name(args)`
    Method(Box<Node>, String, Vec<Node>),
    /// `[items]`
    List(Vec<Node>),
    /// `!operand`
    Not(Box<Node>),
    Binary(Op, Box<Node>, Box<Node>),
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Token {
    Str(String),
    Int(i64),
    Ident(String),
    Dot,
    Comma,
    Open,
    Close,
    OpenSquare,
    CloseSquare,
    Bang,
    And,
    Or,
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
    Coalesce,
}

fn error(at: Range<usize>, message: impl Into<String>) -> Error {
    Error {
        at,
        message: message.into(),
    }
}

fn lex(source: &str) -> Result<Vec<(Token, Range<usize>)>, Error> {
    let bytes = source.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let start = i;
        let byte = bytes[i];
        if byte.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        let two = bytes.get(i..i + 2);
        let token = match (byte, two) {
            (_, Some(b"&&")) => Some((Token::And, 2)),
            (_, Some(b"||")) => Some((Token::Or, 2)),
            (_, Some(b"==")) => Some((Token::Eq, 2)),
            (_, Some(b"!=")) => Some((Token::Ne, 2)),
            (_, Some(b"<=")) => Some((Token::Le, 2)),
            (_, Some(b">=")) => Some((Token::Ge, 2)),
            (_, Some(b"??")) => Some((Token::Coalesce, 2)),
            (b'.', _) => Some((Token::Dot, 1)),
            (b',', _) => Some((Token::Comma, 1)),
            (b'(', _) => Some((Token::Open, 1)),
            (b')', _) => Some((Token::Close, 1)),
            (b'[', _) => Some((Token::OpenSquare, 1)),
            (b']', _) => Some((Token::CloseSquare, 1)),
            (b'!', _) => Some((Token::Bang, 1)),
            (b'<', _) => Some((Token::Lt, 1)),
            (b'>', _) => Some((Token::Gt, 1)),
            _ => None,
        };
        if let Some((token, length)) = token {
            i += length;
            tokens.push((token, start..i));
            continue;
        }
        if byte == b'"' {
            let mut text = String::new();
            i += 1;
            loop {
                match bytes.get(i) {
                    None => return Err(error(start..i, "a string without its closing quote")),
                    Some(b'"') => {
                        i += 1;
                        break;
                    }
                    Some(b'\\') => {
                        let escaped = match bytes.get(i + 1) {
                            Some(b'"') => '"',
                            Some(b'\\') => '\\',
                            _ => {
                                // The backslash and the whole character after it.
                                let after = source[i + 1..].chars().next();
                                let end = i + 1 + after.map_or(0, char::len_utf8);
                                return Err(error(i..end, "an escape other than \\\" or \\\\"));
                            }
                        };
                        text.push(escaped);
                        i += 2;
                    }
                    Some(_) => {
                        // A whole character, however many bytes it takes.
                        let rest = &source[i..];
                        let Some(c) = rest.chars().next() else {
                            return Err(error(start..i, "a string without its closing quote"));
                        };
                        text.push(c);
                        i += c.len_utf8();
                    }
                }
            }
            tokens.push((Token::Str(text), start..i));
            continue;
        }
        if byte.is_ascii_digit() {
            while bytes.get(i).is_some_and(u8::is_ascii_digit) {
                i += 1;
            }
            let value = source[start..i]
                .parse::<i64>()
                .map_err(|_| error(start..i, "a number too large"))?;
            tokens.push((Token::Int(value), start..i));
            continue;
        }
        if byte.is_ascii_alphabetic() || byte == b'_' {
            while bytes
                .get(i)
                .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
            {
                i += 1;
            }
            tokens.push((Token::Ident(source[start..i].to_owned()), start..i));
            continue;
        }
        let length = source[i..].chars().next().map_or(1, char::len_utf8);
        return Err(error(
            start..start + length,
            "a character no expression has",
        ));
    }
    Ok(tokens)
}

struct Parser {
    tokens: Vec<(Token, Range<usize>)>,
    next: usize,
    depth: usize,
    nodes: usize,
    end: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.next).map(|(token, _)| token)
    }

    fn at(&self) -> Range<usize> {
        self.tokens
            .get(self.next)
            .map_or(self.end..self.end, |(_, at)| at.clone())
    }

    fn take(&mut self, wanted: &Token) -> bool {
        if self.peek() == Some(wanted) {
            self.next += 1;
            true
        } else {
            false
        }
    }

    fn expect(&mut self, wanted: &Token, what: &str) -> Result<usize, Error> {
        let at = self.at();
        if self.take(wanted) {
            Ok(at.end)
        } else {
            Err(error(at, format!("expected {what}")))
        }
    }

    fn node(&mut self, expr: Expr, at: Range<usize>) -> Result<Node, Error> {
        self.nodes += 1;
        if self.nodes > MAX_NODES {
            return Err(error(at, format!("more than {MAX_NODES} parts")));
        }
        Ok(Node { expr, at })
    }

    fn nested<T>(&mut self, parse: impl FnOnce(&mut Self) -> Result<T, Error>) -> Result<T, Error> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(error(self.at(), format!("nested deeper than {MAX_DEPTH}")));
        }
        let result = parse(self);
        self.depth -= 1;
        result
    }

    fn expression(&mut self) -> Result<Node, Error> {
        self.nested(Self::coalesce)
    }

    /// `a ?? b ?? c` is `a ?? (b ?? c)`.
    fn coalesce(&mut self) -> Result<Node, Error> {
        let left = self.or()?;
        if self.take(&Token::Coalesce) {
            let right = self.nested(Self::coalesce)?;
            let at = left.at.start..right.at.end;
            return self.node(
                Expr::Binary(Op::Coalesce, Box::new(left), Box::new(right)),
                at,
            );
        }
        Ok(left)
    }

    fn or(&mut self) -> Result<Node, Error> {
        let mut left = self.and()?;
        while self.take(&Token::Or) {
            let right = self.and()?;
            let at = left.at.start..right.at.end;
            left = self.node(Expr::Binary(Op::Or, Box::new(left), Box::new(right)), at)?;
        }
        Ok(left)
    }

    fn and(&mut self) -> Result<Node, Error> {
        let mut left = self.comparison()?;
        while self.take(&Token::And) {
            let right = self.comparison()?;
            let at = left.at.start..right.at.end;
            left = self.node(Expr::Binary(Op::And, Box::new(left), Box::new(right)), at)?;
        }
        Ok(left)
    }

    /// One comparison at most: `a == b == c` is refused, as it means nothing here.
    fn comparison(&mut self) -> Result<Node, Error> {
        let left = self.unary()?;
        let op = match self.peek() {
            Some(Token::Eq) => Op::Eq,
            Some(Token::Ne) => Op::Ne,
            Some(Token::Lt) => Op::Lt,
            Some(Token::Le) => Op::Le,
            Some(Token::Gt) => Op::Gt,
            Some(Token::Ge) => Op::Ge,
            Some(Token::Ident(word)) if word == "in" => Op::In,
            _ => return Ok(left),
        };
        self.next += 1;
        let right = self.unary()?;
        let at = left.at.start..right.at.end;
        self.node(Expr::Binary(op, Box::new(left), Box::new(right)), at)
    }

    fn unary(&mut self) -> Result<Node, Error> {
        let start = self.at().start;
        if self.take(&Token::Bang) {
            let operand = self.nested(Self::unary)?;
            let at = start..operand.at.end;
            return self.node(Expr::Not(Box::new(operand)), at);
        }
        self.postfix()
    }

    fn postfix(&mut self) -> Result<Node, Error> {
        let mut base = self.primary()?;
        loop {
            if self.take(&Token::Dot) {
                let at = self.at();
                let Some(Token::Ident(name)) = self.peek().cloned() else {
                    return Err(error(at, "expected a name after `.`"));
                };
                self.next += 1;
                if self.peek() == Some(&Token::Open) {
                    let (args, end) = self.arguments()?;
                    let at = base.at.start..end;
                    base = self.node(Expr::Method(Box::new(base), name, args), at)?;
                } else {
                    let span = base.at.start..at.end;
                    base = self.node(Expr::Member(Box::new(base), name), span)?;
                }
            } else if self.take(&Token::OpenSquare) {
                let index = self.expression()?;
                let end = self.expect(&Token::CloseSquare, "`]`")?;
                let at = base.at.start..end;
                base = self.node(Expr::Index(Box::new(base), Box::new(index)), at)?;
            } else {
                return Ok(base);
            }
        }
    }

    /// `( a, b, … )`, the opening bracket next.
    fn arguments(&mut self) -> Result<(Vec<Node>, usize), Error> {
        self.expect(&Token::Open, "`(`")?;
        let mut args = Vec::new();
        if let Some(end) = self.closing(&Token::Close) {
            return Ok((args, end));
        }
        loop {
            args.push(self.expression()?);
            if let Some(end) = self.closing(&Token::Close) {
                return Ok((args, end));
            }
            self.expect(&Token::Comma, "`,` or `)`")?;
        }
    }

    fn closing(&mut self, close: &Token) -> Option<usize> {
        let end = self.at().end;
        self.take(close).then_some(end)
    }

    fn primary(&mut self) -> Result<Node, Error> {
        let at = self.at();
        let Some(token) = self.peek().cloned() else {
            return Err(error(at, "expected a value"));
        };
        self.next += 1;
        match token {
            Token::Str(text) => self.node(Expr::Str(text), at),
            Token::Int(value) => self.node(Expr::Int(value), at),
            Token::Ident(word) if word == "true" => self.node(Expr::Bool(true), at),
            Token::Ident(word) if word == "false" => self.node(Expr::Bool(false), at),
            Token::Ident(word) if word == "in" => Err(error(at, "expected a value, not `in`")),
            Token::Ident(name) => {
                if self.peek() == Some(&Token::Open) {
                    let (args, end) = self.arguments()?;
                    self.node(Expr::Call(name, args), at.start..end)
                } else {
                    self.node(Expr::Ident(name), at)
                }
            }
            Token::Open => {
                let mut inner = self.expression()?;
                let end = self.expect(&Token::Close, "`)`")?;
                // The brackets are part of what it is, for whatever reports on it.
                inner.at = at.start..end;
                Ok(inner)
            }
            Token::OpenSquare => {
                let mut items = Vec::new();
                let end = if let Some(end) = self.closing(&Token::CloseSquare) {
                    end
                } else {
                    loop {
                        items.push(self.expression()?);
                        if let Some(end) = self.closing(&Token::CloseSquare) {
                            break end;
                        }
                        self.expect(&Token::Comma, "`,` or `]`")?;
                    }
                };
                self.node(Expr::List(items), at.start..end)
            }
            _ => Err(error(at, "expected a value")),
        }
    }
}

/// Parses `source` into its tree.
pub(crate) fn parse(source: &str) -> Result<Node, Error> {
    if source.len() > MAX_SOURCE {
        return Err(error(
            0..source.len(),
            format!("longer than {MAX_SOURCE} bytes"),
        ));
    }
    let mut parser = Parser {
        tokens: lex(source)?,
        next: 0,
        depth: 0,
        nodes: 0,
        end: source.len(),
    };
    let node = parser.expression()?;
    if parser.next < parser.tokens.len() {
        return Err(error(parser.at(), "expected the end"));
    }
    Ok(node)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn shape(node: &Node) -> String {
        match &node.expr {
            Expr::Str(text) => format!("{text:?}"),
            Expr::Int(value) => value.to_string(),
            Expr::Bool(value) => value.to_string(),
            Expr::Ident(name) => name.clone(),
            Expr::Member(base, name) => format!("{}.{name}", shape(base)),
            Expr::Index(base, index) => format!("{}[{}]", shape(base), shape(index)),
            Expr::Call(name, args) => format!("{name}({})", list(args)),
            Expr::Method(base, name, args) => format!("{}.{name}({})", shape(base), list(args)),
            Expr::List(items) => format!("[{}]", list(items)),
            Expr::Not(operand) => format!("!{}", shape(operand)),
            Expr::Binary(op, left, right) => format!("({} {op:?} {})", shape(left), shape(right)),
        }
    }

    fn list(nodes: &[Node]) -> String {
        nodes.iter().map(shape).collect::<Vec<_>>().join(", ")
    }

    #[test]
    fn precedence_is_cels_with_coalesce_loosest() {
        let node = parse(r#"a || b && c == "x" ?? d ?? e"#).unwrap();
        assert_eq!(
            shape(&node),
            r#"((a Or (b And (c Eq "x"))) Coalesce (d Coalesce e))"#
        );
        let node = parse(r#"!has(auth.api_key) && path.startsWith("/v1/")"#).unwrap();
        assert_eq!(
            shape(&node),
            r#"(!has(auth.api_key) And path.startsWith("/v1/"))"#
        );
        let node = parse(r#"table("accounts")[auth.account].rps ?? 10"#).unwrap();
        assert_eq!(
            shape(&node),
            r#"(table("accounts")[auth.account].rps Coalesce 10)"#
        );
        let node = parse(r#"method in ["GET", "HEAD"]"#).unwrap();
        assert_eq!(shape(&node), r#"(method In ["GET", "HEAD"])"#);
    }

    #[test]
    fn spans_point_at_the_source() {
        let source = r#"method == "POST""#;
        let node = parse(source).unwrap();
        assert_eq!(node.at, 0..source.len());
        let Expr::Binary(_, left, right) = node.expr else {
            panic!("a comparison");
        };
        assert_eq!(&source[left.at], "method");
        assert_eq!(&source[right.at], r#""POST""#);
    }

    #[test]
    fn malformed_sources_are_refused_where_they_go_wrong() {
        for (source, at, message) in [
            (r#"method == "POST"#, 10..15, "closing quote"),
            ("\"\\", 1..2, "escape"),
            ("\"\\é\"", 1..4, "escape"),
            ("method == POST ==", 15..17, "expected the end"),
            ("path.startsWith(", 16..16, "expected a value"),
            ("headers[", 8..8, "expected a value"),
            ("a # b", 2..3, "character"),
            ("99999999999999999999", 0..20, "too large"),
            (r#""\n""#, 1..3, "escape"),
            ("a.", 2..2, "a name after"),
        ] {
            let err = parse(source).unwrap_err();
            assert_eq!(err.at, at, "{source}: {err}");
            assert!(err.message.contains(message), "{source}: {err}");
        }
    }

    #[test]
    fn what_can_be_parsed_is_bounded() {
        let deep = format!(
            "{}a{}",
            "(".repeat(MAX_DEPTH + 1),
            ")".repeat(MAX_DEPTH + 1)
        );
        assert!(parse(&deep).unwrap_err().message.contains("nested"));
        let wide = vec!["a"; MAX_NODES + 1].join(" || ");
        assert!(parse(&wide).unwrap_err().message.contains("parts"));
        let long = "a".repeat(MAX_SOURCE + 1);
        assert!(parse(&long).unwrap_err().message.contains("longer"));
        let nots = format!("{}a", "!".repeat(MAX_DEPTH + 1));
        assert!(parse(&nots).unwrap_err().message.contains("nested"));
    }
}
