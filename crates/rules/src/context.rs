//! The request context (07 §2): what an expression can read about a request, and the
//! tables it can look values up in. The caller implements both over what it holds; every
//! answer is borrowed from it.

use std::net::IpAddr;

/// A field of the context holding text.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Text {
    /// The listener's name.
    Listener,
    /// The route's name.
    Route,
    /// `1.0`, `1.1`, `2` or `3`.
    Protocol,
    /// The request's ID, where its listener gives one.
    Id,
    /// The method.
    Method,
    /// The host, as routed on.
    Host,
    /// The normalised path, as routed on, without the query.
    Path,
    /// The name the TLS ClientHello asked for.
    TlsSni,
    /// The TLS version agreed.
    TlsVersion,
    /// The subject of the client's verified certificate.
    TlsPeerSubject,
    /// The verified API key (stage 9).
    ApiKey,
    /// The account behind it (stage 9).
    Account,
}

/// A field of the context holding an address.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Address {
    /// The client's address, as worked out (PROXY header, trusted forwarding).
    Client,
    /// The address that connected.
    Peer,
}

/// A field of the context holding values by name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Named {
    /// Request headers, by lower-case name.
    Header,
    /// The query's parameters.
    Query,
    /// Cookies.
    Cookie,
    /// Verified token claims (stage 9).
    Claim,
}

/// What a request holds, as expressions read it: every answer borrowed from the request.
pub trait Context {
    /// A text field, if the request has it.
    fn text(&self, field: Text) -> Option<&str>;

    /// An address field, if the request has it.
    fn address(&self, field: Address) -> Option<IpAddr>;

    /// A value by name, if the request has one: for headers, the first of that name.
    fn named(&self, field: Named, name: &str) -> Option<&str>;
}

/// The tables an expression may look values up in, as the config declares them.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Schema {
    /// Each table, with the names of its numeric columns.
    pub tables: Vec<(String, Vec<String>)>,
    /// Whether a key may be what the client said — a header — rather than only what the
    /// gateway established: an experiment, behind the data plane's switch (07 §2).
    pub said_keys: bool,
}

/// The tables' current data, by the positions [`Schema`] gives.
pub trait Tables {
    /// The value in `column` of `table`'s row for `key`, if there is one.
    fn number(&self, table: usize, column: usize, key: &str) -> Option<i64>;
}
