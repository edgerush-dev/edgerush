//! The whole of what a data plane is given to run.

use crate::Route;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::SocketAddr;

/// A data plane's configuration. Listeners and filters are to come.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The routes, in order of precedence among otherwise equal matches.
    pub routes: Vec<Route>,
    /// The upstreams that backends refer to, by name. They have no order.
    pub upstreams: BTreeMap<String, Upstream>,
}

/// A set of endpoints that serve the same thing.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Upstream {
    /// Where to connect: addresses, not names. None is allowed, and means there is nothing
    /// to send a request to — a state a running system passes through, not a mistake.
    pub endpoints: Vec<SocketAddr>,
}
