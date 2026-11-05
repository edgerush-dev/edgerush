//! TCPRoutes and the data plane's Gateway's TCP listeners
//! ([04 §2](../../../docs/04-control-plane.md)): each accepted TCP listener is a `tcp`
//! listener of the data plane's (17), which has one TCP route; until a TCPRoute serves it,
//! that route closes every connection, so that the port is bound as every accepted
//! listener's is (Lucas, Q-c2).

use crate::gateway::GatewayOutcome;
use edgerush_config::{Backend, Protocol, TcpRoute};

/// The TCP route each accepted TCP listener of `gateway` is claimed by, its connections
/// closed: `gateway/<namespace>/<name>/<listener>` (or `listenerset/…`), its one backend
/// invalid.
pub(crate) fn claims(gateway: &GatewayOutcome) -> Vec<TcpRoute> {
    gateway
        .listeners
        .iter()
        .filter(|listener| gateway.ports.get(&listener.port) == Some(&Protocol::Tcp))
        .filter_map(|listener| {
            let served = listener.served.as_ref().ok()?;
            let stater = listener.set.as_ref().unwrap_or(&gateway.gateway);
            let kind = if listener.set.is_some() {
                "listenerset"
            } else {
                "gateway"
            };
            Some(TcpRoute {
                name: format!(
                    "{kind}/{}/{}/{}",
                    stater.namespace, stater.name, listener.name
                ),
                listeners: served.clone(),
                backends: vec![Backend {
                    upstream: None,
                    invalid: Some(format!("no TCPRoute serves listener {}", listener.name)),
                    weight: 1,
                }],
            })
        })
        .collect()
}
