//! What the upstream is told of a request's client (03 §11 in the docs), done to a head of
//! either kind before it is routed, so that a rule's predicates read what the upstream will.
//!
//! From a peer that is not a trusted proxy, the headers only a trusted proxy may send are
//! taken off, and `X-Forwarded-For`, `-Proto` and `-Host` say the gateway's own: the peer,
//! the listener's scheme, the host the client asked for. From a trusted proxy,
//! `X-Forwarded-For` is the client its chain names, and `-Proto` and `-Host` are kept as it
//! sent them, or set if it sent none. `X-Forwarded-For` is always one address, never a
//! chain, so that a backend that reads its first entry reads the client. `Via` gets the
//! gateway's entry after whatever came before it (RFC 9110 §7.6.3).
//!
//! A request given an ID (08 §3) has it in `X-Request-ID`, in place of whatever it came
//! with, from a trusted proxy too.
//!
//! A rule's own changes to the headers come after, and may change any of these but the ID.

use crate::head::Head;
use crate::request::Rejection;
use edgerush_config::{CompiledListener, Protocol};
use edgerush_filters::forwarding::{address_value, client_address, proto_value, via_value};
use edgerush_filters::request_id;
use edgerush_router::Fields;
use http::header::{HeaderName, HeaderValue, VIA};
use std::cell::Cell;
use std::net::{IpAddr, SocketAddr};

/// `X-Forwarded-For`.
pub(crate) const FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
/// `X-Forwarded-Proto`.
pub(crate) const FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
/// `X-Forwarded-Host`.
pub(crate) const FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");

/// The peer a request came from, as its connection knows it: its address in its one form,
/// and that address as `X-Forwarded-For` gives it, written once for all the connection's
/// requests rather than once for each; and, where it is known, the address and port that
/// connected, which differ from it behind a load balancer that sends PROXY headers
/// ([20 §3](../../docs/20-proxy-protocol.md)), for the access log.
#[derive(Debug, Clone)]
pub struct Client {
    /// Why the gateway closed the connection with requests under way, once it has.
    cut: Cell<Option<Cut>>,
    address: IpAddr,
    value: HeaderValue,
    peer: Option<SocketAddr>,
}

impl Client {
    /// The peer at `address`.
    #[must_use]
    pub fn new(address: IpAddr) -> Self {
        let address = address.to_canonical();
        Self {
            cut: Cell::new(None),
            address,
            value: address_value(address),
            peer: None,
        }
    }

    /// The client at `address`, of a connection made from `peer`.
    #[must_use]
    pub fn connected(address: IpAddr, peer: SocketAddr) -> Self {
        Self {
            peer: Some(peer),
            ..Self::new(address)
        }
    }

    /// Its address, in its one form.
    #[must_use]
    pub fn address(&self) -> IpAddr {
        self.address
    }

    /// The address and port that connected, where the connection knows it.
    #[must_use]
    pub fn peer(&self) -> Option<SocketAddr> {
        self.peer
    }

    /// Notes that the gateway is closing the connection for `why`, cutting off what is
    /// under way on it: said before the requests' futures go, whose records end with them.
    pub(crate) fn cut(&self, why: Cut) {
        self.cut.set(Some(why));
    }

    /// Why the gateway closed the connection with requests under way, if it has.
    pub(crate) fn was_cut(&self) -> Option<Cut> {
        self.cut.get()
    }
}

/// Why the gateway closed a connection with requests under way: what their records say in
/// place of a client that left ([21 §3](../../docs/21-access-logs.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Cut {
    /// The drain's time was up.
    Drained,
    /// The worker's storage ran out and the connection held the most of it.
    Exhausted,
    /// Its client had too many of its streams reset.
    TooManyResets,
}

impl Cut {
    /// Its name in a record.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Self::Drained => "drained",
            Self::Exhausted => "exhausted",
            Self::TooManyResets => "too_many_resets",
        }
    }
}

/// Makes `head`, of a request from `client` that came in on `listener`, say what the
/// upstream is to be told of the client, and carry `id` if the request was given one.
///
/// # Errors
///
/// [`Rejection::Edits`] if the head cannot take the changes, which no config the gateway
/// takes comes to (14 §6).
pub(crate) fn forward<H: Head>(
    head: &mut H,
    listener: &CompiledListener,
    client: &Client,
    id: Option<&HeaderValue>,
) -> Result<(), Rejection> {
    let forwarding = &listener.forwarding;
    // QUIC is always over TLS, and only an `https` listener serves it.
    let proto = proto_value(listener.protocol == Protocol::Https);
    if forwarding.trusted_proxies.trusts(client.address) {
        from_trusted_proxy(head, listener, client, proto)?;
        if let Some(id) = id {
            head.set_field(request_id::HEADER, id.clone())?;
        }
    } else {
        // One pass over the names takes off what the client said of forwarding, its ID if
        // the gateway gives one, and what only a trusted proxy may send; what the gateway
        // says is then added, with nothing of the same name left to look for.
        let only = &forwarding.trusted_only_headers;
        let ids = id.is_some();
        head.remove_where(|name| {
            is_the_gateways(name) || (ids && is_request_id(name)) || only.matches(name)
        })?;
        if let Some(id) = id {
            head.append_field(request_id::HEADER, id.clone())?;
        }
        head.append_field(FORWARDED_FOR, client.value.clone())?;
        head.append_field(FORWARDED_PROTO, proto)?;
        // No host to give, the request is refused for it as it is routed.
        if let Some(host) = head.host_value() {
            head.append_field(FORWARDED_HOST, host)?;
        }
    }
    head.append_field(VIA, via_value(head.version()))
}

/// The same for a request from a trusted proxy: the client its chain names, and its scheme
/// and host as it said them, or the gateway's where it said none.
fn from_trusted_proxy<H: Head>(
    head: &mut H,
    listener: &CompiledListener,
    client: &Client,
    proto: HeaderValue,
) -> Result<(), Rejection> {
    let found = client_address(
        client.address,
        head.fields().values(&FORWARDED_FOR),
        &listener.forwarding.trusted_proxies,
    );
    let forwarded_for = if found == client.address {
        client.value.clone()
    } else {
        address_value(found)
    };
    head.set_field(FORWARDED_FOR, forwarded_for)?;
    if !has(head, &FORWARDED_PROTO) {
        head.append_field(FORWARDED_PROTO, proto)?;
    }
    if !has(head, &FORWARDED_HOST)
        && let Some(host) = head.host_value()
    {
        head.append_field(FORWARDED_HOST, host)?;
    }
    Ok(())
}

/// Whether `name`, in whatever case, is one of the three the gateway says itself.
fn is_the_gateways(name: &[u8]) -> bool {
    const FRONT: &[u8] = b"x-forwarded-";
    // Nearly every name is ruled out by its first byte.
    name.first()
        .is_some_and(|first| first.eq_ignore_ascii_case(&b'x'))
        && name
            .get(..FRONT.len())
            .is_some_and(|front| front.eq_ignore_ascii_case(FRONT))
        && name.get(FRONT.len()..).is_some_and(|rest| {
            rest.eq_ignore_ascii_case(b"for")
                || rest.eq_ignore_ascii_case(b"proto")
                || rest.eq_ignore_ascii_case(b"host")
        })
}

/// Whether `name`, in whatever case, is `X-Request-ID`.
fn is_request_id(name: &[u8]) -> bool {
    name.eq_ignore_ascii_case(request_id::HEADER.as_str().as_bytes())
}

/// Whether `head` has a field called `name`.
fn has<H: Head>(head: &H, name: &HeaderName) -> bool {
    head.fields().values(name).next().is_some()
}
