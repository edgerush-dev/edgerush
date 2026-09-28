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
//! A rule's own changes to the headers come after, and may change any of these.

use crate::head::Head;
use crate::request::Rejection;
use edgerush_config::{CompiledListener, Protocol};
use edgerush_filters::forwarding::{address_value, client_address, proto_value, via_value};
use edgerush_router::Fields;
use http::header::{HeaderName, HeaderValue, VIA};
use std::net::IpAddr;

/// `X-Forwarded-For`.
pub(crate) const FORWARDED_FOR: HeaderName = HeaderName::from_static("x-forwarded-for");
/// `X-Forwarded-Proto`.
pub(crate) const FORWARDED_PROTO: HeaderName = HeaderName::from_static("x-forwarded-proto");
/// `X-Forwarded-Host`.
pub(crate) const FORWARDED_HOST: HeaderName = HeaderName::from_static("x-forwarded-host");

/// The peer a request came from, as its connection knows it: its address in its one form,
/// and that address as `X-Forwarded-For` gives it, written once for all the connection's
/// requests rather than once for each.
#[derive(Debug, Clone)]
pub struct Client {
    address: IpAddr,
    value: HeaderValue,
}

impl Client {
    /// The peer at `address`.
    #[must_use]
    pub fn new(address: IpAddr) -> Self {
        let address = address.to_canonical();
        Self {
            address,
            value: address_value(address),
        }
    }

    /// Its address, in its one form.
    #[must_use]
    pub fn address(&self) -> IpAddr {
        self.address
    }
}

/// Makes `head`, of a request from `client` that came in on `listener`, say what the
/// upstream is to be told of the client.
///
/// # Errors
///
/// [`Rejection::Edits`] if the head cannot take the changes, which no config the gateway
/// takes comes to (14 §6).
pub(crate) fn forward<H: Head>(
    head: &mut H,
    listener: &CompiledListener,
    client: &Client,
) -> Result<(), Rejection> {
    let forwarding = &listener.forwarding;
    let trusted = forwarding.trusted_proxies.trusts(client.address);
    if !trusted && !forwarding.trusted_only_headers.is_empty() {
        head.remove_where(|name| forwarding.trusted_only_headers.matches(name))?;
    }
    let forwarded_for = if trusted {
        let found = client_address(
            client.address,
            head.fields().values(&FORWARDED_FOR),
            &forwarding.trusted_proxies,
        );
        if found == client.address {
            client.value.clone()
        } else {
            address_value(found)
        }
    } else {
        client.value.clone()
    };
    head.set_field(FORWARDED_FOR, forwarded_for)?;
    if !trusted || !has(head, &FORWARDED_PROTO) {
        // QUIC is always over TLS, and only an `https` listener serves it.
        head.set_field(
            FORWARDED_PROTO,
            proto_value(listener.protocol == Protocol::Https),
        )?;
    }
    if !trusted || !has(head, &FORWARDED_HOST) {
        match head.host_value() {
            Some(host) => head.set_field(FORWARDED_HOST, host)?,
            // No host to give, and the request is refused for it as it is routed; what
            // the client said is not left meanwhile.
            None if !trusted => {
                head.remove_where(|name| {
                    name.eq_ignore_ascii_case(FORWARDED_HOST.as_str().as_bytes())
                })?;
            }
            None => {}
        }
    }
    head.append_field(VIA, via_value(head.version()))
}

/// Whether `head` has a field called `name`.
fn has<H: Head>(head: &H, name: &HeaderName) -> bool {
    head.fields().values(name).next().is_some()
}
