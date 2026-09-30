//! A request's head as the request core works on it, whatever holds it
//! ([14 §6](../../../docs/14-downstream-server.md)).
//!
//! The core asks a head for what it needs — its method and target, its fields for route
//! predicates, and the few things done to every request's fields: a survey of the rare ones
//! that need work, the cookie string put together, the `Host` field checked or made to
//! agree, what is said about the connection checked and taken off, and a rule's changes.
//! Each kind of head does these its own way. A map head (`http::request::Parts`) does them
//! on its header map; the raw head of our own server does them on the lines it was read in
//! and an overlay of edits, and is held to giving the same result.

use crate::cookies;
use crate::hop_by_hop::{self, ConnectionError, is_hop_by_hop};
use crate::host::{self, HostError};
use crate::request::Rejection;
use crate::upstream::h1::codec::OutgoingFields;
use edgerush_filters::HeaderModifier;
use edgerush_router::Fields;
use http::header::{CONNECTION, COOKIE, HOST, HeaderMap, HeaderName, HeaderValue, TE, TRAILER};
use http::request::Parts;
use http::{Method, Uri, Version};

/// What one pass over a head's fields finds of the rare things that need work.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Survey {
    /// Whether there are hop-by-hop headers to check and to take off.
    pub hop_by_hop: bool,
    /// How many `Cookie` fields there are.
    pub cookie_fields: usize,
    /// Whether it declares trailers (`Trailer`), which go no further than the gateway.
    pub trailer: bool,
}

/// A request's head, as the request core reads and changes it.
pub trait Head {
    /// Its fields, for what reads them by name, as route predicates do.
    type Fields<'a>: Fields
    where
        Self: 'a;

    /// The request method.
    fn method(&self) -> &Method;

    /// The request target.
    fn uri(&self) -> &Uri;

    /// Puts another target in place of this one.
    fn set_uri(&mut self, uri: Uri);

    /// Puts another method in place of this one: an extended CONNECT's `GET` for an
    /// HTTP/1.1 backend, or the other way round ([19 §3](../../../docs/19-websocket.md)).
    fn set_method(&mut self, method: Method);

    /// The protocol an extended CONNECT asks for, `:protocol` (RFC 8441 §4, RFC 9220): none
    /// for any other request, and for every request of HTTP/1's.
    fn protocol(&self) -> Option<&str>;

    /// Its fields.
    fn fields(&self) -> Self::Fields<'_>;

    /// What of the rare things that need work its fields hold.
    fn survey(&self) -> Survey;

    /// Makes one `Cookie` field of several, in their order, the pieces joined by `"; "`.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn join_cookies(&mut self) -> Result<(), Rejection>;

    /// When the target names the host, makes the `Host` field say the same, unless it is
    /// one field that already does. The target's host has already been found to be one
    /// ([`crate::host::bare_host`]).
    ///
    /// # Errors
    ///
    /// [`HostError::Invalid`] if what the target names cannot be a field's value, the field
    /// then left as it was, or [`Rejection::Edits`] if the head cannot take the change.
    fn agree_host(&mut self) -> Result<(), Rejection>;

    /// The only `Host` field, as text ([`crate::host::host_field`]).
    ///
    /// # Errors
    ///
    /// A [`HostError`] if there is none, more than one, or one that is not ASCII.
    fn host_field(&self) -> Result<&str, HostError>;

    /// Checks what `Connection` names, before anything is taken off on its word.
    ///
    /// # Errors
    ///
    /// A [`ConnectionError`] if it names something that is no option, or a header the
    /// gateway keeps: `X-Request-ID` among them if the gateway gave the request its `id`.
    fn check_connection(&self, id: bool) -> Result<(), ConnectionError>;

    /// Takes the hop-by-hop headers off; `TE: trailers` stays if it was said.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn strip_request(&mut self) -> Result<(), Rejection>;

    /// Carries out a rule's changes to the headers.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take them.
    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), Rejection>;

    /// Gives the `Host` field this one value: a rewrite's host, which the upstream is sent
    /// (and an HTTP/2 upstream's `:authority` is made from).
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn set_host(&mut self, host: &HeaderValue) -> Result<(), Rejection>;

    /// Its fields as they stand, as a header map of their own: what a mirror placed before
    /// a change is sent (18 §5 in the docs).
    fn to_map(&self) -> HeaderMap;

    /// Takes off every field whose name, in whatever case it came, `unwanted` picks.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn remove_where(&mut self, unwanted: impl FnMut(&[u8]) -> bool) -> Result<(), Rejection>;

    /// Gives `name` this one value, in place of every one it had.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection>;

    /// Adds a value to `name`, after every one it has.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn append_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection>;

    /// The first `Host` field's value, as a value of its own: without a copy where the head
    /// can share what holds it.
    fn host_value(&self) -> Option<HeaderValue>;

    /// The version it came in.
    fn version(&self) -> Version;
}

impl Head for Parts {
    type Fields<'a> = &'a HeaderMap;

    fn method(&self) -> &Method {
        &self.method
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    fn set_uri(&mut self, uri: Uri) {
        self.uri = uri;
    }

    fn set_method(&mut self, method: Method) {
        self.method = method;
    }

    fn protocol(&self) -> Option<&str> {
        self.extensions
            .get::<::h2::ext::Protocol>()
            .map(::h2::ext::Protocol::as_str)
    }

    fn fields(&self) -> &HeaderMap {
        &self.headers
    }

    fn survey(&self) -> Survey {
        survey(&self.headers)
    }

    fn join_cookies(&mut self) -> Result<(), Rejection> {
        cookies::join(&mut self.headers);
        Ok(())
    }

    fn agree_host(&mut self) -> Result<(), Rejection> {
        let Some(authority) = self.uri.authority() else {
            return Ok(());
        };
        let mut fields = self.headers.get_all(HOST).iter();
        let named = fields
            .next()
            .is_some_and(|field| field == authority.as_str())
            && fields.next().is_none();
        if !named {
            let host = HeaderValue::from_str(authority.as_str()).map_err(|_| HostError::Invalid)?;
            self.headers.insert(HOST, host);
        }
        Ok(())
    }

    fn host_field(&self) -> Result<&str, HostError> {
        host::host_field(&self.headers)
    }

    fn check_connection(&self, id: bool) -> Result<(), ConnectionError> {
        hop_by_hop::check_connection(&self.headers, id)
    }

    fn strip_request(&mut self) -> Result<(), Rejection> {
        hop_by_hop::strip_request(&mut self.headers);
        Ok(())
    }

    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), Rejection> {
        changes.apply(&mut self.headers);
        Ok(())
    }

    fn set_host(&mut self, host: &HeaderValue) -> Result<(), Rejection> {
        self.headers.insert(HOST, host.clone());
        Ok(())
    }

    fn to_map(&self) -> HeaderMap {
        self.headers.clone()
    }

    fn remove_where(&mut self, mut unwanted: impl FnMut(&[u8]) -> bool) -> Result<(), Rejection> {
        // A map has no way to drop entries as it is walked; the names are gathered first,
        // which allocates only when there is one.
        let gone: Vec<HeaderName> = self
            .headers
            .keys()
            .filter(|name| unwanted(name.as_str().as_bytes()))
            .cloned()
            .collect();
        for name in gone {
            self.headers.remove(name);
        }
        Ok(())
    }

    fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        self.headers.insert(name, value);
        Ok(())
    }

    fn append_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        self.headers.append(name, value);
        Ok(())
    }

    fn host_value(&self) -> Option<HeaderValue> {
        self.headers.get(HOST).cloned()
    }

    fn version(&self) -> Version {
        self.version
    }
}

/// A request's head as the rest of its way through the gateway needs it, once the core has
/// decided where it goes: its fields as they are to go upstream, and the changes made to
/// them on the way there.
pub(crate) trait Forwarded: Head {
    /// Its fields as they are to be written upstream.
    type Outgoing: OutgoingFields + ?Sized;

    /// Its fields as they are to be written upstream.
    fn outgoing(&self) -> &Self::Outgoing;

    /// Makes it this hop's to send on: HTTP/1.1, and nothing kept that was about the
    /// connection it came in on.
    fn onward(&mut self);

    /// Has the upstream close the connection after it: `Connection: close`, in place of
    /// any it has.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if the head cannot take the change.
    fn close_connection(&mut self) -> Result<(), Rejection>;
}

impl Forwarded for Parts {
    type Outgoing = HeaderMap;

    fn outgoing(&self) -> &HeaderMap {
        &self.headers
    }

    fn onward(&mut self) {
        self.version = Version::HTTP_11;
        // What the engine attached to the request is about the connection it came in on.
        self.extensions.clear();
    }

    fn close_connection(&mut self) -> Result<(), Rejection> {
        self.headers.insert(CONNECTION, crate::hop_by_hop::CLOSE);
        Ok(())
    }
}

/// One pass over a header map, finding the rare things that need work. Most requests have
/// none of them and pay for the pass alone: looking every name up would cost several times
/// as much.
pub(crate) fn survey(headers: &HeaderMap) -> Survey {
    let mut found = Survey::default();
    let mut plain_te_fields = 0;
    // A header that is repeated comes up once for each of its fields.
    for (name, value) in headers {
        if *name == COOKIE {
            found.cookie_fields += 1;
        } else if *name == TRAILER {
            found.trailer = true;
        } else if *name == TE && value == "trailers" {
            // What every gRPC client says, and already the one form in which `TE` is
            // forwarded: on its own there is nothing to take off only to put it back.
            plain_te_fields += 1;
        } else if is_hop_by_hop(name) {
            found.hop_by_hop = true;
        }
    }
    found.hop_by_hop |= plain_te_fields > 1;
    found
}
