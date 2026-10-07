//! A request's head as our HTTP/2 and HTTP/3 servers hand it over — the header map h2 or our
//! HTTP/3 decoder made — with the fields the request core adds kept beside the map rather
//! than put into it (14 §6 in the docs). Nearly every request has some added: `Host` from
//! `:authority`, what the upstream is told of the client (03 §11), a rule's changes. Put into
//! the map, each is hashed in, and the map grows — reallocating, rehashing and copying — to
//! take them; beside it, each is pushed onto a list lent by the worker.
//!
//! What is taken out is taken out of both. The fields read as the map's, in their order,
//! then the list's, in theirs: what a header map would give had they been put into it.

// `pub` for the benchmarks, which are crates of their own; in an ordinary build none of this
// is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::cookies;
use crate::fields::MOST_ADDED;
use crate::head::{Forwarded, Head, Survey, survey};
use crate::hop_by_hop::{self, ConnectionError, is_hop_by_hop};
use crate::host::HostError;
use crate::request::Rejection;
use crate::upstream::h1::blocks::Blocks;
use crate::upstream::h1::codec::{OutgoingFields, is_framing};
use edgerush_filters::{Edit, HeaderModifier};
use edgerush_router::Fields;
use http::header::{CONNECTION, HOST, HeaderMap, HeaderName, HeaderValue};
use http::request::Parts;
use http::{Method, Uri, Version};
use std::cell::RefCell;
use std::rc::Rc;

/// A request's head held as a header map, with the fields added to it beside the map.
#[derive(Debug)]
pub struct MapHead {
    parts: Parts,
    /// Fields added after what the map holds, in the order they were.
    added: Vec<(HeaderName, HeaderValue)>,
    /// Where the list's room came from, and goes back to when the head is done with.
    lent_by: Option<Rc<RefCell<Blocks>>>,
}

impl MapHead {
    /// The head `parts`, its additions made in a list of its own. For tests and benchmarks:
    /// our own servers' heads are [`MapHead::lent`].
    #[cfg(any(test, feature = "fuzzing"))]
    #[must_use]
    pub fn new(parts: Parts) -> Self {
        Self {
            parts,
            added: Vec::new(),
            lent_by: None,
        }
    }

    /// The same, its additions made in room lent by `blocks` and given back when it is
    /// dropped: our own servers' heads, which would otherwise make that room for every
    /// request.
    pub fn lent(parts: Parts, blocks: &Rc<RefCell<Blocks>>) -> Self {
        let added = blocks.borrow_mut().take_edits();
        Self {
            parts,
            added,
            lent_by: Some(Rc::clone(blocks)),
        }
    }

    /// Its fields, the map's and then the added ones.
    fn view(&self) -> MapFields<'_> {
        MapFields {
            map: &self.parts.headers,
            added: &self.added,
        }
    }

    /// Takes out every field called `name`.
    fn remove(&mut self, name: &HeaderName) {
        self.parts.headers.remove(name);
        self.added.retain(|(added, _)| added != name);
    }

    /// Adds a field after every other.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if as many fields have been added as may be.
    fn push(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        if self.added.len() >= MOST_ADDED {
            return Err(Rejection::Edits);
        }
        self.added.push((name, value));
        Ok(())
    }

    /// Gives `name` this one value, in place of every one it had.
    ///
    /// # Errors
    ///
    /// [`Rejection::Edits`] if as many fields have been added as may be, and then nothing
    /// is changed.
    fn set(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        if self.added.len() >= MOST_ADDED {
            return Err(Rejection::Edits);
        }
        self.remove(&name);
        self.push(name, value)
    }
}

impl Drop for MapHead {
    fn drop(&mut self) {
        // Given back unless the blocks are in use, as they never are when a request ends;
        // then the room is only let go of.
        if let Some(blocks) = &self.lent_by
            && let Ok(mut blocks) = blocks.try_borrow_mut()
        {
            blocks.give_edits(std::mem::take(&mut self.added));
        }
    }
}

/// A map head's fields as they stand: the map's, then those added beside it.
#[derive(Debug, Clone, Copy)]
pub struct MapFields<'a> {
    map: &'a HeaderMap,
    added: &'a [(HeaderName, HeaderValue)],
}

impl Fields for MapFields<'_> {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        let added = self
            .added
            .iter()
            .filter(move |(added, _)| added == name)
            .map(|(_, value)| value.as_bytes());
        self.map
            .get_all(name)
            .iter()
            .map(HeaderValue::as_bytes)
            .chain(added)
    }
}

impl Fields for MapHead {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        let added = self
            .added
            .iter()
            .filter(move |(added, _)| added == name)
            .map(|(_, value)| value.as_bytes());
        self.parts
            .headers
            .get_all(name)
            .iter()
            .map(HeaderValue::as_bytes)
            .chain(added)
    }
}

impl OutgoingFields for MapHead {
    fn written_len(&self) -> usize {
        let added: usize = self
            .added
            .iter()
            .filter(|(name, _)| !is_framing(name))
            .map(|(name, value)| name.as_str().len() + 2 + value.len() + 2)
            .sum();
        self.parts.headers.written_len() + added
    }

    fn write_fields(&self, out: &mut Vec<u8>) {
        self.parts.headers.write_fields(out);
        for (name, value) in &self.added {
            if is_framing(name) {
                continue;
            }
            out.extend_from_slice(name.as_str().as_bytes());
            out.extend_from_slice(b": ");
            out.extend_from_slice(value.as_bytes());
            out.extend_from_slice(b"\r\n");
        }
    }

    fn each_field(&self, mut visit: impl FnMut(&[u8], &[u8])) {
        self.parts.headers.each_field(&mut visit);
        for (name, value) in &self.added {
            visit(name.as_str().as_bytes(), value.as_bytes());
        }
    }

    fn shared_count(&self) -> Option<usize> {
        Some(self.parts.headers.len() + self.added.len())
    }

    fn each_shared(&self, mut visit: impl FnMut(&HeaderName, &HeaderValue)) {
        for (name, value) in &self.parts.headers {
            visit(name, value);
        }
        for (name, value) in &self.added {
            visit(name, value);
        }
    }
}

impl Head for MapHead {
    type Fields<'a> = MapFields<'a>;

    fn method(&self) -> &Method {
        &self.parts.method
    }

    fn uri(&self) -> &Uri {
        &self.parts.uri
    }

    fn set_uri(&mut self, uri: Uri) {
        self.parts.uri = uri;
    }

    fn set_method(&mut self, method: Method) {
        self.parts.method = method;
    }

    fn protocol(&self) -> Option<&str> {
        self.parts.protocol()
    }

    fn fields(&self) -> MapFields<'_> {
        self.view()
    }

    fn survey(&self) -> Survey {
        // Asked before anything is added: the map is all there is.
        survey(&self.parts.headers)
    }

    fn join_cookies(&mut self) -> Result<(), Rejection> {
        // Before anything is added, as the survey.
        cookies::join(&mut self.parts.headers);
        Ok(())
    }

    fn agree_host(&mut self) -> Result<(), Rejection> {
        let Some(authority) = self.parts.uri.authority() else {
            return Ok(());
        };
        let named = {
            let view = self.view();
            let mut fields = view.values(&HOST);
            fields
                .next()
                .is_some_and(|field| field == authority.as_str().as_bytes())
                && fields.next().is_none()
        };
        if !named {
            let host = HeaderValue::from_str(authority.as_str()).map_err(|_| HostError::Invalid)?;
            self.set(HOST, host)?;
        }
        Ok(())
    }

    fn host_field(&self) -> Result<&str, HostError> {
        let mut fields = self.values(&HOST);
        let field = fields.next().ok_or(HostError::Missing)?;
        if fields.next().is_some() {
            return Err(HostError::Repeated);
        }
        // What `HeaderValue::to_str` takes: visible ASCII and tabs.
        if !field
            .iter()
            .all(|&byte| (32..127).contains(&byte) || byte == b'\t')
        {
            return Err(HostError::Invalid);
        }
        std::str::from_utf8(field).map_err(|_| HostError::Invalid)
    }

    fn check_connection(&self, id: bool) -> Result<(), ConnectionError> {
        hop_by_hop::check_connection_values(self.values(&CONNECTION), id)
    }

    fn strip_request(&mut self) -> Result<(), Rejection> {
        hop_by_hop::strip_request(&mut self.parts.headers);
        // Nothing added is about the connection — the core adds none of those, and what
        // `Connection` names may not be one it adds — but a list is held to the same rule.
        self.added.retain(|(name, _)| !is_hop_by_hop(name));
        Ok(())
    }

    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), Rejection> {
        let mut editing = Editing {
            head: self,
            full: false,
        };
        changes.apply(&mut editing);
        if editing.full {
            return Err(Rejection::Edits);
        }
        Ok(())
    }

    fn set_host(&mut self, host: &HeaderValue) -> Result<(), Rejection> {
        self.set(HOST, host.clone())
    }

    fn to_map(&self) -> HeaderMap {
        let mut map = HeaderMap::with_capacity(self.parts.headers.len() + self.added.len());
        for (name, value) in &self.parts.headers {
            map.append(name.clone(), value.clone());
        }
        for (name, value) in &self.added {
            map.append(name.clone(), value.clone());
        }
        map
    }

    fn remove_where(&mut self, mut unwanted: impl FnMut(&[u8]) -> bool) -> Result<(), Rejection> {
        // A map has no way to drop entries as it is walked; the names are gathered first,
        // which allocates only when there is one.
        let gone: Vec<HeaderName> = self
            .parts
            .headers
            .keys()
            .filter(|name| unwanted(name.as_str().as_bytes()))
            .cloned()
            .collect();
        for name in gone {
            self.parts.headers.remove(name);
        }
        self.added
            .retain(|(name, _)| !unwanted(name.as_str().as_bytes()));
        Ok(())
    }

    fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        self.set(name, value)
    }

    fn append_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        self.push(name, value)
    }

    fn host_value(&self) -> Option<HeaderValue> {
        // The first, as the fields read: the map's before any added.
        self.parts.headers.get(HOST).cloned().or_else(|| {
            self.added
                .iter()
                .find(|(name, _)| *name == HOST)
                .map(|(_, value)| value.clone())
        })
    }

    fn version(&self) -> Version {
        self.parts.version
    }
}

impl Forwarded for MapHead {
    type Outgoing = Self;

    fn outgoing(&self) -> &Self {
        self
    }

    fn onward(&mut self) {
        self.parts.version = Version::HTTP_11;
        // What the engine attached to the request is about the connection it came in on.
        self.parts.extensions.clear();
    }

    fn close_connection(&mut self) -> Result<(), Rejection> {
        self.set(CONNECTION, crate::hop_by_hop::CLOSE)
    }
}

/// A map head while a modifier's changes are made to it, keeping note of a change it could
/// not take.
struct Editing<'a> {
    head: &'a mut MapHead,
    full: bool,
}

impl Edit for Editing<'_> {
    fn remove(&mut self, name: &HeaderName) {
        self.head.remove(name);
    }

    fn set(&mut self, name: &HeaderName, value: &HeaderValue) {
        if self.head.set(name.clone(), value.clone()).is_err() {
            self.full = true;
        }
    }

    fn append(&mut self, name: &HeaderName, value: &HeaderValue) {
        if self.head.push(name.clone(), value.clone()).is_err() {
            self.full = true;
        }
    }

    fn append_cookie(&mut self, value: &HeaderValue) {
        // The values themselves, the map's then those added, each with its mark: a piece
        // the client sent not to be indexed keeps the string so (RFC 7541 §6.2.3).
        let cookie = &http::header::COOKIE;
        let had = self
            .head
            .parts
            .headers
            .get_all(cookie)
            .iter()
            .chain(
                self.head
                    .added
                    .iter()
                    .filter(|(name, _)| name == cookie)
                    .map(|(_, value)| value),
            )
            .map(|piece| (piece.as_bytes(), piece.is_sensitive()));
        let joined = edgerush_filters::cookie_with(had, value);
        let edited = match joined {
            Some(whole) => self.head.set(http::header::COOKIE, whole),
            None => self.head.push(http::header::COOKIE, value.clone()),
        };
        if edited.is_err() {
            self.full = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::Request;

    fn parts(fields: &[(&str, &str)]) -> Parts {
        let mut request = Request::builder().uri("/");
        for (name, value) in fields {
            request = request.header(*name, *value);
        }
        request.body(()).unwrap().into_parts().0
    }

    fn values(head: &MapHead, name: &str) -> Vec<String> {
        head.values(&HeaderName::from_bytes(name.as_bytes()).unwrap())
            .map(|value| String::from_utf8(value.to_vec()).unwrap())
            .collect()
    }

    /// Setting, adding and taking out come to what they would on the map itself; nothing
    /// added is put into the map.
    #[test]
    fn edits_beside_the_map_read_as_edits_of_it() {
        let mut head = MapHead::new(parts(&[("host", "a"), ("x-one", "1"), ("x-two", "2")]));
        let capacity = head.parts.headers.capacity();
        head.append_field(
            HeaderName::from_static("x-one"),
            HeaderValue::from_static("1b"),
        )
        .unwrap();
        head.set_field(
            HeaderName::from_static("x-two"),
            HeaderValue::from_static("2b"),
        )
        .unwrap();
        head.set_field(
            HeaderName::from_static("x-new"),
            HeaderValue::from_static("n"),
        )
        .unwrap();
        head.remove_where(|name| name == b"x-new").unwrap();
        head.set_field(HOST, HeaderValue::from_static("b")).unwrap();
        assert_eq!(values(&head, "x-one"), ["1", "1b"]);
        assert_eq!(values(&head, "x-two"), ["2b"]);
        assert!(values(&head, "x-new").is_empty());
        assert_eq!(head.host_field(), Ok("b"));
        assert_eq!(head.host_value().unwrap(), "b");
        assert_eq!(head.parts.headers.capacity(), capacity, "the map was grown");

        let mut map = parts(&[("host", "a"), ("x-one", "1"), ("x-two", "2")]).headers;
        map.append("x-one", HeaderValue::from_static("1b"));
        map.insert("x-two", HeaderValue::from_static("2b"));
        map.insert(HOST, HeaderValue::from_static("b"));
        assert_eq!(head.to_map(), map);
    }

    /// A rule's `add` on `Cookie` joins the cookie string with "; " here too, as on a map
    /// or a raw head, and gives a head with none the cookie added.
    #[test]
    fn a_cookie_a_rule_adds_joins_the_cookie_string() {
        let adds = HeaderModifier::new([], [("cookie", "flag=on")], []).unwrap();
        for (had, whole) in [(&[("cookie", "a=1")][..], "a=1; flag=on"), (&[], "flag=on")] {
            let mut head = MapHead::new(parts(had));
            head.apply(&adds).unwrap();
            assert_eq!(values(&head, "cookie"), [whole]);
        }
        // A cookie string the client sent not to be indexed (RFC 7541 §6.2.3) stays so.
        let mut sent = parts(&[]);
        let mut secret = HeaderValue::from_static("a=1");
        secret.set_sensitive(true);
        sent.headers.insert(http::header::COOKIE, secret);
        let mut head = MapHead::new(sent);
        head.apply(&adds).unwrap();
        assert!(head.to_map()[http::header::COOKIE].is_sensitive());
    }

    /// Our servers' heads add in room the worker lends, and give it back when done.
    #[test]
    fn a_heads_room_is_lent_and_given_back() {
        let blocks = Rc::new(RefCell::new(Blocks::new(
            crate::upstream::h1::blocks::Sizes::default(),
            crate::storage::Storage::new(crate::storage::LIMIT),
        )));
        let mut head = MapHead::lent(parts(&[("host", "a")]), &blocks);
        for n in 0..5 {
            let name = HeaderName::from_bytes(format!("x-{n}").as_bytes()).unwrap();
            head.append_field(name, HeaderValue::from_static("v"))
                .unwrap();
        }
        drop(head);
        let given = blocks.borrow_mut().take_edits();
        assert!(given.is_empty());
        assert!(given.capacity() >= 5, "{}", given.capacity());
    }
}
