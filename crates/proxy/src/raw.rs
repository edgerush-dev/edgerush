//! A request's head as our own server reads it
//! ([14 §6](../../../docs/14-downstream-server.md)): the bytes it arrived as, where each
//! field line lies in them, and an overlay of what the request core changes, doing all that
//! [`Head`] asks as a header map would.

// `pub` for the fuzz targets and benchmarks, which are crates of their own; in an ordinary
// build none of this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::downstream::h1::writer::{AnswerFields, field};
use crate::fields::{Edited, FieldLines, Known, Overlay, OverlayFull, Piece, View};
use crate::h1::{Declaration, declaration};
use crate::head::{Forwarded, Head, Survey};
use crate::hop_by_hop::{self, ConnectionError, is_hop_by_hop_name, options_of};
use crate::host::HostError;
use crate::request::Rejection;
use crate::upstream::h1::blocks::Blocks;
use crate::upstream::h1::codec::{HeldField, OutgoingFields};
use bytes::Bytes;
use edgerush_filters::{Edit, HeaderModifier};
use edgerush_router::Fields;
#[cfg(any(test, feature = "fuzzing"))]
use http::Request;
use http::header::{CONNECTION, COOKIE, DATE, HOST, HeaderName, HeaderValue, TE, TRAILER};
#[cfg(any(test, feature = "fuzzing"))]
use http::request::Parts;
use http::{HeaderMap, Method, Response, StatusCode, Uri, Version, response};
use std::cell::RefCell;
use std::rc::Rc;

/// A request's head as our own server read it: its method and target, the bytes of the
/// head, where each field line lies in them, and an overlay of what the core changes. Its
/// fields are read out of those bytes and every change goes to the overlay, so a line the
/// core leaves alone reaches the upstream as it arrived (14 §6).
#[derive(Debug)]
pub struct RawHead {
    method: Method,
    uri: Uri,
    version: Version,
    head: Bytes,
    lines: FieldLines,
    overlay: Overlay,
    /// Where the overlay's room came from, and goes back to when the head is done with.
    lent_by: Option<Rc<RefCell<Blocks>>>,
}

impl RawHead {
    /// The head in `head`, whose field lines `lines` says where they are. For tests,
    /// benchmarks and fuzz targets: our own server's heads are [`RawHead::lent`].
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn new(method: Method, uri: Uri, version: Version, head: Bytes, lines: FieldLines) -> Self {
        Self {
            method,
            uri,
            version,
            head,
            lines,
            overlay: Overlay::default(),
            lent_by: None,
        }
    }

    /// The same, its edits made in room lent by `blocks` and given back when it is dropped:
    /// our own server's heads, which would otherwise make that room for every request.
    pub fn lent(
        method: Method,
        uri: Uri,
        version: Version,
        head: Bytes,
        lines: FieldLines,
        blocks: &Rc<RefCell<Blocks>>,
    ) -> Self {
        let room = blocks.borrow_mut().take_edits();
        Self {
            method,
            uri,
            version,
            head,
            lines,
            overlay: Overlay::in_room(room),
            lent_by: Some(Rc::clone(blocks)),
        }
    }

    fn view(&self) -> View<'_> {
        self.lines.view(&self.head)
    }

    /// The bytes of the head, which [`RawHead::pieces`] copies from.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn bytes(&self) -> &Bytes {
        &self.head
    }

    /// What its fields, as edited, are written as: runs of the lines kept, then the fields
    /// added, the known headers in `skip` left out.
    pub fn pieces<'a>(&'a self, skip: &'a [Known]) -> impl Iterator<Item = Piece<'a>> {
        self.overlay.pieces(&self.lines, skip)
    }
}

/// What a request's head writer writes itself, and so leaves out of what it copies.
const UPSTREAM_FRAMING: [Known; 2] = [Known::ContentLength, Known::TransferEncoding];

impl Fields for RawHead {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        self.fields().values_of(name)
    }
}

impl OutgoingFields for RawHead {
    fn written_len(&self) -> usize {
        self.pieces(&UPSTREAM_FRAMING)
            .map(|piece| match piece {
                Piece::Copy(span) => span.len(),
                Piece::Field(name, value) => name.as_str().len() + 2 + value.len() + 2,
            })
            .sum()
    }

    fn write_fields(&self, out: &mut Vec<u8>) {
        for piece in self.pieces(&UPSTREAM_FRAMING) {
            match piece {
                // Whole lines as they arrived, line breaks and all.
                Piece::Copy(span) => out.extend_from_slice(self.head.get(span).unwrap_or_default()),
                Piece::Field(name, value) => {
                    out.extend_from_slice(name.as_str().as_bytes());
                    out.extend_from_slice(b": ");
                    out.extend_from_slice(value.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            }
        }
    }

    fn each_field(&self, mut visit: impl FnMut(&[u8], &[u8])) {
        for (name, value) in self.fields().iter() {
            visit(name, value);
        }
    }

    fn each_outgoing(&self, mut visit: impl FnMut(HeldField<'_>)) {
        let fields = self.fields();
        for (name, value) in fields.kept() {
            visit(HeldField::Line(name, value));
        }
        for (name, value) in fields.added() {
            visit(HeldField::Added(name, value));
        }
    }
}

impl Head for RawHead {
    type Fields<'a> = Edited<'a>;

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
        None
    }

    fn fields(&self) -> Edited<'_> {
        self.overlay.edited(self.view())
    }

    fn survey(&self) -> Survey {
        let mut found = Survey::default();
        let mut plain_te_fields = 0;
        // As the map's survey, the names compared in whatever case they arrived in.
        for (name, value) in self.fields().iter() {
            if name.eq_ignore_ascii_case(b"cookie") {
                found.cookie_fields += 1;
            } else if name.eq_ignore_ascii_case(b"trailer") {
                found.trailer = true;
            } else if name.eq_ignore_ascii_case(b"te") && value == b"trailers" {
                plain_te_fields += 1;
            } else if is_hop_by_hop_name(name) {
                found.hop_by_hop = true;
            }
        }
        found.hop_by_hop |= plain_te_fields > 1;
        found
    }

    fn join_cookies(&mut self) -> Result<(), Rejection> {
        let mut joined = Vec::new();
        let mut pieces = 0;
        for piece in self.fields().values(&COOKIE) {
            if pieces > 0 {
                joined.extend_from_slice(b"; ");
            }
            joined.extend_from_slice(piece);
            pieces += 1;
        }
        if pieces < 2 {
            return Ok(());
        }
        // As the map's join: pieces that do not make a header value are left as they came.
        let Ok(whole) = HeaderValue::from_bytes(&joined) else {
            return Ok(());
        };
        let view = self.lines.view(&self.head);
        self.overlay
            .set(&view, COOKIE, whole)
            .map_err(|_| Rejection::Edits)
    }

    fn agree_host(&mut self) -> Result<(), Rejection> {
        let Some(authority) = self.uri.authority() else {
            return Ok(());
        };
        let view = self.lines.view(&self.head);
        let named = {
            let edited = self.overlay.edited(view);
            let mut fields = edited.values(&HOST);
            fields
                .next()
                .is_some_and(|field| field == authority.as_str().as_bytes())
                && fields.next().is_none()
        };
        if !named {
            let host = HeaderValue::from_str(authority.as_str()).map_err(|_| HostError::Invalid)?;
            self.overlay
                .set(&view, HOST, host)
                .map_err(|_| Rejection::Edits)?;
        }
        Ok(())
    }

    fn host_field(&self) -> Result<&str, HostError> {
        let mut fields = self.fields().values_of(&HOST);
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
        hop_by_hop::check_connection_values(self.fields().values(&CONNECTION), id)
    }

    fn strip_request(&mut self) -> Result<(), Rejection> {
        let view = self.lines.view(&self.head);
        let accepts_trailers = self
            .overlay
            .edited(view)
            .values(&TE)
            .flat_map(options_of)
            .any(|option| option.eq_ignore_ascii_case(b"trailers"));
        // What `Connection` names comes from the head as it arrived: the core never adds a
        // `Connection` field, and nothing has taken one off before this.
        strip(view, &mut self.overlay);
        if accepts_trailers {
            self.overlay
                .set(&view, TE, crate::hop_by_hop::TE_TRAILERS)
                .map_err(|_| Rejection::Edits)?;
        }
        Ok(())
    }

    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), Rejection> {
        apply(self.lines.view(&self.head), &mut self.overlay, changes).map_err(|_| Rejection::Edits)
    }

    fn set_host(&mut self, host: &HeaderValue) -> Result<(), Rejection> {
        let view = self.lines.view(&self.head);
        self.overlay
            .set(&view, HOST, host.clone())
            .map_err(|_| Rejection::Edits)
    }

    fn remove_where(&mut self, unwanted: impl FnMut(&[u8]) -> bool) -> Result<(), Rejection> {
        let view = self.lines.view(&self.head);
        self.overlay.remove_where(&view, unwanted);
        Ok(())
    }

    fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        let view = self.lines.view(&self.head);
        self.overlay
            .set(&view, name, value)
            .map_err(|_| Rejection::Edits)
    }

    fn append_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), Rejection> {
        self.overlay
            .append(name, value)
            .map_err(|_| Rejection::Edits)
    }

    fn host_value(&self) -> Option<HeaderValue> {
        let value = self.fields().values_of(&HOST).next()?;
        // A line of the head is shared with it; a value the core put in its place is not
        // in the head's bytes, and is copied.
        let within = self.head.as_ptr_range();
        let at = value.as_ptr_range();
        if !value.is_empty() && within.start <= at.start && at.end <= within.end {
            HeaderValue::from_maybe_shared(self.head.slice_ref(value)).ok()
        } else {
            HeaderValue::from_bytes(value).ok()
        }
    }

    fn to_map(&self) -> HeaderMap {
        let mut map = HeaderMap::with_capacity(self.lines.len());
        for (name, value) in self.fields().iter() {
            // Every line was read as a field, and every edit made as one.
            if let (Ok(name), Ok(value)) =
                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
            {
                map.append(name, value);
            }
        }
        map
    }

    fn version(&self) -> Version {
        self.version
    }
}

impl Forwarded for RawHead {
    type Outgoing = Self;

    fn outgoing(&self) -> &Self {
        self
    }

    fn onward(&mut self) {
        self.version = Version::HTTP_11;
    }

    fn close_connection(&mut self) -> Result<(), Rejection> {
        let view = self.lines.view(&self.head);
        self.overlay
            .set(&view, CONNECTION, crate::hop_by_hop::CLOSE)
            .map_err(|_| Rejection::Edits)
    }
}

impl RawHead {
    /// The head as `http`'s parts, which tests hand to cores written for those.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn into_parts(self) -> Parts {
        let mut headers = HeaderMap::with_capacity(self.lines.len());
        for (name, value) in self.fields().iter() {
            // Every line was a field when it was read, and every field added was made one.
            if let (Ok(name), Ok(value)) =
                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
            {
                headers.append(name, value);
            }
        }
        let (mut parts, ()) = Request::new(()).into_parts();
        parts.method = self.method.clone();
        parts.uri = self.uri.clone();
        parts.version = self.version;
        parts.headers = headers;
        parts
    }
}

impl Drop for RawHead {
    fn drop(&mut self) {
        // Given back unless the blocks are in use, as they never are when a request ends;
        // then the room is only let go of.
        if let Some(blocks) = &self.lent_by
            && let Ok(mut blocks) = blocks.try_borrow_mut()
        {
            blocks.give_edits(self.overlay.take_room());
        }
    }
}

/// An upstream's answer as our own client reads it: its status, the bytes of its head,
/// where each field line lies in them, and an overlay of what the way to the client
/// changes. A line nothing changes reaches the client as it arrived (14 §6).
#[derive(Debug)]
pub struct RawAnswer {
    status: StatusCode,
    head: Bytes,
    lines: FieldLines,
    overlay: Overlay,
}

impl RawAnswer {
    /// The answer whose head is `head`, whose field lines `lines` says where they are.
    pub fn new(status: StatusCode, head: Bytes, lines: FieldLines) -> Self {
        Self {
            status,
            head,
            lines,
            overlay: Overlay::default(),
        }
    }

    /// What the upstream answered.
    pub fn status(&self) -> StatusCode {
        self.status
    }

    /// Its fields as edited, read like a header map.
    pub fn fields(&self) -> Edited<'_> {
        self.overlay.edited(self.lines.view(&self.head))
    }

    /// What its fields, as edited, are written as: runs of the lines kept, then the fields
    /// added, the known headers in `skip` left out.
    pub fn pieces<'a>(&'a self, skip: &'a [Known]) -> impl Iterator<Item = Piece<'a>> {
        self.overlay.pieces(&self.lines, skip)
    }

    /// Takes the names its own `Connection` gave, and those that may never follow, out of
    /// its `Trailer` declaration.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if the declaration that is left cannot be added.
    pub fn filter_declaration(&mut self, nominated: &[HeaderName]) -> Result<(), OverlayFull> {
        filter_declaration(self.lines.view(&self.head), &mut self.overlay, nominated)
    }

    /// Takes off the fields that are about the upstream's connection and not the client's.
    pub fn strip(&mut self) {
        let view = self.lines.view(&self.head);
        // Most answers say nothing about their connection, and one pass over their names is
        // all they pay, as a map's does: looking each of the names up costs several times as
        // much. Without one of them there is no `Connection` to name any other field either.
        // A request's core has surveyed its head for these already.
        if self
            .overlay
            .edited(view)
            .iter()
            .any(|(name, _)| is_hop_by_hop_name(name))
        {
            strip(view, &mut self.overlay);
        }
    }

    /// Makes a rule's changes to the answer.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if more fields are added than an overlay holds, which no config the
    /// gateway takes comes to.
    pub fn apply(&mut self, changes: &HeaderModifier) -> Result<(), OverlayFull> {
        apply(self.lines.view(&self.head), &mut self.overlay, changes)
    }

    /// Gives `name` the one value `value`, in place of every one it had.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if the field cannot be added.
    pub fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), OverlayFull> {
        let view = self.lines.view(&self.head);
        self.overlay.set(&view, name, value)
    }

    /// The answer as `http`'s parts, in this hop's version, for a server that takes those:
    /// HTTP/2's. What was added goes over as it was added, its flags kept (a request's ID
    /// never indexed) and not checked again.
    pub fn into_parts(self) -> response::Parts {
        let fields = self.fields();
        let mut headers = HeaderMap::with_capacity(self.lines.len() + fields.added().count());
        for (name, value) in fields.kept() {
            // Every line was found to be a field when it was read, so nothing here is left
            // out.
            if let (Ok(name), Ok(value)) =
                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
            {
                headers.append(name, value);
            }
        }
        for (name, value) in fields.added() {
            headers.append(name.clone(), value.clone());
        }
        let (mut parts, ()) = Response::new(()).into_parts();
        parts.status = self.status;
        parts.version = Version::HTTP_11;
        parts.headers = headers;
        parts
    }
}

impl Fields for RawAnswer {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        self.fields().values_of(name)
    }
}

/// What the client's head writer writes itself, and so leaves out of what it copies.
const DOWNSTREAM_FRAMING: [Known; 4] = [
    Known::ContentLength,
    Known::TransferEncoding,
    Known::Connection,
    Known::Trailer,
];

impl AnswerFields for RawAnswer {
    fn write_fields(&self, out: &mut Vec<u8>) -> bool {
        for piece in self.pieces(&DOWNSTREAM_FRAMING) {
            match piece {
                // Whole lines as they arrived, line breaks and all.
                Piece::Copy(span) => out.extend_from_slice(self.head.get(span).unwrap_or_default()),
                Piece::Field(name, value) => field(out, name.as_str().as_bytes(), value.as_bytes()),
            }
        }
        self.values(&DATE).next().is_some()
    }
}

/// Takes out the hop-by-hop fields: those the head's own `Connection` names, as it arrived,
/// and those that go whether named or not.
fn strip(view: View<'_>, overlay: &mut Overlay) {
    // One pass over the lines, rather than one for each name. What `Connection` names is
    // most often nothing but `keep-alive`, a field that goes anyway, and then it is not
    // asked again for every line.
    let names_others = view
        .values(&CONNECTION)
        .flat_map(options_of)
        .any(|option| !is_hop_by_hop_name(option));
    let named = |name: &[u8]| {
        names_others
            && view
                .values(&CONNECTION)
                .flat_map(options_of)
                .any(|option| option.eq_ignore_ascii_case(name))
    };
    overlay.remove_where(&view, |name| is_hop_by_hop_name(name) || named(name));
}

/// Makes a modifier's changes to a head's overlay.
fn apply(
    view: View<'_>,
    overlay: &mut Overlay,
    changes: &HeaderModifier,
) -> Result<(), OverlayFull> {
    let mut editing = Editing {
        view,
        overlay,
        full: false,
    };
    changes.apply(&mut editing);
    if editing.full {
        return Err(OverlayFull);
    }
    Ok(())
}

/// Takes what will not arrive out of a head's `Trailer` declaration
/// ([`crate::h1::filter_declaration`]).
fn filter_declaration(
    view: View<'_>,
    overlay: &mut Overlay,
    nominated: &[HeaderName],
) -> Result<(), OverlayFull> {
    match declaration(overlay.edited(view).values_of(&TRAILER), nominated) {
        Declaration::None => Ok(()),
        Declaration::Gone => {
            overlay.remove(&view, &TRAILER);
            Ok(())
        }
        Declaration::Kept(value) => overlay.set(&view, TRAILER, value),
    }
}

/// A raw head's overlay while a modifier's changes are made to it, keeping note of a change
/// it could not take.
struct Editing<'a> {
    view: View<'a>,
    overlay: &'a mut Overlay,
    full: bool,
}

impl Edit for Editing<'_> {
    fn remove(&mut self, name: &HeaderName) {
        self.overlay.remove(&self.view, name);
    }

    fn set(&mut self, name: &HeaderName, value: &HeaderValue) {
        if self
            .overlay
            .set(&self.view, name.clone(), value.clone())
            .is_err()
        {
            self.full = true;
        }
    }

    fn append(&mut self, name: &HeaderName, value: &HeaderValue) {
        if self.overlay.append(name.clone(), value.clone()).is_err() {
            self.full = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Our own server's heads make their edits in room the worker's blocks lend, and give
    /// it back, emptied, when they are done: the next head has it without making it again.
    #[test]
    fn a_heads_room_for_edits_is_lent_and_given_back() {
        let blocks = Rc::new(RefCell::new(Blocks::new(
            crate::upstream::h1::blocks::Sizes::default(),
            crate::storage::Storage::new(crate::storage::LIMIT),
        )));
        let sent = b"GET / HTTP/1.1\r\nhost: a\r\n\r\n";
        let lent = || {
            let mut room = [httparse::EMPTY_HEADER; 4];
            let mut request = httparse::Request::new(&mut room);
            assert!(request.parse(sent).unwrap().is_complete());
            let lines = FieldLines::new(sent, request.headers).unwrap();
            RawHead::lent(
                Method::GET,
                Uri::from_static("/"),
                Version::HTTP_11,
                Bytes::from_static(sent),
                lines,
                &blocks,
            )
        };
        let mut first = lent();
        for n in 0..5 {
            let name = HeaderName::from_bytes(format!("x-{n}").as_bytes()).unwrap();
            first
                .append_field(name, HeaderValue::from_static("v"))
                .unwrap();
        }
        drop(first);
        let given = blocks.borrow_mut().take_edits();
        assert!(given.is_empty());
        let room = given.capacity();
        assert!(room >= 5, "{room}");
        blocks.borrow_mut().give_edits(given);

        let second = lent();
        assert_eq!(
            second.fields().iter().count(),
            1,
            "nothing of the first is left"
        );
        drop(second);
        assert_eq!(blocks.borrow_mut().take_edits().capacity(), room);
    }

    /// An overlay that can take no more refuses the request, rather than dropping a change
    /// or panicking: no config the gateway accepts gets it there, so this drives it there.
    #[test]
    fn a_raw_head_that_cannot_take_its_changes_is_refused() {
        let sent = b"GET / HTTP/1.1\r\nhost: a\r\n\r\n";
        let mut room = [httparse::EMPTY_HEADER; 4];
        let mut request = httparse::Request::new(&mut room);
        assert!(request.parse(sent).unwrap().is_complete());
        let lines = FieldLines::new(sent, request.headers).unwrap();
        let mut raw = RawHead::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            Bytes::from_static(sent),
            lines,
        );
        let names: Vec<String> = (0..edgerush_filters::MOST_PER_LIST)
            .map(|n| format!("x-{n}"))
            .collect();
        let sixteen =
            HeaderModifier::new([], names.iter().map(|name| (name.as_str(), "v")), []).unwrap();
        let fits = crate::fields::MOST_ADDED / edgerush_filters::MOST_PER_LIST;
        for _ in 0..fits {
            assert_eq!(raw.apply(&sixteen), Ok(()));
        }
        assert_eq!(raw.apply(&sixteen), Err(Rejection::Edits));
    }

    /// A request's ID as the core gives it: never indexed.
    fn sensitive_id() -> HeaderValue {
        let mut id = HeaderValue::from_static("0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f");
        id.set_sensitive(true);
        id
    }

    /// An answer made a map keeps what was added as it was added: a value never to be
    /// indexed stays so, and what arrived is read as it was.
    #[test]
    fn an_answer_made_a_map_keeps_what_was_added_as_it_was() {
        let sent =
            b"HTTP/1.1 200 OK\r\nvia: 1.1 a\r\nx-request-id: theirs\r\ncontent-length: 0\r\n\r\n";
        let (_, mut raw) = both_answers(sent).unwrap();
        raw.set_field(HeaderName::from_static("x-request-id"), sensitive_id())
            .unwrap();
        let parts = raw.into_parts();
        let ids: Vec<&HeaderValue> = parts.headers.get_all("x-request-id").iter().collect();
        assert_eq!(ids, [&sensitive_id()]);
        assert!(ids[0].is_sensitive());
        assert_eq!(parts.headers["via"], "1.1 a");
        assert!(!parts.headers["via"].is_sensitive());
        assert_eq!(parts.headers["content-length"], "0");
    }

    /// A raw head hands its fields on as it holds them: the lines kept, as they arrived,
    /// then what was added, as it was added.
    #[test]
    fn a_raw_head_hands_on_its_lines_then_what_was_added() {
        let sent = b"GET / HTTP/1.1\r\nhost: a\r\nx-request-id: mine\r\nvia: 1.0 cdn\r\n\r\n";
        let mut room = [httparse::EMPTY_HEADER; 8];
        let mut request = httparse::Request::new(&mut room);
        assert!(request.parse(sent).unwrap().is_complete());
        let lines = FieldLines::new(sent, request.headers).unwrap();
        let mut raw = RawHead::new(
            Method::GET,
            Uri::from_static("/"),
            Version::HTTP_11,
            Bytes::from_static(sent),
            lines,
        );
        raw.set_field(HeaderName::from_static("x-request-id"), sensitive_id())
            .unwrap();
        raw.append_field(http::header::VIA, HeaderValue::from_static("1.1 edgerush"))
            .unwrap();
        let mut handed = Vec::new();
        raw.each_outgoing(|field| {
            handed.push(match field {
                HeldField::Line(name, value) => (
                    "line",
                    String::from_utf8(name.to_vec()).unwrap(),
                    String::from_utf8(value.to_vec()).unwrap(),
                    false,
                ),
                HeldField::Added(name, value) => (
                    "added",
                    name.to_string(),
                    value.to_str().unwrap().to_owned(),
                    value.is_sensitive(),
                ),
            });
        });
        let expected = [
            ("line", "host", "a", false),
            ("line", "via", "1.0 cdn", false),
            (
                "added",
                "x-request-id",
                "0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f",
                true,
            ),
            ("added", "via", "1.1 edgerush", false),
        ]
        .map(|(kind, name, value, sensitive)| (kind, name.to_owned(), value.to_owned(), sensitive));
        assert_eq!(handed, expected);
    }

    /// An upstream's answer as our own client reads it, and the header map the same bytes
    /// make; `None` for bytes neither would take.
    fn both_answers(sent: &[u8]) -> Option<(HeaderMap, RawAnswer)> {
        let mut room = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut room);
        if !matches!(response.parse(sent), Ok(httparse::Status::Complete(_))) {
            return None;
        }
        let status = http::StatusCode::from_u16(response.code?).ok()?;
        let mut map = HeaderMap::new();
        for field in response.headers.iter() {
            map.append(
                HeaderName::from_bytes(field.name.as_bytes()).ok()?,
                HeaderValue::from_bytes(field.value).ok()?,
            );
        }
        let lines = FieldLines::new(sent, response.headers).ok()?;
        let raw = RawAnswer::new(status, Bytes::copy_from_slice(sent), lines);
        Some((map, raw))
    }

    /// Every field, the name in lower case, sorted by name and in arrival order within
    /// one: what a field section means, whoever holds it (RFC 9110 §5.3).
    fn meaning<'a>(fields: impl Iterator<Item = (&'a [u8], &'a [u8])>) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut all: Vec<(Vec<u8>, Vec<u8>)> = fields
            .map(|(name, value)| (name.to_ascii_lowercase(), value.to_vec()))
            .collect();
        all.sort_by(|one, other| one.0.cmp(&other.0));
        all
    }

    fn of_map(map: &HeaderMap) -> Vec<(Vec<u8>, Vec<u8>)> {
        meaning(
            map.iter()
                .map(|(name, value)| (name.as_str().as_bytes(), value.as_bytes())),
        )
    }

    /// Names in any case, among them every one the answer's rules do something with, and
    /// values that make them mean something: `Connection` naming other fields, `Trailer`
    /// declaring denied and nominated ones, challenges.
    fn answer_field() -> impl proptest::strategy::Strategy<Value = (&'static str, &'static str)> {
        use proptest::prelude::*;
        (
            prop::sample::select(vec![
                "connection",
                "Connection",
                "keep-alive",
                "Keep-Alive",
                "trailer",
                "Trailer",
                "te",
                "transfer-encoding",
                "upgrade",
                "proxy-connection",
                "proxy-authenticate",
                "Proxy-Authentication-Info",
                "www-authenticate",
                "WWW-Authenticate",
                "content-length",
                "date",
                "x-a",
                "X-A",
                "x-b",
                "set-cookie",
            ]),
            prop::sample::select(vec![
                "close",
                "x-a",
                "x-a, keep-alive",
                "X-B, trailer",
                "x-a, x-b, content-length",
                "grpc-status, x-a",
                "x-b, date",
                "NTLM",
                "Negotiate abc",
                "Basic realm=\"a\"",
                "5",
                "chunked",
                "Tue, 15 Nov 1994 08:12:31 GMT",
                "a=1",
                "",
            ]),
        )
    }

    fn edit() -> impl proptest::strategy::Strategy<Value = (&'static str, &'static str)> {
        use proptest::prelude::*;
        (
            prop::sample::select(vec!["x-a", "x-b", "x-c", "trailer", "date", "set-cookie"]),
            prop::sample::select(vec!["1", "2", "x-a", "grpc-status"]),
        )
    }

    proptest::proptest! {
        /// A raw answer is edited as the header map of the same bytes is, by each of the
        /// rules the way to the client puts it through, in the order it does: every field
        /// the same after, whether it counts as a challenge the same, and made into a map
        /// for HTTP/2 the same map.
        #[test]
        fn a_raw_answer_is_edited_as_its_header_map_is(
            status in proptest::sample::select(vec![200u16, 204, 304, 401, 407, 500]),
            fields in proptest::collection::vec(answer_field(), 0..8),
            set in proptest::collection::vec(edit(), 0..3),
            add in proptest::collection::vec(edit(), 0..3),
            remove in proptest::collection::vec(
                proptest::sample::select(vec!["x-a", "x-b", "date", "trailer"]), 0..3),
            content in 0..3u8,
            length in 0..100u64,
            asked_head in proptest::prelude::any::<bool>(),
            old in proptest::prelude::any::<bool>(),
            trailers in proptest::prelude::any::<bool>(),
            persistent in proptest::prelude::any::<bool>(),
        ) {
            use crate::upstream::auth::challenges;
            use proptest::prelude::*;
            let mut sent = format!("HTTP/1.1 {status} Any\r\n").into_bytes();
            for (name, value) in &fields {
                sent.extend_from_slice(format!("{name}: {value}\r\n").as_bytes());
            }
            sent.extend_from_slice(b"\r\n");
            let (mut map, mut raw) = both_answers(&sent).unwrap();
            // A modifier the gateway would take; one it would refuse is no case at all.
            let Ok(changes) = HeaderModifier::new(set, add, remove) else {
                return Ok(());
            };
            let status = raw.status();

            let nominated = crate::hop_by_hop::nominated(&map);
            prop_assert_eq!(&crate::hop_by_hop::nominated(&raw), &nominated);
            crate::h1::filter_declaration(&mut map, &nominated);
            prop_assert_eq!(raw.filter_declaration(&nominated), Ok(()));
            prop_assert_eq!(of_map(&map), meaning(raw.fields().iter()), "declaration");
            prop_assert_eq!(challenges(status, &raw), challenges(status, &map));
            crate::hop_by_hop::strip_response(&mut map);
            raw.strip();
            prop_assert_eq!(of_map(&map), meaning(raw.fields().iter()), "stripped");
            changes.apply(&mut map);
            prop_assert_eq!(raw.apply(&changes), Ok(()));
            prop_assert_eq!(of_map(&map), meaning(raw.fields().iter()), "edited");

            // Written for the client, both ways: the same status line, and the same fields
            // by meaning, the framing and the date the writer adds among them.
            {
                use crate::downstream::h1::date::HttpDate;
                use crate::downstream::h1::writer::{Asked, Content, write_head};
                let content = match content {
                    0 => Content::Empty,
                    1 => Content::Length(length),
                    _ => Content::Unknown,
                };
                let asked = Asked {
                    head: asked_head,
                    version: if old { Version::HTTP_10 } else { Version::HTTP_11 },
                    trailers,
                };
                let date = HttpDate::from_unix(784_111_777);
                let (mut by_map, mut by_raw) = (Vec::new(), Vec::new());
                let from_map = write_head(&mut by_map, status, &map, content, asked, persistent, &date);
                let from_raw = write_head(&mut by_raw, status, &raw, content, asked, persistent, &date);
                prop_assert_eq!(from_raw, from_map);
                let read = |head: &[u8]| {
                    let mut room = [httparse::EMPTY_HEADER; 64];
                    let mut response = httparse::Response::new(&mut room);
                    assert!(response.parse(head).unwrap().is_complete(), "{head:?}");
                    let line = head.split(|&byte| byte == b'\n').next().map(<[u8]>::to_vec);
                    let fields = meaning(response.headers.iter().map(|field| (field.name.as_bytes(), field.value)));
                    (line, fields)
                };
                if from_map.is_ok() {
                    prop_assert_eq!(read(&by_raw), read(&by_map));
                }
            }

            let parts = raw.into_parts();
            prop_assert_eq!(parts.status, status);
            prop_assert_eq!(parts.version, Version::HTTP_11);
            prop_assert_eq!(of_map(&parts.headers), of_map(&map));
        }
    }

    /// An overlay that can take no more refuses the answer's changes, rather than dropping
    /// one or panicking, as a request's does.
    #[test]
    fn a_raw_answer_that_cannot_take_its_changes_says_so() {
        let (_, mut raw) = both_answers(b"HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n").unwrap();
        let names: Vec<String> = (0..edgerush_filters::MOST_PER_LIST)
            .map(|n| format!("x-{n}"))
            .collect();
        let sixteen =
            HeaderModifier::new([], names.iter().map(|name| (name.as_str(), "v")), []).unwrap();
        let fits = crate::fields::MOST_ADDED / edgerush_filters::MOST_PER_LIST;
        for _ in 0..fits {
            assert_eq!(raw.apply(&sixteen), Ok(()));
        }
        assert_eq!(raw.apply(&sixteen), Err(OverlayFull));
    }

    /// **An answer's lines reach the client as the upstream wrote them**: the case of each
    /// name, the space around each value and the order of the lines, in one run, with only
    /// what the writer owns written anew after them.
    #[test]
    fn an_answer_is_written_with_its_lines_as_they_came() {
        use crate::downstream::h1::date::HttpDate;
        use crate::downstream::h1::writer::{Asked, Content, write_head};
        let sent = b"HTTP/1.1 200 OK\r\nX-Mixed-Case:  spaced \r\nContent-Length: 2\r\nDate: Sun, 06 Nov 1994 08:49:37 GMT\r\nconnection: keep-alive\r\n\r\n";
        let (_, mut raw) = both_answers(sent).unwrap();
        raw.strip();
        let asked = Asked {
            head: false,
            version: Version::HTTP_11,
            trailers: false,
        };
        let mut out = Vec::new();
        write_head(
            &mut out,
            raw.status(),
            &raw,
            Content::Length(2),
            asked,
            true,
            &HttpDate::from_unix(0),
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "HTTP/1.1 200 OK\r\nX-Mixed-Case:  spaced \r\nDate: Sun, 06 Nov 1994 08:49:37 GMT\r\ncontent-length: 2\r\n\r\n"
        );
    }

    /// What nothing changes is left as it came: an answer with no hop-by-hop fields and no
    /// rule to meet is one run of its lines, copied whole.
    #[test]
    fn an_answer_left_alone_is_copied_whole() {
        let sent = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nX-Trace: a\r\n\r\n";
        let (_, mut raw) = both_answers(sent).unwrap();
        assert_eq!(raw.filter_declaration(&[]), Ok(()));
        raw.strip();
        let pieces: Vec<_> = raw.pieces(&[]).collect();
        let section = b"HTTP/1.1 200 OK\r\n".len()..sent.len() - 2;
        assert_eq!(pieces, [Piece::Copy(section)]);
    }
}
