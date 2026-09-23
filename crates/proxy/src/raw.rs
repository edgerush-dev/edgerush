//! A request's head as our own server reads it
//! ([14 §6](../../../docs/14-downstream-server.md)): the bytes it arrived as, where each
//! field line lies in them, and an overlay of what the request core changes, doing all that
//! [`Head`] asks as a header map would.

// `pub` for the fuzz targets and benchmarks, which are crates of their own; in an ordinary
// build none of this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::fields::{Edited, FieldLines, Overlay, Piece, View};
use crate::head::{Head, Survey};
use crate::hop_by_hop::{self, ConnectionError, HOP_BY_HOP, is_hop_by_hop_name, options_of};
use crate::host::HostError;
use crate::request::Rejection;
use bytes::Bytes;
use edgerush_filters::{Edit, HeaderModifier};
use edgerush_router::Fields;
use http::header::{CONNECTION, COOKIE, HOST, HeaderName, HeaderValue, TE};
use http::{Method, Uri};

/// A request's head as our own server read it: its method and target, the bytes of the
/// head, where each field line lies in them, and an overlay of what the core changes. Its
/// fields are read out of those bytes and every change goes to the overlay, so a line the
/// core leaves alone reaches the upstream as it arrived (14 §6).
#[derive(Debug)]
pub struct RawHead {
    method: Method,
    uri: Uri,
    head: Bytes,
    lines: FieldLines,
    overlay: Overlay,
}

impl RawHead {
    /// The head in `head`, whose field lines `lines` says where they are.
    pub fn new(method: Method, uri: Uri, head: Bytes, lines: FieldLines) -> Self {
        Self {
            method,
            uri,
            head,
            lines,
            overlay: Overlay::default(),
        }
    }

    fn view(&self) -> View<'_> {
        self.lines.view(&self.head)
    }

    /// The bytes of the head, which [`RawHead::pieces`] copies from.
    pub fn bytes(&self) -> &Bytes {
        &self.head
    }

    /// What its fields, as edited, are written as: runs of the lines kept, then the fields
    /// added.
    pub fn pieces(&self) -> impl Iterator<Item = Piece<'_>> {
        self.overlay.pieces(&self.lines)
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

    fn check_connection(&self) -> Result<(), ConnectionError> {
        hop_by_hop::check_connection_values(self.fields().values(&CONNECTION))
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
        for value in view.values(&CONNECTION) {
            for option in options_of(value) {
                self.overlay.remove_named(&view, option);
            }
        }
        for name in &HOP_BY_HOP {
            self.overlay.remove(&view, name);
        }
        if accepts_trailers {
            self.overlay
                .set(&view, TE, HeaderValue::from_static("trailers"))
                .map_err(|_| Rejection::Edits)?;
        }
        Ok(())
    }

    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), Rejection> {
        let mut editing = Editing {
            view: self.lines.view(&self.head),
            overlay: &mut self.overlay,
            full: false,
        };
        changes.apply(&mut editing);
        if editing.full {
            return Err(Rejection::Edits);
        }
        Ok(())
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
}
