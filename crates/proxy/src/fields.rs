//! The field lines of an HTTP/1 head as they arrived: where each lies in the head's bytes,
//! with the headers the gateway itself reads found once, while the head is read
//! ([14 §6](../../../docs/14-downstream-server.md)).
//!
//! Nothing is copied. A line is a handful of offsets into the head, which its owner keeps,
//! and a [`View`] puts the two together to read like a header map — the values of a name in
//! the order they came — through [`Fields`], so that route predicates see the same either
//! way. A name is found by a scan, which for a head's few lines costs less than building a
//! hash would; the eleven names the gateway reads for itself ([`Known`]) have slots instead,
//! so asking for them again costs nothing. What a line spans, from its first byte to the
//! end of its line break, is kept too: an unchanged line is forwarded as the bytes it
//! arrived as.
//!
//! Edits leave the head alone. An [`Overlay`] records which lines are taken out and what
//! is added, reads as a header map edited the same way would, and gives what is to be
//! written as [`Piece`]s: the kept lines, each run of neighbours one copy, then the added
//! fields.

// `pub` for the fuzz targets, which are a crate of their own; in an ordinary build none of
// this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use crate::h1::MOST_FIELDS;
use edgerush_router::Fields;
use http::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, COOKIE, DATE, EXPECT, HOST, PROXY_AUTHORIZATION, TE,
    TRAILER, TRANSFER_ENCODING,
};
use http::{HeaderName, HeaderValue};
use std::ops::Range;

/// The headers the gateway reads by name for itself, each with a slot of its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Known {
    /// `Host`.
    Host,
    /// `Connection`.
    Connection,
    /// `Content-Length`.
    ContentLength,
    /// `Transfer-Encoding`.
    TransferEncoding,
    /// `TE`.
    Te,
    /// `Expect`.
    Expect,
    /// `Cookie`.
    Cookie,
    /// `Trailer`.
    Trailer,
    /// `Authorization`.
    Authorization,
    /// `Proxy-Authorization`.
    ProxyAuthorization,
    /// `Date`, which the writer asks of every answer.
    Date,
}

impl Known {
    /// Every one of them, in the order of their slots.
    pub const ALL: [Self; 11] = [
        Self::Host,
        Self::Connection,
        Self::ContentLength,
        Self::TransferEncoding,
        Self::Te,
        Self::Expect,
        Self::Cookie,
        Self::Trailer,
        Self::Authorization,
        Self::ProxyAuthorization,
        Self::Date,
    ];

    /// The header's name.
    #[must_use]
    pub fn name(self) -> HeaderName {
        match self {
            Self::Host => HOST,
            Self::Connection => CONNECTION,
            Self::ContentLength => CONTENT_LENGTH,
            Self::TransferEncoding => TRANSFER_ENCODING,
            Self::Te => TE,
            Self::Expect => EXPECT,
            Self::Cookie => COOKIE,
            Self::Trailer => TRAILER,
            Self::Authorization => AUTHORIZATION,
            Self::ProxyAuthorization => PROXY_AUTHORIZATION,
            Self::Date => DATE,
        }
    }

    /// Which of them a field name as it arrived is, in whatever case.
    ///
    /// Asked of every line of every head. Their lengths tell them apart all but once, so a
    /// name is compared with one of them, or two, or none at all.
    fn of_bytes(name: &[u8]) -> Option<Self> {
        let is = |known: &[u8]| name.eq_ignore_ascii_case(known);
        match name.len() {
            2 if is(b"te") => Some(Self::Te),
            4 if is(b"host") => Some(Self::Host),
            4 if is(b"date") => Some(Self::Date),
            6 if is(b"cookie") => Some(Self::Cookie),
            6 if is(b"expect") => Some(Self::Expect),
            7 if is(b"trailer") => Some(Self::Trailer),
            10 if is(b"connection") => Some(Self::Connection),
            13 if is(b"authorization") => Some(Self::Authorization),
            14 if is(b"content-length") => Some(Self::ContentLength),
            17 if is(b"transfer-encoding") => Some(Self::TransferEncoding),
            19 if is(b"proxy-authorization") => Some(Self::ProxyAuthorization),
            _ => None,
        }
    }

    /// Which of them a header name is.
    fn of_name(name: &HeaderName) -> Option<Self> {
        Self::ALL.into_iter().find(|known| known.name() == name)
    }

    fn slot(self) -> usize {
        self as usize
    }
}

/// Where one field line lies in its head.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Line {
    /// The first byte of the name, which is where the line begins.
    start: u32,
    name_end: u32,
    value: (u32, u32),
    /// Just past the line feed that ends the line.
    end: u32,
    known: Option<Known>,
}

/// Where a known header's lines are.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct Slot {
    /// The position of the first line with the name.
    first: u32,
    /// How many lines have it.
    count: u32,
}

/// Why the field lines of a head could not be taken down.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FieldsError {
    /// A field is not a part of the head it is said to be in, or does not end in a line
    /// break there.
    #[error("a field is not a line of its head")]
    NotALine,
    /// More fields than any head is read into.
    #[error("more than {MOST_FIELDS} fields")]
    TooMany,
}

// Every line has a bit of its own in an overlay's record of what it took out.
const _: () = assert!(MOST_FIELDS <= u128::BITS as usize);

/// The field lines of one head: where each is, and where the known headers are.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldLines {
    lines: Vec<Line>,
    slots: [Slot; Known::ALL.len()],
}

impl FieldLines {
    /// Takes down where the fields a parser found in `head` are. Each field's name and
    /// value must be slices of `head` itself, as a parser's are.
    ///
    /// # Errors
    ///
    /// [`FieldsError::NotALine`] if a field is not a part of `head`, or its line is not
    /// ended by a line feed; [`FieldsError::TooMany`] if there are more than
    /// [`MOST_FIELDS`].
    pub fn new(head: &[u8], fields: &[httparse::Header<'_>]) -> Result<Self, FieldsError> {
        if fields.len() > MOST_FIELDS {
            return Err(FieldsError::TooMany);
        }
        let mut lines = Vec::with_capacity(fields.len());
        let mut slots = [Slot::default(); Known::ALL.len()];
        for field in fields {
            let name = within(head, field.name.as_bytes()).ok_or(FieldsError::NotALine)?;
            let value = within(head, field.value).ok_or(FieldsError::NotALine)?;
            // What follows the value up to the line feed is white space the parser left
            // out of it.
            let feed = head
                .get(value.end..)
                .and_then(|rest| rest.iter().position(|&byte| byte == b'\n'))
                .ok_or(FieldsError::NotALine)?;
            let known = Known::of_bytes(field.name.as_bytes());
            let position = offset(lines.len())?;
            if let Some(known) = known {
                let slot = &mut slots[known.slot()];
                if slot.count == 0 {
                    slot.first = position;
                }
                slot.count += 1;
            }
            lines.push(Line {
                start: offset(name.start)?,
                name_end: offset(name.end)?,
                value: (offset(value.start)?, offset(value.end)?),
                end: offset(value.end + feed + 1)?,
                known,
            });
        }
        Ok(Self { lines, slots })
    }

    /// Reads the lines out of the head they were taken from.
    #[must_use]
    pub fn view<'a>(&'a self, head: &'a [u8]) -> View<'a> {
        View { lines: self, head }
    }

    /// How many lines have the known header's name.
    #[cfg(any(test, feature = "fuzzing"))]
    #[must_use]
    pub fn count(&self, known: Known) -> usize {
        self.slots[known.slot()].count as usize
    }

    /// How many lines there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lines.len()
    }

    /// Whether there are none.
    #[cfg(any(test, feature = "fuzzing"))]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// The bytes each line spans in its head, from its first byte to the end of its line
    /// break, in the order the lines came.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn spans(&self) -> impl Iterator<Item = Range<usize>> + '_ {
        self.lines
            .iter()
            .map(|line| line.start as usize..line.end as usize)
    }
}

/// Field lines together with the head they are in.
#[derive(Debug, Clone, Copy)]
pub struct View<'a> {
    lines: &'a FieldLines,
    head: &'a [u8],
}

impl<'a> View<'a> {
    /// The values of the known header, in the order its lines came.
    #[cfg(any(test, feature = "fuzzing"))]
    pub fn known(&self, known: Known) -> impl Iterator<Item = &'a [u8]> + use<'a> {
        let slot = self.lines.slots[known.slot()];
        let head = self.head;
        self.lines
            .lines
            .get(slot.first as usize..)
            .unwrap_or_default()
            .iter()
            .filter(move |line| line.known == Some(known))
            .take(slot.count as usize)
            .map(move |line| bytes(head, line.value.0..line.value.1))
    }

    /// Every field as a name as it arrived and a value, in the order the lines came.
    pub fn iter(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + use<'a> {
        let head = self.head;
        self.lines.lines.iter().map(move |line| {
            (
                bytes(head, line.start..line.name_end),
                bytes(head, line.value.0..line.value.1),
            )
        })
    }
}

impl<'a> View<'a> {
    /// The lines called `name`, each with its position, in the order they came.
    fn called<'n>(
        &self,
        name: &'n HeaderName,
    ) -> impl Iterator<Item = (usize, &'a Line)> + use<'a, 'n> {
        let known = Known::of_name(name);
        let wanted = name.as_str().as_bytes();
        let head = self.head;
        let (first, count) = match known {
            Some(known) => {
                let slot = self.lines.slots[known.slot()];
                (slot.first as usize, slot.count as usize)
            }
            None => (0, usize::MAX),
        };
        self.lines
            .lines
            .get(first..)
            .unwrap_or_default()
            .iter()
            .enumerate()
            .map(move |(at, line)| (first + at, line))
            .filter(move |(_, line)| match known {
                Some(known) => line.known == Some(known),
                // A name that is not a known one is not on a line that has one.
                None => {
                    line.known.is_none()
                        && bytes(head, line.start..line.name_end).eq_ignore_ascii_case(wanted)
                }
            })
            .take(count)
    }

    fn value(&self, line: &Line) -> &'a [u8] {
        bytes(self.head, line.value.0..line.value.1)
    }
}

impl<'a> View<'a> {
    /// The values of `name`, in the order its lines came, for as long as the head they are
    /// read from.
    pub fn values_of<'n>(
        &self,
        name: &'n HeaderName,
    ) -> impl Iterator<Item = &'a [u8]> + use<'a, 'n> {
        let view = *self;
        self.called(name).map(move |(_, line)| view.value(line))
    }
}

impl Fields for View<'_> {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        self.values_of(name)
    }
}

/// The most fields an overlay adds: as many as a head may arrive with.
pub const MOST_ADDED: usize = MOST_FIELDS;

/// An overlay has added as many fields as it may.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("more than {MOST_ADDED} fields added to a head")]
pub struct OverlayFull;

/// Edits to a head's fields that leave the head as it is: which of its lines are taken
/// out, and what is added after the rest. It belongs to the lines it was made against, and
/// is only ever read with them.
///
/// It gives what a header map's edits would — for every name the same values in the same
/// order — while what is kept of the head is still the bytes it arrived as.
#[derive(Debug, Clone, Default)]
pub struct Overlay {
    /// A bit for each of the head's lines, set when the line is taken out.
    removed: u128,
    /// Fields to write after what is kept of the head, in the order they were added.
    added: Vec<(HeaderName, HeaderValue)>,
}

impl Overlay {
    /// Takes out every field called `name`: the head's lines, whatever the case of their
    /// name, and anything added under it before.
    pub fn remove(&mut self, view: &View<'_>, name: &HeaderName) {
        for (at, _) in view.called(name) {
            self.removed |= bit(at);
        }
        self.added.retain(|(added, _)| added != name);
    }

    /// Takes out every field whose name, as it arrived or was added, `unwanted` picks: one
    /// pass over the head's lines, however many names that comes to.
    pub fn remove_where(&mut self, view: &View<'_>, mut unwanted: impl FnMut(&[u8]) -> bool) {
        for (at, line) in view.lines.lines.iter().enumerate() {
            if unwanted(bytes(view.head, line.start..line.name_end)) {
                self.removed |= bit(at);
            }
        }
        self.added
            .retain(|(name, _)| !unwanted(name.as_str().as_bytes()));
    }

    /// Gives `name` this one value, in place of every one it had.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if as many fields have been added as may be, and then nothing
    /// is changed.
    pub fn set(
        &mut self,
        view: &View<'_>,
        name: HeaderName,
        value: HeaderValue,
    ) -> Result<(), OverlayFull> {
        if self.added.len() >= MOST_ADDED {
            return Err(OverlayFull);
        }
        self.remove(view, &name);
        self.added.push((name, value));
        Ok(())
    }

    /// Adds a value to `name`, after every one it has.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if as many fields have been added as may be.
    pub fn append(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), OverlayFull> {
        if self.added.len() >= MOST_ADDED {
            return Err(OverlayFull);
        }
        self.added.push((name, value));
        Ok(())
    }

    /// The head's fields as edited, read like a header map.
    #[must_use]
    pub fn edited<'a>(&'a self, view: View<'a>) -> Edited<'a> {
        Edited {
            view,
            overlay: self,
        }
    }

    /// What the edited fields are written as: the lines kept, each run of them that stood
    /// side by side copied as one, and then the fields added, in the order they were. The
    /// known headers in `skip` are left out wherever they are: those a writer writes
    /// itself, as its framing.
    pub fn pieces<'a>(
        &'a self,
        lines: &'a FieldLines,
        skip: &'a [Known],
    ) -> impl Iterator<Item = Piece<'a>> + 'a {
        let lines = &lines.lines;
        let removed = self.removed;
        let left = move |at: usize, line: &Line| {
            removed & bit(at) == 0 && line.known.is_none_or(|known| !skip.contains(&known))
        };
        let mut at = 0;
        let runs = std::iter::from_fn(move || {
            while lines.get(at).is_some_and(|line| !left(at, line)) {
                at += 1;
            }
            let first = lines.get(at)?;
            let mut last = first;
            at += 1;
            while let Some(line) = lines.get(at).filter(|line| left(at, line)) {
                last = line;
                at += 1;
            }
            Some(Piece::Copy(first.start as usize..last.end as usize))
        });
        let added = self
            .added
            .iter()
            .filter(move |(name, _)| !skip.iter().any(|known| known.name() == name))
            .map(|(name, value)| Piece::Field(name, value));
        runs.chain(added)
    }
}

/// A piece of an edited head's field section, as it is to be written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Piece<'a> {
    /// These bytes of the head, whole lines with their line breaks, as they arrived.
    Copy(Range<usize>),
    /// A field to write out.
    Field(&'a HeaderName, &'a HeaderValue),
}

/// A head's fields as an overlay has edited them.
#[derive(Debug, Clone, Copy)]
pub struct Edited<'a> {
    view: View<'a>,
    overlay: &'a Overlay,
}

impl<'a> Edited<'a> {
    /// Every field as edited, a name as it arrived or was added and a value: the lines
    /// kept, in their order, then what was added.
    pub fn iter(&self) -> impl Iterator<Item = (&'a [u8], &'a [u8])> + use<'a> {
        let (view, overlay) = (self.view, self.overlay);
        let removed = overlay.removed;
        let kept = view
            .lines
            .lines
            .iter()
            .enumerate()
            .filter(move |(at, _)| removed & bit(*at) == 0)
            .map(move |(_, line)| {
                (
                    bytes(view.head, line.start..line.name_end),
                    view.value(line),
                )
            });
        let added = overlay
            .added
            .iter()
            .map(|(name, value)| (name.as_str().as_bytes(), value.as_bytes()));
        kept.chain(added)
    }
}

impl<'a> Edited<'a> {
    /// The values of `name` as edited, for as long as the head they are read from: the
    /// lines kept, in their order, then what was added.
    pub fn values_of<'n>(
        &self,
        name: &'n HeaderName,
    ) -> impl Iterator<Item = &'a [u8]> + use<'a, 'n> {
        let (view, overlay) = (self.view, self.overlay);
        let removed = overlay.removed;
        let kept = view
            .called(name)
            .filter(move |(at, _)| removed & bit(*at) == 0)
            .map(move |(_, line)| view.value(line));
        let added = overlay
            .added
            .iter()
            .filter(move |(added, _)| added == name)
            .map(|(_, value)| value.as_bytes());
        kept.chain(added)
    }
}

impl Fields for Edited<'_> {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
        self.values_of(name)
    }
}

/// A line's bit in an overlay's record of what it took out. A head has no more lines than
/// there are bits (`FieldLines::new`).
fn bit(at: usize) -> u128 {
    u32::try_from(at)
        .ok()
        .and_then(|at| 1u128.checked_shl(at))
        .unwrap_or(0)
}

/// Where `part` lies in `whole`, if it is a part of it.
fn within(whole: &[u8], part: &[u8]) -> Option<Range<usize>> {
    let start = (part.as_ptr() as usize).checked_sub(whole.as_ptr() as usize)?;
    let end = start.checked_add(part.len())?;
    (end <= whole.len()).then_some(start..end)
}

/// An offset into a head, which is far shorter than the four gigabytes one can reach.
fn offset(at: usize) -> Result<u32, FieldsError> {
    u32::try_from(at).map_err(|_| FieldsError::NotALine)
}

/// The bytes a line's offsets give. Lines are only ever read out of the head they were
/// taken from, where every one of their offsets lies; anything else reads as empty rather
/// than panicking.
fn bytes(head: &[u8], range: Range<u32>) -> &[u8] {
    head.get(range.start as usize..range.end as usize)
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_router::{HeaderPredicate, HeaderPredicates};
    use http::{HeaderMap, HeaderValue};
    use proptest::prelude::*;

    /// Which known header a name is, by comparing it with each of them: what `of_bytes`
    /// must agree with.
    fn by_list(name: &[u8]) -> Option<Known> {
        Known::ALL
            .into_iter()
            .find(|known| known.name().as_str().as_bytes().eq_ignore_ascii_case(name))
    }

    /// Every known name is itself, in any case, and a name a byte off is none of them.
    #[test]
    fn a_known_name_is_found_in_any_case() {
        for known in Known::ALL {
            let name = known.name();
            let lower = name.as_str().as_bytes();
            assert_eq!(Known::of_bytes(lower), Some(known), "{name}");
            assert_eq!(
                Known::of_bytes(&lower.to_ascii_uppercase()),
                Some(known),
                "{name}"
            );
            let mut near = lower.to_vec();
            if let Some(last) = near.last_mut() {
                *last = b'x';
            }
            assert_eq!(Known::of_bytes(&near), None, "{name}");
            assert_eq!(Known::of_bytes(&lower[1..]), None, "{name}");
        }
    }

    /// Names of the list in any case, and any names at all.
    fn any_name() -> impl Strategy<Value = String> {
        let listed = (
            prop::sample::select(Known::ALL.to_vec()),
            prop::collection::vec(any::<bool>(), 20),
        )
            .prop_map(|(known, upper)| {
                known
                    .name()
                    .as_str()
                    .chars()
                    .zip(upper)
                    .map(|(c, up)| if up { c.to_ascii_uppercase() } else { c })
                    .collect::<String>()
            });
        prop_oneof![listed, "[a-zA-Z-]{0,20}"]
    }

    proptest! {
        /// And any name at all is the known header comparing it with each would say.
        #[test]
        fn any_name_is_known_as_the_list_says(name in any_name()) {
            prop_assert_eq!(Known::of_bytes(name.as_bytes()), by_list(name.as_bytes()));
        }
    }

    /// A head's field lines as a parser finds them, and the header map the downstream
    /// parser builds of them today.
    fn read(head: &[u8]) -> (FieldLines, HeaderMap) {
        let mut room = [httparse::EMPTY_HEADER; 64];
        let mut request = httparse::Request::new(&mut room);
        let status = request.parse(head).unwrap();
        assert!(status.is_complete());
        let mut map = HeaderMap::new();
        for field in request.headers.iter() {
            map.append(
                HeaderName::from_bytes(field.name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(field.value).unwrap(),
            );
        }
        (FieldLines::new(head, request.headers).unwrap(), map)
    }

    fn values<F: Fields>(fields: &F, name: &HeaderName) -> Vec<Vec<u8>> {
        fields.values(name).map(<[u8]>::to_vec).collect()
    }

    #[test]
    fn values_come_in_the_order_of_their_lines_whatever_the_case_of_the_name() {
        let head = b"GET / HTTP/1.1\r\nX-Tag: a\r\nHost: example.com\r\nx-tag:b \r\nCOOKIE: c=1\r\ncookie:\td=2\r\n\r\n";
        let (lines, _) = read(head);
        let view = lines.view(head);
        let tag = HeaderName::from_static("x-tag");
        assert_eq!(values(&view, &tag), [b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(values(&view, &COOKIE), [b"c=1".to_vec(), b"d=2".to_vec()]);
        assert_eq!(values(&view, &HOST), [b"example.com".to_vec()]);
        assert!(values(&view, &CONNECTION).is_empty());
        assert_eq!(lines.count(Known::Cookie), 2);
        assert_eq!(lines.count(Known::Host), 1);
        assert_eq!(lines.count(Known::Connection), 0);
        assert_eq!(
            view.known(Known::Cookie).collect::<Vec<_>>(),
            [b"c=1".as_slice(), b"d=2"]
        );
    }

    #[test]
    fn a_line_spans_everything_from_its_name_to_its_line_break() {
        let head = b"GET / HTTP/1.1\r\nX-A:  1 \t\r\nx-b:2\r\n\r\n";
        let (lines, _) = read(head);
        let spans: Vec<&[u8]> = lines.spans().map(|span| &head[span]).collect();
        assert_eq!(spans, [b"X-A:  1 \t\r\n".as_slice(), b"x-b:2\r\n"]);
        assert_eq!(
            lines.view(head).iter().collect::<Vec<_>>(),
            [(b"X-A".as_slice(), b"1".as_slice()), (b"x-b", b"2")]
        );
    }

    #[test]
    fn fields_from_elsewhere_are_refused() {
        let head = b"GET / HTTP/1.1\r\nx-a: 1\r\n\r\n";
        let elsewhere = httparse::Header {
            name: "x-a",
            value: b"1",
        };
        assert_eq!(
            FieldLines::new(head, &[elsewhere]).err(),
            Some(FieldsError::NotALine)
        );
    }

    /// Names from a small pool, in any case, among them every known one, so that repeats,
    /// near misses and interleaving come up all the time; values with the white space a
    /// client may put around them.
    fn field() -> impl Strategy<Value = (String, String)> {
        let names = prop::sample::select(vec![
            "x-a",
            "x-b",
            "accept",
            "host",
            "connection",
            "content-length",
            "transfer-encoding",
            "te",
            "expect",
            "cookie",
            "trailer",
            "authorization",
            "proxy-authorization",
            "x-host",
            "hos",
        ]);
        let case = prop::collection::vec(any::<bool>(), 20);
        let value = prop::sample::select(vec!["", "1", "2", "1,2", "a=1; b=2", "v w"]);
        let space = prop::sample::select(vec!["", " ", "\t", "  ", " \t"]);
        (names, case, value, space.clone(), space).prop_map(|(name, case, value, before, after)| {
            let name: String = name
                .chars()
                .zip(case)
                .map(|(c, upper)| if upper { c.to_ascii_uppercase() } else { c })
                .collect();
            let value = if value.is_empty() {
                String::new()
            } else {
                format!("{before}{value}{after}")
            };
            (name, value)
        })
    }

    fn head_of(fields: &[(String, String)]) -> Vec<u8> {
        let mut head = b"GET / HTTP/1.1\r\n".to_vec();
        for (name, value) in fields {
            head.extend_from_slice(format!("{name}:{value}\r\n").as_bytes());
        }
        head.extend_from_slice(b"\r\n");
        head
    }

    fn every_name() -> Vec<HeaderName> {
        let mut names: Vec<HeaderName> = Known::ALL.into_iter().map(Known::name).collect();
        for name in ["x-a", "x-b", "accept", "x-host", "hos", "x-absent"] {
            names.push(HeaderName::from_static(name));
        }
        names
    }

    fn predicate() -> impl Strategy<Value = HeaderPredicate> {
        (
            prop::sample::select(vec!["x-a", "X-B", "host", "Cookie", "te", "x-absent"]),
            prop::sample::select(vec!["1", "2", "1,2", "a=1; b=2", "v w", "1,1"]),
            any::<bool>(),
        )
            .prop_map(|(name, value, is_regex)| {
                if is_regex {
                    HeaderPredicate::regex(name, ".*2.*").unwrap()
                } else {
                    HeaderPredicate::exact(name, value).unwrap()
                }
            })
    }

    fn name(text: &str) -> HeaderName {
        HeaderName::from_bytes(text.as_bytes()).unwrap()
    }

    fn value(text: &str) -> HeaderValue {
        HeaderValue::from_str(text).unwrap()
    }

    /// The head an overlay's pieces make: copied runs and written fields, between the
    /// request line and the empty line.
    fn written(head: &[u8], overlay: &Overlay, lines: &FieldLines) -> Vec<u8> {
        let mut out = b"GET / HTTP/1.1\r\n".to_vec();
        for piece in overlay.pieces(lines, &[]) {
            match piece {
                Piece::Copy(span) => out.extend_from_slice(&head[span]),
                Piece::Field(name, value) => {
                    out.extend_from_slice(name.as_str().as_bytes());
                    out.extend_from_slice(b": ");
                    out.extend_from_slice(value.as_bytes());
                    out.extend_from_slice(b"\r\n");
                }
            }
        }
        out.extend_from_slice(b"\r\n");
        out
    }

    #[test]
    fn removing_a_name_takes_out_its_lines_in_any_case_and_what_was_added_under_it() {
        let head = b"GET / HTTP/1.1\r\nX-A: 1\r\nx-b: 2\r\nx-a: 3\r\n\r\n";
        let (lines, _) = read(head);
        let view = lines.view(head);
        let mut overlay = Overlay::default();
        overlay.append(name("x-a"), value("4")).unwrap();
        overlay.remove(&view, &name("x-a"));
        let edited = overlay.edited(view);
        assert!(values(&edited, &name("x-a")).is_empty());
        assert_eq!(values(&edited, &name("x-b")), [b"2".to_vec()]);
        assert_eq!(
            written(head, &overlay, &lines),
            b"GET / HTTP/1.1\r\nx-b: 2\r\n\r\n"
        );
    }

    #[test]
    fn setting_replaces_every_line_and_appending_adds_after_them() {
        let head = b"GET / HTTP/1.1\r\nHost: a\r\nX-Tag: 1\r\nx-tag: 2\r\n\r\n";
        let (lines, _) = read(head);
        let view = lines.view(head);
        let mut overlay = Overlay::default();
        overlay.set(&view, HOST, value("b")).unwrap();
        overlay.append(name("x-tag"), value("3")).unwrap();
        let edited = overlay.edited(view);
        assert_eq!(values(&edited, &HOST), [b"b".to_vec()]);
        assert_eq!(
            values(&edited, &name("x-tag")),
            [b"1".to_vec(), b"2".to_vec(), b"3".to_vec()]
        );
        // What is kept goes as it came, case and spacing too; what is new follows.
        assert_eq!(
            written(head, &overlay, &lines),
            b"GET / HTTP/1.1\r\nX-Tag: 1\r\nx-tag: 2\r\nhost: b\r\nx-tag: 3\r\n\r\n"
        );
    }

    #[test]
    fn lines_kept_side_by_side_are_copied_as_one_run() {
        let head = b"GET / HTTP/1.1\r\na: 1\r\nb: 2\r\nc: 3\r\nd: 4\r\ne: 5\r\n\r\n";
        let (lines, _) = read(head);
        let view = lines.view(head);
        let mut overlay = Overlay::default();
        overlay.remove(&view, &name("c"));
        let copied: Vec<&[u8]> = overlay
            .pieces(&lines, &[])
            .map(|piece| match piece {
                Piece::Copy(span) => &head[span],
                Piece::Field(..) => panic!("nothing was added"),
            })
            .collect();
        assert_eq!(
            copied,
            [b"a: 1\r\nb: 2\r\n".as_slice(), b"d: 4\r\ne: 5\r\n"]
        );

        let untouched = Overlay::default();
        assert_eq!(
            untouched.pieces(&lines, &[]).count(),
            1,
            "one run for a whole head"
        );
    }

    #[test]
    fn what_may_be_added_is_bounded_and_a_refused_edit_changes_nothing() {
        let head = b"GET / HTTP/1.1\r\nx-a: 1\r\n\r\n";
        let (lines, _) = read(head);
        let view = lines.view(head);
        let mut overlay = Overlay::default();
        for _ in 0..MOST_ADDED {
            overlay.append(name("x-b"), value("2")).unwrap();
        }
        assert_eq!(overlay.append(name("x-b"), value("2")), Err(OverlayFull));
        assert_eq!(
            overlay.set(&view, name("x-a"), value("3")),
            Err(OverlayFull)
        );
        assert_eq!(values(&overlay.edited(view), &name("x-a")), [b"1".to_vec()]);
    }

    #[test]
    fn a_head_with_more_fields_than_any_is_read_into_is_refused() {
        let mut head = b"GET / HTTP/1.1\r\n".to_vec();
        for _ in 0..=MOST_FIELDS {
            head.extend_from_slice(b"x-a: 1\r\n");
        }
        head.extend_from_slice(b"\r\n");
        let mut room = [httparse::EMPTY_HEADER; MOST_FIELDS + 1];
        let mut request = httparse::Request::new(&mut room);
        assert!(request.parse(&head).unwrap().is_complete());
        assert_eq!(
            FieldLines::new(&head, request.headers).err(),
            Some(FieldsError::TooMany)
        );
    }

    /// An edit as the gateway makes them: taking a name out, setting it to one value, or
    /// adding a value to it.
    #[derive(Debug, Clone)]
    enum Edit {
        Remove(&'static str),
        Set(&'static str, &'static str),
        Append(&'static str, &'static str),
    }

    fn edit() -> impl Strategy<Value = Edit> {
        let names = prop::sample::select(vec![
            "x-a",
            "x-b",
            "host",
            "connection",
            "te",
            "cookie",
            "x-new",
        ]);
        let values = prop::sample::select(vec!["1", "2", "trailers", "a=1; b=2"]);
        (0..3u8, names, values).prop_map(|(kind, name, value)| match kind {
            0 => Edit::Remove(name),
            1 => Edit::Set(name, value),
            _ => Edit::Append(name, value),
        })
    }

    proptest! {
        #[test]
        fn edits_through_an_overlay_read_and_write_as_the_same_edits_to_a_map(
            fields in prop::collection::vec(field(), 0..12),
            edits in prop::collection::vec(edit(), 0..8),
        ) {
            let head = head_of(&fields);
            let (lines, mut map) = read(&head);
            let view = lines.view(&head);
            let mut overlay = Overlay::default();
            for edit in &edits {
                match *edit {
                    Edit::Remove(n) => {
                        map.remove(n);
                        overlay.remove(&view, &name(n));
                    }
                    Edit::Set(n, v) => {
                        map.insert(name(n), value(v));
                        overlay.set(&view, name(n), value(v)).unwrap();
                    }
                    Edit::Append(n, v) => {
                        map.append(name(n), value(v));
                        overlay.append(name(n), value(v)).unwrap();
                    }
                }
            }
            let edited = overlay.edited(view);
            let mut names = every_name();
            names.push(name("x-new"));
            for n in &names {
                prop_assert_eq!(values(&edited, n), values(&map, n), "{}", n);
            }

            // Written out and read again, it is the same head, whatever it looks like.
            let out = written(&head, &overlay, &lines);
            let (_, again) = read(&out);
            for n in &names {
                prop_assert_eq!(values(&again, n), values(&map, n), "{}", n);
            }

            // Runs are as long as they can be.
            let pieces: Vec<Piece<'_>> = overlay.pieces(&lines, &[]).collect();
            for pair in pieces.windows(2) {
                if let [Piece::Copy(before), Piece::Copy(after)] = pair {
                    prop_assert_ne!(before.end, after.start);
                }
            }
        }

        #[test]
        fn a_view_reads_as_the_map_the_parser_builds(
            fields in prop::collection::vec(field(), 0..12),
            rule in prop::collection::vec(predicate(), 0..4),
        ) {
            let head = head_of(&fields);
            let (lines, map) = read(&head);
            let view = lines.view(&head);
            for name in every_name() {
                prop_assert_eq!(values(&view, &name), values(&map, &name), "{}", name);
            }
            for known in Known::ALL {
                prop_assert_eq!(lines.count(known), map.get_all(known.name()).iter().count());
                let through_slot: Vec<&[u8]> = view.known(known).collect();
                let through_name: Vec<&[u8]> = view.values(&known.name()).collect();
                prop_assert_eq!(through_slot, through_name);
            }
            let rule = HeaderPredicates::new(rule);
            prop_assert_eq!(rule.matches(&view), rule.matches(&map));
        }

        #[test]
        fn the_spans_of_the_lines_are_the_field_section_exactly(
            fields in prop::collection::vec(field(), 0..12),
        ) {
            let head = head_of(&fields);
            let (lines, _) = read(&head);
            let section = &head[b"GET / HTTP/1.1\r\n".len()..head.len() - 2];
            let joined: Vec<u8> =
                lines.spans().flat_map(|span| head[span].iter().copied()).collect();
            prop_assert_eq!(joined.as_slice(), section);
            prop_assert_eq!(lines.len(), fields.len());
            prop_assert_eq!(lines.is_empty(), fields.is_empty());
        }
    }
}
