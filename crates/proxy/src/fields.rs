//! The field lines of an HTTP/1 head as they arrived: where each lies in the head's bytes,
//! with the headers the gateway itself reads found once, while the head is read
//! ([14 §6](../../../docs/14-downstream-server.md)).
//!
//! Nothing is copied. A line is a handful of offsets into the head, which its owner keeps,
//! and a [`View`] puts the two together to read like a header map — the values of a name in
//! the order they came — through [`Fields`], so that route predicates see the same either
//! way. A name is found by a scan, which for a head's few lines costs less than building a
//! hash would; the ten names the gateway reads for itself ([`Known`]) have slots instead,
//! so asking for them again costs nothing. What a line spans, from its first byte to the
//! end of its line break, is kept too: an unchanged line is forwarded as the bytes it
//! arrived as.

// `pub` for the fuzz targets, which are a crate of their own; in an ordinary build none of
// this is API.
#![cfg_attr(not(feature = "fuzzing"), allow(unreachable_pub))]

use edgerush_router::Fields;
use http::HeaderName;
use http::header::{
    AUTHORIZATION, CONNECTION, CONTENT_LENGTH, COOKIE, EXPECT, HOST, PROXY_AUTHORIZATION, TE,
    TRAILER, TRANSFER_ENCODING,
};
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
}

impl Known {
    /// Every one of them, in the order of their slots.
    pub const ALL: [Self; 10] = [
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
        }
    }

    /// Which of them a field name as it arrived is, in whatever case.
    fn of_bytes(name: &[u8]) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|known| known.name().as_str().as_bytes().eq_ignore_ascii_case(name))
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
#[derive(Debug, Clone, Copy)]
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
#[derive(Debug, Clone, Copy, Default)]
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
}

/// The field lines of one head: where each is, and where the known headers are.
#[derive(Debug, Clone, Default)]
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
    /// ended by a line feed.
    pub fn new(head: &[u8], fields: &[httparse::Header<'_>]) -> Result<Self, FieldsError> {
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
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lines.is_empty()
    }

    /// The bytes each line spans in its head, from its first byte to the end of its line
    /// break, in the order the lines came.
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

impl Fields for View<'_> {
    fn values(&self, name: &HeaderName) -> impl Iterator<Item = &[u8]> {
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
            .filter(move |line| match known {
                Some(known) => line.known == Some(known),
                // A name that is not a known one is not on a line that has one.
                None => {
                    line.known.is_none()
                        && bytes(head, line.start..line.name_end).eq_ignore_ascii_case(wanted)
                }
            })
            .take(count)
            .map(move |line| bytes(head, line.value.0..line.value.1))
    }
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

    proptest! {
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
