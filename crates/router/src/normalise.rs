//! Path normalisation: the one form of a request path that is both matched and forwarded,
//! so that the gateway and the upstream can never read a path differently.
//!
//! A path in normal form starts with `/`; has no empty, `.` or `..` segments (a trailing
//! slash is allowed); and consists of plain path characters and percent-encodings in
//! upper-case hex that stand for anything but an unreserved character, a control character,
//! a slash or a backslash. Every path has at most one normal form, and paths that could be
//! read in more than one way have none: they are rejected.

use std::borrow::Cow;

/// Brings a request path into normal form, or rejects it as ambiguous.
///
/// `path` is the path alone, without the query string. Dot segments are resolved, duplicate
/// slashes merged, percent-encoded unreserved characters decoded (`%61` is `a`), other
/// encodings upper-cased, and raw bytes that do not belong in a path encoded. A path that
/// is already in normal form — the usual case — is returned as it is, without allocating.
///
/// # Errors
///
/// Returns a [`NormaliseError`] for paths that are not absolute or are ambiguous; the
/// request should be answered with 400.
pub fn normalise_path(path: &str) -> Result<Cow<'_, str>, NormaliseError> {
    let segments = path.strip_prefix('/').ok_or(NormaliseError::NotAbsolute)?;
    if is_normal(segments) {
        return Ok(Cow::Borrowed(path));
    }

    let mut normal = String::with_capacity(path.len());
    let mut trailing_slash = false;
    for segment in segments.split('/') {
        let start = normal.len();
        normal.push('/');
        let encoded_dot = write_segment(segment, &mut normal)?;
        let written = normal.get(start + 1..).unwrap_or_default();

        let dots = dot_segment(written.as_bytes());
        if dots.is_some() && (encoded_dot || written.contains(';')) {
            return Err(NormaliseError::AmbiguousDotSegment);
        }
        // Empty and dot segments leave no trace of their own, except that one at the very
        // end stands for a trailing slash.
        trailing_slash = dots.is_some() || written.is_empty();
        if trailing_slash {
            normal.truncate(start);
        }
        if dots == Some(Dots::Parent) {
            let parent = normal.rfind('/').ok_or(NormaliseError::AboveRoot)?;
            normal.truncate(parent);
        }
    }
    if trailing_slash {
        normal.push('/');
    }
    Ok(Cow::Owned(normal))
}

/// Whether the path whose part after the first `/` is `segments` needs no work: the check
/// that keeps the usual case free of allocation. It may say no to a path that turns out to
/// be normal (any `%` does), never yes to one that is not.
fn is_normal(segments: &str) -> bool {
    let segments = segments.as_bytes();
    let mut segment_start = 0;
    for (at, &byte) in segments.iter().enumerate() {
        if byte == b'/' {
            let segment = segments.get(segment_start..at).unwrap_or_default();
            if segment.is_empty() || dot_segment(segment).is_some() {
                return false;
            }
            segment_start = at + 1;
        } else if !PLAIN[usize::from(byte)] {
            return false;
        }
    }
    // The last segment may be empty: a trailing slash.
    let last = segments.get(segment_start..).unwrap_or_default();
    dot_segment(last).is_none()
}

/// Appends `segment` with canonical percent-encoding. Says whether an encoded dot was
/// decoded on the way: a dot segment spelt that way is an attempt to hide it.
fn write_segment(segment: &str, normal: &mut String) -> Result<bool, NormaliseError> {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";

    let mut encoded_dot = false;
    let mut bytes = segment.bytes();
    while let Some(byte) = bytes.next() {
        let encoded = byte == b'%';
        let byte = if encoded {
            let mut digit = || bytes.next().and_then(hex_value);
            match (digit(), digit()) {
                (Some(high), Some(low)) => (high << 4) | low,
                _ => return Err(NormaliseError::InvalidPercentEncoding),
            }
        } else {
            byte
        };
        match byte {
            b'/' | b'\\' if encoded => return Err(NormaliseError::EncodedSeparator),
            b'\\' => return Err(NormaliseError::Backslash),
            0..=0x1F | 0x7F => return Err(NormaliseError::ControlCharacter),
            _ if is_unreserved(byte) || (!encoded && is_plain(byte)) => {
                encoded_dot |= encoded && byte == b'.';
                normal.push(char::from(byte));
            }
            _ => {
                // A nibble is below 16, so both positions exist.
                normal.push('%');
                normal.push(char::from(HEX[usize::from(byte >> 4)]));
                normal.push(char::from(HEX[usize::from(byte & 0xF)]));
            }
        }
    }
    Ok(encoded_dot)
}

fn hex_value(digit: u8) -> Option<u8> {
    char::from(digit)
        .to_digit(16)
        .and_then(|value| u8::try_from(value).ok())
}

/// [`is_plain`] for every byte, because the usual case is nothing but this check. A `u8`
/// always has a position in it.
const PLAIN: [bool; 256] = {
    let mut plain = [false; 256];
    let mut byte = u8::MIN;
    loop {
        plain[byte as usize] = is_plain(byte);
        if byte == u8::MAX {
            break plain;
        }
        byte += 1;
    }
};

/// The characters that never need encoding anywhere in a URI (RFC 3986 "unreserved").
const fn is_unreserved(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~')
}

/// The characters that may stand in a path segment as they are (RFC 3986 "pchar", less
/// `%`). The reserved ones among them are not the same as their encoded form, so neither
/// is turned into the other.
const fn is_plain(byte: u8) -> bool {
    is_unreserved(byte)
        || matches!(
            byte,
            b'!' | b'$'
                | b'&'
                | b'\''
                | b'('
                | b')'
                | b'*'
                | b'+'
                | b','
                | b';'
                | b'='
                | b':'
                | b'@'
        )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Dots {
    Current,
    Parent,
}

/// Whether the segment is `.` or `..` once path parameters (`;…`) are set aside, the way
/// servers that know path parameters read it.
fn dot_segment(segment: &[u8]) -> Option<Dots> {
    match segment {
        [b'.'] | [b'.', b';', ..] => Some(Dots::Current),
        [b'.', b'.'] | [b'.', b'.', b';', ..] => Some(Dots::Parent),
        _ => None,
    }
}

/// Why a request path has no normal form.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NormaliseError {
    /// The path does not start with `/` (or is empty).
    #[error("path does not start with `/`")]
    NotAbsolute,
    /// A `%` that is not followed by two hex digits.
    #[error("path contains a malformed percent-encoding")]
    InvalidPercentEncoding,
    /// An encoded slash or backslash (`%2F`, `%5C`): a separator to some servers, an
    /// ordinary character to others.
    #[error("path contains an encoded slash or backslash")]
    EncodedSeparator,
    /// A backslash, which some servers read as a slash.
    #[error("path contains a backslash")]
    Backslash,
    /// A control character, raw or encoded.
    #[error("path contains a control character")]
    ControlCharacter,
    /// A `.` or `..` segment spelt with an encoded dot or carrying path parameters
    /// (`%2e%2e`, `..;x`): a dot segment to some servers, an ordinary name to others.
    #[error("path contains a disguised `.` or `..` segment")]
    AmbiguousDotSegment,
    /// `..` segments that climb above the root.
    #[error("path climbs above the root")]
    AboveRoot,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn normalised(path: &str) -> String {
        normalise_path(path).unwrap().into_owned()
    }

    #[test]
    fn normal_paths_are_returned_as_they_are_without_allocating() {
        let paths = [
            "/",
            "/shop",
            "/shop/",
            "/shop/cart/42",
            "/.well-known/acme-challenge/x_Y-z~1",
            "/a.b/..c/d../...",
            "/odata/Orders(1)/$value",
            "/matrix;v=1/it's,a+b=c:d@e!&*",
        ];
        for path in paths {
            assert!(
                matches!(normalise_path(path), Ok(Cow::Borrowed(same)) if same == path),
                "{path:?}"
            );
        }
    }

    #[test]
    fn dot_segments_are_resolved() {
        assert_eq!(normalised("/a/./b"), "/a/b");
        assert_eq!(normalised("/a/../b"), "/b");
        assert_eq!(normalised("/a/b/../../c"), "/c");
        assert_eq!(normalised("/./a"), "/a");
        assert_eq!(normalised("/a/b/.."), "/a/");
        assert_eq!(normalised("/a/b/."), "/a/b/");
        assert_eq!(normalised("/a/.."), "/");
        assert_eq!(normalised("/."), "/");
    }

    #[test]
    fn duplicate_slashes_are_merged_and_a_trailing_slash_is_kept() {
        assert_eq!(normalised("//"), "/");
        assert_eq!(normalised("//a"), "/a");
        assert_eq!(normalised("/a//b///c"), "/a/b/c");
        assert_eq!(normalised("/a//"), "/a/");
        assert_eq!(normalised("/a/.//"), "/a/");
    }

    #[test]
    fn encoded_unreserved_characters_are_decoded() {
        assert_eq!(normalised("/%61dmin"), "/admin");
        assert_eq!(normalised("/%41%5A%7a%30%39%2D%5f%7E"), "/AZz09-_~");
        assert_eq!(normalised("/%2Ehidden/v1%2e2"), "/.hidden/v1.2");
    }

    #[test]
    fn other_encodings_are_kept_in_upper_case() {
        assert_eq!(normalised("/a%20b"), "/a%20b");
        assert_eq!(normalised("/caf%c3%a9"), "/caf%C3%A9");
        assert_eq!(normalised("/100%25"), "/100%25");
        // Reserved characters mean something else when encoded, so they stay as they came.
        assert_eq!(normalised("/a%3Bb;c/%40@/%2b+"), "/a%3Bb;c/%40@/%2B+");
    }

    #[test]
    fn raw_bytes_that_do_not_belong_in_a_path_are_encoded() {
        assert_eq!(normalised("/a b"), "/a%20b");
        assert_eq!(normalised("/café"), "/caf%C3%A9");
        assert_eq!(
            normalised("/<x>|{y}^`\"[z]"),
            "/%3Cx%3E%7C%7By%7D%5E%60%22%5Bz%5D"
        );
        assert_eq!(normalised("/a?b#c"), "/a%3Fb%23c");
    }

    #[test]
    fn every_spelling_of_a_path_has_the_same_normal_form() {
        for spelling in [
            "/caf%C3%A9/x",
            "/caf%c3%a9/x",
            "/café/x",
            "//caf%C3%A9/./y/../%78",
        ] {
            assert_eq!(normalised(spelling), "/caf%C3%A9/x", "{spelling:?}");
        }
    }

    #[test]
    fn ambiguous_paths_are_rejected_with_the_reason() {
        let cases = [
            ("", NormaliseError::NotAbsolute),
            ("*", NormaliseError::NotAbsolute),
            ("a/b", NormaliseError::NotAbsolute),
            ("%2Fa", NormaliseError::NotAbsolute),
            ("/a%", NormaliseError::InvalidPercentEncoding),
            ("/a%4", NormaliseError::InvalidPercentEncoding),
            ("/a%4g", NormaliseError::InvalidPercentEncoding),
            ("/a%+1", NormaliseError::InvalidPercentEncoding),
            ("/a%%41", NormaliseError::InvalidPercentEncoding),
            ("/a%é", NormaliseError::InvalidPercentEncoding),
            ("/a%2Fb", NormaliseError::EncodedSeparator),
            ("/a%2fb", NormaliseError::EncodedSeparator),
            ("/a%5Cb", NormaliseError::EncodedSeparator),
            ("/a%5c..%5cb", NormaliseError::EncodedSeparator),
            ("/a\\b", NormaliseError::Backslash),
            ("/a/..\\b", NormaliseError::Backslash),
            ("/a\0", NormaliseError::ControlCharacter),
            ("/a\r\nb", NormaliseError::ControlCharacter),
            ("/a\u{7f}", NormaliseError::ControlCharacter),
            ("/a%00", NormaliseError::ControlCharacter),
            ("/a%0d%0Ab", NormaliseError::ControlCharacter),
            ("/a%7F", NormaliseError::ControlCharacter),
            ("/a/%2e%2e/b", NormaliseError::AmbiguousDotSegment),
            ("/a/.%2E/b", NormaliseError::AmbiguousDotSegment),
            ("/a/%2e/b", NormaliseError::AmbiguousDotSegment),
            ("/a/..;/b", NormaliseError::AmbiguousDotSegment),
            ("/a/..;x=y/b", NormaliseError::AmbiguousDotSegment),
            ("/a/.;x", NormaliseError::AmbiguousDotSegment),
            ("/a/%2e%2e;x/b", NormaliseError::AmbiguousDotSegment),
            ("/..", NormaliseError::AboveRoot),
            ("/../a", NormaliseError::AboveRoot),
            ("/a/../../b", NormaliseError::AboveRoot),
            ("/a/../..", NormaliseError::AboveRoot),
        ];
        for (path, reason) in cases {
            assert_eq!(normalise_path(path), Err(reason), "{path:?}");
        }
    }

    #[test]
    fn errors_describe_themselves() {
        assert_eq!(
            NormaliseError::AboveRoot.to_string(),
            "path climbs above the root"
        );
    }

    /// The definition of normal form from the module documentation, checked directly.
    fn is_in_normal_form(path: &str) -> bool {
        let Some(rest) = path.strip_prefix('/') else {
            return false;
        };
        let segments: Vec<&str> = rest.split('/').collect();
        let (last, others) = segments.split_last().unwrap();
        let well_formed = |segment: &str| {
            let name = segment.split(';').next().unwrap();
            let mut rest = segment.as_bytes();
            let mut encodings_are_canonical = true;
            while let Some((&byte, tail)) = rest.split_first() {
                rest = tail;
                if byte == b'%' {
                    let (digits, tail) = rest.split_at_checked(2).unwrap_or((b"", rest));
                    rest = tail;
                    let digits = std::str::from_utf8(digits).unwrap_or("");
                    let canonical = digits.len() == 2
                        && digits
                            .bytes()
                            .all(|digit| digit.is_ascii_digit() || (b'A'..=b'F').contains(&digit));
                    let value = u8::from_str_radix(digits, 16).unwrap_or(0);
                    encodings_are_canonical &= canonical
                        && !is_unreserved(value)
                        && !value.is_ascii_control()
                        && value != b'/'
                        && value != b'\\';
                } else {
                    encodings_are_canonical &= is_plain(byte);
                }
            }
            name != "." && name != ".." && encodings_are_canonical
        };
        others
            .iter()
            .all(|segment| !segment.is_empty() && well_formed(segment))
            && well_formed(last)
    }

    /// The specification, written in separate steps with no regard for cost: decode every
    /// segment into a list, then resolve the list with a stack.
    fn reference(path: &str) -> Option<String> {
        let segments: Vec<(String, bool)> = path
            .strip_prefix('/')?
            .split('/')
            .map(reference_segment)
            .collect::<Option<_>>()?;

        let mut stack: Vec<&str> = Vec::new();
        let mut trailing_slash = false;
        for (segment, encoded_dot) in &segments {
            let name = segment.split(';').next().unwrap();
            let is_dots = name == "." || name == "..";
            if is_dots && (*encoded_dot || name != segment) {
                return None;
            }
            trailing_slash = is_dots || segment.is_empty();
            if name == ".." {
                stack.pop()?;
            } else if !trailing_slash {
                stack.push(segment);
            }
        }
        let mut normal: String = stack.iter().flat_map(|segment| ["/", segment]).collect();
        if trailing_slash || normal.is_empty() {
            normal.push('/');
        }
        Some(normal)
    }

    fn reference_segment(segment: &str) -> Option<(String, bool)> {
        let bytes = segment.as_bytes();
        let mut canonical = String::new();
        let mut encoded_dot = false;
        let mut at = 0;
        while at < bytes.len() {
            let (byte, encoded) = if bytes[at] == b'%' {
                let digits = bytes.get(at + 1..at + 3)?;
                if !digits.iter().all(u8::is_ascii_hexdigit) {
                    return None;
                }
                at += 3;
                let digits = std::str::from_utf8(digits).unwrap();
                (u8::from_str_radix(digits, 16).unwrap(), true)
            } else {
                at += 1;
                (bytes[at - 1], false)
            };
            if byte.is_ascii_control() || byte == b'\\' || (encoded && byte == b'/') {
                return None;
            }
            let keep_raw = is_unreserved(byte) || (!encoded && b"!$&'()*+,;=:@".contains(&byte));
            if keep_raw {
                canonical.push(char::from(byte));
                encoded_dot |= encoded && byte == b'.';
            } else {
                canonical.push_str(&format!("%{byte:02X}"));
            }
        }
        Some((canonical, encoded_dot))
    }

    /// Paths over an alphabet chosen to hit every rule often: separators, dots, the
    /// encodings of dots, slashes, backslashes, letters and controls, path parameters, and
    /// raw bytes that need encoding.
    fn nasty_path() -> impl Strategy<Value = String> {
        let piece = prop_oneof![
            4 => Just("/"),
            3 => Just("."),
            2 => Just("a"),
            1 => Just("B"),
            1 => Just("%2e"),
            1 => Just("%2E"),
            1 => Just("%2f"),
            1 => Just("%5C"),
            1 => Just("%61"),
            1 => Just("%20"),
            1 => Just("%c3"),
            1 => Just("%0a"),
            1 => Just("%"),
            1 => Just("%4"),
            1 => Just(";"),
            1 => Just(";x"),
            1 => Just("\\"),
            1 => Just(" "),
            1 => Just("é"),
            1 => Just("\u{1}"),
            1 => Just("+"),
        ];
        (any::<bool>(), prop::collection::vec(piece, 0..10)).prop_map(|(absolute, pieces)| {
            let path = pieces.concat();
            if absolute { format!("/{path}") } else { path }
        })
    }

    proptest! {
        #[test]
        fn normalising_agrees_with_the_step_by_step_reference(path in nasty_path()) {
            let normal = normalise_path(&path).ok().map(Cow::into_owned);
            prop_assert_eq!(normal, reference(&path));
        }

        #[test]
        fn output_is_in_normal_form_and_normalising_it_again_changes_nothing(
            path in nasty_path()
        ) {
            if let Ok(normal) = normalise_path(&path) {
                prop_assert!(is_in_normal_form(&normal), "{normal:?}");
                let again = normalise_path(&normal);
                prop_assert_eq!(again.as_deref(), Ok(&*normal));
            }
        }

        #[test]
        fn paths_that_skip_the_work_are_in_normal_form(path in nasty_path()) {
            if let Ok(Cow::Borrowed(same)) = normalise_path(&path) {
                prop_assert!(is_in_normal_form(same), "{same:?}");
            }
        }

        #[test]
        fn arbitrary_text_never_panics(path in any::<String>()) {
            let absolute = format!("/{path}");
            let _ = (normalise_path(&path), normalise_path(&absolute));
        }
    }
}
