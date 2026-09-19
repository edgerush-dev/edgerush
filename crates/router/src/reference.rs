//! The specification as code: slow, obvious implementations that the real ones are
//! compared with, by this crate's property tests and by the fuzz targets.
//!
//! Only built for tests and with the `reference` feature. Nothing here is shared with the
//! code it checks, and nothing here minds its cost.

/// What [`normalise_path`](crate::normalise_path) must return, as `Some`, or that it must
/// reject the path, as `None` — worked out in separate steps: make every segment's
/// percent-encoding canonical, then resolve the list of segments with a stack.
#[must_use]
pub fn normalise_path(path: &str) -> Option<String> {
    let segments: Vec<(String, bool)> = path
        .strip_prefix('/')?
        .split('/')
        .map(canonical_segment)
        .collect::<Option<_>>()?;

    let mut stack: Vec<&str> = Vec::new();
    let mut trailing_slash = false;
    for (segment, encoded_dot) in &segments {
        let name = segment.split_once(';').map_or(&**segment, |(name, _)| name);
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

/// The segment with canonical percent-encoding, and whether an encoded dot was decoded.
fn canonical_segment(segment: &str) -> Option<(String, bool)> {
    let mut canonical = String::new();
    let mut encoded_dot = false;
    let mut rest = segment.as_bytes();
    while let Some((&first, tail)) = rest.split_first() {
        let (byte, encoded) = if first == b'%' {
            let (digits, tail) = tail.split_at_checked(2)?;
            if !digits.iter().all(u8::is_ascii_hexdigit) {
                return None;
            }
            rest = tail;
            let digits = std::str::from_utf8(digits).ok()?;
            (u8::from_str_radix(digits, 16).ok()?, true)
        } else {
            rest = tail;
            (first, false)
        };
        if byte.is_ascii_control() || byte == b'\\' || (encoded && byte == b'/') {
            return None;
        }
        let unreserved = byte.is_ascii_alphanumeric() || b"-._~".contains(&byte);
        if unreserved || (!encoded && b"!$&'()*+,;=:@".contains(&byte)) {
            canonical.push(char::from(byte));
            encoded_dot |= encoded && byte == b'.';
        } else {
            canonical.push_str(&format!("%{byte:02X}"));
        }
    }
    Some((canonical, encoded_dot))
}
