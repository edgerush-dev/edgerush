//! Fuzzes the field lines of a head against the header map the same fields make: any bytes
//! at all, and whatever the parser finds in them.
//!
//! For every name a head has, and every name the gateway reads for itself, the lines must
//! give the values the map does, in the same order; the slots must agree with a scan; and
//! the lines' spans must follow one another with nothing between them, so that forwarding
//! them as they arrived leaves nothing out and puts nothing in.
//!
//! Seeded from the boundary corpus the proxy's tests check in, with new finds kept apart:
//! `cargo fuzz run h1_fields corpus/h1_fields ../crates/proxy/tests/corpus/h1_request`.

#![no_main]

use edgerush_proxy::fields::{FieldLines, Known};
use edgerush_router::Fields;
use http::{HeaderMap, HeaderName, HeaderValue};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| {
    let mut room = [httparse::EMPTY_HEADER; 32];
    let mut request = httparse::Request::new(&mut room);
    let Ok(httparse::Status::Complete(_)) = request.parse(bytes) else {
        return;
    };
    let lines = FieldLines::new(bytes, request.headers).expect("a parser's fields are lines");
    let view = lines.view(bytes);

    // The map the downstream parser builds, when every field is one it can hold.
    let mut map = HeaderMap::new();
    for field in request.headers.iter() {
        let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(field.name.as_bytes()),
            HeaderValue::from_bytes(field.value),
        ) else {
            return;
        };
        map.append(name, value);
    }

    let names = map
        .keys()
        .cloned()
        .chain(Known::ALL.into_iter().map(Known::name))
        .collect::<Vec<_>>();
    for name in &names {
        let from_lines: Vec<&[u8]> = view.values(name).collect();
        let from_map: Vec<&[u8]> = Fields::values(&map, name).collect();
        assert_eq!(from_lines, from_map, "{name}");
    }
    for known in Known::ALL {
        assert_eq!(lines.count(known), map.get_all(known.name()).iter().count());
        let through_slot: Vec<&[u8]> = view.known(known).collect();
        let through_name: Vec<&[u8]> = view.values(&known.name()).collect();
        assert_eq!(through_slot, through_name);
    }

    let mut spans = lines.spans();
    if let Some(first) = spans.next() {
        let mut end = first.end;
        for span in spans {
            assert_eq!(span.start, end, "a gap between lines");
            end = span.end;
        }
    }
});
