//! Access-log records, and the line of JSON each is written as
//! ([08 §2](../../../docs/08-observability.md), [21 §3](../../../docs/21-access-logs.md)).
//!
//! A [`Record`] borrows what the request path knew; [`Record::write`] appends it as one
//! line. Written by hand, not through `fmt`: a record is made for every request of a
//! listener that logs, and the formatting machinery costs several times what the bytes do.
//! Strings are escaped as RFC 8259 asks — `"`, `\` and the control characters — and a
//! `&str` is UTF-8 already, so nothing else needs escaping and nothing is lost.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

/// What a record is of.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Kind {
    /// An HTTP request, made at the end of its answer.
    #[default]
    Request,
    /// A WebSocket, once its handshake is answered.
    WebSocketOpen,
    /// A WebSocket, once it has ended.
    WebSocketClose,
    /// A `tcp` or `tls` listener's connection, once it has ended.
    Connection,
}

impl Kind {
    fn name(self) -> &'static [u8] {
        match self {
            Self::Request => b"request",
            Self::WebSocketOpen => b"websocket_open",
            Self::WebSocketClose => b"websocket_close",
            Self::Connection => b"connection",
        }
    }
}

/// The HTTP version a request came in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Protocol {
    /// HTTP/1.0.
    Http10,
    /// HTTP/1.1.
    Http11,
    /// HTTP/2.
    Http2,
    /// HTTP/3.
    Http3,
}

impl Protocol {
    fn name(self) -> &'static [u8] {
        match self {
            Self::Http10 => b"1.0",
            Self::Http11 => b"1.1",
            Self::Http2 => b"2",
            Self::Http3 => b"3",
        }
    }
}

/// One record: a request, a WebSocket's opening or end, or a passthrough connection. What
/// does not apply to it is `None` and left out of its line, except an HTTP record's status,
/// which is `null` when the client got none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Record<'a> {
    /// When the request's head arrived, or the connection was accepted, in milliseconds
    /// since the Unix epoch.
    pub time_ms: u64,
    /// What it is of.
    pub kind: Kind,
    /// The request's ID, if its listener gives one.
    pub id: Option<&'a str>,
    /// The listener's name.
    pub listener: &'a str,
    /// The client, as the gateway works it out.
    pub client: Option<IpAddr>,
    /// The address that connected.
    pub peer: Option<SocketAddr>,
    /// The HTTP version; none for a passthrough connection.
    pub protocol: Option<Protocol>,
    /// The request's method, as received.
    pub method: Option<&'a str>,
    /// The request's host, as received.
    pub host: Option<&'a str>,
    /// The request's path, as received, its query included.
    pub path: Option<&'a str>,
    /// The status the client got.
    pub status: Option<u16>,
    /// Why the gateway answered itself, the answer failed or the tunnel ended.
    pub reason: Option<&'a str>,
    /// The route that matched.
    pub route: Option<&'a str>,
    /// The rule's position in its route.
    pub rule: Option<usize>,
    /// The upstream the request or connection went to.
    pub upstream: Option<&'a str>,
    /// The last try's endpoint.
    pub endpoint: Option<SocketAddr>,
    /// How many tries there were.
    pub tries: Option<u32>,
    /// A gRPC call's status.
    pub grpc_status: Option<u32>,
    /// Body bytes received, or what a tunnel carried from the client.
    pub bytes_in: Option<u64>,
    /// Body bytes sent, or what a tunnel carried to the client.
    pub bytes_out: Option<u64>,
    /// From the head arriving to the answer's last byte going, or the tunnel's end, in
    /// microseconds.
    pub duration_us: Option<u64>,
    /// From the head arriving to the upstream's answer head, in microseconds.
    pub upstream_us: Option<u64>,
}

impl Record<'_> {
    /// Appends the record to `out` as one line of JSON, its newline included.
    pub fn write(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(b"{\"time\":\"");
        time(out, self.time_ms);
        out.extend_from_slice(b"\",\"kind\":\"");
        out.extend_from_slice(self.kind.name());
        out.push(b'"');
        if let Some(id) = self.id {
            out.extend_from_slice(b",\"id\":");
            string(out, id);
        }
        out.extend_from_slice(b",\"listener\":");
        string(out, self.listener);
        if let Some(client) = self.client {
            out.extend_from_slice(b",\"client\":\"");
            ip(out, client);
            out.push(b'"');
        }
        if let Some(peer) = self.peer {
            out.extend_from_slice(b",\"peer\":\"");
            socket(out, peer);
            out.push(b'"');
        }
        if let Some(protocol) = self.protocol {
            out.extend_from_slice(b",\"protocol\":\"");
            out.extend_from_slice(protocol.name());
            out.push(b'"');
        }
        optional_string(out, b",\"method\":", self.method);
        optional_string(out, b",\"host\":", self.host);
        optional_string(out, b",\"path\":", self.path);
        match (self.status, self.kind) {
            (Some(status), _) => {
                out.extend_from_slice(b",\"status\":");
                decimal(out, u64::from(status));
            }
            (None, Kind::Connection) => {}
            (None, _) => out.extend_from_slice(b",\"status\":null"),
        }
        optional_string(out, b",\"reason\":", self.reason);
        optional_string(out, b",\"route\":", self.route);
        if let Some(rule) = self.rule {
            out.extend_from_slice(b",\"rule\":");
            decimal(out, u64::try_from(rule).unwrap_or(u64::MAX));
        }
        optional_string(out, b",\"upstream\":", self.upstream);
        if let Some(endpoint) = self.endpoint {
            out.extend_from_slice(b",\"endpoint\":\"");
            socket(out, endpoint);
            out.push(b'"');
        }
        optional_number(out, b",\"tries\":", self.tries.map(u64::from));
        optional_number(out, b",\"grpc_status\":", self.grpc_status.map(u64::from));
        optional_number(out, b",\"bytes_in\":", self.bytes_in);
        optional_number(out, b",\"bytes_out\":", self.bytes_out);
        if let Some(us) = self.duration_us {
            out.extend_from_slice(b",\"duration_ms\":");
            milliseconds(out, us);
        }
        if let Some(us) = self.upstream_us {
            out.extend_from_slice(b",\"upstream_ms\":");
            milliseconds(out, us);
        }
        out.extend_from_slice(b"}\n");
    }
}

fn optional_string(out: &mut Vec<u8>, key: &[u8], value: Option<&str>) {
    if let Some(value) = value {
        out.extend_from_slice(key);
        string(out, value);
    }
}

fn optional_number(out: &mut Vec<u8>, key: &[u8], value: Option<u64>) {
    if let Some(value) = value {
        out.extend_from_slice(key);
        decimal(out, value);
    }
}

/// Appends `value` as a JSON string (RFC 8259 §7): quoted, with `"`, `\` and the control
/// characters escaped, and everything else as it is.
fn string(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    out.push(b'"');
    // Nearly every string needs nothing escaped, and is then copied in one go.
    if plain(bytes) {
        out.extend_from_slice(bytes);
    } else {
        escaped(out, bytes);
    }
    out.push(b'"');
}

/// Whether none of `bytes` needs escaping: none is a control character, `"` or `\`. Looked
/// at eight at a time, in a `u64`, by the bit tricks of "Bit Twiddling Hacks" (Sean Eron
/// Anderson, public domain: "Determine if a word has a byte less than n", and "… equal to
/// n" as a zero byte after an exclusive or). Each finds every byte it is for and finds
/// nothing in a word that has none; it may also mark bytes beside one it found, which a
/// yes or no does not mind.
fn plain(bytes: &[u8]) -> bool {
    const ONES: u64 = 0x0101_0101_0101_0101;
    const HIGHS: u64 = 0x8080_8080_8080_8080;
    let below = |word: u64, n: u8| word.wrapping_sub(ONES * u64::from(n)) & !word & HIGHS;
    let (words, rest) = bytes.as_chunks::<8>();
    let found = words.iter().fold(0, |found, &word| {
        let word = u64::from_le_bytes(word);
        found
            | below(word, 0x20)
            | below(word ^ (ONES * u64::from(b'"')), 1)
            | below(word ^ (ONES * u64::from(b'\\')), 1)
    });
    found == 0
        && rest
            .iter()
            .all(|&byte| byte >= 0x20 && byte != b'"' && byte != b'\\')
}

/// Appends `bytes` with what RFC 8259 asks escaped, the short escapes where it has them, and
/// runs of bytes that need nothing copied whole.
fn escaped(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut from = 0;
    for (at, &byte) in bytes.iter().enumerate() {
        let short: &[u8] = match byte {
            b'"' => b"\\\"",
            b'\\' => b"\\\\",
            b'\n' => b"\\n",
            b'\r' => b"\\r",
            b'\t' => b"\\t",
            0x00..=0x1f => b"",
            _ => continue,
        };
        out.extend_from_slice(&bytes[from..at]);
        if short.is_empty() {
            out.extend_from_slice(&[
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[usize::from(byte >> 4)],
                HEX[usize::from(byte & 0x0f)],
            ]);
        } else {
            out.extend_from_slice(short);
        }
        from = at + 1;
    }
    out.extend_from_slice(&bytes[from..]);
}

/// Appends `ms` since the Unix epoch in RFC 3339, in UTC, to the millisecond:
/// `2026-10-04T09:12:33.123Z`.
fn time(out: &mut Vec<u8>, ms: u64) {
    let seconds = ms / 1000;
    let (year, month, day) = civil(seconds / 86_400);
    let of_day = seconds % 86_400;
    // Four digits up to the year 9999, the last RFC 3339 has; after it, whatever it is.
    if year < 10_000 {
        two(out, year / 100);
        two(out, year);
    } else {
        decimal(out, year);
    }
    out.push(b'-');
    two(out, month);
    out.push(b'-');
    two(out, day);
    out.push(b'T');
    two(out, of_day / 3600);
    out.push(b':');
    two(out, of_day / 60 % 60);
    out.push(b':');
    two(out, of_day % 60);
    out.push(b'.');
    three(out, ms);
    out.push(b'Z');
}

/// The year, month and day of the `days`th day after 1970-01-01, by Howard Hinnant's
/// `civil_from_days` ("chrono-Compatible Low-Level Date Algorithms", public domain), for
/// days on or after the epoch: years of 400 years, of which every one has the same days.
fn civil(days: u64) -> (u64, u64, u64) {
    // From 0000-03-01, so that a leap day is the last day of its year.
    let shifted = days + 719_468;
    let era = shifted / 146_097;
    let of_era = shifted % 146_097;
    let year_of_era = (of_era - of_era / 1460 + of_era / 36_524 - of_era / 146_096) / 365;
    let of_year = of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    // Months from March, of 153 days to every five.
    let month_from_march = (5 * of_year + 2) / 153;
    let day = of_year - (153 * month_from_march + 2) / 5 + 1;
    let month = if month_from_march < 10 {
        month_from_march + 3
    } else {
        month_from_march - 9
    };
    let year = year_of_era + era * 400 + u64::from(month <= 2);
    (year, month, day)
}

/// Appends `us` microseconds as milliseconds with three decimals, by integer division.
fn milliseconds(out: &mut Vec<u8>, us: u64) {
    decimal(out, us / 1000);
    out.push(b'.');
    three(out, us);
}

/// The two digits of every number below a hundred, in order: `00`, `01`, … `99`.
const PAIRS: [u8; 200] = {
    let mut pairs = [0; 200];
    let mut n = 0;
    while n < 100 {
        pairs[2 * n] = b'0' + (n / 10) as u8;
        pairs[2 * n + 1] = b'0' + (n % 10) as u8;
        n += 1;
    }
    pairs
};

/// The last decimal digit of `value`.
fn digit(value: u64) -> u8 {
    // A remainder of ten is below ten, so it fits a byte.
    b'0' + u8::try_from(value % 10).unwrap_or(0)
}

/// The last two decimal digits of `value`.
fn pair(value: u64) -> [u8; 2] {
    // A remainder of a hundred is below a hundred, so it fits.
    let at = usize::try_from(value % 100).unwrap_or(0) * 2;
    [PAIRS[at], PAIRS[at + 1]]
}

/// Appends the last two decimal digits of `value`, a nought first where it has one.
fn two(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&pair(value));
}

/// Appends the last three decimal digits of `value`, noughts first where it has fewer.
fn three(out: &mut Vec<u8>, value: u64) {
    let [tens, ones] = pair(value);
    out.extend_from_slice(&[digit(value / 100), tens, ones]);
}

/// Appends `value` in decimal. Most numbers in a record are small, and are written a digit
/// or two at a time without a copy; a larger one is made on the stack two digits at a time
/// and copied once.
fn decimal(out: &mut Vec<u8>, value: u64) {
    if value < 10 {
        out.push(digit(value));
    } else if value < 100 {
        two(out, value);
    } else if value < 1000 {
        three(out, value);
    } else if value < 10_000 {
        two(out, value / 100);
        two(out, value);
    } else if value < 100_000 {
        out.push(digit(value / 10_000));
        two(out, value / 100);
        two(out, value);
    } else if value < 1_000_000 {
        three(out, value / 1000);
        three(out, value);
    } else {
        // The largest u64 is twenty digits.
        let mut digits = [0u8; 20];
        let mut at = digits.len();
        let mut rest = value;
        while rest >= 10 {
            at -= 2;
            [digits[at], digits[at + 1]] = pair(rest);
            rest /= 100;
        }
        if rest > 0 {
            at -= 1;
            digits[at] = digit(rest);
        }
        out.extend_from_slice(&digits[at..]);
    }
}

/// Appends an address as RFC 5952 writes IPv6, and as the standard library writes both.
fn ip(out: &mut Vec<u8>, ip: IpAddr) {
    match ip {
        IpAddr::V4(v4) => ipv4(out, v4),
        IpAddr::V6(v6) => ipv6(out, v6),
    }
}

fn ipv4(out: &mut Vec<u8>, ip: Ipv4Addr) {
    let [a, b, c, d] = ip.octets();
    decimal(out, u64::from(a));
    out.push(b'.');
    decimal(out, u64::from(b));
    out.push(b'.');
    decimal(out, u64::from(c));
    out.push(b'.');
    decimal(out, u64::from(d));
}

/// RFC 5952: lower case, no leading zeros, the first of the longest runs of two or more
/// zero groups as `::` (§4), and an IPv4-mapped address in mixed notation (§5).
fn ipv6(out: &mut Vec<u8>, ip: Ipv6Addr) {
    if let Some(v4) = ip.to_ipv4_mapped() {
        out.extend_from_slice(b"::ffff:");
        ipv4(out, v4);
        return;
    }
    let groups = ip.segments();
    let (mut longest_at, mut longest, mut run_at, mut run) = (0, 0, 0, 0);
    for (at, &group) in groups.iter().enumerate() {
        if group == 0 {
            if run == 0 {
                run_at = at;
            }
            run += 1;
            if run > longest {
                (longest_at, longest) = (run_at, run);
            }
        } else {
            run = 0;
        }
    }
    if longest > 1 {
        hex_groups(out, &groups[..longest_at]);
        out.extend_from_slice(b"::");
        hex_groups(out, &groups[longest_at + longest..]);
    } else {
        hex_groups(out, &groups);
    }
}

/// Appends `groups` in lower-case hexadecimal without leading zeros, `:` between them.
fn hex_groups(out: &mut Vec<u8>, groups: &[u16]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for (at, &group) in groups.iter().enumerate() {
        if at > 0 {
            out.push(b':');
        }
        let mut started = false;
        for shift in [12, 8, 4, 0] {
            let nibble = usize::from((group >> shift) & 0xf);
            if started || nibble != 0 || shift == 0 {
                out.push(HEX[nibble]);
                started = true;
            }
        }
    }
}

/// Appends an address and port: `192.0.2.1:80`, `[2001:db8::1]:80`.
fn socket(out: &mut Vec<u8>, address: SocketAddr) {
    match address {
        SocketAddr::V4(v4) => ipv4(out, *v4.ip()),
        SocketAddr::V6(v6) => {
            out.push(b'[');
            ipv6(out, *v6.ip());
            out.push(b']');
        }
    }
    out.push(b':');
    decimal(out, u64::from(address.port()));
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::Value;

    fn line(record: &Record<'_>) -> String {
        let mut out = Vec::new();
        record.write(&mut out);
        String::from_utf8(out).unwrap()
    }

    fn proxied() -> Record<'static> {
        Record {
            time_ms: 1_759_569_153_123,
            kind: Kind::Request,
            id: Some("0199af5e-3a1b-7c2d-8e4f-5a6b7c8d9e0f"),
            listener: "web",
            client: Some(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 7))),
            peer: Some("10.0.0.5:51234".parse().unwrap()),
            protocol: Some(Protocol::Http2),
            method: Some("GET"),
            host: Some("example.com"),
            path: Some("/v1/a?b=1"),
            status: Some(200),
            reason: None,
            route: Some("api"),
            rule: Some(0),
            upstream: Some("api"),
            endpoint: Some("10.1.2.3:8080".parse().unwrap()),
            tries: Some(1),
            grpc_status: None,
            bytes_in: Some(0),
            bytes_out: Some(512),
            duration_us: Some(1234),
            upstream_us: Some(87),
        }
    }

    /// Every key in its order, as 21 §3 lists them, and only those that apply.
    #[test]
    fn a_proxied_request_is_one_line_with_its_keys_in_order() {
        assert_eq!(
            line(&proxied()),
            "{\"time\":\"2025-10-04T09:12:33.123Z\",\"kind\":\"request\",\
             \"id\":\"0199af5e-3a1b-7c2d-8e4f-5a6b7c8d9e0f\",\"listener\":\"web\",\
             \"client\":\"203.0.113.7\",\"peer\":\"10.0.0.5:51234\",\"protocol\":\"2\",\
             \"method\":\"GET\",\"host\":\"example.com\",\"path\":\"/v1/a?b=1\",\"status\":200,\
             \"route\":\"api\",\"rule\":0,\"upstream\":\"api\",\"endpoint\":\"10.1.2.3:8080\",\
             \"tries\":1,\"bytes_in\":0,\"bytes_out\":512,\"duration_ms\":1.234,\
             \"upstream_ms\":0.087}\n"
        );
    }

    /// A client that got no answer has a `null` status on an HTTP record; a passthrough
    /// connection has no status at all.
    #[test]
    fn a_status_never_given_is_null_and_a_connection_has_none() {
        let gone = Record {
            status: None,
            reason: Some("client_closed"),
            ..proxied()
        };
        assert!(line(&gone).contains(",\"status\":null,\"reason\":\"client_closed\","));
        let tunnel = Record {
            kind: Kind::Connection,
            listener: "db",
            protocol: None,
            method: None,
            host: None,
            path: None,
            status: None,
            reason: Some("closed"),
            ..proxied()
        };
        let written = line(&tunnel);
        assert!(written.contains("\"kind\":\"connection\""), "{written}");
        assert!(!written.contains("status"), "{written}");
        assert!(!written.contains("protocol"), "{written}");
    }

    #[test]
    fn the_kinds_and_versions_have_their_names() {
        for (kind, name) in [
            (Kind::Request, "request"),
            (Kind::WebSocketOpen, "websocket_open"),
            (Kind::WebSocketClose, "websocket_close"),
            (Kind::Connection, "connection"),
        ] {
            let written = line(&Record { kind, ..proxied() });
            assert!(
                written.contains(&format!("\"kind\":\"{name}\"")),
                "{written}"
            );
        }
        for (protocol, name) in [
            (Protocol::Http10, "1.0"),
            (Protocol::Http11, "1.1"),
            (Protocol::Http2, "2"),
            (Protocol::Http3, "3"),
        ] {
            let written = line(&Record {
                protocol: Some(protocol),
                ..proxied()
            });
            assert!(
                written.contains(&format!("\"protocol\":\"{name}\"")),
                "{written}"
            );
        }
    }

    /// RFC 8259's escapes, the short ones where it has them; nothing else is touched,
    /// whatever script it is in.
    #[test]
    fn strings_are_escaped_as_json_asks_and_no_more() {
        let written = line(&Record {
            path: Some("/a\"b\\c\nd\re\tf\u{1}g\u{1f}h\u{7f}é✓"),
            ..proxied()
        });
        assert!(
            written.contains(r#""path":"/a\"b\\c\nd\re\tf\u0001g\u001fh"#),
            "{written}"
        );
        assert!(written.contains("\u{7f}é✓\""), "{written}");
    }

    #[test]
    fn addresses_are_written_in_their_one_form() {
        let written = line(&Record {
            client: Some(IpAddr::V6(Ipv6Addr::new(0x2001, 0xdb8, 0, 0, 0, 0, 0, 1))),
            peer: Some("[2001:db8::2]:443".parse().unwrap()),
            endpoint: Some("[::ffff:192.0.2.1]:80".parse().unwrap()),
            ..proxied()
        });
        assert!(written.contains("\"client\":\"2001:db8::1\""), "{written}");
        assert!(
            written.contains("\"peer\":\"[2001:db8::2]:443\""),
            "{written}"
        );
        assert!(
            written.contains("\"endpoint\":\"[::ffff:192.0.2.1]:80\""),
            "{written}"
        );
    }

    #[test]
    fn durations_are_milliseconds_with_three_decimals() {
        for (us, ms) in [
            (0, "0.000"),
            (7, "0.007"),
            (87, "0.087"),
            (1000, "1.000"),
            (1_234_567, "1234.567"),
        ] {
            let written = line(&Record {
                duration_us: Some(us),
                ..proxied()
            });
            assert!(
                written.contains(&format!("\"duration_ms\":{ms},")),
                "{written}"
            );
        }
    }

    #[test]
    fn times_are_rfc_3339_in_utc() {
        for (ms, time) in [
            (0, "1970-01-01T00:00:00.000Z"),
            (951_782_400_000, "2000-02-29T00:00:00.000Z"),
            (951_868_799_999, "2000-02-29T23:59:59.999Z"),
            (4_107_542_400_000, "2100-03-01T00:00:00.000Z"),
        ] {
            assert!(
                line(&Record {
                    time_ms: ms,
                    ..proxied()
                })
                .starts_with(&format!("{{\"time\":\"{time}\"")),
                "{ms}"
            );
        }
    }

    /// The date, day by day from the epoch, as the Gregorian calendar has it.
    fn walked(days: u64) -> (u64, u64, u64) {
        let leap = |year: u64| {
            year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
        };
        let (mut year, mut left) = (1970, days);
        loop {
            let length = if leap(year) { 366 } else { 365 };
            if left < length {
                break;
            }
            left -= length;
            year += 1;
        }
        let lengths = [
            31,
            if leap(year) { 29 } else { 28 },
            31,
            30,
            31,
            30,
            31,
            31,
            30,
            31,
            30,
            31,
        ];
        let mut month = 1;
        for length in lengths {
            if left < length {
                break;
            }
            left -= length;
            month += 1;
        }
        (year, month, left + 1)
    }

    proptest! {
        #[test]
        fn every_day_is_the_calendar_s(days in 0u64..200_000) {
            prop_assert_eq!(civil(days), walked(days));
        }

        /// Every part of a time where it belongs, up to the last year RFC 3339 has.
        #[test]
        fn every_time_is_written_part_by_part(ms in 0u64..253_402_300_800_000) {
            let mut out = Vec::new();
            time(&mut out, ms);
            let (year, month, day) = walked(ms / 86_400_000);
            let of_day = ms / 1000 % 86_400;
            prop_assert_eq!(
                String::from_utf8(out).unwrap(),
                format!(
                    "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:03}Z",
                    of_day / 3600,
                    of_day / 60 % 60,
                    of_day % 60,
                    ms % 1000
                )
            );
        }

        /// Eight bytes at a time says what one at a time says, whatever the bytes and
        /// wherever in a word the first one that needs escaping falls.
        #[test]
        fn a_string_is_plain_exactly_when_no_byte_needs_escaping(
            bytes in proptest::collection::vec(
                prop_oneof![Just(b'"'), Just(b'\\'), Just(0x1f), Just(0x20), Just(0x7f), any::<u8>()],
                0..40,
            ),
        ) {
            prop_assert_eq!(
                plain(&bytes),
                bytes.iter().all(|&byte| byte >= 0x20 && byte != b'"' && byte != b'\\')
            );
        }

        /// Each of the ways a number is written, small and large, against the standard
        /// library's.
        #[test]
        fn numbers_are_written_as_the_standard_library_writes_them(
            value in any::<u64>(),
            small in 0u64..100_000,
        ) {
            for value in [value, small * 10, small, small / 10, small / 100, small / 1000] {
                let mut out = Vec::new();
                decimal(&mut out, value);
                prop_assert_eq!(String::from_utf8(out).unwrap(), value.to_string());
            }
        }

        /// Addresses against the standard library's own RFC 5952, with IPv6 groups that
        /// are mostly zero, so that every way of compressing them comes up.
        #[test]
        fn addresses_are_written_as_the_standard_library_writes_them(
            address in any::<IpAddr>(),
            groups in proptest::array::uniform8(prop_oneof![Just(0u16), Just(1u16), any::<u16>()]),
        ) {
            for address in [address, IpAddr::V6(Ipv6Addr::from(groups))] {
                let mut out = Vec::new();
                ip(&mut out, address);
                prop_assert_eq!(String::from_utf8(out).unwrap(), address.to_string());
            }
        }

        /// Whatever a record holds, its line is one line of JSON that says what it holds.
        #[test]
        fn every_record_reads_back_as_what_it_was_made_from(
            time_ms in 0u64..253_402_300_800_000,
            id in proptest::option::of(".*"),
            listener in ".*",
            client in proptest::option::of(any::<IpAddr>()),
            peer in proptest::option::of(any::<SocketAddr>()),
            method in proptest::option::of(".*"),
            path in proptest::option::of("\\PC*|.*"),
            status in proptest::option::of(any::<u16>()),
            rule in proptest::option::of(any::<usize>()),
            tries in proptest::option::of(any::<u32>()),
            bytes_out in proptest::option::of(any::<u64>()),
            duration_us in proptest::option::of(0u64..1 << 50),
        ) {
            let record = Record {
                time_ms,
                id: id.as_deref(),
                listener: &listener,
                client,
                peer,
                method: method.as_deref(),
                path: path.as_deref(),
                status,
                rule,
                tries,
                bytes_out,
                duration_us,
                ..Record::default()
            };
            let written = line(&record);
            prop_assert!(written.ends_with('\n'));
            prop_assert_eq!(written.matches('\n').count(), 1);
            let read: Value = serde_json::from_str(&written).unwrap();
            prop_assert_eq!(read["listener"].as_str(), Some(listener.as_str()));
            prop_assert_eq!(read.get("id").and_then(Value::as_str), id.as_deref());
            prop_assert_eq!(read.get("method").and_then(Value::as_str), method.as_deref());
            prop_assert_eq!(read.get("path").and_then(Value::as_str), path.as_deref());
            prop_assert_eq!(
                read.get("client").and_then(Value::as_str).map(|c| c.parse::<IpAddr>().unwrap()),
                client
            );
            prop_assert_eq!(
                read.get("peer").and_then(Value::as_str).map(|p| p.parse::<SocketAddr>().unwrap().to_string()),
                peer.map(|p| SocketAddr::new(p.ip(), p.port()).to_string())
            );
            prop_assert_eq!(read["status"].as_u64(), status.map(u64::from));
            prop_assert!(read.get("status").is_some());
            prop_assert_eq!(read.get("rule").and_then(Value::as_u64), rule.map(|r| r as u64));
            prop_assert_eq!(read.get("tries").and_then(Value::as_u64), tries.map(u64::from));
            prop_assert_eq!(read.get("bytes_out").and_then(Value::as_u64), bytes_out);
            if let Some(us) = duration_us {
                let ms = read["duration_ms"].as_f64().unwrap();
                prop_assert!((ms - us as f64 / 1000.0).abs() <= ms.abs() * 1e-12 + 1e-9);
            }
            let (date, _) = written[9..].split_once('"').unwrap();
            prop_assert_eq!(date.len(), 24);
        }
    }
}
