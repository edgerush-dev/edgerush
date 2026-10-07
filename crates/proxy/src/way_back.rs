//! An answer on its way back to the client: what the gateway does to it, whichever way it
//! holds it — the lines our own client read, or a map — and whichever path it came by
//! ([22 §5](../../../docs/22-explain-and-test.md)). An upstream's answer is edited for the
//! hop it goes on ([`upstream_answer`]), a redirect's is made ([`redirect_answer`]), a gRPC
//! call the gateway answers itself is answered as gRPC answers ([`call_rejected`],
//! [`call_redirected`]), and every answer, the gateway's own among them, says last what each
//! one says ([`every_answer`]). `serve` calls these on each of its paths, so that an answer
//! is edited the same whichever it took, and `edgerush test` calls them to tell the answer a
//! test's client would get.
//!
//! Nothing here does I/O: what each step needs is passed in.

use crate::fields::OverlayFull;
use crate::grpc::status::is_grpc;
use crate::hop_by_hop;
use crate::metrics::Answer;
use crate::request::Rejection;
use crate::websocket::{self, Key, WEBSOCKET};
use edgerush_config::CompiledListener;
use edgerush_filters::{HeaderModifier, request_id};
use edgerush_router::Fields;
use http::header::{CONTENT_TYPE, LOCATION, SEC_WEBSOCKET_ACCEPT, UPGRADE};
use http::{HeaderName, HeaderValue, Method, Response, StatusCode, Version};

/// An answer's head, as the way back edits it: its status and its fields.
pub trait AnswerHead {
    /// What its fields are read as.
    type Fields: Fields + ?Sized;

    /// Its fields, as edited so far.
    fn as_fields(&self) -> &Self::Fields;

    /// What it answers.
    fn status(&self) -> StatusCode;

    /// Makes it answer `status` instead.
    fn set_status(&mut self, status: StatusCode);

    /// Takes the names its own `Connection` gave, `nominated`, and those that may never
    /// follow, out of its `Trailer` declaration.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if the declaration that is left cannot be added.
    fn filter_declaration(&mut self, nominated: &[HeaderName]) -> Result<(), OverlayFull>;

    /// Takes off the fields that are about the upstream's connection and not the client's.
    fn strip(&mut self);

    /// Makes a rule's changes to it.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if more fields are added than it holds, which no config the gateway
    /// takes comes to.
    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), OverlayFull>;

    /// Gives `name` the one value `value`, in place of every one it had.
    ///
    /// # Errors
    ///
    /// [`OverlayFull`] if the field cannot be added.
    fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), OverlayFull>;

    /// Takes out every field called `name`.
    fn remove_field(&mut self, name: &HeaderName);
}

/// An answer held as a map: an HTTP/2 upstream's, and the gateway's own. A map takes every
/// field it is given, so nothing here fails.
impl<B> AnswerHead for Response<B> {
    type Fields = http::HeaderMap;

    fn as_fields(&self) -> &http::HeaderMap {
        self.headers()
    }

    fn status(&self) -> StatusCode {
        Response::status(self)
    }

    fn set_status(&mut self, status: StatusCode) {
        *self.status_mut() = status;
    }

    fn filter_declaration(&mut self, nominated: &[HeaderName]) -> Result<(), OverlayFull> {
        crate::h1::filter_declaration(self.headers_mut(), nominated);
        Ok(())
    }

    fn strip(&mut self) {
        hop_by_hop::strip_response(self.headers_mut());
    }

    fn apply(&mut self, changes: &HeaderModifier) -> Result<(), OverlayFull> {
        changes.apply(self.headers_mut());
        Ok(())
    }

    fn set_field(&mut self, name: HeaderName, value: HeaderValue) -> Result<(), OverlayFull> {
        self.headers_mut().insert(name, value);
        Ok(())
    }

    fn remove_field(&mut self, name: &HeaderName) {
        self.headers_mut().remove(name);
    }
}

/// What an upstream's answer is edited by: its rule, and what its request asked.
#[derive(Debug, Clone, Copy, Default)]
pub struct Way<'a> {
    /// The rule's changes to an answer's fields, if it makes any.
    pub changes: Option<&'a HeaderModifier>,
    /// Whether the client spoke HTTP/1.1, which alone has `Upgrade`: a 426 that offers
    /// WebSocket says so to it, and to no other (19 §2).
    pub upgradable: bool,
    /// For a WebSocket handshake, how its switch is told.
    pub websocket: Option<WebSocket<'a>>,
}

/// A WebSocket handshake, as its answer tells of the switch (19 §2 to §4).
#[derive(Debug, Clone, Copy)]
pub struct WebSocket<'a> {
    /// The key an HTTP/1.1 client sent, whose switch is a 101 with the Accept of that key;
    /// none for an extended CONNECT, on a stream of an HTTP/2 or HTTP/3 client, whose
    /// switch is a 200 (RFC 8441 §5).
    pub(crate) client: Option<&'a Key>,
    /// Whether it went to its backend as an extended CONNECT, whose switch is any 2xx
    /// (RFC 9110 §9.3.6); otherwise it went as an HTTP/1.1 upgrade, whose switch is a 101.
    pub(crate) connected: bool,
}

/// Why an upstream's answer cannot go to the client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Failed {
    /// Its edits could not all be made: more fields added than a raw answer holds.
    Edits,
    /// A 2xx to an extended CONNECT that is not the switch: to a CONNECT every 2xx opens the
    /// tunnel (RFC 9110 §9.3.6), and a page the backend served in place of the switch would
    /// be read as WebSocket frames.
    NotSwitched,
}

impl From<OverlayFull> for Failed {
    fn from(_: OverlayFull) -> Self {
        Self::Edits
    }
}

/// Edits an upstream's answer for the client (14 §6): a name its own `Connection` gave
/// does not travel on, and is not declared onwards either; then what is about the upstream's
/// connection comes off, and the rule's changes are made. Last, a WebSocket's client is told
/// of its switch as it asked for it, and an HTTP/1.1 client is told that a 426 offers
/// WebSocket (19 §2 to §4).
///
/// # Errors
///
/// [`Failed`], and then the answer does not go.
pub fn upstream_answer<A: AnswerHead + ?Sized>(
    answer: &mut A,
    way: &Way<'_>,
) -> Result<(), Failed> {
    let nominated = hop_by_hop::nominated(answer.as_fields());
    // Read before the hop-by-hop fields come off: RFC 9110 §15.5.22 has a 426 name the
    // protocol it wants, and WebSocket is one the gateway can switch to (19 §2).
    let offers = answer.status() == StatusCode::UPGRADE_REQUIRED
        && way.upgradable
        && websocket::offered(answer.as_fields());
    answer.filter_declaration(&nominated)?;
    answer.strip();
    if let Some(changes) = way.changes {
        answer.apply(changes)?;
    }
    match &way.websocket {
        Some(handshake) => switch(answer, handshake, offers),
        None if offers => answer.set_field(UPGRADE, WEBSOCKET).map_err(Failed::from),
        None => Ok(()),
    }
}

/// Tells a WebSocket's client of its switch, or of none.
fn switch<A: AnswerHead + ?Sized>(
    answer: &mut A,
    handshake: &WebSocket<'_>,
    offers: bool,
) -> Result<(), Failed> {
    let status = answer.status();
    let switched = if handshake.connected {
        status.is_success()
    } else {
        status == StatusCode::SWITCHING_PROTOCOLS
    };
    match handshake.client {
        // A 101 that says it switched to WebSocket, with the Accept of the client's own key:
        // an HTTP/1.1 backend's was of the gateway's, and an HTTP/2 backend's switch has none.
        Some(key) if switched => {
            answer.set_status(StatusCode::SWITCHING_PROTOCOLS);
            answer.set_field(UPGRADE, WEBSOCKET)?;
            answer.set_field(SEC_WEBSOCKET_ACCEPT, key.accept_value())?;
        }
        Some(_) if offers => answer.set_field(UPGRADE, WEBSOCKET)?,
        Some(_) => {}
        // An extended CONNECT's client is told of the switch with a 200 (RFC 8441 §5), and
        // with no Accept, which has no key to be of here, whatever backend or rule gave one.
        None if switched => {
            answer.set_status(StatusCode::OK);
            answer.remove_field(&SEC_WEBSOCKET_ACCEPT);
            answer.remove_field(&UPGRADE);
        }
        // Never 2xx of anything else: to a CONNECT, that opens the tunnel.
        None if status.is_success() => return Err(Failed::NotSwitched),
        None => {}
    }
    Ok(())
}

/// Makes a redirect's answer of `answer`: its `status` and `location`, with its rule's
/// `changes` to an answer's fields.
///
/// # Errors
///
/// [`Failed::Edits`] if the changes cannot all be made, which a map never fails at.
pub fn redirect_answer<A: AnswerHead + ?Sized>(
    answer: &mut A,
    status: StatusCode,
    location: HeaderValue,
    changes: Option<&HeaderModifier>,
) -> Result<(), Failed> {
    answer.set_status(status);
    answer.set_field(LOCATION, location)?;
    if let Some(changes) = changes {
        answer.apply(changes)?;
    }
    Ok(())
}

/// Makes `answer` the gateway's own as the final recipient of a TRACE or OPTIONS that may
/// be forwarded no further (RFC 9110 §7.6.2): 200 to OPTIONS and 405 to TRACE, which it does
/// not reflect, each with the `Allow` RFC 9110 §15.5.6 asks of a 405: what it answers
/// itself.
///
/// # Errors
///
/// As [`redirect_answer`]'s, which a map never gives.
pub fn final_recipient_answer<A: AnswerHead + ?Sized>(
    answer: &mut A,
    options: bool,
) -> Result<(), Failed> {
    answer.set_status(if options {
        StatusCode::OK
    } else {
        StatusCode::METHOD_NOT_ALLOWED
    });
    answer.set_field(http::header::ALLOW, HeaderValue::from_static("OPTIONS"))?;
    Ok(())
}

/// What every answer says last, the gateway's own among them: that its listener serves
/// HTTP/3 as well, `alt_svc`, if it does (03 §4), and the request's ID, `id`, in place of
/// any the upstream gave, if its listener gives one.
pub fn every_answer<A: AnswerHead + ?Sized>(
    answer: &mut A,
    alt_svc: Option<&HeaderModifier>,
    id: Option<HeaderValue>,
) {
    // A raw answer's overlay holds as many fields as a rule adds and more; these are two.
    if let Some(alt_svc) = alt_svc {
        let _added = answer.apply(alt_svc);
    }
    if let Some(id) = id {
        let _set = answer.set_field(request_id::HEADER, id);
    }
}

/// The `Alt-Svc` a listener's answers carry: HTTP/3 on `port`, where its UDP socket is, for
/// as long as its config says (RFC 7838 §3). None for a listener whose config has no HTTP/3.
#[must_use]
pub fn alt_svc(listener: &CompiledListener, port: u16) -> Option<HeaderModifier> {
    let http3 = listener.http3?;
    let value = format!("h3=\":{port}\"; ma={}", http3.alt_svc_max_age);
    HeaderModifier::new([("alt-svc", value.as_str())], [], []).ok()
}

/// Whether a request is a gRPC call, which the gateway answers itself as gRPC does: HTTP/2
/// or HTTP/3 with one gRPC content type, read from its head as the client sent it (15 §6),
/// and never a CONNECT, which a 2xx would tell its tunnel opened (RFC 9110 §9.3.6).
/// What `serve` reads a call by, without the deadline it reads beside it; a test holds the
/// two to each other.
#[must_use]
pub fn is_grpc_call<F: Fields + ?Sized>(version: Version, method: &Method, fields: &F) -> bool {
    if !matches!(version, Version::HTTP_2 | Version::HTTP_3) || method == Method::CONNECT {
        return false;
    }
    let mut types = fields.values(&CONTENT_TYPE);
    types.next().is_some_and(is_grpc) && types.next().is_none()
}

/// Makes `answer` the data plane's own answer to a gRPC call it refuses for `rejection`: a
/// trailers-only answer, 200 with the status gRPC gives the cause, which is how gRPC answers
/// a call it fails before any message (15 §6).
pub fn call_rejected<A: AnswerHead + ?Sized>(answer: &mut A, rejection: Rejection) {
    call_answer(answer, rejection.into());
}

/// The same for a gRPC call its rule redirects: no gRPC client follows a redirect.
pub fn call_redirected<A: AnswerHead + ?Sized>(answer: &mut A) {
    call_answer(answer, Answer::Redirected);
}

/// Makes `answer` the data plane's own answer to a gRPC call, for `reason`.
pub(crate) fn call_answer<A: AnswerHead + ?Sized>(answer: &mut A, reason: Answer) {
    let (code, why) = reason.grpc();
    answer.set_status(StatusCode::OK);
    // An answer of the gateway's own holds these three and more: nothing here fails.
    let _told = answer
        .set_field(CONTENT_TYPE, HeaderValue::from_static("application/grpc"))
        .and_then(|()| answer.set_field(HeaderName::from_static("grpc-status"), code.value()))
        .and_then(|()| {
            answer.set_field(
                HeaderName::from_static("grpc-message"),
                crate::grpc::status::message(why),
            )
        });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fields::FieldLines;
    use crate::raw::RawAnswer;
    use bytes::Bytes;
    use http::HeaderMap;
    use proptest::prelude::*;

    /// RFC 6455 §1.3's example key, and its Accept.
    const KEY: &[u8] = b"dGhlIHNhbXBsZSBub25jZQ==";
    const ACCEPT: &str = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=";

    /// What an answer says: its status, and every field by name, in order within a name
    /// (RFC 9110 §5.3).
    type Said = (u16, Vec<(String, String)>);

    /// An answer as both the lines our own client reads and the map the same bytes make.
    fn both(sent: &str) -> (RawAnswer, Response<()>) {
        let sent = Bytes::copy_from_slice(sent.as_bytes());
        let mut room = [httparse::EMPTY_HEADER; 32];
        let mut response = httparse::Response::new(&mut room);
        assert!(response.parse(&sent).unwrap().is_complete());
        let status = StatusCode::from_u16(response.code.unwrap()).unwrap();
        let mut map = Response::new(());
        *map.status_mut() = status;
        for field in response.headers.iter() {
            map.headers_mut().append(
                HeaderName::from_bytes(field.name.as_bytes()).unwrap(),
                HeaderValue::from_bytes(field.value).unwrap(),
            );
        }
        let lines = FieldLines::new(&sent, response.headers).unwrap();
        (RawAnswer::new(status, sent.clone(), lines), map)
    }

    fn said(status: StatusCode, headers: &HeaderMap) -> Said {
        let mut fields: Vec<(String, String)> = headers
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_str().unwrap().to_owned()))
            .collect();
        fields.sort_by(|one, other| one.0.cmp(&other.0));
        (status.as_u16(), fields)
    }

    fn of_raw(raw: RawAnswer) -> Said {
        let parts = raw.into_parts();
        said(parts.status, &parts.headers)
    }

    fn of_map(map: &Response<()>) -> Said {
        said(map.status(), map.headers())
    }

    /// What the answer `sent` comes to by `way`, which must be the same both ways it can
    /// be held.
    fn edited(sent: &str, way: &Way<'_>) -> Result<Said, Failed> {
        let (mut raw, mut map) = both(sent);
        let by_raw = upstream_answer(&mut raw, way);
        let by_map = upstream_answer(&mut map, way);
        assert_eq!(by_raw, by_map, "{sent}");
        by_raw?;
        let (raw, map) = (of_raw(raw), of_map(&map));
        assert_eq!(raw, map, "{sent}");
        Ok(raw)
    }

    fn expected(status: u16, fields: &[(&str, &str)]) -> Result<Said, Failed> {
        let mut fields: Vec<(String, String)> = fields
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect();
        fields.sort_by(|one, other| one.0.cmp(&other.0));
        Ok((status, fields))
    }

    fn modifier(set: &[(&str, &str)], add: &[(&str, &str)], remove: &[&str]) -> HeaderModifier {
        HeaderModifier::new(
            set.iter().copied(),
            add.iter().copied(),
            remove.iter().copied(),
        )
        .unwrap()
    }

    /// A WebSocket handshake's way: the client's key for an HTTP/1.1 client, none for an
    /// extended CONNECT; `connected` for one that went to its backend as an extended CONNECT.
    fn handshake(client: Option<&Key>, connected: bool) -> Way<'_> {
        Way {
            changes: None,
            upgradable: client.is_some(),
            websocket: Some(WebSocket { client, connected }),
        }
    }

    #[test]
    fn what_the_answers_connection_names_neither_travels_on_nor_stays_declared() {
        let sent = "HTTP/1.1 200 OK\r\nConnection: x-a, keep-alive\r\nKeep-Alive: timeout=5\r\n\
                    x-a: 1\r\nTrailer: x-a, grpc-status\r\ncontent-type: text/plain\r\n\r\n";
        assert_eq!(
            edited(sent, &Way::default()),
            expected(
                200,
                &[("content-type", "text/plain"), ("trailer", "grpc-status")]
            )
        );
        // A declaration that comes to nothing goes.
        let sent = "HTTP/1.1 200 OK\r\nConnection: x-a\r\nTrailer: x-a, upgrade\r\n\r\n";
        assert_eq!(edited(sent, &Way::default()), expected(200, &[]));
    }

    /// The rule's changes come after the upstream's connection fields are off: a field the
    /// upstream's `Connection` named, which the rule adds, is the rule's.
    #[test]
    fn the_rules_changes_are_made_to_what_is_left() {
        let sent = "HTTP/1.1 200 OK\r\nConnection: cache-tag\r\ncache-tag: theirs\r\n\
                    server: gunicorn\r\nx-a: 1\r\n\r\n";
        let changes = modifier(
            &[("x-served-by", "edgerush")],
            &[("cache-tag", "orders")],
            &["server"],
        );
        let way = Way {
            changes: Some(&changes),
            ..Way::default()
        };
        assert_eq!(
            edited(sent, &way),
            expected(
                200,
                &[
                    ("cache-tag", "orders"),
                    ("x-a", "1"),
                    ("x-served-by", "edgerush")
                ]
            )
        );
    }

    #[test]
    fn a_426_that_offers_websocket_says_so_to_an_http11_client_alone() {
        let offers = "HTTP/1.1 426 Upgrade Required\r\nUpgrade: h2c, WebSocket\r\n\
                      Connection: Upgrade\r\n\r\n";
        let upgradable = Way {
            upgradable: true,
            ..Way::default()
        };
        assert_eq!(
            edited(offers, &upgradable),
            expected(426, &[("upgrade", "websocket")])
        );
        assert_eq!(edited(offers, &Way::default()), expected(426, &[]));
        // Another protocol is not one the gateway can switch to, and is not offered on.
        let other = "HTTP/1.1 426 Upgrade Required\r\nUpgrade: h2c\r\n\r\n";
        assert_eq!(edited(other, &upgradable), expected(426, &[]));
        // Only a 426 says what it wants.
        let other = "HTTP/1.1 200 OK\r\nUpgrade: websocket\r\n\r\n";
        assert_eq!(edited(other, &upgradable), expected(200, &[]));
        // A refused handshake's 426 offers as well.
        let key = Key::read(KEY).unwrap();
        assert_eq!(
            edited(offers, &handshake(Some(&key), false)),
            expected(426, &[("upgrade", "websocket")])
        );
    }

    #[test]
    fn an_http11_clients_switch_is_a_101_with_the_accept_of_its_own_key() {
        let key = Key::read(KEY).unwrap();
        let switched = [("sec-websocket-accept", ACCEPT), ("upgrade", "websocket")];
        // An HTTP/1.1 backend's, with the Accept of the gateway's key.
        let ours = Key::of(*b"the gateway's 16").accept_value();
        let sent = format!(
            "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n\
             Sec-WebSocket-Accept: {}\r\n\r\n",
            ours.to_str().unwrap()
        );
        assert_eq!(
            edited(&sent, &handshake(Some(&key), false)),
            expected(101, &switched)
        );
        // An HTTP/2 backend's, any 2xx to its extended CONNECT.
        for status in ["200 OK", "204 No Content"] {
            let sent = format!("HTTP/1.1 {status}\r\nx-a: 1\r\n\r\n");
            let mut fields = switched.to_vec();
            fields.push(("x-a", "1"));
            assert_eq!(
                edited(&sent, &handshake(Some(&key), true)),
                expected(101, &fields)
            );
        }
        // A refusal goes as it came, from either.
        let refused = "HTTP/1.1 403 Forbidden\r\nx-a: 1\r\n\r\n";
        for connected in [false, true] {
            assert_eq!(
                edited(refused, &handshake(Some(&key), connected)),
                expected(403, &[("x-a", "1")])
            );
        }
        // A 2xx from an HTTP/1.1 backend is not its switch.
        let page = "HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n";
        assert_eq!(
            edited(page, &handshake(Some(&key), false)),
            expected(200, &[("x-a", "1")])
        );
    }

    #[test]
    fn an_extended_connects_switch_is_a_200_and_nothing_else_is_2xx() {
        // An HTTP/1.1 backend's 101, without what said it switched to the gateway.
        let sent = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n\
                    Connection: Upgrade\r\nSec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n\
                    x-a: 1\r\n\r\n";
        assert_eq!(
            edited(sent, &handshake(None, false)),
            expected(200, &[("x-a", "1")])
        );
        // An HTTP/2 backend's: any 2xx, and without an Accept it should not have given.
        for status in ["200 OK", "204 No Content"] {
            let sent = format!("HTTP/1.1 {status}\r\nsec-websocket-accept: a\r\nx-a: 1\r\n\r\n");
            assert_eq!(
                edited(&sent, &handshake(None, true)),
                expected(200, &[("x-a", "1")])
            );
        }
        // Nor one a rule gives it, from either.
        let changes = modifier(&[("sec-websocket-accept", "b")], &[], &[]);
        for (sent, connected) in [
            ("HTTP/1.1 101 Switching Protocols\r\nx-a: 1\r\n\r\n", false),
            ("HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n", true),
        ] {
            let way = Way {
                changes: Some(&changes),
                ..handshake(None, connected)
            };
            assert_eq!(edited(sent, &way), expected(200, &[("x-a", "1")]));
        }
        // A page in place of an HTTP/1.1 backend's switch does not go.
        for status in ["200 OK", "204 No Content"] {
            let sent = format!("HTTP/1.1 {status}\r\nx-a: 1\r\n\r\n");
            assert_eq!(
                edited(&sent, &handshake(None, false)),
                Err(Failed::NotSwitched)
            );
        }
        // A refusal goes as it came, from either.
        for connected in [false, true] {
            assert_eq!(
                edited(
                    "HTTP/1.1 404 Not Found\r\nx-a: 1\r\n\r\n",
                    &handshake(None, connected)
                ),
                expected(404, &[("x-a", "1")])
            );
        }
    }

    /// No config the gateway takes fills a raw answer's overlay; one that is filled here
    /// fails its edits, rather than dropping one, and does not go.
    #[test]
    fn an_answer_that_cannot_take_its_edits_does_not_go() {
        let (mut raw, _) = both("HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n");
        for n in 0.. {
            let name = HeaderName::from_bytes(format!("x-{n}").as_bytes()).unwrap();
            if raw.set_field(name, HeaderValue::from_static("v")).is_err() {
                break;
            }
        }
        let changes = modifier(&[("x-served-by", "edgerush")], &[], &[]);
        let way = Way {
            changes: Some(&changes),
            ..Way::default()
        };
        assert_eq!(upstream_answer(&mut raw, &way), Err(Failed::Edits));
    }

    #[test]
    fn a_redirect_is_its_status_and_location_then_its_rules_changes() {
        let location = HeaderValue::from_static("https://shop.example.com/open");
        let check = |changes: Option<&HeaderModifier>, fields: &[(&str, &str)]| {
            let (mut raw, mut map) = both("HTTP/1.1 200 OK\r\n\r\n");
            let status = StatusCode::MOVED_PERMANENTLY;
            let by_raw = redirect_answer(&mut raw, status, location.clone(), changes);
            let by_map = redirect_answer(&mut map, status, location.clone(), changes);
            assert_eq!((by_raw, by_map), (Ok(()), Ok(())));
            assert_eq!(of_raw(raw), of_map(&map));
            assert_eq!(Ok(of_map(&map)), expected(301, fields));
        };
        let place = ("location", "https://shop.example.com/open");
        check(None, &[place]);
        check(
            Some(&modifier(&[("cache-control", "no-store")], &[], &[])),
            &[("cache-control", "no-store"), place],
        );
        // The rule's are made last, its `Location` among them.
        check(
            Some(&modifier(&[("location", "/elsewhere")], &[], &[])),
            &[("location", "/elsewhere")],
        );
    }

    #[test]
    fn every_answer_says_whether_http3_is_served_and_the_requests_id_last() {
        let alt_svc = modifier(&[("alt-svc", "h3=\":443\"; ma=86400")], &[], &[]);
        let id = HeaderValue::from_static("0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f");
        let check = |alt_svc: Option<&HeaderModifier>,
                     id: Option<&HeaderValue>,
                     fields: &[(&str, &str)]| {
            let (mut raw, mut map) =
                both("HTTP/1.1 200 OK\r\nx-request-id: theirs\r\nx-a: 1\r\n\r\n");
            every_answer(&mut raw, alt_svc, id.cloned());
            every_answer(&mut map, alt_svc, id.cloned());
            assert_eq!(of_raw(raw), of_map(&map));
            assert_eq!(Ok(of_map(&map)), expected(200, fields));
        };
        check(None, None, &[("x-a", "1"), ("x-request-id", "theirs")]);
        check(
            Some(&alt_svc),
            Some(&id),
            &[
                ("alt-svc", "h3=\":443\"; ma=86400"),
                ("x-a", "1"),
                ("x-request-id", "0199e8a4-7c1b-7d2e-9a57-3f1c2b4d5e6f"),
            ],
        );
    }

    /// A call the gateway refuses or redirects is told so as gRPC tells it: 200, its content
    /// type and the status gRPC gives the cause, whatever the answer held before.
    #[test]
    fn a_grpc_call_the_gateway_answers_is_answered_as_grpc_answers() {
        let check = |make: &dyn Fn(&mut dyn AnswerHead<Fields = HeaderMap>),
                     code: &str,
                     why: &str| {
            let (_, mut map) = both("HTTP/1.1 404 Not Found\r\ncontent-type: text/plain\r\n\r\n");
            make(&mut map);
            assert_eq!(
                Ok(of_map(&map)),
                expected(
                    200,
                    &[
                        ("content-type", "application/grpc"),
                        ("grpc-message", why),
                        ("grpc-status", code)
                    ]
                )
            );
        };
        check(
            &|answer| call_rejected(answer, Rejection::NoRoute),
            "12",
            "no route serves this method",
        );
        check(
            &|answer| call_rejected(answer, Rejection::NoBackend),
            "14",
            "no upstream can serve the call",
        );
        check(
            &|answer| call_redirected(answer),
            "12",
            "the route answers with a redirect, which a call cannot follow",
        );
        // The same for a raw answer, as the module's every step is.
        let (mut raw, mut map) = both("HTTP/1.1 200 OK\r\nx-a: 1\r\n\r\n");
        call_rejected(&mut raw, Rejection::NoRoute);
        call_rejected(&mut map, Rejection::NoRoute);
        assert_eq!(of_raw(raw), of_map(&map));
    }

    /// Names in any case, among them every one the way back does something with, and values
    /// that make them mean something.
    fn field() -> impl Strategy<Value = (&'static str, &'static str)> {
        (
            prop::sample::select(vec![
                "connection",
                "Connection",
                "keep-alive",
                "trailer",
                "upgrade",
                "Upgrade",
                "sec-websocket-accept",
                "location",
                "x-request-id",
                "x-a",
                "X-A",
                "x-b",
            ]),
            prop::sample::select(vec![
                "upgrade",
                "x-a",
                "x-a, keep-alive",
                "X-B, trailer",
                "grpc-status, x-a",
                "websocket",
                "h2c, WebSocket",
                "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=",
                "1",
            ]),
        )
    }

    fn change() -> impl Strategy<Value = (&'static str, &'static str)> {
        (
            prop::sample::select(vec![
                "x-a",
                "x-b",
                "upgrade",
                "sec-websocket-accept",
                "trailer",
            ]),
            prop::sample::select(vec!["1", "2", "websocket", "x-a"]),
        )
    }

    proptest! {
        /// An answer is edited the same whichever way it is held: the same status and
        /// fields, or the same reason it does not go.
        #[test]
        fn an_answer_is_edited_the_same_whichever_way_it_is_held(
            status in prop::sample::select(vec![
                "101 Switching Protocols", "200 OK", "204 No Content", "403 Forbidden",
                "426 Upgrade Required", "500 Internal Server Error",
            ]),
            fields in prop::collection::vec(field(), 0..8),
            set in prop::collection::vec(change(), 0..3),
            add in prop::collection::vec(change(), 0..3),
            remove in prop::collection::vec(
                prop::sample::select(vec!["x-a", "upgrade", "trailer"]), 0..3),
            upgradable in any::<bool>(),
            websocket in prop::option::of((any::<bool>(), any::<bool>())),
        ) {
            let mut sent = format!("HTTP/1.1 {status}\r\n");
            for (name, value) in &fields {
                sent.push_str(&format!("{name}: {value}\r\n"));
            }
            sent.push_str("\r\n");
            // A modifier the gateway would take; one it would refuse is no case at all.
            let Ok(changes) = HeaderModifier::new(set, add, remove) else {
                return Ok(());
            };
            let key = Key::read(KEY).unwrap();
            let way = Way {
                changes: Some(&changes),
                upgradable,
                websocket: websocket.map(|(client, connected)| WebSocket {
                    client: client.then_some(&key),
                    connected,
                }),
            };
            let _same = edited(&sent, &way);
        }

        /// A call is told as `serve`'s request reads one: by its version, its method and
        /// its content types, whatever its deadline says.
        #[test]
        fn a_call_is_told_as_serve_tells_one(
            version in prop::sample::select(vec![
                Version::HTTP_10, Version::HTTP_11, Version::HTTP_2, Version::HTTP_3,
            ]),
            method in prop::sample::select(vec![Method::POST, Method::GET, Method::CONNECT]),
            types in prop::collection::vec(prop::sample::select(vec![
                "application/grpc", "application/grpc+proto", "Application/GRPC",
                "application/grpc; charset=utf-8", "application/grpcx", "application/json",
                "text/plain", "",
            ]), 0..3),
            timeouts in prop::collection::vec(prop::sample::select(vec![
                "1S", "100m", "x", "",
            ]), 0..3),
        ) {
            let mut fields = HeaderMap::new();
            for value in &types {
                fields.append(CONTENT_TYPE, HeaderValue::from_static(value));
            }
            for value in &timeouts {
                fields.append("grpc-timeout", HeaderValue::from_static(value));
            }
            let call =
                crate::grpc::call::Call::of(version, &method, &fields, tokio::time::Instant::now);
            prop_assert_eq!(is_grpc_call(version, &method, &fields), call.is_some());
        }
    }
}
