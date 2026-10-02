//! What the pinned `quiche` (0.30.0) actually does, pinned before EdgeRush builds on it.
//!
//! These are probes, not tests of EdgeRush: each drives the library through the in-memory
//! pipe and scripted peer in `h3_peer` and asserts what it did, so that the driver, the
//! connection-ID scheme and the limits of 16 §3–§6 rest on behaviour seen rather than on an
//! API's name ([16 §7](../../../docs/16-http3.md), step 0). A version of quiche that
//! behaves otherwise fails here first. Where a probe finds that something is left to the
//! application, it says so; the driver is where that is handled.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

// Kept beside the code whose own tests also drive it.
#[path = "../src/h3_peer.rs"]
mod h3_peer;

use h3_peer::{
    Datagram, Pipe, client_addr, client_config, code, frame, frame_bytes, frame_head, get,
    h3_headers, headers, id, server_addr, server_config, server_tls, stream_type, varint,
};
use quiche::h3::{self, Event};
use quiche::{PathEvent, Shutdown};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Server connection IDs are 17 bytes, as 16 §3 has them.
const ID_LEN: usize = 17;

/// The field-section size 16 §6 announces to quiche: twice the 64 KiB head limit, which
/// the driver keeps itself.
const FIELD_SECTION: u64 = 128 << 10;

fn server_ids() -> quiche::Config {
    server_config(server_tls())
}

fn h3_config() -> h3::Config {
    let mut config = h3::Config::new().unwrap();
    config.set_max_field_section_size(FIELD_SECTION);
    config
}

/// A pipe with HTTP/3 running on both sides and the SETTINGS exchanged.
fn h3_pipe() -> (Pipe, h3::Connection, h3::Connection) {
    let mut pipe = Pipe::new(&mut server_ids(), &id(0xa5, ID_LEN));
    let client = h3::Connection::with_transport(&mut pipe.client, &h3_config()).unwrap();
    let server = h3::Connection::with_transport(&mut pipe.server, &h3_config()).unwrap();
    pipe.advance();
    (pipe, client, server)
}

/// A pipe whose client speaks raw HTTP/3 over the transport, its control stream open.
fn raw_pipe() -> (Pipe, h3::Connection) {
    let mut pipe = Pipe::new(&mut server_ids(), &id(0xa5, ID_LEN));
    let server = h3::Connection::with_transport(&mut pipe.server, &h3_config()).unwrap();
    pipe.raw_client_control();
    pipe.advance();
    (pipe, server)
}

fn status(code: &str) -> Vec<h3::Header> {
    h3_headers(&[(":status", code)])
}

fn post() -> Vec<h3::Header> {
    h3_headers(&[
        (":method", "POST"),
        (":scheme", "https"),
        (":authority", h3_peer::SERVER_NAME),
        (":path", "/upload"),
    ])
}

/// The events of `events` in order, with the header lists reduced to their `:status` or
/// their first field, which is what the probes tell heads apart by.
fn shape(events: &[(u64, Event)]) -> Vec<(u64, String)> {
    events
        .iter()
        .map(|(stream, event)| {
            let name = match event {
                Event::Headers { list, more_frames } => {
                    let first = list
                        .iter()
                        .find(|field| quiche::h3::NameValue::name(*field) == b":status")
                        .or(list.first())
                        .map(|field| {
                            String::from_utf8_lossy(quiche::h3::NameValue::value(field))
                                .into_owned()
                        })
                        .unwrap_or_default();
                    format!("headers {first} more={more_frames}")
                }
                Event::Data => "data".to_owned(),
                Event::Finished => "finished".to_owned(),
                Event::Reset(code) => format!("reset {code:#x}"),
                Event::GoAway => "goaway".to_owned(),
                Event::PriorityUpdate => "priority".to_owned(),
            };
            (*stream, name)
        })
        .collect()
}

/// [`shape`] without the stream IDs, for probes that use one stream.
fn names(events: &[(u64, Event)]) -> Vec<String> {
    shape(events).into_iter().map(|(_, name)| name).collect()
}

fn closed_with(conn: &quiche::Connection, error_code: u64) {
    let error = conn.peer_error().expect("the peer closed the connection");
    assert!(error.is_app, "an HTTP/3 error, not a transport one");
    assert_eq!(error.error_code, error_code, "{error:?}");
}

// ===== Connection IDs: every one the server uses is EdgeRush's =====

/// `accept` takes the server's first ID as given, 20 bytes included, and once the client
/// has heard it, everything the client sends names it. quiche mints no server ID of its
/// own: the only IDs a client can reach the connection by are the ones EdgeRush chose,
/// which is what lets them say which worker owns it (16 §3).
#[test]
fn accept_uses_our_id_and_mints_none() {
    let scid = id(0xa5, ID_LEN);
    let mut pipe = Pipe::new(&mut server_ids(), &scid);
    pipe.client
        .stream_send(0, b"after the handshake", true)
        .unwrap();
    pipe.advance();

    assert_eq!(pipe.server.source_id(), scid);
    assert_eq!(pipe.client.destination_id(), scid);
    assert_eq!(pipe.server.active_scids(), 1);
    assert_eq!(pipe.client.available_dcids(), 0);
    // The first flight names the client's own choice; every later datagram names ours.
    let first = &pipe.to_server[0];
    assert_ne!(first.dcid(ID_LEN), scid.as_ref());
    for datagram in &pipe.to_server[pipe.to_server.len() - 2..] {
        assert_eq!(datagram.dcid(ID_LEN), scid.as_ref());
    }
}

/// `new_scid` announces an ID of ours with a reset token of ours, and the number a client
/// holds is capped by the smaller of the two sides' `active_connection_id_limit`: quiche's
/// default is 2, so one extra ID, and the next is refused rather than retiring one.
#[test]
fn new_scid_announces_our_id_up_to_the_limit() {
    let mut pipe = Pipe::new(&mut server_ids(), &id(0xa5, ID_LEN));
    assert_eq!(pipe.server.scids_left(), 1);

    let sequence = pipe
        .server
        .new_scid(&id(0xb6, ID_LEN), 0xb6, false)
        .unwrap();
    pipe.advance();

    assert_eq!(sequence, 1);
    assert_eq!(pipe.client.available_dcids(), 1);
    assert_eq!(
        pipe.server.new_scid(&id(0xb7, ID_LEN), 0xb7, false),
        Err(quiche::Error::IdLimit)
    );
}

/// The stateless-reset token of a connection's first ID is not an argument of `accept`: it
/// is read from the `Config` at the moment of the accept. So the driver sets it on the
/// config just before each accept, and a change made afterwards leaves the connections
/// already accepted with their own (16 §3: tokens are a keyed hash of each ID). A reset
/// closes the client outright, without draining.
#[test]
fn reset_token_is_taken_from_the_config_at_accept() {
    let mut config = server_ids();
    config.set_stateless_reset_token(Some(0x1111));
    let mut first = Pipe::new(&mut config, &id(0xa1, ID_LEN));
    config.set_stateless_reset_token(Some(0x2222));
    let mut second = Pipe::new(&mut config, &id(0xa2, ID_LEN));

    // RFC 9000 §10.3: a short header, unpredictable bytes, the token last.
    let reset = |token: u128| {
        let mut bytes = vec![0x40];
        bytes.extend([0x77; 30]);
        bytes.extend(token.to_be_bytes());
        Datagram {
            bytes,
            from: server_addr(),
            to: client_addr(),
        }
    };
    first.deliver_to_client(reset(0x2222));
    assert!(
        !first.client.is_closed(),
        "the second token reset the first"
    );
    first.deliver_to_client(reset(0x1111));
    assert!(first.client.is_closed());
    second.deliver_to_client(reset(0x2222));
    assert!(second.client.is_closed());
}

/// Retry is a primitive: quiche writes the packet and checks the transport parameters
/// that bind it, and EdgeRush mints and checks the token. A client that has been sent a
/// Retry comes back to the Retry's ID with the token; the server then accepts with the
/// ID the client first used, or the handshake fails.
#[test]
fn retry_binds_the_first_id_or_the_handshake_fails() {
    for tell_quiche in [true, false] {
        let mut config = client_config();
        let client = quiche::connect(
            Some(h3_peer::SERVER_NAME),
            &id(0xc1, 16),
            client_addr(),
            server_addr(),
            &mut config,
        )
        .unwrap();
        // A server not yet made: the Retry is stateless.
        let mut pipe = Pipe::of(
            client,
            quiche::accept(
                &id(0xee, ID_LEN),
                None,
                server_addr(),
                "203.0.113.9:1".parse().unwrap(),
                &mut server_ids(),
            )
            .unwrap(),
        );
        let mut initial = pipe.client_flush().remove(0);
        let header = quiche::Header::from_slice(&mut initial.bytes, ID_LEN).unwrap();
        assert_eq!(header.token.as_deref(), Some(&[][..]));
        let first_dcid = header.dcid.clone().into_owned();
        let retry_id = id(0xa7, ID_LEN);
        let mut out = vec![0; 1_500];
        let len = quiche::retry(
            &header.scid,
            &header.dcid,
            &retry_id,
            b"token of ours",
            header.version,
            &mut out,
        )
        .unwrap();
        pipe.deliver_to_client(Datagram {
            bytes: out[..len].to_vec(),
            from: server_addr(),
            to: client_addr(),
        });

        let again = pipe.client_flush();
        let header = quiche::Header::from_slice(&mut again[0].bytes.clone(), ID_LEN).unwrap();
        assert_eq!(header.dcid, retry_id);
        assert_eq!(header.token.as_deref(), Some(&b"token of ours"[..]));

        let odcid = tell_quiche.then_some(&first_dcid);
        pipe.server = quiche::accept(
            &retry_id,
            odcid,
            server_addr(),
            client_addr(),
            &mut server_ids(),
        )
        .unwrap();
        for datagram in again {
            pipe.deliver_to_server(datagram);
        }
        pipe.advance();

        assert_eq!(pipe.server.is_established(), tell_quiche);
        if !tell_quiche {
            // TRANSPORT_PARAMETER_ERROR: the parameters do not match the Retry. The client's
            // TLS finished before it checked them, so it is its error that tells, not
            // whether it calls itself established.
            let error = pipe.server.peer_error().expect("the client gave up");
            assert_eq!((error.is_app, error.error_code), (false, 0x8));
        }
    }
}

/// `timeout_instant` names the next time quiche must be called back, idle close included,
/// and `on_timeout` at that time is what closes an idle connection: quiche keeps no timer
/// and closes nothing on its own (16 §4, timers).
#[test]
fn idle_close_needs_on_timeout_at_the_instant_named() {
    let mut config = server_ids();
    config.set_max_idle_timeout(200);
    let mut pipe = Pipe::new(&mut config, &id(0xa5, ID_LEN));
    let quiet_since = Instant::now();

    std::thread::sleep(Duration::from_millis(400));
    assert!(!pipe.server.is_closed(), "closed with nobody calling it");

    for _ in 0..50 {
        let Some(at) = pipe.server.timeout_instant() else {
            break;
        };
        std::thread::sleep(at.saturating_duration_since(Instant::now()));
        pipe.server.on_timeout();
    }
    assert!(pipe.server.is_closed() && pipe.server.is_timed_out());
    assert!(quiet_since.elapsed() >= Duration::from_millis(200));
    assert_eq!(pipe.server.timeout_instant(), None);
}

/// A known connection from a new address is validated and migrated to even though the
/// server announced active migration disabled: NAT rebinding keeps working, and the server
/// answers on the new path (16 §3). The events queue until drained.
#[test]
fn rebinding_is_followed_with_migration_disabled() {
    let mut pipe = Pipe::new(&mut server_ids(), &id(0xa5, ID_LEN));
    let rebound = "192.0.2.1:5555".parse().unwrap();
    pipe.client_from = rebound;
    pipe.client
        .stream_send(0, b"from a new port", true)
        .unwrap();
    pipe.advance();

    let mut events = Vec::new();
    while let Some(event) = pipe.server.path_event_next() {
        events.push(event);
    }
    assert!(events.contains(&PathEvent::New(server_addr(), rebound)));
    assert!(events.contains(&PathEvent::Validated(server_addr(), rebound)));
    assert!(events.contains(&PathEvent::PeerMigrated(server_addr(), rebound)));
    assert_eq!(pipe.to_client.last().unwrap().to, rebound);
    assert!(pipe.server.is_established() && !pipe.server.is_closed());
}

// ===== TLS: the listener's context, as quiche leaves it =====

/// A context built as a listener's is kept, callbacks and all, except where quiche sets its
/// own: the SNI callback runs and sees the client's name, but ALPN is quiche's (it installs
/// its own selection for `h3`), so the QUIC side needs a context of its own that shares the
/// TCP side's certificates and choice, not the TCP context itself (16 §1, step 2).
#[test]
fn our_sni_callback_runs_but_alpn_is_quiche_s() {
    let seen = Arc::new(Mutex::new(None));
    let mut tls = server_tls();
    let record = Arc::clone(&seen);
    tls.set_servername_callback(move |ssl, _| {
        *record.lock().unwrap() = ssl
            .servername(boring::ssl::NameType::HOST_NAME)
            .map(str::to_owned);
        Ok(())
    });
    tls.set_alpn_select_callback(|_, _| Ok(b"h2"));

    let pipe = Pipe::new(&mut server_config(tls), &id(0xa5, ID_LEN));

    assert_eq!(seen.lock().unwrap().as_deref(), Some(h3_peer::SERVER_NAME));
    assert_eq!(pipe.server.application_proto(), b"h3");
}

/// Two configurations over separate contexts but one ticket key resume each other's
/// sessions: a new `Config` for new connections — a reload, a certificate rotation —
/// keeps resumption, as the TCP side's front context does (16 step 2).
#[test]
fn a_shared_ticket_key_resumes_across_configs() {
    let key = [0x5a; 48];
    let mut before = server_ids();
    before.set_ticket_key(&key).unwrap();
    let mut pipe = Pipe::new(&mut before, &id(0xa5, ID_LEN));
    let session = pipe.client.session().expect("a ticket").to_vec();
    assert!(!pipe.client.is_resumed());

    let mut after = server_ids();
    after.set_ticket_key(&key).unwrap();
    let mut client = quiche::connect(
        Some(h3_peer::SERVER_NAME),
        &id(0xc2, 16),
        client_addr(),
        server_addr(),
        &mut client_config(),
    )
    .unwrap();
    client.set_session(&session).unwrap();
    let server = quiche::accept(
        &id(0xa6, ID_LEN),
        None,
        server_addr(),
        client_addr(),
        &mut after,
    )
    .unwrap();
    pipe = Pipe::of(client, server);
    pipe.advance();
    assert!(pipe.client.is_established() && pipe.client.is_resumed());
}

// ===== HTTP/3 semantics (16 §5) =====

/// A 1xx goes out with `send_response(fin = false)` and the final head after it with
/// `send_additional_headers`, and the client sees them as two heads in order. quiche does
/// not know a 1xx from a final head: a head after the final one is sent too, so the
/// responder is what forbids it (16 §5).
#[test]
fn interim_then_final_and_nothing_stops_a_head_after_the_final() {
    let (mut pipe, mut client, mut server) = h3_pipe();
    let stream = client
        .send_request(&mut pipe.client, &h3_headers(&get()), true)
        .unwrap();
    pipe.advance();
    pipe.server_events(&mut server);

    server
        .send_response(&mut pipe.server, stream, &status("103"), false)
        .unwrap();
    server
        .send_additional_headers(&mut pipe.server, stream, &status("200"), false, false)
        .unwrap();
    server
        .send_body(&mut pipe.server, stream, b"hello", true)
        .unwrap();
    pipe.advance();

    let (events, error) = pipe.client_events(&mut client);
    assert_eq!(error, None);
    assert_eq!(
        shape(&events),
        [
            (stream, "headers 103 more=true".to_owned()),
            (stream, "headers 200 more=true".to_owned()),
            (stream, "data".to_owned()),
        ]
    );

    let late = client
        .send_request(&mut pipe.client, &h3_headers(&get()), true)
        .unwrap();
    pipe.advance();
    pipe.server_events(&mut server);
    server
        .send_response(&mut pipe.server, late, &status("200"), false)
        .unwrap();
    assert_eq!(
        server.send_additional_headers(&mut pipe.server, late, &status("103"), false, false),
        Ok(())
    );
}

/// Trailers go both ways as a second HEADERS, sent with `is_trailer_section` and received
/// as a second `Headers` event. That event waits until the body before it has been read:
/// `Data` only says bytes are there, and nothing past them surfaces until they are taken,
/// so a request's trailers reach the core only as fast as its body is pulled.
#[test]
fn trailers_both_ways() {
    let (mut pipe, mut client, mut server) = h3_pipe();
    let stream = client
        .send_request(&mut pipe.client, &post(), false)
        .unwrap();
    client
        .send_body(&mut pipe.client, stream, b"upload", false)
        .unwrap();
    client
        .send_additional_headers(
            &mut pipe.client,
            stream,
            &h3_headers(&[("x-sum", "request")]),
            true,
            true,
        )
        .unwrap();
    pipe.advance();

    let (events, error) = pipe.server_events(&mut server);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers POST more=true", "data"]);
    let mut body = [0; 64];
    assert_eq!(server.recv_body(&mut pipe.server, stream, &mut body), Ok(6));
    assert_eq!(&body[..6], b"upload");
    let (events, error) = pipe.server_events(&mut server);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers request more=false", "finished"]);

    server
        .send_response(&mut pipe.server, stream, &status("200"), false)
        .unwrap();
    server
        .send_body(&mut pipe.server, stream, b"answer", false)
        .unwrap();
    server
        .send_additional_headers(
            &mut pipe.server,
            stream,
            &h3_headers(&[("x-sum", "answer")]),
            true,
            true,
        )
        .unwrap();
    pipe.advance();

    let (events, error) = pipe.client_events(&mut client);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers 200 more=true", "data"]);
    assert_eq!(client.recv_body(&mut pipe.client, stream, &mut body), Ok(6));
    let (events, error) = pipe.client_events(&mut client);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers answer more=false", "finished"]);
}

/// A third HEADERS on a request stream is refused by quiche, not handed on.
#[test]
fn a_third_headers_is_a_connection_error() {
    let (mut pipe, mut server) = raw_pipe();
    let mut bytes = headers(&get());
    bytes.extend(headers(&[("x-trailer", "1")]));
    bytes.extend(headers(&[("x-third", "1")]));
    pipe.raw_client_send(0, &bytes, true);
    pipe.advance();

    let (_, error) = pipe.server_events(&mut server);
    pipe.advance();
    assert_eq!(error, Some(h3::Error::FrameUnexpected));
    closed_with(&pipe.client, code::FRAME_UNEXPECTED);
}

/// An answer can be sent whole while the upload is still coming, and the rest of the
/// upload stopped with STOP_SENDING: the client learns it when it next writes, even before
/// it has read the answer.
#[test]
fn an_early_answer_during_an_upload_then_stop_sending() {
    let (mut pipe, mut client, mut server) = h3_pipe();
    let stream = client
        .send_request(&mut pipe.client, &post(), false)
        .unwrap();
    client
        .send_body(&mut pipe.client, stream, &[0x61; 1_000], false)
        .unwrap();
    pipe.advance();
    pipe.server_events(&mut server);

    server
        .send_response(&mut pipe.server, stream, &status("413"), false)
        .unwrap();
    server
        .send_body(&mut pipe.server, stream, b"too large", true)
        .unwrap();
    pipe.server
        .stream_shutdown(stream, Shutdown::Read, code::NO_ERROR)
        .unwrap();
    pipe.advance();

    let (events, _) = pipe.client_events(&mut client);
    assert_eq!(names(&events), ["headers 413 more=true", "data"]);
    assert_eq!(
        client.send_body(&mut pipe.client, stream, b"more", false),
        Err(h3::Error::TransportError(quiche::Error::StreamStopped(
            code::NO_ERROR
        )))
    );
    let mut body = [0; 64];
    assert_eq!(client.recv_body(&mut pipe.client, stream, &mut body), Ok(9));
    let (events, _) = pipe.client_events(&mut client);
    assert_eq!(names(&events), ["finished"]);
}

/// A client's reset reaches the server as `Reset` with its code; a stream the server gives
/// up is reset with the code the server chooses, which the client sees the same way.
#[test]
fn resets_both_ways_carry_their_codes() {
    let (mut pipe, mut client, mut server) = h3_pipe();
    let cancelled = client
        .send_request(&mut pipe.client, &post(), false)
        .unwrap();
    let rejected = client
        .send_request(&mut pipe.client, &post(), false)
        .unwrap();
    pipe.advance();
    pipe.server_events(&mut server);

    pipe.client
        .stream_shutdown(cancelled, Shutdown::Write, code::REQUEST_CANCELLED)
        .unwrap();
    pipe.server
        .stream_shutdown(rejected, Shutdown::Write, code::REQUEST_REJECTED)
        .unwrap();
    pipe.server
        .stream_shutdown(rejected, Shutdown::Read, code::REQUEST_REJECTED)
        .unwrap();
    pipe.advance();

    let (events, _) = pipe.server_events(&mut server);
    assert!(
        shape(&events).contains(&(cancelled, "reset 0x10c".to_owned())),
        "{events:?}"
    );
    let (events, _) = pipe.client_events(&mut client);
    assert!(
        shape(&events).contains(&(rejected, "reset 0x10b".to_owned())),
        "{events:?}"
    );
}

/// GOAWAY carries exactly the ID it is given, whatever quiche's documentation calls it, so
/// EdgeRush passes the first stream not processed, as RFC 9114 §5.2 means; an increase is
/// refused. quiche's client stops sending requests; its server goes on handing over
/// requests on streams at or past the ID, so refusing them is EdgeRush's (16 §5).
#[test]
fn goaway_sends_the_id_given_and_the_server_still_takes_later_streams() {
    let (mut pipe, mut client, mut server) = h3_pipe();
    let served = client
        .send_request(&mut pipe.client, &h3_headers(&get()), true)
        .unwrap();
    pipe.advance();
    pipe.server_events(&mut server);

    server.send_goaway(&mut pipe.server, served + 4).unwrap();
    assert_eq!(
        server.send_goaway(&mut pipe.server, served + 8),
        Err(h3::Error::IdError)
    );
    pipe.advance();
    let (events, _) = pipe.client_events(&mut client);
    assert!(
        shape(&events).contains(&(served + 4, "goaway".to_owned())),
        "{events:?}"
    );
    assert_eq!(
        client.send_request(&mut pipe.client, &h3_headers(&get()), true),
        Err(h3::Error::FrameUnexpected)
    );

    // A client that sends one anyway, underneath its HTTP/3 layer.
    pipe.raw_client_send(served + 4, &headers(&get()), true);
    pipe.advance();
    let (events, error) = pipe.server_events(&mut server);
    assert_eq!(error, None);
    assert!(
        shape(&events).contains(&(served + 4, "headers GET more=false".to_owned())),
        "{events:?}"
    );
}

// ===== Frame bounds against a hostile client (16 §2) =====

/// A HEADERS frame is refused on its declared length alone, past 1.5 × the announced field
/// section size, before any of its payload has arrived; up to that, a partial frame is
/// held and nothing is reported. The limit announced is the one configured.
#[test]
fn headers_are_refused_on_declared_length() {
    let bound = FIELD_SECTION * 3 / 2;

    let (mut pipe, mut server) = raw_pipe();
    let mut bytes = frame_head(frame::HEADERS, bound);
    bytes.extend([0; 1_000]);
    pipe.raw_client_send(0, &bytes, false);
    pipe.advance();
    assert_eq!(pipe.server_events(&mut server), (Vec::new(), None));
    pipe.advance();
    assert!(!pipe.client.is_closed());

    let (mut pipe, mut server) = raw_pipe();
    pipe.raw_client_send(0, &frame_head(frame::HEADERS, bound + 1), false);
    pipe.advance();
    let (_, error) = pipe.server_events(&mut server);
    pipe.advance();
    assert_eq!(error, Some(h3::Error::ExcessiveLoad));
    closed_with(&pipe.client, code::EXCESSIVE_LOAD);

    let (mut pipe, mut client, _) = h3_pipe();
    pipe.client_events(&mut client);
    assert!(
        client
            .peer_settings_raw()
            .unwrap()
            .contains(&(0x6, FIELD_SECTION))
    );
}

/// A field section within the frame bound but past the announced size once decoded closes
/// the whole connection, as the frame bound does: there is no answer for the one request.
/// Below the announced size a head reaches the driver whole, which is why quiche is told
/// twice the head limit: a head that is only too big is the driver's to answer 431
/// (16 §6).
#[test]
fn a_field_section_past_the_announced_size_closes_the_connection() {
    let (mut pipe, mut server) = raw_pipe();
    let within = "v".repeat(FIELD_SECTION as usize - 1_000);
    let mut fields = get();
    fields.push(("x-long", &within));
    pipe.raw_client_send(0, &headers(&fields), true);
    pipe.advance();
    let (events, error) = pipe.server_events(&mut server);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers GET more=false", "finished"]);

    let (mut pipe, mut server) = raw_pipe();
    let long = "v".repeat(FIELD_SECTION as usize + 1_000);
    let mut fields = get();
    fields.push(("x-long", &long));
    let bytes = headers(&fields);
    assert!((bytes.len() as u64) < FIELD_SECTION * 3 / 2);
    pipe.raw_client_send(0, &bytes, true);
    pipe.advance();

    let (events, error) = pipe.server_events(&mut server);
    pipe.advance();
    assert_eq!(events, []);
    assert_eq!(error, Some(h3::Error::ExcessiveLoad));
    closed_with(&pipe.client, code::EXCESSIVE_LOAD);
}

/// An unknown frame is discarded as it arrives and its credit returned: a client can send
/// sixteen times the stream window of it and then a request, and the request is served.
#[test]
fn unknown_frames_are_discarded_and_their_credit_returned() {
    let window = 64 << 10;
    let mut config = server_ids();
    config.set_initial_max_stream_data_bidi_remote(window);
    config.set_max_stream_window(window);
    let mut pipe = Pipe::new(&mut config, &id(0xa5, ID_LEN));
    let mut server = h3::Connection::with_transport(&mut pipe.server, &h3_config()).unwrap();
    pipe.raw_client_control();
    pipe.advance();

    let junk = 16 * window as usize;
    let mut left = frame_head(frame::RESERVED, junk as u64);
    left.extend(vec![0x6a; junk]);
    left.extend(headers(&get()));
    let mut rounds = 0;
    while !left.is_empty() {
        rounds += 1;
        assert!(rounds < 10_000, "credit stopped coming back");
        let written = match pipe.client.stream_send(0, &left, true) {
            Ok(written) => written,
            Err(quiche::Error::Done) => 0,
            Err(error) => panic!("{error:?}"),
        };
        left.drain(..written);
        pipe.advance();
        let (events, error) = pipe.server_events(&mut server);
        assert_eq!(error, None);
        if left.is_empty() {
            assert_eq!(names(&events), ["headers GET more=false", "finished"]);
        } else {
            assert_eq!(events, []);
        }
    }
}

/// SETTINGS longer than 256 bytes closes the connection on its declared length, as
/// H3_FRAME_ERROR rather than H3_EXCESSIVE_LOAD.
#[test]
fn oversized_settings_close_the_connection() {
    let mut pipe = Pipe::new(&mut server_ids(), &id(0xa5, ID_LEN));
    let mut server = h3::Connection::with_transport(&mut pipe.server, &h3_config()).unwrap();
    let mut bytes = varint(stream_type::CONTROL);
    bytes.extend(frame_head(frame::SETTINGS, 257));
    pipe.raw_client_send(2, &bytes, false);
    pipe.advance();

    let (_, error) = pipe.server_events(&mut server);
    pipe.advance();
    assert_eq!(error, Some(h3::Error::FrameError));
    closed_with(&pipe.client, code::FRAME_ERROR);
}

/// A unidirectional stream of an unknown type is stopped and its data dropped; the
/// connection goes on serving requests. The STOP_SENDING carries H3_NO_ERROR, where RFC
/// 9114 §6.2 says H3_STREAM_CREATION_ERROR SHOULD be used: harmless, since the client
/// only learns to stop.
#[test]
fn unknown_stream_types_are_stopped() {
    let (mut pipe, mut server) = raw_pipe();
    let mut bytes = varint(stream_type::RESERVED);
    bytes.extend([0x6a; 100]);
    pipe.raw_client_send(6, &bytes, false);
    pipe.advance();
    pipe.server_events(&mut server);
    pipe.advance();

    assert_eq!(
        pipe.client.stream_send(6, b"more", false),
        Err(quiche::Error::StreamStopped(code::NO_ERROR))
    );
    pipe.raw_client_send(0, &headers(&get()), true);
    pipe.advance();
    let (events, error) = pipe.server_events(&mut server);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers GET more=false", "finished"]);
}

/// A field section that refers to the dynamic table is refused: quiche keeps none, and
/// announces a capacity of zero, so no client can make it keep one.
#[test]
fn dynamic_table_references_are_refused() {
    let (mut pipe, mut server) = raw_pipe();
    // Required Insert Count 1 (encoded as 2), Base 0, then an indexed line into the
    // dynamic table (RFC 9204 §4.5.2, T = 0).
    let section = [0x02, 0x00, 0x80];
    pipe.raw_client_send(0, &frame_bytes(frame::HEADERS, &section), true);
    pipe.advance();

    let (_, error) = pipe.server_events(&mut server);
    pipe.advance();
    assert_eq!(error, Some(h3::Error::QpackDecompressionFailed));
    closed_with(&pipe.client, code::QPACK_DECOMPRESSION_FAILED);
}

/// The QPACK encoder stream is read and dropped; instructions on it do not fail the
/// connection, since no table exists for them to fill. (Kept with the dynamic-table
/// probe: together they are what "quiche keeps no dynamic table" means.)
#[test]
fn qpack_encoder_instructions_are_dropped() {
    let (mut pipe, mut server) = raw_pipe();
    let mut bytes = varint(stream_type::QPACK_ENCODER);
    // Set Dynamic Table Capacity 0 (RFC 9204 §4.3.1).
    bytes.push(0x20);
    pipe.raw_client_send(6, &bytes, false);
    pipe.raw_client_send(0, &headers(&get()), true);
    pipe.advance();

    let (events, error) = pipe.server_events(&mut server);
    assert_eq!(error, None);
    assert_eq!(names(&events), ["headers GET more=false", "finished"]);
}

/// Small answers on several streams go out in one packet, in the order they were written.
/// The published quiche writes one stream's frame a packet; the vendored one fills the
/// packet (`vendor/quiche/VENDORED.md`, 16 §2), and this is what says it still does.
#[test]
fn several_streams_share_a_packet() {
    let (mut pipe, mut client, mut server) = h3_pipe();
    let streams: Vec<u64> = (0..8)
        .map(|_| {
            client
                .send_request(&mut pipe.client, &h3_headers(&get()), true)
                .unwrap()
        })
        .collect();
    pipe.advance();
    pipe.server_events(&mut server);

    for &stream in &streams {
        server
            .send_response(&mut pipe.server, stream, &status("200"), false)
            .unwrap();
        server
            .send_body(&mut pipe.server, stream, b"hello", true)
            .unwrap();
    }
    let mut buf = vec![0; 1_350];
    let mut datagrams = Vec::new();
    while let Ok((len, info)) = pipe.server.send(&mut buf) {
        datagrams.push(Datagram {
            bytes: buf[..len].to_vec(),
            from: info.from,
            to: info.to,
        });
    }
    assert_eq!(datagrams.len(), 1);
    for datagram in datagrams {
        pipe.deliver_to_client(datagram);
    }

    let (events, error) = pipe.client_events(&mut client);
    assert_eq!(error, None);
    let answered: Vec<u64> = events
        .iter()
        .filter(|(_, event)| matches!(event, Event::Headers { .. }))
        .map(|(stream, _)| *stream)
        .collect();
    assert_eq!(answered, streams);
}

/// A stream whose frame does not fit beside another's in a packet goes in a later one.
/// Coalescing fills a packet frame after frame; when what is left cannot take the next
/// stream's frame header, that stream waits, all its data still to send, for the next.
#[test]
fn a_stream_left_out_of_a_full_packet_is_sent_in_a_later_one() {
    // A stream ID of 2^30 takes an 8-byte varint and an offset past 16 KiB a 4-byte one:
    // a 15-byte frame header, more than another stream's frame may leave in a packet.
    const LATE: u64 = 1 << 30;
    const FIRST: usize = 16 << 10;
    const REST: usize = 1_000;
    // Some length of the other stream's answer leaves 13 or 14 bytes after its frame.
    for early_len in 1_000..1_200 {
        let mut config = server_ids();
        config.set_initial_max_streams_bidi(1 << 29);
        let mut pipe = Pipe::new(&mut config, &id(0xa5, ID_LEN));
        pipe.client.stream_send(0, b"a", false).unwrap();
        pipe.client.stream_send(LATE, b"b", false).unwrap();
        pipe.advance();
        // Stream 0 first in every packet, the late stream after it.
        pipe.server.stream_priority(0, 0, false).unwrap();
        // The late stream's offset past 16 KiB, as far as the window takes it each round.
        let mut sent = 0;
        while sent < FIRST {
            sent += pipe
                .server
                .stream_send(LATE, &vec![b'x'; FIRST - sent], false)
                .unwrap();
            pipe.advance();
        }

        let early = vec![b'e'; early_len];
        assert_eq!(pipe.server.stream_send(0, &early, false), Ok(early_len));
        let rest = vec![b'y'; REST];
        assert_eq!(pipe.server.stream_send(LATE, &rest, true), Ok(REST));
        pipe.advance();

        let mut buf = vec![0; 64 << 10];
        let (mut got, mut fin) = (0, false);
        while let Ok((len, end)) = pipe.client.stream_recv(LATE, &mut buf) {
            got += len;
            fin = end;
        }
        assert!(
            fin && got == FIRST + REST,
            "beside {early_len} bytes: {got} of {} bytes, fin {fin}",
            FIRST + REST
        );
    }
}

// 17 bytes, the length 16 §3 gives server IDs, is within what QUIC allows and quiche takes.
const _: () = assert!(ID_LEN <= quiche::MAX_CONN_ID_LEN);
