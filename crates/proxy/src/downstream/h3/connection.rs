//! One HTTP/3 connection's driver, and the task of each of its requests
//! ([16 §4, §5](../../../../../docs/16-http3.md)).
//!
//! The driver is woken when its listener hands the connection a datagram, when a request's
//! task has read or written, and when a deadline comes. It hands on what quiche has for the
//! streams, starts a task for each new request, gives the connection its spare IDs, and
//! sends what quiche wants sent. Its deadlines, one alarm in the worker's timers for the
//! soonest: quiche's own; the handshake, 30 s from the first packet, then the first
//! request, 10 s from the handshake's end; the keep-alive, once no request is open; the
//! drain's bound. And while a request or tunnel is open, a PING once the client has been
//! quiet for half the idle timeout in force, so that quiche's idle timer, which sees only
//! packets, never ends a connection with a stream open and a client that answers (16 §6).
//!
//! A request whose head is malformed is reset with `H3_MESSAGE_ERROR`; one whose head is too
//! large is answered 431; one on a stream past the GOAWAY sent is reset with
//! `H3_REQUEST_REJECTED`, which a client may send again elsewhere (RFC 9114 §5.2). A
//! connection whose client has had more than 1,024 requests reset or refused for its own
//! errors, its head, body or trailers, is closed with `H3_EXCESSIVE_LOAD`, as h2 closes an
//! HTTP/2 one (15 §3).

use crate::downstream::h1::connection::{Answered, expects_continue};
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h3::body::IncomingH3;
use crate::downstream::h3::code;
use crate::downstream::h3::conn::{Conn, Slot, State, Stream};
use crate::downstream::h3::head::{self, Refused, RequestHead};
use crate::downstream::h3::listener::Shared;
use crate::downstream::h3::send::{Unsent, flush};
use crate::downstream::h3::stream::H3Stream;
use crate::downstream::h3::writer::{Responder, SendError};
use crate::forwarding::{Client, Cut};
use crate::interim::Interim;
use crate::request_body::RequestBody;
use crate::timers::Alarm;
use crate::tunnel::Carried;
use bytes::Bytes;
use http::header::{DATE, HeaderValue};
use http::{Method, Request, Response, StatusCode, Version};
use http_body::Body;
use quiche::h3::Event;
use std::error::Error as StdError;
use std::future::{Future, poll_fn};
use std::net::SocketAddr;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Instant;

/// What the driver keeps between its turns.
struct Driving {
    accepted: Instant,
    /// A request has come.
    asked: bool,
    /// No request has been open since then.
    quiet_since: Option<Instant>,
    /// The client's datagrams quiche had taken when the driver last saw the count change,
    /// and when that was.
    heard: (u64, Instant),
    /// When the last PING was sent to keep the connection.
    pinged_at: Option<Instant>,
    /// How long a connection with a stream open may be quiet before it is sent a PING:
    /// half the idle timeout in force, known once the handshake is done; none for none.
    ping_after: Option<Duration>,
    /// Still in its handshake, and counted as one.
    handshaking: bool,
    /// When the handshake ended, from which the first request is timed.
    established_at: Option<Instant>,
    /// The ID the client first chose, which finds the connection until the handshake is
    /// done.
    chosen: Vec<u8>,
    /// Every other ID the connection is known by in the listener's table.
    ids: Vec<Vec<u8>>,
    seen: Seen,
    /// When a draining connection is closed regardless.
    drain_by: Option<Instant>,
    /// Where its requests are from: the peer of the last path validated, the handshake's
    /// address until the client moves and its new path is validated (03 §11). The path in
    /// use is no proof: quiche moves to a new address on one datagram from it, which anyone
    /// able to forge a source address can send (RFC 9000 §9.3).
    proved: SocketAddr,
    /// To be closed once what is queued has gone: the GOAWAY above all, which quiche would
    /// drop if the connection were closed with it still queued.
    to_close: bool,
    /// Heads too large that were answered 431 since the last turn, to be told to the
    /// listener with the client they came from.
    refused: u32,
    /// The connection is closing: only quiche's own deadline is left to keep.
    closing: bool,
    unsent: Unsent,
    /// What has come of each HTTP/0.9 request line not yet ended.
    #[cfg(any(test, feature = "interop"))]
    hq_lines: std::collections::HashMap<u64, Vec<u8>>,
}

/// The requests a connection has seen, as a GOAWAY counts them.
#[derive(Debug, Default)]
struct Seen {
    /// Past the highest request stream seen, which a GOAWAY names as the first not seen.
    /// Streams below it may still come, out of order: a lost packet is sent again after
    /// later ones.
    next: u64,
    /// The GOAWAY sent, if one was: no request at or past it is taken.
    goaway: Option<u64>,
}

/// A request the driver found, for its task to answer.
struct Found {
    id: u64,
    head: RequestHead,
    ended: bool,
    /// Over HTTP/0.9, to be answered with the body alone.
    #[cfg(any(test, feature = "interop"))]
    hq: bool,
}

/// Drives `conn` until it closes. `chosen` is the ID the client first sent to, and `from`
/// the address it sent from, which completing the handshake proves; `opened` is held as
/// long as the connection lives.
pub(crate) async fn drive<R, F, B, D, G>(
    conn: Rc<Conn>,
    shared: Rc<Shared>,
    chosen: Vec<u8>,
    from: SocketAddr,
    respond: Rc<R>,
    date: Rc<D>,
    opened: G,
) where
    R: Fn(Request<RequestBody>, Interim, Rc<Client>) -> F + 'static,
    F: Future<Output = Answered<B>> + 'static,
    B: Body<Data = Bytes> + 'static,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate + 'static,
{
    let settings = shared.settings;
    let accepted = Instant::now();
    let first_id = conn.with(|state| state.quic.source_id().to_vec());
    let mut driving = Driving {
        accepted,
        asked: false,
        quiet_since: Some(accepted),
        heard: (0, accepted),
        pinged_at: None,
        ping_after: None,
        handshaking: true,
        established_at: None,
        chosen,
        ids: vec![first_id],
        seen: Seen::default(),
        drain_by: None,
        proved: from,
        to_close: false,
        refused: 0,
        closing: false,
        unsent: Unsent::new(),
        #[cfg(any(test, feature = "interop"))]
        hq_lines: std::collections::HashMap::new(),
    };
    let mut alarm = Alarm::new(&shared.timers, None);
    let mut drain_heard = pin!(conn.drain.notified());
    // Room in the socket, waited for when a flush leaves datagrams unsent. The socket is
    // every connection's: tokio wakes each task waiting on `writable()`, where
    // `poll_send_ready` keeps only the last task's waker.
    let mut room = pin!(shared.socket.writable());
    let mut found = Vec::new();
    poll_fn(|cx| {
        conn.drive_with(cx.waker());
        loop {
            if driving.drain_by.is_none() && conn.drain.poll_on(drain_heard.as_mut(), cx).is_ready()
            {
                driving.drain_by = Some(Instant::now() + settings.drain_within);
                go_away(&conn, &mut driving);
            }
            if conn.take_stirred() || !driving.unsent.is_empty() {
                turn(&conn, &shared, &mut driving, &mut found);
                account(&conn, &shared);
                let proved = driving.proved;
                let client = if found.is_empty() {
                    None
                } else {
                    Some(conn.with(|state| client_now(proved, &mut state.client)))
                };
                // A head answered 431 here has a record all the same (21 §4).
                for _ in 0..std::mem::take(&mut driving.refused) {
                    (shared.refused)(conn.with(|state| client_now(proved, &mut state.client)));
                }
                for request in found.drain(..) {
                    // Made above whenever there is a request.
                    let Some(client) = &client else { break };
                    driving.asked = true;
                    let stream = Stream::adopt(&conn, request.id);
                    #[cfg(any(test, feature = "interop"))]
                    if request.hq {
                        let _detached = tokio::task::spawn_local(super::hq::answer(
                            stream,
                            request.head,
                            Rc::clone(client),
                            Rc::clone(&respond),
                            settings.stream_idle,
                        ));
                        continue;
                    }
                    conn.asked();
                    let _detached = tokio::task::spawn_local(request_task(
                        stream,
                        request.head,
                        request.ended,
                        Rc::clone(client),
                        Rc::clone(&respond),
                        Rc::clone(&date),
                        settings.stream_idle,
                    ));
                }
                flush(&conn, &shared, settings.datagram, &mut driving.unsent);
                if driving.to_close && !driving.closing && driving.unsent.is_empty() {
                    conn.with(|state| {
                        // Fails only for a connection already closing.
                        let _closing = state.quic.close(true, code::NO_ERROR, b"");
                    });
                    driving.closing = true;
                    flush(&conn, &shared, settings.datagram, &mut driving.unsent);
                }
                if conn.with(|state| state.quic.is_closed()) {
                    return Poll::Ready(());
                }
            }
            // What the socket had no room for goes once it has.
            if !driving.unsent.is_empty() && room.as_mut().poll(cx).is_ready() {
                room.set(shared.socket.writable());
                continue;
            }
            // A rapid reset (CVE-2023-44487) is closed, as HTTP/2 closes one (15 §3), and so is
            // a client that has made the server reset too many of its requests.
            if !driving.closing
                && (conn.resetting(settings.reset_judged_after)
                    || conn.provoking(settings.provoked_resets))
            {
                conn.with(|state| {
                    state.cut(Cut::TooManyResets);
                    // Fails only for a connection already closing.
                    let _closing = state.quic.close(true, code::EXCESSIVE_LOAD, b"");
                });
                driving.closing = true;
                conn.stir();
                continue;
            }
            let (open, heard) = conn.with(|state| (state.streams.len(), state.heard));
            let now = Instant::now();
            if heard != driving.heard.0 {
                driving.heard = (heard, now);
            }
            if open == 0 {
                driving.quiet_since.get_or_insert(now);
            } else {
                driving.quiet_since = None;
            }
            // Draining, a connection goes once nothing is open and every answer has arrived,
            // or at the drain's bound: closing ends quiche's sending again of what was lost.
            if !driving.to_close
                && driving.drain_by.is_some()
                && open == 0
                && conn.with(State::delivered)
            {
                close(&conn, &mut driving);
                continue;
            }
            let quic_due = conn
                .with(|state| state.quic.timeout_instant())
                .map(Instant::from_std);
            let (first_due, quiet_due, drain_due) = if driving.to_close {
                (None, None, None)
            } else {
                (
                    // The handshake, then the first request, each in its own time.
                    (!driving.asked).then(|| {
                        driving
                            .established_at
                            .map_or(driving.accepted + settings.handshake, |at| {
                                at + settings.first_request
                            })
                    }),
                    driving
                        .quiet_since
                        .filter(|_| driving.asked)
                        .map(|since| since + settings.keep_alive),
                    driving.drain_by,
                )
            };
            let ping_due = driving
                .ping_after
                .filter(|_| open > 0 && !driving.to_close && !driving.closing)
                .and_then(|after| ping_due(after, driving.heard.1, driving.pinged_at));
            let due = [quic_due, first_due, quiet_due, drain_due, ping_due]
                .into_iter()
                .flatten()
                .min();
            let came = due.is_some_and(|due| alarm.poll_until(cx, due).is_ready());
            if !came {
                if conn.is_stirred() {
                    continue;
                }
                return Poll::Pending;
            }
            let now = Instant::now();
            if quic_due.is_some_and(|due| due <= now) {
                conn.with(|state| state.quic.on_timeout());
                conn.stir();
            }
            if ping_due.is_some_and(|due| due <= now) {
                // Fails only with no path in use, when there is no one to keep.
                let _pinging = conn.with(|state| state.quic.send_ack_eliciting());
                driving.pinged_at = Some(now);
                conn.stir();
            }
            let over = [first_due, quiet_due, drain_due]
                .into_iter()
                .flatten()
                .any(|due| due <= now);
            if over {
                // At the drain's bound, what is still under way is cut off.
                if drain_due.is_some_and(|due| due <= now) {
                    conn.with(|state| state.cut(Cut::Drained));
                }
                close(&conn, &mut driving);
            }
        }
    })
    .await;
    // Closed: every stream's task learns it, and the connection's IDs find nothing more.
    // Its charge goes when the last thing holding it lets go of it, with quiche's memory.
    conn.with(|state| {
        state.closed = true;
        for slot in state.streams.values_mut() {
            slot.wake();
        }
    });
    {
        let mut table = shared.table.borrow_mut();
        for id in driving.ids.iter().chain(std::iter::once(&driving.chosen)) {
            table.remove(id.as_slice());
        }
    }
    if driving.handshaking {
        shared
            .handshakes
            .set(shared.handshakes.get().saturating_sub(1));
    }
    shared
        .connections
        .set(shared.connections.get().saturating_sub(1));
    if shared.drain.is_on() {
        // Kept for the listener if it is not waiting: it looks at its next wait.
        shared.ended.notify_one();
    }
    drop(opened);
}

/// Charges the worker what quiche holds for `conn` (16 §6). A worker that has run out closes
/// its QUIC connection charged the most, which lets go of its charge at once, and tries
/// again, until the charge fits or `conn` is the one closed. If none holds anything, the
/// storage is the other protocols', and `conn`, which cannot be paid for, is closed.
fn account(conn: &Rc<Conn>, shared: &Shared) {
    while conn.charge(&shared.storage).is_err() {
        let heaviest = shared
            .table
            .borrow()
            .values()
            .max_by_key(|held| held.charged())
            .filter(|held| held.charged() > 0)
            .cloned();
        heaviest.as_ref().unwrap_or(conn).shed();
    }
}

/// Closes the connection with nothing wrong, once the client has been told which requests
/// were not seen, so that it sends them again (RFC 9114 §5.2).
fn close(conn: &Conn, driving: &mut Driving) {
    go_away(conn, driving);
    driving.to_close = true;
    conn.stir();
}

/// How long a connection with a stream open may be quiet before it is sent a PING: half the
/// idle timeout in force, `ours` or the client's `theirs_ms`, whichever is lower, a zero
/// being none (RFC 9000 §10.1); none if neither has one.
fn ping_after(ours: Duration, theirs_ms: u64) -> Option<Duration> {
    let theirs = Duration::from_millis(theirs_ms);
    let idle = match (ours.is_zero(), theirs.is_zero()) {
        (true, true) => return None,
        (true, false) => theirs,
        (false, true) => ours,
        (false, false) => ours.min(theirs),
    };
    Some(idle / 2)
}

/// When a connection whose client was last heard at `heard` is due a PING, `after` the
/// silence began: one a silence, none if one has gone since. What the server sends is so
/// bounded by what the client sends, however short an idle timeout the client asks for —
/// pinging on unanswered, every half of it, would answer one datagram with as many PINGs
/// as the congestion window and quiche's idle timer let through. A client that answers
/// none is left to that timer, which a PING restarts only once anyway.
fn ping_due(after: Duration, heard: Instant, pinged: Option<Instant>) -> Option<Instant> {
    match pinged {
        Some(at) if at > heard => None,
        _ => Some(heard + after),
    }
}

/// Sends a GOAWAY naming the first request not seen, once.
fn go_away(conn: &Conn, driving: &mut Driving) {
    if driving.seen.goaway.is_some() {
        return;
    }
    let first_unseen = driving.seen.next;
    let sent = conn.with(|state| {
        let State { quic, h3, .. } = state;
        h3.as_mut()
            .is_some_and(|h3| h3.send_goaway(quic, first_unseen).is_ok())
    });
    if sent {
        driving.seen.goaway = Some(first_unseen);
        conn.stir();
    }
}

/// One turn of the driver's, with the connection in hand: HTTP/3 once the handshake is
/// done, spare IDs, and everything quiche has for the streams.
fn turn(conn: &Rc<Conn>, shared: &Shared, driving: &mut Driving, found: &mut Vec<Found>) {
    let mut issued = Vec::new();
    let mut retired = Vec::new();
    // quiche holds no more than `streams` of the client's streams at once, so pruned past
    // twice that, the answers being delivered cost a lookup or two each.
    let most_delivering = usize::try_from(shared.settings.streams)
        .unwrap_or(usize::MAX)
        .saturating_mul(2);
    let established = conn.with(|state| {
        if state.delivering.len() > most_delivering {
            state.delivered();
        }
        let State {
            quic,
            h3,
            streams,
            delivering,
            ..
        } = state;
        let established = driving.handshaking && quic.is_established();
        // HTTP/3 only where the handshake agreed on it: the interop build offers HTTP/0.9
        // as well.
        if established {
            let theirs = quic
                .peer_transport_params()
                .map_or(0, |params| params.max_idle_timeout);
            driving.ping_after = ping_after(shared.settings.idle_timeout, theirs);
        }
        if established && quic.application_proto() == b"h3" {
            match quiche::h3::Connection::with_transport(quic, &shared.h3) {
                Ok(connection) => *h3 = Some(connection),
                Err(_) => {
                    let _closing = quic.close(true, 0x101, b"");
                }
            }
        }
        // Drained every turn, so that nothing queues without bound (16 §6). A server's quiche
        // tells of a move only once the path moved to is validated, or when it moves back to
        // one that was.
        while let Some(event) = quic.path_event_next() {
            if let quiche::PathEvent::PeerMigrated(_, peer) = event {
                driving.proved = peer;
            }
        }
        while let Some(id) = quic.retired_scid_next() {
            retired.push(id.to_vec());
        }
        if quic.is_established() {
            while quic.scids_left() > 0 {
                let Ok((id, reset)) = shared.issuer.borrow_mut().issue() else {
                    break;
                };
                if quic
                    .new_scid(&quiche::ConnectionId::from_ref(&id), reset, false)
                    .is_err()
                {
                    break;
                }
                issued.push(id.to_vec());
            }
        }
        if let Some(h3) = h3.as_mut() {
            let mut provoked = 0;
            events(
                quic,
                h3,
                streams,
                delivering,
                shared.settings.head_limit,
                &mut driving.seen,
                found,
                &mut driving.refused,
                &mut provoked,
            );
            conn.provoked(provoked);
        }
        #[cfg(any(test, feature = "interop"))]
        if quic.is_established() && quic.application_proto() == super::hq::ALPN {
            let mut asked = Vec::new();
            super::hq::requests(quic, streams, &mut driving.hq_lines, &mut asked);
            found.extend(asked.into_iter().map(|asked| Found {
                id: asked.id,
                head: asked.head,
                ended: true,
                hq: true,
            }));
        }
        while let Some(id) = quic.stream_writable_next() {
            let Some(slot) = streams.get_mut(&id) else {
                continue;
            };
            // quiche reports a stream the client stopped as writable, at once and once. Its
            // task is told whatever it waits on, so that the exchange goes with the answer
            // nobody wants (RFC 9114 §4.1.1): quiche gives the client the stream's credit
            // back once its RESET_STREAM is acknowledged.
            if let Err(quiche::Error::StreamStopped(code)) = quic.stream_capacity(id) {
                slot.stopped = Some(code);
                slot.wake();
            } else if let Some(writer) = slot.writer.take() {
                writer.wake();
            }
        }
        established
    });
    let mut table = shared.table.borrow_mut();
    for id in retired {
        table.remove(id.as_slice());
        driving.ids.retain(|kept| *kept != id);
    }
    for id in issued {
        table.insert(id.clone(), Rc::clone(conn));
        driving.ids.push(id);
    }
    if established {
        // Done with its handshake, the client sends to the IDs it was given.
        table.remove(driving.chosen.as_slice());
        if driving.handshaking {
            driving.handshaking = false;
            driving.established_at = Some(Instant::now());
            shared
                .handshakes
                .set(shared.handshakes.get().saturating_sub(1));
        }
    }
}

/// Hands on what quiche's HTTP/3 layer has, stream by stream. A 431 the driver answers
/// itself joins `delivering`, as a task's whole answer does, and is counted in `refused`.
/// Each head reset for the client's error, or refused 431, is counted in `provoked`; a
/// request rejected after the GOAWAY is not, since the client may have sent it before the
/// GOAWAY reached it (RFC 9114 §5.2).
#[expect(
    clippy::too_many_arguments,
    reason = "each is a different part of the connection's state that a turn hands on to"
)]
fn events(
    quic: &mut quiche::Connection,
    h3: &mut quiche::h3::Connection,
    streams: &mut std::collections::HashMap<u64, Slot>,
    delivering: &mut Vec<u64>,
    head_limit: usize,
    seen: &mut Seen,
    found: &mut Vec<Found>,
    refused: &mut u32,
    provoked: &mut u32,
) {
    loop {
        let (id, event) = match h3.poll(quic) {
            Ok(polled) => polled,
            // Done, or an error that quiche has closed the connection over itself.
            Err(_) => return,
        };
        match event {
            Event::Headers { list, more_frames } => {
                // A second head on a stream with a task is its trailers. A stream whose task
                // has ended has stopped reading, so quiche hands on nothing more of it.
                if let Some(slot) = streams.get_mut(&id) {
                    slot.trailers = Some(head::trailers(&list, head_limit));
                    slot.wake();
                    continue;
                }
                if seen.goaway.is_some_and(|goaway| id >= goaway) {
                    shut(quic, id, code::REQUEST_REJECTED);
                    continue;
                }
                seen.next = seen.next.max(id + 4);
                match head::request(&list, head_limit) {
                    // A head that ends the stream leaves no room for the body it declares.
                    Ok(head) if !more_frames && head.length.is_some_and(|length| length > 0) => {
                        shut(quic, id, code::MESSAGE_ERROR);
                        *provoked += 1;
                    }
                    Ok(head) => {
                        streams.insert(
                            id,
                            Slot {
                                stopped: stopped_already(quic, id),
                                ..Slot::default()
                            },
                        );
                        found.push(Found {
                            id,
                            head,
                            ended: !more_frames,
                            #[cfg(any(test, feature = "interop"))]
                            hq: false,
                        });
                    }
                    Err(Refused::TooLarge) => {
                        if too_large(quic, h3, id) {
                            delivering.push(id);
                            *refused += 1;
                        }
                        *provoked += 1;
                    }
                    Err(Refused::Malformed(_)) => {
                        shut(quic, id, code::MESSAGE_ERROR);
                        *provoked += 1;
                    }
                }
            }
            Event::Data => {
                if let Some(reader) = streams.get_mut(&id).and_then(|slot| slot.reader.take()) {
                    reader.wake();
                }
            }
            Event::Finished => {
                if let Some(slot) = streams.get_mut(&id) {
                    slot.finished = true;
                    slot.wake();
                }
            }
            Event::Reset(code) => {
                if let Some(slot) = streams.get_mut(&id) {
                    slot.reset = Some(code);
                    slot.wake();
                }
            }
            Event::PriorityUpdate => {
                // Held by quiche until taken; priorities are not acted on (16 §1).
                let _taken = h3.take_last_priority_update(id);
            }
            // The client's GOAWAY concerns pushes, which the server makes none of.
            Event::GoAway => {}
        }
    }
}

/// The code of a stop the client sent on stream `id` before its request was seen, for the
/// stream's slot to hold as it is made. Such a stop was told of with no slot to hear it,
/// and is not told again. quiche may have let the stream go since, both its sides done: a
/// side of ours can be done before the request is seen only by the client's stop, whose
/// code went with it.
pub(super) fn stopped_already(quic: &mut quiche::Connection, id: u64) -> Option<u64> {
    match quic.stream_capacity(id) {
        Err(quiche::Error::StreamStopped(code)) => Some(code),
        Err(quiche::Error::InvalidStreamState(_)) => Some(code::REQUEST_CANCELLED),
        _ => None,
    }
}

/// Resets both sides of stream `id` with `code`.
fn shut(quic: &mut quiche::Connection, id: u64, code: u64) {
    let _reset = quic.stream_shutdown(id, quiche::Shutdown::Write, code);
    let _stopped = quic.stream_shutdown(id, quiche::Shutdown::Read, code);
}

/// Answers 431 to a request whose head is past the limit, and reads no more of it: true if
/// the answer went, false if the stream was reset instead.
fn too_large(quic: &mut quiche::Connection, h3: &mut quiche::h3::Connection, id: u64) -> bool {
    let status = StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE;
    let mut headers = http::HeaderMap::new();
    headers.insert(http::header::CONTENT_LENGTH, HeaderValue::from_static("0"));
    let fields = head::answer(&status, &headers);
    if h3.send_response(quic, id, &fields, true).is_err() {
        shut(quic, id, code::REQUEST_REJECTED);
        return false;
    }
    let _stopped = quic.stream_shutdown(id, quiche::Shutdown::Read, code::NO_ERROR);
    true
}

/// Sends what waits in `interim` to be passed on, and a `100` of the continue decision's
/// own if it wants one. A `101` is never sent: HTTP/3 has no upgrade (RFC 9114 §4.5).
fn send_interim(responder: &mut Responder, interim: &Interim) {
    while let Some((status, headers)) = interim.next_forwarded() {
        if status == StatusCode::SWITCHING_PROTOCOLS {
            continue;
        }
        let mut head = Response::new(());
        *head.status_mut() = status;
        *head.headers_mut() = headers;
        // Refused only once the final head has gone, when there is nothing to tell.
        let _sent = responder.interim(&head);
    }
    if interim.take_local_continue() {
        let mut head = Response::new(());
        *head.status_mut() = StatusCode::CONTINUE;
        let _sent = responder.interim(&head);
    }
}

/// Who the requests found now are from: `peer`, the last address proved, which may not be
/// the one the connection began on (RFC 9000 §9). The client last made is kept while that
/// stays the same, so that it is not written out again for every request.
fn client_now(peer: SocketAddr, made: &mut Option<Rc<Client>>) -> Rc<Client> {
    match made {
        Some(client) if client.peer() == Some(peer) => Rc::clone(client),
        _ => {
            let client = Rc::new(Client::connected(peer.ip(), peer));
            *made = Some(Rc::clone(&client));
            client
        }
    }
}

/// A request's task: its answer, boxed. The answer's future is as large as its largest
/// state, the exchange's included, several kilobytes; a task holds its future inline and
/// moves all of it as the task is made and as it finishes. Boxed once here, what the task
/// holds and moves is a pointer.
fn request_task<R, F, B, D>(
    stream: Stream,
    head: RequestHead,
    ended: bool,
    client: Rc<Client>,
    respond: Rc<R>,
    date: Rc<D>,
    idle: Duration,
) -> Pin<Box<impl Future<Output = ()>>>
where
    R: Fn(Request<RequestBody>, Interim, Rc<Client>) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate,
{
    Box::pin(answer(stream, head, ended, client, respond, date, idle))
}

/// Answers one request.
async fn answer<R, F, B, D>(
    stream: Stream,
    head: RequestHead,
    ended: bool,
    client: Rc<Client>,
    respond: Rc<R>,
    date: Rc<D>,
    idle: Duration,
) where
    R: Fn(Request<RequestBody>, Interim, Rc<Client>) -> F,
    F: Future<Output = Answered<B>>,
    B: Body<Data = Bytes>,
    B::Error: Into<Box<dyn StdError + Send + Sync>>,
    D: Fn() -> HttpDate,
{
    // Read before any filter touches the head, as for HTTP/1 and HTTP/2 (14 §5).
    let interim = Interim::listened(
        expects_continue(&head.parts.headers),
        Version::HTTP_3,
        ended,
    );
    // An extended CONNECT's stream is a WebSocket's once it is answered, not a body: the
    // request goes to the core with none, and the stream is read by the tunnel (19 §3).
    let connect = head.parts.method == Method::CONNECT
        && head.parts.extensions.get::<::h2::ext::Protocol>().is_some();
    let body = if connect {
        RequestBody::None
    } else {
        RequestBody::H3(
            IncomingH3::new(Rc::clone(&stream.conn), stream.id, head.length, ended, idle)
                .heard_by(interim.clone()),
        )
    };
    let request = Request::from_parts(head.parts, body);
    let mut responder = Responder::new(Rc::clone(&stream.conn), stream.id, idle);
    let mut answering = pin!(respond(request, interim.clone(), client));
    let answered = poll_fn(|cx| {
        if responder.poll_reset(cx).is_ready() {
            return Poll::Ready(None);
        }
        let answered = answering.as_mut().poll(cx);
        // What the exchange heard, or the continue decision made, in this turn goes out now.
        send_interim(&mut responder, &interim);
        answered.map(Some)
    })
    .await;
    // Reset or stopped by the client, or the connection gone: nothing is to be sent.
    let Some(answered) = answered else {
        if responder.given_up() {
            stream.conn.given_up_early();
        }
        return;
    };
    interim.final_head();
    send_interim(&mut responder, &interim);
    let (mut head, body) = answered.into_response().into_parts();
    if !head.headers.contains_key(DATE)
        && let Ok(now) = HeaderValue::from_bytes(date().as_bytes())
    {
        head.headers.insert(DATE, now);
    }
    // A WebSocket the core switched: the stream stays open after its 200, and is carried.
    let switched = if connect {
        interim.take_switched()
    } else {
        None
    };
    let end = body.is_end_stream() && switched.is_none();
    if let Err(error) = responder.final_head(&head, end).await {
        // Given up before the head had room: the stream may be quiche's no more, and its
        // error say only that.
        if responder.given_up() {
            return stream.conn.given_up_early();
        }
        match error {
            SendError::TimedOut => return responder.reset(code::REQUEST_CANCELLED),
            SendError::H3(_) => return responder.reset(code::INTERNAL_ERROR),
            _ => return,
        }
    }
    if let Some(switched) = switched {
        drop(body);
        responder.idle_for(switched.bounds.idle);
        // The tunnel's clock, which counts both ways, is the reader's: its own would run out
        // on a WebSocket whose client is quiet while its backend talks (19 §5).
        let incoming =
            IncomingH3::new(Rc::clone(&stream.conn), stream.id, None, ended, idle).unwatched();
        let mut tunnel = H3Stream::new(incoming, responder);
        // Closed by both ends is whole; anything else resets the stream as it goes.
        let carried = switched.carry(&mut tunnel, None, &stream.conn.drain).await;
        if carried.how == Carried::Closed {
            stream.answered();
        }
        return;
    }
    // A body that failed has had its stream reset with its own code; one the client stopped
    // or reset, or the connection took, is reset as the stream goes.
    if !end && responder.send_body(body).await.is_err() {
        return;
    }
    stream.answered();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::h3_peer::{self, Pipe, id, server_config, server_tls};

    /// A PING is due at half the lower of the two idle timeouts, a zero being none.
    #[test]
    fn a_ping_is_due_at_half_the_idle_timeout_in_force() {
        let ours = Duration::from_secs(30);
        assert_eq!(ping_after(ours, 0), Some(Duration::from_secs(15)));
        assert_eq!(ping_after(ours, 10_000), Some(Duration::from_secs(5)));
        assert_eq!(ping_after(ours, 60_000), Some(Duration::from_secs(15)));
        assert_eq!(
            ping_after(Duration::ZERO, 8_000),
            Some(Duration::from_secs(4))
        );
        assert_eq!(ping_after(Duration::ZERO, 0), None);
    }

    /// One PING a silence: after one, none is due until the client is heard again, so that
    /// a client asking for a 1 ms idle timeout gets no PING a millisecond for a datagram.
    #[test]
    fn a_ping_is_sent_once_a_silence() {
        let after = Duration::from_micros(500);
        let heard = Instant::now();
        assert_eq!(ping_due(after, heard, None), Some(heard + after));
        let pinged = heard + after;
        assert_eq!(ping_due(after, heard, Some(pinged)), None);
        // Heard again, the next silence gets its PING.
        let again = pinged + Duration::from_millis(3);
        assert_eq!(ping_due(after, again, Some(pinged)), Some(again + after));
        // A PING before a client last heard is no reason to wait.
        assert_eq!(ping_due(after, pinged, Some(pinged)), Some(pinged + after));
    }

    /// Two requests whose datagrams come in the other order, as they do when the first is
    /// lost and sent again: both are found, and the first stream not seen is past both.
    #[test]
    fn requests_that_come_out_of_order_are_both_taken() {
        let mut pipe = Pipe::new(&mut server_config(server_tls()), &id(0xa5, 17));
        let config = quiche::h3::Config::new().unwrap();
        let mut client = quiche::h3::Connection::with_transport(&mut pipe.client, &config).unwrap();
        let mut server = quiche::h3::Connection::with_transport(&mut pipe.server, &config).unwrap();
        pipe.advance();
        let get = h3_peer::h3_headers(&h3_peer::get());
        let first = client.send_request(&mut pipe.client, &get, true).unwrap();
        let first_flight = pipe.client_flush();
        let second = client.send_request(&mut pipe.client, &get, true).unwrap();
        for datagram in pipe.client_flush() {
            pipe.deliver_to_server(datagram);
        }
        for datagram in first_flight {
            pipe.deliver_to_server(datagram);
        }

        let mut streams = std::collections::HashMap::new();
        let mut seen = Seen::default();
        let mut found = Vec::new();
        events(
            &mut pipe.server,
            &mut server,
            &mut streams,
            &mut Vec::new(),
            64 << 10,
            &mut seen,
            &mut found,
            &mut 0,
            &mut 0,
        );
        let mut taken: Vec<u64> = found.iter().map(|found| found.id).collect();
        taken.sort_unstable();
        assert_eq!(taken, [first, second]);
        assert_eq!(seen.next, second + 4);
    }

    /// What a future `make` returns takes, without one being made.
    fn size_of_made<A, F>(_make: impl FnOnce(A) -> F) -> usize {
        std::mem::size_of::<F>()
    }

    /// A request's task holds a pointer to its answer's future, not the future itself, which
    /// the task would copy whole as it is made and as it finishes (16 §4).
    #[test]
    fn a_request_task_holds_its_answer_boxed() {
        type Respond = fn(
            Request<RequestBody>,
            Interim,
            Rc<Client>,
        ) -> std::future::Ready<Answered<http_body_util::Full<Bytes>>>;
        type Date = fn() -> HttpDate;
        type Asked = (Stream, RequestHead, Rc<Client>, Rc<Respond>, Rc<Date>);
        let task = size_of_made(|(stream, head, client, respond, date): Asked| {
            request_task(stream, head, true, client, respond, date, Duration::ZERO)
        });
        let answer = size_of_made(|(stream, head, client, respond, date): Asked| {
            answer(stream, head, true, client, respond, date, Duration::ZERO)
        });
        assert_eq!(
            task,
            std::mem::size_of::<usize>(),
            "a request's task holds {task} bytes; its answer's future is {answer}"
        );
    }
}
