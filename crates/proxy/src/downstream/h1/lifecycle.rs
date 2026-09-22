//! The other oracle: what a connection's life must have been, given what its client sent.
//!
//! A reader cannot answer this. Which requests reached the upstream, which answer was
//! whose, whether the connection went on or closed, whether anything was left held: all
//! of that is in what happened, not in the bytes of any one message. So this takes the
//! bytes the client sent, splits them with the reference reader ([`super::reference`]),
//! and holds a [`Trace`] of what the harness saw against the rules of
//! [14 §4](../../../../docs/14-downstream-server.md) and §9: one dispatch per request, in
//! order and never ahead; answers in request order; nothing after a request that was
//! refused or whose body was left unread; the connection carried on only where it may;
//! and nothing held at the end.
//!
//! **A trace is facts, not a verdict.** Every event in it is something the harness
//! watched — the scripted upstream saw a request, the client read an answer, the socket
//! closed — and none of it is the server's opinion about its own connection.

use super::reference::{BodyReading, Reading, read};
use crate::upstream::h1::H1Limits;

/// Something the harness saw happen on a client connection, in the order it happened.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Event {
    /// The upstream received the request at this position among those the client sent.
    Dispatched(usize),
    /// The upstream stopped taking this request's body before its end: it answered first
    /// and said to stop, or its connection went.
    UploadAbandoned(usize),
    /// The client received a final answer.
    Answered {
        /// The request the answer says it is for, where the upstream tagged it; `None`
        /// for an answer of the gateway's own.
        request: Option<usize>,
        /// Its status.
        status: u16,
        /// Whether its body ended cleanly, framing and all.
        complete: bool,
    },
    /// The server closed the connection.
    Closed,
}

/// What the harness saw of one client connection.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Trace {
    /// What happened, in order.
    pub events: Vec<Event>,
    /// Exchange permits still held once the connection was gone.
    pub permits_left: usize,
}

/// What a connection's life got wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Violation {
    /// A request reached the upstream that is not one the client sent whole enough to
    /// send, or was refused.
    NotDispatchable(usize),
    /// A request reached the upstream twice.
    DispatchedTwice(usize),
    /// A request reached the upstream before the one before it was answered.
    DispatchedAhead(usize),
    /// The client received an answer for a request other than the next one it asked.
    OutOfOrder {
        /// The request whose answer was due.
        due: usize,
        /// The request the answer was for.
        given: usize,
    },
    /// An answer arrived for no request the client sent.
    AnswerForNothing,
    /// A request refused by the reader was answered with something other than a refusal.
    RefusalNotAnError(usize),
    /// Something happened on the connection after it should have closed.
    CarriedOn,
    /// The connection should have closed, and did not.
    NotClosed,
    /// A request the client sent whole was never answered, on a connection that could
    /// carry it.
    Unanswered(usize),
    /// Exchange permits were left held.
    PermitsHeld(usize),
}

/// A request the client sent, as far as its connection's life turns on it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Sent {
    /// The reader must refuse it: it is not a request, or it is one this project's
    /// policy or bounds refuse.
    refused: bool,
    /// Its body is all there, so the request is whole.
    whole: bool,
    /// HTTP leaves the connection open after it, by its version and `Connection`.
    persistent: bool,
}

/// Splits what the client sent into requests, as the reference reads them: up to and
/// including the first that is refused or is not whole, after which nothing is a request.
fn requests(bytes: &[u8], limits: &H1Limits) -> Vec<Sent> {
    let mut sent = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        match read(&bytes[at..]) {
            Reading::Unfinished => break,
            Reading::Invalid(_) => {
                sent.push(Sent {
                    refused: true,
                    whole: false,
                    persistent: false,
                });
                break;
            }
            Reading::Read(request) => {
                let within = request.notable.is_empty()
                    && request.measured.request_line <= limits.request_line
                    && request.measured.head <= limits.head
                    && request.measured.fields <= limits.fields;
                let end = match &request.body {
                    BodyReading::Whole(body) => Some(body.end),
                    BodyReading::Unfinished | BodyReading::Invalid(_) => None,
                };
                sent.push(Sent {
                    refused: !within,
                    whole: end.is_some(),
                    persistent: request.persistent,
                });
                match end {
                    Some(end) if within => at += end,
                    _ => break,
                }
            }
        }
    }
    sent
}

/// Holds what the harness saw of a connection against what the client sent.
///
/// # Errors
///
/// The first rule the connection's life broke.
pub fn judge(bytes: &[u8], trace: &Trace, limits: &H1Limits) -> Result<(), Violation> {
    let sent = requests(bytes, limits);
    let mut dispatched = vec![false; sent.len()];
    let mut abandoned = vec![false; sent.len()];
    // The request whose answer is due next.
    let mut due = 0;
    // The connection may not carry anything more.
    let mut ended = false;
    let mut closed = false;

    for &event in &trace.events {
        match event {
            Event::Closed => closed = true,
            _ if ended || closed => return Err(Violation::CarriedOn),
            Event::Dispatched(index) => {
                let Some(request) = sent.get(index) else {
                    return Err(Violation::NotDispatchable(index));
                };
                if request.refused {
                    return Err(Violation::NotDispatchable(index));
                }
                if dispatched[index] || index < due {
                    return Err(Violation::DispatchedTwice(index));
                }
                if index > due {
                    return Err(Violation::DispatchedAhead(index));
                }
                dispatched[index] = true;
            }
            Event::UploadAbandoned(index) => {
                if let Some(flag) = abandoned.get_mut(index) {
                    *flag = true;
                }
            }
            Event::Answered {
                request,
                status,
                complete,
            } => {
                let Some(asked) = sent.get(due) else {
                    return Err(Violation::AnswerForNothing);
                };
                if let Some(given) = request
                    && given != due
                {
                    return Err(Violation::OutOfOrder { due, given });
                }
                if asked.refused && (request.is_some() || status < 400) {
                    return Err(Violation::RefusalNotAnError(due));
                }
                // The connection goes on only where nothing is left in doubt: the request
                // whole and persistent, its body not left unread, its answer finished.
                ended = asked.refused
                    || !asked.whole
                    || !asked.persistent
                    || abandoned[due]
                    || !complete;
                due += 1;
            }
        }
    }

    if ended && !closed {
        return Err(Violation::NotClosed);
    }
    // A request sent whole, on a connection still open to it, is owed an answer. One not
    // all sent is not: the client may yet send the rest, or never.
    if !ended && !closed && sent.get(due).is_some_and(|request| request.whole) {
        return Err(Violation::Unanswered(due));
    }
    if trace.permits_left > 0 {
        return Err(Violation::PermitsHeld(trace.permits_left));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use Event::{Answered, Closed, Dispatched, UploadAbandoned};

    fn limits() -> H1Limits {
        H1Limits::default()
    }

    const GET: &[u8] = b"GET /0 HTTP/1.1\r\nhost: a\r\n\r\n";

    fn answer(request: usize) -> Event {
        Answered {
            request: Some(request),
            status: 200,
            complete: true,
        }
    }

    fn judged(bytes: &[u8], events: &[Event]) -> Result<(), Violation> {
        judge(
            bytes,
            &Trace {
                events: events.to_vec(),
                permits_left: 0,
            },
            &limits(),
        )
    }

    fn two() -> Vec<u8> {
        [GET, GET].concat()
    }

    #[test]
    fn requests_answered_one_after_another_is_a_good_life() {
        let bytes = two();
        assert_eq!(
            judged(
                &bytes,
                &[Dispatched(0), answer(0), Dispatched(1), answer(1)]
            ),
            Ok(())
        );
        // The connection may stay open, or the server may close it after both.
        assert_eq!(
            judged(
                &bytes,
                &[Dispatched(0), answer(0), Dispatched(1), answer(1), Closed]
            ),
            Ok(())
        );
        // An answer of the gateway's own, with nothing dispatched, is an answer.
        let local = Answered {
            request: None,
            status: 502,
            complete: true,
        };
        assert_eq!(judged(&bytes, &[local, Dispatched(1), answer(1)]), Ok(()));
    }

    /// Pipelined requests go upstream one at a time, each after the one before it was
    /// answered: no speculative dispatch.
    #[test]
    fn a_request_is_never_sent_ahead_or_twice() {
        let bytes = two();
        assert_eq!(
            judged(
                &bytes,
                &[Dispatched(0), Dispatched(1), answer(0), answer(1)]
            ),
            Err(Violation::DispatchedAhead(1))
        );
        assert_eq!(
            judged(&bytes, &[Dispatched(0), Dispatched(0), answer(0)]),
            Err(Violation::DispatchedTwice(0))
        );
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), Dispatched(0)]),
            Err(Violation::DispatchedTwice(0))
        );
        assert_eq!(
            judged(&bytes, &[Dispatched(2)]),
            Err(Violation::NotDispatchable(2))
        );
    }

    #[test]
    fn answers_come_in_the_order_their_requests_did() {
        let bytes = two();
        assert_eq!(
            judged(
                &bytes,
                &[
                    Dispatched(0),
                    Answered {
                        request: Some(1),
                        status: 200,
                        complete: true
                    }
                ]
            ),
            Err(Violation::OutOfOrder { due: 0, given: 1 })
        );
        assert_eq!(
            judged(
                &bytes,
                &[
                    Dispatched(0),
                    answer(0),
                    Dispatched(1),
                    answer(1),
                    answer(1)
                ]
            ),
            Err(Violation::AnswerForNothing)
        );
    }

    /// A request the reader refuses is answered with a refusal, after every answer before
    /// it, and nothing after it is a request.
    #[test]
    fn nothing_after_a_refused_request_is_served() {
        let bytes = [GET, b"GET / HTTP/1.1\r\nhost : a\r\n\r\n", GET].concat();
        let refusal = Answered {
            request: None,
            status: 400,
            complete: true,
        };
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), refusal, Closed]),
            Ok(())
        );
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), Dispatched(1)]),
            Err(Violation::NotDispatchable(1))
        );
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), refusal, Dispatched(2)]),
            Err(Violation::CarriedOn)
        );
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), refusal]),
            Err(Violation::NotClosed)
        );
        let accepted = Answered {
            request: None,
            status: 200,
            complete: true,
        };
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), accepted, Closed]),
            Err(Violation::RefusalNotAnError(1))
        );
    }

    /// An upload the upstream stopped taking leaves the rest of the body unread, and it is
    /// never read as the next request: the connection closes after the answer.
    #[test]
    fn an_abandoned_upload_ends_the_connection() {
        let upload = b"POST /0 HTTP/1.1\r\nhost: a\r\ncontent-length: 5\r\n\r\nhello";
        let bytes = [&upload[..], GET].concat();
        let refused_early = Answered {
            request: Some(0),
            status: 413,
            complete: true,
        };
        assert_eq!(
            judged(
                &bytes,
                &[Dispatched(0), UploadAbandoned(0), refused_early, Closed]
            ),
            Ok(())
        );
        assert_eq!(
            judged(
                &bytes,
                &[
                    Dispatched(0),
                    UploadAbandoned(0),
                    refused_early,
                    Dispatched(1)
                ]
            ),
            Err(Violation::CarriedOn)
        );
    }

    #[test]
    fn a_connection_its_request_or_answer_ends_is_closed() {
        let closing = b"GET /0 HTTP/1.1\r\nhost: a\r\nconnection: close\r\n\r\n";
        let bytes = [&closing[..], GET].concat();
        assert_eq!(judged(&bytes, &[Dispatched(0), answer(0), Closed]), Ok(()));
        assert_eq!(
            judged(&bytes, &[Dispatched(0), answer(0), Dispatched(1)]),
            Err(Violation::CarriedOn)
        );
        // An answer whose body did not end cleanly ends the connection too.
        let broken = Answered {
            request: Some(0),
            status: 200,
            complete: false,
        };
        assert_eq!(
            judged(&two(), &[Dispatched(0), broken]),
            Err(Violation::NotClosed)
        );
        // HTTP/1.0 without keep-alive.
        assert_eq!(
            judged(b"GET /0 HTTP/1.0\r\n\r\n", &[Dispatched(0), answer(0)]),
            Err(Violation::NotClosed)
        );
    }

    /// A request whose body is still arriving may be answered early; the connection
    /// closes after it, the rest of the body never read as a request.
    #[test]
    fn a_request_not_all_sent_may_be_answered_and_then_the_connection_closes() {
        let bytes = b"POST /0 HTTP/1.1\r\nhost: a\r\ncontent-length: 50\r\n\r\npartial";
        assert_eq!(judged(bytes, &[Dispatched(0), answer(0), Closed]), Ok(()));
        assert_eq!(judged(bytes, &[]), Ok(()), "it need not be answered at all");
        assert_eq!(
            judged(bytes, &[Dispatched(0), answer(0)]),
            Err(Violation::NotClosed)
        );
    }

    #[test]
    fn a_request_sent_whole_on_a_connection_that_could_carry_it_is_answered() {
        assert_eq!(
            judged(&two(), &[Dispatched(0), answer(0)]),
            Err(Violation::Unanswered(1))
        );
        assert_eq!(
            judged(&two(), &[Dispatched(0), answer(0), Closed]),
            Ok(()),
            "unless it closed"
        );
    }

    #[test]
    fn nothing_is_held_at_the_end() {
        let trace = Trace {
            events: vec![Dispatched(0), answer(0), Closed],
            permits_left: 1,
        };
        assert_eq!(
            judge(GET, &trace, &limits()),
            Err(Violation::PermitsHeld(1))
        );
    }
}
