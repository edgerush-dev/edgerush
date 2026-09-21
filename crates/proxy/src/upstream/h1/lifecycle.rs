//! The other oracle: whether a connection may carry another exchange.
//!
//! A framer cannot answer this. Whether a connection is still good turns on what
//! happened on it — whether the request all went, whether the answer all came, whether
//! anything failed, whether bytes nobody asked for are sitting in front of it — and none
//! of that is in the answer's bytes. So this takes a [`Trace`] of what happened and
//! applies the five conditions of [13 §6](../../../../docs/13-http1-upstream.md) to it,
//! and the harness holds each client's own verdict against what this says
//! ([13 §8](../../../../docs/13-http1-upstream.md)).
//!
//! **A trace is facts, not a verdict.** Every field of it is something the harness
//! watched: the script said whether the peer closed, the reference framer said where the
//! message ended and what its framing was, and the run said whether anything failed.
//! None of it is a client's opinion about its own connection, which is the thing being
//! checked.
//!
//! The fifth condition — that the identity is still eligible, the worker still exists,
//! and the age and idle bounds allow a return — is the pool's and not a single
//! connection's, so nothing here has a view on it.

/// What happened on a connection. Every field is observed, and none of them is any
/// client's verdict about whether the connection is still good.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Trace {
    /// The request body ended successfully and every framing byte of it was written.
    pub request_finished: bool,
    /// Some of the upload was given up on — the answer came first, or a deadline did.
    pub upload_abandoned: bool,
    /// Exactly one final answer finished, its chunk terminator and trailers included.
    pub response_finished: bool,
    /// HTTP leaves the connection usable: the version allows it and the answer's own
    /// `Connection` did not ask for closure.
    pub persistent: bool,
    /// The answer's body was delimited by the connection closing, so there is no next
    /// message on it by construction.
    pub close_delimited: bool,
    /// The connection became something that is not HTTP/1.1.
    pub tunnel: bool,
    /// This end asked for closure in the request it sent.
    pub asked_to_close: bool,
    /// Something failed: a parse, a body, the transport, or a deadline.
    pub failed: bool,
    /// The exchange was cancelled.
    pub cancelled: bool,
    /// The peer said more than the message needed. Whatever those bytes are, they are
    /// not an answer to a request that has not been sent yet.
    pub surplus: bool,
    /// The peer closed its end, so there is nothing to reuse.
    pub peer_closed: bool,
}

impl Trace {
    /// An exchange that went through from end to end: the baseline every test moves one
    /// fact away from, so that what each condition is worth can be seen one at a time.
    #[must_use]
    pub fn finished() -> Self {
        Self {
            request_finished: true,
            upload_abandoned: false,
            response_finished: true,
            persistent: true,
            close_delimited: false,
            tunnel: false,
            asked_to_close: false,
            failed: false,
            cancelled: false,
            surplus: false,
            peer_closed: false,
        }
    }
}

/// Why a connection may not be used again. The first reason that applies, in the order
/// [13 §6](../../../../docs/13-http1-upstream.md) puts its conditions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refused {
    /// The request did not all go. What the peer read is not a whole message, so what it
    /// says next is an answer to something nobody sent.
    UnfinishedRequest,
    /// Part of the upload was given up on, so the peer is still expecting bytes that
    /// will never come.
    AbandonedUpload,
    /// The answer did not finish. Its remaining bytes would be read as the next
    /// answer's.
    UnfinishedResponse,
    /// One side asked for the connection to close.
    NotPersistent,
    /// The answer's body was delimited by the close.
    ClosedDelimited,
    /// The connection is no longer HTTP/1.1.
    Tunnel,
    /// This end asked for closure itself.
    AskedToClose,
    /// Something failed.
    Failed,
    /// The exchange was cancelled.
    Cancelled,
    /// Bytes the message did not account for are in front of the connection.
    Surplus,
    /// The peer's end is closed.
    PeerClosed,
}

/// Whether the connection may carry another exchange, and why not when it may not.
///
/// The conditions are conjunctive and the order is only about which reason is reported:
/// a connection needs every one of them, and a reason that arrives second is no less
/// true than the one that arrives first.
pub fn reusable(trace: &Trace) -> Result<(), Refused> {
    // 1. The request all went.
    if !trace.request_finished {
        return Err(Refused::UnfinishedRequest);
    }
    if trace.upload_abandoned {
        return Err(Refused::AbandonedUpload);
    }
    // 2. Exactly one answer finished.
    if !trace.response_finished {
        return Err(Refused::UnfinishedResponse);
    }
    // 3. HTTP leaves the connection usable, and neither end asked otherwise.
    if !trace.persistent {
        return Err(Refused::NotPersistent);
    }
    if trace.close_delimited {
        return Err(Refused::ClosedDelimited);
    }
    if trace.tunnel {
        return Err(Refused::Tunnel);
    }
    if trace.asked_to_close {
        return Err(Refused::AskedToClose);
    }
    // 4. Nothing failed, nothing is left over, and the socket is still open.
    if trace.failed {
        return Err(Refused::Failed);
    }
    if trace.cancelled {
        return Err(Refused::Cancelled);
    }
    if trace.surplus {
        return Err(Refused::Surplus);
    }
    if trace.peer_closed {
        return Err(Refused::PeerClosed);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The trace with one fact changed, which is the only way to see what a condition is
    /// worth: a test that breaks two things at once cannot say which of them mattered.
    fn but(change: impl FnOnce(&mut Trace)) -> Result<(), Refused> {
        let mut trace = Trace::finished();
        change(&mut trace);
        reusable(&trace)
    }

    #[test]
    fn an_exchange_that_went_through_leaves_the_connection_usable() {
        assert_eq!(reusable(&Trace::finished()), Ok(()));
    }

    #[test]
    fn the_request_has_to_have_all_gone() {
        assert_eq!(
            but(|trace| trace.request_finished = false),
            Err(Refused::UnfinishedRequest)
        );
        assert_eq!(
            but(|trace| trace.upload_abandoned = true),
            Err(Refused::AbandonedUpload)
        );
    }

    #[test]
    fn the_answer_has_to_have_all_come() {
        assert_eq!(
            but(|trace| trace.response_finished = false),
            Err(Refused::UnfinishedResponse)
        );
    }

    #[test]
    fn http_has_to_leave_the_connection_usable() {
        assert_eq!(
            but(|trace| trace.persistent = false),
            Err(Refused::NotPersistent)
        );
        assert_eq!(
            but(|trace| trace.close_delimited = true),
            Err(Refused::ClosedDelimited)
        );
        assert_eq!(but(|trace| trace.tunnel = true), Err(Refused::Tunnel));
        assert_eq!(
            but(|trace| trace.asked_to_close = true),
            Err(Refused::AskedToClose)
        );
    }

    #[test]
    fn nothing_may_have_failed_or_been_left_over() {
        assert_eq!(but(|trace| trace.failed = true), Err(Refused::Failed));
        assert_eq!(but(|trace| trace.cancelled = true), Err(Refused::Cancelled));
        assert_eq!(but(|trace| trace.surplus = true), Err(Refused::Surplus));
        assert_eq!(
            but(|trace| trace.peer_closed = true),
            Err(Refused::PeerClosed)
        );
    }

    #[test]
    fn every_condition_is_needed_and_one_being_met_vouches_for_nothing() {
        // A complete answer on a persistent connection is still no reason to keep one
        // whose request never finished, and a finished request is no reason to keep one
        // with bytes left in front of it. Each condition is tested above by itself; this
        // is the pair that a reader most expects to excuse each other.
        let mut trace = Trace::finished();
        trace.request_finished = false;
        assert_eq!(reusable(&trace), Err(Refused::UnfinishedRequest));
        let mut trace = Trace::finished();
        trace.surplus = true;
        assert_eq!(reusable(&trace), Err(Refused::Surplus));
    }
}
