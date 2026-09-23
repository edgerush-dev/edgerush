//! What one request's informational answers do on their way from the upstream exchange to
//! the downstream driver, and the continue decision both ends report to
//! ([14 §5](../../docs/14-downstream-server.md)).
//!
//! A side channel, as the engine's client carries 1xx to a callback: our own server makes one
//! for each request it reads and hands it to the request core with the request — an argument
//! of its own, since a request's extensions take only what may cross threads, and this may
//! not. The upstream exchange tells it what it sees in wire order, and the server writes out
//! what it is to forward while it waits for the final answer. A request that comes with none
//! — one the engine's server read — is served by an exchange with a channel of its own that
//! nobody listens on: the same continue decision, and every 1xx consumed.
//!
//! What waits here is bounded by the exchange's own interim limits (13 §7), and the server
//! takes it in the same turn it polls the answer in. Worker-local, never `Send`.

use crate::downstream::h1::continuing::{Coordinator, Expectation, Relay};
use http::{HeaderMap, StatusCode, Version};
use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;

/// One request's informational answers and continue decision, shared by the exchange that
/// sends it upstream and the server that read it.
#[derive(Debug, Clone)]
pub(crate) struct Interim(Rc<RefCell<State>>);

#[derive(Debug)]
struct State {
    /// The client sent `Expect: 100-continue`, read before any filter touched the head.
    client: bool,
    version: Version,
    /// The body the client framed has nothing in it.
    nothing_to_send: bool,
    /// Whether a server listens: what nobody will write is not kept.
    listened: bool,
    /// Made when the exchange begins, and knows what went upstream; or, for a request no
    /// exchange of ours carries, when its body is first asked for.
    coordinator: Option<Coordinator>,
    /// Interim answers to pass on, in the order they came, their hop-by-hop fields off.
    forwarded: VecDeque<(StatusCode, HeaderMap)>,
}

impl Interim {
    /// For a request read by our own server: whether its client asked to be told before
    /// sending its body, the version it spoke, and whether the body it framed is nothing.
    pub(crate) fn listened(client: bool, version: Version, nothing_to_send: bool) -> Self {
        Self::new(client, version, nothing_to_send, true)
    }

    /// For an exchange whose request nobody listens for informational answers on.
    pub(crate) fn unheard() -> Self {
        Self::new(false, Version::HTTP_11, false, false)
    }

    fn new(client: bool, version: Version, nothing_to_send: bool, listened: bool) -> Self {
        Self(Rc::new(RefCell::new(State {
            client,
            version,
            nothing_to_send,
            listened,
            coordinator: None,
            forwarded: VecDeque::new(),
        })))
    }

    // What the exchange tells it.

    /// The exchange begins: whether the request going upstream, filters and all, carries
    /// `Expect: 100-continue`, and whether its body is framed as nothing.
    pub(crate) fn begin(&self, upstream: bool, nothing_to_send: bool) {
        let mut state = self.0.borrow_mut();
        let expectation = Expectation {
            client: state.client,
            version: state.version,
            upstream,
            nothing_to_send,
        };
        state.coordinator = Some(Coordinator::new(expectation));
    }

    /// The last byte of the request head has been taken by the socket. Says whether to arm
    /// the continue wait.
    pub(crate) fn head_sent(&self) -> bool {
        self.with(Coordinator::head_sent)
    }

    /// The continue wait ran out.
    pub(crate) fn wait_expired(&self) {
        self.with(Coordinator::wait_expired);
    }

    /// An interim answer from upstream, in wire order. Kept to be passed on, less its
    /// hop-by-hop fields, if it is the client's to hear and a server listens; consumed
    /// otherwise.
    pub(crate) fn upstream_interim(&self, status: StatusCode, mut headers: HeaderMap) {
        let relay = self.with(|coordinator| coordinator.upstream_interim(status));
        let mut state = self.0.borrow_mut();
        if relay == Relay::Forward && state.listened {
            crate::hop_by_hop::strip_response(&mut headers);
            state.forwarded.push_back((status, headers));
        }
    }

    /// The final answer, in wire order.
    pub(crate) fn final_head(&self) {
        self.with(Coordinator::final_head);
    }

    /// Whether the upload may be polled now.
    pub(crate) fn may_poll_upload(&self) -> bool {
        self.with(|coordinator| coordinator.may_poll_upload())
    }

    /// Whether the upload was abandoned before it started: the answer came instead of the
    /// upstream's leave to send it.
    pub(crate) fn abandoned(&self) -> bool {
        self.with(|coordinator| coordinator.abandoned())
    }

    // What the server tells it and asks of it.

    /// The body has been asked for and nothing of it is here. A client still waiting for
    /// leave to send it is given it, unless an upstream expectation is still to be settled.
    pub(crate) fn body_wanted(&self) {
        self.with(Coordinator::ready_for_body);
    }

    /// The client sent body bytes without waiting to be told.
    pub(crate) fn client_sent_body(&self) {
        self.with(Coordinator::client_sent_body);
    }

    /// The next interim answer to pass on, if one is waiting.
    pub(crate) fn next_forwarded(&self) -> Option<(StatusCode, HeaderMap)> {
        self.0.borrow_mut().forwarded.pop_front()
    }

    /// A local `100` to write, if one is wanted. At most one per request.
    pub(crate) fn take_local_continue(&self) -> bool {
        self.with(Coordinator::take_local_continue)
    }

    /// The coordinator, made now if no exchange of ours began one: what goes upstream is
    /// then not ours to know, and nothing holds the body back.
    fn with<T>(&self, act: impl FnOnce(&mut Coordinator) -> T) -> T {
        let mut state = self.0.borrow_mut();
        let state = &mut *state;
        let coordinator = state.coordinator.get_or_insert_with(|| {
            Coordinator::new(Expectation {
                client: state.client,
                version: state.version,
                upstream: false,
                nothing_to_send: state.nothing_to_send,
            })
        });
        act(coordinator)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderValue;

    fn fields(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut fields = HeaderMap::new();
        for (name, value) in pairs {
            fields.append(*name, HeaderValue::from_static(value));
        }
        fields
    }

    fn early_hints() -> HeaderMap {
        fields(&[("link", "</style.css>; rel=preload")])
    }

    /// Interim answers are passed on in the order they came, each less its hop-by-hop
    /// fields and those its `Connection` named; nothing after the final answer is.
    #[test]
    fn interim_answers_are_passed_on_in_order_without_their_hop_fields() {
        let interim = Interim::listened(false, Version::HTTP_11, false);
        interim.begin(false, false);
        interim.upstream_interim(
            StatusCode::from_u16(103).unwrap(),
            fields(&[
                ("link", "</a.css>; rel=preload"),
                ("connection", "x-hop"),
                ("x-hop", "1"),
                ("keep-alive", "timeout=5"),
            ]),
        );
        interim.upstream_interim(StatusCode::PROCESSING, HeaderMap::new());
        interim.final_head();
        interim.upstream_interim(StatusCode::from_u16(103).unwrap(), early_hints());

        let (status, first) = interim.next_forwarded().unwrap();
        assert_eq!(status.as_u16(), 103);
        assert_eq!(first, fields(&[("link", "</a.css>; rel=preload")]));
        let (status, _) = interim.next_forwarded().unwrap();
        assert_eq!(status, StatusCode::PROCESSING);
        assert!(
            interim.next_forwarded().is_none(),
            "passed on after the final"
        );
    }

    /// An HTTP/1.0 client is never sent one (RFC 9110 §15.2), and nothing is kept for a
    /// request nobody listens on.
    #[test]
    fn nothing_is_kept_for_a_client_that_cannot_hear_it_or_nobody_listening() {
        let old = Interim::listened(false, Version::HTTP_10, false);
        old.begin(false, false);
        old.upstream_interim(StatusCode::from_u16(103).unwrap(), early_hints());
        assert!(old.next_forwarded().is_none());

        let unheard = Interim::unheard();
        unheard.begin(false, false);
        unheard.upstream_interim(StatusCode::from_u16(103).unwrap(), early_hints());
        assert!(unheard.next_forwarded().is_none());
    }

    /// The client's expectation, forwarded: the upstream's `100` is what releases the
    /// upload and is passed on, and no local one is made.
    #[test]
    fn an_upstream_continue_is_passed_on_and_releases_the_upload() {
        let interim = Interim::listened(true, Version::HTTP_11, false);
        interim.begin(true, false);
        assert!(!interim.may_poll_upload());
        assert!(interim.head_sent(), "the wait was not armed");
        interim.upstream_interim(StatusCode::CONTINUE, HeaderMap::new());
        assert!(interim.may_poll_upload());
        assert_eq!(interim.next_forwarded().unwrap().0, StatusCode::CONTINUE);
        interim.body_wanted();
        assert!(!interim.take_local_continue());
    }

    /// The wait runs out first: the upload starts, and one local `100` goes to the client,
    /// the upstream's later one passed on as well.
    #[test]
    fn the_wait_running_out_releases_the_upload_and_makes_one_local_continue() {
        let interim = Interim::listened(true, Version::HTTP_11, false);
        interim.begin(true, false);
        assert!(interim.head_sent());
        interim.wait_expired();
        assert!(interim.may_poll_upload());
        assert!(interim.take_local_continue());
        assert!(!interim.take_local_continue(), "a second local 100");
        interim.upstream_interim(StatusCode::CONTINUE, HeaderMap::new());
        assert_eq!(interim.next_forwarded().unwrap().0, StatusCode::CONTINUE);
    }

    /// A final answer while the upload is held abandons it, and cancels a local `100` not
    /// yet taken.
    #[test]
    fn a_final_answer_while_held_abandons_the_upload() {
        let interim = Interim::listened(true, Version::HTTP_11, false);
        interim.begin(true, false);
        assert!(interim.head_sent());
        interim.final_head();
        assert!(interim.abandoned());
        interim.wait_expired();
        assert!(!interim.take_local_continue());
    }

    /// A request no exchange of ours carries: nothing holds the body back, and a client
    /// waiting for leave is given it when its body is first asked for, as the engine's
    /// server does. One that sends without waiting is sent nothing.
    #[test]
    fn with_no_exchange_of_ours_the_client_is_released_when_its_body_is_asked_for() {
        let asked = Interim::listened(true, Version::HTTP_11, false);
        assert!(asked.may_poll_upload());
        asked.body_wanted();
        assert!(asked.take_local_continue());

        let sent = Interim::listened(true, Version::HTTP_11, false);
        sent.client_sent_body();
        sent.body_wanted();
        assert!(!sent.take_local_continue());

        let nothing = Interim::listened(true, Version::HTTP_11, true);
        nothing.body_wanted();
        assert!(!nothing.take_local_continue(), "leave to send nothing");
    }

    /// An expectation the gateway added on the way, by a filter, is its own: the `100` it
    /// brings releases the upload and is not passed on (RFC 9110 §15.2).
    #[test]
    fn a_continue_the_gateway_asked_for_is_not_passed_on() {
        let interim = Interim::listened(false, Version::HTTP_11, false);
        interim.begin(true, false);
        interim.upstream_interim(StatusCode::CONTINUE, HeaderMap::new());
        assert!(interim.may_poll_upload());
        assert!(interim.next_forwarded().is_none());
    }
}
