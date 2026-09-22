//! One exchange's `Expect: 100-continue`: when the upload may start, whether the continue
//! wait runs, and which `100 Continue` the client is sent.
//!
//! One coordinator per exchange owns the decision and its timer, so that nothing polling a
//! body makes a second, independent continue policy
//! ([14 §5](../../../../docs/14-downstream-server.md)). It is told what happened, in wire
//! order, and asked what to do; it keeps no clock and writes nothing. The timer is the
//! caller's, armed when this says so and reported when it runs out.
//!
//! The policy: the client's expectation is forwarded unless a filter changed it; an
//! upstream `100` is relayed promptly; if the wait runs out first, the upload starts and
//! one local `100` is sent to a client still waiting for permission. Other interim answers
//! never release the wait, and a final answer ends it without a `100` being made up.

use http::{StatusCode, Version};

/// What is known about an exchange's expectation before any of it has gone upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Expectation {
    /// The client sent `Expect: 100-continue`, read before any filter touched the head.
    pub client: bool,
    /// The version the client spoke, which decides whether it can be sent a 1xx at all.
    pub version: Version,
    /// The request going upstream carries `Expect: 100-continue`, after filters.
    pub upstream: bool,
    /// The body is framed as having nothing in it: no body, or a length of nothing.
    /// There is nothing to hold back, and nothing to ask permission for
    /// ([RFC 9110 §10.1.1](https://www.rfc-editor.org/rfc/rfc9110.html#section-10.1.1)).
    pub nothing_to_send: bool,
}

/// What to do with an interim answer from upstream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Relay {
    /// Pass it on to the client, in the order it came.
    Forward,
    /// Consume it: the client cannot be sent one, or the gateway asked for it itself.
    Consume,
}

/// Where the upload stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Upload {
    /// Waiting for the upstream's word or the end of the wait.
    Held,
    /// May be polled.
    Permitted,
    /// A final answer came while it was held: it never starts.
    Abandoned,
}

/// Where the local `100` stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Local {
    /// None is wanted, or yet wanted.
    None,
    /// One is wanted and has not yet been handed to the writer.
    Queued,
    /// It was handed to the writer: from there it is the writer's to finish or close on.
    Taken,
}

/// The continue decision of one exchange.
#[derive(Debug, Clone)]
pub struct Coordinator {
    expectation: Expectation,
    upload: Upload,
    /// The client is still waiting for permission to send its body.
    client_waiting: bool,
    /// The continue wait has been armed and has not ended.
    waiting: bool,
    /// A final head has been seen: nothing interim goes to the client after it.
    finished: bool,
    local: Local,
}

impl Coordinator {
    /// The decision for an exchange whose expectation is `expectation`.
    pub fn new(expectation: Expectation) -> Self {
        let can_hear = expectation.version == Version::HTTP_11;
        // Held only where the upstream was asked to say yes first and there is something
        // to hold back; otherwise the body goes as soon as the exchange wants it.
        let held = expectation.upstream && !expectation.nothing_to_send;
        Self {
            expectation,
            upload: if held {
                Upload::Held
            } else {
                Upload::Permitted
            },
            client_waiting: expectation.client && can_hear && !expectation.nothing_to_send,
            waiting: false,
            finished: false,
            local: Local::None,
        }
    }

    /// The last byte of the upstream request head has been accepted by the socket: the
    /// moment the continue wait starts, and not before (13 §7).
    ///
    /// Says whether to arm the wait.
    pub fn head_sent(&mut self) -> bool {
        if self.upload == Upload::Held && !self.waiting && !self.finished {
            self.waiting = true;
            return true;
        }
        false
    }

    /// The exchange is ready to poll the body, with no upstream expectation to wait on —
    /// a filter took it away, or there never was one. A client still waiting for
    /// permission is given it now.
    pub fn ready_for_body(&mut self) {
        if self.upload == Upload::Permitted && !self.finished {
            self.release_client();
        }
    }

    /// An interim answer from upstream, in wire order. Says what to do with it.
    pub fn upstream_interim(&mut self, status: StatusCode) -> Relay {
        if self.finished || status == StatusCode::SWITCHING_PROTOCOLS {
            return Relay::Consume;
        }
        if status == StatusCode::CONTINUE {
            // Only a 100 says to send the body; saying something else is not saying yes.
            self.waiting = false;
            if self.upload == Upload::Held {
                self.upload = Upload::Permitted;
            }
            // A 100 the gateway itself asked for is not the client's to hear
            // (RFC 9110 §15.2's exception for a proxy that requested the 1xx).
            let gateway_asked = self.expectation.upstream && !self.expectation.client;
            if gateway_asked {
                return Relay::Consume;
            }
            // The client's permission, from the one that could give it.
            self.client_waiting = false;
            if self.local == Local::Queued {
                self.local = Local::None;
            }
        }
        if self.expectation.version == Version::HTTP_11 {
            Relay::Forward
        } else {
            Relay::Consume
        }
    }

    /// The continue wait ran out. The upload starts, and a client still waiting for
    /// permission is given it locally — both from this one transition.
    pub fn wait_expired(&mut self) {
        if !self.waiting {
            return;
        }
        self.waiting = false;
        if self.upload == Upload::Held {
            self.upload = Upload::Permitted;
            self.release_client();
        }
    }

    /// The client sent body bytes without waiting to be told: it needs no permission any
    /// more, and a `100` would tell it nothing.
    pub fn client_sent_body(&mut self) {
        self.client_waiting = false;
        if self.local == Local::Queued {
            self.local = Local::None;
        }
    }

    /// A final head, in wire order. It ends the wait, abandons an upload still held back,
    /// and cancels a local `100` not yet handed to the writer. One already handed over is
    /// the writer's to finish.
    pub fn final_head(&mut self) {
        self.finished = true;
        self.waiting = false;
        self.client_waiting = false;
        if self.upload == Upload::Held {
            self.upload = Upload::Abandoned;
        }
        if self.local == Local::Queued {
            self.local = Local::None;
        }
    }

    /// Whether the body may be polled now.
    pub fn may_poll_upload(&self) -> bool {
        self.upload == Upload::Permitted
    }

    /// Whether the upload was abandoned before it started.
    pub fn abandoned(&self) -> bool {
        self.upload == Upload::Abandoned
    }

    /// Whether the continue wait is running.
    pub fn waiting(&self) -> bool {
        self.waiting
    }

    /// Takes a local `100` to write, if one is wanted. At most one per exchange, ever.
    pub fn take_local_continue(&mut self) -> bool {
        if self.local == Local::Queued {
            self.local = Local::Taken;
            return true;
        }
        false
    }

    fn release_client(&mut self) {
        if self.client_waiting && self.local == Local::None {
            self.local = Local::Queued;
        }
        self.client_waiting = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const BOTH: Expectation = Expectation {
        client: true,
        version: Version::HTTP_11,
        upstream: true,
        nothing_to_send: false,
    };

    fn early_hints() -> StatusCode {
        StatusCode::from_u16(103).unwrap()
    }

    #[test]
    fn the_body_waits_for_the_upstreams_word_and_its_100_is_relayed() {
        let mut exchange = Coordinator::new(BOTH);
        assert!(!exchange.may_poll_upload(), "held from the start");
        assert!(exchange.head_sent(), "the wait starts with the head");
        assert!(!exchange.may_poll_upload());
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Forward
        );
        assert!(exchange.may_poll_upload());
        assert!(!exchange.waiting());
        // The client had the upstream's own word, so the wait running out later says
        // nothing more.
        exchange.wait_expired();
        assert!(!exchange.take_local_continue());
    }

    #[test]
    fn the_wait_running_out_starts_the_upload_and_sends_one_local_100() {
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.wait_expired();
        assert!(exchange.may_poll_upload());
        assert!(exchange.take_local_continue());
        assert!(!exchange.take_local_continue(), "one, and only one");
        // A later upstream 100 is still forwarded: the client may hear it twice, which
        // is allowed; hearing none of the upstream's would not be.
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Forward
        );
        assert!(!exchange.take_local_continue());
    }

    #[test]
    fn nothing_but_a_100_releases_the_body() {
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        assert_eq!(exchange.upstream_interim(early_hints()), Relay::Forward);
        assert!(!exchange.may_poll_upload());
        assert!(exchange.waiting());
    }

    /// An early final answer ends the wait and the upload never starts; no 100 is made up.
    #[test]
    fn a_final_answer_while_waiting_abandons_the_upload() {
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.final_head();
        assert!(exchange.abandoned());
        assert!(!exchange.may_poll_upload());
        assert!(!exchange.waiting());
        exchange.wait_expired();
        assert!(!exchange.take_local_continue());
        assert!(!exchange.may_poll_upload());
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Consume
        );
    }

    /// The same poll can bring the wait's end, an upstream 100 and a final head. The driver
    /// hands them over in wire order, and each order has its own answer.
    #[test]
    fn races_in_one_poll_are_settled_in_wire_order() {
        // Expiry first, then the upstream's 100 before the local one was handed over: the
        // local one is dropped, because the client is about to hear the upstream's own.
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.wait_expired();
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Forward
        );
        assert!(
            !exchange.take_local_continue(),
            "the upstream's own 100 replaced it"
        );
        assert!(exchange.may_poll_upload());

        // Expiry, then a final head before the local 100 was handed over: dropped.
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.wait_expired();
        exchange.final_head();
        assert!(!exchange.take_local_continue());
        assert!(
            exchange.may_poll_upload(),
            "it had started; a final head does not un-start it"
        );

        // Expiry, the local 100 handed over, then a final head: the writer finishes it.
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.wait_expired();
        assert!(exchange.take_local_continue());
        exchange.final_head();
        assert!(!exchange.take_local_continue());

        // A final head first: nothing after it.
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.final_head();
        exchange.wait_expired();
        assert!(!exchange.take_local_continue());
        assert!(exchange.abandoned());
    }

    /// A filter that took the expectation away leaves no upstream wait: the client is given
    /// permission when the exchange is ready for its body.
    #[test]
    fn an_expectation_a_filter_removed_is_satisfied_here() {
        let mut exchange = Coordinator::new(Expectation {
            upstream: false,
            ..BOTH
        });
        assert!(exchange.may_poll_upload());
        assert!(!exchange.head_sent(), "no wait to arm");
        assert!(
            !exchange.take_local_continue(),
            "not before the exchange wants the body"
        );
        exchange.ready_for_body();
        assert!(exchange.take_local_continue());
        exchange.ready_for_body();
        assert!(!exchange.take_local_continue());
    }

    /// An expectation the gateway added: the body waits for the upstream, but the 100 the
    /// gateway asked for is not relayed, and the client, who asked nothing, is sent nothing.
    #[test]
    fn an_expectation_a_filter_added_is_the_gateways_own() {
        let mut exchange = Coordinator::new(Expectation {
            client: false,
            ..BOTH
        });
        assert!(!exchange.may_poll_upload());
        assert!(exchange.head_sent());
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Consume
        );
        assert!(exchange.may_poll_upload());
        assert_eq!(exchange.upstream_interim(early_hints()), Relay::Forward);

        let mut exchange = Coordinator::new(Expectation {
            client: false,
            ..BOTH
        });
        exchange.head_sent();
        exchange.wait_expired();
        assert!(exchange.may_poll_upload());
        assert!(!exchange.take_local_continue());
    }

    #[test]
    fn a_client_that_sent_its_body_anyway_is_not_told_it_may() {
        let mut exchange = Coordinator::new(BOTH);
        exchange.head_sent();
        exchange.client_sent_body();
        exchange.wait_expired();
        assert!(exchange.may_poll_upload());
        assert!(!exchange.take_local_continue());
    }

    /// Nothing to send is nothing to hold back and nothing to ask permission for.
    #[test]
    fn an_expectation_on_an_empty_body_holds_nothing_back() {
        let mut exchange = Coordinator::new(Expectation {
            nothing_to_send: true,
            ..BOTH
        });
        assert!(exchange.may_poll_upload());
        assert!(!exchange.head_sent());
        exchange.ready_for_body();
        assert!(!exchange.take_local_continue());
        // The upstream's 100 is still the client's to hear: it asked.
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Forward
        );
    }

    #[test]
    fn an_http_1_0_client_hears_no_interim_answer_and_is_sent_no_100() {
        let ten = Expectation {
            version: Version::HTTP_10,
            ..BOTH
        };
        let mut exchange = Coordinator::new(ten);
        exchange.head_sent();
        assert_eq!(exchange.upstream_interim(early_hints()), Relay::Consume);
        exchange.wait_expired();
        assert!(exchange.may_poll_upload());
        assert!(!exchange.take_local_continue());
        let mut exchange = Coordinator::new(ten);
        exchange.head_sent();
        assert_eq!(
            exchange.upstream_interim(StatusCode::CONTINUE),
            Relay::Consume
        );
        assert!(exchange.may_poll_upload());
    }

    #[test]
    fn no_expectation_is_no_wait_and_no_100() {
        let mut exchange = Coordinator::new(Expectation {
            client: false,
            upstream: false,
            ..BOTH
        });
        assert!(exchange.may_poll_upload());
        assert!(!exchange.head_sent());
        exchange.ready_for_body();
        assert!(!exchange.take_local_continue());
        assert_eq!(
            exchange.upstream_interim(StatusCode::SWITCHING_PROTOCOLS),
            Relay::Consume
        );
    }

    #[derive(Debug, Clone, Copy)]
    enum Event {
        HeadSent,
        ReadyForBody,
        Interim(u16),
        Expired,
        ClientSentBody,
        Final,
        Take,
    }

    fn event() -> impl Strategy<Value = Event> {
        prop_oneof![
            Just(Event::HeadSent),
            Just(Event::ReadyForBody),
            prop::sample::select(vec![100_u16, 101, 102, 103]).prop_map(Event::Interim),
            Just(Event::Expired),
            Just(Event::ClientSentBody),
            Just(Event::Final),
            Just(Event::Take),
        ]
    }

    fn expectation() -> impl Strategy<Value = Expectation> {
        (any::<bool>(), any::<bool>(), any::<bool>(), any::<bool>()).prop_map(
            |(client, old, upstream, nothing_to_send)| Expectation {
                client,
                version: if old {
                    Version::HTTP_10
                } else {
                    Version::HTTP_11
                },
                upstream,
                nothing_to_send,
            },
        )
    }

    proptest! {
        /// Whatever happens, in whatever order: at most one local 100, none to a client
        /// that did not ask or cannot hear one, none after a final head; no 1xx forwarded
        /// to a 1.0 client or after a final head; an upload once permitted stays so, one
        /// abandoned never starts, and the wait runs only for an upstream expectation.
        #[test]
        fn the_decision_keeps_its_promises(
            expectation in expectation(),
            events in prop::collection::vec(event(), 0..24),
        ) {
            let mut exchange = Coordinator::new(expectation);
            let mut locals = 0;
            let mut finished = false;
            let mut permitted = exchange.may_poll_upload();
            for event in events {
                match event {
                    Event::HeadSent => {
                        let armed = exchange.head_sent();
                        prop_assert!(!armed || expectation.upstream);
                    }
                    Event::ReadyForBody => exchange.ready_for_body(),
                    Event::Interim(code) => {
                        let relay = exchange.upstream_interim(StatusCode::from_u16(code).unwrap());
                        if relay == Relay::Forward {
                            prop_assert!(!finished);
                            prop_assert_eq!(expectation.version, Version::HTTP_11);
                            prop_assert_ne!(code, 101);
                        }
                    }
                    Event::Expired => exchange.wait_expired(),
                    Event::ClientSentBody => exchange.client_sent_body(),
                    Event::Final => {
                        exchange.final_head();
                        finished = true;
                    }
                    Event::Take => {
                        if exchange.take_local_continue() {
                            locals += 1;
                            prop_assert!(!finished);
                            prop_assert!(expectation.client);
                            prop_assert_eq!(expectation.version, Version::HTTP_11);
                            prop_assert!(!expectation.nothing_to_send);
                        }
                    }
                }
                prop_assert!(locals <= 1);
                if permitted {
                    prop_assert!(exchange.may_poll_upload(), "a permitted upload stopped");
                }
                permitted = exchange.may_poll_upload();
                if exchange.abandoned() {
                    prop_assert!(!exchange.may_poll_upload());
                }
                if exchange.waiting() {
                    prop_assert!(expectation.upstream && !finished);
                }
            }
        }
    }
}
