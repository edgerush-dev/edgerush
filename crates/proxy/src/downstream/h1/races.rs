//! The continue coordinator, the writer and the commitment tracker, driven together against
//! a scripted socket, as the connection driver will drive them (14 §4, §5).
//!
//! Each is tested alone where it is written. What only shows when they are composed is the
//! race: a local `100` already part-written when the final head arrives, an upstream
//! failing while an interim head is on its way, a socket taking a few bytes at a time. What
//! the client receives must still read as one answer — interim heads, then a final head
//! and its body — and never a status line spliced into an unfinished head.

use super::continuing::{Coordinator, Expectation, Relay};
use super::date::HttpDate;
use super::outbound::{OnFailure, Outbound};
use super::writer::{Asked, Content, write_head, write_interim};
use crate::upstream::h1::reference::{self, Reading};
use http::{HeaderMap, StatusCode, Version};
use proptest::prelude::*;

/// What happens, in wire order.
#[derive(Debug, Clone, Copy)]
enum Step {
    /// The upstream request head has gone.
    HeadSent,
    /// The continue wait ran out.
    Expired,
    /// The upstream sent this interim status.
    Interim(u16),
    /// The upstream sent its final head, and a two-byte body with it.
    Final(u16),
    /// The upstream failed before a final head of its own.
    Failed,
    /// The client sent body bytes without waiting.
    ClientSentBody,
    /// The socket took up to this many bytes.
    Socket(usize),
    /// A write of everything queued waited, as a TLS write does whose record is sealed:
    /// the socket sends what it was offered before anything, whatever it is offered next.
    Waits,
}

/// A driver of the three pieces, reduced to what the race is about.
struct Driver {
    coordinator: Coordinator,
    outbound: Outbound,
    asked: Asked,
    /// Written and not yet accepted by the socket.
    queued: Vec<u8>,
    /// What the client has received.
    wire: Vec<u8>,
    /// Local `100`s written.
    locals: usize,
    /// Upstream `100`s forwarded.
    relayed_continues: usize,
    /// The final status that was written, the upstream's or a local one.
    final_status: Option<u16>,
    /// The connection was closed rather than answered.
    closed: bool,
    /// The upstream failed, and what had not gone of its interim heads was dropped.
    failed: bool,
    /// What a write that waited offered, which the socket owes.
    sealed: Vec<u8>,
}

impl Driver {
    fn new(expectation: Expectation) -> Self {
        Self {
            coordinator: Coordinator::new(expectation),
            outbound: Outbound::default(),
            asked: Asked {
                head: false,
                version: expectation.version,
                trailers: false,
            },
            queued: Vec::new(),
            wire: Vec::new(),
            locals: 0,
            relayed_continues: 0,
            final_status: None,
            closed: false,
            failed: false,
            sealed: Vec::new(),
        }
    }

    fn interim(&mut self, status: u16) {
        let before = self.queued.len();
        let status = StatusCode::from_u16(status).unwrap();
        write_interim(&mut self.queued, status, &HeaderMap::new(), self.asked).unwrap();
        self.outbound.interim(self.queued.len() - before).unwrap();
    }

    fn final_head(&mut self, status: u16) {
        let before = self.queued.len();
        let status = StatusCode::from_u16(status).unwrap();
        let date = HttpDate::from_unix(0);
        let content = Content::Length(2);
        write_head(
            &mut self.queued,
            status,
            &HeaderMap::new(),
            content,
            self.asked,
            true,
            &date,
        )
        .unwrap();
        self.outbound
            .final_head(self.queued.len() - before)
            .unwrap();
        self.queued.extend_from_slice(b"ok");
        self.final_status = Some(status.as_u16());
    }

    /// The writing half of a turn: what the events before it settled is written now, a
    /// local 100 among it, and not before — so an expiry and a final head can arrive in one
    /// turn, as they can on a real connection.
    fn local_continue(&mut self) {
        if self.coordinator.take_local_continue() {
            self.locals += 1;
            self.interim(100);
        }
    }

    fn step(&mut self, step: Step) {
        if self.closed
            || (self.final_status.is_some() && !matches!(step, Step::Socket(_) | Step::Waits))
        {
            return;
        }
        match step {
            Step::HeadSent => {
                let _armed = self.coordinator.head_sent();
            }
            Step::Expired => self.coordinator.wait_expired(),
            Step::ClientSentBody => self.coordinator.client_sent_body(),
            Step::Interim(status) => {
                let status_code = StatusCode::from_u16(status).unwrap();
                if self.coordinator.upstream_interim(status_code) == Relay::Forward {
                    if status == 100 {
                        self.relayed_continues += 1;
                    }
                    self.interim(status);
                }
            }
            Step::Final(status) => {
                self.coordinator.final_head();
                self.final_head(status);
            }
            Step::Failed => {
                self.failed = true;
                self.coordinator.final_head();
                match self.outbound.on_failure() {
                    OnFailure::Answer => {
                        // Nothing is part-written: what has not gone never will.
                        self.queued.clear();
                        self.outbound = Outbound::default();
                        self.final_head(502);
                    }
                    OnFailure::FinishThenAnswer(left) => {
                        // The rest of the head that has begun, and nothing after it.
                        self.queued.truncate(left);
                        self.outbound = Outbound::default();
                        self.outbound.interim(left).unwrap();
                        self.final_head(502);
                    }
                    OnFailure::Close => self.closed = true,
                }
            }
            Step::Waits => {
                self.local_continue();
                if self.sealed.is_empty() && !self.queued.is_empty() {
                    self.sealed = self.queued.clone();
                    self.outbound.pending();
                }
            }
            Step::Socket(most) => {
                self.local_continue();
                if !self.sealed.is_empty() {
                    // What is offered now finishes the sealed write: the socket sends what
                    // it sealed and says that many bytes of what it is offered went. TLS
                    // refuses an offer shorter than what it holds, and the connection ends.
                    let sealed = std::mem::take(&mut self.sealed);
                    if self.queued.len() < sealed.len() {
                        self.closed = true;
                        return;
                    }
                    self.wire.extend(sealed.iter());
                    self.queued.drain(..sealed.len());
                    self.outbound.accepted(sealed.len());
                    return;
                }
                let taken = most.min(self.queued.len());
                self.wire.extend(self.queued.drain(..taken));
                self.outbound.accepted(taken);
            }
        }
    }
}

fn step() -> impl Strategy<Value = Step> {
    prop_oneof![
        Just(Step::HeadSent),
        Just(Step::Expired),
        prop::sample::select(vec![100_u16, 102, 103]).prop_map(Step::Interim),
        prop::sample::select(vec![200_u16, 404, 413]).prop_map(Step::Final),
        Just(Step::Failed),
        Just(Step::ClientSentBody),
        (0_usize..40).prop_map(Step::Socket),
        (0_usize..40).prop_map(Step::Socket),
        Just(Step::Waits),
    ]
}

fn expectation() -> impl Strategy<Value = Expectation> {
    (
        any::<bool>(),
        prop::bool::weighted(0.2),
        any::<bool>(),
        any::<bool>(),
    )
        .prop_map(|(client, old, upstream, nothing_to_send)| Expectation {
            client,
            version: if old {
                Version::HTTP_10
            } else {
                Version::HTTP_11
            },
            upstream,
            nothing_to_send,
        })
}

/// The race §4 names, by hand: a local `100` part-written when the final head arrives is
/// finished, and the final head follows it whole.
#[test]
fn a_part_written_local_100_is_finished_before_the_final_head() {
    let mut driver = Driver::new(Expectation {
        client: true,
        version: Version::HTTP_11,
        upstream: true,
        nothing_to_send: false,
    });
    driver.step(Step::HeadSent);
    driver.step(Step::Expired);
    driver.step(Step::Socket(7));
    assert_eq!(driver.locals, 1);
    driver.step(Step::Final(200));
    driver.step(Step::Socket(usize::MAX));
    let text = String::from_utf8(driver.wire).unwrap();
    assert!(
        text.starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 200 OK\r\n"),
        "{text}"
    );
}

/// And an upstream that fails while that `100` is part-written: the `100` is finished
/// before the local answer, never cut short by it.
#[test]
fn a_failure_mid_100_finishes_it_before_answering() {
    let mut driver = Driver::new(Expectation {
        client: true,
        version: Version::HTTP_11,
        upstream: true,
        nothing_to_send: false,
    });
    driver.step(Step::HeadSent);
    driver.step(Step::Expired);
    driver.step(Step::Socket(5));
    driver.step(Step::Failed);
    driver.step(Step::Socket(usize::MAX));
    let text = String::from_utf8(driver.wire).unwrap();
    assert!(
        text.starts_with("HTTP/1.1 100 Continue\r\n\r\nHTTP/1.1 502 Bad Gateway\r\n"),
        "{text}"
    );
}

/// Two interim heads in a write that waited are sealed together: an upstream that fails
/// then has both finished before its 502, as the socket owes them (A03-03, C32).
#[test]
fn interim_heads_sealed_together_are_both_finished_before_answering() {
    let mut driver = Driver::new(Expectation {
        client: false,
        version: Version::HTTP_11,
        upstream: true,
        nothing_to_send: false,
    });
    driver.step(Step::HeadSent);
    driver.step(Step::Interim(103));
    driver.step(Step::Interim(102));
    driver.step(Step::Waits);
    driver.step(Step::Failed);
    driver.step(Step::Socket(usize::MAX));
    driver.step(Step::Socket(usize::MAX));
    let text = String::from_utf8(driver.wire).unwrap();
    assert!(
        text.starts_with(
            "HTTP/1.1 103 Early Hints\r\n\r\nHTTP/1.1 102 Processing\r\n\r\nHTTP/1.1 502 Bad Gateway\r\n"
        ),
        "{text}"
    );
}

proptest! {
    #[test]
    fn what_the_client_receives_is_always_one_answer(
        expectation in expectation(),
        steps in prop::collection::vec(step(), 0..24),
    ) {
        let mut driver = Driver::new(expectation);
        for step in steps {
            driver.step(step);
        }
        // Whatever was left is written, and an exchange with no answer yet is given one
        // by the upstream.
        driver.step(Step::Final(200));
        // A sealed write finished first, then the rest.
        driver.step(Step::Socket(usize::MAX));
        driver.step(Step::Socket(usize::MAX));

        prop_assert!(driver.locals <= 1, "{} local 100s", driver.locals);
        if expectation.version == Version::HTTP_10 {
            prop_assert_eq!(driver.locals, 0);
        }
        if driver.closed {
            // Closed after the final head began: what went is the upstream's, cut short.
            return Ok(());
        }
        let Reading::Read(answer) = reference::read(&driver.wire, reference::Asked::Anything, true)
        else {
            prop_assert!(false, "not one answer: {:?}", String::from_utf8_lossy(&driver.wire));
            unreachable!();
        };
        prop_assert_eq!(Some(answer.status), driver.final_status);
        prop_assert_eq!(answer.body, b"ok".to_vec());
        let continues = answer.interim.iter().filter(|interim| interim.status == 100).count();
        // Every 100 written reached the client, unless a failure dropped heads that had not
        // begun to go — which 14 §4 allows — and then never more than were written.
        if driver.failed {
            prop_assert!(continues <= driver.locals + driver.relayed_continues);
        } else {
            prop_assert_eq!(continues, driver.locals + driver.relayed_continues);
        }
        if expectation.version == Version::HTTP_10 {
            prop_assert!(answer.interim.is_empty(), "an interim head to HTTP/1.0");
        }
    }
}
