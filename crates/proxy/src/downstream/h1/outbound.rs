//! Which heads of an answer have reached the client, and so what may still be done instead.
//!
//! The writer only appends bytes; the connection driver writes them and reports how many
//! the socket accepted. This tracks the heads among those bytes — informational heads,
//! then the final head — and answers the two questions a failure asks: can the final head
//! still be replaced, and if a local answer is wanted instead, may it be written now
//! ([14 §4](../../../../docs/14-downstream-server.md)).
//!
//! The final head is **committed** by its first byte the socket accepts, or by a write that
//! offered it and did not finish: TLS seals what a write offers and sends it however the
//! write ends, so a head left in a write that waits may be on its way, and over every
//! transport it is taken to be. Until then a final head that is wholly queued can be
//! replaced. An informational head does not commit anything, but one begun — part-written,
//! or offered — must be finished before any other head follows it: a new status line in the
//! middle of a head is two messages spliced into one.

use std::collections::VecDeque;

/// Why a head cannot be queued.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OutboundError {
    /// A head after the final head, or a second final head.
    #[error("the final head has already been queued")]
    AfterFinal,
    /// A replacement for a final head that has begun to go.
    #[error("the final head has begun to go and cannot be replaced")]
    Committed,
    /// A replacement where no final head was queued.
    #[error("there is no final head to replace")]
    NoFinal,
}

/// What a failure before the answer is complete allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnFailure {
    /// Nothing is part-written: drop what has not gone and write a local answer now.
    Answer,
    /// Informational heads have begun to go: write the rest of them — this many bytes, the
    /// pieces at the front of the queue — within the existing write deadline, then the
    /// local answer, or close.
    FinishThenAnswer(usize),
    /// The final head has begun to go: the answer is the upstream's, and all that is left
    /// is to end the connection without a clean end.
    Close,
}

/// A head among the queued bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Interim,
    Final,
}

/// The heads of one answer on their way to the socket.
#[derive(Debug, Default)]
pub struct Outbound {
    /// Heads not yet wholly accepted, in the order they were queued, and their lengths.
    /// At most the interim bound and one final head, which the caller's own limits keep.
    heads: VecDeque<(Kind, usize)>,
    /// How much of the front head has been accepted.
    front_sent: usize,
    /// How many heads at the front were offered in a write that did not finish, and so are
    /// begun whatever the socket has said of them.
    offered: usize,
    final_queued: bool,
    committed: bool,
}

impl Outbound {
    /// Queues an informational head of `length` bytes.
    ///
    /// # Errors
    ///
    /// The final head was already queued.
    pub fn interim(&mut self, length: usize) -> Result<(), OutboundError> {
        if self.final_queued {
            return Err(OutboundError::AfterFinal);
        }
        self.heads.push_back((Kind::Interim, length));
        Ok(())
    }

    /// Queues the final head, `length` bytes.
    ///
    /// # Errors
    ///
    /// A final head was already queued.
    pub fn final_head(&mut self, length: usize) -> Result<(), OutboundError> {
        if self.final_queued {
            return Err(OutboundError::AfterFinal);
        }
        self.final_queued = true;
        self.heads.push_back((Kind::Final, length));
        Ok(())
    }

    /// Where the final head is among the heads not yet wholly accepted, which are queued
    /// one piece each and in this order; `None` if it is not queued or has all gone.
    pub fn final_position(&self) -> Option<usize> {
        self.heads.iter().position(|&(kind, _)| kind == Kind::Final)
    }

    /// Whether the queued final head could still be replaced: not one byte of it gone.
    pub fn can_replace_final(&self) -> bool {
        self.final_queued && !self.committed
    }

    /// Replaces the queued final head with one of `length` bytes, which the caller writes
    /// where the old one began.
    ///
    /// # Errors
    ///
    /// There is no final head queued, or it has begun to go.
    pub fn replace_final(&mut self, length: usize) -> Result<(), OutboundError> {
        if !self.final_queued {
            return Err(OutboundError::NoFinal);
        }
        if self.committed {
            return Err(OutboundError::Committed);
        }
        if let Some(last) = self.heads.back_mut() {
            *last = (Kind::Final, length);
        }
        Ok(())
    }

    /// The socket accepted `count` more bytes. What follows the final head is body, and
    /// is not counted here.
    pub fn accepted(&mut self, mut count: usize) {
        while count > 0 {
            let Some(&(kind, length)) = self.heads.front() else {
                return;
            };
            if kind == Kind::Final {
                self.committed = true;
            }
            let left = length - self.front_sent;
            if count < left {
                self.front_sent += count;
                return;
            }
            count -= left;
            self.front_sent = 0;
            self.heads.pop_front();
            self.offered = self.offered.saturating_sub(1);
        }
    }

    /// A write of everything queued did not finish. The transport may hold it all the same,
    /// sealed — TLS does — and send it whatever is offered next: every head queued now is
    /// begun, the final head committed.
    pub fn pending(&mut self) {
        self.offered = self.heads.len();
        if self.heads.iter().any(|&(kind, _)| kind == Kind::Final) {
            self.committed = true;
        }
    }

    /// Ready for the next answer on the connection, as a new one would be, keeping the
    /// room the last one made: a new queue for every answer is an allocation for every
    /// answer.
    pub fn reset(&mut self) {
        self.heads.clear();
        self.front_sent = 0;
        self.offered = 0;
        self.final_queued = false;
        self.committed = false;
    }

    /// Whether the final head has begun to go.
    pub fn committed(&self) -> bool {
        self.committed
    }

    /// Whether every queued head has gone whole.
    pub fn heads_sent(&self) -> bool {
        self.heads.is_empty()
    }

    /// What a failure now allows.
    pub fn on_failure(&self) -> OnFailure {
        if self.committed {
            return OnFailure::Close;
        }
        // Every head offered in a write that waited, or the front one part-written: all
        // informational, a final head among them having committed the answer.
        let begun = if self.offered > 0 {
            self.offered
        } else {
            usize::from(self.front_sent > 0)
        };
        if begun == 0 {
            return OnFailure::Answer;
        }
        let left: usize = self
            .heads
            .iter()
            .take(begun)
            .map(|&(_, length)| length)
            .sum();
        OnFailure::FinishThenAnswer(left - self.front_sent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    /// Reset in any state, it is as a new one, with the room it had.
    #[test]
    fn a_reset_queue_is_as_new_and_keeps_its_room() {
        let mut out = Outbound::default();
        out.interim(10).unwrap();
        out.final_head(40).unwrap();
        out.accepted(12);
        assert!(out.committed());
        let room = out.heads.capacity();
        out.reset();
        assert!(!out.committed());
        assert!(!out.can_replace_final());
        assert!(out.heads_sent());
        assert_eq!(out.on_failure(), Outbound::default().on_failure());
        out.final_head(30).unwrap();
        assert!(out.can_replace_final());
        assert_eq!(out.heads.capacity(), room, "the room went");
    }

    #[test]
    fn a_final_head_can_be_replaced_until_its_first_byte_goes() {
        let mut out = Outbound::default();
        out.final_head(40).unwrap();
        assert!(out.can_replace_final());
        assert_eq!(out.on_failure(), OnFailure::Answer);
        out.replace_final(30).unwrap();
        out.accepted(1);
        assert!(out.committed());
        assert!(!out.can_replace_final());
        assert_eq!(out.replace_final(20), Err(OutboundError::Committed));
        assert_eq!(out.on_failure(), OnFailure::Close);
        out.accepted(29);
        assert!(out.heads_sent());
    }

    /// A head offered in a write that did not finish may be on its way — TLS seals what it is
    /// offered and sends it — so a final head offered so is committed, and an informational
    /// one is finished before anything follows it, all of it (A03-03, C32).
    #[test]
    fn a_head_offered_in_a_write_that_waits_is_begun() {
        let mut out = Outbound::default();
        out.final_head(40).unwrap();
        out.pending();
        assert!(out.committed());
        assert!(!out.can_replace_final());
        assert_eq!(out.on_failure(), OnFailure::Close);

        let mut out = Outbound::default();
        out.interim(10).unwrap();
        out.pending();
        assert_eq!(out.on_failure(), OnFailure::FinishThenAnswer(10));
        // A final head queued after the write that waited was not offered in it.
        out.final_head(40).unwrap();
        assert!(out.can_replace_final());
        out.accepted(4);
        assert_eq!(out.on_failure(), OnFailure::FinishThenAnswer(6));
        // The interim head gone, nothing of the final head was offered while it waited.
        out.accepted(6);
        assert_eq!(out.on_failure(), OnFailure::Answer);
        assert!(out.can_replace_final());

        // Two informational heads offered together are both begun.
        let mut out = Outbound::default();
        out.interim(10).unwrap();
        out.interim(12).unwrap();
        out.pending();
        assert_eq!(out.on_failure(), OnFailure::FinishThenAnswer(22));
        out.accepted(10);
        assert_eq!(out.on_failure(), OnFailure::FinishThenAnswer(12));
        out.reset();
        out.interim(10).unwrap();
        assert_eq!(out.on_failure(), OnFailure::Answer, "a reset forgets it");
    }

    /// Every split of an informational head: before any of it, part of it, all of it.
    #[test]
    fn a_part_written_interim_head_is_finished_before_anything_else() {
        for sent in 0..=10 {
            let mut out = Outbound::default();
            out.interim(10).unwrap();
            out.final_head(40).unwrap();
            out.accepted(sent);
            let expected = match sent {
                0 | 10 => OnFailure::Answer,
                sent => OnFailure::FinishThenAnswer(10 - sent),
            };
            assert_eq!(out.on_failure(), expected, "{sent} bytes gone");
            assert!(!out.committed(), "an interim head commits nothing");
            assert!(out.can_replace_final());
        }
    }

    /// Bytes that finish an informational head and begin the final one in one write.
    #[test]
    fn one_write_across_the_boundary_commits_the_final_head() {
        let mut out = Outbound::default();
        out.interim(10).unwrap();
        out.interim(8).unwrap();
        out.final_head(40).unwrap();
        out.accepted(18);
        assert!(
            !out.committed(),
            "exactly at the boundary, nothing of the final has gone"
        );
        out.accepted(1);
        assert!(out.committed());
    }

    #[test]
    fn nothing_queues_after_the_final_head() {
        let mut out = Outbound::default();
        assert_eq!(out.replace_final(1), Err(OutboundError::NoFinal));
        out.final_head(40).unwrap();
        assert_eq!(out.interim(10), Err(OutboundError::AfterFinal));
        assert_eq!(out.final_head(40), Err(OutboundError::AfterFinal));
    }

    #[test]
    fn body_bytes_after_the_heads_change_nothing() {
        let mut out = Outbound::default();
        out.final_head(4).unwrap();
        out.accepted(4 + 1000);
        assert!(out.heads_sent());
        assert!(out.committed());
        assert_eq!(out.on_failure(), OnFailure::Close);
    }

    proptest! {
        /// Against a reference that keeps every byte: committed exactly when a byte of the
        /// final head is among those accepted, and a part-written interim head is reported
        /// with exactly what is left of it, however the writes were split.
        #[test]
        fn it_agrees_with_counting_every_byte(
            interims in prop::collection::vec(1_usize..20, 0..4),
            final_length in 1_usize..30,
            writes in prop::collection::vec(0_usize..25, 0..12),
        ) {
            let mut out = Outbound::default();
            let mut owner = Vec::new();
            for (at, &length) in interims.iter().enumerate() {
                out.interim(length).unwrap();
                owner.extend(std::iter::repeat_n(Some(at), length));
            }
            out.final_head(final_length).unwrap();
            owner.extend(std::iter::repeat_n(None, final_length));

            let mut gone = 0;
            for write in writes {
                out.accepted(write);
                gone = (gone + write).min(owner.len() + 1000);
                let heads_gone = gone.min(owner.len());
                let committed = owner[..heads_gone].iter().any(Option::is_none);
                prop_assert_eq!(out.committed(), committed);
                let expected = if committed {
                    OnFailure::Close
                } else if heads_gone == 0 || heads_gone == owner.len() {
                    OnFailure::Answer
                } else {
                    let current = owner[heads_gone - 1];
                    let next = owner[heads_gone];
                    if current == next && current.is_some() {
                        let left = owner[heads_gone..].iter().take_while(|o| **o == current).count();
                        OnFailure::FinishThenAnswer(left)
                    } else {
                        OnFailure::Answer
                    }
                };
                prop_assert_eq!(out.on_failure(), expected);
            }
        }
    }
}
