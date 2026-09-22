//! Which heads of an answer have reached the client, and so what may still be done instead.
//!
//! The writer only appends bytes; the connection driver writes them and reports how many
//! the socket accepted. This tracks the heads among those bytes — informational heads,
//! then the final head — and answers the two questions a failure asks: can the final head
//! still be replaced, and if a local answer is wanted instead, may it be written now
//! ([14 §4](../../../../docs/14-downstream-server.md)).
//!
//! The final head is **committed** by its first byte the socket accepts. Until then a
//! final head that is wholly queued can be replaced. An informational head does not commit
//! anything, but one part-written must be finished before any other head follows it: a new
//! status line in the middle of a head is two messages spliced into one.

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
    /// An informational head is part-written: write the rest of it — this many bytes —
    /// within the existing write deadline, then the local answer, or close.
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
        }
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
        match self.heads.front() {
            Some(&(Kind::Interim, length)) if self.front_sent > 0 => {
                OnFailure::FinishThenAnswer(length - self.front_sent)
            }
            _ => OnFailure::Answer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

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
