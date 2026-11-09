//! The data planes' routing on the store: `edgerush_ratelimit`'s participant reaching the
//! mode's pods by the table it was last told. A key's owner is its shard's, by `shard_of` and
//! the table; its batches carry the key's hash and the table's stamp; the pods' answers, their
//! own or relayed, reach the participant as a [`Reply`] of the table's.

use crate::message::{Answers, Batch, Entry};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{NodeId, Table};
use edgerush_ratelimit::service::pooled::{Replied, Reply, Routing};

/// The table a data plane holds, if a pod has told it one.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Routed(Option<Table>);

impl Routed {
    /// Routing by `table`, or by none yet.
    #[must_use]
    pub const fn new(table: Option<Table>) -> Self {
        Self(table)
    }

    /// The table, if it has one.
    #[must_use]
    pub const fn table(&self) -> Option<&Table> {
        self.0.as_ref()
    }

    /// `answers` from `from`, relayed through `via` if not sent by `from` itself, as the
    /// participant takes them: the served keys' answers, and the table if later than the
    /// batch's. Entries unserved are left out: an elsewhere comes with the later table, which
    /// moves its key; a not serving waits for the key's next send.
    #[must_use]
    pub fn reply(from: NodeId, via: Option<NodeId>, answers: Answers) -> Reply<Self> {
        Reply {
            from,
            via,
            life: answers.life,
            number: answers.number,
            made: answers.made,
            entries: answers
                .entries
                .iter()
                .map(|answer| Replied {
                    key: answer.key,
                    behind: answer.answered.behind,
                    applied: answer.answered.applied,
                    others: answer.answered.others,
                })
                .collect(),
            routing: answers.table.map(|table| Self(Some(table))),
        }
    }

    /// The shard of the key of `hash`, with the table.
    fn shard(&self, hash: u64) -> Option<(&Table, u16)> {
        let table = self.0.as_ref()?;
        // Every table of a cluster has the same shard count, below `u16::MAX` (`Config`).
        let shards = u16::try_from(table.shards.len()).unwrap_or(u16::MAX);
        Some((table, shard_of(hash, shards)))
    }
}

impl Routing for Routed {
    type Pod = NodeId;
    type Batch = Batch;

    fn owner(&self, _: &[u8], hash: u64) -> Option<NodeId> {
        let (table, shard) = self.shard(hash)?;
        table.owner(shard)
    }

    /// One of the shard's replicas other than `owner`, chosen by the key, as the synchronous
    /// mode's data plane chooses one: an owner's keys spread over its replicas rather than
    /// falling on one. The replica forwards the entries to the owner, once, and relays its
    /// answers back.
    fn through(&self, hash: u64, owner: NodeId) -> Option<NodeId> {
        let (table, shard) = self.shard(hash)?;
        let replicas = &table.shards.get(usize::from(shard))?.replicas;
        let others = || replicas.iter().copied().filter(|replica| *replica != owner);
        let count = u64::try_from(others().count())
            .ok()
            .filter(|count| *count > 0)?;
        // The key is already a keyed hash, its bits spread: below `count`, a usize's count.
        others().nth(usize::try_from(hash % count).ok()?)
    }

    fn polled(&self, turn: u64) -> Option<NodeId> {
        let members = &self.0.as_ref()?.members;
        let count = u64::try_from(members.len())
            .ok()
            .filter(|count| *count > 0)?;
        members
            .keys()
            .nth(usize::try_from(turn % count).ok()?)
            .copied()
    }

    /// A table is later by its stamp; any table is later than none.
    fn later(&self, than: &Self) -> bool {
        match (&self.0, &than.0) {
            (Some(table), Some(than)) => table.stamp() > than.stamp(),
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    fn names(&self, pod: NodeId) -> bool {
        self.0
            .as_ref()
            .is_some_and(|table| table.members.contains_key(&pod))
    }

    fn batch(
        &self,
        _: NodeId,
        batch: edgerush_ratelimit::service::Batch,
        hash: impl Fn(&[u8]) -> u64,
    ) -> Batch {
        Batch {
            from: batch.from,
            stamp: self.0.as_ref().map_or((0, 0), Table::stamp),
            made: batch.made,
            entries: batch
                .entries
                .iter()
                .map(|entry| Entry {
                    key: hash(&entry.key),
                    contract: entry.contract,
                    report: entry.report,
                })
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(
        clippy::unwrap_used,
        reason = "test set-up: a helper's failure fails the test"
    )]

    use super::*;
    use crate::message::{Answer, Unserved, Why};
    use crate::owner::Answered;
    use edgerush_cluster::table::first;
    use edgerush_ratelimit::service::{self, Contract, Report};
    use std::collections::{BTreeMap, BTreeSet};
    use std::num::NonZeroU64;

    const SHARDS: u16 = 64;

    /// The first table of `members`, each shard with `replicas` copies, at version `version`.
    fn table(members: &[NodeId], replicas: usize, version: u64) -> Table {
        let members: BTreeMap<NodeId, String> = members
            .iter()
            .map(|member| (*member, format!("rl-{member}:7000")))
            .collect();
        let mut table = first(&members, SHARDS, replicas, 1);
        table.version = version;
        table
    }

    fn routed(members: &[NodeId], replicas: usize) -> Routed {
        Routed::new(Some(table(members, replicas, 1)))
    }

    #[test]
    fn a_key_is_owned_by_its_shards_owner() {
        let routed = routed(&[1, 2, 3], 2);
        let table = routed.table().unwrap();
        for hash in 0..1_000 {
            let owner = table.owner(shard_of(hash, SHARDS));
            assert_eq!(routed.owner(b"ignored", hash), owner);
        }
        assert_eq!(Routed::default().owner(b"k", 1), None, "no table, no owner");
    }

    /// A key goes through one of its shard's replicas, never the owner, chosen by the key, so
    /// that an owner's keys spread over every replica it has.
    #[test]
    fn a_key_goes_through_a_replica_of_its_shard_chosen_by_the_key() {
        let routed = routed(&[1, 2, 3, 4], 3);
        let table = routed.table().unwrap();
        let mut chosen: BTreeMap<u64, BTreeSet<NodeId>> = BTreeMap::new();
        for hash in 0..4_000 {
            let shard = &table.shards[usize::from(shard_of(hash, SHARDS))];
            let owner = shard.owner.unwrap();
            let through = routed.through(hash, owner).unwrap();
            assert_ne!(through, owner);
            assert!(shard.replicas.contains(&through), "{hash}");
            chosen.entry(owner).or_default().insert(through);
        }
        for (owner, through) in chosen {
            assert_eq!(
                through.len(),
                3,
                "owner {owner}'s keys over all its replicas"
            );
        }
    }

    #[test]
    fn a_shard_with_no_other_replica_is_gone_through_nowhere() {
        let lone = routed(&[1], 2);
        assert_eq!(lone.through(5, 1), None);
        assert_eq!(Routed::default().through(5, 1), None);
    }

    #[test]
    fn its_members_are_polled_in_turn() {
        let routed = routed(&[4, 7, 9], 1);
        let polled: Vec<_> = (0..6).map(|turn| routed.polled(turn).unwrap()).collect();
        assert_eq!(polled, vec![4, 7, 9, 4, 7, 9]);
        assert_eq!(Routed::default().polled(0), None);
        assert!(routed.names(7));
        assert!(!routed.names(8));
        assert!(!Routed::default().names(7));
    }

    #[test]
    fn a_table_is_later_by_its_stamp_and_any_is_later_than_none() {
        let (one, two) = (
            Routed::new(Some(table(&[1, 2], 1, 1))),
            Routed::new(Some(table(&[1, 2], 1, 2))),
        );
        assert!(two.later(&one));
        assert!(!one.later(&two));
        assert!(!one.later(&one));
        assert!(one.later(&Routed::default()));
        assert!(!Routed::default().later(&one));
        assert!(!Routed::default().later(&Routed::default()));
    }

    fn contract() -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(100),
            window: n(1_000_000_000),
            burst: n(10),
        }
    }

    /// A batch carries each key as the hash given, and the table's stamp: the pod answers
    /// with its own table if later.
    #[test]
    fn a_batch_carries_the_keys_hashes_and_the_tables_stamp() {
        let routed = routed(&[1, 2], 1);
        let report = Report::Poll;
        let batch = service::Batch {
            from: 9,
            made: 33,
            entries: vec![
                service::Entry {
                    key: Box::from(&b"ab"[..]),
                    contract: contract(),
                    report,
                },
                service::Entry {
                    key: Box::from(&b"xyz"[..]),
                    contract: contract(),
                    report,
                },
            ],
        };
        let sent = routed.batch(1, batch, |bytes| bytes.len() as u64 * 100);
        assert_eq!(
            sent,
            Batch {
                from: 9,
                stamp: routed.table().unwrap().stamp(),
                made: 33,
                entries: vec![
                    Entry {
                        key: 200,
                        contract: contract(),
                        report
                    },
                    Entry {
                        key: 300,
                        contract: contract(),
                        report
                    },
                ],
            }
        );
        let empty = service::Batch {
            from: 9,
            made: 0,
            entries: Vec::new(),
        };
        let none = Routed::default().batch(1, empty, |_| 0);
        assert_eq!(none.stamp, (0, 0), "no table: the earliest stamp");
    }

    /// A pod's answers as the participant takes them: the served keys' answers, the unserved
    /// left out, the table as the routing; relayed ones as the owner's.
    #[test]
    fn answers_are_replied_as_the_owners_the_unserved_left_out() {
        let answered = Answered {
            behind: 7,
            applied: 3,
            others: 2,
        };
        let table = table(&[1, 2], 1, 4);
        let answers = Answers {
            life: 11,
            number: 12,
            made: 13,
            entries: vec![Answer { key: 5, answered }],
            unserved: vec![Unserved {
                key: 6,
                why: Why::Elsewhere,
            }],
            table: Some(table.clone()),
        };
        let reply = Routed::reply(1, Some(2), answers);
        assert_eq!(
            reply,
            Reply {
                from: 1,
                via: Some(2),
                life: 11,
                number: 12,
                made: 13,
                entries: vec![Replied {
                    key: 5,
                    behind: 7,
                    applied: 3,
                    others: 2,
                }],
                routing: Some(Routed::new(Some(table))),
            }
        );
    }
}
