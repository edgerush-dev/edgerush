//! The data planes' routing on the store: `edgerush_ratelimit`'s participant reaching the
//! mode's pods by the table it was last told. A key is the store's 128-bit key: the
//! participant's hash of it the first half, a second run of the keyed hash under a secret
//! drawn from the first's the second, made once as the participant first keeps the key, and
//! the mode's bit set (`edgerush_cluster::key`). Its owner is its shard's, by `shard_of` and
//! the table; its batches carry the key and the table's stamp; the pods' answers, their own
//! or relayed, reach the participant as a [`Reply`] of the table's, matched by the first half.

use crate::message::{Answers, Batch, Entry};
use edgerush_cluster::key::{drawn, joined, lower};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Key, Kind, NodeId, Table};
use edgerush_ratelimit::service::Keyed;
use edgerush_ratelimit::service::pooled::{Replied, Reply, Routing};
use std::hash::{BuildHasher, Hasher};

/// The table a data plane holds, if a pod has told it one, and the second run of its keyed
/// hash.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Routed {
    table: Option<Table>,
    second: Keyed,
}

impl Routed {
    /// Routing by no table yet, keys hashed under `secret`, the one every data plane shares:
    /// the participant's hash under it the first run, the second under a secret drawn from it.
    #[must_use]
    pub fn new(secret: [u64; 2]) -> Self {
        Self {
            table: None,
            second: Keyed::new(drawn(secret)),
        }
    }

    /// The same routing, by `table`: what the participant is told as a pod tells the table.
    #[must_use]
    pub fn told(&self, table: Table) -> Self {
        Self {
            table: Some(table),
            second: self.second,
        }
    }

    /// The table, if it has one.
    #[must_use]
    pub const fn table(&self) -> Option<&Table> {
        self.table.as_ref()
    }

    /// The store's key of `bytes`, `hash` being the participant's for them.
    #[must_use]
    pub fn store_key(&self, bytes: &[u8], hash: u64) -> Key {
        whole(hash, self.key(bytes, hash))
    }

    /// `answers` from `from`, relayed through `via` if not sent by `from` itself, as the
    /// participant takes them: the served keys' answers, by the keys' first halves, which are
    /// the participant's hashes, and the table if later than the batch's. Entries unserved
    /// are left out: an elsewhere comes with the later table, which moves its key; a not
    /// serving waits for the key's next send.
    #[must_use]
    pub fn reply(&self, from: NodeId, via: Option<NodeId>, answers: Answers) -> Reply<Self> {
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
                    key: lower(answer.key),
                    behind: answer.answered.behind,
                    applied: answer.answered.applied,
                    others: answer.answered.others,
                })
                .collect(),
            routing: answers.table.map(|table| self.told(table)),
        }
    }

    /// The shard of `key`, with the table.
    fn shard(&self, key: Key) -> Option<(&Table, u16)> {
        let table = self.table.as_ref()?;
        // Every table of a cluster has the same shard count, below `u16::MAX` (`Config`).
        let shards = u16::try_from(table.shards.len()).unwrap_or(u16::MAX);
        Some((table, shard_of(key, shards)))
    }
}

/// The store's key of the participant's `hash` and the routing's `second` half: the mode's.
const fn whole(hash: u64, second: u64) -> Key {
    Kind::Asynchronous.key(joined(hash, second))
}

impl Routing for Routed {
    type Pod = NodeId;
    type Batch = Batch;
    /// The key's second half.
    type Key = u64;

    /// The second run of the keyed hash over `bytes`, the bytes alone written, as the
    /// participant's first is.
    fn key(&self, bytes: &[u8], _: u64) -> u64 {
        let mut state = self.second.build_hasher();
        state.write(bytes);
        state.finish()
    }

    fn owner(&self, _: &[u8], hash: u64, second: u64) -> Option<NodeId> {
        let (table, shard) = self.shard(whole(hash, second))?;
        table.owner(shard)
    }

    /// One of the shard's replicas other than `owner`, chosen by the key, as the synchronous
    /// mode's data plane chooses one: an owner's keys spread over its replicas rather than
    /// falling on one. The replica forwards the entries to the owner, once, and relays its
    /// answers back.
    fn through(&self, hash: u64, second: u64, owner: NodeId) -> Option<NodeId> {
        let key = whole(hash, second);
        let (table, shard) = self.shard(key)?;
        let replicas = &table.shards.get(usize::from(shard))?.replicas;
        let others = || replicas.iter().copied().filter(|replica| *replica != owner);
        let count = Key::try_from(others().count())
            .ok()
            .filter(|count| *count > 0)?;
        // The key is already a keyed hash, its bits spread: below `count`, a usize's count.
        others().nth(usize::try_from(key % count).ok()?)
    }

    /// None: a data plane is told the table on every stream to a pod, as it opens and as
    /// the pod commits one, and a key whose owner has gone unanswered goes through a replica,
    /// whose stream tells it the replica's. An empty batch would ask for nothing more, and
    /// take the pod's one exchange in flight, for a report to wait behind.
    fn polled(&self, _: u64) -> Option<NodeId> {
        None
    }

    /// A table is later by its stamp; any table is later than none.
    fn later(&self, than: &Self) -> bool {
        match (&self.table, &than.table) {
            (Some(table), Some(than)) => table.stamp() > than.stamp(),
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    fn names(&self, pod: NodeId) -> bool {
        self.table
            .as_ref()
            .is_some_and(|table| table.members.contains_key(&pod))
    }

    fn batch(
        &self,
        _: NodeId,
        batch: edgerush_ratelimit::service::Batch,
        keys: &[(u64, u64)],
    ) -> Batch {
        Batch {
            from: batch.from,
            stamp: self.table.as_ref().map_or((0, 0), Table::stamp),
            made: batch.made,
            entries: batch
                .entries
                .iter()
                .zip(keys)
                .map(|(entry, (hash, second))| Entry {
                    key: whole(*hash, *second),
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
    const SECRET: [u64; 2] = [1, 2];

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
        Routed::new(SECRET).told(table(members, replicas, 1))
    }

    /// The participant's hash of `bytes`, under [`SECRET`], as it hashes them.
    fn hashed(bytes: &[u8]) -> u64 {
        let mut state = Keyed::new(SECRET).build_hasher();
        state.write(bytes);
        state.finish()
    }

    /// Distinct keys' bytes, and the participant's hashes of them.
    fn keys(count: u64) -> impl Iterator<Item = (Vec<u8>, u64)> {
        (0..count).map(|n| {
            let bytes = format!("key-{n}").into_bytes();
            let hash = hashed(&bytes);
            (bytes, hash)
        })
    }

    /// A key of the store is its first half the participant's hash, its second the second
    /// run's, under another secret, and its top bit the asynchronous mode's.
    #[test]
    fn a_key_is_the_two_runs_with_the_modes_bit() {
        let routed = Routed::new(SECRET);
        for (bytes, hash) in keys(100) {
            let key = routed.store_key(&bytes, hash);
            assert_eq!(lower(key), hash);
            assert_eq!(Kind::of(key), Kind::Asynchronous);
            let second = routed.key(&bytes, hash);
            assert_ne!(second, hash, "the second run under a secret of its own");
            assert_eq!(key, whole(hash, second));
        }
        assert_ne!(
            Routed::new([3, 4]).store_key(b"k", hashed(b"k")),
            routed.store_key(b"k", hashed(b"k")),
            "another secret, another key"
        );
    }

    /// A key's owner is its shard's, the shard taken from the whole key: keys of one first
    /// half, as two colliding there would be, may fall apart.
    #[test]
    fn a_key_is_owned_by_its_shards_owner() {
        let routed = routed(&[1, 2, 3], 2);
        let table = routed.table().unwrap();
        for (bytes, hash) in keys(1_000) {
            let second = routed.key(&bytes, hash);
            let owner = table.owner(shard_of(whole(hash, second), SHARDS));
            assert_eq!(routed.owner(&bytes, hash, second), owner);
        }
        let shards: BTreeSet<u16> = (0..64)
            .map(|second| shard_of(whole(7, second), SHARDS))
            .collect();
        assert!(shards.len() > 1, "the second half places keys too");
        assert_eq!(
            Routed::new(SECRET).owner(b"k", 1, 2),
            None,
            "no table, no owner"
        );
    }

    /// A key goes through one of its shard's replicas, never the owner, chosen by the key, so
    /// that an owner's keys spread over every replica it has.
    #[test]
    fn a_key_goes_through_a_replica_of_its_shard_chosen_by_the_key() {
        let routed = routed(&[1, 2, 3, 4], 3);
        let table = routed.table().unwrap();
        let mut chosen: BTreeMap<u64, BTreeSet<NodeId>> = BTreeMap::new();
        for (bytes, hash) in keys(4_000) {
            let second = routed.key(&bytes, hash);
            let shard = &table.shards[usize::from(shard_of(whole(hash, second), SHARDS))];
            let owner = shard.owner.unwrap();
            let through = routed.through(hash, second, owner).unwrap();
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
        assert_eq!(lone.through(5, 6, 1), None);
        assert_eq!(Routed::new(SECRET).through(5, 6, 1), None);
    }

    #[test]
    fn no_member_is_polled_and_its_members_are_named() {
        let routed = routed(&[4, 7, 9], 1);
        assert_eq!((0..6).find_map(|turn| routed.polled(turn)), None);
        assert_eq!(Routed::new(SECRET).polled(0), None);
        assert!(routed.names(7));
        assert!(!routed.names(8));
        assert!(!Routed::new(SECRET).names(7));
    }

    #[test]
    fn a_table_is_later_by_its_stamp_and_any_is_later_than_none() {
        let none = Routed::new(SECRET);
        let (one, two) = (
            none.told(table(&[1, 2], 1, 1)),
            none.told(table(&[1, 2], 1, 2)),
        );
        assert!(two.later(&one));
        assert!(!one.later(&two));
        assert!(!one.later(&one));
        assert!(one.later(&none));
        assert!(!none.later(&one));
        assert!(!none.later(&none));
    }

    fn contract() -> Contract {
        let n = |n| NonZeroU64::new(n).unwrap();
        Contract {
            limit: n(100),
            window: n(1_000_000_000),
            burst: n(10),
        }
    }

    /// A data plane on the store sends no empty batch, however long a key in use goes
    /// unheard: tables come down its streams unasked, and an empty batch would only take a
    /// pod's one exchange in flight, for a report to wait behind (#305). The key's reports
    /// still go, every cycle.
    #[test]
    fn a_key_unheard_sends_its_reports_and_never_an_empty_batch() {
        use edgerush_ratelimit::service::Keyed;
        use edgerush_ratelimit::service::pooled::{Decision, Participant};
        const CYCLE: u64 = 10_000_000;
        let mut plane =
            Participant::new(1, CYCLE, routed(&[1, 2, 3], 2), Keyed::new(SECRET)).unwrap();
        plane.set_one_in_flight(true);
        for at in 1..=8 {
            let decided = plane.decide_all(&[(b"k".as_slice(), contract())], at * CYCLE - 1);
            assert_eq!(decided, Decision::Admit);
            plane.on_cycle(at * CYCLE);
            let sent = plane.take_exchanges();
            assert!(!sent.is_empty(), "cycle {at}: its report went");
            for (pod, exchange, batch) in sent {
                assert!(
                    !batch.entries.is_empty(),
                    "cycle {at}: an empty batch to {pod}"
                );
                // Failed at once, as to an owner gone: the next exchange may go.
                plane.exchanged(pod, exchange);
            }
        }
    }

    /// What a participant sends is each key as the store's, whole: the second half the one
    /// the routing made of the key's bytes, kept since, not another key's.
    #[test]
    fn a_participants_batches_carry_each_keys_whole_key() {
        use edgerush_ratelimit::service::pooled::{Decision, Participant};
        const CYCLE: u64 = 10_000_000;
        let routed = routed(&[1], 1);
        let mut plane = Participant::new(1, CYCLE, routed.clone(), Keyed::new(SECRET)).unwrap();
        let keys: Vec<Vec<u8>> = keys(20).map(|(bytes, _)| bytes).collect();
        for key in &keys {
            let decided = plane.decide_all(&[(key.as_slice(), contract())], CYCLE - 1);
            assert_eq!(decided, Decision::Admit);
        }
        plane.on_cycle(CYCLE);
        let sent: Vec<Key> = plane
            .take_exchanges()
            .into_iter()
            .flat_map(|(_, _, batch)| batch.entries.into_iter().map(|entry| entry.key))
            .collect();
        let expected: BTreeSet<Key> = keys
            .iter()
            .map(|key| routed.store_key(key, hashed(key)))
            .collect();
        assert_eq!(sent.into_iter().collect::<BTreeSet<Key>>(), expected);
    }

    /// A batch carries each key as the store's of the halves given, and the table's stamp:
    /// the pod answers with its own table if later.
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
        let sent = routed.batch(1, batch, &[(200, 9), (300, 9)]);
        assert_eq!(
            sent,
            Batch {
                from: 9,
                stamp: routed.table().unwrap().stamp(),
                made: 33,
                entries: vec![
                    Entry {
                        key: whole(200, 9),
                        contract: contract(),
                        report
                    },
                    Entry {
                        key: whole(300, 9),
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
        let none = Routed::new(SECRET).batch(1, empty, &[]);
        assert_eq!(none.stamp, (0, 0), "no table: the earliest stamp");
    }

    /// A pod's answers as the participant takes them: the served keys' answers by their first
    /// halves, the unserved left out, the table as the routing; relayed ones as the owner's.
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
            entries: vec![Answer {
                key: whole(5, 8),
                answered,
            }],
            unserved: vec![Unserved {
                key: whole(6, 8),
                why: Why::Elsewhere,
            }],
            table: Some(table.clone()),
        };
        let routed = Routed::new(SECRET);
        let reply = routed.reply(1, Some(2), answers);
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
                routing: Some(routed.told(table)),
            }
        );
    }
}
