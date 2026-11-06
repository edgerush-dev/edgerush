//! The data planes' side of the store, on the stream [`pod::decide_stream`] opens
//! (`membership-design.md`, "What it asks of the data plane"): a key hashed as every data
//! plane hashes it, the table learnt from whichever pod tells it, the pod a key is asked of,
//! the messages asked and the answers read, and when a give-back is sent again. Pieces a data
//! plane's own streams are driven with, as the model's client (`cluster-sim`) drives them:
//! nothing here holds a stream, runs a task or reads a clock.
//!
//! [`pod::decide_stream`]: super::pod::decide_stream

use super::wire::{self, Asking, Invalid, Telling};
use edgerush_cluster::table::shard_of;
use edgerush_cluster::{Message as StoreMessage, Stamp, Table};
/// What a data plane names, asks and is told in the store's terms, for its side to use with
/// these pieces: a member's id, a time in nanoseconds, the limit a key is held to and an
/// owner's answer.
pub use edgerush_cluster::{NodeId, Time};
use edgerush_cluster_synchronous::Message;
pub use edgerush_cluster_synchronous::{Answer, Rule};
use edgerush_ratelimit::service::Keyed;
use std::hash::{BuildHasher, Hasher};

/// How long a give-back goes unanswered before it is sent again the first time: far beyond a
/// request's deadline, as it has none, so that one whose answer is only late is not given back
/// twice, and soon enough that one lost holds its allowance little longer. The model's client's.
pub const GIVE_BACK_AGAIN: Time = 100_000_000;

/// How long it goes unanswered before each send after: past a crashed owner's failover and an
/// answer late, so that the last send reaches the owner then named.
pub const GIVE_BACK_LATER: Time = 1_000_000_000;

/// The sends of a give-back that may go unanswered before it is dropped: one whose answers are
/// late is credited at most this many times, its charge standing should every one be lost.
pub const GIVE_BACK_SENDS: u8 = 3;

/// A data plane's view of the store: its hash for keys and the latest table it was told.
#[derive(Clone, Debug)]
pub struct Plane {
    keyed: Keyed,
    table: Option<Table>,
}

/// What a pod told a data plane, its table taken in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Told {
    /// Its table: taken if later than the one held.
    Table,
    /// The answer to the data plane's request `id`; an `Elsewhere`'s table taken in as a
    /// table told is, and left out.
    Answer {
        /// The data plane's number for the request.
        id: u64,
        /// The answer.
        answer: Answer,
    },
}

impl Plane {
    /// A data plane's, hashing keys under `secret`, which every data plane shares and clients
    /// never see; it has no table until a pod tells it one.
    #[must_use]
    pub fn new(secret: [u64; 2]) -> Self {
        Self {
            keyed: Keyed::new(secret),
            table: None,
        }
    }

    /// The store's key for a data plane's `bytes`: alike on every data plane under the same
    /// secret. Written in whole rather than through `Hash`, whose framing of a slice is the
    /// standard library's to change, and two data planes built apart must agree.
    #[must_use]
    pub fn key(&self, bytes: &[u8]) -> u64 {
        let mut hasher = self.keyed.build_hasher();
        hasher.write(bytes);
        hasher.finish()
    }

    /// The table it holds, if a pod has told it one.
    #[must_use]
    pub const fn table(&self) -> Option<&Table> {
        self.table.as_ref()
    }

    /// The number of the table it holds, `(0, 0)` for none: what a request carries, so that a
    /// pod with a later one tells it.
    #[must_use]
    pub fn stamp(&self) -> Stamp {
        self.table.as_ref().map_or((0, 0), Table::stamp)
    }

    /// Takes `table` if it is later than the one it holds, whichever pod told it: whether it
    /// did. A pod behind the rest may tell an older one, which changes nothing.
    pub fn learn(&mut self, table: Table) -> bool {
        let later = table.stamp() > self.stamp();
        if later {
            self.table = Some(table);
        }
        later
    }

    /// The owner its table names for `key`, if it has a table and the key's shard an owner.
    #[must_use]
    pub fn owner(&self, key: u64) -> Option<NodeId> {
        let table = self.table.as_ref()?;
        table.owner(shard_of(key, shard_count(table)))
    }

    /// Where `member` is reached, as its table has it.
    #[must_use]
    pub fn address(&self, member: NodeId) -> Option<&str> {
        self.table
            .as_ref()?
            .members
            .get(&member)
            .map(String::as_str)
    }

    /// The member to ask about `key`: its owner, unless `avoid` says the owner is out of reach
    /// (its stream down, its breaker open, or it said it is not serving); then, to forward it,
    /// one of its shard's replicas not avoided, holding a copy it may decide the key from while
    /// its owner is failed over; else another member not avoided. Each is chosen by the key,
    /// not at random, so that an owner's keys spread over those it may go through rather than
    /// falling on one. None with no table, or with every member avoided.
    pub fn through(&self, key: u64, avoid: impl Fn(NodeId) -> bool) -> Option<NodeId> {
        let table = self.table.as_ref()?;
        if let Some(owner) = self.owner(key).filter(|owner| !avoid(*owner)) {
            return Some(owner);
        }
        let shard = table
            .shards
            .get(usize::from(shard_of(key, shard_count(table))));
        let replicas = || {
            shard
                .into_iter()
                .flat_map(|shard| &shard.replicas)
                .copied()
                .filter(|replica| !avoid(*replica))
        };
        // The owner, if any, is avoided here, so among none of these.
        let others = || {
            table
                .members
                .keys()
                .copied()
                .filter(|member| !avoid(*member))
        };
        chosen_by(key, replicas).or_else(|| chosen_by(key, others))
    }

    /// The request `id` under `key`, held to `rule` if it has one, as the stream carries it up.
    #[must_use]
    pub fn request(&self, id: u64, key: u64, rule: Option<Rule>) -> Option<Asking> {
        self.asking(id, key, rule, false, 0)
    }

    /// The give-back `id` of one charged under `key` and `rule`, answered with `receipt`, for a
    /// request then refused or given up on, as the stream carries it up: to be sent to the
    /// key's owner now, as any request is, whoever charged it.
    #[must_use]
    pub fn give_back(&self, id: u64, key: u64, rule: Option<Rule>, receipt: u64) -> Option<Asking> {
        self.asking(id, key, rule, true, receipt)
    }

    fn asking(
        &self,
        id: u64,
        key: u64,
        rule: Option<Rule>,
        give_back: bool,
        receipt: u64,
    ) -> Option<Asking> {
        // The stream names its client, and a data plane's request is its first hop: the pod
        // sets both whatever is sent.
        wire::asking(Message::Request {
            client: 0,
            id,
            key,
            stamp: self.stamp(),
            hops: 0,
            via: None,
            give_back,
            rule,
            receipt,
        })
    }

    /// Reads what a pod told on the stream, taking in any table it carries.
    ///
    /// # Errors
    ///
    /// A message the mode cannot use: see [`Invalid`].
    pub fn told(&mut self, telling: Telling) -> Result<Told, Invalid> {
        match wire::told_from(telling)? {
            Message::Store(StoreMessage::TableReply { table }) => {
                let _later = self.learn(table);
                Ok(Told::Table)
            }
            Message::Reply { id, answer, .. } => {
                let answer = match answer {
                    Answer::Elsewhere(Some(table)) => {
                        let _later = self.learn(table);
                        Answer::Elsewhere(None)
                    }
                    answer => answer,
                };
                Ok(Told::Answer { id, answer })
            }
            // `told_from` makes nothing else.
            _ => Err(Invalid::Missing),
        }
    }
}

/// The one of `among` that `key` chooses, if there is any.
fn chosen_by<I: Iterator<Item = NodeId>>(key: u64, among: impl Fn() -> I) -> Option<NodeId> {
    let count = u64::try_from(among().count())
        .ok()
        .filter(|count| *count > 0)?;
    // The key is already a keyed hash, its bits spread: below `count`, a usize's count.
    let at = usize::try_from(key % count).ok()?;
    among().nth(at)
}

/// Its shard count: every table of a cluster has the same, below `u16::MAX` (`Config`).
fn shard_count(table: &Table) -> u16 {
    u16::try_from(table.shards.len()).unwrap_or(u16::MAX)
}

/// A give-back sent and not yet answered: when it is to be sent again, or dropped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Giving {
    sent: Time,
    unanswered: u8,
}

/// What a give-back unanswered is due.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Due {
    /// Nothing yet.
    Waiting,
    /// Sending again, to the key's owner as the table now names it.
    Again,
    /// Dropping: sent [`GIVE_BACK_SENDS`] times, every answer missing, its charge left to
    /// stand.
    Dropped,
}

impl Giving {
    /// One first sent at `now`.
    #[must_use]
    pub const fn sent(now: Time) -> Self {
        Self {
            sent: now,
            unanswered: 0,
        }
    }

    /// What it is due at `now`, unanswered: sent again [`GIVE_BACK_AGAIN`] after the first
    /// send, then [`GIVE_BACK_LATER`] after each, and dropped once [`GIVE_BACK_SENDS`] have
    /// gone unanswered, as the model's client does.
    pub fn due(&mut self, now: Time) -> Due {
        if self.unanswered >= GIVE_BACK_SENDS {
            return Due::Dropped;
        }
        let wait = if self.unanswered == 0 {
            GIVE_BACK_AGAIN
        } else {
            GIVE_BACK_LATER
        };
        if now.saturating_sub(self.sent) < wait {
            return Due::Waiting;
        }
        self.unanswered += 1;
        if self.unanswered >= GIVE_BACK_SENDS {
            return Due::Dropped;
        }
        self.sent = now;
        Due::Again
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::table::{Shard, first};
    use proptest::prelude::*;
    use std::collections::BTreeMap;

    const SECRET: [u64; 2] = [0x0123_4567_89ab_cdef, 0xfedc_ba98_7654_3210];
    const SHARDS: u16 = 64;

    /// The first table of `members`, each at an address of its own, at `term`.
    fn table(members: &[NodeId], term: u64) -> Table {
        let members: BTreeMap<NodeId, String> = members
            .iter()
            .map(|member| (*member, format!("rl-{member}:7000")))
            .collect();
        first(&members, SHARDS, 1, term)
    }

    fn plane(members: &[NodeId]) -> Plane {
        let mut plane = Plane::new(SECRET);
        assert!(plane.learn(table(members, 1)));
        plane
    }

    /// A key whose shard `owner` owns.
    fn key_of(plane: &Plane, owner: NodeId, from: u64) -> u64 {
        (from..)
            .find(|key| plane.owner(*key) == Some(owner))
            .unwrap()
    }

    #[test]
    fn a_key_hashes_alike_under_the_same_secret_and_apart_under_another() {
        let (one, again) = (Plane::new(SECRET), Plane::new(SECRET));
        let other = Plane::new([SECRET[0], SECRET[1] ^ 1]);
        for bytes in [&b""[..], b"a", b"api\x00203.0.113.7", &[7; 40]] {
            assert_eq!(one.key(bytes), again.key(bytes));
            assert_ne!(one.key(bytes), other.key(bytes), "{bytes:?}");
        }
        assert_ne!(one.key(b"api\0a"), one.key(b"api\0b"));
    }

    #[test]
    fn with_no_table_it_names_no_owner_and_asks_nobody() {
        let plane = Plane::new(SECRET);
        assert_eq!(plane.stamp(), (0, 0));
        assert_eq!(plane.owner(1), None);
        assert_eq!(plane.through(1, |_| false), None);
        assert_eq!(plane.address(1), None);
    }

    #[test]
    fn a_table_is_taken_only_if_later() {
        let mut plane = plane(&[1, 2, 3]);
        assert!(!plane.learn(table(&[1, 2], 1)), "the same number taken");
        assert!(!plane.learn(table(&[1, 2], 0)), "an earlier taken");
        assert_eq!(plane.table().unwrap().members.len(), 3);
        assert!(plane.learn(table(&[1, 2], 2)));
        assert_eq!(plane.stamp(), (2, 1));
        assert_eq!(plane.table().unwrap().members.len(), 2);
    }

    #[test]
    fn a_key_goes_to_the_owner_its_shard_has_at_its_address() {
        let plane = plane(&[1, 2, 3]);
        let table = plane.table().unwrap();
        for key in 0..500 {
            let owner = table.owner(shard_of(key, SHARDS));
            assert_eq!(plane.owner(key), owner);
            assert_eq!(plane.through(key, |_| false), owner);
        }
        assert_eq!(plane.address(2), Some("rl-2:7000"));
        assert_eq!(plane.address(4), None);
    }

    /// A key whose owner is out of reach goes through one of its shard's replicas reached,
    /// which holds its copy, chosen by the key so that the owner's keys spread over them; with
    /// none reached, through another member, spread likewise.
    #[test]
    fn an_owner_out_of_reach_has_its_keys_spread_over_its_shards_replicas_reached() {
        let members: BTreeMap<NodeId, String> = (1..=5)
            .map(|member| (member, format!("rl-{member}:7000")))
            .collect();
        let mut plane = Plane::new(SECRET);
        assert!(plane.learn(first(&members, SHARDS, 2, 1)));
        let table = plane.table().unwrap().clone();
        let (mut replicas, mut rest) = (BTreeMap::new(), BTreeMap::new());
        let mut key = 0;
        for _ in 0..400 {
            key = key_of(&plane, 1, key + 1);
            let shard = &table.shards[usize::from(shard_of(key, SHARDS))];
            let through = plane.through(key, |member| member == 1).unwrap();
            assert!(shard.replicas.contains(&through), "{through} not a replica");
            *replicas.entry(through).or_insert(0) += 1;
            // The same key the same way, every time.
            assert_eq!(plane.through(key, |member| member == 1), Some(through));
            // One replica avoided too, the other.
            let other = plane.through(key, |member| member == 1 || member == through);
            assert!(
                other.is_some_and(|other| other != through && shard.replicas.contains(&other)),
                "{other:?}"
            );
            let gone = |member: NodeId| member == 1 || shard.replicas.contains(&member);
            let beyond = plane.through(key, gone).unwrap();
            assert!(!gone(beyond), "{beyond}");
            *rest.entry(beyond).or_insert(0) += 1;
        }
        for spread in [&replicas, &rest] {
            assert_eq!(spread.len(), 2, "{spread:?}");
            for (member, count) in spread {
                assert!(*count > 120, "{member} took {count} of 400");
            }
        }
        // None when every one is avoided.
        assert_eq!(plane.through(key, |_| true), None);
    }

    #[test]
    fn a_shard_without_an_owner_is_asked_of_any_member() {
        let mut plane = Plane::new(SECRET);
        let mut orphaned = table(&[1, 2], 1);
        orphaned.shards = vec![
            Shard {
                owner: None,
                replicas: Vec::new(),
                term: 0,
            };
            usize::from(SHARDS)
        ];
        assert!(plane.learn(orphaned));
        assert_eq!(plane.owner(5), None);
        assert!(matches!(plane.through(5, |_| false), Some(1 | 2)));
    }

    #[test]
    fn a_request_and_a_give_back_carry_the_table_the_rule_and_the_receipt() {
        let plane = plane(&[1, 2]);
        let rule = Rule::Window {
            per: 10,
            window: 1_000_000_000,
        };
        let asked = wire::asked_from(plane.request(7, 42, Some(rule)).unwrap(), 9).unwrap();
        assert_eq!(
            asked,
            Message::Request {
                client: 9,
                id: 7,
                key: 42,
                stamp: (1, 1),
                hops: 0,
                via: None,
                give_back: false,
                rule: Some(rule),
                receipt: 0,
            }
        );
        let back = wire::asked_from(plane.give_back(8, 42, Some(rule), 5).unwrap(), 9).unwrap();
        assert_eq!(
            back,
            Message::Request {
                client: 9,
                id: 8,
                key: 42,
                stamp: (1, 1),
                hops: 0,
                via: None,
                give_back: true,
                rule: Some(rule),
                receipt: 5,
            }
        );
    }

    #[test]
    fn what_a_pod_tells_is_read_its_tables_taken_in() {
        let mut plane = plane(&[1, 2]);
        let told = |message| wire::telling(message).unwrap();
        let newer = table(&[1, 2, 3], 2);
        let table_told = told(
            StoreMessage::TableReply {
                table: newer.clone(),
            }
            .into(),
        );
        assert_eq!(plane.told(table_told), Ok(Told::Table));
        assert_eq!(plane.stamp(), (2, 1));
        let later = table(&[1, 3], 3);
        let elsewhere = told(Message::Reply {
            client: 0,
            id: 4,
            answer: Answer::Elsewhere(Some(later)),
        });
        assert_eq!(
            plane.told(elsewhere),
            Ok(Told::Answer {
                id: 4,
                answer: Answer::Elsewhere(None),
            })
        );
        assert_eq!(plane.stamp(), (3, 1));
        // An older one, told late, changes nothing.
        let older = told(StoreMessage::TableReply { table: newer }.into());
        assert_eq!(plane.told(older), Ok(Told::Table));
        assert_eq!(plane.stamp(), (3, 1));
        for answer in [
            Answer::Admitted(6),
            Answer::Limited { wait: 250 },
            Answer::NotServing,
            Answer::Elsewhere(None),
        ] {
            let reply = told(Message::Reply {
                client: 0,
                id: 5,
                answer: answer.clone(),
            });
            assert_eq!(plane.told(reply), Ok(Told::Answer { id: 5, answer }));
        }
    }

    #[test]
    fn a_give_back_is_sent_again_at_100_ms_then_a_second_on_and_dropped_after_three() {
        let ms = |n: Time| n * 1_000_000;
        let mut giving = Giving::sent(ms(10));
        assert_eq!(giving.due(ms(109)), Due::Waiting);
        assert_eq!(giving.due(ms(110)), Due::Again);
        assert_eq!(giving.due(ms(1_109)), Due::Waiting);
        assert_eq!(giving.due(ms(1_110)), Due::Again);
        assert_eq!(giving.due(ms(2_109)), Due::Waiting);
        assert_eq!(giving.due(ms(2_110)), Due::Dropped);
        assert_eq!(giving.due(ms(9_000)), Due::Dropped);
    }

    proptest! {
        /// Whatever tables come, in whatever order, the one held is the latest told.
        #[test]
        fn the_table_held_never_goes_back(terms in proptest::collection::vec(0_u64..6, 1..20)) {
            let mut plane = Plane::new(SECRET);
            let mut latest = (0, 0);
            for term in terms {
                let taken = plane.learn(table(&[1, 2], term));
                prop_assert_eq!(taken, (term, 1) > latest);
                latest = latest.max((term, 1));
                prop_assert_eq!(plane.stamp(), latest);
            }
        }

        /// However it is looked at, a give-back is sent at most three times in all.
        #[test]
        fn a_give_back_is_sent_at_most_three_times(
            looks in proptest::collection::vec(0_u64..3_000_000_000, 0..40)
        ) {
            let mut looks = looks;
            looks.sort_unstable();
            let mut giving = Giving::sent(0);
            let mut sends = 1;
            let mut dropped = false;
            for now in looks {
                match giving.due(now) {
                    Due::Again => {
                        prop_assert!(!dropped);
                        sends += 1;
                    }
                    Due::Dropped => dropped = true,
                    Due::Waiting => prop_assert!(!dropped),
                }
            }
            prop_assert!(sends <= GIVE_BACK_SENDS);
        }
    }
}
