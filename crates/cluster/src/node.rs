//! One pod's part of the cluster: a state machine fed messages and its clock.

use crate::message::{Message, Stamp};
use crate::table::{self, Table, is_quorum, shard_of};
use crate::{Config, Held, Kept, NodeId, ShardId, Time, first_term, generation_of};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::Arc;

/// What a pod gives back to be sent: each message with whom it is for.
pub type Outbox<V> = Vec<(NodeId, Message<V>)>;

/// Where a pod stands in its term.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Role {
    /// It follows, or waits for, a leader.
    Follower,
    /// It asks the voters whether they would vote for it, changing nothing.
    PreCandidate {
        /// The term it would stand in.
        term: u64,
        /// Those that would.
        granted: BTreeSet<NodeId>,
        /// Since when.
        since: Time,
    },
    /// It stands for election in its term.
    Candidate {
        /// Those that voted for it.
        joins: BTreeSet<NodeId>,
        /// Since when.
        since: Time,
        /// Forming the term's generation of the cluster, the pods the control plane named
        /// with it, with their addresses: its votes are counted against them, whatever naming
        /// comes after.
        named: Option<BTreeMap<NodeId, String>>,
    },
    /// It leads its term.
    Leader {
        /// The table it has published and is waiting to see accepted.
        pending: Option<Pending>,
    },
}

/// A table published, waiting for a majority.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pending {
    table: Arc<Table>,
    acks: BTreeSet<NodeId>,
    since: Time,
}

/// What an update of a key changed, for its owner's next copy to its replicas.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Changed {
    /// Nothing: nothing to copy.
    Nothing,
    /// The value: copied whole.
    Whole,
    /// One part of it, numbered as the value numbers its parts — a rolling window's slot: the
    /// next copy carries the parts changed since the last alone.
    Part(u64),
}

/// What a pod last heard from another.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Heard {
    at: Time,
    term: u64,
    leader: Option<NodeId>,
    committed: Stamp,
    fresh: bool,
    /// When it sent it, on its own clock: echoed back in this pod's heartbeats.
    sent: Time,
    /// Its wall clock when it sent it: 0 for none.
    wall: Time,
    /// When this pod sent the latest heartbeat it had heard from this pod, on this pod's
    /// clock, if it heard one lately: what a lease is timed from.
    echo: Option<Time>,
    /// Whether it held a lease when it sent it.
    leased: bool,
}

/// A copy coming in parts, as far as its parts have come in turn.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Taking {
    /// The copy: its shard term, the replica's assignment and its number.
    copy: (u64, u64, u64),
    /// The number of the part that comes next in turn.
    next: u32,
    /// Of a whole copy, the keys its parts have named: those the last part keeps.
    named: BTreeSet<u64>,
    /// Whether its last part is in: kept so that, sent again, it is answered again.
    done: bool,
}

/// As owner: a copy of many parts to one replica, sent a few unanswered parts ahead, each cut
/// as it goes from what the shard holds then.
#[derive(Clone, Debug, PartialEq, Eq)]
struct Sending {
    /// The copy: its shard term, the replica's assignment, the copy it follows, its number,
    /// and the table it goes under.
    term: u64,
    assignment: u64,
    after: Option<u64>,
    version: u64,
    under: Stamp,
    /// The keys it carries, in order: all the shard holds, or, for a repair, those named.
    keys: Option<BTreeSet<u64>>,
    /// The last key of the parts sent so far, if any is.
    reached: Option<u64>,
    /// The number of the next part.
    next: u32,
    /// Whether its last part has gone.
    ended: bool,
    /// Its parts unanswered, in order.
    sent: Vec<Sent>,
    /// When a part of it was last answered, or it was last sent again.
    at: Time,
}

/// A part of a [`Sending`] gone and not yet answered: the keys it covered, after the last of
/// the part before it and to its own last, so that, sent again, it covers the same.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Sent {
    part: u32,
    after: Option<u64>,
    /// Its last key; none for a part that covers no key.
    to: Option<u64>,
    /// Whether more parts follow it, as it said when first sent.
    more: bool,
    /// What it carried when first sent, by the store's count.
    bytes: u64,
}

impl Sending {
    /// A copy under shard term and assignment `copy`, following copy `after`, numbered
    /// `version`, under table `under`, of `keys`, or of all the shard holds; none of it sent.
    const fn new(
        copy: (u64, u64),
        after: Option<u64>,
        version: u64,
        under: Stamp,
        keys: Option<BTreeSet<u64>>,
    ) -> Self {
        Self {
            term: copy.0,
            assignment: copy.1,
            after,
            version,
            under,
            keys,
            reached: None,
            next: 0,
            ended: false,
            sent: Vec::new(),
            at: 0,
        }
    }

    /// Its part numbered `part` of `shard`, saying whether `more` follow, carrying `entries`
    /// and `dropped`.
    fn part<V>(
        &self,
        shard: ShardId,
        (part, more): (u32, bool),
        entries: Vec<(u64, Kept<V>)>,
        dropped: Vec<u64>,
    ) -> Message<V> {
        Message::Replicate {
            shard,
            term: self.term,
            assignment: self.assignment,
            after: self.after,
            version: self.version,
            under: self.under,
            entries,
            dropped,
            part,
            more,
        }
    }
}

/// One pod in one life.
#[derive(Clone, Debug)]
pub struct Node<V> {
    id: NodeId,
    /// Where it is reached: given as it asks to join, for the table to carry.
    address: String,
    config: Config,
    /// Every pod it knows how to reach.
    peers: BTreeSet<NodeId>,
    term: u64,
    role: Role,
    /// The leader it follows in its term, itself if it leads.
    leader: Option<NodeId>,
    heard: BTreeMap<NodeId, Heard>,
    /// How far each peer's wall clock is off from its own, ahead if positive, as its latest
    /// heartbeat with a round trip short enough to tell says.
    skews: BTreeMap<NodeId, i64>,
    /// Tables, shared rather than copied where they are read: one never changes once made.
    accepted: Option<Arc<Table>>,
    committed: Option<Arc<Table>>,
    /// When it last sent its heartbeats, and its copies, and last swept its keys.
    beat_at: Option<Time>,
    cycle_at: Option<Time>,
    swept_at: Option<Time>,
    /// Keys it has forgotten, their time come, and keys it has evicted for the cap.
    forgotten: u64,
    evicted: u64,
    /// When it is to stand for election, if no leader is heard by then.
    elect_at: Option<Time>,
    /// The highest term it has seen anyone in.
    seen_term: u64,
    /// Each key's value, by shard: its own shards and its copies.
    data: BTreeMap<ShardId, BTreeMap<u64, Kept<V>>>,
    /// Keys it changed as owner since its last copies.
    dirty: BTreeMap<ShardId, BTreeSet<u64>>,
    /// Of keys it changed as owner since its last copies only in parts, as their mode numbers
    /// them, those parts: all those copies carry of them — a rolling window's slots changed,
    /// not the up to a thousand it holds.
    parts_changed: BTreeMap<(ShardId, u64), BTreeSet<u64>>,
    /// As owner: each shard's number for its copies, raised with every copy of what changed,
    /// from 1, a whole copy, when it took the shard.
    versions: BTreeMap<ShardId, u64>,
    /// As owner: how far each replica holds each shard without a gap, as it last said, and
    /// since when it has said no more.
    acked: BTreeMap<(ShardId, NodeId), u64>,
    behind: BTreeMap<(ShardId, NodeId), Time>,
    /// As owner: for each replica, the keys of each copy it has not yet said it holds, by the
    /// copy's number: what a repair sends again.
    unacked: BTreeMap<(ShardId, NodeId), BTreeMap<u64, BTreeSet<u64>>>,
    /// As owner: each replica's assignment to each shard, by a number of this pod's that never
    /// repeats, so that its answers from another — before it was taken off the shard and put
    /// back, or before the copies were numbered afresh — count for nothing.
    assignments: BTreeMap<(ShardId, NodeId), u64>,
    next_assignment: u64,
    /// As replica: how far it holds each shard without a gap: the owner it had it from (a pod
    /// in one life, never another's), the shard term, the owner's assignment and the number.
    have: BTreeMap<ShardId, (NodeId, u64, u64, u64)>,
    /// As replica: the latest of its owners' tables it was sent each shard's copies under.
    copied_under: BTreeMap<ShardId, Stamp>,
    /// As replica: each copy of a shard coming in parts, by the shard and the pod sending it.
    taking: BTreeMap<(ShardId, NodeId), Taking>,
    /// As owner: each copy of many parts going to a replica, by the replica and the shard.
    sending: BTreeMap<(NodeId, ShardId), Sending>,
    /// As taker: each owner's last changes of a shard coming in parts, by the shard and that
    /// owner: their shard term and the number of the part that comes next in turn.
    handing_in: BTreeMap<(ShardId, NodeId), (u64, u32)>,
    /// Shards whose taker has caught up and that it has told the leader so, to whom, on the
    /// move planned from which table, last told when: served here still, until the table names
    /// the taker the owner.
    announced: BTreeMap<ShardId, (NodeId, Stamp, Time)>,
    /// As taker named the owner: shards it waits for the old owner's last changes of before it
    /// serves them, since when; and shards whose last changes came first, by shard term.
    awaiting: BTreeMap<ShardId, Time>,
    handed_in: BTreeMap<ShardId, u64>,
    /// The latest table each pod has said it accepted: one it holds, accepted or committed,
    /// so that the next goes to it as what changed. Its heartbeats, every 100 ms, lag tables
    /// published every few milliseconds while shards move.
    accepted_by: BTreeMap<NodeId, Stamp>,
    /// As leader: new members asking in, each when last asked and with the address it gave,
    /// and members asking to drain, each when last asked.
    joining: BTreeMap<NodeId, (Time, String)>,
    draining_asked: BTreeMap<NodeId, Time>,
    /// Takers reported caught up, each on the move planned from which table: waiting to be
    /// published, kept by every voter, as the leader may change before it acts.
    handed: BTreeMap<ShardId, (NodeId, Stamp)>,
    /// Whether it has been told to stop, its shards to be handed on first.
    draining: bool,
    /// Whether it has become ready since its table last named it: see [`ready`](Self::ready).
    became_ready: bool,
    /// Its wall clock less its own, by which it decides requests under a rule: kept close to
    /// every other pod's, as NTP keeps it, where its own clock need agree with nobody's.
    wall_offset: i64,
    /// The pod it last had a table or a commit from, and when.
    tabled: Option<(NodeId, Time)>,
    /// The generation the control plane named it to form, with the others named and their
    /// addresses, and the latest table any of them had; and when the control plane last
    /// repeated it.
    grant: Option<(u64, BTreeMap<NodeId, String>, Stamp)>,
    granted_at: Time,
}

impl<V: Held> Node<V> {
    /// Pod `id`, reached at `address`, new, knowing how to reach `peers`.
    #[must_use]
    pub fn new(
        id: NodeId,
        address: String,
        config: Config,
        peers: impl IntoIterator<Item = NodeId>,
    ) -> Self {
        Self {
            id,
            address,
            config,
            peers: peers.into_iter().filter(|peer| *peer != id).collect(),
            term: 0,
            role: Role::Follower,
            leader: None,
            heard: BTreeMap::new(),
            skews: BTreeMap::new(),
            accepted: None,
            committed: None,
            beat_at: None,
            cycle_at: None,
            swept_at: None,
            forgotten: 0,
            evicted: 0,
            elect_at: None,
            seen_term: 0,
            data: BTreeMap::new(),
            dirty: BTreeMap::new(),
            parts_changed: BTreeMap::new(),
            versions: BTreeMap::new(),
            acked: BTreeMap::new(),
            behind: BTreeMap::new(),
            unacked: BTreeMap::new(),
            assignments: BTreeMap::new(),
            next_assignment: 0,
            have: BTreeMap::new(),
            copied_under: BTreeMap::new(),
            taking: BTreeMap::new(),
            sending: BTreeMap::new(),
            handing_in: BTreeMap::new(),
            announced: BTreeMap::new(),
            awaiting: BTreeMap::new(),
            handed_in: BTreeMap::new(),
            accepted_by: BTreeMap::new(),
            joining: BTreeMap::new(),
            draining_asked: BTreeMap::new(),
            handed: BTreeMap::new(),
            draining: false,
            became_ready: false,
            tabled: None,
            wall_offset: 0,
            grant: None,
            granted_at: 0,
        }
    }

    /// Keys it has forgotten, by its own wall clock, their time come.
    #[must_use]
    pub const fn forgotten(&self) -> u64 {
        self.forgotten
    }

    /// Keys it has evicted, owned or copies, past `max_key_bytes`.
    #[must_use]
    pub const fn evicted(&self) -> u64 {
        self.evicted
    }

    /// About the bytes its keys take, owned and copies: what `max_key_bytes` caps.
    #[must_use]
    pub fn key_bytes(&self) -> u64 {
        self.data
            .values()
            .flat_map(BTreeMap::values)
            .map(Kept::bytes)
            .sum()
    }

    /// The keys it holds of `shard`, owned or a copy.
    #[must_use]
    pub fn keys_of(&self, shard: ShardId) -> BTreeSet<u64> {
        self.data
            .get(&shard)
            .map(|keys| keys.keys().copied().collect())
            .unwrap_or_default()
    }

    /// Its identity.
    #[must_use]
    pub const fn id(&self) -> NodeId {
        self.id
    }

    /// Its term.
    #[must_use]
    pub const fn term(&self) -> u64 {
        self.term
    }

    /// Where it stands.
    #[must_use]
    pub const fn role(&self) -> &Role {
        &self.role
    }

    /// Whether it leads.
    #[must_use]
    pub const fn is_leader(&self) -> bool {
        matches!(self.role, Role::Leader { .. })
    }

    /// The table it has applied.
    #[must_use]
    pub fn committed(&self) -> Option<&Table> {
        self.committed.as_deref()
    }

    /// The number of the latest table it has accepted, `(0, 0)` for none.
    #[must_use]
    pub fn accepted(&self) -> Stamp {
        self.accepted_stamp()
    }

    /// The pods it knows how to reach.
    #[must_use]
    pub const fn peers(&self) -> &BTreeSet<NodeId> {
        &self.peers
    }

    /// A pod it may now reach.
    pub fn add_peer(&mut self, peer: NodeId) {
        if peer != self.id {
            self.peers.insert(peer);
        }
    }

    /// Sets `key`'s value as if it had always held it, owned or a copy, never forgotten: for a
    /// simulation to start a cluster holding many keys, not for serving.
    pub fn load(&mut self, key: u64, value: V) {
        let shard = shard_of(key, self.config.shards);
        self.data
            .entry(shard)
            .or_default()
            .insert(key, Kept::new(value));
    }

    /// Sets its wall clock to its own clock and `offset`, as the clock it decides requests
    /// under a rule by.
    pub const fn set_wall_offset(&mut self, offset: i64) {
        self.wall_offset = offset;
    }

    /// Its wall clock at `now` on its own: what keys are forgotten by, and what a mode decides
    /// by.
    #[must_use]
    pub const fn wall(&self, now: Time) -> Time {
        now.saturating_add_signed(self.wall_offset)
    }

    /// Its configuration.
    #[must_use]
    pub const fn config(&self) -> &Config {
        &self.config
    }

    /// Updates `key` as its owner, if it serves the key's shard at `now`: `change` is given the
    /// key — a new one empty — and the wall clock, and says what it changed, for the next copy
    /// to the shard's replicas. `None`, and nothing called, if it does not serve the shard.
    pub fn update<R>(
        &mut self,
        key: u64,
        now: Time,
        change: impl FnOnce(&mut Kept<V>, Time) -> (R, Changed),
    ) -> Option<R> {
        let shard = shard_of(key, self.config.shards);
        if !self.serves(shard, now) {
            return None;
        }
        let wall = self.wall(now);
        let kept = self.data.entry(shard).or_default().entry(key).or_default();
        let (result, changed) = change(kept, wall);
        match changed {
            Changed::Nothing => {}
            Changed::Whole => {
                self.dirty.entry(shard).or_default().insert(key);
                self.parts_changed.remove(&(shard, key));
            }
            Changed::Part(part) => {
                // A key already changed whole since its last copy is copied whole.
                let whole = !self.parts_changed.contains_key(&(shard, key))
                    && self
                        .dirty
                        .get(&shard)
                        .is_some_and(|keys| keys.contains(&key));
                if !whole {
                    self.parts_changed
                        .entry((shard, key))
                        .or_default()
                        .insert(part);
                    self.dirty.entry(shard).or_default().insert(key);
                }
            }
        }
        Some(result)
    }

    /// Updates its copy of `key` as [`update`](Self::update) updates an owner's, if its table
    /// names it a replica of the key's shard, it holds its lease, and it has not heard the
    /// shard's owner within `stop_serving` — by when an owner dead, or cut off from the
    /// majority, has stopped serving: for a mode that decides a key from a copy while its owner
    /// is failed over. What `change` changes is copied to nobody, and lost if another replica
    /// takes the shard. `None`, and nothing called, otherwise.
    pub fn update_copy<R>(
        &mut self,
        key: u64,
        now: Time,
        change: impl FnOnce(&mut Kept<V>, Time) -> (R, Changed),
    ) -> Option<R> {
        let shard = shard_of(key, self.config.shards);
        let table = self.committed.as_ref()?;
        let entry = table.shards.get(usize::from(shard))?;
        let owner = entry.owner?;
        if !entry.replicas.contains(&self.id)
            || !self.leased(table, now)
            || self.heard_within(owner, now, self.config.stop_serving)
        {
            return None;
        }
        let wall = self.wall(now);
        let kept = self.data.entry(shard).or_default().entry(key).or_default();
        let (result, _) = change(kept, wall);
        Some(result)
    }

    /// The value it holds for `key`, owned or a copy.
    #[must_use]
    pub fn value(&self, key: u64) -> Option<V> {
        let shard = shard_of(key, self.config.shards);
        self.data
            .get(&shard)?
            .get(&key)
            .map(|kept| kept.value.clone())
    }

    /// How many keys it holds, owned and copies.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.data.values().map(BTreeMap::len).sum()
    }

    /// How many keys it holds of the shards its committed table names it the owner of: none
    /// with no table. A count by shard, never a walk of the keys.
    #[must_use]
    pub fn owned_key_count(&self) -> usize {
        self.committed.as_ref().map_or(0, |table| {
            table
                .owned_by(self.id)
                .filter_map(|shard| self.data.get(&shard))
                .map(BTreeMap::len)
                .sum()
        })
    }

    /// Whether it serves `shard` at `now`: its table names it the owner, it is not waiting for
    /// the shard's last changes from the owner it was handed on from, and it and a majority of
    /// the voting set hear each other within `stop_serving`.
    #[must_use]
    pub fn serves(&self, shard: ShardId, now: Time) -> bool {
        let Some(table) = &self.committed else {
            return false;
        };
        table.owner(shard) == Some(self.id)
            && !self.awaiting.contains_key(&shard)
            && self.leased(table, now)
    }

    /// Whether it and a majority of `table`'s voting set hear each other, within
    /// `stop_serving`.
    fn leased(&self, table: &Table, now: Time) -> bool {
        is_quorum(&table.voting, |member| {
            member == self.id || self.mutual(member, now, self.config.stop_serving)
        })
    }

    /// Whether `member` and this pod have heard each other within `within`: its latest
    /// heartbeat echoes one this pod sent within `within`, on this pod's own clock. Both ways,
    /// so that a pod whose own heartbeats are lost counts nobody, though it hears them all; and
    /// timed from the sending, so that old heartbeats reaching it late, saying they hear it,
    /// renew nothing.
    fn mutual(&self, member: NodeId, now: Time, within: Time) -> bool {
        self.heard.get(&member).is_some_and(|heard| {
            heard
                .echo
                .is_some_and(|sent| now.saturating_sub(sent) <= within)
        })
    }

    /// Whether it is ready, as its readiness probe says. It first becomes ready once its table
    /// names it a member holding its share of shards, no moves outstanding, and it hears every
    /// member: a rollout, taking its next step on a new pod's first ready, so waits out a
    /// crash's failover. From then on it is ready while its table names it and it holds its
    /// lease. Were every pod to need every member heard and no moves outstanding throughout,
    /// one member going quiet, or shards moving, would make every pod unready at once, and
    /// the service's DNS name answer with nobody.
    #[must_use]
    pub fn ready(&self, now: Time) -> bool {
        self.became_ready
            && self
                .committed
                .as_ref()
                .is_some_and(|table| table.has_member(self.id) && self.leased(table, now))
    }

    /// Whether it may first become ready: see [`ready`](Self::ready).
    fn may_become_ready(&self, now: Time) -> bool {
        let Some(table) = &self.committed else {
            return false;
        };
        let share = usize::from(self.config.shards) / table.members.len().max(1);
        table.has_member(self.id)
            && table.moves.is_empty()
            && table.owned_by(self.id).count() >= share
            && self.hears_every_member(now)
    }

    /// Whether it has heard every other member of its table within two heartbeats: none gone
    /// that its table still names, so no failover to come.
    fn hears_every_member(&self, now: Time) -> bool {
        let recent = self.config.heartbeat * 2;
        self.committed.as_ref().is_some_and(|table| {
            table
                .members
                .keys()
                .all(|member| *member == self.id || self.heard_within(*member, now, recent))
        })
    }

    fn heard_within(&self, member: NodeId, now: Time, within: Time) -> bool {
        self.heard
            .get(&member)
            .is_some_and(|heard| now.saturating_sub(heard.at) <= within)
    }

    /// Forgets the peers neither its tables name nor it has heard from within `FORGET_AFTER`
    /// silences: every pod restarted is a new one, and without this each would be sent
    /// heartbeats for good. One forgotten that is not gone says so with its next heartbeat.
    fn forget_gone(&mut self, now: Time) {
        // With no table yet, it cannot tell the pods it is to form or join with from any other.
        if self.committed.is_none() {
            return;
        }
        let named = |peer: NodeId| {
            [&self.committed, &self.accepted]
                .into_iter()
                .flatten()
                .any(|table| table.has_member(peer) || table.votes(peer))
        };
        let forget = self.config.silence.saturating_mul(FORGET_AFTER);
        let gone: Vec<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| !named(*peer) && !self.heard_within(*peer, now, forget))
            .collect();
        for peer in gone {
            self.peers.remove(&peer);
            self.heard.remove(&peer);
            self.skews.remove(&peer);
            self.accepted_by.remove(&peer);
        }
    }

    /// The peers whose wall clocks are off from its own by more than `skew_warning`, each by
    /// how far, ahead if positive, as their heartbeats last told.
    #[must_use]
    pub fn skewed(&self) -> BTreeMap<NodeId, i64> {
        let most = self.config.skew_warning;
        self.skews
            .iter()
            .filter(|(_, skew)| skew.unsigned_abs() > most)
            .map(|(peer, skew)| (*peer, *skew))
            .collect()
    }

    /// How far the wall clock of `heard`'s sender is off from its own, ahead if positive: the
    /// sender's when it sent it against its own in the middle of the round trip its echo
    /// measures, the sending somewhere within it. None without an echo or a wall clock, or over
    /// a round trip longer than two heartbeats, whose middle tells too little.
    fn skew_of(&self, heard: &Heard) -> Option<i64> {
        let echo = heard.echo?;
        if heard.wall == 0 {
            return None;
        }
        let trip = heard.at.checked_sub(echo)?;
        if trip > self.config.heartbeat.saturating_mul(2) {
            return None;
        }
        let middle = self.wall(echo.saturating_add(trip / 2));
        i64::try_from(i128::from(heard.wall) - i128::from(middle)).ok()
    }

    /// Told to stop: it hands its shards on and leaves the voting set first. Done once
    /// [`drained`](Self::drained).
    pub fn drain(&mut self) {
        self.draining = true;
    }

    /// Whether it may now stop without anyone noticing: told to drain, and its table has it
    /// owning nothing, voting nowhere and leading nobody — or it never was a member. Or with
    /// no one to notice: its table has every member draining, itself among them, so nobody
    /// is left to take what it holds, and waiting would only end in its grace period's kill.
    #[must_use]
    pub fn drained(&self) -> bool {
        if !self.draining {
            return false;
        }
        let Some(table) = &self.committed else {
            return true;
        };
        if !table.has_member(self.id) {
            return true;
        }
        if !table.is_draining(self.id) {
            return false;
        }
        let none_to_take = table
            .members
            .keys()
            .all(|member| table.is_draining(*member));
        none_to_take
            || (table.owned_by(self.id).next().is_none()
                && !table.votes(self.id)
                && !self.is_leader())
    }

    /// Its stamp for the table it has accepted, `(0, 0)` for none.
    fn accepted_stamp(&self) -> Stamp {
        self.accepted.as_deref().map_or((0, 0), Table::stamp)
    }

    /// The number of the table it has applied, `(0, 0)` for none.
    #[must_use]
    pub fn committed_stamp(&self) -> Stamp {
        self.committed.as_deref().map_or((0, 0), Table::stamp)
    }

    /// The voting sets an election or a commit needs a majority of: the committed one and, if
    /// it differs, the accepted one.
    fn voting_sets(&self) -> Vec<Vec<NodeId>> {
        let mut sets = Vec::new();
        if let Some(table) = &self.committed {
            sets.push(table.voting.clone());
        }
        if let Some(table) = &self.accepted
            && sets.last() != Some(&table.voting)
        {
            sets.push(table.voting.clone());
        }
        sets
    }

    /// Every member that votes in any of [`voting_sets`](Self::voting_sets).
    fn voters(&self) -> BTreeSet<NodeId> {
        self.voting_sets().into_iter().flatten().collect()
    }

    /// Whether `who` are a majority of every voting set.
    fn majority(&self, who: &BTreeSet<NodeId>) -> bool {
        let sets = self.voting_sets();
        !sets.is_empty()
            && sets
                .iter()
                .all(|set| is_quorum(set, |member| who.contains(&member)))
    }

    /// Whether it hears a live leader: one it follows, heard within `silence` — a table from
    /// it as good as a heartbeat — or itself.
    fn hears_leader(&self, now: Time) -> bool {
        let silence = self.config.silence;
        match self.leader {
            Some(leader) if leader == self.id => self.is_leader(),
            Some(leader) => {
                self.heard_within(leader, now, silence)
                    || self.tabled.is_some_and(|(from, at)| {
                        from == leader && now.saturating_sub(at) <= silence
                    })
            }
            None => false,
        }
    }

    /// Its timer at `now`.
    pub fn tick(&mut self, now: Time) -> Outbox<V> {
        let mut out = Outbox::<V>::new();
        // Left out of its table, it must become ready again as a new member does.
        let member = self
            .committed
            .as_ref()
            .is_some_and(|table| table.has_member(self.id));
        self.became_ready = member && (self.became_ready || self.may_become_ready(now));
        if self
            .beat_at
            .is_none_or(|at| now.saturating_sub(at) >= self.config.heartbeat)
        {
            self.beat_at = Some(now);
            self.forget_gone(now);
            let beat = self.heartbeat(now);
            out.extend(self.peers.iter().map(|peer| (*peer, beat.clone())));
            self.ask_in(&mut out);
            self.say_lacking(&mut out);
        }
        if self
            .cycle_at
            .is_none_or(|at| now.saturating_sub(at) >= self.config.cycle)
        {
            self.cycle_at = Some(now);
            self.replicate(now, &mut out);
            self.hand_on(now, &mut out);
        }
        if self
            .swept_at
            .is_none_or(|at| now.saturating_sub(at) >= SWEEP)
        {
            self.swept_at = Some(now);
            self.sweep(now);
        }
        match self.role {
            Role::Leader { ref pending } => {
                let since = pending.as_ref().map(|pending| pending.since);
                self.lead(now, since, &mut out);
            }
            Role::PreCandidate { since, .. } | Role::Candidate { since, .. } => {
                // An election that came to nothing, its asks or answers lost: try again after two
                // heartbeats, not a whole silence, which leaves the table unchangeable.
                if now.saturating_sub(since) > self.config.heartbeat * 2 {
                    self.role = Role::Follower;
                    self.elect_at = None;
                }
            }
            Role::Follower => self.watch_leader(now, &mut out),
        }
        out
    }

    /// Its heartbeat at `now`: whom it follows, its table, when it sent it, and whom it hears,
    /// each with when that pod sent the latest heartbeat it heard from it.
    fn heartbeat(&self, now: Time) -> Message<V> {
        let within = self.config.stop_serving;
        Message::Heartbeat {
            term: self.term,
            leader: self.leader,
            committed: self.committed_stamp(),
            fresh: self.committed.is_none(),
            sent: now,
            wall: self.wall(now),
            hears: self
                .heard
                .iter()
                .filter(|(_, heard)| now.saturating_sub(heard.at) <= within)
                .map(|(peer, heard)| (*peer, heard.sent))
                .collect(),
            leased: self
                .committed
                .as_ref()
                .is_some_and(|table| self.leased(table, now)),
        }
    }

    /// A new member, or one its table has lost, asks to be taken in; one draining asks to be
    /// let go. To every pod with a table, as it does not know which leads.
    fn ask_in(&self, out: &mut Outbox<V>) {
        let member = self
            .committed
            .as_ref()
            .is_some_and(|table| table.has_member(self.id));
        let with_table: Vec<NodeId> = self
            .heard
            .iter()
            .filter(|(_, heard)| !heard.fresh)
            .map(|(peer, _)| *peer)
            .collect();
        if !member && !with_table.is_empty() {
            let ask = Message::JoinCluster {
                address: self.address.clone(),
            };
            out.extend(with_table.iter().map(|peer| (*peer, ask.clone())));
        }
        let let_go = self
            .committed
            .as_ref()
            .is_some_and(|table| table.has_member(self.id) && !table.is_draining(self.id));
        if self.draining && let_go {
            out.extend(with_table.iter().map(|peer| (*peer, Message::Drain)));
        }
    }

    /// As a follower with no live leader: named to form a generation of the cluster, it stands
    /// in it; otherwise, after `silence` and a part of `election_jitter` of its own, it asks for
    /// pre-votes — unless it has heard of a generation later than its table's, whose table it
    /// then waits for. A new pod not named waits to be let in.
    fn watch_leader(&mut self, now: Time, out: &mut Outbox<V>) {
        if self.hears_leader(now) {
            self.elect_at = None;
            return;
        }
        if self.named_ahead() {
            self.stand_to_form(now, out);
            return;
        }
        if (self.committed.is_none() && self.accepted.is_none()) || self.outdated() {
            return;
        }
        // A voter stands too, member or not: one the table took out while it was cut off may
        // be, with others like it, all that is left of a majority of the voting set.
        let may_stand = self.committed.as_ref().is_some_and(|table| {
            (table.has_member(self.id) || self.voters().contains(&self.id))
                && !table.is_draining(self.id)
        });
        if !may_stand {
            return;
        }
        let jitter = self.jitter(self.seen_term.max(self.term));
        // Noticing the leader gone took `silence` already: its own part of the jitter more.
        let at = *self.elect_at.get_or_insert(now + jitter);
        if now < at {
            return;
        }
        let term = self.seen_term.max(self.term) + 1;
        let mut granted = BTreeSet::new();
        if self.voters().contains(&self.id) {
            granted.insert(self.id);
        }
        self.role = Role::PreCandidate {
            term,
            granted,
            since: now,
        };
        let ask = Message::PreVote {
            term,
            accepted: self.accepted_stamp(),
        };
        out.extend(
            self.voters()
                .into_iter()
                .filter(|voter| *voter != self.id)
                .map(|voter| (voter, ask.clone())),
        );
        self.after_pre_vote(now, out);
    }

    /// A pod's own part of `election_jitter`, different for each pod and each term.
    fn jitter(&self, term: u64) -> Time {
        let spread = self.config.election_jitter.max(1);
        table::mix(self.id ^ term.rotate_left(32)) % spread
    }

    /// Whether the control plane has named it to form a generation it has committed no table
    /// of yet: one it has only accepted may be all that is left of the generation's first.
    fn named_ahead(&self) -> bool {
        self.grant
            .as_ref()
            .is_some_and(|(generation, _, _)| *generation > generation_of(self.committed_stamp().0))
    }

    /// Whether it has heard of a generation later than its latest table's: its voting set is
    /// then of a cluster formed again without it, and an ordinary election would carry that
    /// set into the later generation's terms.
    fn outdated(&self) -> bool {
        generation_of(self.seen_term) > generation_of(self.accepted_stamp().0)
    }

    /// Named to form a generation of the cluster: after its own part of `election_jitter`, it
    /// stands in the generation's first term, or the next one it has not seen, asking the
    /// others named for their votes. A naming forms its own generation only: once a later one
    /// has been heard of, it is spent; and only while the control plane repeats it, as it does
    /// every heartbeat until the generation has formed: unrepeated for `silence`, it lapses, so
    /// that a pod cut off from the control plane, having heard of no later generation, forms
    /// nothing in an old one beside it. Behind the latest table any of those named had, it does
    /// not stand: formed from its table, the cluster would drop the counts of the pods holding
    /// the later one. One of those stands, or, every one of them gone, the control plane names
    /// again.
    fn stand_to_form(&mut self, now: Time, out: &mut Outbox<V>) {
        let Some((generation, named, latest)) = self.grant.clone() else {
            return;
        };
        if generation_of(self.seen_term) > generation
            || now.saturating_sub(self.granted_at) > self.config.silence
        {
            self.grant = None;
            return;
        }
        if self.accepted_stamp() < latest {
            return;
        }
        let term = self
            .seen_term
            .max(self.term)
            .saturating_add(1)
            .max(first_term(generation));
        let at = *self.elect_at.get_or_insert(now + self.jitter(term));
        if now < at {
            return;
        }
        self.term = term;
        self.seen_term = term;
        self.leader = None;
        self.role = Role::Candidate {
            joins: std::iter::once(self.id).collect(),
            since: now,
            named: Some(named.clone()),
        };
        let ask = Message::StartJoin {
            term,
            accepted: self.accepted_stamp(),
        };
        out.extend(
            named
                .keys()
                .filter(|peer| **peer != self.id)
                .map(|peer| (*peer, ask.clone())),
        );
        self.after_join(now, out);
    }

    /// As leader: steps down without a majority, or with a table unaccepted for `silence`;
    /// otherwise publishes the next change, if there is one.
    fn lead(&mut self, now: Time, pending: Option<Time>, out: &mut Outbox<V>) {
        let silence = self.config.silence;
        // It leads only while a majority hears it, as it hears them; and it judges members the
        // same way, so that one it hears that does not hear it, holding no lease, has its
        // shards failed over, as a pod gone would.
        let mutual: BTreeSet<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| self.mutual(*peer, now, silence))
            .chain(std::iter::once(self.id))
            .collect();
        if !self.majority(&mutual) {
            self.step_down();
            return;
        }
        // Of those it hears that do not hear it, it keeps those whose heartbeats say they hold
        // a lease, serving through the others; one that holds none serves nothing.
        let heard: BTreeSet<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| {
                mutual.contains(peer)
                    || (self.heard_within(*peer, now, silence)
                        && self.heard.get(peer).is_some_and(|heard| heard.leased))
            })
            .chain(std::iter::once(self.id))
            .collect();
        // A voter in a later term — one that stood to form the generation while it could not
        // hear the winner — never takes this leader's tables: it stands down, for an election
        // above that term.
        let ahead = self.voters().iter().any(|voter| {
            self.heard
                .get(voter)
                .is_some_and(|heard| heard.term > self.term)
        });
        if ahead {
            self.step_down();
            return;
        }
        if let Some(since) = pending {
            if now.saturating_sub(since) > silence {
                self.step_down();
            }
            return;
        }
        let Some(table) = self.committed.clone() else {
            return;
        };
        // A leader told to drain asks itself, as the others ask it.
        if self.draining {
            self.draining_asked.insert(self.id, now);
        }
        self.joining
            .retain(|_, (at, _)| now.saturating_sub(*at) <= silence);
        self.draining_asked
            .retain(|_, at| now.saturating_sub(*at) <= silence);
        let mut joining: BTreeMap<NodeId, String> = self
            .joining
            .iter()
            .map(|(member, (_, address))| (*member, address.clone()))
            .collect();
        // A leader its table leaves out — one it accepted, never committed, then won with —
        // takes itself in: it asks nobody to, as it is the one asked.
        if !self.draining {
            joining.insert(self.id, self.address.clone());
        }
        // Those that have answered its latest heartbeats, it with them.
        let recent = self.config.heartbeat * 2;
        let answering: BTreeSet<NodeId> = self
            .peers
            .iter()
            .copied()
            .filter(|peer| self.mutual(*peer, now, recent))
            .chain(std::iter::once(self.id))
            .collect();
        // The next table, made only once something changes it: most ticks nothing does.
        let mut next = table::repair(&table, &heard, &answering, &joining, self.config.replicas);
        for member in self.draining_asked.keys() {
            let current = next.as_ref().unwrap_or(&table);
            if current.has_member(*member) && !current.is_draining(*member) {
                let changed = next.get_or_insert_with(|| Table::clone(&table));
                changed.draining.push(*member);
                changed.draining.sort_unstable();
                changed.voting = table::voting_set(changed, &heard);
            }
        }
        for (shard, (to, from)) in std::mem::take(&mut self.handed) {
            if let Some(done) = table::handed_on(next.as_ref().unwrap_or(&table), shard, to, from) {
                next = Some(done);
            }
        }
        if let Some(moves) = table::plan_moves(
            next.as_ref().unwrap_or(&table),
            &heard,
            &answering,
            MOVES_AT_ONCE,
        ) {
            next = Some(moves);
        }
        // A leader leaving hands the lead on once the table it publishes no longer has it vote.
        let Some(mut next) = next.filter(|next| !next.same_content(&table)) else {
            // Not with moves outstanding: they would wait out an election for their last step.
            if table.is_draining(self.id) && !table.votes(self.id) && table.moves.is_empty() {
                self.abdicate(now, out);
            }
            return;
        };
        next.term = self.term;
        next.version = table.version + 1;
        self.publish(next, now, out);
    }

    /// As leader, its work now rather than at its next tick.
    fn lead_now(&mut self, now: Time, out: &mut Outbox<V>) {
        if let Role::Leader { ref pending } = self.role {
            let since = pending.as_ref().map(|pending| pending.since);
            self.lead(now, since, out);
        }
    }

    /// A table published by `from`: accepted, and answered, if it is ahead of what this pod
    /// has accepted, from a leader in a later term or in its own.
    fn on_publish(&mut self, from: NodeId, table: Table, now: Time, out: &mut Outbox<V>) {
        self.tabled = Some((from, now));
        let ahead = table.term > self.term || (table.term == self.term && !self.is_leader());
        if ahead && table.stamp() > self.accepted_stamp() {
            if table.term > self.term {
                self.term = table.term;
            }
            self.seen_term = self.seen_term.max(table.term);
            if self.is_leader()
                || matches!(
                    self.role,
                    Role::Candidate { .. } | Role::PreCandidate { .. }
                )
            {
                self.role = Role::Follower;
            }
            self.leader = Some(from);
            let stamp = table.stamp();
            self.accepted = Some(Arc::new(table));
            out.push((from, Message::PublishAck { stamp }));
        }
    }

    /// Publishes `table` to every member and every pod it knows, accepting it itself: to each
    /// that has said it accepted the table committed before, or whose latest heartbeat says
    /// it holds it, what changed from it; to the rest the whole table.
    fn publish(&mut self, table: Table, now: Time, out: &mut Outbox<V>) {
        let table = Arc::new(table);
        self.accepted = Some(Arc::clone(&table));
        let mut acks = BTreeSet::new();
        acks.insert(self.id);
        let whole = Message::Publish {
            table: Table::clone(&table),
        };
        let changes = self.committed.as_ref().map(|base| {
            let message = Message::PublishChanges {
                base: base.stamp(),
                changes: table.changes_from(base),
            };
            (base.stamp(), message)
        });
        let to: BTreeSet<NodeId> = table
            .members
            .keys()
            .copied()
            .chain(self.peers.iter().copied())
            .filter(|peer| *peer != self.id)
            .collect();
        for peer in to {
            let holds = |base: Stamp| {
                self.accepted_by.get(&peer) == Some(&base)
                    || self
                        .heard
                        .get(&peer)
                        .is_some_and(|heard| heard.committed == base)
            };
            let message = match &changes {
                Some((base, message)) if holds(*base) => message.clone(),
                _ => whole.clone(),
            };
            out.push((peer, message));
        }
        self.role = Role::Leader {
            pending: Some(Pending {
                table,
                acks,
                since: now,
            }),
        };
        self.try_commit(now, out);
    }

    /// Commits the pending table once its acks are a majority of the committed voting set and
    /// of its own.
    fn try_commit(&mut self, now: Time, out: &mut Outbox<V>) {
        let Role::Leader {
            pending: Some(pending),
        } = &self.role
        else {
            return;
        };
        let old = self.committed.as_ref().map_or_else(
            || pending.table.voting.clone(),
            |table| table.voting.clone(),
        );
        let acked = |member: NodeId| pending.acks.contains(&member);
        if !is_quorum(&old, acked) || !is_quorum(&pending.table.voting, acked) {
            return;
        }
        let table = Arc::clone(&pending.table);
        self.role = Role::Leader { pending: None };
        let stamp = table.stamp();
        let to: BTreeSet<NodeId> = table
            .members
            .keys()
            .copied()
            .chain(self.peers.iter().copied())
            .filter(|peer| *peer != self.id)
            .collect();
        out.extend(to.into_iter().map(|peer| (peer, Message::Commit { stamp })));
        self.apply(table, now, out);
    }

    /// Hands the lead on: forgets it leads, so that the others' pre-votes are granted.
    fn abdicate(&mut self, now: Time, out: &mut Outbox<V>) {
        self.step_down();
        // Its heartbeats now say it follows nobody, which tells the voters at once.
        let Message::Heartbeat {
            term,
            committed,
            fresh,
            sent,
            wall,
            hears,
            leased,
            ..
        } = self.heartbeat(now)
        else {
            return;
        };
        let beat = Message::Heartbeat {
            term,
            leader: None,
            committed,
            fresh,
            sent,
            wall,
            hears,
            leased,
        };
        out.extend(self.peers.iter().map(|peer| (*peer, beat.clone())));
    }

    fn step_down(&mut self) {
        self.role = Role::Follower;
        if self.leader == Some(self.id) {
            self.leader = None;
        }
        self.elect_at = None;
    }

    /// Applies a committed `table`: its own shards as it now has them, copies sent to new
    /// replicas of its shards, and what it holds for shards no longer its own dropped.
    fn apply(&mut self, table: Arc<Table>, now: Time, out: &mut Outbox<V>) {
        let before = self.committed.take();
        self.swap(before.as_deref(), &table, now, out);
        // Reports on moves the table no longer asks for: done, or dropped.
        self.handed
            .retain(|shard, (to, from)| table.asked(*shard) == Some((*to, *from)));
        for (index, shard) in table.shards.iter().enumerate() {
            let Ok(id) = ShardId::try_from(index) else {
                continue;
            };
            let mine = shard.owner == Some(self.id);
            let copy = shard.replicas.contains(&self.id);
            let taking = table.move_of(id) == Some(self.id);
            // A copy sent under a later table than this one names this pod a copy there: kept,
            // as it said it holds it, and the owner may hand the shard to it on that word.
            let ahead = self
                .copied_under
                .get(&id)
                .is_some_and(|under| *under > table.stamp());
            if !mine && !copy && !taking && !ahead {
                // What it held of the shard dropped, so is how far it held it: an owner that
                // never saw it taken off, sending the next copy, is told it holds nothing.
                self.data.remove(&id);
                self.dirty.remove(&id);
                self.have.remove(&id);
                self.copied_under.remove(&id);
            }
            if !mine {
                self.announced.remove(&id);
                continue;
            }
            // A replica new to this shard is sent all it holds; taking the shard, every replica
            // is, its copies numbered afresh.
            let had: Option<Vec<NodeId>> = before
                .as_ref()
                .and_then(|before| before.shards.get(index))
                .filter(|before| before.owner == Some(self.id))
                .map(|before| before.replicas.clone());
            if had.is_none() {
                self.versions.insert(id, 1);
                self.acked.retain(|(shard, _), _| *shard != id);
                self.behind.retain(|(shard, _), _| *shard != id);
                self.unacked.retain(|(shard, _), _| *shard != id);
                self.sending.retain(|(_, shard), _| *shard != id);
            }
            let version = self.versions.get(&id).copied().unwrap_or(1);
            let had = had.unwrap_or_default();
            for replica in shard
                .replicas
                .iter()
                .filter(|replica| !had.contains(replica))
            {
                let assignment = self.assign(id, *replica);
                let copy =
                    Sending::new((shard.term, assignment), None, version, table.stamp(), None);
                self.start(*replica, id, copy, now, out);
            }
        }
        let still = |(shard, replica): &(ShardId, NodeId)| {
            table.owner(*shard) == Some(self.id) && copies_to(&table, *shard).contains(replica)
        };
        self.acked.retain(|copy, _| still(copy));
        self.behind.retain(|copy, _| still(copy));
        self.unacked.retain(|copy, _| still(copy));
        self.assignments.retain(|copy, _| still(copy));
        self.sending
            .retain(|(replica, shard), _| still(&(*shard, *replica)));
        for member in table.members.keys() {
            self.add_peer(*member);
        }
        if self
            .accepted
            .as_ref()
            .is_none_or(|accepted| accepted.stamp() < table.stamp())
        {
            self.accepted = Some(Arc::clone(&table));
        }
        // Left out, it must become ready again as a new member does, though a table taking it
        // back come before its next tick.
        if !table.has_member(self.id) {
            self.became_ready = false;
        }
        self.committed = Some(table);
        // A move it is asked for starts now, not at its next cycle.
        self.hand_on(now, out);
    }

    /// As an owner: what changed in each of its shards since its last copies, to their
    /// replicas.
    fn replicate(&mut self, now: Time, out: &mut Outbox<V>) {
        let Some(table) = self.committed.clone() else {
            return;
        };
        for (shard, keys) in std::mem::take(&mut self.dirty) {
            let Some(entry) = table.shards.get(usize::from(shard)) else {
                continue;
            };
            if entry.owner != Some(self.id) {
                continue;
            }
            let version = self.versions.entry(shard).or_insert(1);
            let after = *version;
            *version += 1;
            let version = *version;
            let (mut entries, dropped) = self.split(shard, keys.iter().copied());
            // A rolling window's slots changed alone: slots merge each by the larger, so a part
            // is as safe as the whole, which a repair still sends.
            for (key, kept) in &mut entries {
                if let Some(changed) = self.parts_changed.remove(&(shard, *key)) {
                    kept.value = kept.value.only(&changed);
                }
            }
            self.parts_changed
                .retain(|(changed, _), _| *changed != shard);
            let held = self.data.get(&shard).map_or(0, BTreeMap::len);
            let bytes = part_bytes(&entries, &dropped);
            let alone = entries.len() + dropped.len() <= 1 || bytes <= self.config.part_bytes;
            for replica in &copies_to(&table, shard) {
                let mut assignment = self.assignment(shard, *replica);
                let unacked = self.unacked.entry((shard, *replica)).or_default();
                unacked.insert(version, keys.clone());
                if unacked.len() > UNACKED_MOST {
                    // The two oldest kept as one, under the later number: an answer covering
                    // that number covers both.
                    if let (Some((_, older)), Some((number, newer))) =
                        (unacked.pop_first(), unacked.pop_first())
                    {
                        unacked.insert(number, older.union(&newer).copied().collect());
                    }
                    // Unanswered so long that the keys to send it again outnumber those the
                    // shard holds, gone ones among them: a new assignment in their place, the
                    // repair sending it the whole shard, so that what it keeps stays within
                    // the shard's size.
                    let kept: BTreeSet<&u64> = unacked.values().flatten().collect();
                    if kept.len() > held {
                        assignment = self.assign(shard, *replica);
                    }
                }
                let under = self.committed_stamp();
                if alone {
                    let copy = Message::Replicate {
                        shard,
                        term: entry.term,
                        assignment,
                        after: Some(after),
                        version,
                        under,
                        entries: entries.clone(),
                        dropped: dropped.clone(),
                        part: 0,
                        more: false,
                    };
                    out.push((*replica, copy));
                } else if !self.sending.contains_key(&(*replica, shard)) {
                    // Changes of many parts go as a whole copy of many does, a few parts at a
                    // time, so that it has one copy of many parts going to a replica of a shard
                    // at most. With one going, they wait in `unacked` for the repair after it,
                    // as changes the replica could not take before it would.
                    let copy = Sending::new(
                        (entry.term, assignment),
                        Some(after),
                        version,
                        under,
                        Some(keys.clone()),
                    );
                    self.start(*replica, shard, copy, now, out);
                }
            }
        }
        // A replica that has said no more for `REPAIR_AFTER` cycles while behind — a copy lost, or its
        // answer — is sent again what changed since it last said, as one copy following that,
        // and again every `REPAIR_AFTER` cycles until it says it has caught up; all the shard holds if
        // it has said nothing yet.
        for (index, entry) in table.shards.iter().enumerate() {
            let Ok(shard) = ShardId::try_from(index) else {
                continue;
            };
            if entry.owner != Some(self.id) {
                continue;
            }
            let version = self.versions.get(&shard).copied().unwrap_or(1);
            for replica in &copies_to(&table, shard) {
                let copy = (shard, *replica);
                if self.acked.get(&copy).copied().unwrap_or(0) >= version {
                    self.behind.remove(&copy);
                    continue;
                }
                // A copy in parts going to it is its repair, sent again part by part.
                if self.sending.contains_key(&(*replica, shard)) {
                    continue;
                }
                let since = *self.behind.entry(copy).or_insert(now);
                if now.saturating_sub(since) < self.config.cycle.saturating_mul(REPAIR_AFTER) {
                    continue;
                }
                self.behind.insert(copy, now);
                let assignment = self.assignment(shard, *replica);
                let acked = self.acked.get(&copy).copied().unwrap_or(0);
                let (after, keys) = if acked == 0 {
                    (None, None)
                } else {
                    let keys: BTreeSet<u64> = self
                        .unacked
                        .get(&copy)
                        .into_iter()
                        .flat_map(BTreeMap::values)
                        .flatten()
                        .copied()
                        .collect();
                    (Some(acked), Some(keys))
                };
                let under = self.committed_stamp();
                let copy = Sending::new((entry.term, assignment), after, version, under, keys);
                self.start(*replica, shard, copy, now, out);
            }
        }
        self.pace(now, out);
    }

    /// As replica: to the owner of each shard its table makes it a replica of and of which it
    /// holds nothing from that owner, that it lacks it — unasked, so that an owner that never
    /// saw it taken off and put back, no copy to send, learns it.
    fn say_lacking(&self, out: &mut Outbox<V>) {
        let Some(table) = &self.committed else {
            return;
        };
        for (index, entry) in table.shards.iter().enumerate() {
            let Ok(shard) = ShardId::try_from(index) else {
                continue;
            };
            let Some(owner) = entry.owner.filter(|owner| *owner != self.id) else {
                continue;
            };
            let holds = self
                .have
                .get(&shard)
                .is_some_and(|(from, term, _, _)| (*from, *term) == (owner, entry.term));
            if entry.replicas.contains(&self.id) && !holds {
                out.push((
                    owner,
                    Message::Lacking {
                        shard,
                        term: entry.term,
                    },
                ));
            }
        }
    }

    /// A new assignment of `replica` to `shard`: nothing it said before counts.
    fn assign(&mut self, shard: ShardId, replica: NodeId) -> u64 {
        self.next_assignment += 1;
        let copy = (shard, replica);
        self.assignments.insert(copy, self.next_assignment);
        self.acked.remove(&copy);
        self.behind.remove(&copy);
        self.unacked.remove(&copy);
        self.sending.remove(&(replica, shard));
        self.next_assignment
    }

    /// Sends `copy` of `shard` to `replica`, in place of any going to it: at once if it fits one
    /// part, as every copy did before copies went in parts; else part by part, as
    /// [`pace`](Self::pace) makes room.
    fn start(
        &mut self,
        replica: NodeId,
        shard: ShardId,
        copy: Sending,
        now: Time,
        out: &mut Outbox<V>,
    ) {
        self.sending.remove(&(replica, shard));
        let held = self.data.get(&shard);
        let (keys, bytes) = match &copy.keys {
            None => held.map_or((0, 0), |held| {
                (held.len(), held.values().map(Kept::bytes).sum())
            }),
            Some(keys) => (
                keys.len(),
                keys.iter()
                    .map(|key| held.and_then(|held| held.get(key)).map_or(8, Kept::bytes))
                    .sum(),
            ),
        };
        if keys <= 1 || bytes <= self.config.part_bytes {
            let (entries, dropped) = match &copy.keys {
                None => (self.all_of(shard), Vec::new()),
                Some(keys) => self.split(shard, keys.iter().copied()),
            };
            out.push((replica, copy.part(shard, (0, false), entries, dropped)));
            return;
        }
        self.sending
            .insert((replica, shard), Sending { at: now, ..copy });
        self.pace(now, out);
    }

    /// Sends the parts of copies going in parts that their answers make room for — at most
    /// `window_bytes` unanswered to any one replica, across its shards, in shard order — and
    /// again, from the first, those unanswered for [`PART_AGAIN`] cycles, each over the keys it
    /// was.
    fn pace(&mut self, now: Time, out: &mut Outbox<V>) {
        let again = self.config.cycle.saturating_mul(PART_AGAIN);
        let most = self.config.part_bytes;
        let window = self.config.window_bytes;
        let mut unanswered = self.unanswered();
        for ((replica, shard), sending) in &mut self.sending {
            let held = self.data.get(shard);
            let keys = sending.keys.as_ref();
            if !sending.sent.is_empty() && now.saturating_sub(sending.at) >= again {
                sending.at = now;
                for sent in &sending.sent {
                    // A part that covered no key covers none again.
                    let (entries, dropped, _) = match sent.to {
                        Some(to) => cut_range(held, keys, sent.after, Some(to), None),
                        None => (Vec::new(), Vec::new(), None),
                    };
                    let part = (sent.part, sent.more);
                    out.push((*replica, sending.part(*shard, part, entries, dropped)));
                }
            }
            let going = unanswered.entry(*replica).or_default();
            while !sending.ended && (*going == 0 || going.saturating_add(most) <= window) {
                let (entries, dropped, to) =
                    cut_range(held, keys, sending.reached, None, Some(most));
                let bytes = part_bytes(&entries, &dropped);
                let more = to.is_some_and(|to| key_after(held, keys, to));
                let part = sending.next;
                out.push((
                    *replica,
                    sending.part(*shard, (part, more), entries, dropped),
                ));
                sending.sent.push(Sent {
                    part,
                    after: sending.reached,
                    to,
                    more,
                    bytes,
                });
                sending.reached = to.or(sending.reached);
                sending.next = part.saturating_add(1);
                sending.ended = !more;
                *going = going.saturating_add(bytes);
            }
        }
    }

    /// The bytes of copies of many parts it has unanswered to each replica, by the store's
    /// count.
    fn unanswered(&self) -> BTreeMap<NodeId, u64> {
        let mut unanswered: BTreeMap<NodeId, u64> = BTreeMap::new();
        for ((replica, _), sending) in &self.sending {
            let bytes = sending.sent.iter().map(|sent| sent.bytes).sum::<u64>();
            let going = unanswered.entry(*replica).or_default();
            *going = going.saturating_add(bytes);
        }
        unanswered
    }

    /// The most bytes of copies of many parts it has unanswered to any one replica, by the
    /// store's count: never past `window_bytes`, but for a part of one key past it.
    #[must_use]
    pub fn most_unanswered_bytes(&self) -> u64 {
        self.unanswered().into_values().max().unwrap_or(0)
    }

    /// A part of a copy out of turn: one it has taken already, its answer lost, is answered
    /// again with how far it has the parts, so that the owner goes on. One ahead of a part lost
    /// is left for the owner to send again from that one.
    fn answer_again(
        shard: ShardId,
        (term, assignment, version): (u64, u64, u64),
        part: u32,
        next: Option<u32>,
    ) -> Option<Message<V>> {
        next.filter(|next| part < *next).map(|next| Message::Took {
            shard,
            term,
            assignment,
            version,
            part: next.saturating_sub(1),
        })
    }

    /// The assignment of `replica` to `shard`, a new one if it has none.
    fn assignment(&mut self, shard: ShardId, replica: NodeId) -> u64 {
        match self.assignments.get(&(shard, replica)) {
            Some(assignment) => *assignment,
            None => self.assign(shard, replica),
        }
    }

    /// Forgets every key whose time has come by its wall clock, owned or a copy, sending
    /// nothing: every pod holding it forgets it by its own, and from then the key is decided
    /// as a new one, as it would be had it been kept.
    fn sweep(&mut self, now: Time) {
        let wall = self.wall(now);
        for (shard, keys) in &mut self.data {
            let before = keys.len();
            keys.retain(|_, kept| kept.forget_at > wall);
            if keys.len() < before {
                self.forgotten += (before - keys.len()) as u64;
                if let Some(dirty) = self.dirty.get_mut(shard) {
                    dirty.retain(|key| keys.contains_key(key));
                }
            }
        }
        let data = &self.data;
        self.parts_changed
            .retain(|(shard, key), _| data.get(shard).is_some_and(|keys| keys.contains_key(key)));
        // Copies coming in parts that will not finish: their shard's term gone by, or, for a
        // replica's, sent by a pod its table no longer names the owner.
        let table = self.committed.as_deref();
        let known = |shard: ShardId| {
            table
                .and_then(|table| table.shards.get(usize::from(shard)))
                .map_or(0, |entry| entry.term)
        };
        self.taking.retain(|(shard, from), taking| {
            taking.copy.0 >= known(*shard)
                && table.is_some_and(|table| table.owner(*shard) == Some(*from))
        });
        self.handing_in
            .retain(|(shard, _), (term, _)| *term >= known(*shard));
        self.evict();
    }

    /// Past `max_key_bytes`, evicts keys, owned or copies, those it may forget soonest first —
    /// as EdgeRush's pods evict the keys whose buckets are full soonest — down to nine tenths,
    /// so that its memory stays within the cap. One of its own is a change, so that its
    /// replicas drop it with the next copy and hold what it holds; a copy goes with no
    /// message, its owner's to keep, lost only should that owner then fail over, an
    /// overshoot. A pod evicting is under specified, its counts not guaranteed either way
    /// (`membership-design.md`).
    fn evict(&mut self) {
        let Some(cap) = self.config.max_key_bytes else {
            return;
        };
        let mut bytes = self.key_bytes();
        if bytes <= cap {
            return;
        }
        let mut held: Vec<(Time, ShardId, u64, u64)> = self
            .data
            .iter()
            .flat_map(|(shard, keys)| {
                keys.iter()
                    .map(|(key, kept)| (kept.forget_at, *shard, *key, kept.bytes()))
            })
            .collect();
        held.sort_unstable();
        let goal = cap / 10 * 9;
        for (_, shard, key, size) in held {
            if bytes <= goal {
                break;
            }
            if let Some(keys) = self.data.get_mut(&shard) {
                keys.remove(&key);
            }
            let own = self
                .committed
                .as_ref()
                .is_some_and(|table| table.owner(shard) == Some(self.id));
            if own {
                self.dirty.entry(shard).or_default().insert(key);
            }
            self.evicted += 1;
            bytes = bytes.saturating_sub(size);
        }
    }

    /// Of `keys` of `shard`, those it holds, with their values, and those it no longer holds,
    /// evicted: what a copy of the changes carries.
    fn split(
        &self,
        shard: ShardId,
        keys: impl Iterator<Item = u64>,
    ) -> (Vec<(u64, Kept<V>)>, Vec<u64>) {
        let held = self.data.get(&shard);
        let mut entries = Vec::new();
        let mut dropped = Vec::new();
        for key in keys {
            match held.and_then(|data| data.get(&key)) {
                Some(kept) => entries.push((key, kept.clone())),
                None => dropped.push(key),
            }
        }
        (entries, dropped)
    }

    /// Every key it holds of `shard`, with its value.
    fn all_of(&self, shard: ShardId) -> Vec<(u64, Kept<V>)> {
        self.data
            .get(&shard)
            .map(|keys| {
                keys.iter()
                    .map(|(key, value)| (*key, value.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// As an owner asked to move a shard: stops answering for it and sends all it holds to the
    /// member taking it, again every [`HANDOFF_AGAIN`] cycles until the table moves it, or drops the
    /// move and the shard is answered for again.
    fn hand_on(&mut self, now: Time, out: &mut Outbox<V>) {
        let Some(table) = self.committed.clone() else {
            return;
        };
        let wait = self.config.cycle.saturating_mul(HANDOFF_WAIT);
        let again = self.config.cycle.saturating_mul(HANDOFF_AGAIN);
        // Waited long enough for an old owner's last changes — it gone since — the shard is
        // served without them.
        self.awaiting
            .retain(|_, since| now.saturating_sub(*since) < wait);
        self.announced
            .retain(|shard, (to, from, _)| table.asked(*shard) == Some((*to, *from)));
        // Not a shard it waits for the last changes of: handed on before they came, they
        // would reach it under a shard term gone by, and be lost.
        let moves: Vec<(ShardId, NodeId, Stamp)> = table
            .moves
            .iter()
            .copied()
            .filter(|(shard, _, _)| {
                table.owner(*shard) == Some(self.id) && !self.awaiting.contains_key(shard)
            })
            .collect();
        for (shard, to, from) in moves {
            let copy = (shard, to);
            let term = table
                .shards
                .get(usize::from(shard))
                .map_or(0, |entry| entry.term);
            // Catching up, the shard served throughout: the taker copied as a replica is, from
            // all the shard holds, until it holds all but a few cycles' changes.
            if !self.assignments.contains_key(&copy) {
                let assignment = self.assign(shard, to);
                let version = self.versions.get(&shard).copied().unwrap_or(1);
                let copy = Sending::new((term, assignment), None, version, table.stamp(), None);
                self.start(to, shard, copy, now, out);
                continue;
            }
            let whole = self.acked.get(&copy).is_some_and(|acked| *acked > 0);
            let behind = self.unacked.get(&copy).map_or(0, BTreeMap::len);
            let told = self
                .announced
                .get(&shard)
                .is_some_and(|(_, _, at)| now.saturating_sub(*at) < again);
            if !whole || behind > CAUGHT_UP || told {
                continue;
            }
            // Caught up: the taker told, to tell the leader itself — a taker gone tells nobody,
            // and the shard stays here, where it is served still — again every HANDOFF_AGAIN cycles
            // until the table moves the shard.
            self.announced.insert(shard, (to, from, now));
            out.push((to, Message::CaughtUp { shard, from }));
        }
    }

    /// A table applied that moves shards by plan: as their old owner, it sends each taker the
    /// changes it has not said it holds and those not yet copied, having stopped serving them
    /// as the table names another; as a taker named the owner, it waits for those before it
    /// serves, unless they came first.
    fn swap(&mut self, before: Option<&Table>, table: &Table, now: Time, out: &mut Outbox<V>) {
        let Some(before) = before else {
            return;
        };
        for (index, entry) in table.shards.iter().enumerate() {
            let Ok(shard) = ShardId::try_from(index) else {
                continue;
            };
            let Some(to) = before.move_of(shard) else {
                continue;
            };
            let moved = before.owner(shard) != entry.owner && entry.owner == Some(to);
            if !moved {
                continue;
            }
            if before.owner(shard) == Some(self.id) {
                let keys: BTreeSet<u64> = self
                    .unacked
                    .get(&(shard, to))
                    .into_iter()
                    .flat_map(BTreeMap::values)
                    .flatten()
                    .chain(self.dirty.get(&shard).into_iter().flatten())
                    .copied()
                    .collect();
                let (entries, dropped) = self.split(shard, keys.into_iter());
                let last = Message::Handoff {
                    shard,
                    term: entry.term,
                    entries,
                    dropped,
                    part: 0,
                    more: false,
                };
                send_in_parts(out, to, last, self.config.part_bytes);
            } else if to == self.id && self.handed_in.get(&shard) != Some(&entry.term) {
                self.awaiting.insert(shard, now);
            }
        }
        let me = self.id;
        self.awaiting
            .retain(|shard, _| table.owner(*shard) == Some(me));
        // Last changes under a term newer than this table's for their shard came first, from a
        // table not yet applied: a taker behind may apply tables between that name it neither
        // mover nor owner (#281). Once the tables catch up with their term, the shard handed
        // to another, they go.
        self.handed_in.retain(|shard, term| {
            table.move_of(*shard) == Some(me)
                || table.owner(*shard) == Some(me)
                || table
                    .shards
                    .get(usize::from(*shard))
                    .is_some_and(|entry| *term > entry.term)
        });
    }

    /// A message from `from`, at `now`.
    pub fn receive(&mut self, from: NodeId, message: Message<V>, now: Time) -> Outbox<V> {
        let mut out = Outbox::<V>::new();
        match message {
            Message::Heartbeat {
                term,
                leader,
                committed,
                fresh,
                sent,
                wall,
                hears,
                leased,
            } => {
                let echo = hears
                    .iter()
                    .find(|(peer, _)| *peer == self.id)
                    .map(|(_, sent)| *sent);
                self.on_heartbeat(
                    from,
                    Heard {
                        at: now,
                        term,
                        leader,
                        committed,
                        fresh,
                        sent,
                        wall,
                        echo,
                        leased,
                    },
                    &mut out,
                );
            }
            Message::TableRequest => {
                if let Some(table) = &self.committed {
                    out.push((
                        from,
                        Message::TableReply {
                            table: Table::clone(table),
                        },
                    ));
                }
            }
            Message::TableReply { table } => {
                if table.stamp() > self.committed_stamp() {
                    self.apply(Arc::new(table), now, &mut out);
                }
            }
            Message::PreVote { term, accepted } => {
                let granted = term > self.term
                    && !self.hears_leader(now)
                    && accepted >= self.accepted_stamp()
                    && self.committed.is_some();
                out.push((from, Message::PreVoteReply { term, granted }));
            }
            Message::PreVoteReply { term, granted } => {
                if let Role::PreCandidate {
                    term: standing,
                    granted: votes,
                    ..
                } = &mut self.role
                    && *standing == term
                    && granted
                {
                    votes.insert(from);
                    self.after_pre_vote(now, &mut out);
                }
            }
            Message::StartJoin { term, accepted } => {
                self.seen_term = self.seen_term.max(term);
                if term > self.term {
                    self.term = term;
                    self.step_down();
                    self.leader = None;
                    // Never for a candidate behind the table it has accepted: a majority of
                    // voters holds the committed one, so whoever wins a majority holds it too.
                    if accepted < self.accepted_stamp() {
                        return out;
                    }
                    out.push((from, Message::Join { term }));
                }
            }
            Message::Join { term } => self.on_join(from, term, now, &mut out),
            Message::Publish { table } => self.on_publish(from, table, now, &mut out),
            // The changes from a table it holds, accepted or committed, as the leader knew; one
            // it holds no longer is taken for nothing: it learns the table as a pod behind
            // does.
            Message::PublishChanges { base, changes } => {
                let table = self
                    .committed
                    .iter()
                    .chain(&self.accepted)
                    .find(|held| held.stamp() == base)
                    .and_then(|held| held.with_changes(changes));
                if let Some(table) = table {
                    self.on_publish(from, table, now, &mut out);
                }
            }
            Message::PublishAck { stamp } => {
                let held = self.accepted_by.entry(from).or_default();
                *held = (*held).max(stamp);
                if let Role::Leader {
                    pending: Some(pending),
                } = &mut self.role
                    && pending.table.stamp() == stamp
                {
                    pending.acks.insert(from);
                    self.try_commit(now, &mut out);
                }
            }
            Message::Commit { stamp } => {
                self.tabled = Some((from, now));
                if let Some(accepted) = &self.accepted
                    && accepted.stamp() == stamp
                    && stamp > self.committed_stamp()
                {
                    let table = Arc::clone(accepted);
                    self.apply(table, now, &mut out);
                }
            }
            Message::Form {
                generation,
                voters,
                latest,
            } => {
                let held = self.grant.as_ref().map(|(named, _, _)| *named);
                if held == Some(generation) {
                    self.granted_at = now;
                } else if held.is_none_or(|named| generation > named)
                    && generation > generation_of(self.committed_stamp().0)
                {
                    self.grant = Some((generation, voters, latest));
                    self.granted_at = now;
                }
            }
            Message::Probe { life, round } => out.push((
                from,
                Message::Report {
                    life,
                    round,
                    committed: self.committed_stamp(),
                    voting: self
                        .committed
                        .as_ref()
                        .map(|table| table.voting.clone())
                        .unwrap_or_default(),
                },
            )),
            Message::Report { .. } => {}
            Message::JoinCluster { address } => {
                if self.is_leader() {
                    self.joining.insert(from, (now, address));
                }
            }
            Message::Drain => {
                if self.is_leader() {
                    self.draining_asked.insert(from, now);
                }
            }
            Message::Replicate {
                shard,
                term,
                assignment,
                after,
                version,
                under,
                entries,
                dropped,
                part,
                more,
            } => {
                // Each part is taken as it comes, as a copy is; the copy is held, and said
                // held, only once its last part is in with every part before it, in turn. Each
                // part short of the last is answered, so that the owner sends the next; a part
                // out of turn, one before it lost, is left for the owner to send again from that
                // one. A copy of one part leaves alone the copy of many it may come between.
                let taking = (shard, from);
                let copy = (term, assignment, version);
                let alone = part == 0 && !more;
                // A first part starts the copy, unless it is the first of the copy it is taking,
                // sent again: the parts after it it has answered for, and its owner sends on
                // from them.
                let going = self
                    .taking
                    .get(&taking)
                    .is_some_and(|taken| taken.copy == copy && !taken.done);
                if part == 0 && more && !going {
                    let first = Taking {
                        copy,
                        next: 0,
                        named: BTreeSet::new(),
                        done: false,
                    };
                    self.taking.insert(taking, first);
                }
                let next = self
                    .taking
                    .get(&taking)
                    .filter(|taken| taken.copy == copy)
                    .map(|taken| taken.next);
                if !alone && next != Some(part) {
                    out.extend(
                        Self::answer_again(shard, copy, part, next).map(|took| (from, took)),
                    );
                    return out;
                }
                let named = |entries: &[(u64, Kept<V>)]| -> BTreeSet<u64> {
                    if after.is_none() {
                        entries.iter().map(|(key, _)| *key).collect()
                    } else {
                        BTreeSet::new()
                    }
                };
                let whole = if alone {
                    Some(Taking {
                        copy,
                        next: 1,
                        named: named(&entries),
                        done: true,
                    })
                } else {
                    if let Some(taken) = self.taking.get_mut(&taking) {
                        taken.next = part.saturating_add(1);
                        taken.named.extend(named(&entries));
                    }
                    // The last kept, its keys' names taken, so that, sent again, it is answered
                    // again.
                    self.taking.get_mut(&taking).filter(|_| !more).map(|taken| {
                        taken.done = true;
                        Taking {
                            copy,
                            next: taken.next,
                            named: std::mem::take(&mut taken.named),
                            done: true,
                        }
                    })
                };
                // It holds the shard without a gap as far as this copy if it is whole, or
                // follows the last it so held from the same owner in the same assignment; it
                // says how far in that assignment, either way. A deletion is taken only from a
                // copy that follows, never out of order: the repair sends it again.
                let have = self.have.get(&shard).copied();
                let follows =
                    after.is_none_or(|after| have == Some((from, term, assignment, after)));
                // A whole copy from the owner its table names replaces what it holds of the
                // shard, keys its owner has since evicted among them, unless it holds a later
                // copy from it already, which overtook it; one from another, arriving late,
                // only adds, as it may be older than what it holds. Each part replaces the keys
                // it names, and the last drops those no part named.
                let newer = have.is_none_or(|(owner, had, given, held)| {
                    (owner, had, given) != (from, term, assignment) || held < version
                });
                let replace = after.is_none()
                    && newer
                    && self
                        .committed
                        .as_ref()
                        .is_some_and(|table| table.owner(shard) == Some(from));
                let dropped = if follows { dropped } else { Vec::new() };
                if !self.on_copy(shard, term, entries, &dropped, replace) {
                    self.taking.remove(&taking);
                    return out;
                }
                if !alone {
                    out.push((
                        from,
                        Message::Took {
                            shard,
                            term,
                            assignment,
                            version,
                            part,
                        },
                    ));
                }
                if let Some(whole) = whole {
                    if replace {
                        self.keep_only(shard, &whole.named);
                    }
                    let latest = self.copied_under.entry(shard).or_default();
                    *latest = (*latest).max(under);
                    let further = have.is_none_or(|(owner, had, given, held)| {
                        (owner, had, given) != (from, term, assignment) || held < version
                    });
                    if follows && further {
                        self.have.insert(shard, (from, term, assignment, version));
                    }
                    let held = self
                        .have
                        .get(&shard)
                        .filter(|(owner, had, given, _)| {
                            (*owner, *had, *given) == (from, term, assignment)
                        })
                        .map_or(0, |(_, _, _, held)| *held);
                    out.push((
                        from,
                        Message::Replicated {
                            shard,
                            term,
                            assignment,
                            version: held,
                        },
                    ));
                }
            }
            Message::Replicated {
                shard,
                term,
                assignment,
                version,
            } => {
                let owner = self.committed.as_ref().and_then(|table| {
                    table
                        .shards
                        .get(usize::from(shard))
                        .filter(|entry| entry.owner == Some(self.id) && entry.term == term)
                });
                let current = self.assignments.get(&(shard, from)) == Some(&assignment);
                if owner.is_some() && current {
                    let copy = (shard, from);
                    let acked = self.acked.entry(copy).or_insert(0);
                    if version > *acked {
                        // It has said more: no longer behind since before, nor owed those copies.
                        *acked = version;
                        self.behind.remove(&copy);
                        if let Some(unacked) = self.unacked.get_mut(&copy) {
                            unacked.retain(|number, _| *number > version);
                        }
                    } else if version < *acked {
                        // It says it holds less than it said: it has lost what it held — a
                        // whole copy from an owner it had the shard from before, arriving late,
                        // taken over this one — and is sent the shard again.
                        *acked = 0;
                        self.unacked.remove(&copy);
                    }
                }
            }
            Message::Took {
                shard,
                term,
                assignment,
                version,
                part,
            } => {
                if let Some(sending) = self.sending.get_mut(&(from, shard))
                    && (sending.term, sending.assignment, sending.version)
                        == (term, assignment, version)
                {
                    let before = sending.sent.len();
                    sending.sent.retain(|sent| sent.part > part);
                    let answered = sending.sent.len() < before;
                    if answered {
                        sending.at = now;
                    }
                    // Its last part answered, the copy is in.
                    if sending.ended && sending.sent.is_empty() {
                        self.sending.remove(&(from, shard));
                    }
                    if answered {
                        self.pace(now, &mut out);
                    }
                }
            }
            Message::Lacking { shard, term } => {
                let replica = self.committed.as_ref().is_some_and(|table| {
                    table.shards.get(usize::from(shard)).is_some_and(|entry| {
                        entry.owner == Some(self.id)
                            && entry.term == term
                            && entry.replicas.contains(&from)
                    })
                });
                let copy = (shard, from);
                if replica && self.acked.get(&copy).is_some_and(|acked| *acked > 0) {
                    // Whatever it said before, it holds nothing now: its repair is all the
                    // shard holds.
                    self.acked.insert(copy, 0);
                    self.unacked.remove(&copy);
                }
            }
            Message::Handoff {
                shard,
                term,
                entries,
                dropped,
                part,
                more,
            } => {
                // The old owner's last changes: served from now, or as soon as the table names
                // this pod the owner, if they come first; once their last part is in, with
                // every part before it, in turn. Should a part be lost, it serves without them
                // once it has waited for them, as when they are lost whole.
                let handing = (shard, from);
                let in_turn = part == 0 || self.handing_in.get(&handing) == Some(&(term, part));
                let taken = self.on_copy(shard, term, entries, &dropped, false);
                if taken && in_turn && more {
                    self.handing_in
                        .insert(handing, (term, part.saturating_add(1)));
                } else {
                    self.handing_in.remove(&handing);
                }
                if taken && in_turn && !more {
                    self.awaiting.remove(&shard);
                    self.handed_in.insert(shard, term);
                }
            }
            // Kept by every voter, not the leader alone: the leader may change before it acts.
            // Told it has caught up: the leader told, and every voter, as a leader may just have
            // changed; kept whether or not it leads, as a voter does every report.
            Message::CaughtUp { shard, from } => {
                let tell: BTreeSet<NodeId> = self
                    .leader
                    .into_iter()
                    .chain(self.voters())
                    .filter(|member| *member != self.id)
                    .collect();
                let done = Message::HandedOff {
                    shard,
                    to: self.id,
                    from,
                };
                out.extend(tell.into_iter().map(|member| (member, done.clone())));
                self.handed.insert(shard, (self.id, from));
                self.lead_now(now, &mut out);
            }
            Message::HandedOff { shard, to, from } => {
                self.handed.insert(shard, (to, from));
                self.lead_now(now, &mut out);
            }
        }
        out
    }

    fn on_heartbeat(&mut self, from: NodeId, heard: Heard, out: &mut Outbox<V>) {
        // Only pods send heartbeats: whoever does is a peer, clients never.
        self.add_peer(from);
        // One sent before the one it holds — delayed, or on a stream since replaced — would
        // take it back in time.
        if self
            .heard
            .get(&from)
            .is_some_and(|held| held.sent > heard.sent)
        {
            return;
        }
        if let Some(skew) = self.skew_of(&heard) {
            self.skews.insert(from, skew);
        }
        let Heard {
            at: now,
            term,
            leader,
            committed,
            echo,
            ..
        } = heard;
        let known = self.heard_within(from, now, self.config.stop_serving);
        self.heard.insert(from, heard);
        // It does not hear this pod yet, or this pod had not been hearing it: answered at once,
        // so that each knows within a round trip, not a heartbeat, that the other hears it.
        if echo.is_none() || !known {
            out.push((from, self.heartbeat(now)));
        }
        self.seen_term = self.seen_term.max(term);
        // A leader's own heartbeat in a later term: it won; follow it.
        if leader == Some(from) && term >= self.term {
            if term > self.term || !self.is_leader() {
                if term > self.term {
                    self.term = term;
                }
                if !matches!(self.role, Role::Follower) {
                    self.role = Role::Follower;
                }
                self.leader = Some(from);
                self.elect_at = None;
            }
        } else if self.leader == Some(from) && leader != Some(from) {
            // The leader it followed says it leads no more.
            self.leader = None;
        }
        if committed > self.committed_stamp() {
            out.push((from, Message::TableRequest));
        }
    }

    /// As a pre-candidate: with pre-votes from a majority, it stands in a new term.
    fn after_pre_vote(&mut self, now: Time, out: &mut Outbox<V>) {
        let Role::PreCandidate { granted, .. } = &self.role else {
            return;
        };
        if !self.majority(granted) {
            return;
        }
        if self.outdated() {
            self.role = Role::Follower;
            return;
        }
        let term = self.seen_term.max(self.term) + 1;
        self.term = term;
        self.seen_term = term;
        self.leader = None;
        let mut joins = BTreeSet::new();
        if self.voters().contains(&self.id) {
            joins.insert(self.id);
        }
        self.role = Role::Candidate {
            joins,
            since: now,
            named: None,
        };
        let ask = Message::StartJoin {
            term,
            accepted: self.accepted_stamp(),
        };
        out.extend(
            self.voters()
                .into_iter()
                .filter(|voter| *voter != self.id)
                .map(|voter| (voter, ask.clone())),
        );
        self.after_join(now, out);
    }

    fn on_join(&mut self, from: NodeId, term: u64, now: Time, out: &mut Outbox<V>) {
        if term != self.term {
            return;
        }
        let Role::Candidate { joins, .. } = &mut self.role else {
            return;
        };
        joins.insert(from);
        self.after_join(now, out);
    }

    /// As a candidate: with votes from a majority it leads, and publishes its table again in
    /// its own term — or, forming a generation of the cluster with a majority of those named,
    /// the generation's first table: the one it had from the generation before, if it had one,
    /// else a new one, either way with all those named voting.
    fn after_join(&mut self, now: Time, out: &mut Outbox<V>) {
        let Role::Candidate { joins, named, .. } = &self.role else {
            return;
        };
        let forming = named.is_some();
        let table = if let Some(named) = named {
            let voters: Vec<NodeId> = named.keys().copied().collect();
            if !is_quorum(&voters, |member| joins.contains(&member)) {
                return;
            }
            // Those that voted, with the addresses the control plane named them with: every
            // vote is from one named, as only they are asked.
            let formers: BTreeMap<NodeId, String> = named
                .iter()
                .filter(|(member, _)| joins.contains(member))
                .map(|(member, address)| (*member, address.clone()))
                .collect();
            // All of those named vote, so that every table of the generation needs a majority
            // of them; the leader works the set down to odd as usual.
            let mut table = match &self.accepted {
                Some(accepted) => {
                    let mut table = Table::clone(accepted);
                    table.version += 1;
                    for (member, address) in formers {
                        table.members.entry(member).or_insert(address);
                    }
                    table
                }
                None => table::first(
                    &formers,
                    self.config.shards,
                    self.config.replicas,
                    self.term,
                ),
            };
            table.term = self.term;
            table.voting = voters;
            table
        } else {
            if !self.majority(joins) {
                return;
            }
            let Some(accepted) = &self.accepted else {
                return;
            };
            let mut table = Table::clone(accepted);
            table.term = self.term;
            table.version += 1;
            table
        };
        if forming {
            // Its own first table, as the base of the next: no committed table to start from.
            self.committed = None;
        }
        self.leader = Some(self.id);
        self.role = Role::Leader { pending: None };
        self.publish(table, now, out);
    }

    /// A copy of `shard` at shard term `term`: refused if this pod knows the shard under a
    /// later term, else, if `replace`, each key's value replaced by its value in `entries`, or
    /// merged with what it holds; the keys `dropped` dropped. Whether it was taken.
    /// Owning the shard — a taker given the old owner's last changes after its replicas were
    /// sent all it held — a key raised or dropped is a change of its own, copied on to its
    /// replicas.
    fn on_copy(
        &mut self,
        shard: ShardId,
        term: u64,
        entries: Vec<(u64, Kept<V>)>,
        dropped: &[u64],
        replace: bool,
    ) -> bool {
        let entry = self
            .committed
            .as_ref()
            .and_then(|table| table.shards.get(usize::from(shard)));
        let known = entry.map_or(0, |entry| entry.term);
        if term < known {
            return false;
        }
        let mine = entry.is_some_and(|entry| entry.owner == Some(self.id));
        let keys = self.data.entry(shard).or_default();
        for (key, value) in entries {
            if replace && !mine {
                keys.insert(key, value);
                continue;
            }
            let raised = keys.entry(key).or_default().merge(&value);
            if raised && mine {
                self.dirty.entry(shard).or_default().insert(key);
                // Raised by a copy, not a slot it charged: copied on whole.
                self.parts_changed.remove(&(shard, key));
            }
        }
        for key in dropped {
            if keys.remove(key).is_some() && mine {
                self.dirty.entry(shard).or_default().insert(*key);
            }
        }
        true
    }

    /// Of `shard`, which it does not own, only the keys `named`: what is left of a whole copy
    /// taken from its owner, all its parts in, the keys the owner has since evicted gone.
    fn keep_only(&mut self, shard: ShardId, named: &BTreeSet<u64>) {
        let mine = self
            .committed
            .as_ref()
            .is_some_and(|table| table.owner(shard) == Some(self.id));
        if let Some(keys) = self.data.get_mut(&shard).filter(|_| !mine) {
            keys.retain(|key, _| named.contains(key));
        }
    }
}

/// Pushes `message`, a [`Message::Replicate`] or [`Message::Handoff`] with all its keys, to
/// `to` in parts of at most `most` bytes each by the store's count, at least one key a part;
/// any other message as it is.
fn send_in_parts<V: Held>(out: &mut Outbox<V>, to: NodeId, message: Message<V>, most: u64) {
    match message {
        Message::Replicate {
            shard,
            term,
            assignment,
            after,
            version,
            under,
            entries,
            dropped,
            ..
        } => {
            let parts = cut(entries, dropped, most);
            let count = parts.len();
            for (index, (entries, dropped)) in parts.into_iter().enumerate() {
                out.push((
                    to,
                    Message::Replicate {
                        shard,
                        term,
                        assignment,
                        after,
                        version,
                        under,
                        entries,
                        dropped,
                        part: part_number(index),
                        more: index + 1 < count,
                    },
                ));
            }
        }
        Message::Handoff {
            shard,
            term,
            entries,
            dropped,
            ..
        } => {
            let parts = cut(entries, dropped, most);
            let count = parts.len();
            for (index, (entries, dropped)) in parts.into_iter().enumerate() {
                out.push((
                    to,
                    Message::Handoff {
                        shard,
                        term,
                        entries,
                        dropped,
                        part: part_number(index),
                        more: index + 1 < count,
                    },
                ));
            }
        }
        other => out.push((to, other)),
    }
}

/// A part cut from a copy: its keys with their values, those it drops, and the last key it
/// covers, if any.
type Cut<V> = (Vec<(u64, Kept<V>)>, Vec<u64>, Option<u64>);

/// The keys of a part of a copy, after `after` and to `to` if given, of `keys` or of all
/// `held`, each with its value, or dropped if no longer held — up to `most` bytes by the
/// store's count, at least one key, if given — and the last key it covers.
fn cut_range<V: Held>(
    held: Option<&BTreeMap<u64, Kept<V>>>,
    keys: Option<&BTreeSet<u64>>,
    after: Option<u64>,
    to: Option<u64>,
    most: Option<u64>,
) -> Cut<V> {
    let range = (
        after.map_or(Bound::Unbounded, Bound::Excluded),
        to.map_or(Bound::Unbounded, Bound::Included),
    );
    let named: Box<dyn Iterator<Item = u64> + '_> = match (keys, held) {
        (Some(keys), _) => Box::new(keys.range(range).copied()),
        (None, Some(held)) => Box::new(held.range(range).map(|(key, _)| *key)),
        (None, None) => Box::new(std::iter::empty()),
    };
    let mut entries = Vec::new();
    let mut dropped = Vec::new();
    let mut bytes: u64 = 0;
    let mut last = None;
    for key in named {
        let kept = held.and_then(|held| held.get(&key));
        let size = kept.map_or(8, Kept::bytes);
        if most.is_some_and(|most| last.is_some() && bytes.saturating_add(size) > most) {
            break;
        }
        bytes = bytes.saturating_add(size);
        last = Some(key);
        match kept {
            Some(kept) => entries.push((key, kept.clone())),
            None => dropped.push(key),
        }
    }
    (entries, dropped, last)
}

/// What a part carrying `entries` and `dropped` takes by the store's count, a key dropped
/// counting as the 8 bytes of its number.
fn part_bytes<V: Held>(entries: &[(u64, Kept<V>)], dropped: &[u64]) -> u64 {
    entries
        .iter()
        .map(|(_, kept)| kept.bytes())
        .sum::<u64>()
        .saturating_add(8 * dropped.len() as u64)
}

/// Whether a copy of `keys`, or of all `held`, has a key past `key`.
fn key_after<V>(
    held: Option<&BTreeMap<u64, Kept<V>>>,
    keys: Option<&BTreeSet<u64>>,
    key: u64,
) -> bool {
    let past = (Bound::Excluded(key), Bound::Unbounded);
    match (keys, held) {
        (Some(keys), _) => keys.range(past).next().is_some(),
        (None, Some(held)) => held.range(past).next().is_some(),
        (None, None) => false,
    }
}

/// A part's number from its place: past `u32::MAX` parts, which a shard's keys never come
/// near, the number stays there, and the receiver, seeing a part out of turn, answers nothing
/// rather than hold a copy it may lack part of.
fn part_number(index: usize) -> u32 {
    u32::try_from(index).unwrap_or(u32::MAX)
}

/// A copy's parts, each its keys with their values and the keys it drops.
type Parts<V> = Vec<(Vec<(u64, Kept<V>)>, Vec<u64>)>;

/// The parts of a copy: its `entries` in order, then the keys `dropped`, each part at most
/// `most` bytes by the store's count — a key dropped counting as the 8 bytes of its number —
/// or one key if that one alone passes it; one part, empty, for none, as an empty copy still
/// says how far the replica holds the shard.
fn cut<V: Held>(entries: Vec<(u64, Kept<V>)>, dropped: Vec<u64>, most: u64) -> Parts<V> {
    let mut parts = vec![(Vec::new(), Vec::new())];
    let mut bytes: u64 = 0;
    let mut room = |size: u64, parts: &mut Parts<V>| {
        if bytes > 0 && bytes.saturating_add(size) > most {
            parts.push((Vec::new(), Vec::new()));
            bytes = 0;
        }
        bytes = bytes.saturating_add(size);
    };
    for (key, kept) in entries {
        room(kept.bytes(), &mut parts);
        if let Some((entries, _)) = parts.last_mut() {
            entries.push((key, kept));
        }
    }
    for key in dropped {
        room(8, &mut parts);
        if let Some((_, dropped)) = parts.last_mut() {
            dropped.push(key);
        }
    }
    parts
}

/// Moves the leader asks for at once: sixteen took a rollout of three pods over 1 024
/// shards 1.95 s where four took 2.5 s, refusing nothing either way, and published half as
/// much, each table carrying four times the moves.
const MOVES_AT_ONCE: usize = 16;

/// Whom an owner copies `shard` to: its replicas, and a member taking it, catching up.
fn copies_to(table: &Table, shard: ShardId) -> Vec<NodeId> {
    let mut to: Vec<NodeId> = table
        .shards
        .get(usize::from(shard))
        .map(|entry| entry.replicas.clone())
        .unwrap_or_default();
    if let Some(taker) = table.move_of(shard)
        && !to.contains(&taker)
    {
        to.push(taker);
    }
    to
}

/// How many silences a peer no table names goes unheard before it is forgotten.
const FORGET_AFTER: Time = 2;

/// How many copy cycles an owner whose taker has caught up waits for the table to move the
/// shard before it tells the leader again: 50 ms at the defaults.
const HANDOFF_AGAIN: Time = 5;

/// How many copy cycles a taker named the owner waits for the old owner's last changes before
/// it serves the shard without them, the old owner gone since, they are not coming: 50 ms at
/// the defaults.
const HANDOFF_WAIT: Time = 5;

/// The most cycles' copies a taker may have yet to answer for its owner to hand it the shard:
/// what it lacks then goes with the handoff.
const CAUGHT_UP: usize = 4;

/// How many copy cycles an owner waits for a replica to say it has caught up before it sends it
/// all the shard holds: 50 ms at the defaults.
const REPAIR_AFTER: Time = 5;

/// How many copy cycles an owner waits for a part's answer before it sends the parts of the copy
/// unanswered again: 250 ms at the defaults. A part of 1 MiB takes 84 ms to send at 100 Mbit/s,
/// two 168 ms, so that it sends nothing for nothing above about 67 Mbit/s, and a part lost costs
/// a quarter of a second.
const PART_AGAIN: Time = 25;

/// The most unanswered copies an owner keeps a record of for one replica: past it, the two
/// oldest are kept as one, so that a repair sends a few keys more, and the record stays
/// within the shard's keys however long a replica goes unanswering.
const UNACKED_MOST: usize = 64;

/// How often a pod forgets the keys whose time has come, as EdgeRush's pods sweep theirs: a
/// key is kept up to this long past its time, which changes nothing it decides.
const SWEEP: Time = 1_000_000_000;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kept::Counts;

    type Node = super::Node<Counts>;
    type Outbox = super::Outbox<Counts>;

    /// A key's count, as the store's tests keep it.
    trait CountOf {
        fn count(&self, key: u64) -> Option<u64>;
    }

    impl CountOf for Node {
        fn count(&self, key: u64) -> Option<u64> {
            self.value(key).map(|value| value.total())
        }
    }
    use crate::ms;

    /// Where pod `id` is reached, in these tests.
    fn address(id: NodeId) -> String {
        format!("pod-{id}:7000")
    }

    /// `members`, each with its address.
    fn named(members: &[NodeId]) -> BTreeMap<NodeId, String> {
        members
            .iter()
            .map(|member| (*member, address(*member)))
            .collect()
    }

    fn config() -> Config {
        let mut config = Config::defaults();
        config.shards = 4;
        config
    }

    /// Pod 2 holding the first table of pods 1, 2 and 3.
    fn member() -> Node {
        member_under(config())
    }

    fn member_under(config: Config) -> Node {
        let mut node = Node::new(2, address(2), config, [1, 3]);
        let members = named(&[1, 2, 3]);
        let table = table::first(&members, 4, 2, 1);
        let _ = node.receive(1, Message::TableReply { table }, ms(1));
        node
    }

    fn key_in(shard: ShardId) -> u64 {
        (0..).find(|key| shard_of(*key, 4) == shard).unwrap_or(0)
    }

    /// The first table carries the addresses the control plane named the pods with, and a pod
    /// asking to join gives its own, which the leader's next table carries to every member.
    #[test]
    fn the_table_carries_the_addresses_pods_were_named_with_or_gave() {
        let mut nodes: BTreeMap<NodeId, Node> = [1, 2, 3]
            .into_iter()
            .map(|id| (id, Node::new(id, address(id), config(), [1, 2, 3])))
            .collect();
        let form = Message::Form {
            generation: 1,
            voters: named(&[1, 2, 3]),
            latest: (0, 0),
        };
        // Every message is delivered a millisecond after it is sent; the control plane, 99,
        // repeats its naming every heartbeat.
        let mut sent: Vec<(NodeId, NodeId, Message<Counts>)> = Vec::new();
        for at in 1..3_000 {
            let now = ms(at);
            if at % 100 == 1 {
                sent.extend([1, 2, 3].map(|pod| (99, pod, form.clone())));
            }
            if at == 1_000 {
                assert!(
                    nodes
                        .values()
                        .all(|node| node.committed().map(|table| &table.members)
                            == Some(&named(&[1, 2, 3]))),
                    "formed with the addresses named"
                );
                nodes.insert(4, Node::new(4, address(4), config(), [1, 2, 3]));
            }
            for (from, to, message) in std::mem::take(&mut sent) {
                if let Some(node) = nodes.get_mut(&to) {
                    let out = node.receive(from, message, now);
                    sent.extend(out.into_iter().map(|(next, message)| (to, next, message)));
                }
            }
            for (id, node) in &mut nodes {
                let out = node.tick(now);
                sent.extend(out.into_iter().map(|(to, message)| (*id, to, message)));
            }
        }
        for node in nodes.values() {
            assert_eq!(
                node.committed().map(|table| &table.members),
                Some(&named(&[1, 2, 3, 4]))
            );
        }
    }

    /// Told to stop, a pod whose table has every member draining, itself among them, has no
    /// one to hand its shards to: it is drained, owning some still. One member not draining
    /// can take them, and it waits for that; so it does while its own draining is uncommitted.
    #[test]
    fn a_pod_no_member_is_left_to_take_from_is_drained() {
        for (members, draining, drained) in [
            (vec![2], vec![2], true),
            (vec![1, 2, 3], vec![1, 2, 3], true),
            (vec![2], vec![], false),
            (vec![1, 2, 3], vec![2, 3], false),
            (vec![1, 2, 3], vec![1, 3], false),
        ] {
            let mut node = Node::new(2, address(2), config(), [1, 3]);
            let mut table = table::first(&named(&members), 4, 2, 1);
            table.draining.clone_from(&draining);
            let _ = node.receive(1, Message::TableReply { table }, ms(1));
            node.drain();
            assert!(
                node.committed()
                    .is_some_and(|table| table.owned_by(2).next().is_some()),
                "{members:?} {draining:?}: owns nothing"
            );
            assert_eq!(node.drained(), drained, "{members:?} {draining:?}");
        }
    }

    /// A copy under a shard term older than the one the pod knows is refused: it is from an
    /// owner the shard has moved on from.
    #[test]
    fn a_copy_from_an_owner_moved_on_from_is_refused() {
        let mut node = member();
        let key = key_in(0);
        let copy = |term| Message::Replicate {
            shard: 0,
            term,
            assignment: 1,
            after: None,
            version: 1,
            under: (0, 0),
            entries: vec![(key, Kept::new(Counts::of(5)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, copy(0), ms(2));
        assert_eq!(node.count(key), None, "term 0, below the shard's 1");
        let _ = node.receive(1, copy(1), ms(3));
        assert_eq!(node.count(key), Some(5));
    }

    /// An owner's eviction of a key of its own reaches its replicas: its next copy of the
    /// shard to each carries the key as a deletion, so that no replica keeps it.
    #[test]
    fn an_owners_eviction_goes_to_its_replicas_as_a_deletion() {
        let mut capped = config();
        capped.max_key_bytes = Some(100);
        let mut node = Node::new(1, address(1), capped, [2, 3]);
        let members = named(&[1, 2, 3]);
        let table = table::first(&members, 4, 2, 1);
        let _ = node.receive(2, Message::TableReply { table }, ms(1));
        let keys: Vec<u64> = (0..).filter(|key| shard_of(*key, 4) == 0).take(3).collect();
        for key in &keys {
            node.load(*key, Counts::of(1));
        }
        let out: Outbox = (2..=40).flat_map(|at| node.tick(ms(at))).collect();
        assert_eq!(node.evicted(), 1);
        let gone: Vec<u64> = keys
            .iter()
            .copied()
            .filter(|key| !node.keys_of(0).contains(key))
            .collect();
        for replica in [2, 3] {
            let told = out.iter().any(|(to, message)| {
                *to == replica
                    && matches!(message, Message::Replicate { shard: 0, dropped, .. } if *dropped == gone)
            });
            assert!(told, "replica {replica} not told of {gone:?}");
        }
    }

    /// Past its cap, at its sweep, a pod evicts the keys it may forget soonest first, copies
    /// among them, down to nine tenths of the cap.
    #[test]
    fn past_its_cap_a_pod_evicts_the_keys_it_may_forget_soonest() {
        let mut capped = config();
        capped.max_key_bytes = Some(100);
        let mut node = Node::new(2, address(2), capped, [1, 3]);
        let members = named(&[1, 2, 3]);
        let table = table::first(&members, 4, 2, 1);
        let _ = node.receive(1, Message::TableReply { table }, ms(1));
        let keys: Vec<u64> = (0..).filter(|key| shard_of(*key, 4) == 0).take(3).collect();
        let kept = |forget_at| Kept {
            value: Counts::of(1),
            forget_at,
        };
        let copy = Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after: None,
            version: 1,
            under: (0, 0),
            entries: vec![
                (keys[0], kept(ms(30_000))),
                (keys[1], kept(ms(10_000))),
                (keys[2], kept(ms(20_000))),
            ],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, copy, ms(2));
        // Three keys of 40 bytes, 120 against 100: one evicted brings it to 80, under 90.
        let _ = node.tick(ms(3));
        assert_eq!(node.keys_of(0), [keys[0], keys[2]].into_iter().collect());
        assert_eq!(node.evicted(), 1);
    }

    /// A whole copy from the owner the table names replaces what a replica holds of the
    /// shard, a key the owner has since evicted among it; one from another pod, which may be
    /// older, only adds.
    #[test]
    fn a_whole_copy_from_the_owner_replaces_what_a_replica_holds() {
        let mut node = member();
        let key = key_in(0);
        let other = (key + 1..).find(|key| shard_of(*key, 4) == 0).unwrap_or(0);
        let whole = |assignment| Message::Replicate {
            shard: 0,
            term: 1,
            assignment,
            after: None,
            version: 1,
            under: (0, 0),
            entries: vec![(key, Kept::new(Counts::of(5)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        node.load(other, Counts::of(7));
        let _ = node.receive(3, whole(1), ms(2));
        assert_eq!((node.count(key), node.count(other)), (Some(5), Some(7)));
        let _ = node.receive(1, whole(2), ms(3));
        assert_eq!((node.count(key), node.count(other)), (Some(5), None));
    }

    /// A whole copy overtaken on its way by a later copy only adds: replacing would drop what
    /// the later one brought.
    #[test]
    fn a_whole_copy_overtaken_by_a_later_one_only_adds() {
        let mut node = member();
        let key = key_in(0);
        let later = (key + 1..).find(|key| shard_of(*key, 4) == 0).unwrap_or(0);
        let whole = Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after: None,
            version: 1,
            under: (0, 0),
            entries: vec![(key, Kept::new(Counts::of(5)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let next = Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after: Some(1),
            version: 2,
            under: (0, 0),
            entries: vec![(later, Kept::new(Counts::of(1)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, whole.clone(), ms(2));
        let _ = node.receive(1, next, ms(3));
        let _ = node.receive(1, whole, ms(4));
        assert_eq!(
            node.count(later),
            Some(1),
            "replaced by the copy it overtook"
        );
    }

    /// A key dropped by its owner is dropped by a replica from a copy that follows the last it
    /// holds, never from one out of order: a deletion overtaking a later charge of the key
    /// would lose it. The repair sends it again.
    #[test]
    fn a_replica_drops_a_key_only_from_a_copy_in_order() {
        let mut node = member();
        let key = key_in(0);
        let other = (key + 1..).find(|key| shard_of(*key, 4) == 0).unwrap_or(0);
        let copy = |after, version, dropped| Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after,
            version,
            under: (0, 0),
            entries: if after.is_none() {
                vec![
                    (key, Kept::new(Counts::of(5))),
                    (other, Kept::new(Counts::of(7))),
                ]
            } else {
                Vec::new()
            },
            dropped,
            part: 0,
            more: false,
        };
        let _ = node.receive(1, copy(None, 1, Vec::new()), ms(2));
        let _ = node.receive(1, copy(Some(3), 4, vec![other]), ms(3));
        assert_eq!(node.count(other), Some(7), "dropped out of order");
        let _ = node.receive(1, copy(Some(1), 2, vec![key]), ms(4));
        assert_eq!(node.count(key), None);
    }

    /// The first `count` keys of shard 0.
    fn keys_in_0(count: usize) -> Vec<u64> {
        (0..)
            .filter(|key| shard_of(*key, 4) == 0)
            .take(count)
            .collect()
    }

    /// The parts 1 sends 2 of a whole copy of shard 0 holding `entries`, cut at `most` bytes.
    fn whole_in_parts(entries: Vec<(u64, Kept<Counts>)>, most: u64) -> Outbox {
        let whole = Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after: None,
            version: 1,
            under: (0, 0),
            entries,
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let mut out = Vec::new();
        send_in_parts(&mut out, 2, whole, most);
        out
    }

    /// Of what a pod sent, the numbers of the copies it said it holds.
    fn said_held(out: &Outbox) -> Vec<u64> {
        out.iter()
            .filter_map(|(_, message)| match message {
                Message::Replicated { version, .. } => Some(*version),
                _ => None,
            })
            .collect()
    }

    /// A copy is cut into parts each within the bound, or of one key where that key alone
    /// passes it, its keys and then its deletions in order, none lost; numbered from 0, only
    /// the last saying no more follow; an empty copy is one part, empty.
    #[test]
    fn a_copy_is_cut_into_parts_within_the_bound() {
        let entries: Vec<(u64, Kept<Counts>)> = keys_in_0(10)
            .into_iter()
            .map(|key| (key, Kept::new(Counts::of(key))))
            .collect();
        let size = entries[0].1.bytes();
        let dropped = vec![7, 8, 9];
        for most in [1, size, size * 3, size * 3 + 1, u64::MAX] {
            let parts = cut(entries.clone(), dropped.clone(), most);
            for (keys, gone) in &parts {
                let bytes =
                    keys.iter().map(|(_, kept)| kept.bytes()).sum::<u64>() + 8 * gone.len() as u64;
                assert!(
                    bytes <= most || keys.len() + gone.len() == 1,
                    "{bytes} at {most}"
                );
                assert!(!keys.is_empty() || !gone.is_empty(), "an empty part");
            }
            let again: Vec<_> = parts.iter().flat_map(|(keys, _)| keys.clone()).collect();
            assert_eq!(again, entries, "at {most}");
            let gone: Vec<u64> = parts.iter().flat_map(|(_, gone)| gone.clone()).collect();
            assert_eq!(gone, dropped, "at {most}");
        }
        assert_eq!(
            cut::<Counts>(Vec::new(), Vec::new(), 1),
            vec![(Vec::new(), Vec::new())]
        );
        let numbered: Vec<(u32, bool)> = whole_in_parts(entries, size * 3)
            .iter()
            .map(|(_, message)| match message {
                Message::Replicate { part, more, .. } => (*part, *more),
                _ => (u32::MAX, false),
            })
            .collect();
        assert_eq!(numbered, [(0, true), (1, true), (2, true), (3, false)]);
    }

    /// A whole copy in parts leaves a replica holding what the same copy taken whole would:
    /// each key at the owner's value, one it held higher among them, and a key the owner has
    /// since evicted dropped, at the last part only, so that midway no key is lost. It says
    /// it holds the copy once, at its last part.
    #[test]
    fn a_whole_copy_in_parts_ends_as_the_copy_whole_does() {
        let keys = keys_in_0(4);
        let entries: Vec<(u64, Kept<Counts>)> = keys[..3]
            .iter()
            .map(|key| (*key, Kept::new(Counts::of(5))))
            .collect();
        let holding = |node: &mut Node| {
            node.load(keys[0], Counts::of(9));
            node.load(keys[3], Counts::of(7));
        };
        let mut whole = member();
        holding(&mut whole);
        for (_, part) in whole_in_parts(entries.clone(), u64::MAX) {
            let _ = whole.receive(1, part, ms(2));
        }
        let mut parted = member();
        holding(&mut parted);
        let parts = whole_in_parts(entries, 1);
        assert_eq!(parts.len(), 3);
        let mut said = Vec::new();
        for (index, (_, part)) in parts.into_iter().enumerate() {
            said.extend(said_held(&parted.receive(1, part, ms(2))));
            if index == 0 {
                assert_eq!(parted.count(keys[0]), Some(5), "not set to the owner's");
                assert_eq!(parted.count(keys[3]), Some(7), "lost midway");
            }
        }
        for key in &keys {
            assert_eq!(parted.count(*key), whole.count(*key), "key {key}");
        }
        assert_eq!(parted.count(keys[3]), None, "kept, evicted by the owner");
        assert_eq!(said, [1]);
    }

    /// A copy with a part lost on the way is not said held, nor are the keys the owner
    /// evicted dropped: its last part comes out of turn. Sent again whole, it is.
    #[test]
    fn a_copy_missing_a_part_is_left_unanswered() {
        let keys = keys_in_0(4);
        let entries: Vec<(u64, Kept<Counts>)> = keys[..3]
            .iter()
            .map(|key| (*key, Kept::new(Counts::of(5))))
            .collect();
        let mut node = member();
        node.load(keys[3], Counts::of(7));
        let mut said = Vec::new();
        for (index, (_, part)) in whole_in_parts(entries.clone(), 1).into_iter().enumerate() {
            if index != 1 {
                said.extend(said_held(&node.receive(1, part, ms(2))));
            }
        }
        assert_eq!(said, Vec::<u64>::new());
        assert_eq!(
            node.count(keys[3]),
            Some(7),
            "dropped by a copy it lacks a part of"
        );
        for (_, part) in whole_in_parts(entries, 1) {
            said.extend(said_held(&node.receive(1, part, ms(3))));
        }
        assert_eq!(said, [1]);
        assert_eq!(node.count(keys[3]), None);
    }

    /// A copy coming in parts from a pod its table does not name the owner is forgotten at
    /// the sweep, what it brought kept: one that might never finish holds no keys' names for
    /// long. The parts after the sweep come out of turn, and the copy is not said held.
    #[test]
    fn a_copy_in_parts_from_another_than_the_owner_is_forgotten_at_the_sweep() {
        let keys = keys_in_0(3);
        let entries: Vec<(u64, Kept<Counts>)> = keys
            .iter()
            .map(|key| (*key, Kept::new(Counts::of(5))))
            .collect();
        let finish = |sweep: bool| {
            let mut node = member();
            let mut said = Vec::new();
            for (index, (_, part)) in whole_in_parts(entries.clone(), 1).into_iter().enumerate() {
                said.extend(said_held(&node.receive(3, part, ms(2))));
                if index == 0 && sweep {
                    let _ = node.tick(ms(2));
                }
            }
            (said, node.count(keys[0]))
        };
        assert_eq!(finish(false), (vec![1], Some(5)));
        assert_eq!(finish(true), (Vec::new(), Some(5)));
    }

    /// A copy of what changed, in parts, is said held as far as its number at its last part
    /// only, its deletions taken as they come in turn.
    #[test]
    fn changes_in_parts_are_said_held_at_their_last_part() {
        let keys = keys_in_0(3);
        let mut node = member();
        for (_, part) in whole_in_parts(vec![(keys[2], Kept::new(Counts::of(1)))], u64::MAX) {
            let _ = node.receive(1, part, ms(2));
        }
        let changes = Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after: Some(1),
            version: 2,
            under: (0, 0),
            entries: keys[..2]
                .iter()
                .map(|key| (*key, Kept::new(Counts::of(3))))
                .collect(),
            dropped: vec![keys[2]],
            part: 0,
            more: false,
        };
        let mut parts = Vec::new();
        send_in_parts(&mut parts, 2, changes, 1);
        assert_eq!(parts.len(), 3);
        let said: Vec<Vec<u64>> = parts
            .into_iter()
            .map(|(_, part)| said_held(&node.receive(1, part, ms(3))))
            .collect();
        assert_eq!(said, [vec![], vec![], vec![2]]);
        assert_eq!(node.count(keys[2]), None);
    }

    /// An old owner's last changes in parts are in once their last part is, with every part
    /// before it, each part's keys taken as it comes; with a part lost, they are not in, and
    /// the taker waits for them as for changes lost whole.
    #[test]
    fn last_changes_in_parts_are_in_only_with_every_part() {
        let keys = keys_in_0(3);
        let term = member().committed().map_or(0, |table| table.shards[0].term);
        let last = Message::Handoff {
            shard: 0,
            term,
            entries: keys
                .iter()
                .map(|key| (*key, Kept::new(Counts::of(4))))
                .collect(),
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let mut parts = Vec::new();
        send_in_parts(&mut parts, 2, last, 1);
        assert_eq!(parts.len(), 3);
        let mut taker = member();
        for (index, (_, part)) in parts.iter().cloned().enumerate() {
            let _ = taker.receive(1, part, ms(2));
            assert_eq!(taker.handed_in.contains_key(&0), index == 2, "part {index}");
        }
        let mut lacking = member();
        for (index, (_, part)) in parts.into_iter().enumerate() {
            if index != 1 {
                let _ = lacking.receive(1, part, ms(2));
            }
        }
        assert!(!lacking.handed_in.contains_key(&0), "in without a part");
        assert_eq!(
            lacking.count(keys[2]),
            Some(4),
            "a part not taken as it came"
        );
    }

    /// Pods 1 and 2 holding the first table of 1, 2 and 3 over 8 shards, copies cut at three
    /// keys a part, two parts' worth unanswered at most, 1 holding `per` keys of count 1 in each shard it owns before it takes the
    /// table, so that its first copies to its replicas carry them; with what 1 sent 2 then.
    fn owner_of_many_parts(per: usize) -> (Node, Node, Outbox) {
        let mut config = config();
        config.shards = 8;
        config.part_bytes = 3 * Kept::new(Counts::of(1)).bytes();
        config.window_bytes = 2 * config.part_bytes;
        let first = table::first(&named(&[1, 2, 3]), 8, 2, 1);
        let mut owner = Node::new(1, address(1), config, [2, 3]);
        let owned: Vec<ShardId> = first.owned_by(1).collect();
        for shard in owned {
            for key in (0..).filter(|key| shard_of(*key, 8) == shard).take(per) {
                owner.load(key, Counts::of(1));
            }
        }
        let mut replica = Node::new(2, address(2), config, [1, 3]);
        let reply = Message::TableReply {
            table: first.clone(),
        };
        let _ = replica.receive(1, reply, ms(1));
        let sent = owner.receive(2, Message::TableReply { table: first }, ms(1));
        (owner, replica, sent)
    }

    /// The keys it owns are those of the shards its committed table names it the owner of;
    /// the rest it holds are copies, and with no table it owns none.
    #[test]
    fn it_owns_the_keys_of_the_shards_its_table_names_it_the_owner_of() {
        let mut config = config();
        config.shards = 8;
        let first = table::first(&named(&[1, 2, 3]), 8, 2, 1);
        let mut node = Node::new(1, address(1), config, [2, 3]);
        // Two keys in each shard.
        for shard in 0..8 {
            for key in (0..).filter(|key| shard_of(*key, 8) == shard).take(2) {
                node.load(key, Counts::of(1));
            }
        }
        assert_eq!(
            (node.key_count(), node.owned_key_count()),
            (16, 0),
            "no table"
        );
        let _ = node.receive(2, Message::TableReply { table: first }, ms(1));
        let owned = owned_by_1(&node).len();
        assert!(
            0 < owned && owned < 8,
            "it owns some shards, not all: {owned}"
        );
        assert_eq!((node.key_count(), node.owned_key_count()), (16, 2 * owned));
    }

    /// The shards 1 owns.
    fn owned_by_1(owner: &Node) -> Vec<ShardId> {
        owner
            .committed()
            .map(|table| table.owned_by(1).collect())
            .unwrap_or_default()
    }

    /// The parts of copies of many parts 1 has unanswered to 2.
    fn unanswered_to_2(owner: &Node) -> usize {
        owner
            .sending
            .iter()
            .filter(|((replica, _), _)| *replica == 2)
            .map(|(_, sending)| sending.sent.len())
            .sum()
    }

    /// Passes the copies 1 sent 2 to 2 and 2's answers back to 1, and so on until no more go
    /// between them, at `now`, a copy passed only if `keep` says so: the most parts 1 had
    /// unanswered to 2 meanwhile, and the copies 2 said it holds.
    fn exchange(
        owner: &mut Node,
        replica: &mut Node,
        sent: Outbox,
        now: Time,
        mut keep: impl FnMut(&Message<Counts>) -> bool,
    ) -> (usize, Vec<u64>) {
        let copies = |sent: Outbox| {
            sent.into_iter()
                .filter(|(to, message)| *to == 2 && matches!(message, Message::Replicate { .. }))
                .map(|(_, message)| message)
        };
        let mut most = unanswered_to_2(owner);
        let mut said = Vec::new();
        let mut going: std::collections::VecDeque<Message<Counts>> = copies(sent).collect();
        while let Some(copy) = going.pop_front() {
            if !keep(&copy) {
                continue;
            }
            let answers = replica.receive(1, copy, now);
            said.extend(said_held(&answers));
            for (to, answer) in answers {
                if to == 1 && matches!(answer, Message::Took { .. } | Message::Replicated { .. }) {
                    going.extend(copies(owner.receive(2, answer, now)));
                    most = most.max(unanswered_to_2(owner));
                }
            }
        }
        (most, said)
    }

    /// What `node` holds of `shard`, each key with its count.
    fn counts_of(node: &Node, shard: ShardId) -> BTreeMap<u64, u64> {
        node.keys_of(shard)
            .into_iter()
            .map(|key| (key, node.count(key).unwrap_or(0)))
            .collect()
    }

    /// Whole copies of many parts go no faster than their parts are answered: never more than
    /// two unanswered to one replica across all the shards copied to it, the next sent as one
    /// is answered, until every shard is copied whole and said held.
    #[test]
    fn copies_of_many_parts_go_no_faster_than_their_answers() {
        let (mut owner, mut replica, sent) = owner_of_many_parts(10);
        let shards = owned_by_1(&owner);
        assert!(shards.len() >= 2, "too few shards of 1's: {shards:?}");
        let ahead = sent
            .iter()
            .filter(|(to, message)| *to == 2 && matches!(message, Message::Replicate { .. }))
            .count();
        assert_eq!(ahead, 2, "sent ahead of its answers: {sent:?}");
        let (most, said) = exchange(&mut owner, &mut replica, sent, ms(2), |_| true);
        assert_eq!(most, 2);
        assert_eq!(said.len(), shards.len(), "said held: {said:?}");
        for shard in shards {
            assert_eq!(counts_of(&replica, shard), counts_of(&owner, shard));
        }
        assert_eq!(unanswered_to_2(&owner), 0);
        assert!(owner.sending.keys().all(|(replica, _)| *replica != 2));
    }

    /// Keys changed behind a whole copy's parts, added and evicted ahead of them while it goes,
    /// leave its replica, once the copy and the changes after it are in, holding what the owner
    /// holds, the key evicted gone though the replica held it before.
    #[test]
    fn keys_changed_mid_copy_end_as_the_owner_holds_them() {
        let (mut owner, mut replica, sent) = owner_of_many_parts(10);
        let shard = owned_by_1(&owner)[0];
        let keys: Vec<u64> = owner.keys_of(shard).into_iter().collect();
        let added = (keys[9] + 1..)
            .find(|key| shard_of(*key, 8) == shard)
            .unwrap_or(0);
        replica.load(keys[9], Counts::of(7));
        let first = sent
            .iter()
            .find(|(to, message)| {
                *to == 2 && matches!(message, Message::Replicate { shard: copied, part: 0, .. } if *copied == shard)
            })
            .cloned();
        let Some((_, first)) = first else {
            panic!("no first part of shard {shard}: {sent:?}");
        };
        let answers = replica.receive(1, first, ms(2));
        let held = owner.data.entry(shard).or_default();
        held.insert(keys[0], Kept::new(Counts::of(9)));
        held.insert(added, Kept::new(Counts::of(4)));
        held.remove(&keys[9]);
        owner
            .dirty
            .entry(shard)
            .or_default()
            .extend([keys[0], added, keys[9]]);
        let rest: Outbox = sent
            .into_iter()
            .filter(|(_, message)| {
                !matches!(message, Message::Replicate { shard: copied, part: 0, .. } if *copied == shard)
            })
            .chain(answers.into_iter().flat_map(|(_, answer)| owner.receive(2, answer, ms(2))))
            .collect();
        let _ = exchange(&mut owner, &mut replica, rest, ms(2), |_| true);
        assert_eq!(replica.count(keys[9]), None, "kept, evicted by the owner");
        let changes = owner.tick(ms(20));
        let _ = exchange(&mut owner, &mut replica, changes, ms(20), |_| true);
        assert_eq!(counts_of(&replica, shard), counts_of(&owner, shard));
        assert_eq!(replica.count(keys[0]), Some(9));
        assert_eq!(replica.count(added), Some(4));
    }

    /// A part lost on the way holds up its copy, which is not sent whole again: 250 ms on, the
    /// parts unanswered go again, each over the keys it covered — a key of the part lost grown
    /// meanwhile past what a part cut afresh would take with it — and the copies go on to their
    /// end, no key between two parts.
    #[test]
    fn a_copy_with_a_part_lost_goes_on_from_that_part() {
        let (mut owner, mut replica, sent) = owner_of_many_parts(10);
        let shards = owned_by_1(&owner);
        let mut lost = None;
        let (_, said) = exchange(&mut owner, &mut replica, sent, ms(2), |copy| match copy {
            Message::Replicate {
                shard,
                part: 1,
                entries,
                ..
            } if lost.is_none() => {
                lost = Some((
                    *shard,
                    entries.iter().map(|(key, _)| *key).collect::<Vec<_>>(),
                ));
                false
            }
            _ => true,
        });
        let Some((shard, keys)) = lost else {
            panic!("no part lost");
        };
        assert!(said.len() < shards.len(), "every copy in, a part lost");
        let grown = keys[keys.len() - 1];
        if let Some(kept) = owner
            .data
            .get_mut(&shard)
            .and_then(|held| held.get_mut(&grown))
        {
            kept.value.add(1);
            kept.value.add(2);
        }
        owner.dirty.entry(shard).or_default().insert(grown);
        // Ticked every 5 ms, its copies every 10: parts past the first going to 2 are parts
        // sent again, the grown key's change copy of one part besides.
        let mut again = Vec::new();
        for step in 1..=60 {
            let now = ms(2 + step * 5);
            let out = owner.tick(now);
            let to_2: Vec<(u32, bool)> = out
                .iter()
                .filter_map(|(to, message)| match message {
                    Message::Replicate { part, after, .. } if *to == 2 => {
                        Some((*part, after.is_none()))
                    }
                    _ => None,
                })
                .collect();
            assert!(
                !to_2.contains(&(0, true)),
                "sent whole again at {now}: {to_2:?}"
            );
            if to_2.iter().any(|(part, _)| *part > 0) {
                again.push(now);
            }
            let (most, _) = exchange(&mut owner, &mut replica, out, now, |_| true);
            assert!(most <= 2, "{most} unanswered");
        }
        assert!(
            again
                .first()
                .is_some_and(|at| (ms(252)..=ms(262)).contains(at)),
            "sent again at {again:?}"
        );
        for shard in shards {
            assert_eq!(counts_of(&replica, shard), counts_of(&owner, shard));
        }
        assert_eq!(unanswered_to_2(&owner), 0);
    }

    /// A part of a copy taken twice, sent again as its answer was lost, is answered again and
    /// changes nothing, the copy's last among them.
    #[test]
    fn a_part_taken_twice_is_answered_again() {
        let keys = keys_in_0(3);
        let entries: Vec<(u64, Kept<Counts>)> = keys
            .iter()
            .map(|key| (*key, Kept::new(Counts::of(5))))
            .collect();
        let parts = whole_in_parts(entries, 1);
        let mut node = member();
        let took = |out: &Outbox| -> Vec<u32> {
            out.iter()
                .filter_map(|(_, message)| match message {
                    Message::Took { part, .. } => Some(*part),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(took(&node.receive(1, parts[0].1.clone(), ms(2))), [0]);
        assert_eq!(took(&node.receive(1, parts[1].1.clone(), ms(2))), [1]);
        assert_eq!(took(&node.receive(1, parts[1].1.clone(), ms(2))), [1]);
        assert_eq!(node.taking.get(&(0, 1)).map(|taken| taken.next), Some(2));
        let last = node.receive(1, parts[2].1.clone(), ms(2));
        assert_eq!((took(&last), said_held(&last)), (vec![2], vec![1]));
        let again = node.receive(1, parts[2].1.clone(), ms(3));
        assert_eq!((took(&again), said_held(&again)), (vec![2], vec![]));
        for key in keys {
            assert_eq!(node.count(key), Some(5));
        }
    }

    /// A copy's first part sent again after the parts after it were taken and answered keeps
    /// them: answered again, it starts nothing anew, and the copy goes on from where its owner,
    /// told of them, sends it — so that a part sent again and then lost holds nothing up.
    #[test]
    fn a_first_part_sent_again_keeps_the_parts_taken() {
        let keys = keys_in_0(4);
        let entries: Vec<(u64, Kept<Counts>)> = keys
            .iter()
            .map(|key| (*key, Kept::new(Counts::of(5))))
            .collect();
        let parts = whole_in_parts(entries, 1);
        let mut node = member();
        let took = |out: &Outbox| -> Vec<u32> {
            out.iter()
                .filter_map(|(_, message)| match message {
                    Message::Took { part, .. } => Some(*part),
                    _ => None,
                })
                .collect()
        };
        let _ = node.receive(1, parts[0].1.clone(), ms(2));
        let _ = node.receive(1, parts[1].1.clone(), ms(2));
        assert_eq!(took(&node.receive(1, parts[0].1.clone(), ms(3))), [1]);
        assert_eq!(took(&node.receive(1, parts[2].1.clone(), ms(4))), [2]);
        let last = node.receive(1, parts[3].1.clone(), ms(4));
        assert_eq!((took(&last), said_held(&last)), (vec![3], vec![1]));
    }

    /// A copy of one part coming between the parts of a copy of many leaves it to go on.
    #[test]
    fn a_copy_of_one_part_leaves_a_copy_in_parts_going() {
        let keys = keys_in_0(3);
        let entries: Vec<(u64, Kept<Counts>)> = keys
            .iter()
            .map(|key| (*key, Kept::new(Counts::of(5))))
            .collect();
        let parts = whole_in_parts(entries, 1);
        let mut node = member();
        let _ = node.receive(1, parts[0].1.clone(), ms(2));
        let changes = Message::Replicate {
            shard: 0,
            term: 1,
            assignment: 1,
            after: Some(1),
            version: 2,
            under: (0, 0),
            entries: vec![(keys[0], Kept::new(Counts::of(6)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, changes, ms(2));
        let _ = node.receive(1, parts[1].1.clone(), ms(2));
        assert_eq!(said_held(&node.receive(1, parts[2].1.clone(), ms(2))), [1]);
    }

    /// A pod no table names, heard once and then not for two silences — gone, or restarted as
    /// a new one — is sent no more heartbeats; members unheard as long still are.
    #[test]
    fn a_pod_forgets_peers_no_table_names_once_long_unheard() {
        let mut node = member();
        let beat = Message::Heartbeat {
            term: 1,
            leader: None,
            committed: (0, 0),
            fresh: true,
            sent: ms(2),
            wall: 0,
            hears: Vec::new(),
            leased: true,
        };
        let _ = node.receive(9, beat, ms(2));
        let sent_to = |out: &Outbox, to: NodeId| {
            out.iter()
                .any(|(peer, message)| *peer == to && matches!(message, Message::Heartbeat { .. }))
        };
        let early: Outbox = (1..=10).flat_map(|at| node.tick(ms(at * 100))).collect();
        assert!(sent_to(&early, 9), "a pod heard of late is a peer");
        let _ = (11..=13).flat_map(|at| node.tick(ms(at * 100))).count();
        let late: Outbox = (14..=20).flat_map(|at| node.tick(ms(at * 100))).collect();
        assert!(!sent_to(&late, 9), "still sent heartbeats");
        assert!(sent_to(&late, 1) && sent_to(&late, 3));
    }

    /// A heartbeat sent before the one it holds, arriving after it, changes nothing: what it
    /// echoes back is still the later one.
    #[test]
    fn a_heartbeat_arriving_after_a_later_one_is_ignored() {
        let mut node = member();
        let beat = |sent| Message::Heartbeat {
            term: 1,
            leader: None,
            committed: (0, 0),
            fresh: false,
            sent,
            wall: 0,
            hears: Vec::new(),
            leased: true,
        };
        let _ = node.receive(1, beat(ms(50)), ms(10));
        let _ = node.receive(1, beat(ms(20)), ms(11));
        let out = node.tick(ms(12));
        let echoed = out.iter().find_map(|(to, message)| match message {
            Message::Heartbeat { hears, .. } if *to == 3 => hears
                .iter()
                .find(|(peer, _)| *peer == 1)
                .map(|(_, sent)| *sent),
            _ => None,
        });
        assert_eq!(echoed, Some(ms(50)));
    }

    /// A peer's wall clock is told from its heartbeat against the middle of the round trip its
    /// echo measures: one off by more than `skew_warning` is skewed, one within it is not, and
    /// a heartbeat over a round trip too long, or with no wall clock, tells nothing.
    #[test]
    fn a_peer_whose_wall_clock_is_off_by_more_than_the_warning_is_skewed() {
        let mut node = member();
        node.set_wall_offset(i64::try_from(ms(10_000)).unwrap());
        let beat = |sent, wall, echo| Message::Heartbeat {
            term: 1,
            leader: None,
            committed: (0, 0),
            fresh: false,
            sent,
            wall,
            hears: vec![(2, echo)],
            leased: true,
        };
        let skew = |time: Time| i64::try_from(time).unwrap();
        // Its own heartbeat sent at 950 ms echoed at 1 s: its wall clock 10.975 s in between.
        let _ = node.receive(1, beat(ms(10), ms(11_275), ms(950)), ms(1_000));
        let _ = node.receive(3, beat(ms(10), ms(10_775), ms(950)), ms(1_000));
        assert_eq!(node.skewed(), BTreeMap::from([(1, skew(ms(300)))]));
        // A round trip of three heartbeats, and no wall clock, leave what it had.
        let _ = node.receive(1, beat(ms(20), ms(10_850), ms(700)), ms(1_000));
        let _ = node.receive(1, beat(ms(30), 0, ms(990)), ms(1_000));
        assert_eq!(node.skewed(), BTreeMap::from([(1, skew(ms(300)))]));
        // Back in step.
        let _ = node.receive(1, beat(ms(40), ms(11_095), ms(1_090)), ms(1_100));
        assert!(node.skewed().is_empty());
    }

    /// A table from the leader it follows is as good as a heartbeat: just given one, it grants
    /// no pre-vote against that leader, though no heartbeat of the leader's has reached it.
    #[test]
    fn a_pod_given_a_table_by_its_leader_grants_no_pre_vote() {
        let mut node = member();
        let mut table = node.committed().unwrap().clone();
        table.version += 1;
        let _ = node.receive(1, Message::Publish { table }, ms(1_000));
        let out = node.receive(
            3,
            Message::PreVote {
                term: 5,
                accepted: (5, 0),
            },
            ms(1_001),
        );
        assert!(
            matches!(
                out.as_slice(),
                [(3, Message::PreVoteReply { granted: false, .. })]
            ),
            "{out:?}"
        );
    }

    /// A table published as what changed from one the pod holds — committed, or accepted and
    /// not yet committed — is accepted and answered as the whole would be; changes from one it
    /// does not hold are taken for nothing.
    #[test]
    fn a_pod_takes_a_table_as_changes_from_one_it_holds() {
        let mut node = member();
        let committed = node.committed().unwrap().clone();
        let mut accepted = committed.clone();
        accepted.version += 1;
        accepted.shards[1].term += 1;
        let changes = |from: &Table, to: &Table| Message::PublishChanges {
            base: from.stamp(),
            changes: to.changes_from(from),
        };
        let acked = |out: &Outbox| {
            out.iter().find_map(|(_, message)| match message {
                Message::PublishAck { stamp } => Some(*stamp),
                _ => None,
            })
        };
        let out = node.receive(1, changes(&committed, &accepted), ms(10));
        assert_eq!(acked(&out), Some(accepted.stamp()));
        let mut next = accepted.clone();
        next.version += 1;
        next.shards[2].term += 1;
        let out = node.receive(1, changes(&accepted, &next), ms(11));
        assert_eq!(acked(&out), Some(next.stamp()), "from the table accepted");
        let mut elsewhere = committed.clone();
        elsewhere.version += 7;
        let mut after = elsewhere.clone();
        after.version += 1;
        let out = node.receive(1, changes(&elsewhere, &after), ms(12));
        assert_eq!(acked(&out), None, "from a table it never held");
        let _ = node.receive(
            1,
            Message::Commit {
                stamp: next.stamp(),
            },
            ms(13),
        );
        assert_eq!(node.committed(), Some(&next));
    }

    /// A naming overtaken: a pod named to form a generation that then hears of a later one never
    /// stands under the naming, within the time it would otherwise hold.
    #[test]
    fn a_naming_overtaken_by_a_later_generation_is_spent() {
        let mut node = Node::new(4, address(4), config(), [3, 5]);
        let form = Message::Form {
            generation: 1,
            voters: named(&[3, 4, 5]),
            latest: (0, 0),
        };
        let _ = node.receive(99, form, ms(1));
        let beat = Message::Heartbeat {
            term: first_term(2),
            leader: None,
            committed: (0, 0),
            fresh: false,
            sent: ms(1),
            wall: 0,
            hears: Vec::new(),
            leased: true,
        };
        let _ = node.receive(3, beat, ms(1));
        let stood = (2..500).any(|at| {
            node.tick(ms(at))
                .iter()
                .any(|(_, message)| matches!(message, Message::StartJoin { .. }))
        });
        assert!(!stood, "stood under a naming overtaken");
    }

    /// A pod that has heard of a later generation than its table's stands in no ordinary
    /// election: its voting set would carry into the later generation's terms.
    #[test]
    fn an_outdated_pod_stands_in_no_ordinary_election() {
        let mut node = member();
        let beat = Message::Heartbeat {
            term: first_term(2),
            leader: None,
            committed: (0, 0),
            fresh: false,
            sent: ms(2),
            wall: 0,
            hears: Vec::new(),
            leased: true,
        };
        let _ = node.receive(3, beat, ms(2));
        let asked = (3..3_000).any(|at| {
            node.tick(ms(at))
                .iter()
                .any(|(_, message)| matches!(message, Message::PreVote { .. }))
        });
        assert!(!asked, "an outdated pod asked for pre-votes");
    }

    /// A naming the control plane stops repeating lapses: named once and left alone for longer
    /// than a silence, a pod never stands; named again, it does.
    #[test]
    fn a_naming_not_repeated_lapses() {
        let mut node = Node::new(4, address(4), config(), [3, 5]);
        let form = Message::Form {
            generation: 1,
            voters: named(&[3, 4, 5]),
            latest: (0, 0),
        };
        let stands = |node: &mut Node, from: u64| {
            (from..from + 1_000).any(|at| {
                node.tick(ms(at))
                    .iter()
                    .any(|(_, message)| matches!(message, Message::StartJoin { .. }))
            })
        };
        let _ = node.receive(99, form.clone(), ms(1));
        assert!(!stands(&mut node, 700), "stood on a naming not repeated");
        let _ = node.receive(99, form, ms(1_800));
        assert!(stands(&mut node, 1_801));
    }

    /// A copy sent under an owner's table later than any this pod has applied — the owner a
    /// table ahead, naming it a replica — is kept through a table applied after that is behind
    /// still, though that table names it no copy: it said it holds the copy, and the owner may
    /// hand the shard to it on that word.
    #[test]
    fn a_copy_from_a_table_ahead_is_kept_through_one_behind() {
        let mut node = Node::new(2, address(2), config(), [1, 3, 4]);
        let members = named(&[1, 2, 3, 4]);
        let behind = table::first(&members, 4, 2, 1);
        let shard = 2;
        assert!(!behind.shards[usize::from(shard)].replicas.contains(&2));
        let _ = node.receive(
            1,
            Message::TableReply {
                table: behind.clone(),
            },
            ms(1),
        );
        let key = key_in(shard);
        let copy = Message::Replicate {
            shard,
            term: 2,
            assignment: 1,
            after: None,
            version: 1,
            under: (1, 9),
            entries: vec![(key, Kept::new(Counts::of(7)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let said = node.receive(1, copy, ms(2));
        assert!(
            said.iter()
                .any(|(_, message)| matches!(message, Message::Replicated { .. })),
            "{said:?}"
        );
        let mut later = behind;
        later.version = 2;
        let _ = node.receive(3, Message::TableReply { table: later }, ms(3));
        assert_eq!(node.count(key), Some(7), "dropped what it said it holds");
    }

    /// One vote a term: a second candidate in the same term gets none.
    #[test]
    fn a_pod_votes_once_in_a_term() {
        let mut node = member();
        let accepted = node.accepted();
        let ask = Message::StartJoin { term: 5, accepted };
        let first = node.receive(1, ask.clone(), ms(2));
        assert!(
            matches!(first.as_slice(), [(1, Message::Join { term: 5, .. })]),
            "{first:?}"
        );
        let second = node.receive(3, ask, ms(3));
        assert!(second.is_empty(), "{second:?}");
        let later = node.receive(3, Message::StartJoin { term: 6, accepted }, ms(4));
        assert!(
            matches!(later.as_slice(), [(3, Message::Join { term: 6, .. })]),
            "{later:?}"
        );
    }

    /// A candidate behind the table a pod has accepted gets no vote, though the pod takes its
    /// term; one level with it does.
    #[test]
    fn a_pod_votes_for_no_candidate_behind_its_table() {
        let mut node = member();
        let accepted = node.accepted();
        let behind = (accepted.0, accepted.1 - 1);
        let ask = |term, accepted| Message::StartJoin { term, accepted };
        let refused = node.receive(1, ask(5, behind), ms(2));
        assert!(refused.is_empty(), "{refused:?}");
        assert_eq!(node.term(), 5);
        let level = node.receive(3, ask(6, accepted), ms(3));
        assert!(
            matches!(level.as_slice(), [(3, Message::Join { term: 6, .. })]),
            "{level:?}"
        );
    }

    /// Named to form a generation with pods one of which had a later table than any it has
    /// accepted, a pod never stands: formed from its table, the cluster would drop that pod's
    /// counts. Named with none later, it does.
    #[test]
    fn a_pod_named_behind_the_latest_table_does_not_stand_to_form() {
        let stands = |latest| {
            let mut node = Node::new(4, address(4), config(), [3, 5]);
            let form = Message::Form {
                generation: 2,
                voters: named(&[3, 4, 5]),
                latest,
            };
            let _ = node.receive(99, form, ms(1));
            (2..1_000).any(|at| {
                node.tick(ms(at))
                    .iter()
                    .any(|(_, message)| matches!(message, Message::StartJoin { .. }))
            })
        };
        assert!(
            !stands((first_term(1), 40)),
            "stood behind the latest table"
        );
        assert!(stands((0, 0)));
    }

    /// An owner asked to hand a shard on serves it while the taker catches up, and once it has,
    /// tells the leader and serves it still; the table naming the taker the owner, it stops,
    /// and sends the taker the changes it has not said it holds.
    #[test]
    fn an_owner_serves_a_shard_until_the_table_names_its_taker() {
        let mut node = member();
        let shard = (0..4).find(|shard| {
            node.committed()
                .is_some_and(|table| table.owner(*shard) == Some(2))
        });
        let shard = shard.unwrap_or(0);
        for peer in [1, 3] {
            let beat = Message::Heartbeat {
                term: 1,
                leader: Some(1),
                committed: (1, 1),
                fresh: false,
                sent: ms(5),
                wall: 0,
                hears: vec![(1, ms(5)), (2, ms(5)), (3, ms(5))],
                leased: true,
            };
            let _ = node.receive(peer, beat, ms(5));
        }
        let mut moving = node.committed().cloned().unwrap();
        moving.version = 2;
        moving.moves = vec![(shard, 3, moving.stamp())];
        let _ = node.receive(
            1,
            Message::TableReply {
                table: moving.clone(),
            },
            ms(7),
        );
        assert!(
            node.serves(shard, ms(8)),
            "served while the taker catches up"
        );
        let assignment = node.assignments.get(&(shard, 3)).copied().unwrap_or(0);
        let version = node.versions.get(&shard).copied().unwrap_or(1);
        let caught_up = Message::Replicated {
            shard,
            term: 1,
            assignment,
            version,
        };
        let _ = node.receive(3, caught_up, ms(9));
        let out = node.tick(ms(30));
        assert!(
            out.iter()
                .any(|(to, message)| *to == 3 && matches!(message, Message::CaughtUp { .. })),
            "the taker never told: {out:?}"
        );
        assert!(
            node.serves(shard, ms(31)),
            "served until the table moves it"
        );
        let Some(moved) = table::moved(&moving, shard, 3) else {
            panic!("not moved");
        };
        let mut moved = moved;
        moved.version = 3;
        let out = node.receive(1, Message::TableReply { table: moved }, ms(32));
        assert!(
            !node.serves(shard, ms(33)),
            "served though its taker is the owner"
        );
        assert!(
            out.iter()
                .any(|(to, message)| *to == 3 && matches!(message, Message::Handoff { .. })),
            "the taker never sent the last changes: {out:?}"
        );
    }

    /// A replica updates its copy of a key only once it has not heard the shard's owner for
    /// `stop_serving`, holding its own lease, and marks nothing to copy; not a key of a shard
    /// it owns, nor once cut off.
    #[test]
    fn a_replica_updates_its_copy_only_with_the_owner_unheard_and_its_lease_held() {
        let mut node = member();
        let table = node.committed().cloned().unwrap();
        let theirs = (0..4).find(|shard| table.owner(*shard) == Some(1));
        let mine = (0..4).find(|shard| table.owner(*shard) == Some(2));
        let (Some(theirs), Some(mine)) = (theirs, mine) else {
            panic!("pods 1 and 2 do not both own a shard");
        };
        assert!(table.shards[usize::from(theirs)].replicas.contains(&2));
        let (key, own) = (key_in(theirs), key_in(mine));
        let beat = |node: &mut Node, from: NodeId, now: Time| {
            let beat = Message::Heartbeat {
                term: 1,
                leader: Some(1),
                committed: (1, 1),
                fresh: false,
                sent: now,
                wall: 0,
                hears: vec![(1, now), (2, now), (3, now)],
                leased: true,
            };
            let _ = node.receive(from, beat, now);
        };
        let add = |kept: &mut Kept<Counts>, _| {
            kept.value.add(0);
            ((), Changed::Whole)
        };
        assert_eq!(node.update_copy(key, ms(2), add), None, "with no lease");
        beat(&mut node, 1, ms(5));
        beat(&mut node, 3, ms(5));
        assert_eq!(node.update_copy(key, ms(6), add), None, "its owner heard");
        // The owner unheard since, the third pod still heard: past `stop_serving` 415 ms on.
        beat(&mut node, 3, ms(300));
        assert_eq!(node.update_copy(key, ms(405), add), None, "400 ms unheard");
        assert_eq!(node.update_copy(key, ms(420), add), Some(()));
        assert_eq!(node.count(key), Some(1));
        assert!(
            !node.dirty.values().any(|keys| keys.contains(&key)),
            "a copy's change marked to copy"
        );
        assert_eq!(node.update_copy(own, ms(420), add), None, "a key it owns");
        // Cut off: no lease.
        assert_eq!(node.update_copy(key, ms(900), add), None, "cut off");
        assert_eq!(node.count(key), Some(1));
    }

    /// A taker the table names the owner of a shard moved to it by plan serves it only once it
    /// has the old owner's last changes, at once if they came first — or, the old owner gone,
    /// after [`HANDOFF_WAIT`] cycles.
    #[test]
    fn a_taker_serves_only_once_it_has_the_last_changes() {
        let mut node = member();
        let table = node.committed().cloned().unwrap();
        let Some(shard) = (0..4).find(|shard| table.owner(*shard) == Some(1)) else {
            panic!("pod 1 owns nothing");
        };
        let lease = |node: &mut Node, now: Time| {
            for peer in [1, 3] {
                let beat = Message::Heartbeat {
                    term: 1,
                    leader: Some(1),
                    committed: node.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = node.receive(peer, beat, now);
            }
        };
        let mut moving = table;
        moving.version = 2;
        moving.moves = vec![(shard, 2, moving.stamp())];
        let _ = node.receive(
            1,
            Message::TableReply {
                table: moving.clone(),
            },
            ms(2),
        );
        let Some(mut moved) = table::moved(&moving, shard, 2) else {
            panic!("not moved");
        };
        moved.version = 3;
        let term = moved.shards[usize::from(shard)].term;
        let _ = node.receive(1, Message::TableReply { table: moved }, ms(3));
        lease(&mut node, ms(4));
        assert!(
            !node.serves(shard, ms(4)),
            "served without the last changes"
        );
        let last = Message::Handoff {
            shard,
            term,
            entries: vec![(key_in(shard), Kept::new(Counts::of(3)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, last, ms(5));
        assert!(
            node.serves(shard, ms(5)),
            "not served with the last changes"
        );
        // Another, the last changes reaching it before the table naming it the owner: served
        // as soon as the table does.
        let mut early = member();
        let _ = early.receive(
            1,
            Message::TableReply {
                table: moving.clone(),
            },
            ms(2),
        );
        let Some(mut moved) = table::moved(early.committed().unwrap(), shard, 2) else {
            panic!("not moved");
        };
        moved.version = 3;
        let last = Message::Handoff {
            shard,
            term: moved.shards[usize::from(shard)].term,
            entries: vec![(key_in(shard), Kept::new(Counts::of(3)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = early.receive(1, last, ms(3));
        let _ = early.receive(1, Message::TableReply { table: moved }, ms(4));
        lease(&mut early, ms(5));
        assert!(early.serves(shard, ms(5)), "waited for what it had");
        // Another, further behind (#281): the last changes reaching it before it has the table
        // asking for the move, and a table between that does not name it yet applied first.
        // Served as soon as the table naming it the owner is, with them.
        let mut behind = member();
        let Some(mut between) = behind.committed().cloned() else {
            panic!("no table");
        };
        between.version = 2;
        let mut asking = between.clone();
        asking.version = 3;
        asking.moves = vec![(shard, 2, asking.stamp())];
        let Some(mut handed) = table::moved(&asking, shard, 2) else {
            panic!("not moved");
        };
        handed.version = 4;
        let last = Message::Handoff {
            shard,
            term: handed.shards[usize::from(shard)].term,
            entries: vec![(key_in(shard), Kept::new(Counts::of(3)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = behind.receive(1, last, ms(2));
        for (at, table) in [(3, between), (4, asking), (5, handed)] {
            let _ = behind.receive(1, Message::TableReply { table }, ms(at));
        }
        lease(&mut behind, ms(6));
        assert!(behind.serves(shard, ms(6)), "dropped what it had, behind");
        assert_eq!(behind.count(key_in(shard)), Some(3));
        // Another, its last changes never coming: served after the wait.
        let mut waiting = member();
        let _ = waiting.receive(1, Message::TableReply { table: moving }, ms(2));
        let Some(mut moved) = table::moved(waiting.committed().unwrap(), shard, 2) else {
            panic!("not moved");
        };
        moved.version = 3;
        let _ = waiting.receive(1, Message::TableReply { table: moved }, ms(3));
        lease(&mut waiting, ms(60));
        let _ = waiting.tick(ms(60));
        assert!(waiting.serves(shard, ms(60)), "never served without them");
    }

    /// Last changes that came first, under a term the tables then give the shard with another
    /// owner, are not kept: the move they were for was dropped, the shard handed elsewhere.
    #[test]
    fn last_changes_that_came_first_go_once_the_shard_is_handed_to_another() {
        let mut node = member();
        let Some(table) = node.committed().cloned() else {
            panic!("no table");
        };
        let Some(shard) = (0..4).find(|shard| table.owner(*shard) == Some(1)) else {
            panic!("pod 1 owns nothing");
        };
        let mut asking = table;
        asking.version = 2;
        asking.moves = vec![(shard, 3, asking.stamp())];
        let Some(mut elsewhere) = table::moved(&asking, shard, 3) else {
            panic!("not moved");
        };
        elsewhere.version = 3;
        let last = Message::Handoff {
            shard,
            term: elsewhere.shards[usize::from(shard)].term,
            entries: vec![(key_in(shard), Kept::new(Counts::of(3)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, last, ms(2));
        let _ = node.receive(1, Message::TableReply { table: asking }, ms(3));
        assert!(
            node.handed_in.contains_key(&shard),
            "dropped before its table"
        );
        let _ = node.receive(1, Message::TableReply { table: elsewhere }, ms(4));
        assert!(
            !node.handed_in.contains_key(&shard),
            "kept, the shard handed to another"
        );
    }

    /// The handoff's waits are five copy cycles, as long as the cycle is configured: at a
    /// cycle of 20 ms, an owner whose taker has caught up tells it so again every 100 ms, and
    /// a taker named the owner, the old owner's last changes never coming, serves after 100
    /// ms — not the defaults' 50 ms.
    #[test]
    fn a_handoffs_waits_are_five_cycles() {
        let mut config = config();
        config.cycle = ms(20);
        let lease = |node: &mut Node, now: Time| {
            for peer in [1, 3] {
                let beat = Message::Heartbeat {
                    term: 1,
                    leader: Some(1),
                    committed: node.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = node.receive(peer, beat, now);
            }
        };
        // An owner, its taker caught up, the table never moving the shard.
        let mut owner = member_under(config);
        let table = owner.committed().cloned().unwrap();
        let Some(shard) = (0..4).find(|shard| table.owner(*shard) == Some(2)) else {
            panic!("pod 2 owns nothing");
        };
        lease(&mut owner, ms(5));
        let mut moving = table.clone();
        moving.version = 2;
        moving.moves = vec![(shard, 3, moving.stamp())];
        let _ = owner.receive(1, Message::TableReply { table: moving }, ms(7));
        let caught_up = Message::Replicated {
            shard,
            term: 1,
            assignment: owner.assignments.get(&(shard, 3)).copied().unwrap_or(0),
            version: owner.versions.get(&shard).copied().unwrap_or(1),
        };
        let _ = owner.receive(3, caught_up, ms(9));
        let told: Vec<Time> = (10..600)
            .map(ms)
            .filter(|now| {
                lease(&mut owner, *now);
                owner
                    .tick(*now)
                    .iter()
                    .any(|(to, message)| *to == 3 && matches!(message, Message::CaughtUp { .. }))
            })
            .collect();
        assert!(told.len() >= 3, "told at {told:?}");
        for pair in told.windows(2) {
            let apart = pair[1] - pair[0];
            assert!(
                (ms(100)..=ms(120)).contains(&apart),
                "told {apart} ns apart: {told:?}"
            );
        }
        // A taker named the owner, the last changes never coming.
        let mut taker = member_under(config);
        let Some(shard) = (0..4).find(|shard| table.owner(*shard) == Some(1)) else {
            panic!("pod 1 owns nothing");
        };
        let mut moving = table;
        moving.version = 2;
        moving.moves = vec![(shard, 2, moving.stamp())];
        let _ = taker.receive(1, Message::TableReply { table: moving }, ms(2));
        let Some(mut moved) = table::moved(taker.committed().unwrap(), shard, 2) else {
            panic!("not moved");
        };
        moved.version = 3;
        let _ = taker.receive(1, Message::TableReply { table: moved }, ms(3));
        let served = (4..300).map(ms).find(|now| {
            lease(&mut taker, *now);
            let _ = taker.tick(*now);
            taker.serves(shard, *now)
        });
        assert!(
            served.is_some_and(|at| (ms(103)..=ms(125)).contains(&at)),
            "served at {served:?}"
        );
    }

    /// A taker named the owner, waiting for the old owner's last changes, does not hand the
    /// shard on, though the table already asks it to: arriving under a shard term since moved
    /// on, the last changes would be refused, and their counts lost. Once they are in, it does.
    #[test]
    fn a_shard_waited_for_is_not_handed_on() {
        let mut node = member();
        let table = node.committed().cloned().unwrap();
        let Some(shard) = (0..4).find(|shard| table.owner(*shard) == Some(1)) else {
            panic!("pod 1 owns nothing");
        };
        let lease = |node: &mut Node, now: Time| {
            for peer in [1, 3] {
                let beat = Message::Heartbeat {
                    term: 1,
                    leader: Some(1),
                    committed: node.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = node.receive(peer, beat, now);
            }
        };
        let mut moving = table;
        moving.version = 2;
        moving.moves = vec![(shard, 2, moving.stamp())];
        let _ = node.receive(
            1,
            Message::TableReply {
                table: moving.clone(),
            },
            ms(2),
        );
        let Some(mut moved) = table::moved(&moving, shard, 2) else {
            panic!("not moved");
        };
        moved.version = 3;
        let term = moved.shards[usize::from(shard)].term;
        let mut onward = moved.clone();
        let _ = node.receive(1, Message::TableReply { table: moved }, ms(3));
        onward.version = 4;
        onward.moves = vec![(shard, 3, onward.stamp())];
        let _ = node.receive(1, Message::TableReply { table: onward }, ms(4));
        lease(&mut node, ms(5));
        // Pod 3, a replica already, holds all the shard does: caught up at once.
        let assignment = node.assignments.get(&(shard, 3)).copied().unwrap_or(0);
        let version = node.versions.get(&shard).copied().unwrap_or(1);
        let said = Message::Replicated {
            shard,
            term,
            assignment,
            version,
        };
        let _ = node.receive(3, said, ms(5));
        let hands_on = |out: &Outbox| {
            out.iter().any(|(to, message)| {
                *to == 3
                    && matches!(message, Message::CaughtUp { shard: moving, .. } if *moving == shard)
            })
        };
        let early: Outbox = (6..20).flat_map(|at| node.tick(ms(at))).collect();
        assert!(!hands_on(&early), "handed on while waiting: {early:?}");
        let last = Message::Handoff {
            shard,
            term,
            entries: vec![(key_in(shard), Kept::new(Counts::of(3)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, last, ms(20));
        lease(&mut node, ms(21));
        let later: Outbox = (21..40).flat_map(|at| node.tick(ms(at))).collect();
        assert!(hands_on(&later), "never handed on: {later:?}");
    }

    /// Pod 1 holding the first table of 1, 2 and 3, a shard of its own copied to 2, and the
    /// whole copy it sent 2: its shard, the copy's assignment and number.
    fn owner_copying_to_2() -> (Node, ShardId, u64, u64) {
        owner_copying_to_2_under(config())
    }

    fn owner_copying_to_2_under(config: Config) -> (Node, ShardId, u64, u64) {
        let mut owner = Node::new(1, address(1), config, [2, 3]);
        let members = named(&[1, 2, 3]);
        let first = table::first(&members, 4, 2, 1);
        let Some(shard) = (0..4).find(|shard| {
            first.owner(*shard) == Some(1)
                && first.shards[usize::from(*shard)].replicas.contains(&2)
        }) else {
            panic!("no shard of 1 copied to 2");
        };
        let sent = owner.receive(2, Message::TableReply { table: first }, ms(1));
        let Some((assignment, version)) = sent.iter().find_map(|(to, message)| match message {
            Message::Replicate {
                shard: copied,
                assignment,
                version,
                ..
            } if *to == 2 && *copied == shard => Some((*assignment, *version)),
            _ => None,
        }) else {
            panic!("no copy to 2");
        };
        (owner, shard, assignment, version)
    }

    /// A replica that never answers is repaired every five copy cycles, as long as the cycle
    /// is configured: 100 ms apart at a cycle of 20 ms, not the defaults' 50 ms.
    #[test]
    fn a_replica_unanswering_is_repaired_every_five_cycles() {
        let mut config = config();
        config.cycle = ms(20);
        let (mut owner, shard, _, _) = owner_copying_to_2_under(config);
        let repairs: Vec<Time> = (1..200)
            .map(|step| ms(1 + step * 5))
            .filter(|now| {
                owner.tick(*now).iter().any(|(to, message)| {
                    *to == 2
                        && matches!(
                            message,
                            Message::Replicate { shard: copied, after: None, .. }
                                if *copied == shard
                        )
                })
            })
            .collect();
        assert!(repairs.len() >= 3, "repaired at {repairs:?}");
        for pair in repairs.windows(2) {
            let apart = pair[1] - pair[0];
            assert!(
                (ms(100)..=ms(120)).contains(&apart),
                "repaired {apart} ns apart: {repairs:?}"
            );
        }
    }

    /// A replica that says it holds less than it said — a whole copy from an owner it had the
    /// shard from before, arriving late, taken over the current one — is sent the shard again,
    /// so that it catches up rather than being repaired, unanswered, for good.
    #[test]
    fn a_replica_saying_it_holds_less_is_sent_the_shard_again() {
        let (mut owner, shard, assignment, version) = owner_copying_to_2();
        let said = |version| Message::Replicated {
            shard,
            term: 1,
            assignment,
            version,
        };
        let _ = owner.receive(2, said(version), ms(2));
        let _ = owner.receive(2, said(0), ms(3));
        let again = (1..40).any(|at| {
            owner.tick(ms(3 + at * 5)).iter().any(|(to, message)| {
                *to == 2
                    && matches!(
                        message,
                        Message::Replicate { shard: copied, after: None, .. } if *copied == shard
                    )
            })
        });
        assert!(again, "never sent the shard again");
    }

    /// A replica that never answers costs its owner a bounded record of what to send it
    /// again, however many copies go unanswered.
    #[test]
    fn copies_a_replica_never_answers_are_kept_within_a_bound() {
        let (mut owner, shard, _, _) = owner_copying_to_2();
        let key = (0..).find(|key| shard_of(*key, 4) == shard).unwrap_or(0);
        for beat in 0..400 {
            let now = ms(2 + beat * 5);
            for peer in [2, 3] {
                let heard = Message::Heartbeat {
                    term: owner.term(),
                    leader: Some(2),
                    committed: owner.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = owner.receive(peer, heard, now);
            }
            let _ = owner.update(key, now, |kept, _| {
                kept.value.add(0);
                kept.forget_at = Time::MAX;
                ((), Changed::Whole)
            });
            let _ = owner.tick(now);
        }
        let kept = owner.unacked.get(&(shard, 2)).map_or(0, BTreeMap::len);
        assert!(
            owner.count(key).is_some_and(|count| count > 300),
            "{:?}",
            owner.count(key)
        );
        assert!(kept <= UNACKED_MOST, "{kept} copies kept");
    }

    /// A ready pod left out of its table and taken back before its next tick, a move to it
    /// outstanding: not ready, as it must become ready again as a new member does.
    #[test]
    fn a_pod_left_out_and_taken_back_between_ticks_is_not_ready() {
        let mut three = config();
        three.shards = 3;
        let first = table::first(&named(&[1, 2, 3]), 3, 2, 1);
        let mut node = Node::new(1, address(1), three, [2, 3, 4]);
        let _ = node.receive(
            3,
            Message::TableReply {
                table: first.clone(),
            },
            0,
        );
        let hear = |node: &mut Node, table: &Table, now: Time| {
            for peer in [2, 3] {
                let beat = Message::Heartbeat {
                    term: 1,
                    leader: Some(3),
                    committed: table.stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now)],
                    leased: true,
                };
                let _ = node.receive(peer, beat, now);
            }
        };
        hear(&mut node, &first, 0);
        let _ = node.tick(0);
        assert!(node.ready(0));
        let live = [2, 3, 4].into_iter().collect();
        let joining = named(&[4]);
        let Some(mut without) = table::repair(&first, &live, &live, &joining, 2) else {
            panic!("pod 1 not left out");
        };
        without.version = 2;
        let _ = node.receive(
            3,
            Message::TableReply {
                table: without.clone(),
            },
            ms(1),
        );
        let mut back = without.clone();
        back.members = named(&[1, 2, 3, 4]);
        back.version = 3;
        back.moves.push((0, 1, without.stamp()));
        let _ = node.receive(
            3,
            Message::TableReply {
                table: back.clone(),
            },
            ms(2),
        );
        hear(&mut node, &back, ms(2));
        let _ = node.tick(ms(5));
        assert!(!node.ready(ms(5)), "ready again, a move to it outstanding");
    }

    /// A replica that hears its owner but whose answers never come, under churning keys the
    /// cap evicts: the keys its owner keeps to send it again stay within what the shard holds
    /// and what a second adds, rather than growing with every key changed since, and the
    /// owner sends it the whole shard again under a new assignment.
    #[test]
    fn keys_kept_for_a_replica_never_answering_stay_within_the_shards_size() {
        let mut capped = config();
        capped.max_key_bytes = Some(200);
        let (mut owner, shard, assignment, _) = owner_copying_to_2_under(capped);
        let mut keys = (0..).filter(|key| shard_of(*key, 4) == shard);
        let mut most = 0;
        let mut whole_again = false;
        for beat in 0..1_000 {
            let now = ms(2 + beat * 5);
            for peer in [2, 3] {
                let heard = Message::Heartbeat {
                    term: owner.term(),
                    leader: Some(2),
                    committed: owner.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                let _ = owner.receive(peer, heard, now);
            }
            let _ = owner.update(keys.next().unwrap_or(0), now, |kept, _| {
                kept.value.add(0);
                kept.forget_at = Time::MAX;
                ((), Changed::Whole)
            });
            whole_again |= owner.tick(now).iter().any(|(to, message)| {
                matches!(message, Message::Replicate { shard: copied, assignment: given, after: None, .. }
                    if *to == 2 && *copied == shard && *given != assignment)
            });
            let kept: BTreeSet<u64> = owner
                .unacked
                .get(&(shard, 2))
                .into_iter()
                .flat_map(BTreeMap::values)
                .flatten()
                .copied()
                .collect();
            most = most.max(kept.len());
        }
        assert!(owner.evicted() > 500, "{} evicted", owner.evicted());
        assert!(most <= 250, "{most} keys kept to send again");
        assert!(
            whole_again,
            "never sent the whole shard under a new assignment"
        );
    }

    /// A key changed whole and in a part before the same copy, in either order, is copied
    /// whole: a part is all a copy carries only of a key changed in nothing but parts.
    #[test]
    fn a_key_changed_whole_and_in_a_part_is_copied_whole() {
        type Change = fn(&mut Kept<Counts>, Time) -> ((), Changed);
        let whole: Change = |kept, _| {
            kept.value.add(1);
            ((), Changed::Whole)
        };
        let part: Change = |kept, _| {
            kept.value.add(0);
            ((), Changed::Part(0))
        };
        for (whole_first, changes) in [(true, [whole, part]), (false, [part, whole])] {
            let (mut owner, shard, _, _) = owner_copying_to_2();
            let key = (0..).find(|key| shard_of(*key, 4) == shard).unwrap_or(0);
            let mut served = false;
            let mut copied = None;
            for beat in 0..100 {
                let now = ms(2 + beat * 5);
                for peer in [2, 3] {
                    let heard = Message::Heartbeat {
                        term: owner.term(),
                        leader: Some(2),
                        committed: owner.committed_stamp(),
                        fresh: false,
                        sent: now,
                        wall: 0,
                        hears: vec![(1, now), (2, now), (3, now)],
                        leased: true,
                    };
                    let _ = owner.receive(peer, heard, now);
                }
                if !served {
                    served = changes
                        .iter()
                        .all(|change| owner.update(key, now, change).is_some());
                }
                copied = owner
                    .tick(now)
                    .into_iter()
                    .find_map(|(to, message)| match message {
                        Message::Replicate {
                            shard: sent,
                            after: Some(_),
                            entries,
                            ..
                        } if to == 2 && sent == shard => entries
                            .into_iter()
                            .find(|(held, _)| *held == key)
                            .map(|(_, kept)| kept.value),
                        _ => None,
                    });
                if copied.is_some() {
                    break;
                }
            }
            assert!(served, "never served");
            assert_eq!(
                copied,
                Some(Counts([(0, 1), (1, 1)].into_iter().collect())),
                "whole first: {whole_first}"
            );
        }
    }

    /// A replica taken off a shard and put back under the same shard term, the whole copy it
    /// is then sent lost: its answers from before, reaching the owner late, count for nothing,
    /// and the owner sends it the shard again.
    #[test]
    fn a_replica_put_back_is_sent_the_shard_again_whatever_it_said_before() {
        let mut owner = Node::new(1, address(1), config(), [2, 3]);
        let members = named(&[1, 2, 3]);
        let first = table::first(&members, 4, 2, 1);
        let Some(shard) = (0..4).find(|shard| {
            first.owner(*shard) == Some(1)
                && first.shards[usize::from(*shard)].replicas.contains(&2)
        }) else {
            panic!("no shard of 1 copied to 2");
        };
        let sent = owner.receive(
            2,
            Message::TableReply {
                table: first.clone(),
            },
            ms(1),
        );
        let Some((assignment, version)) = sent.iter().find_map(|(to, message)| match message {
            Message::Replicate {
                shard: copied,
                assignment,
                version,
                ..
            } if *to == 2 && *copied == shard => Some((*assignment, *version)),
            _ => None,
        }) else {
            panic!("no copy to 2");
        };
        let term = first.shards[usize::from(shard)].term;
        // 2 taken off the shard, then put back; the whole copy then sent it is lost.
        let mut off = first.clone();
        off.version = 2;
        off.shards[usize::from(shard)]
            .replicas
            .retain(|replica| *replica != 2);
        let _ = owner.receive(2, Message::TableReply { table: off.clone() }, ms(2));
        let mut back = off;
        back.version = 3;
        back.shards[usize::from(shard)].replicas.push(2);
        let _ = owner.receive(2, Message::TableReply { table: back }, ms(3));
        // Its answer to the first copy, sent before it was taken off, arrives now.
        let _ = owner.receive(
            2,
            Message::Replicated {
                shard,
                term,
                assignment,
                version,
            },
            ms(4),
        );
        let again = (1..40).any(|at| {
            owner.tick(ms(4 + at * 5)).iter().any(|(to, message)| {
                *to == 2
                    && matches!(
                        message,
                        Message::Replicate { shard: copied, after: None, .. } if *copied == shard
                    )
            })
        });
        assert!(again, "never sent the shard again");
    }

    /// A replica taken off a shard and put back while its owner, catching up, never saw it
    /// taken off: the owner keeps its assignment and sends the next copy, and the replica, its
    /// data of the shard dropped, does not say it holds the shard.
    #[test]
    fn a_replica_that_dropped_a_shard_never_says_it_holds_it() {
        let mut node = member();
        let table = node.committed().cloned().unwrap();
        let Some(shard) = (0..4).find(|shard| {
            table.owner(*shard) == Some(1)
                && table.shards[usize::from(*shard)].replicas.contains(&2)
        }) else {
            panic!("no shard of 1 copied to 2");
        };
        let term = table.shards[usize::from(shard)].term;
        let key = key_in(shard);
        let copy = |after, version| Message::Replicate {
            shard,
            term,
            assignment: 1,
            after,
            version,
            under: (0, 0),
            entries: vec![(key, Kept::new(Counts::of(5)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, copy(None, 1), ms(2));
        let mut off = table.clone();
        off.version = 2;
        off.shards[usize::from(shard)]
            .replicas
            .retain(|replica| *replica != 2);
        let _ = node.receive(1, Message::TableReply { table: off.clone() }, ms(3));
        let mut back = off;
        back.version = 3;
        back.shards[usize::from(shard)].replicas.push(2);
        let _ = node.receive(1, Message::TableReply { table: back }, ms(4));
        let answer = node.receive(1, copy(Some(1), 2), ms(5));
        let said = answer.iter().find_map(|(_, message)| match message {
            Message::Replicated { version, .. } => Some(*version),
            _ => None,
        });
        assert_eq!(said, Some(0), "said it holds a shard it dropped");
    }

    /// A replica that holds nothing of a shard its table makes it a replica of says so to the
    /// owner, unasked: taken off and put back, its owner never seeing it taken off, it would
    /// otherwise wait for a copy that never comes, the key it lacks quiet.
    #[test]
    fn a_replica_lacking_a_shard_says_so_unasked() {
        let mut node = member();
        let table = node.committed().cloned().unwrap();
        let Some(shard) = (0..4).find(|shard| {
            table.owner(*shard) == Some(1)
                && table.shards[usize::from(*shard)].replicas.contains(&2)
        }) else {
            panic!("no shard of 1 copied to 2");
        };
        let term = table.shards[usize::from(shard)].term;
        let whole = Message::Replicate {
            shard,
            term,
            assignment: 1,
            after: None,
            version: 1,
            under: (0, 0),
            entries: vec![(key_in(shard), Kept::new(Counts::of(5)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, whole, ms(2));
        let lacking = |out: &Outbox| {
            out.iter().any(|(to, message)| {
                *to == 1
                    && matches!(message, Message::Lacking { shard: lacked, .. } if *lacked == shard)
            })
        };
        assert!(
            !(1..40).any(|at| lacking(&node.tick(ms(2 + at * 5)))),
            "said so holding it"
        );
        let mut off = table.clone();
        off.version = 2;
        off.shards[usize::from(shard)]
            .replicas
            .retain(|replica| *replica != 2);
        let _ = node.receive(1, Message::TableReply { table: off.clone() }, ms(300));
        let mut back = off;
        back.version = 3;
        back.shards[usize::from(shard)].replicas.push(2);
        let _ = node.receive(1, Message::TableReply { table: back }, ms(301));
        assert!(
            (1..40).any(|at| lacking(&node.tick(ms(301 + at * 5)))),
            "never said so"
        );
    }

    /// An owner told by a replica that it lacks the shard sends it the shard again, whatever
    /// the replica said before.
    #[test]
    fn an_owner_told_a_replica_lacks_the_shard_sends_it_again() {
        let (mut owner, shard, assignment, version) = owner_copying_to_2();
        let said = Message::Replicated {
            shard,
            term: 1,
            assignment,
            version,
        };
        let _ = owner.receive(2, said, ms(2));
        let _ = owner.receive(2, Message::Lacking { shard, term: 1 }, ms(3));
        let again = (1..40).any(|at| {
            owner.tick(ms(3 + at * 5)).iter().any(|(to, message)| {
                *to == 2
                    && matches!(
                        message,
                        Message::Replicate { shard: copied, after: None, .. } if *copied == shard
                    )
            })
        });
        assert!(again, "never sent the shard again");
    }

    /// Assignment numbers are an owner's own: a copy from another owner — one that formed the
    /// cluster again, under the same shard term, and numbered this pod's assignment as the
    /// last did — following the number this pod last held from the first, its whole copy lost,
    /// does not make it say it holds the shard.
    #[test]
    fn a_replica_never_lines_up_copies_from_two_owners() {
        let mut node = member();
        let term = 1;
        let key = key_in(0);
        let copy = |after, version, count| Message::Replicate {
            shard: 0,
            term,
            assignment: 1,
            after,
            version,
            under: (0, 0),
            entries: vec![(key, Kept::new(Counts::of(count)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, copy(None, 1, 5), ms(2));
        let answer = node.receive(4, copy(Some(1), 2, 7), ms(3));
        let said = answer.iter().find_map(|(_, message)| match message {
            Message::Replicated { version, .. } => Some(*version),
            _ => None,
        });
        assert_eq!(said, Some(0), "lined up a copy from another owner");
    }

    /// An owner numbering its copies afresh — the cluster formed again, the replica a replica
    /// throughout — under a new assignment, its whole copy lost: a copy of the new assignment
    /// following the number the replica last held under the old one does not make it say it
    /// holds the shard.
    #[test]
    fn a_replica_never_lines_up_copies_across_assignments() {
        let mut node = member();
        let table = node.committed().cloned().unwrap();
        let Some(shard) = (0..4).find(|shard| {
            table.owner(*shard) == Some(1)
                && table.shards[usize::from(*shard)].replicas.contains(&2)
        }) else {
            panic!("no shard of 1 copied to 2");
        };
        let term = table.shards[usize::from(shard)].term;
        let key = key_in(shard);
        let copy = |assignment, after, version, count| Message::Replicate {
            shard,
            term,
            assignment,
            after,
            version,
            under: (0, 0),
            entries: vec![(key, Kept::new(Counts::of(count)))],
            dropped: Vec::new(),
            part: 0,
            more: false,
        };
        let _ = node.receive(1, copy(1, None, 1, 5), ms(2));
        let _ = node.receive(1, copy(1, Some(1), 2, 6), ms(3));
        // The new assignment's whole copy is lost; its next copy follows number 2.
        let answer = node.receive(1, copy(2, Some(2), 3, 7), ms(4));
        let said = answer.iter().find_map(|(_, message)| match message {
            Message::Replicated { version, .. } => Some(*version),
            _ => None,
        });
        assert_eq!(said, Some(0), "lined up copies of two assignments");
    }

    /// A newer naming reaching a candidate standing under an older one changes nothing about
    /// that candidacy: its votes are counted against the naming it stood under, so two of the
    /// first naming's five voters, named alone in the next, never make it lead.
    #[test]
    fn a_candidacy_counts_votes_against_the_naming_it_stood_under() {
        let mut node = Node::new(1, address(1), config(), [2, 3, 4, 5]);
        let form = |generation, voters: &[NodeId]| Message::Form {
            generation,
            voters: named(voters),
            latest: (0, 0),
        };
        let _ = node.receive(99, form(1, &[1, 2, 3, 4, 5]), ms(1));
        let stood = (1..200).any(|at| {
            node.tick(ms(at * 5))
                .iter()
                .any(|(_, message)| matches!(message, Message::StartJoin { .. }))
        });
        assert!(stood);
        let term = node.term();
        let _ = node.receive(99, form(2, &[1, 2]), ms(1_001));
        let _ = node.receive(2, Message::Join { term }, ms(1_002));
        assert!(
            !node.is_leader(),
            "led on two votes of the five it stood under"
        );
    }

    /// A pod given a generation's first table before its own naming reaches it — its first
    /// naming lost — its leader then gone before the table committed: the naming, arriving
    /// late, is still taken, and it stands to form the generation with the table it holds.
    #[test]
    fn a_naming_reaching_a_pod_after_the_first_table_is_still_taken() {
        let mut node = Node::new(3, address(3), config(), [1, 2]);
        let term = first_term(1);
        let _ = node.receive(
            1,
            Message::StartJoin {
                term,
                accepted: (0, 0),
            },
            ms(1),
        );
        let members = named(&[1, 2, 3]);
        let mut first = table::first(&members, 4, 2, term);
        first.voting = vec![1, 2, 3];
        let _ = node.receive(1, Message::Publish { table: first }, ms(2));
        assert_eq!(generation_of(node.accepted().0), 1);
        let _ = node.receive(
            99,
            Message::Form {
                generation: 1,
                voters: named(&[1, 2, 3]),
                latest: (0, 0),
            },
            ms(3),
        );
        // Named again every heartbeat, as the control plane does until the generation forms.
        let stood = (4..200).any(|at| {
            if at % 20 == 0 {
                let _ = node.receive(
                    99,
                    Message::Form {
                        generation: 1,
                        voters: named(&[1, 2, 3]),
                        latest: (0, 0),
                    },
                    ms(at * 5),
                );
            }
            node.tick(ms(at * 5))
                .iter()
                .any(|(_, message)| matches!(message, Message::StartJoin { .. }))
        });
        assert!(stood, "it never stood with the table it holds");
    }

    /// A voter in a later term — one that stood to form the generation while cut off, then
    /// took the winner's table before standing again — never takes the leader's tables: the
    /// leader stands down, and leads again above that term.
    #[test]
    fn a_leader_hearing_a_voter_in_a_later_term_stands_down() {
        let mut node = Node::new(1, address(1), config(), [2, 3]);
        let members = named(&[1, 2, 3]);
        let first = table::first(&members, 4, 2, 1);
        let _ = node.receive(2, Message::TableReply { table: first }, ms(1));
        let ahead = 50;
        let mut led_below = false;
        let mut now = ms(1);
        // Pods 2 and 3 grant and accept whatever it asks, and hear it; from the second second
        // pod 3 is in term 50, following nobody.
        while now < ms(3_000) {
            now += ms(10);
            let mut queue = node.tick(now);
            led_below |= node.is_leader() && node.term() < ahead;
            for peer in [2, 3] {
                let later = peer == 3 && now > ms(1_000) && node.term() < ahead;
                let beat = Message::Heartbeat {
                    term: if later { ahead } else { node.term() },
                    leader: (node.is_leader() && !later).then_some(1),
                    committed: node.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                queue.extend(node.receive(peer, beat, now));
            }
            while let Some((to, message)) = queue.pop() {
                let answer = match message {
                    Message::PreVote { term, .. } => Message::PreVoteReply {
                        term,
                        granted: true,
                    },
                    Message::StartJoin { term, .. } => Message::Join { term },
                    Message::Publish { table } => Message::PublishAck {
                        stamp: table.stamp(),
                    },
                    Message::PublishChanges { changes, .. } => Message::PublishAck {
                        stamp: (changes.term, changes.version),
                    },
                    _ => continue,
                };
                queue.extend(node.receive(to, answer, now));
            }
        }
        assert!(led_below, "it led before pod 3 went ahead");
        assert!(
            node.is_leader() && node.term() > ahead,
            "term {}",
            node.term()
        );
    }

    /// A pod can accept a table that leaves it out, one that was never committed, and then win
    /// the next election with it: as leader it takes itself back in, as nobody else will.
    #[test]
    fn a_leader_left_out_of_its_table_takes_itself_back_in() {
        let mut node = Node::new(1, address(1), config(), [2, 3]);
        let members = named(&[1, 2, 3]);
        let first = table::first(&members, 4, 2, 1);
        let _ = node.receive(
            2,
            Message::TableReply {
                table: first.clone(),
            },
            ms(1),
        );
        let mut without = first;
        without.version = 2;
        without.members = named(&[2, 3]);
        for shard in &mut without.shards {
            shard.replicas.retain(|replica| *replica != 1);
            if shard.owner == Some(1) {
                shard.owner = shard.replicas.pop();
            }
        }
        let _ = node.receive(2, Message::Publish { table: without }, ms(2));
        // Pods 2 and 3 grant and accept whatever it asks, and hear it.
        let mut now = ms(2);
        while now < ms(3_000) {
            now += ms(10);
            let mut queue = node.tick(now);
            for peer in [2, 3] {
                let beat = Message::Heartbeat {
                    term: node.term(),
                    leader: node.is_leader().then_some(1),
                    committed: node.committed_stamp(),
                    fresh: false,
                    sent: now,
                    wall: 0,
                    hears: vec![(1, now), (2, now), (3, now)],
                    leased: true,
                };
                queue.extend(node.receive(peer, beat, now));
            }
            while let Some((to, message)) = queue.pop() {
                let answer = match message {
                    Message::PreVote { term, .. } => Message::PreVoteReply {
                        term,
                        granted: true,
                    },
                    Message::StartJoin { term, .. } => Message::Join { term },
                    Message::Publish { table } => Message::PublishAck {
                        stamp: table.stamp(),
                    },
                    Message::PublishChanges { changes, .. } => Message::PublishAck {
                        stamp: (changes.term, changes.version),
                    },
                    _ => continue,
                };
                queue.extend(node.receive(to, answer, now));
            }
        }
        assert!(node.is_leader());
        assert!(
            node.committed().is_some_and(|table| table.has_member(1)),
            "{:?}",
            node.committed()
        );
    }

    /// A voter the table has taken out of its members, cut off for a while, stands when the
    /// leader goes: else the leader's crash, with a majority of the voting set alive but none
    /// of them members, leaves nobody to lead.
    #[test]
    fn a_voter_that_is_not_a_member_stands_when_the_leader_goes() {
        let mut node = Node::new(3, address(3), config(), [1, 2]);
        let members = named(&[1, 2, 3]);
        let mut table = table::first(&members, 4, 2, 1);
        table.members = named(&[2]);
        for shard in &mut table.shards {
            shard.owner = Some(2);
            shard.replicas.clear();
        }
        let _ = node.receive(2, Message::TableReply { table }, ms(1));
        let mut asked = false;
        let mut now = ms(1);
        while now < ms(3_000) && !asked {
            now += ms(10);
            asked = node
                .tick(now)
                .iter()
                .any(|(_, message)| matches!(message, Message::PreVote { .. }));
        }
        assert!(asked, "never stood");
    }

    /// A pod that still hears its leader refuses a pre-vote, so a pod that alone has lost the
    /// leader cannot unseat it; once the leader has gone unheard for `silence`, it grants it.
    #[test]
    fn a_pre_vote_is_refused_while_the_leader_is_heard() {
        let mut node = member();
        let beat = Message::Heartbeat {
            term: 1,
            leader: Some(1),
            committed: (1, 1),
            fresh: false,
            sent: ms(10),
            wall: 0,
            hears: vec![(1, ms(10)), (2, ms(10)), (3, ms(10))],
            leased: true,
        };
        let _ = node.receive(1, beat, ms(10));
        let ask = Message::PreVote {
            term: 2,
            accepted: (1, 1),
        };
        let refused = node.receive(3, ask.clone(), ms(20));
        assert!(
            refused.contains(&(
                3,
                Message::PreVoteReply {
                    term: 2,
                    granted: false
                }
            )),
            "{refused:?}"
        );
        let granted = node.receive(3, ask, ms(10) + config().silence + ms(1));
        assert!(
            granted.contains(&(
                3,
                Message::PreVoteReply {
                    term: 2,
                    granted: true
                }
            )),
            "{granted:?}"
        );
    }
}
