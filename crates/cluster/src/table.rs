//! The table the leader publishes, and the pure rules it is changed by: who votes, who owns
//! which shard, and who keeps its replicas.

use crate::{NodeId, ShardId};
use std::collections::{BTreeMap, BTreeSet};

/// One shard: its owner, its replicas, and its term, raised whenever its owner changes, so that
/// a copy from an owner it has moved on from is refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Shard {
    /// The member that answers for its keys; none if no member could take it.
    pub owner: Option<NodeId>,
    /// The members that keep a copy, to take over from.
    pub replicas: Vec<NodeId>,
    /// Raised at every change of owner.
    pub term: u64,
}

/// The cluster's state as the leader publishes it, numbered by the leader's term and a version
/// that rises with every table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Table {
    /// The term of the leader that published it.
    pub term: u64,
    /// Rises with every table, across terms.
    pub version: u64,
    /// The members, in order.
    pub members: Vec<NodeId>,
    /// The members whose votes count, in order; one gone may be kept, counted absent.
    pub voting: Vec<NodeId>,
    /// Members on their way out: they own nothing new and are not kept in the voting set.
    pub draining: Vec<NodeId>,
    /// The shards, by number.
    pub shards: Vec<Shard>,
    /// Shards to be handed by their owner to another member, a replica caught up: draining
    /// members' shards, and those moved to even the load out. Each with the number of the
    /// table it was planned from: a move dropped and asked for again is another move.
    pub moves: Vec<(ShardId, NodeId, (u64, u64))>,
}

/// A table as what changed from the one before it: its number, its few lists whole, and the
/// shards that changed alone — what a leader publishes to a pod holding the one before, as
/// Elasticsearch publishes diffs, nearly all of a table being its shards.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Changes {
    /// The table's term.
    pub term: u64,
    /// Its version.
    pub version: u64,
    /// Its members.
    pub members: Vec<NodeId>,
    /// Its voting set.
    pub voting: Vec<NodeId>,
    /// Its members draining.
    pub draining: Vec<NodeId>,
    /// Its moves.
    pub moves: Vec<(ShardId, NodeId, (u64, u64))>,
    /// The shards changed, by number.
    pub shards: Vec<(ShardId, Shard)>,
}

impl Table {
    /// What changed from `base` to this table.
    #[must_use]
    pub fn changes_from(&self, base: &Self) -> Changes {
        let shards = self
            .shards
            .iter()
            .enumerate()
            .filter(|(index, shard)| base.shards.get(*index) != Some(*shard))
            .filter_map(|(index, shard)| Some((ShardId::try_from(index).ok()?, shard.clone())))
            .collect();
        Changes {
            term: self.term,
            version: self.version,
            members: self.members.clone(),
            voting: self.voting.clone(),
            draining: self.draining.clone(),
            moves: self.moves.clone(),
            shards,
        }
    }

    /// The table `changes` make of this one; none if they name a shard it lacks.
    #[must_use]
    pub fn with_changes(&self, changes: Changes) -> Option<Self> {
        let mut shards = self.shards.clone();
        for (index, shard) in changes.shards {
            *shards.get_mut(usize::from(index))? = shard;
        }
        Some(Self {
            term: changes.term,
            version: changes.version,
            members: changes.members,
            voting: changes.voting,
            draining: changes.draining,
            shards,
            moves: changes.moves,
        })
    }

    /// Its number: a later table has a larger one.
    #[must_use]
    pub const fn stamp(&self) -> (u64, u64) {
        (self.term, self.version)
    }

    /// The owner of `shard`, if it has one.
    #[must_use]
    pub fn owner(&self, shard: ShardId) -> Option<NodeId> {
        self.shards
            .get(usize::from(shard))
            .and_then(|shard| shard.owner)
    }

    /// Whether `member` is one.
    #[must_use]
    pub fn has_member(&self, member: NodeId) -> bool {
        self.members.binary_search(&member).is_ok()
    }

    /// Whether `member` votes.
    #[must_use]
    pub fn votes(&self, member: NodeId) -> bool {
        self.voting.binary_search(&member).is_ok()
    }

    /// Whether `member` is draining.
    #[must_use]
    pub fn is_draining(&self, member: NodeId) -> bool {
        self.draining.binary_search(&member).is_ok()
    }

    /// The shards `member` owns.
    pub fn owned_by(&self, member: NodeId) -> impl Iterator<Item = ShardId> + '_ {
        self.shards
            .iter()
            .enumerate()
            .filter(move |(_, shard)| shard.owner == Some(member))
            .filter_map(|(index, _)| ShardId::try_from(index).ok())
    }

    /// The same table but for its number: whether publishing `other` would change anything.
    #[must_use]
    pub fn same_content(&self, other: &Self) -> bool {
        self.members == other.members
            && self.voting == other.voting
            && self.draining == other.draining
            && self.shards == other.shards
            && self.moves == other.moves
    }

    /// Where `shard` is being moved to, if it is.
    #[must_use]
    pub fn move_of(&self, shard: ShardId) -> Option<NodeId> {
        self.asked(shard).map(|(to, _)| to)
    }

    /// Where `shard` is being moved to, if it is, and the number of the table that move was
    /// planned from.
    #[must_use]
    pub fn asked(&self, shard: ShardId) -> Option<(NodeId, (u64, u64))> {
        self.moves
            .iter()
            .find(|(moving, _, _)| *moving == shard)
            .map(|(_, to, from)| (*to, *from))
    }
}

/// The shard of `key`, among `shards`.
#[must_use]
pub fn shard_of(key: u64, shards: u16) -> ShardId {
    let shards = u64::from(shards.max(1));
    // Below `shards`, a u16, so the conversion holds.
    ShardId::try_from(mix(key) % shards).unwrap_or(0)
}

/// SplitMix64's finaliser: spreads a key's bits over the shards.
#[must_use]
pub const fn mix(mut x: u64) -> u64 {
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

/// Whether the members `have` says yes to are more than half of `voting`. An empty set has no
/// majority.
pub fn is_quorum(voting: &[NodeId], have: impl Fn(NodeId) -> bool) -> bool {
    let yes = voting.iter().filter(|member| have(**member)).count();
    yes * 2 > voting.len()
}

/// The largest odd number not above `n`, or nothing for nothing.
#[must_use]
pub const fn round_down_to_odd(n: usize) -> usize {
    if n == 0 { 0 } else { n - (1 - n % 2) }
}

/// The voting set the leader works towards, as Elasticsearch's reconfigurator chooses: the
/// live members that are not draining, rounded down to an odd number, but at least three once
/// the set has held three — a member gone is then kept, counted absent, rather than leave
/// fewer, though one draining is not, and the set is then odd all the same. Live members
/// already voting come first, then the others by identity. A set the live members are no
/// majority of is not worked towards: the present one is kept.
#[must_use]
pub fn voting_set(table: &Table, live: &BTreeSet<NodeId>) -> Vec<NodeId> {
    let eligible: Vec<NodeId> = table
        .members
        .iter()
        .copied()
        .filter(|member| live.contains(member) && !table.is_draining(*member))
        .collect();
    let floor = if table.voting.len() >= 3 { 3 } else { 1 };
    let target = round_down_to_odd(eligible.len()).max(floor);
    let mut chosen: Vec<NodeId> = eligible
        .iter()
        .copied()
        .filter(|member| table.votes(*member))
        .collect();
    for member in &eligible {
        if chosen.len() >= target {
            break;
        }
        if !chosen.contains(member) {
            chosen.push(*member);
        }
    }
    chosen.truncate(target);
    // Short of the target: voters gone stay, counted absent, as long as they are not draining.
    for member in &table.voting {
        if chosen.len() >= target {
            break;
        }
        if !chosen.contains(member) && !table.is_draining(*member) {
            chosen.push(*member);
        }
    }
    // Still short, with voters draining given way: odd all the same, the live ones kept.
    chosen.truncate(round_down_to_odd(chosen.len()));
    chosen.sort_unstable();
    if chosen.is_empty() || !is_quorum(&chosen, |member| live.contains(&member)) {
        return table.voting.clone();
    }
    chosen
}

/// The first table of a cluster formed by `members`: the lowest of them voting, as many as is
/// odd, and the shards dealt out among them, each with `replicas` copies on the members after
/// its owner, and the table's term: the generation in its upper half, so that a cluster formed
/// without the latest table, its holder cut off, outranks every owner of an earlier one.
#[must_use]
pub fn first(members: &BTreeSet<NodeId>, shards: u16, replicas: usize, term: u64) -> Table {
    let members: Vec<NodeId> = members.iter().copied().collect();
    let voting: Vec<NodeId> = members
        .iter()
        .copied()
        .take(round_down_to_odd(members.len()))
        .collect();
    let shards = (0..usize::from(shards))
        .map(|index| {
            let owner = members.get(index % members.len().max(1)).copied();
            let replicas = (1..=replicas.min(members.len().saturating_sub(1)))
                .filter_map(|step| members.get((index + step) % members.len()).copied())
                .collect();
            Shard {
                owner,
                replicas,
                term,
            }
        })
        .collect();
    Table {
        term,
        version: 1,
        members,
        voting,
        draining: Vec::new(),
        shards,
        moves: Vec::new(),
    }
}

/// What the leader changes in `table` given the members it hears (`live`), those that have
/// answered its latest heartbeats (`answering`) and those asking to join (`joining`): new
/// members taken in, members gone taken out, each shard whose owner is gone given to a live
/// replica with its term raised, replicas gone replaced so that each shard keeps `replicas`
/// live copies where it can, and the voting set worked towards. `None` when nothing changes.
/// The term and version are left to the publisher.
#[must_use]
pub fn repair(
    table: &Table,
    live: &BTreeSet<NodeId>,
    answering: &BTreeSet<NodeId>,
    joining: &BTreeSet<NodeId>,
    replicas: usize,
) -> Option<Table> {
    let mut next = table.clone();
    let mut members: BTreeSet<NodeId> = table
        .members
        .iter()
        .copied()
        .filter(|member| live.contains(member))
        .collect();
    members.extend(
        joining
            .iter()
            .copied()
            .filter(|member| live.contains(member)),
    );
    next.members = members.iter().copied().collect();
    next.draining.retain(|member| members.contains(member));
    let gone = |member: &NodeId| !members.contains(member);
    for shard in &mut next.shards {
        shard.replicas.retain(|replica| !gone(replica));
        if shard.owner.is_some_and(|owner| gone(&owner)) {
            // The first live replica not draining that has answered the leader — one the
            // leader hears but that does not hear it may hold no lease, and never serve — else
            // the first not draining, else any live replica.
            let not_draining = |replica: &NodeId| !next.draining.contains(replica);
            let promoted = shard
                .replicas
                .iter()
                .copied()
                .find(|replica| not_draining(replica) && answering.contains(replica))
                .or_else(|| shard.replicas.iter().copied().find(not_draining))
                .or_else(|| shard.replicas.first().copied());
            if let Some(promoted) = promoted {
                shard.owner = Some(promoted);
                shard.replicas.retain(|replica| *replica != promoted);
                shard.term += 1;
            }
        }
    }
    // A shard every copy of which is gone, a majority left: a live member takes it empty, its
    // counts lost, an overshoot, rather than nobody serving it for good — one that has
    // answered the leader and is not draining first, fewest owned first.
    for index in 0..next.shards.len() {
        if !next.shards[index].owner.is_some_and(|owner| gone(&owner)) {
            continue;
        }
        let taker = members.iter().copied().min_by_key(|member| {
            (
                next.draining.contains(member),
                !answering.contains(member),
                next.owned_by(*member).count(),
                *member,
            )
        });
        if let (Some(taker), Some(shard)) = (taker, next.shards.get_mut(index)) {
            shard.owner = Some(taker);
            shard.term += 1;
        }
    }
    fill_replicas(&mut next, replicas);
    next.voting = voting_set(&next, live);
    (!next.same_content(table)).then_some(next)
}

/// Gives every shard `replicas` copies where there are members for them, each new one on the
/// member holding fewest so far, never its owner, and none on a member draining.
fn fill_replicas(table: &mut Table, replicas: usize) {
    let mut load: BTreeMap<NodeId, usize> = table
        .members
        .iter()
        .filter(|member| !table.draining.contains(member))
        .map(|member| (*member, 0))
        .collect();
    for shard in &table.shards {
        for member in shard.owner.iter().chain(&shard.replicas) {
            if let Some(count) = load.get_mut(member) {
                *count += 1;
            }
        }
    }
    for shard in &mut table.shards {
        while shard.replicas.len() < replicas {
            let pick = load
                .iter()
                .filter(|(member, _)| {
                    shard.owner != Some(**member) && !shard.replicas.contains(member)
                })
                .min_by_key(|(member, count)| (**count, **member))
                .map(|(member, _)| *member);
            let Some(pick) = pick else { break };
            shard.replicas.push(pick);
            if let Some(count) = load.get_mut(&pick) {
                *count += 1;
            }
        }
    }
}

/// The moves the leader asks for, at most `most` at once: each shard of a draining owner to a
/// live replica not draining (or else the live member owning fewest), then shards from the
/// live members owning most to those owning fewest, until no two differ by more than one.
/// Moves already asked for stay while their shard's owner and target still stand; the rest go.
/// A shard is moved only to a member that has answered the leader's latest heartbeats
/// (`answering`): one the leader hears but that does not hear it may hold no lease, and never
/// serve what it takes. `None` when nothing changes.
#[must_use]
pub fn plan_moves(
    table: &Table,
    live: &BTreeSet<NodeId>,
    answering: &BTreeSet<NodeId>,
    most: usize,
) -> Option<Table> {
    let mut next = table.clone();
    let takers: Vec<NodeId> = table
        .members
        .iter()
        .copied()
        .filter(|member| {
            live.contains(member) && answering.contains(member) && !table.is_draining(*member)
        })
        .collect();
    next.moves.retain(|(shard, to, _)| {
        let owner = table.owner(*shard);
        takers.contains(to) && owner.is_some_and(|owner| owner != *to && live.contains(&owner))
    });
    let mut owned: BTreeMap<NodeId, usize> = takers.iter().map(|member| (*member, 0)).collect();
    for shard in &table.shards {
        if let Some(count) = shard.owner.and_then(|owner| owned.get_mut(&owner)) {
            *count += 1;
        }
    }
    // A move already asked for counts as done.
    for (shard, to, _) in &next.moves {
        if let Some(count) = table.owner(*shard).and_then(|owner| owned.get_mut(&owner)) {
            *count = count.saturating_sub(1);
        }
        if let Some(count) = owned.get_mut(to) {
            *count += 1;
        }
    }
    let fewest = |owned: &BTreeMap<NodeId, usize>| {
        owned
            .iter()
            .min_by_key(|(member, count)| (**count, **member))
            .map(|(member, _)| *member)
    };
    // Draining owners first.
    for (index, shard) in table.shards.iter().enumerate() {
        if next.moves.len() >= most {
            break;
        }
        let Ok(id) = ShardId::try_from(index) else {
            continue;
        };
        let Some(owner) = shard.owner else { continue };
        if !table.is_draining(owner) || next.move_of(id).is_some() || !live.contains(&owner) {
            continue;
        }
        let to = shard
            .replicas
            .iter()
            .copied()
            .find(|replica| takers.contains(replica))
            .or_else(|| fewest(&owned));
        if let Some(to) = to {
            next.moves.push((id, to, table.stamp()));
            if let Some(count) = owned.get_mut(&to) {
                *count += 1;
            }
        }
    }
    // Then evening out.
    while next.moves.len() < most {
        let Some(light) = fewest(&owned) else { break };
        let heavy = owned
            .iter()
            .max_by_key(|(member, count)| (**count, std::cmp::Reverse(**member)))
            .map(|(member, count)| (*member, *count));
        let Some((heavy, most_owned)) = heavy else {
            break;
        };
        let least = owned.get(&light).copied().unwrap_or(0);
        if most_owned <= least + 1 {
            break;
        }
        // One of the heavy member's shards not moving yet, one the light member keeps a copy
        // of if there is one.
        let candidates: Vec<ShardId> = table
            .owned_by(heavy)
            .filter(|shard| next.move_of(*shard).is_none())
            .collect();
        let pick = candidates
            .iter()
            .copied()
            .find(|shard| {
                table
                    .shards
                    .get(usize::from(*shard))
                    .is_some_and(|shard| shard.replicas.contains(&light))
            })
            .or_else(|| candidates.first().copied());
        let Some(pick) = pick else { break };
        next.moves.push((pick, light, table.stamp()));
        if let Some(count) = owned.get_mut(&heavy) {
            *count -= 1;
        }
        if let Some(count) = owned.get_mut(&light) {
            *count += 1;
        }
    }
    next.moves.sort_unstable();
    (next.moves != table.moves).then_some(next)
}

/// `table` with `shard` handed to `to`: its owner, its term raised, its move done, `to` no
/// longer among its replicas and the old owner among them instead unless it is draining.
#[must_use]
pub fn moved(table: &Table, shard: ShardId, to: NodeId) -> Option<Table> {
    let mut next = table.clone();
    let entry = next.shards.get_mut(usize::from(shard))?;
    let old = entry.owner?;
    if old == to {
        return None;
    }
    entry.owner = Some(to);
    entry.term += 1;
    entry.replicas.retain(|replica| *replica != to);
    if !table.is_draining(old) {
        entry.replicas.insert(0, old);
    }
    next.moves.retain(|(moving, _, _)| *moving != shard);
    Some(next)
}

/// `table` with `shard` handed to `to`, as its owner reports `to` caught up on the move planned
/// from table `from`: `None` unless the table asks for that move still, not one dropped since,
/// or asked for again after, for which `to` may hold nothing.
#[must_use]
pub fn handed_on(table: &Table, shard: ShardId, to: NodeId, from: (u64, u64)) -> Option<Table> {
    if table.asked(shard) != Some((to, from)) {
        return None;
    }
    moved(table, shard, to)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(members: &[NodeId]) -> BTreeSet<NodeId> {
        members.iter().copied().collect()
    }

    /// A table's changes from the one before carry only the shards changed, and make it again
    /// from that one; a shard the base lacks makes nothing.
    #[test]
    fn a_tables_changes_make_it_again_from_the_one_before() {
        let base = first(&set(&[1, 2, 3]), 6, 2, 1);
        let mut next = base.clone();
        next.version = base.version + 1;
        next.draining = vec![3];
        next.shards[2].owner = Some(1);
        next.shards[2].term += 1;
        let changes = next.changes_from(&base);
        assert_eq!(changes.shards, vec![(2, next.shards[2].clone())]);
        assert_eq!(base.with_changes(changes.clone()), Some(next));
        let short = first(&set(&[1, 2, 3]), 2, 2, 1);
        assert_eq!(short.with_changes(changes), None);
    }

    #[test]
    fn a_first_table_deals_the_shards_out_evenly_with_their_replicas() {
        let table = first(&set(&[1, 2, 3]), 6, 2, 1);
        assert_eq!(table.voting, [1, 2, 3]);
        let owners: Vec<Option<NodeId>> = table.shards.iter().map(|shard| shard.owner).collect();
        assert_eq!(
            owners,
            [Some(1), Some(2), Some(3), Some(1), Some(2), Some(3)]
        );
        assert_eq!(table.shards[0].replicas, [2, 3]);
        assert_eq!(table.shards[2].replicas, [1, 2]);
        let four = first(&set(&[1, 2, 3, 4]), 4, 2, 1);
        assert_eq!(four.voting, [1, 2, 3], "four rounded down to three");
    }

    #[test]
    fn the_voting_set_is_odd_and_keeps_three_with_one_gone() {
        let table = first(&set(&[1, 2, 3]), 3, 2, 1);
        // One gone: kept, counted absent, as three is the floor.
        assert_eq!(voting_set(&table, &set(&[1, 2])), [1, 2, 3]);
        // A fourth joins and the gone one is replaced.
        let mut four = table.clone();
        four.members = vec![1, 2, 3, 4];
        assert_eq!(voting_set(&four, &set(&[1, 2, 4])), [1, 2, 4]);
        // Five, all live: all five vote.
        let mut five = table.clone();
        five.members = vec![1, 2, 3, 4, 5];
        assert_eq!(voting_set(&five, &set(&[1, 2, 3, 4, 5])), [1, 2, 3, 4, 5]);
        // Four live of five: rounded down to three, the live voters first.
        five.voting = vec![1, 2, 3, 4, 5];
        assert_eq!(voting_set(&five, &set(&[1, 2, 3, 4])), [1, 2, 3]);
        // A draining voter gives way.
        let mut draining = four.clone();
        draining.draining = vec![1];
        assert_eq!(voting_set(&draining, &set(&[1, 2, 3, 4])), [2, 3, 4]);
        // A draining voter with only two others to take its place: one of them, not two, as
        // a set of two is no stronger than one and needs both.
        let mut two_left = table.clone();
        two_left.draining = vec![1];
        assert_eq!(voting_set(&two_left, &set(&[1, 2, 3])), [2]);
    }

    #[test]
    fn a_set_the_live_members_are_no_majority_of_is_not_taken() {
        let table = first(&set(&[1, 2, 3]), 3, 2, 1);
        // Only one live: three would be kept with two absent, which it is no majority of.
        assert_eq!(voting_set(&table, &set(&[1])), [1, 2, 3]);
    }

    #[test]
    fn a_member_gone_has_its_shards_taken_by_a_replica_with_their_term_raised() {
        let table = first(&set(&[1, 2, 3]), 3, 2, 1);
        let next = repair(&table, &set(&[2, 3]), &set(&[2, 3]), &BTreeSet::new(), 2).unwrap();
        assert_eq!(next.members, [2, 3]);
        assert_eq!(next.shards[0].owner, Some(2));
        assert_eq!(next.shards[0].term, 2);
        assert_eq!(next.shards[1].term, 1, "not its owner: unchanged");
        for shard in &next.shards {
            assert!(!shard.replicas.contains(&1));
            assert!(shard.owner.is_some());
        }
        assert_eq!(
            next.voting,
            [1, 2, 3],
            "the floor keeps the gone one, absent"
        );
        assert_eq!(
            repair(&next, &set(&[2, 3]), &set(&[2, 3]), &BTreeSet::new(), 2),
            None
        );
    }

    /// A shard every copy of which is gone, a majority left: a live member takes it empty,
    /// fewest owned first, rather than its gone owner keeping it for good — under no replicas,
    /// or with its owner and its one replica gone together.
    #[test]
    fn a_shard_with_every_copy_gone_is_taken_by_a_live_member() {
        let table = first(&set(&[1, 2, 3]), 3, 0, 1);
        let next = repair(&table, &set(&[1, 3]), &set(&[1, 3]), &BTreeSet::new(), 0).unwrap();
        assert_eq!(next.members, [1, 3]);
        for shard in &next.shards {
            assert!(shard.owner.is_some_and(|owner| owner != 2), "{next:?}");
        }
        let taken = next.shards.iter().find(|shard| shard.term == 2).unwrap();
        assert!(taken.replicas.is_empty());
        let wide = first(&set(&[1, 2, 3, 4, 5]), 10, 1, 1);
        let both = |shard: &Shard| shard.owner == Some(1) && shard.replicas == [2];
        assert!(wide.shards.iter().any(both), "{wide:?}");
        let live = set(&[3, 4, 5]);
        let next = repair(&wide, &live, &live, &BTreeSet::new(), 1).unwrap();
        for shard in &next.shards {
            assert!(
                shard.owner.is_some_and(|owner| live.contains(&owner)),
                "{next:?}"
            );
            assert_eq!(shard.replicas.len(), 1, "{shard:?}");
        }
    }

    #[test]
    fn a_member_joining_takes_replicas_and_a_vote_in_place_of_one_gone() {
        let table = first(&set(&[1, 2, 3]), 3, 2, 1);
        let gone = repair(&table, &set(&[2, 3]), &set(&[2, 3]), &BTreeSet::new(), 2).unwrap();
        let joined = repair(&gone, &set(&[2, 3, 4]), &set(&[2, 3, 4]), &set(&[4]), 2).unwrap();
        assert_eq!(joined.members, [2, 3, 4]);
        assert_eq!(joined.voting, [2, 3, 4]);
        for shard in &joined.shards {
            assert_eq!(shard.replicas.len(), 2, "{shard:?}");
        }
    }

    #[test]
    fn a_draining_owner_has_its_shards_moved_to_replicas_a_few_at_a_time() {
        let mut table = first(&set(&[1, 2, 3, 4]), 8, 2, 1);
        table.draining = vec![1];
        let next = plan_moves(&table, &set(&[1, 2, 3, 4]), &set(&[1, 2, 3, 4]), 1).unwrap();
        assert_eq!(
            next.moves,
            [(0, 2, table.stamp())],
            "one at a time, to its first replica"
        );
        let all = plan_moves(&table, &set(&[1, 2, 3, 4]), &set(&[1, 2, 3, 4]), 16).unwrap();
        assert!(all.moves.iter().any(|(shard, _, _)| *shard == 4));
        for (shard, to, _) in &all.moves {
            assert_ne!(*to, 1);
            if table.owner(*shard) == Some(1) {
                assert!(table.shards[usize::from(*shard)].replicas.contains(to));
            }
        }
        let done = moved(&all, 0, 2).unwrap();
        assert_eq!(done.owner(0), Some(2));
        assert_eq!(done.shards[0].term, 2);
        assert!(
            !done.shards[0].replicas.contains(&1),
            "draining: not kept as a copy"
        );
        assert_eq!(done.move_of(0), None);
    }

    #[test]
    fn a_member_joining_is_given_shards_until_no_two_differ_by_more_than_one() {
        let mut table = first(&set(&[1, 2, 3]), 9, 2, 1);
        table.members = vec![1, 2, 3, 4];
        let mut live = set(&[1, 2, 3, 4]);
        let mut now = table;
        for _ in 0..10 {
            let Some(next) = plan_moves(&now, &live, &live, 2) else {
                break;
            };
            now = next;
            let moves = now.moves.clone();
            for (shard, to, _) in moves {
                now = moved(&now, shard, to).unwrap();
            }
        }
        let counts: Vec<usize> = [1, 2, 3, 4]
            .iter()
            .map(|member| now.owned_by(*member).count())
            .collect();
        assert!(
            counts.iter().all(|count| (2..=3).contains(count)),
            "{counts:?}"
        );
        live.remove(&4);
        assert_eq!(
            plan_moves(&now, &live, &live, 2),
            None,
            "nothing moves to a member gone"
        );
    }

    /// A shard is moved only to a member that has answered the leader's latest heartbeats: one
    /// heard but not hearing may hold no lease, and, named the owner, never serve what it took.
    #[test]
    fn a_shard_is_moved_only_to_a_member_that_answers() {
        let all = set(&[1, 2, 3, 4]);
        let mut table = first(&all, 8, 2, 1);
        table.draining = vec![1];
        let next = plan_moves(&table, &all, &set(&[1, 3, 4]), 16).unwrap();
        assert_eq!(
            next.move_of(0),
            Some(3),
            "its first replica does not answer"
        );
        assert!(
            next.moves.iter().all(|planned| planned.1 != 2),
            "{:?}",
            next.moves
        );
    }

    /// A move dropped, its taker no longer answering, then asked for again once it answers, is
    /// another move: a report that it caught up on the first, reaching a leader late, does not
    /// complete it.
    #[test]
    fn a_move_asked_for_again_is_not_completed_by_a_report_on_the_one_before() {
        let all = set(&[1, 2, 3, 4]);
        let mut table = first(&all, 8, 2, 1);
        table.draining = vec![1];
        // Each table as published: numbered one on.
        let publish = |mut next: Table| {
            next.version += 1;
            next
        };
        let asked = publish(plan_moves(&table, &all, &all, 1).unwrap());
        let (shard, to, before) = asked.moves[0];
        let dropped = publish(plan_moves(&asked, &all, &set(&[1]), 1).unwrap());
        assert_eq!(dropped.move_of(shard), None);
        let again = publish(plan_moves(&dropped, &all, &all, 1).unwrap());
        let (_, now) = again.asked(shard).unwrap();
        assert_eq!(again.move_of(shard), Some(to));
        assert_eq!(
            handed_on(&again, shard, to, before),
            None,
            "the move before"
        );
        let done = handed_on(&again, shard, to, now).unwrap();
        assert_eq!(done.owner(shard), Some(to));
    }

    #[test]
    fn keys_spread_over_the_shards() {
        let mut counts = [0_u32; 16];
        for key in 0..16_000 {
            counts[usize::from(shard_of(key, 16))] += 1;
        }
        for count in counts {
            assert!((900..1_100).contains(&count), "{counts:?}");
        }
    }
}
