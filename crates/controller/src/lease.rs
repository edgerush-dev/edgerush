//! Leader election by a Kubernetes Lease ([02 §3](../../../docs/02-architecture.md)): every
//! replica serves, and the one that holds the Lease writes status.
//!
//! The election itself is pure: told what the Lease says and what time it is, it says what to
//! write, and whether this replica leads. As client-go's elector does, it never compares
//! the Lease's renew time with its own clock, which another replica's may not agree with:
//! a Lease it sees no change in for [`Timing::duration`], by its own monotonic clock, is
//! taken as abandoned. [`elect`] runs it against the API server.

use edgerush_kubernetes::{ApiError, ApiServer};
use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use k8s_openapi::jiff::Timestamp;
use std::time::{Duration, SystemTime};
use tokio::sync::watch;
use tokio::time::Instant;

/// How the Lease is held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timing {
    /// How long a Lease unrenewed is held.
    pub duration: Duration,
    /// How long a leader goes on leading without a renewal written.
    pub renew_deadline: Duration,
    /// How often the Lease is looked at, and renewed or tried.
    pub retry: Duration,
}

impl Default for Timing {
    /// client-go's, which Contour, Envoy Gateway and ingress-nginx keep: 15 s, 10 s, 2 s.
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(15),
            renew_deadline: Duration::from_secs(10),
            retry: Duration::from_secs(2),
        }
    }
}

/// What to do with the Lease.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Act {
    /// Create it, held by this replica: there is none.
    Create(Lease),
    /// Write it so: renewed, or taken over.
    Replace(Lease),
    /// Nothing: another holds it.
    Wait,
}

/// What a Lease says of its holding: its holder, and when it was last renewed.
type Record = (Option<String>, Option<MicroTime>);

/// One replica's view of the Lease.
#[derive(Debug, Clone)]
pub(crate) struct Election {
    /// Who this replica is, as the Lease names its holder.
    me: String,
    namespace: String,
    name: String,
    timing: Timing,
    /// The holder and renew time last seen, and when they were first seen so.
    seen: Option<(Record, Instant)>,
    /// When this replica last wrote the Lease held by itself, while it still holds it.
    renewed: Option<Instant>,
}

impl Election {
    pub(crate) fn new(me: String, namespace: String, name: String, timing: Timing) -> Self {
        Self {
            me,
            namespace,
            name,
            timing,
            seen: None,
            renewed: None,
        }
    }

    /// What to write, the Lease being `current`, at `now` and `wall`, the wall clock's time.
    pub(crate) fn act(&mut self, current: Option<&Lease>, now: Instant, wall: Timestamp) -> Act {
        let Some(current) = current else {
            self.renewed = None;
            return Act::Create(self.held(None, wall, 0));
        };
        let spec = current.spec.clone().unwrap_or_default();
        let record = (spec.holder_identity.clone(), spec.renew_time.clone());
        match &self.seen {
            Some((seen, _)) if *seen == record => {}
            _ => self.seen = Some((record, now)),
        }
        let holder = spec
            .holder_identity
            .as_deref()
            .filter(|holder| !holder.is_empty());
        if holder == Some(self.me.as_str()) {
            let mut renewed = current.clone();
            if let Some(spec) = renewed.spec.as_mut() {
                spec.renew_time = Some(MicroTime(wall));
                spec.lease_duration_seconds = Some(seconds(self.timing.duration));
            }
            return Act::Replace(renewed);
        }
        // Another's: no longer this replica's, if it ever was.
        self.renewed = None;
        let since = self.seen.as_ref().map_or(now, |(_, since)| *since);
        if holder.is_some() && now < since + self.timing.duration {
            return Act::Wait;
        }
        let transitions = spec.lease_transitions.unwrap_or(0).saturating_add(1);
        Act::Replace(self.held(Some(current), wall, transitions))
    }

    /// The Lease as this replica holds it from `wall`: `current`'s, taken over, or a new one.
    fn held(&self, current: Option<&Lease>, wall: Timestamp, transitions: i32) -> Lease {
        let metadata = current.map_or_else(
            || ObjectMeta {
                name: Some(self.name.clone()),
                namespace: Some(self.namespace.clone()),
                ..ObjectMeta::default()
            },
            |current| current.metadata.clone(),
        );
        Lease {
            metadata,
            spec: Some(LeaseSpec {
                holder_identity: Some(self.me.clone()),
                lease_duration_seconds: Some(seconds(self.timing.duration)),
                acquire_time: Some(MicroTime(wall)),
                renew_time: Some(MicroTime(wall)),
                lease_transitions: Some(transitions),
                ..LeaseSpec::default()
            }),
        }
    }

    /// `written`, held by this replica, was written at `now`.
    pub(crate) fn written(&mut self, written: &Lease, now: Instant) {
        let spec = written.spec.clone().unwrap_or_default();
        self.seen = Some(((spec.holder_identity, spec.renew_time), now));
        self.renewed = Some(now);
    }

    /// Whether this replica leads at `now`: it holds the Lease, and wrote it so within the
    /// renew deadline.
    pub(crate) fn leads(&self, now: Instant) -> bool {
        self.renewed
            .is_some_and(|renewed| now < renewed + self.timing.renew_deadline)
    }
}

/// The wall clock's time, as a Lease holds one; the epoch for a clock before it.
pub(crate) fn wall_clock() -> Timestamp {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .ok()
        .and_then(|since| {
            let seconds = i64::try_from(since.as_secs()).ok()?;
            let nanoseconds = i32::try_from(since.subsec_nanos()).ok()?;
            Timestamp::new(seconds, nanoseconds).ok()
        })
        .unwrap_or(Timestamp::UNIX_EPOCH)
}

fn seconds(duration: Duration) -> i32 {
    i32::try_from(duration.as_secs()).unwrap_or(i32::MAX)
}

/// Elects, through `api`, by Lease `name` in `namespace`, `me` among the replicas, for as
/// long as it is polled: whether this one leads published on `leading`, and each failure to
/// read or write the Lease handed to `failed`, the Lease tried again at the next try.
pub async fn elect(
    api: &ApiServer,
    namespace: &str,
    name: &str,
    me: &str,
    timing: Timing,
    leading: &watch::Sender<bool>,
    failed: impl Fn(&ApiError),
) {
    let mut election = Election::new(me.to_owned(), namespace.to_owned(), name.to_owned(), timing);
    let mut ticks = tokio::time::interval(timing.retry);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        ticks.tick().await;
        let wall = wall_clock();
        match api.object::<Lease>(namespace, name).await {
            Ok(current) => {
                let written = match election.act(current.as_ref(), Instant::now(), wall) {
                    Act::Create(lease) => Some(api.create(namespace, &lease).await),
                    Act::Replace(lease) => Some(api.replace(namespace, name, &lease).await),
                    Act::Wait => None,
                };
                match written {
                    Some(Ok(lease)) => election.written(&lease, Instant::now()),
                    // Another wrote it first: the next try reads what it wrote.
                    Some(Err(error)) if error.is_conflict() => {}
                    Some(Err(error)) => failed(&error),
                    None => {}
                }
            }
            Err(error) => failed(&error),
        }
        leading.send_if_modified(|leads| {
            let now = election.leads(Instant::now());
            std::mem::replace(leads, now) != now
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn wall(seconds: i64) -> Timestamp {
        Timestamp::from_second(1_800_000_000 + seconds).unwrap()
    }

    /// The Lease, held by `holder`, renewed at `renewed`, at `version`.
    fn lease(holder: &str, renewed: i64, version: &str) -> Lease {
        Lease {
            metadata: ObjectMeta {
                name: Some("edgerush".to_owned()),
                namespace: Some("edge".to_owned()),
                resource_version: Some(version.to_owned()),
                ..ObjectMeta::default()
            },
            spec: Some(LeaseSpec {
                holder_identity: Some(holder.to_owned()),
                lease_duration_seconds: Some(15),
                renew_time: Some(MicroTime(wall(renewed))),
                lease_transitions: Some(3),
                ..LeaseSpec::default()
            }),
        }
    }

    fn election() -> Election {
        Election::new(
            "me".to_owned(),
            "edge".to_owned(),
            "edgerush".to_owned(),
            Timing::default(),
        )
    }

    fn holder(act: &Act) -> Option<String> {
        match act {
            Act::Create(lease) | Act::Replace(lease) => lease
                .spec
                .as_ref()
                .and_then(|spec| spec.holder_identity.clone()),
            Act::Wait => None,
        }
    }

    #[test]
    fn with_no_lease_one_is_made_held_by_this_replica_and_it_leads_once_written() {
        let start = Instant::now();
        let mut election = election();
        let Act::Create(made) = election.act(None, start, wall(0)) else {
            panic!("not made")
        };
        let spec = made.spec.clone().unwrap();
        assert_eq!(spec.holder_identity.as_deref(), Some("me"));
        assert_eq!(spec.lease_duration_seconds, Some(15));
        assert_eq!(spec.lease_transitions, Some(0));
        assert_eq!(spec.acquire_time, Some(MicroTime(wall(0))));
        assert_eq!(made.metadata.name.as_deref(), Some("edgerush"));
        assert_eq!(made.metadata.namespace.as_deref(), Some("edge"));
        assert!(!election.leads(start), "leads before it wrote the Lease");
        election.written(&made, start);
        assert!(election.leads(start));
        // Its own is renewed, as of the version read.
        let renewed = election.act(
            Some(&lease("me", 0, "8")),
            start + Duration::from_secs(2),
            wall(2),
        );
        let Act::Replace(renewed) = renewed else {
            panic!("not renewed")
        };
        assert_eq!(renewed.metadata.resource_version.as_deref(), Some("8"));
        assert_eq!(renewed.spec.unwrap().renew_time, Some(MicroTime(wall(2))));
        // Leading lasts the renew deadline from the last write, and no longer.
        assert!(election.leads(start + Duration::from_millis(9_900)));
        assert!(!election.leads(start + Duration::from_secs(10)));
    }

    #[test]
    fn anothers_lease_is_waited_on_until_it_goes_unrenewed_for_its_duration() {
        let start = Instant::now();
        let at = |seconds| start + Duration::from_secs(seconds);
        let mut election = election();
        // Renewed on another's clock, far from this one's: not compared.
        let theirs = lease("them", -3_600, "5");
        assert_eq!(election.act(Some(&theirs), at(0), wall(0)), Act::Wait);
        assert_eq!(election.act(Some(&theirs), at(14), wall(14)), Act::Wait);
        // Renewed meanwhile: the duration counts again from when that was seen.
        let renewed = lease("them", -3_598, "6");
        assert_eq!(election.act(Some(&renewed), at(14), wall(14)), Act::Wait);
        assert_eq!(election.act(Some(&renewed), at(28), wall(28)), Act::Wait);
        let taken = election.act(Some(&renewed), at(29), wall(29));
        assert_eq!(holder(&taken).as_deref(), Some("me"));
        let Act::Replace(taken) = taken else {
            panic!("not taken")
        };
        assert_eq!(taken.metadata.resource_version.as_deref(), Some("6"));
        let spec = taken.spec.unwrap();
        assert_eq!(spec.lease_transitions, Some(4));
        assert_eq!(spec.acquire_time, Some(MicroTime(wall(29))));
        assert!(!election.leads(at(29)), "leads before it wrote the Lease");
    }

    #[test]
    fn a_lease_with_no_holder_is_taken_at_once_and_one_taken_by_another_is_let_go() {
        let start = Instant::now();
        let mut election = election();
        let released = lease("", 0, "5");
        assert_eq!(
            holder(&election.act(Some(&released), start, wall(0))).as_deref(),
            Some("me")
        );
        election.written(&lease("me", 0, "6"), start);
        assert!(election.leads(start));
        // Another has it now: this replica stops at once.
        assert_eq!(
            election.act(Some(&lease("them", 1, "7")), start, wall(1)),
            Act::Wait
        );
        assert!(!election.leads(start));
    }
}
