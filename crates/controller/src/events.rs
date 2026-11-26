//! Events on the objects with no status to say what a translation tells of them
//! ([04 §6](../../../docs/04-control-plane.md)): an Ingress, a Service, a RateLimitPolicy
//! none of whose targets is there. The leader alone writes them, as it does status: one
//! for each distinct thing told of an object, however many data planes tell it, created
//! as it is first told and refreshed every [`REFRESH`] while it holds, so that it outlives
//! the API server's Events, an hour by default; once it is no longer told it is no longer
//! written, and expires. What it writes follows what is told, not the cluster's churn.
//!
//! [`Ledger`] decides what is due, and holds no clock of its own; [`write()`] does it.

use crate::Said;
use edgerush_kubernetes::{ApiServer, lease};
use edgerush_telemetry::{Exposition, Kind};
use edgerush_translate::Told;
use k8s_openapi::api::core::v1::ObjectReference;
use k8s_openapi::api::events::v1::Event;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{MicroTime, ObjectMeta};
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::time::Duration;
use tokio::sync::watch;
use tokio::time::Instant;

/// How long an Event goes unwritten while what it tells holds: well within the API server's
/// default TTL of an hour, as client-go refreshes its series.
pub const REFRESH: Duration = Duration::from_secs(30 * 60);

/// How long after a write that failed it is tried again.
pub const RETRY: Duration = Duration::from_secs(60);

/// The most bytes of an Event's `note`, as the API server takes it.
pub const MOST_NOTE: usize = 1024;

/// The controller that reports them.
pub const CONTROLLER: &str = "edgerush.dev/control-plane";

/// What the controller did, as every Event of it says.
pub const ACTION: &str = "Translate";

/// The counter of the Events written.
pub const COUNTER: &str = "edgerush_events_written_total";

/// One thing told of an object, as it is now: the object's uid with it, so that one made
/// again under its name is told afresh, not given its predecessor's Event.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Wanted {
    /// What is told.
    pub told: Told,
    /// The object's uid.
    pub uid: String,
}

/// A write that is due.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Write {
    /// An Event to create.
    Create(Wanted),
    /// An Event `name` to refresh: its series to say it has been seen `count` times.
    Refresh {
        /// What it tells.
        wanted: Wanted,
        /// Its name.
        name: String,
        /// Its count, this time among them.
        count: i32,
    },
}

impl Write {
    /// What it tells.
    #[must_use]
    pub fn wanted(&self) -> &Wanted {
        match self {
            Self::Create(wanted) | Self::Refresh { wanted, .. } => wanted,
        }
    }
}

/// What came of a write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Created, by the name the API server gave it.
    Created(String),
    /// Refreshed.
    Refreshed,
    /// The Event refreshed was not there: deleted, or expired sooner than [`REFRESH`].
    Gone,
    /// It failed, and is tried again after [`RETRY`].
    Failed,
}

/// An Event written, or one whose creation failed and is due again.
#[derive(Debug)]
struct Entry {
    /// Its name; none until it is created.
    name: Option<String>,
    /// How many times it has been written.
    count: i32,
    /// When it is next due.
    due: Instant,
}

/// The Events this replica has written while it leads, and when each is next due.
#[derive(Debug, Default)]
pub struct Ledger {
    written: BTreeMap<Wanted, Entry>,
}

impl Ledger {
    /// None written.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// What is due at `now` for what is `wanted`: each new one created, each written
    /// [`REFRESH`] ago refreshed, and each that failed [`RETRY`] ago written again. What is
    /// no longer wanted is forgotten.
    pub fn due(&mut self, wanted: &BTreeSet<Wanted>, now: Instant) -> Vec<Write> {
        self.written.retain(|written, _| wanted.contains(written));
        wanted
            .iter()
            .filter_map(|wanted| match self.written.get(wanted) {
                None => Some(Write::Create(wanted.clone())),
                Some(entry) if entry.due > now => None,
                Some(Entry { name: None, .. }) => Some(Write::Create(wanted.clone())),
                Some(Entry {
                    name: Some(name),
                    count,
                    ..
                }) => Some(Write::Refresh {
                    wanted: wanted.clone(),
                    name: name.clone(),
                    count: count.saturating_add(1),
                }),
            })
            .collect()
    }

    /// Takes what came of `write` at `now`.
    pub fn done(&mut self, write: &Write, outcome: Outcome, now: Instant) {
        let wanted = write.wanted().clone();
        match (write, outcome) {
            (_, Outcome::Created(name)) => {
                self.written.insert(
                    wanted,
                    Entry {
                        name: Some(name),
                        count: 1,
                        due: now + REFRESH,
                    },
                );
            }
            (Write::Refresh { count, .. }, Outcome::Refreshed) => {
                if let Some(entry) = self.written.get_mut(&wanted) {
                    entry.count = *count;
                    entry.due = now + REFRESH;
                }
            }
            (_, Outcome::Gone) => {
                self.written.insert(
                    wanted,
                    Entry {
                        name: None,
                        count: 0,
                        due: now,
                    },
                );
            }
            (_, Outcome::Failed | Outcome::Refreshed) => {
                self.written
                    .entry(wanted)
                    .and_modify(|entry| entry.due = now + RETRY)
                    .or_insert(Entry {
                        name: None,
                        count: 0,
                        due: now + RETRY,
                    });
            }
        }
    }

    /// When the next write is due, of those written; none if none is.
    #[must_use]
    pub fn next(&self) -> Option<Instant> {
        self.written.values().map(|entry| entry.due).min()
    }

    /// Forgets every Event written, as this replica stops leading: the next to lead writes
    /// its own.
    pub fn forget(&mut self) {
        self.written.clear();
    }
}

/// What was written of an Event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Written {
    /// It was created.
    Created,
    /// Its series was refreshed.
    Refreshed,
}

/// The counter of the Events written, by outcome.
#[derive(Debug, Default)]
pub struct EventsWritten {
    created: u64,
    refreshed: u64,
    failed: u64,
}

impl EventsWritten {
    /// None written yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes `said`, counting it if it says an Event written or not.
    pub fn take(&mut self, said: &Said<'_>) {
        if let Said::Event { written, .. } = said {
            let counted = match written {
                Ok(Written::Created) => &mut self.created,
                Ok(Written::Refreshed) => &mut self.refreshed,
                Err(_) => &mut self.failed,
            };
            *counted = counted.saturating_add(1);
        }
    }

    /// The counter, in the Prometheus text format.
    #[must_use]
    pub fn exposition(&self) -> String {
        let mut exposition = Exposition::new();
        exposition.family(
            COUNTER,
            Kind::Counter,
            "Events written on objects with no status to say what a translation tells of them, by outcome.",
        );
        for (outcome, count) in [
            ("created", self.created),
            ("refreshed", self.refreshed),
            ("failed", self.failed),
        ] {
            exposition.sample(COUNTER, &[("outcome", outcome)], count);
        }
        exposition.finish()
    }
}

/// The Event to create for `wanted`, at `now`, by `instance`.
#[must_use]
pub fn event(wanted: &Wanted, instance: &str, now: MicroTime) -> Event {
    let object = &wanted.told.object;
    Event {
        metadata: ObjectMeta {
            // The API server adds five characters to make it unique; a prefix may end in a
            // `-`, never a `.`.
            generate_name: Some(format!("{}-", object.name)),
            namespace: Some(object.namespace.clone()),
            ..ObjectMeta::default()
        },
        event_time: Some(now),
        type_: Some(
            if wanted.told.warning {
                "Warning"
            } else {
                "Normal"
            }
            .to_owned(),
        ),
        reason: Some(wanted.told.reason.to_owned()),
        action: Some(ACTION.to_owned()),
        note: Some(cut(&wanted.told.note).to_owned()),
        regarding: Some(ObjectReference {
            api_version: Some(api_version(object.kind).to_owned()),
            kind: Some(object.kind.to_owned()),
            namespace: Some(object.namespace.clone()),
            name: Some(object.name.clone()),
            uid: Some(wanted.uid.clone()),
            ..ObjectReference::default()
        }),
        reporting_controller: Some(CONTROLLER.to_owned()),
        reporting_instance: Some(instance.to_owned()),
        ..Event::default()
    }
}

/// The merge patch that refreshes an Event's series to `count`, last seen at `now`.
#[must_use]
pub fn refresh(count: i32, now: &MicroTime) -> serde_json::Value {
    serde_json::json!({ "series": { "count": count, "lastObservedTime": now } })
}

/// `note` as an Event takes it: at most [`MOST_NOTE`] bytes, cut at a character's start.
#[must_use]
pub fn cut(note: &str) -> &str {
    if note.len() <= MOST_NOTE {
        return note;
    }
    let mut end = MOST_NOTE;
    while !note.is_char_boundary(end) {
        end -= 1;
    }
    &note[..end]
}

/// The API version of the kinds told of.
fn api_version(kind: &str) -> &'static str {
    match kind {
        "Ingress" => "networking.k8s.io/v1",
        "RateLimitPolicy" => "gateway.edgerush.dev/v1alpha1",
        _ => "v1",
    }
}

/// Writes, through `api`, as `instance`, the Events of what `wanted` holds, as far as
/// `leading` says this replica leads (always, without it), and says each write to `said`;
/// a refusal for want of the rights is said as it starts and as it stops, apart. Every
/// Event is forgotten as this replica stops leading. Runs for as long as it is polled, or
/// until `wanted`'s sender is gone.
pub async fn write(
    api: Rc<ApiServer>,
    instance: String,
    mut wanted: watch::Receiver<BTreeSet<Wanted>>,
    mut leading: Option<watch::Receiver<bool>>,
    said: Rc<dyn Fn(Said<'_>)>,
) {
    let mut ledger = Ledger::new();
    let mut forbidden = false;
    let leads = |leading: &Option<watch::Receiver<bool>>| {
        leading.as_ref().is_none_or(|leading| *leading.borrow())
    };
    loop {
        if leads(&leading) {
            let now = Instant::now();
            let set = wanted.borrow_and_update().clone();
            for write in ledger.due(&set, now) {
                if !leads(&leading) {
                    break;
                }
                let at = MicroTime(lease::wall_clock());
                let (outcome, answer) = match &write {
                    Write::Create(wanted) => {
                        let event = event(wanted, &instance, at);
                        match api.create(&wanted.told.object.namespace, &event).await {
                            Ok(made) => match made.metadata.name {
                                Some(name) => (Outcome::Created(name), Ok(Written::Created)),
                                // Made, but unnamed: it cannot be refreshed, so it is made
                                // again once due.
                                None => (Outcome::Failed, Ok(Written::Created)),
                            },
                            Err(error) => (Outcome::Failed, Err(error)),
                        }
                    }
                    Write::Refresh {
                        wanted,
                        name,
                        count,
                    } => {
                        let patch = refresh(*count, &at);
                        match api
                            .patch::<Event>(&wanted.told.object.namespace, name, &patch)
                            .await
                        {
                            Ok(()) => (Outcome::Refreshed, Ok(Written::Refreshed)),
                            Err(error) if error.is_not_found() => (Outcome::Gone, Err(error)),
                            Err(error) => (Outcome::Failed, Err(error)),
                        }
                    }
                };
                // Refused for want of the rights: said as it starts, and as a write next
                // succeeds, not at every try.
                let refused = matches!(&answer, Err(error) if error.is_forbidden());
                match &answer {
                    Err(error) if refused && !forbidden => {
                        said(Said::EventsForbidden(Some(error)));
                        forbidden = true;
                    }
                    Ok(_) if forbidden => {
                        said(Said::EventsForbidden(None));
                        forbidden = false;
                    }
                    _ => {}
                }
                said(Said::Event {
                    told: &write.wanted().told,
                    written: answer.as_ref().map(|written| *written),
                    forbidden: refused,
                });
                ledger.done(&write, outcome, Instant::now());
            }
        } else {
            ledger.forget();
        }
        // Until what is wanted changes, the leadership does, or the next write is due.
        let next = ledger.next().filter(|_| leads(&leading));
        let leadership = async {
            match leading.as_mut() {
                Some(leading) => leading.changed().await.is_ok(),
                None => std::future::pending().await,
            }
        };
        let due = async {
            match next {
                Some(next) => tokio::time::sleep_until(next).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            changed = wanted.changed() => if changed.is_err() { return },
            // The election gone, this replica no longer knows that it leads.
            changed = leadership => if !changed { return },
            () = due => {}
        }
    }
}

#[cfg(test)]
mod tests;
