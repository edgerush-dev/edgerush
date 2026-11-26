use super::*;
use edgerush_kubernetes::ApiError;
use edgerush_translate::Object;

fn told(kind: &'static str, name: &str, note: &str) -> Told {
    Told {
        object: Object {
            kind,
            namespace: "shop".to_owned(),
            name: name.to_owned(),
        },
        warning: true,
        reason: "NotSupportedYet",
        note: note.to_owned(),
    }
}

fn wanted(name: &str, note: &str) -> Wanted {
    Wanted {
        told: told("Ingress", name, note),
        uid: format!("uid-{name}"),
    }
}

fn set(wanted: &[&Wanted]) -> BTreeSet<Wanted> {
    wanted.iter().map(|&wanted| wanted.clone()).collect()
}

/// Every write due, done as `outcome` says for each, at `now`.
fn do_all(
    ledger: &mut Ledger,
    wanted: &BTreeSet<Wanted>,
    now: Instant,
    outcome: impl Fn(&Write) -> Outcome,
) -> Vec<Write> {
    let due = ledger.due(wanted, now);
    for write in &due {
        ledger.done(write, outcome(write), now);
    }
    due
}

fn created(write: &Write) -> Outcome {
    match write {
        Write::Create(wanted) => Outcome::Created(format!("{}.1", wanted.told.object.name)),
        Write::Refresh { .. } => Outcome::Refreshed,
    }
}

/// What is first told is created, and nothing more is due until [`REFRESH`] has passed:
/// then its series is refreshed, counted on, and again a [`REFRESH`] later; however many
/// times what is wanted is given again meanwhile, as each round gives it (scenarios 1-3).
#[test]
fn an_event_is_created_once_and_refreshed_every_half_hour_while_told() {
    let web = wanted("web", "not supported yet: annotations");
    let wanted = set(&[&web]);
    let mut ledger = Ledger::new();
    let start = Instant::now();
    assert_eq!(
        do_all(&mut ledger, &wanted, start, created),
        [Write::Create(web.clone())]
    );
    assert_eq!(ledger.next(), Some(start + REFRESH));
    for minute in 1..30 {
        let now = start + Duration::from_secs(60 * minute);
        assert_eq!(ledger.due(&wanted, now), [], "at minute {minute}");
    }
    let refreshed = |count| Write::Refresh {
        wanted: web.clone(),
        name: "web.1".to_owned(),
        count,
    };
    assert_eq!(
        do_all(&mut ledger, &wanted, start + REFRESH, created),
        [refreshed(2)]
    );
    assert_eq!(ledger.due(&wanted, start + REFRESH + RETRY), []);
    assert_eq!(
        do_all(&mut ledger, &wanted, start + REFRESH * 2, created),
        [refreshed(3)]
    );
}

/// What is told no longer is forgotten, never written again, and left to expire; told
/// again, it is created afresh. A changed note is a new thing told: created, the old one
/// forgotten (scenarios 4, 5).
#[test]
fn what_is_no_longer_told_is_forgotten_and_a_changed_note_is_new() {
    let first = wanted("web", "annotation `a` is neither \"true\" nor \"false\"");
    let second = wanted("web", "annotation `b` is neither \"true\" nor \"false\"");
    let mut ledger = Ledger::new();
    let start = Instant::now();
    do_all(&mut ledger, &set(&[&first]), start, created);
    assert_eq!(
        do_all(&mut ledger, &set(&[&second]), start, created),
        [Write::Create(second.clone())]
    );
    assert_eq!(ledger.next(), Some(start + REFRESH));
    assert_eq!(ledger.due(&set(&[]), start + REFRESH), []);
    assert_eq!(ledger.next(), None);
    assert_eq!(
        ledger.due(&set(&[&first]), start + REFRESH),
        [Write::Create(first)]
    );
}

/// An object made again under its name has another uid: told afresh, its predecessor's
/// Event not refreshed for it (scenario 14).
#[test]
fn an_object_made_again_is_told_afresh() {
    let before = wanted("web", "not supported yet: annotations");
    let after = Wanted {
        uid: "uid-web-again".to_owned(),
        ..before.clone()
    };
    let mut ledger = Ledger::new();
    let start = Instant::now();
    do_all(&mut ledger, &set(&[&before]), start, created);
    assert_eq!(
        ledger.due(&set(&[&after]), start + REFRESH),
        [Write::Create(after)]
    );
}

/// A write that fails is due again a [`RETRY`] later, and not before: a creation, created
/// then; a refresh, refreshed then, its count where it was (scenario 11).
#[test]
fn a_write_that_fails_is_tried_again_a_minute_later() {
    let web = wanted("web", "not supported yet: annotations");
    let wanted = set(&[&web]);
    let mut ledger = Ledger::new();
    let start = Instant::now();
    do_all(&mut ledger, &wanted, start, |_| Outcome::Failed);
    assert_eq!(ledger.next(), Some(start + RETRY));
    assert_eq!(ledger.due(&wanted, start + RETRY / 2), []);
    let retried = start + RETRY;
    assert_eq!(
        do_all(&mut ledger, &wanted, retried, created),
        [Write::Create(web.clone())]
    );
    let refreshing = retried + REFRESH;
    do_all(&mut ledger, &wanted, refreshing, |_| Outcome::Failed);
    assert_eq!(ledger.next(), Some(refreshing + RETRY));
    assert_eq!(
        ledger.due(&wanted, refreshing + RETRY),
        [Write::Refresh {
            wanted: web,
            name: "web.1".to_owned(),
            count: 2,
        }]
    );
}

/// An Event refreshed that is not there, deleted or expired, is created afresh at once
/// (scenario 12).
#[test]
fn a_refresh_of_an_event_not_there_creates_it_afresh() {
    let web = wanted("web", "not supported yet: annotations");
    let wanted = set(&[&web]);
    let mut ledger = Ledger::new();
    let start = Instant::now();
    do_all(&mut ledger, &wanted, start, created);
    let refreshing = start + REFRESH;
    do_all(&mut ledger, &wanted, refreshing, |_| Outcome::Gone);
    assert_eq!(ledger.next(), Some(refreshing));
    assert_eq!(
        do_all(&mut ledger, &wanted, refreshing, created),
        [Write::Create(web.clone())]
    );
    assert_eq!(
        ledger.due(&wanted, refreshing + REFRESH),
        [Write::Refresh {
            wanted: web,
            name: "web.1".to_owned(),
            count: 2,
        }]
    );
}

/// A replica that stops leading forgets what it wrote: as it leads again, everything told
/// is new to it (scenario 10).
#[test]
fn what_was_written_is_forgotten_as_leading_stops() {
    let web = wanted("web", "not supported yet: annotations");
    let wanted = set(&[&web]);
    let mut ledger = Ledger::new();
    let start = Instant::now();
    do_all(&mut ledger, &wanted, start, created);
    ledger.forget();
    assert_eq!(ledger.next(), None);
    assert_eq!(ledger.due(&wanted, start), [Write::Create(web)]);
}

/// The Event says what is told of the object, its uid among it, and who reports it.
#[test]
fn an_event_says_what_is_told_of_its_object() {
    let mut web = wanted("web", "not supported yet: annotations");
    let at = MicroTime(lease::wall_clock());
    let event = event(&web, "control-0", at.clone());
    assert_eq!(event.metadata.generate_name.as_deref(), Some("web-"));
    assert_eq!(event.metadata.name, None);
    assert_eq!(event.metadata.namespace.as_deref(), Some("shop"));
    assert_eq!(event.event_time, Some(at));
    assert_eq!(event.type_.as_deref(), Some("Warning"));
    assert_eq!(event.reason.as_deref(), Some("NotSupportedYet"));
    assert_eq!(event.action.as_deref(), Some(ACTION));
    assert_eq!(
        event.note.as_deref(),
        Some("not supported yet: annotations")
    );
    assert_eq!(event.reporting_controller.as_deref(), Some(CONTROLLER));
    assert_eq!(event.reporting_instance.as_deref(), Some("control-0"));
    assert_eq!(event.series, None);
    let regarding = event.regarding.unwrap();
    assert_eq!(
        regarding.api_version.as_deref(),
        Some("networking.k8s.io/v1")
    );
    assert_eq!(regarding.kind.as_deref(), Some("Ingress"));
    assert_eq!(regarding.namespace.as_deref(), Some("shop"));
    assert_eq!(regarding.name.as_deref(), Some("web"));
    assert_eq!(regarding.uid.as_deref(), Some("uid-web"));
    // A note is Normal.
    web.told.warning = false;
    let event = super::event(&web, "control-0", MicroTime(lease::wall_clock()));
    assert_eq!(event.type_.as_deref(), Some("Normal"));
    for (kind, version) in [
        ("Service", "v1"),
        ("RateLimitPolicy", "gateway.edgerush.dev/v1alpha1"),
    ] {
        let told = Wanted {
            told: told(kind, "web", "x"),
            uid: "u".to_owned(),
        };
        let event = super::event(&told, "control-0", MicroTime(lease::wall_clock()));
        assert_eq!(
            event.regarding.unwrap().api_version.as_deref(),
            Some(version),
            "{kind}"
        );
    }
}

/// A refresh merges the series alone, its count and when last seen.
#[test]
fn a_refresh_patches_the_series_alone() {
    let at = MicroTime(lease::wall_clock());
    let patch = refresh(3, &at);
    assert_eq!(
        patch,
        serde_json::json!({ "series": { "count": 3, "lastObservedTime": at } })
    );
}

/// A note past what the API server takes is cut there, at a character's start (scenario
/// 15).
#[test]
fn a_long_note_is_cut_at_a_characters_start() {
    let short = "a".repeat(MOST_NOTE);
    assert_eq!(cut(&short), short);
    let long = "a".repeat(MOST_NOTE + 1);
    assert_eq!(cut(&long).len(), MOST_NOTE);
    // A two-byte character straddling the limit is left out whole.
    let straddling = format!("{}é", "a".repeat(MOST_NOTE - 1));
    assert_eq!(cut(&straddling), "a".repeat(MOST_NOTE - 1));
}

/// Each Event written, or not, is counted by its outcome.
#[test]
fn events_written_are_counted_by_outcome() {
    let web = told("Ingress", "web", "x");
    let refusal = ApiError::Answered {
        code: 403,
        message: "forbidden".to_owned(),
    };
    let mut counted = EventsWritten::new();
    for written in [
        Ok(Written::Created),
        Ok(Written::Created),
        Ok(Written::Refreshed),
        Err(&refusal),
    ] {
        counted.take(&Said::Event {
            told: &web,
            written,
            forbidden: false,
        });
    }
    let exposition = counted.exposition();
    for line in [
        "edgerush_events_written_total{outcome=\"created\"} 2",
        "edgerush_events_written_total{outcome=\"refreshed\"} 1",
        "edgerush_events_written_total{outcome=\"failed\"} 1",
    ] {
        assert!(exposition.contains(line), "{line} in {exposition}");
    }
}
