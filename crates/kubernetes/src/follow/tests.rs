use super::*;
use crate::Timing;
use crate::fake::*;
use bytes::Bytes;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::time::timeout;

/// Slice `name` at `version`.
fn slice(name: &str, version: u32) -> String {
    format!(
        r#"{{"metadata":{{"name":"{name}","namespace":"shop","resourceVersion":"{version}"}},"addressType":"IPv4","endpoints":[]}}"#
    )
}

/// A page of these slices, continuing to `next` or the last, as of `version`.
fn page(version: &str, next: Option<&str>, slices: &[String]) -> Answer {
    let next = next
        .map(|next| format!(r#","continue":"{next}""#))
        .unwrap_or_default();
    answer(
        200,
        &format!(
            r#"{{"apiVersion":"discovery.k8s.io/v1","kind":"EndpointSliceList","metadata":{{"resourceVersion":"{version}"{next}}},"items":[{}]}}"#,
            slices.join(",")
        ),
    )
}

/// A watch that sends these events, then ends.
fn events(events: &[String]) -> Answer {
    answer(
        200,
        &events.iter().map(|e| format!("{e}\n")).collect::<String>(),
    )
}

fn added(slice: &str) -> String {
    format!(r#"{{"type":"ADDED","object":{slice}}}"#)
}

/// A watch that stays open and quiet.
fn open() -> Answer {
    Answer {
        status: 200,
        chunks: vec![Bytes::from_static(b"\n")],
        pause: Duration::from_secs(60),
    }
}

fn status(code: u16) -> Answer {
    answer(
        code,
        &format!(
            r#"{{"kind":"Status","apiVersion":"v1","metadata":{{}},"status":"Failure","message":"code {code}","code":{code}}}"#
        ),
    )
}

/// The names of the slices published.
fn names(slices: &Slices) -> Vec<String> {
    slices
        .slices
        .iter()
        .map(|slice| slice.metadata.name.clone().unwrap_or_default())
        .collect()
}

const QUICK: Retry = Retry {
    first: Duration::from_millis(10),
    most: Duration::from_millis(40),
};

/// The failures a follow said.
type Said = Arc<Mutex<Vec<Failure>>>;

/// `fake` followed for namespace `shop`: what it publishes, the failures it says, and the
/// task, which ends once the receiver is dropped.
fn followed(
    fake: &Fake,
    token_name: &str,
) -> (
    watch::Receiver<Arc<Slices>>,
    Said,
    tokio::task::JoinHandle<()>,
) {
    let api = fake.client(&token(token_name, "secret"), Timing::default());
    let (out, published) = watch::channel(Arc::new(Slices::default()));
    let failures = Arc::new(Mutex::new(Vec::new()));
    let said = Arc::clone(&failures);
    let task = tokio::spawn(async move {
        follow(&api, "shop", QUICK, out, |failure| {
            said.lock().unwrap().push(failure.clone());
        })
        .await;
    });
    (published, failures, task)
}

/// The next slices published.
async fn next(published: &mut watch::Receiver<Arc<Slices>>) -> Arc<Slices> {
    timeout(Duration::from_secs(10), published.changed())
        .await
        .expect("nothing published in time")
        .unwrap();
    Arc::clone(&published.borrow_and_update())
}

/// Waits until the fake has seen a request whose path ends with `end`.
async fn asked(fake: &Fake, end: &str) {
    timeout(Duration::from_secs(10), async {
        while !fake.seen().iter().any(|seen| seen.path.ends_with(end)) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("never asked for …{end}: {:?}", fake.seen()));
}

#[tokio::test]
async fn a_list_read_whole_is_published_then_each_change_and_the_watch_resumes() {
    let watches = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&watches);
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            match counted.fetch_add(1, Ordering::SeqCst) {
                0 => events(&[added(&slice("c", 11))]),
                _ => open(),
            }
        } else if path.contains("continue=p2") {
            page("10", None, &[slice("b", 10)])
        } else {
            page("", Some("p2"), &[slice("a", 9)])
        }
    })
    .await;
    let (mut published, failures, task) = followed(&fake, "follow-changes");

    // Both pages, published once, as the namespace's slices.
    let first = next(&mut published).await;
    assert!(first.listed);
    assert_eq!(names(&first), ["a", "b"]);
    assert_eq!(names(&*next(&mut published).await), ["a", "b", "c"]);
    // The watch the server ended is begun again from the last version, at once.
    asked(&fake, "resourceVersion=11").await;
    assert!(fake.seen()[2].path.ends_with("resourceVersion=10"));
    assert!(failures.lock().unwrap().is_empty());

    drop(published);
    timeout(Duration::from_secs(5), task)
        .await
        .expect("the follow went on with no one looking")
        .unwrap();
}

#[tokio::test]
async fn a_namespace_without_slices_is_published_as_listed() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            open()
        } else {
            page("3", None, &[])
        }
    })
    .await;
    let (mut published, _, _task) = followed(&fake, "follow-empty");
    let first = next(&mut published).await;
    assert!(first.listed);
    assert!(first.slices.is_empty());
}

#[tokio::test]
async fn a_failure_is_said_and_retried_and_gone_lists_afresh() {
    let (lists, watches) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
    let (listed, watched) = (Arc::clone(&lists), Arc::clone(&watches));
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch=true") {
            match watched.fetch_add(1, Ordering::SeqCst) {
                0 => status(410),
                _ => open(),
            }
        } else {
            match listed.fetch_add(1, Ordering::SeqCst) {
                0 => status(500),
                1 => page("5", None, &[slice("a", 5)]),
                _ => page("9", None, &[slice("a", 5), slice("b", 8)]),
            }
        }
    })
    .await;
    let (mut published, failures, _task) = followed(&fake, "follow-failures");
    assert_eq!(names(&*next(&mut published).await), ["a"]);
    // After the watch is gone, the slices are listed afresh and published whole.
    assert_eq!(names(&*next(&mut published).await), ["a", "b"]);
    asked(&fake, "resourceVersion=9").await;
    let codes: Vec<u16> = failures
        .lock()
        .unwrap()
        .iter()
        .map(|failure| match failure {
            Failure::Api(ApiError::Answered { code, .. }) => *code,
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(codes, [500, 410]);
}

#[tokio::test]
async fn following_ends_while_failures_are_waited_out_once_no_one_looks() {
    let fake = Fake::start(&["127.0.0.1"], |_| status(500)).await;
    let (published, failures, task) = followed(&fake, "follow-ends");
    timeout(Duration::from_secs(10), async {
        while failures.lock().unwrap().len() < 3 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    drop(published);
    timeout(Duration::from_secs(5), task)
        .await
        .expect("the follow went on with no one looking")
        .unwrap();
}

#[test]
fn waits_double_to_the_most_each_between_half_and_all() {
    let retry = Retry {
        first: Duration::from_millis(800),
        most: Duration::from_secs(30),
    };
    let mut waits = Waits {
        retry,
        failed: 0,
        random: RandomState::new(),
    };
    let mut cap = retry.first;
    for _ in 0..40 {
        let wait = waits.next();
        assert!(wait >= cap / 2 && wait <= cap, "{wait:?} for {cap:?}");
        cap = (cap * 2).min(retry.most);
    }
    waits.failed = 0;
    assert!(waits.next() <= retry.first);
}
