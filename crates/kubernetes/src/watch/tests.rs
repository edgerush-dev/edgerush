use super::*;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ListMeta, ObjectMeta, Status};
use proptest::prelude::*;

/// Slice `name` at version `version`, its content told apart by `content`.
fn slice(name: &str, version: u64, content: u8) -> EndpointSlice {
    EndpointSlice {
        metadata: ObjectMeta {
            name: Some(name.to_owned()),
            namespace: Some("shop".to_owned()),
            resource_version: Some(version.to_string()),
            ..ObjectMeta::default()
        },
        address_type: format!("IPv4-{content}"),
        ..EndpointSlice::default()
    }
}

/// A page of these slices, continuing to `next`, or the last, as of `version`.
fn page(
    items: Vec<EndpointSlice>,
    next: Option<&str>,
    version: Option<&str>,
) -> List<EndpointSlice> {
    List {
        items,
        metadata: ListMeta {
            continue_: next.map(str::to_owned),
            resource_version: version.map(str::to_owned),
            ..ListMeta::default()
        },
    }
}

fn names(watch: &Watch<EndpointSlice>) -> Vec<String> {
    watch
        .objects()
        .map(|slice| slice.metadata.name.clone().unwrap_or_default())
        .collect()
}

fn status(code: i32) -> WatchEvent<EndpointSlice> {
    WatchEvent::ErrorStatus(Status {
        code: Some(code),
        message: Some(format!("code {code}")),
        ..Status::default()
    })
}

/// A watch that has listed `a` and `b` as of version 2.
fn listed() -> Watch<EndpointSlice> {
    let mut watch = Watch::new();
    watch
        .page(page(
            vec![slice("a", 1, 0), slice("b", 2, 0)],
            None,
            Some("2"),
        ))
        .unwrap();
    watch
}

#[test]
fn a_list_is_read_a_page_at_a_time_then_watched_from_its_version() {
    let mut watch = Watch::new();
    assert_eq!(
        watch.request(),
        Request::List {
            continue_from: None
        }
    );
    assert!(!watch.listed());

    assert_eq!(
        watch.page(page(vec![slice("a", 1, 0)], Some("next"), None)),
        Ok(false)
    );
    assert_eq!(
        watch.request(),
        Request::List {
            continue_from: Some("next".to_owned())
        }
    );
    // A part read is not the slices yet.
    assert_eq!(watch.objects().count(), 0);
    assert!(!watch.listed());

    assert_eq!(
        watch.page(page(vec![slice("b", 2, 0)], None, Some("7"))),
        Ok(true)
    );
    assert_eq!(names(&watch), ["a", "b"]);
    assert!(watch.listed());
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "7".to_owned()
        }
    );
}

#[test]
fn an_empty_list_is_listed_all_the_same() {
    let mut watch = Watch::new();
    assert_eq!(watch.page(page(Vec::new(), None, Some("3"))), Ok(false));
    assert!(watch.listed());
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "3".to_owned()
        }
    );
}

#[test]
fn changes_are_applied_and_move_the_version_on() {
    let mut watch = listed();
    assert_eq!(watch.event(WatchEvent::Added(slice("c", 3, 0))), Ok(true));
    assert_eq!(
        watch.event(WatchEvent::Modified(slice("a", 4, 1))),
        Ok(true)
    );
    // The same content again is no change, though the version moves on.
    assert_eq!(
        watch.event(WatchEvent::Modified(slice("a", 4, 1))),
        Ok(false)
    );
    assert_eq!(watch.event(WatchEvent::Deleted(slice("b", 5, 0))), Ok(true));
    assert_eq!(
        watch.event(WatchEvent::Deleted(slice("z", 6, 0))),
        Ok(false)
    );
    assert_eq!(names(&watch), ["a", "c"]);
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "6".to_owned()
        }
    );
}

#[test]
fn a_bookmark_moves_the_version_on_alone() {
    let mut watch = listed();
    let bookmark = WatchEvent::Bookmark {
        annotations: BTreeMap::new(),
        resource_version: "9".to_owned(),
    };
    assert_eq!(watch.event(bookmark), Ok(false));
    assert_eq!(names(&watch), ["a", "b"]);
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "9".to_owned()
        }
    );
}

#[test]
fn gone_lists_afresh_and_keeps_the_slices_until_the_list_is_read_whole() {
    let mut watch = listed();
    assert_eq!(watch.event(status(410)), Err(WatchError::Gone));
    assert_eq!(
        watch.request(),
        Request::List {
            continue_from: None
        }
    );
    assert_eq!(names(&watch), ["a", "b"]);
    assert!(watch.listed());
    // No change is taken while there is no watch.
    assert_eq!(
        watch.event(WatchEvent::Added(slice("c", 3, 0))),
        Err(WatchError::NotWatching)
    );

    // `b` was deleted while the watch was gone: the new list says so by leaving it out.
    assert_eq!(
        watch.page(page(vec![slice("a", 1, 0)], Some("next"), None)),
        Ok(false)
    );
    assert_eq!(names(&watch), ["a", "b"]);
    assert_eq!(
        watch.page(page(vec![slice("c", 8, 0)], None, Some("9"))),
        Ok(true)
    );
    assert_eq!(names(&watch), ["a", "c"]);
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "9".to_owned()
        }
    );
}

#[test]
fn a_list_gone_part_way_begins_again() {
    let mut watch = listed();
    watch.gone();
    watch
        .page(page(vec![slice("x", 1, 0)], Some("next"), None))
        .unwrap();
    // Its continue token expired: the list begins again, and the part read is dropped.
    watch.gone();
    assert_eq!(
        watch.request(),
        Request::List {
            continue_from: None
        }
    );
    assert_eq!(
        watch.page(page(vec![slice("a", 1, 0)], None, Some("4"))),
        Ok(true)
    );
    assert_eq!(names(&watch), ["a"]);
}

#[test]
fn the_same_slices_listed_afresh_are_no_change() {
    let mut watch = listed();
    watch.gone();
    assert_eq!(
        watch.page(page(
            vec![slice("b", 2, 0), slice("a", 1, 0)],
            None,
            Some("5")
        )),
        Ok(false)
    );
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "5".to_owned()
        }
    );
}

#[test]
fn an_answer_without_a_version_lists_afresh() {
    let mut watch = Watch::new();
    assert_eq!(
        watch.page(page(vec![slice("a", 1, 0)], None, None)),
        Err(WatchError::NoVersion)
    );
    assert_eq!(
        watch.request(),
        Request::List {
            continue_from: None
        }
    );
    assert!(!watch.listed());

    let mut watch = listed();
    let mut unversioned = slice("c", 3, 0);
    unversioned.metadata.resource_version = None;
    assert_eq!(
        watch.event(WatchEvent::Added(unversioned)),
        Err(WatchError::NoVersion)
    );
    assert_eq!(
        watch.request(),
        Request::List {
            continue_from: None
        }
    );
    assert_eq!(names(&watch), ["a", "b"]);
}

#[test]
fn another_error_resumes_the_watch_where_it_was() {
    let mut watch = listed();
    assert_eq!(
        watch.event(status(500)),
        Err(WatchError::Failed("code 500".to_owned()))
    );
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "2".to_owned()
        }
    );
    assert_eq!(names(&watch), ["a", "b"]);
}

#[test]
fn a_change_before_any_list_is_passed_over() {
    let mut watch = Watch::new();
    assert_eq!(
        watch.event(WatchEvent::Added(slice("a", 1, 0))),
        Err(WatchError::NotWatching)
    );
    assert_eq!(watch.objects().count(), 0);
}

#[test]
fn objects_are_told_apart_by_namespace_and_name() {
    let mut elsewhere = slice("a", 3, 1);
    elsewhere.metadata.namespace = Some("cart".to_owned());
    let mut watch = listed();
    assert_eq!(watch.event(WatchEvent::Added(elsewhere.clone())), Ok(true));
    // By namespace, then name.
    assert_eq!(names(&watch), ["a", "a", "b"]);
    assert_eq!(watch.objects().next(), Some(&elsewhere));
    assert_eq!(watch.event(WatchEvent::Deleted(slice("a", 4, 0))), Ok(true));
    assert_eq!(
        watch.objects().collect::<Vec<_>>(),
        [&elsewhere, &slice("b", 2, 0)]
    );
}

#[test]
fn a_slice_without_a_name_is_passed_over() {
    let mut nameless = slice("a", 3, 0);
    nameless.metadata.name = None;
    let mut watch = Watch::new();
    assert_eq!(
        watch.page(page(
            vec![nameless.clone(), slice("b", 2, 0)],
            None,
            Some("3")
        )),
        Ok(true)
    );
    assert_eq!(names(&watch), ["b"]);
    assert_eq!(watch.event(WatchEvent::Added(nameless)), Ok(false));
    assert_eq!(
        watch.request(),
        Request::Watch {
            from: "3".to_owned()
        }
    );
}

/// What happens in the cluster, and to the watch, a step at a time.
#[derive(Debug, Clone)]
enum Step {
    /// Slice `name` is put with this content, and the watch is told.
    Put(u8, u8),
    /// Slice `name` is deleted, and the watch is told.
    Delete(u8),
    /// A bookmark.
    Bookmark,
    /// The cluster changes this slice while the watch has lost its version (410): it
    /// lists afresh, in pages of this size.
    GoneWhile(u8, u8, usize),
}

fn steps() -> impl Strategy<Value = Vec<Step>> {
    let step = prop_oneof![
        (0..5u8, 0..3u8).prop_map(|(name, content)| Step::Put(name, content)),
        (0..5u8).prop_map(Step::Delete),
        Just(Step::Bookmark),
        (0..5u8, 0..3u8, 1..4usize)
            .prop_map(|(name, content, size)| Step::GoneWhile(name, content, size)),
    ];
    prop::collection::vec(step, 0..30)
}

proptest! {
    /// Whatever the cluster does, and however often the watch loses its version, the slices
    /// are the cluster's after each answer read whole, and stay what they were while a list
    /// is part read.
    #[test]
    fn the_slices_follow_the_cluster(steps in steps()) {
        let mut cluster: BTreeMap<String, EndpointSlice> = BTreeMap::new();
        let mut version = 1u64;
        let mut watch = Watch::new();
        watch.page(page(Vec::new(), None, Some("1"))).unwrap();
        for step in steps {
            version += 1;
            match step {
                Step::Put(name, content) => {
                    let name = format!("s{name}");
                    let put = slice(&name, version, content);
                    let event = match cluster.insert(name, put.clone()) {
                        None => WatchEvent::Added(put),
                        Some(_) => WatchEvent::Modified(put),
                    };
                    watch.event(event).unwrap();
                }
                Step::Delete(name) => {
                    let name = format!("s{name}");
                    let event = match cluster.remove(&name) {
                        Some(mut deleted) => {
                            deleted.metadata.resource_version = Some(version.to_string());
                            WatchEvent::Deleted(deleted)
                        }
                        // Nothing to delete: the version moves on by a bookmark instead.
                        None => WatchEvent::Bookmark {
                            annotations: BTreeMap::new(),
                            resource_version: version.to_string(),
                        },
                    };
                    watch.event(event).unwrap();
                }
                Step::Bookmark => {
                    let bookmark = WatchEvent::Bookmark {
                        annotations: BTreeMap::new(),
                        resource_version: version.to_string(),
                    };
                    prop_assert_eq!(watch.event(bookmark), Ok(false));
                }
                Step::GoneWhile(name, content, size) => {
                    let before: Vec<EndpointSlice> = watch.objects().cloned().collect();
                    prop_assert_eq!(watch.event(status(410)), Err(WatchError::Gone));
                    let name = format!("s{name}");
                    cluster.insert(name.clone(), slice(&name, version, content));
                    let items: Vec<EndpointSlice> = cluster.values().cloned().collect();
                    let pages: Vec<&[EndpointSlice]> = items.chunks(size).collect();
                    for (at, part) in pages.iter().enumerate() {
                        let last = at + 1 == pages.len();
                        let next = (!last).then(|| format!("page{}", at + 1));
                        watch.page(page(part.to_vec(), next.as_deref(), last.then(|| version.to_string()).as_deref())).unwrap();
                        if !last {
                            let now: Vec<EndpointSlice> = watch.objects().cloned().collect();
                            prop_assert_eq!(&now, &before);
                        }
                    }
                    if pages.is_empty() {
                        watch.page(page(Vec::new(), None, Some(&version.to_string()))).unwrap();
                    }
                }
            }
            let now: Vec<&EndpointSlice> = watch.objects().collect();
            let expected: Vec<&EndpointSlice> = cluster.values().collect();
            prop_assert_eq!(now, expected);
            prop_assert_eq!(watch.request(), Request::Watch { from: version.to_string() });
        }
    }
}
