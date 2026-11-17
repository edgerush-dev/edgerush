#![allow(
    clippy::unwrap_used,
    reason = "tests: an object that cannot be read is a broken test"
)]

use super::*;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
use k8s_openapi::api::core::v1::ConfigMap;
use std::time::Duration;

fn namespace(name: &str) -> Scope {
    Scope::Namespace(name.to_owned())
}

fn parts(names: &[&str]) -> BTreeSet<Scope> {
    names.iter().map(|&name| namespace(name)).collect()
}

/// A list of ConfigMaps `names` in `namespace`, as the API server answers it.
fn config_maps(namespace: &str, names: &[&str]) -> String {
    let items: Vec<serde_json::Value> = names
        .iter()
        .map(|name| serde_json::json!({ "metadata": { "name": name, "namespace": namespace } }))
        .collect();
    serde_json::json!({
        "apiVersion": "v1",
        "kind": "ConfigMapList",
        "metadata": { "resourceVersion": "5" },
        "items": items,
    })
    .to_string()
}

/// A fake that lists ConfigMaps `a` in namespace `a` and `b` in `b`, Namespaces `a` and `b`
/// for a selector, and refuses every other list; each watch open and quiet.
async fn fake() -> Fake {
    Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            return Answer {
                status: 200,
                chunks: vec![bytes::Bytes::from_static(b"\n")],
                pause: Duration::from_secs(60),
            };
        }
        for name in ["a", "b"] {
            if path.starts_with(&format!("/api/v1/namespaces/{name}/configmaps?")) {
                return answer(200, &config_maps(name, &[name]));
            }
        }
        if path.starts_with("/api/v1/namespaces?") && path.contains("labelSelector=team") {
            let list = serde_json::json!({
                "apiVersion": "v1",
                "kind": "NamespaceList",
                "metadata": { "resourceVersion": "5" },
                "items": [{ "metadata": { "name": "a" } }, { "metadata": { "name": "b" } }],
            });
            return answer(200, &list.to_string());
        }
        answer(403, "")
    })
    .await
}

/// Waits until `seen` holds what `wanted` says of it, for 5 s at most.
async fn until<T>(seen: &watch::Receiver<T>, wanted: impl Fn(&T) -> bool) -> bool {
    let mut seen = seen.clone();
    tokio::time::timeout(Duration::from_secs(5), seen.wait_for(|now| wanted(now)))
        .await
        .is_ok()
}

fn names(across: &Across<ConfigMap>) -> Vec<String> {
    across
        .objects()
        .into_iter()
        .filter_map(|object| object.metadata.name)
        .collect()
}

#[test]
fn only_parts_listed_give_objects() {
    let listed = |names: &[&str], listed: bool| {
        Arc::new(Listed {
            listed,
            objects: names
                .iter()
                .map(|&name| ConfigMap {
                    metadata: k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta {
                        name: Some(name.to_owned()),
                        ..Default::default()
                    },
                    ..ConfigMap::default()
                })
                .collect(),
        })
    };
    let across = Across {
        parts: BTreeMap::from([
            (namespace("a"), listed(&["a"], true)),
            (namespace("b"), listed(&[], false)),
        ]),
        listed: true,
    };
    assert_eq!(names(&across), ["a"]);
    assert!(across.listed_in(&namespace("a")));
    assert!(!across.listed_in(&namespace("b")));
    assert!(!across.listed_in(&namespace("c")));
}

/// Listed once every part first held is; a part added after that cannot be listed holds
/// up none of the others, and one let go of takes its objects with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_added_later_is_not_waited_on_and_one_let_go_of_is_gone() {
    let fake = fake().await;
    let api = Rc::new(fake.client(&token("across", "secret"), Timing::default()));
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (setting, held) = watch::channel(None);
            let (out, across) = watch::channel(Arc::new(Across::<ConfigMap>::default()));
            let _following = tokio::task::spawn_local(follow_across(api, held, out, |_, _| {}));
            // Nothing is followed until the parts are known.
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(across.borrow().parts.is_empty());
            setting.send_replace(Some(parts(&["a", "b"])));
            assert!(
                until(&across, |now| now.listed && names(now) == ["a", "b"]).await,
                "{:?}",
                across.borrow()
            );
            // `c` is refused: followed, never listed, and waited on by nothing.
            setting.send_replace(Some(parts(&["a", "b", "c"])));
            assert!(
                until(&across, |now| now.parts.contains_key(&namespace("c"))).await,
                "{:?}",
                across.borrow()
            );
            assert!(across.borrow().listed);
            assert_eq!(names(&across.borrow()), ["a", "b"]);
            setting.send_replace(Some(parts(&["b", "c"])));
            assert!(
                until(&across, |now| now.listed && names(now) == ["b"]).await,
                "{:?}",
                across.borrow()
            );
        })
        .await;
}

/// A part first held that cannot be listed holds the whole back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_first_held_is_waited_on() {
    let fake = fake().await;
    let api = Rc::new(fake.client(&token("across", "secret"), Timing::default()));
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_setting, held) = watch::channel(Some(parts(&["a", "c"])));
            let (out, across) = watch::channel(Arc::new(Across::<ConfigMap>::default()));
            let _following = tokio::task::spawn_local(follow_across(api, held, out, |_, _| {}));
            assert!(
                until(&across, |now| now.listed_in(&namespace("a"))).await,
                "{:?}",
                across.borrow()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!across.borrow().listed);
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_scope_of_parts_holds_its_own_namespace_beside_them_and_the_cluster_alone() {
    let fake = fake().await;
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            for (scope, wanted) in [
                (
                    WatchScope::Parts(vec![namespace("shop")]),
                    parts(&["edge", "shop"]),
                ),
                (WatchScope::Parts(vec![namespace("edge")]), parts(&["edge"])),
                (
                    WatchScope::Parts(vec![Scope::Cluster]),
                    BTreeSet::from([Scope::Cluster]),
                ),
            ] {
                let api = Rc::new(fake.client(&token("across", "secret"), Timing::default()));
                let (out, held) = watch::channel(None);
                let _watching = tokio::task::spawn_local(watch_parts(
                    api,
                    scope,
                    namespace("edge"),
                    out,
                    |_| {},
                ));
                assert!(
                    until(&held, |now| now.as_ref() == Some(&wanted)).await,
                    "{:?}",
                    *held.borrow()
                );
            }
        })
        .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_selectors_scope_is_its_namespaces_once_listed_and_its_own() {
    let fake = fake().await;
    let api = Rc::new(fake.client(&token("across", "secret"), Timing::default()));
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (out, held) = watch::channel(None);
            let _watching = tokio::task::spawn_local(watch_parts(
                api,
                WatchScope::Selector("team".to_owned()),
                namespace("edge"),
                out,
                |_| {},
            ));
            assert!(
                until(&held, |now| now.as_ref()
                    == Some(&parts(&["a", "b", "edge"])))
                .await,
                "{:?}",
                *held.borrow()
            );
        })
        .await;
}
