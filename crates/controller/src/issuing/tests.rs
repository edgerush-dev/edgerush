#![allow(
    clippy::unwrap_used,
    reason = "tests: an authority that cannot be made is a broken test"
)]

use super::*;
use edgerush_certificates::authority::{AUTHORITY_SECONDS, LEAF_SECONDS};
use edgerush_certificates::{Identity, spiffe_id as id_of};
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
use std::cell::RefCell;

fn issuer(kind: Kind) -> Issuer {
    Issuer {
        namespace: "edge".to_owned(),
        control_plane: "payments".to_owned(),
        cluster_domain: "cluster.local".to_owned(),
        authority: "edgerush-controller-payments-authority".to_owned(),
        kind,
    }
}

/// Now, a few seconds on: the test's own certificates are valid from the second they are
/// made, which may come after the clock was read.
fn now() -> i64 {
    unix_now() + 5
}

/// DataPlane `name` of ControlPlane `control_plane`, its Accepted condition `accepted` if
/// any, as of generation 1, at generation 2.
fn data_plane(name: &str, control_plane: &str, accepted: Option<&str>) -> DataPlane {
    let status = accepted.map_or(String::new(), |status| {
        format!(
            "status:\n  conditions:\n    - {{ type: Accepted, status: \"{status}\", reason: R, message: \"\", lastTransitionTime: \"2026-11-13T10:00:00Z\", observedGeneration: 1 }}\n"
        )
    });
    serde_saphyr::from_str(&format!(
        "metadata: {{ name: {name}, namespace: edge, generation: 2, uid: uid-{name} }}\nspec:\n  controlPlaneRef: {{ name: {control_plane} }}\n  ingress: {{ httpPort: 80, httpsPort: 443 }}\n{status}"
    ))
    .unwrap()
}

#[test]
fn each_workload_is_known_by_its_service_accounts_id_in_its_control_planes_trust_domain() {
    let issuer = issuer(Kind::Made);
    let own = issuer.own_leaf();
    assert_eq!(own.common_name, "edgerush-controller-payments");
    assert_eq!(
        own.dns,
        [
            "edgerush-controller-payments.edge.svc",
            "edgerush-controller-payments.edge.svc.cluster.local"
        ]
    );
    assert_eq!(
        own.spiffe_id,
        "spiffe://payments.cluster.local/ns/edge/sa/edgerush-controller-payments"
    );
    let web = issuer.data_plane_leaf("web");
    assert!(web.dns.is_empty());
    assert_eq!(
        web.spiffe_id,
        "spiffe://payments.cluster.local/ns/edge/sa/edgerush-proxy-web"
    );
}

#[test]
fn a_secret_holds_what_is_written_as_cert_manager_writes_one() {
    let authority = Authority::make("payments", now()).unwrap();
    let held = authority.written();
    let object = secret("s", "edge", &held, None, Some("7".to_owned()));
    assert_eq!(object.type_.as_deref(), Some("kubernetes.io/tls"));
    assert_eq!(object.metadata.resource_version.as_deref(), Some("7"));
    let keys: Vec<&String> = object.data.as_ref().unwrap().keys().collect();
    assert_eq!(keys, ["ca.crt", "tls.crt", "tls.key"]);
    assert_eq!(written(&object), Some(held));
    let mut missing = object;
    missing.data.as_mut().unwrap().remove("ca.crt");
    assert_eq!(written(&missing), None);
}

#[test]
fn only_a_leader_makes_or_signs_again_and_a_provided_authority_is_only_read() {
    let now = now();
    let made = issuer(Kind::Made);
    assert!(matches!(step(&made, None, now, true), Step::Make(_)));
    let Step::Wait(why) = step(&made, None, now, false) else {
        panic!("a follower made it");
    };
    assert!(why.contains("which the leader makes"), "{why}");
    let Step::Wait(why) = step(&issuer(Kind::Provided), None, now, true) else {
        panic!("a provided authority made");
    };
    assert!(why.contains("the authority provided is not there"), "{why}");
    // There, it is read; due, the leader alone signs it again, as of its version.
    let fresh = Authority::make("payments", now).unwrap().written();
    let found = secret(&made.authority, "edge", &fresh, None, Some("3".to_owned()));
    assert!(matches!(
        step(&made, Some(&found), now, true),
        Step::Ready(_)
    ));
    let old = Authority::make("payments", now - AUTHORITY_SECONDS + 86_400)
        .unwrap()
        .written();
    let old = secret(&made.authority, "edge", &old, None, Some("4".to_owned()));
    let Step::Resign(again, version) = step(&made, Some(&old), now, true) else {
        panic!("not signed again");
    };
    assert_eq!(version.as_deref(), Some("4"));
    assert!(!again.due(now));
    assert!(matches!(
        step(&made, Some(&old), now, false),
        Step::Ready(_)
    ));
    // What cannot be read is said, and nothing written over it.
    let mut broken = found;
    broken.data.as_mut().unwrap().remove("tls.key");
    let Step::Wait(why) = step(&made, Some(&broken), now, true) else {
        panic!("an unreadable authority used");
    };
    assert!(why.contains("cannot be read"), "{why}");
}

#[test]
fn a_certificate_is_issued_again_at_half_its_life_or_once_its_bundle_changes() {
    let now = now();
    let authority = Authority::make("payments", now).unwrap();
    let leaf = issuer(Kind::Made).data_plane_leaf("web");
    assert!(reissue(None, &authority, now));
    let issued = authority.issue(&leaf, now).unwrap();
    assert!(!reissue(Some(&issued), &authority, now));
    assert!(reissue(Some(&issued), &authority, now + LEAF_SECONDS / 2));
    // Another authority's, or of another bundle, as once the authority is signed again.
    let other = Authority::make("payments", now).unwrap();
    assert!(reissue(Some(&issued), &other, now));
    let again = authority.resign(now).unwrap();
    assert!(reissue(Some(&issued), &again, now));
}

/// A provided authority replaced by one of a new key: the control plane's own certificate
/// of the one before is kept, trusting both, until the switch, and only then signed anew,
/// so that a workload still trusting the one before alone never refuses it.
#[test]
fn its_own_certificate_of_a_replaced_authority_is_kept_until_the_switch() {
    let now = now();
    let root = edgerush_certificates::testing::Authority::new("platform root");
    let provided = |name: &str| {
        let intermediate = root.issue_authority(name);
        Authority::read(
            &Written {
                certificate: intermediate.chain,
                key: intermediate.key,
                bundle: root.pem(),
            },
            Kind::Provided,
            now,
        )
        .unwrap()
    };
    let old = provided("payments");
    let leaf = issuer(Kind::Provided).own_leaf();
    let held = old.issue(&leaf, now).unwrap();
    assert_eq!(own_step(Some(&held), &old, now, None), Own::Keep);
    assert_eq!(own_step(None, &old, now, None), Own::Issue);
    let new = provided("payments").trusting(&[&held.bundle], now);
    let switch_at = Some(now + SWITCH_AFTER);
    let Own::Rebundle(kept) = own_step(Some(&held), &new, now, switch_at) else {
        panic!("not kept");
    };
    assert_eq!(
        (&kept.certificate, &kept.key),
        (&held.certificate, &held.key)
    );
    assert_eq!(kept.bundle, new.trusted_pem());
    assert_eq!(own_step(Some(&kept), &new, now + 60, switch_at), Own::Keep);
    assert_eq!(
        own_step(Some(&kept), &new, now + SWITCH_AFTER, switch_at),
        Own::Issue
    );
    // Of an authority it no longer trusts at all, signed anew at once.
    let unrelated = provided("payments");
    assert_eq!(
        own_step(Some(&held), &unrelated, now, switch_at),
        Own::Issue
    );
}

#[test]
fn the_data_planes_kept_are_its_own_control_planes_ever_accepted() {
    let planes = [
        data_plane("web", "payments", Some("True")),
        data_plane("api", "payments", None),
        data_plane("refused", "payments", Some("False")),
        data_plane("other", "shop", Some("True")),
    ];
    let names: Vec<&str> = served(&issuer(Kind::Made), &planes)
        .iter()
        .map(|data_plane| data_plane.metadata.name.as_deref().unwrap())
        .collect();
    // Accepted as of generation 1, at 2: still served, so still kept.
    assert_eq!(names, ["web"]);
}

/// A list of `items`, of `kind`.
fn page(kind: &str, items: &[serde_json::Value]) -> Answer {
    answer(
        200,
        &serde_json::json!({ "kind": format!("{kind}List"), "metadata": { "resourceVersion": "5" }, "items": items }).to_string(),
    )
}

fn open() -> Answer {
    Answer {
        status: 200,
        chunks: vec![bytes::Bytes::from_static(b"\n")],
        pause: Duration::from_secs(60),
    }
}

/// Runs [`issue`] against a fake that lists `secrets` and `planes` in `edge` and answers
/// every write 201, until `done` holds of what it said and what it was asked.
async fn run(
    name: &str,
    secrets: Vec<serde_json::Value>,
    planes: Vec<serde_json::Value>,
    leads: bool,
    done: impl Fn(&[String], &Fake, &Option<Written>) -> bool,
) -> (Fake, Vec<String>, Option<Written>) {
    run_as(name, Kind::Made, secrets, planes, leads, done).await
}

/// [`run`], of an authority of `kind`.
async fn run_as(
    name: &str,
    kind: Kind,
    secrets: Vec<serde_json::Value>,
    planes: Vec<serde_json::Value>,
    leads: bool,
    done: impl Fn(&[String], &Fake, &Option<Written>) -> bool,
) -> (Fake, Vec<String>, Option<Written>) {
    let fake = Fake::start_by_method(&["127.0.0.1"], move |method, path| {
        if path.contains("watch=true") {
            return open();
        }
        match (method, path) {
            ("GET", path) if path.starts_with("/api/v1/namespaces/edge/secrets?") => {
                page("Secret", &secrets)
            }
            ("GET", path)
                if path.starts_with(
                    "/apis/gateway.edgerush.dev/v1alpha1/namespaces/edge/dataplanes?",
                ) =>
            {
                page("DataPlane", &planes)
            }
            ("POST" | "PUT", _) => answer(201, "{}"),
            _ => answer(500, ""),
        }
    })
    .await;
    let api = fake.client(&token(name, "secret"), Timing::default());
    let lines: Rc<RefCell<Vec<String>>> = Rc::default();
    let (own, mut held) = watch::channel(None);
    let (_lead, leading) = watch::channel(leads);
    let local = tokio::task::LocalSet::new();
    let heard = Rc::clone(&lines);
    let owned = local
        .run_until(async {
            let issuing = tokio::task::spawn_local(issue(
                api,
                issuer(kind),
                Some(leading),
                own,
                move |line| heard.borrow_mut().push(line.to_owned()),
            ));
            tokio::time::timeout(Duration::from_secs(10), async {
                while !done(&lines.borrow(), &fake, &held.borrow_and_update().clone()) {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("never done: {:?}", lines.borrow()));
            issuing.abort();
            held.borrow().clone()
        })
        .await;
    let said = lines.borrow().clone();
    (fake, said, owned)
}

fn json<T: serde::Serialize>(value: &T) -> serde_json::Value {
    serde_json::to_value(value).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_leader_makes_the_authority_and_a_follower_waits_on_it() {
    let posted = |fake: &Fake| {
        fake.seen()
            .into_iter()
            .filter(|seen| seen.method == "POST")
            .collect::<Vec<_>>()
    };
    let (fake, _, own) = run(
        "issuing-make",
        Vec::new(),
        Vec::new(),
        true,
        |_, fake, _| !posted(fake).is_empty(),
    )
    .await;
    let posts = posted(&fake);
    assert_eq!(posts[0].path, "/api/v1/namespaces/edge/secrets");
    let made: Secret = serde_json::from_str(&posts[0].body).unwrap();
    assert_eq!(
        made.metadata.name.as_deref(),
        Some("edgerush-controller-payments-authority")
    );
    let held = written(&made).unwrap();
    assert!(Authority::read(&held, Kind::Made, now()).is_ok());
    // Nothing is signed with an authority not yet read back.
    assert_eq!(own, None);
    let (fake, said, own) = run(
        "issuing-follow",
        Vec::new(),
        Vec::new(),
        false,
        |said, _, _| {
            said.iter()
                .any(|line| line.contains("which the leader makes"))
        },
    )
    .await;
    assert!(posted(&fake).is_empty(), "a follower wrote: {said:?}");
    assert_eq!(own, None);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn its_own_certificate_is_signed_and_each_data_planes_written_by_the_leader() {
    let authority = Authority::make("payments", now()).unwrap();
    let stored = secret(
        "edgerush-controller-payments-authority",
        "edge",
        &authority.written(),
        None,
        Some("2".to_owned()),
    );
    let planes = vec![json(&serde_json::json!({
        "metadata": { "name": "web", "namespace": "edge", "generation": 1, "uid": "uid-web" },
        "spec": { "controlPlaneRef": { "name": "payments" }, "ingress": { "httpPort": 80, "httpsPort": 443 } },
        "status": { "conditions": [{ "type": "Accepted", "status": "True", "reason": "R", "message": "", "lastTransitionTime": "2026-11-13T10:00:00Z", "observedGeneration": 1 }] },
    }))];
    let wrote = |fake: &Fake| fake.seen().into_iter().any(|seen| seen.method == "POST");
    let (fake, said, own) = run(
        "issuing-identities",
        vec![json(&stored)],
        planes.clone(),
        true,
        |said, _, own| {
            own.is_some()
                && said
                    .iter()
                    .any(|line| line.contains("certificate of DataPlane edge/web written"))
        },
    )
    .await;
    // Its own: of its Service's names and its ID, trusted by the authority.
    let own = own.unwrap();
    assert!(authority.trusts(&own, now()));
    let identity = Identity::read(&edgerush_config::Certificate {
        chain: own.certificate.clone(),
        key: own.key.clone(),
    })
    .unwrap();
    assert!(identity.covers("edgerush-controller-payments.edge.svc"));
    // The DataPlane's, owned by it, of its own ID.
    let post = fake
        .seen()
        .into_iter()
        .find(|seen| seen.method == "POST")
        .unwrap();
    assert_eq!(post.path, "/api/v1/namespaces/edge/secrets");
    let written_secret: Secret = serde_json::from_str(&post.body).unwrap();
    assert_eq!(
        written_secret.metadata.name.as_deref(),
        Some("edgerush-proxy-web-identity")
    );
    let owners = written_secret.metadata.owner_references.clone().unwrap();
    assert_eq!(
        (owners[0].kind.as_str(), owners[0].uid.as_str()),
        ("DataPlane", "uid-web")
    );
    let issued = written(&written_secret).unwrap();
    assert!(authority.trusts(&issued, now()));
    let leaf = Identity::read(&edgerush_config::Certificate {
        chain: issued.certificate.clone(),
        key: issued.key.clone(),
    })
    .unwrap()
    .leaf;
    assert_eq!(
        id_of(&leaf).as_deref(),
        Some("spiffe://payments.cluster.local/ns/edge/sa/edgerush-proxy-web")
    );
    assert!(
        said.iter()
            .any(|line| line.contains("certificate of DataPlane edge/web written")),
        "{said:?}"
    );
    // A follower signs its own, and writes nothing.
    let (fake, said, own) = run(
        "issuing-identities-follower",
        vec![json(&stored)],
        planes,
        false,
        |_, _, own| own.is_some(),
    )
    .await;
    assert!(!wrote(&fake), "a follower wrote: {said:?}");
    assert!(own.is_some());
}

/// The platform's authority replaced by one of a new key, a DataPlane's Secret holding a
/// certificate of the one before: the one before is remembered from that Secret and trusted
/// beside the new, by the stream and in the DataPlane's certificate written again (05 §2).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaced_provided_authority_is_remembered_from_its_workloads_secrets() {
    let now = now();
    let root = edgerush_certificates::testing::Authority::new("platform root");
    let intermediate = |name: &str| {
        let made = root.issue_authority(name);
        Written {
            certificate: made.chain,
            key: made.key,
            bundle: root.pem(),
        }
    };
    let old = Authority::read(&intermediate("payments"), Kind::Provided, now).unwrap();
    let held = old
        .issue(&issuer(Kind::Provided).data_plane_leaf("web"), now)
        .unwrap();
    let replaced = intermediate("payments");
    let secrets = vec![
        json(&secret(
            "edgerush-controller-payments-authority",
            "edge",
            &replaced,
            None,
            Some("9".to_owned()),
        )),
        json(&secret(
            "edgerush-proxy-web-identity",
            "edge",
            &held,
            None,
            Some("3".to_owned()),
        )),
    ];
    let planes = vec![json(&serde_json::json!({
        "metadata": { "name": "web", "namespace": "edge", "generation": 1, "uid": "uid-web" },
        "spec": { "controlPlaneRef": { "name": "payments" }, "ingress": { "httpPort": 80, "httpsPort": 443 } },
        "status": { "conditions": [{ "type": "Accepted", "status": "True", "reason": "R", "message": "", "lastTransitionTime": "2026-11-13T10:00:00Z", "observedGeneration": 1 }] },
    }))];
    let (fake, said, own) = run_as(
        "issuing-rotation",
        Kind::Provided,
        secrets,
        planes,
        true,
        |said, _, own| {
            own.is_some()
                && said
                    .iter()
                    .any(|line| line.contains("certificate of DataPlane edge/web written"))
        },
    )
    .await;
    let new = Authority::read(&replaced, Kind::Provided, now).unwrap();
    // The stream trusts both: the DataPlane's certificate of the one before is let in.
    let own = own.unwrap();
    let both = Authority::read(&replaced, Kind::Provided, now)
        .unwrap()
        .trusting(&[&held.bundle], now)
        .trusted_pem();
    assert_eq!(own.bundle, both, "{said:?}");
    // Its certificate, written again of the new, trusting both until it is mounted.
    let put = fake
        .seen()
        .into_iter()
        .find(|seen| seen.method == "PUT")
        .unwrap();
    let rewritten: Secret = serde_json::from_str(&put.body).unwrap();
    let rewritten = written(&rewritten).unwrap();
    assert_eq!(rewritten.bundle, both);
    assert!(new.signed(&rewritten, now));
}
