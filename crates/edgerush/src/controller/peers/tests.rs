use super::*;
use edgerush_certificates::testing::Authority;
use edgerush_control_stream::{CONFIG_VERSION, ConfigVersion};
use std::cell::RefCell;

/// The name every replica's certificate carries, its Service's.
const NAME: &str = "edgerush-controller-edge.edge.svc";
/// The control plane's own identity.
const OWN: &str = "spiffe://edge.cluster.local/ns/edge/sa/edgerush-controller-edge";

fn replica(pod: &str, address: &str, ready: bool) -> Replica {
    Replica {
        pod: pod.to_owned(),
        address: address.parse().unwrap(),
        ready,
    }
}

#[test]
fn a_holder_is_its_pod_and_when_it_started() {
    assert_eq!(
        pod_of("edgerush-controller-edge-6d9f_1795166354199"),
        "edgerush-controller-edge-6d9f"
    );
    assert_eq!(pod_of("alone"), "alone");
}

/// The leader is the pod the Lease names, found among the replicas, ready or not: none
/// while this one leads, nor while the Lease names none, nor one no endpoint is of.
#[test]
fn the_leader_is_the_pod_the_lease_names_among_the_replicas() {
    let replicas = [
        replica("controller-a", "10.0.0.2", true),
        replica("controller-b", "10.0.0.3", false),
    ];
    assert_eq!(
        leader_at(false, Some("controller-b_17"), &replicas, 9444),
        Some("10.0.0.3:9444".parse().unwrap())
    );
    assert_eq!(
        leader_at(true, Some("controller-b_17"), &replicas, 9444),
        None
    );
    assert_eq!(leader_at(false, None, &replicas, 9444), None);
    assert_eq!(
        leader_at(false, Some("controller-c_17"), &replicas, 9444),
        None
    );
    assert_eq!(
        leader_at(
            false,
            Some("controller-a_17"),
            &[replica("controller-a", "fd00::2", true)],
            9444
        ),
        Some("[fd00::2]:9444".parse().unwrap())
    );
}

/// A snapshot read is taken in; one of a config version this replica does not read is
/// said, and taken as none of the leader's, so that it translates afresh; what is no
/// snapshot is said, and the one held kept.
#[test]
fn a_snapshot_the_leader_sends_is_taken_in_or_said() {
    let (snapshot, held) = watch::channel(None);
    let said = RefCell::new(Vec::new());
    let say = |line: &str| said.borrow_mut().push(line.to_owned());
    let this = ConfigVersion::served(CONFIG_VERSION).unwrap();
    let sent = Snapshot {
        count: 3,
        ..Snapshot::default()
    };
    took(&sent.encode(this).unwrap(), &snapshot, &say);
    assert_eq!(held.borrow().as_deref(), Some(&sent));
    took(b"not a snapshot", &snapshot, &say);
    assert_eq!(
        held.borrow().as_deref(),
        Some(&sent),
        "the one held let go of"
    );
    assert!(said.borrow()[0].starts_with("the leader's snapshot is not taken in: "));
    let later = format!(
        r#"{{"config_version":{},"count":4,"planes":{{}}}}"#,
        CONFIG_VERSION + 1
    );
    took(later.as_bytes(), &snapshot, &say);
    assert_eq!(held.borrow().as_deref(), Some(&Snapshot::default()));
    assert!(
        said.borrow()[1]
            .ends_with("; this replica translates afresh, as a control plane started clean does"),
        "{:?}",
        said.borrow()
    );
}

/// A follower follows the leader the Lease names, found among the replicas, over the
/// listener it serves its snapshot on: the latest as it dials, each later one as it is
/// taken; and lets it go once it leads itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_follower_takes_each_snapshot_of_the_leader_the_lease_names() {
    let authority = Authority::new("edge");
    let own = Credentials {
        certificate: authority.issue(&[NAME, OWN]),
        authorities: authority.pem(),
    };
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let listener = TcpListener::from_std(listener).unwrap();
    let said: Rc<RefCell<Vec<String>>> = Rc::default();
    let say: Rc<dyn Fn(&str)> = {
        let said = Rc::clone(&said);
        Rc::new(move |line: &str| said.borrow_mut().push(line.to_owned()))
    };
    let first = Snapshot {
        count: 1,
        ..Snapshot::default()
    };
    let (leaders, leaders_held) = watch::channel(Some(Arc::new(first.clone())));
    let (signed_out, signed) = watch::channel(Some(own.clone()));
    let (followers, mut followed) = watch::channel(None::<Arc<Snapshot>>);
    let (leads, leading) = watch::channel(false);
    let (_holding, holder) = watch::channel(Some("leader-pod_17".to_owned()));
    let (_replicas_out, replicas) =
        watch::channel::<Arc<[Replica]>>(Arc::from([replica("leader-pod", "127.0.0.1", true)]));
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let serving = tokio::task::spawn_local(serve_snapshot(
                listener,
                signed.clone(),
                OWN.to_owned(),
                leaders_held,
                Rc::clone(&say),
                async move {
                    let _ = stopped.await;
                },
            ));
            let following = tokio::task::spawn_local(follow_leader(
                Peers {
                    id: OWN.to_owned(),
                    name: NAME.to_owned(),
                    port,
                },
                leading,
                holder,
                replicas,
                signed,
                followers,
                Rc::clone(&say),
            ));
            let next = |followed: &mut watch::Receiver<Option<Arc<Snapshot>>>| {
                let mut followed = followed.clone();
                async move {
                    tokio::time::timeout(Duration::from_secs(5), followed.changed())
                        .await
                        .expect("nothing followed")
                        .unwrap();
                    followed.borrow_and_update().clone()
                }
            };
            assert_eq!(next(&mut followed).await.as_deref(), Some(&first));
            followed.borrow_and_update();
            let second = Snapshot {
                count: 2,
                ..Snapshot::default()
            };
            leaders.send_replace(Some(Arc::new(second.clone())));
            assert_eq!(next(&mut followed).await.as_deref(), Some(&second));
            followed.borrow_and_update();
            // It leads itself: the leader it followed is let go of.
            leads.send_replace(true);
            let gone = || {
                said.borrow()
                    .iter()
                    .any(|line| line.contains("no longer follows: the follower is gone"))
            };
            tokio::time::timeout(Duration::from_secs(5), async {
                while !gone() {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("the leader not let go of: {:?}", said.borrow()));
            following.abort();
            let _ = stop.send(());
            tokio::time::timeout(Duration::from_secs(5), serving)
                .await
                .expect("the listener did not stop")
                .unwrap()
                .unwrap();
            drop(signed_out);
        })
        .await;
}

/// A leader that refuses this replica, their config versions sharing none, is not dialled
/// again: this replica starts clean, loudly, where it holds no snapshot yet; any other end
/// is dialled again.
#[test]
fn a_replica_the_leader_refuses_starts_clean() {
    let (snapshot, held) = watch::channel(None);
    let said = RefCell::new(Vec::new());
    let say = |line: &str| said.borrow_mut().push(line.to_owned());
    let refused = Unfollowed::Refused("config version 9 is no longer written".to_owned());
    assert!(!ended_with(&refused, "10.0.0.3:9444", &snapshot, &say));
    assert_eq!(held.borrow().as_deref(), Some(&Snapshot::default()));
    assert_eq!(
        said.borrow()[0],
        "the leader at 10.0.0.3:9444 refused this replica: config version 9 is no longer \
         written; this replica translates afresh, as a control plane started clean does"
    );
    let holding = Snapshot {
        count: 5,
        ..Snapshot::default()
    };
    snapshot.send_replace(Some(Arc::new(holding.clone())));
    assert!(!ended_with(&refused, "10.0.0.3:9444", &snapshot, &say));
    assert_eq!(
        held.borrow().as_deref(),
        Some(&holding),
        "the one held let go of"
    );
    assert!(ended_with(
        &Unfollowed::Closed,
        "10.0.0.3:9444",
        &snapshot,
        &say
    ));
    assert!(ended_with(
        &Unfollowed::Unreachable("refused".to_owned()),
        "10.0.0.3:9444",
        &snapshot,
        &say
    ));
    assert_eq!(held.borrow().as_deref(), Some(&holding));
}

/// A replica that holds no snapshot once the time is up starts clean, loudly; one that
/// holds one keeps it, and nothing is said.
#[tokio::test]
async fn a_replica_holding_none_in_time_starts_clean() {
    let said: Rc<RefCell<Vec<String>>> = Rc::default();
    let say: Rc<dyn Fn(&str)> = {
        let said = Rc::clone(&said);
        Rc::new(move |line: &str| said.borrow_mut().push(line.to_owned()))
    };
    let (snapshot, held) = watch::channel(None);
    clean_after(Duration::from_millis(10), snapshot, Rc::clone(&say)).await;
    assert_eq!(held.borrow().as_deref(), Some(&Snapshot::default()));
    assert_eq!(
        said.borrow().as_slice(),
        [
            "no snapshot of the leader's held after 0 s: this replica translates afresh, as a \
          control plane started clean does"
        ]
    );
    let holding = Snapshot {
        count: 5,
        ..Snapshot::default()
    };
    let (snapshot, held) = watch::channel(Some(Arc::new(holding.clone())));
    clean_after(Duration::from_millis(10), snapshot, say).await;
    assert_eq!(held.borrow().as_deref(), Some(&holding));
    assert_eq!(said.borrow().len(), 1);
}

/// A replica serving `snapshot` on a port of its own, until the oneshot is sent on.
fn serving(
    own: &Credentials,
    snapshot: Snapshot,
    say: &Rc<dyn Fn(&str)>,
) -> (u16, tokio::sync::oneshot::Sender<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    let listener = TcpListener::from_std(listener).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let (signed_out, signed) = watch::channel(Some(own.clone()));
    let (served, held) = watch::channel(Some(Arc::new(snapshot)));
    let say = Rc::clone(say);
    tokio::task::spawn_local(async move {
        let _kept = (signed_out, served);
        serve_snapshot(listener, signed, OWN.to_owned(), held, say, async move {
            let _ = stopped.await;
        })
        .await
    });
    (port, stop)
}

/// The replica taking the lease leads only once it has taken the newest snapshot there is,
/// of the highest count among its own and the ready replicas': another's newer, or its
/// own; with none, a clean start, loudly. A replica not ready is not asked, nor is its own
/// pod. No longer elected, it no longer leads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_replica_taking_the_lease_leads_from_the_newest_snapshot() {
    let authority = Authority::new("edge");
    let own = Credentials {
        certificate: authority.issue(&[NAME, OWN]),
        authorities: authority.pem(),
    };
    let said: Rc<RefCell<Vec<String>>> = Rc::default();
    let say: Rc<dyn Fn(&str)> = {
        let said = Rc::clone(&said);
        Rc::new(move |line: &str| said.borrow_mut().push(line.to_owned()))
    };
    let counted = |count| Snapshot {
        count,
        ..Snapshot::default()
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (port, _stop) = serving(&own, counted(7), &say);
            let (elect, elected) = watch::channel(false);
            let (replicas_out, replicas) = watch::channel::<Arc<[Replica]>>(Arc::from([
                // Its own pod, where nothing answers: never asked.
                replica("me", "127.0.0.2", true),
                replica("other", "127.0.0.1", true),
            ]));
            let (_signed_out, signed) = watch::channel(Some(own.clone()));
            let (snapshot, held) = watch::channel(Some(Arc::new(counted(5))));
            let (leads, leading) = watch::channel(false);
            let _taking = tokio::task::spawn_local(take_over(
                Peers {
                    id: OWN.to_owned(),
                    name: NAME.to_owned(),
                    port,
                },
                "me".to_owned(),
                Duration::from_secs(5),
                elected,
                replicas,
                signed,
                snapshot.clone(),
                leads,
                Rc::clone(&say),
            ));
            let led = |want: bool| {
                let mut leading = leading.clone();
                async move {
                    tokio::time::timeout(
                        Duration::from_secs(10),
                        leading.wait_for(|now| *now == want),
                    )
                    .await
                    .is_ok()
                }
            };
            tokio::time::sleep(Duration::from_millis(100)).await;
            assert!(!*leading.borrow(), "led unelected");
            // Another's newer: taken.
            elect.send_replace(true);
            assert!(led(true).await, "{:?}", said.borrow());
            assert_eq!(held.borrow().as_deref(), Some(&counted(7)));
            let from =
                format!("leading from the snapshot of the replica at 127.0.0.1:{port}, of count 7");
            assert!(said.borrow().contains(&from), "{:?}", said.borrow());
            assert!(
                !said.borrow().iter().any(|line| line.contains("127.0.0.2")),
                "its own pod asked: {:?}",
                said.borrow()
            );
            elect.send_replace(false);
            assert!(led(false).await);
            // Its own newer: kept.
            snapshot.send_replace(Some(Arc::new(counted(9))));
            elect.send_replace(true);
            assert!(led(true).await);
            assert_eq!(held.borrow().as_deref(), Some(&counted(9)));
            let own_kept = "leading from this replica's snapshot, of count 9".to_owned();
            assert!(said.borrow().contains(&own_kept), "{:?}", said.borrow());
            elect.send_replace(false);
            assert!(led(false).await);
            // None of its own, and the other not ready: a clean start.
            snapshot.send_replace(None);
            replicas_out.send_replace(Arc::from([replica("other", "127.0.0.1", false)]));
            elect.send_replace(true);
            assert!(led(true).await);
            assert_eq!(held.borrow().as_deref(), Some(&Snapshot::default()));
            let clean = "no ready replica holds a snapshot: leading from a clean start".to_owned();
            assert!(said.borrow().contains(&clean), "{:?}", said.borrow());
        })
        .await;
}

/// A ready replica that gives no snapshot in time holds up the one taking the lease no
/// longer than it waits on each.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replica_that_gives_nothing_holds_up_the_lease_taker_for_its_wait_alone() {
    let authority = Authority::new("edge");
    let own = Credentials {
        certificate: authority.issue(&[NAME, OWN]),
        authorities: authority.pem(),
    };
    let said: Rc<RefCell<Vec<String>>> = Rc::default();
    let say: Rc<dyn Fn(&str)> = {
        let said = Rc::clone(&said);
        Rc::new(move |line: &str| said.borrow_mut().push(line.to_owned()))
    };
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // It serves, but holds no snapshot yet: it gives none.
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let port = listener.local_addr().unwrap().port();
            let listener = TcpListener::from_std(listener).unwrap();
            let (_signed_out, signed) = watch::channel(Some(own.clone()));
            let (_none, nothing) = watch::channel(None::<Arc<Snapshot>>);
            tokio::task::spawn_local(serve_snapshot(
                listener,
                signed.clone(),
                OWN.to_owned(),
                nothing,
                Rc::clone(&say),
                std::future::pending(),
            ));
            let (_elect, elected) = watch::channel(true);
            let (_replicas_out, replicas) =
                watch::channel::<Arc<[Replica]>>(Arc::from([replica("quiet", "127.0.0.1", true)]));
            let (snapshot, held) = watch::channel(None);
            let (leads, mut leading) = watch::channel(false);
            let started = tokio::time::Instant::now();
            let _taking = tokio::task::spawn_local(take_over(
                Peers {
                    id: OWN.to_owned(),
                    name: NAME.to_owned(),
                    port,
                },
                "me".to_owned(),
                Duration::from_millis(300),
                elected,
                replicas,
                signed,
                snapshot,
                leads,
                Rc::clone(&say),
            ));
            tokio::time::timeout(Duration::from_secs(5), leading.wait_for(|now| *now))
                .await
                .expect("never led")
                .unwrap();
            assert!(
                started.elapsed() < Duration::from_secs(3),
                "{:?}",
                started.elapsed()
            );
            assert_eq!(held.borrow().as_deref(), Some(&Snapshot::default()));
            let quiet = format!("the replica at 127.0.0.1:{port} gave no snapshot within 0 s");
            assert!(said.borrow().contains(&quiet), "{:?}", said.borrow());
        })
        .await;
}
