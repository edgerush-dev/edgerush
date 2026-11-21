use super::*;
use crate::{Credentials, acceptor_of, connector_to};
use edgerush_certificates::testing::Authority;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The name the replicas' certificates carry, as the controller's Service names it.
const NAME: &str = "controller.test";
/// The control plane's own identity.
const OWN: &str = "spiffe://edge.test/ns/edge/sa/edgerush-controller-edge";
/// A data plane's, which its authority vouches for too.
const PLANE: &str = "spiffe://edge.test/ns/edge/sa/edgerush-proxy-web";

fn credentials(authority: &Authority, id: &str) -> Credentials {
    Credentials {
        certificate: authority.issue(&[NAME, id]),
        authorities: authority.pem(),
    }
}

/// A replica serving its snapshots on a port of its own, and what it hears.
struct Replica {
    address: String,
    heard: mpsc::UnboundedReceiver<Followed>,
    /// The config versions each snapshot was written at.
    written: mpsc::UnboundedReceiver<u32>,
    snapshots: watch::Sender<Option<Arc<Vec<u8>>>>,
    stop: Option<oneshot::Sender<()>>,
    served: JoinHandle<Result<(), ServeError>>,
}

impl Replica {
    /// One presenting `credentials`, taking the control plane's own identity alone, holding
    /// `snapshot`, if any.
    fn start(credentials: &Credentials, snapshot: Option<&[u8]>) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(listener).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (_tls, accepting) = watch::channel(Arc::new(acceptor_of(credentials, OWN).unwrap()));
        let (snapshots, taken) = watch::channel(snapshot.map(|bytes| Arc::new(bytes.to_vec())));
        let (said, heard) = mpsc::unbounded_channel();
        let (wrote, written) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let served = tokio::spawn(serve_peers(
            listener,
            accepting,
            taken,
            move |snapshot: &Vec<u8>, version: ConfigVersion| {
                let _ = wrote.send(version.get());
                Ok(snapshot.clone())
            },
            move |_peer: SocketAddr, heard: Followed| {
                let _ = said.send(heard);
            },
            async move {
                let _ = stopped.await;
            },
        ));
        Self {
            address,
            heard,
            written,
            snapshots,
            stop: Some(stop),
            served,
        }
    }

    /// The next thing heard, within a few seconds.
    async fn next(&mut self) -> Followed {
        tokio::time::timeout(Duration::from_secs(5), self.heard.recv())
            .await
            .expect("nothing heard")
            .expect("the server is gone")
    }

    /// Takes `snapshot`, in place of what it held.
    fn take(&self, snapshot: &[u8]) {
        self.snapshots
            .send_replace(Some(Arc::new(snapshot.to_vec())));
    }

    /// Tells it to stop, and waits for it to have.
    async fn stop(mut self) {
        let _ = self.stop.take().unwrap().send(());
        tokio::time::timeout(Duration::from_secs(5), self.served)
            .await
            .expect("the server did not stop")
            .unwrap()
            .unwrap();
    }
}

/// What a follower of this build asks with.
fn this_build() -> Follow {
    Follow {
        config_version: CONFIG_VERSION,
        oldest_config_version: OLDEST_CONFIG_VERSION,
    }
}

/// A replica following the one at `address`, asking with `follow`, each snapshot it takes
/// sent on.
fn follower(
    address: &str,
    credentials: &Credentials,
    follow: Follow,
) -> (mpsc::UnboundedReceiver<Vec<u8>>, JoinHandle<Unfollowed>) {
    let tls = connector_to(credentials, OWN).unwrap();
    let (took, taken) = mpsc::unbounded_channel();
    let address = address.to_owned();
    let following = tokio::spawn(async move {
        follow_with(&address, NAME, &tls, follow, |snapshot| {
            let _ = took.send(snapshot);
        })
        .await
    });
    (taken, following)
}

async fn taken(taken: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Vec<u8> {
    tokio::time::timeout(Duration::from_secs(5), taken.recv())
        .await
        .expect("nothing taken")
        .unwrap()
}

async fn ended(following: JoinHandle<Unfollowed>) -> Unfollowed {
    tokio::time::timeout(Duration::from_secs(5), following)
        .await
        .expect("the stream did not end")
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_follower_takes_the_latest_snapshot_and_each_later_one() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, Some(b"first"));
    let (mut took, following) = follower(&replica.address, &own, this_build());
    assert_eq!(taken(&mut took).await, b"first");
    assert_eq!(
        replica.next().await,
        Followed::Asked {
            version: CONFIG_VERSION,
            written: CONFIG_VERSION,
        }
    );
    replica.take(b"second");
    assert_eq!(taken(&mut took).await, b"second");
    following.abort();
}

#[tokio::test(flavor = "multi_thread")]
async fn nothing_is_sent_before_a_snapshot_is_taken() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, None);
    let (mut took, following) = follower(&replica.address, &own, this_build());
    assert!(matches!(replica.next().await, Followed::Asked { .. }));
    let early = tokio::time::timeout(Duration::from_millis(300), took.recv()).await;
    assert!(early.is_err(), "{early:?} taken before any snapshot");
    replica.take(b"first");
    assert_eq!(taken(&mut took).await, b"first");
    following.abort();
}

/// A snapshot crosses whole whatever its size: in one part, or in as many as it takes,
/// each within the message limit.
#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_of_any_size_crosses_whole() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let replica = Replica::start(&own, None);
    let (mut took, following) = follower(&replica.address, &own, this_build());
    for size in [0, 1, PART, PART + 1, 3 * PART + 5] {
        let snapshot: Vec<u8> = (0..size).map(|at| (at % 251) as u8).collect();
        replica.take(&snapshot);
        let crossed = taken(&mut took).await;
        assert!(
            crossed == snapshot,
            "{size} bytes crossed as {}",
            crossed.len()
        );
    }
    following.abort();
}

#[test]
fn a_snapshot_is_cut_into_parts_its_last_marked() {
    assert_eq!(
        parts(Vec::new()),
        [Part {
            bytes: Vec::new(),
            last: true,
        }]
    );
    for (size, count) in [(1, 1), (PART, 1), (PART + 1, 2), (3 * PART + 5, 4)] {
        let snapshot: Vec<u8> = (0..size).map(|at| (at % 251) as u8).collect();
        let parts = parts(snapshot.clone());
        assert_eq!(parts.len(), count, "{size} bytes");
        assert!(parts.iter().all(|part| part.bytes.len() <= PART));
        let lasts: Vec<bool> = parts.iter().map(|part| part.last).collect();
        let mut marked = vec![false; count];
        marked[count - 1] = true;
        assert_eq!(lasts, marked, "{size} bytes");
        let whole: Vec<u8> = parts.into_iter().flat_map(|part| part.bytes).collect();
        assert!(whole == snapshot, "{size} bytes");
    }
}

/// As a config for a data plane: written at the older of the follower's version and the
/// replica's.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_is_written_at_the_older_version() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, Some(b"first"));
    let newer = Follow {
        config_version: CONFIG_VERSION + 1,
        oldest_config_version: OLDEST_CONFIG_VERSION,
    };
    let (mut took, following) = follower(&replica.address, &own, newer);
    assert_eq!(taken(&mut took).await, b"first");
    assert_eq!(
        replica.next().await,
        Followed::Asked {
            version: CONFIG_VERSION + 1,
            written: CONFIG_VERSION,
        }
    );
    assert_eq!(replica.written.recv().await, Some(CONFIG_VERSION));
    following.abort();
}

/// A follower older than the replica still writes for, or one that reads none it writes,
/// is refused, and told why; nothing is written for it.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_outside_the_window_is_refused() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, Some(b"first"));
    let older = Follow {
        config_version: OLDEST_CONFIG_VERSION - 1,
        oldest_config_version: OLDEST_CONFIG_VERSION - 1,
    };
    let newer = Follow {
        config_version: CONFIG_VERSION + 2,
        oldest_config_version: CONFIG_VERSION + 1,
    };
    for (follow, why) in [
        (
            older,
            format!(
                "config version {} is no longer written: this replica writes \
                 {OLDEST_CONFIG_VERSION} to {CONFIG_VERSION}",
                OLDEST_CONFIG_VERSION - 1
            ),
        ),
        (
            newer,
            format!(
                "the follower reads config version {} and after, and this replica writes \
                 {CONFIG_VERSION} at most",
                CONFIG_VERSION + 1
            ),
        ),
    ] {
        let (_took, following) = follower(&replica.address, &own, follow);
        assert_eq!(ended(following).await, Unfollowed::Refused(why.clone()));
        assert_eq!(replica.next().await, Followed::Unserved(why));
    }
    assert!(replica.written.try_recv().is_err());
}

/// The authority vouches for a data plane too: its certificate is refused all the same,
/// in the handshake, as no data plane reads a replica's snapshots.
#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_is_refused() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, Some(b"first"));
    let plane = credentials(&authority, PLANE);
    let (_took, following) = follower(&replica.address, &plane, this_build());
    let unfollowed = ended(following).await;
    assert!(
        matches!(unfollowed, Unfollowed::Unreachable(_)),
        "{unfollowed:?}"
    );
    assert!(matches!(replica.next().await, Followed::Refused(_)));
    assert!(replica.written.try_recv().is_err());
}

/// Nor does a replica follow what answers with another identity than the control plane's.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_of_another_identity_is_not_followed() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let plane = credentials(&authority, PLANE);
    let replica = Replica::start(&plane, Some(b"first"));
    let (mut took, following) = follower(&replica.address, &own, this_build());
    assert!(matches!(ended(following).await, Unfollowed::Unreachable(_)));
    assert!(took.try_recv().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn stopping_ends_each_follower() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, Some(b"first"));
    let (mut took, following) = follower(&replica.address, &own, this_build());
    assert_eq!(taken(&mut took).await, b"first");
    assert!(matches!(replica.next().await, Followed::Asked { .. }));
    let _ = replica.stop.take().unwrap().send(());
    let stopping = "the replica is stopping".to_owned();
    assert_eq!(ended(following).await, Unfollowed::Ended(stopping.clone()));
    assert_eq!(replica.next().await, Followed::Closed(stopping));
    tokio::time::timeout(Duration::from_secs(5), replica.served)
        .await
        .expect("the server did not stop")
        .unwrap()
        .unwrap();
}

/// A follower gone is let go of while no snapshot changes, not only when one is next sent.
#[tokio::test(flavor = "multi_thread")]
async fn a_follower_gone_is_let_go() {
    let authority = Authority::new("edge");
    let own = credentials(&authority, OWN);
    let mut replica = Replica::start(&own, Some(b"first"));
    let (mut took, following) = follower(&replica.address, &own, this_build());
    assert_eq!(taken(&mut took).await, b"first");
    assert!(matches!(replica.next().await, Followed::Asked { .. }));
    following.abort();
    assert_eq!(
        replica.next().await,
        Followed::Closed("the follower is gone".to_owned())
    );
    replica.stop().await;
}
