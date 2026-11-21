//! The controller's replicas among themselves, as the command runs them
//! ([04 §2](../../../../docs/04-control-plane.md)): every replica serves the snapshot it
//! holds on `--peers`, with its own certificate, taking the control plane's own identity
//! alone; one that follows finds the leader, the pod the Lease names among its Service's
//! endpoints, and follows the leader's, each taken into the snapshot it translates from.

use edgerush_certificates::authority::Written;
use edgerush_control_service::{
    Credentials, Followed, ServeError, SslConnector, Unfollowed, acceptor_of, connector_to,
    follow_peer, serve_peers,
};
use edgerush_controller::own_pods::Replica;
use edgerush_controller::snapshot::{Snapshot, SnapshotError};
use std::future::Future;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};

/// How long a follower waits before dialling the leader again, once its stream ended.
const AGAIN: Duration = Duration::from_secs(1);

/// What the replicas know each other by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Peers {
    /// The control plane's own SPIFFE ID, which alone the listener takes, and the leader
    /// is to present.
    pub(super) id: String,
    /// The name of the control plane's Service, which every replica's certificate carries.
    pub(super) name: String,
    /// The port each replica serves its snapshot on.
    pub(super) port: u16,
}

/// The pod a Lease's holder is: `<pod>_<start>`, as the command names a replica.
pub(super) fn pod_of(holder: &str) -> &str {
    holder.split_once('_').map_or(holder, |(pod, _)| pod)
}

/// Where the leader serves its snapshot, for a replica that follows: the endpoint among
/// `replicas` of the pod the Lease's `holder` is, at `port`; none while it `leads`, or
/// until both say it.
pub(super) fn leader_at(
    leads: bool,
    holder: Option<&str>,
    replicas: &[Replica],
    port: u16,
) -> Option<SocketAddr> {
    if leads {
        return None;
    }
    let pod = pod_of(holder?);
    replicas
        .iter()
        .find(|replica| replica.pod == pod)
        .map(|replica| SocketAddr::new(replica.address, port))
}

/// Serves `snapshot` on `listener` until `stop`, with each certificate the issuer signs on
/// `signed`, from the first: nothing is served until it signs one, and each after it takes
/// the next connection. Only the control plane's own `id` is taken. What is heard is said
/// to `say`.
pub(super) async fn serve_snapshot(
    listener: TcpListener,
    mut signed: watch::Receiver<Option<Credentials>>,
    id: String,
    snapshot: watch::Receiver<Option<Arc<Snapshot>>>,
    say: Rc<dyn Fn(&str)>,
    stop: impl Future<Output = ()> + Send,
) -> Result<(), ServeError> {
    let tls = loop {
        let first = signed.borrow_and_update().clone();
        if let Some(first) = first {
            match acceptor_of(&first, &id) {
                Ok(first) => break watch::Sender::new(Arc::new(first)),
                Err(error) => say(&format!(
                    "the replicas' listener's certificate cannot be used: {error}"
                )),
            }
        }
        if signed.changed().await.is_err() {
            return Ok(());
        }
    };
    let accepting = tls.subscribe();
    let telling = Rc::clone(&say);
    tokio::task::spawn_local(async move {
        while signed.changed().await.is_ok() {
            let Some(next) = signed.borrow_and_update().clone() else {
                continue;
            };
            match acceptor_of(&next, &id) {
                Ok(next) => {
                    tls.send_replace(Arc::new(next));
                }
                Err(error) => telling(&format!(
                    "the replicas' listener's certificate cannot be used: {error}"
                )),
            }
        }
    });
    // Heard on the server's own tasks, and said here, where `say` lives.
    let (heard, mut hearing) = mpsc::unbounded_channel::<String>();
    tokio::task::spawn_local(async move {
        while let Some(line) = hearing.recv().await {
            say(&line);
        }
    });
    serve_peers(
        listener,
        accepting,
        snapshot,
        |snapshot: &Snapshot, version| snapshot.encode(version).map_err(|error| error.to_string()),
        move |peer, followed| {
            let _gone = heard.send(said_of(peer, &followed));
        },
        stop,
    )
    .await
}

/// What is said of what the listener heard of a replica.
fn said_of(peer: SocketAddr, followed: &Followed) -> String {
    match followed {
        Followed::Refused(why) => format!("replica {peer} refused: {why}"),
        Followed::Asked { version, written } => format!(
            "replica {peer} follows this one's snapshot, at config version {written} (it reads {version})"
        ),
        Followed::Unserved(why) => format!("replica {peer} not served: {why}"),
        Followed::Closed(why) => format!("replica {peer} no longer follows: {why}"),
    }
}

/// Follows the leader's snapshots for as long as this replica does not lead, each taken
/// into `snapshot`: the leader found as [`leader_at`] says from `leading`, `holder` and
/// `replicas`, dialled with the latest of `signed`, its certificate to carry the control
/// plane's own ID and its Service's name, and dialled again a moment after each stream ends
/// and whenever whom to dial changes. One of a config version this replica does not read
/// is said, and the snapshot taken as none of the leader's, so that it translates afresh,
/// as a control plane started clean does. Ends once any of them is gone.
pub(super) async fn follow_leader(
    peers: Peers,
    mut leading: watch::Receiver<bool>,
    mut holder: watch::Receiver<Option<String>>,
    mut replicas: watch::Receiver<Arc<[Replica]>>,
    mut signed: watch::Receiver<Option<Credentials>>,
    snapshot: watch::Sender<Option<Arc<Snapshot>>>,
    say: Rc<dyn Fn(&str)>,
) {
    let at = |leading: &watch::Receiver<bool>,
              holder: &watch::Receiver<Option<String>>,
              replicas: &watch::Receiver<Arc<[Replica]>>| {
        leader_at(
            *leading.borrow(),
            holder.borrow().as_deref(),
            &replicas.borrow(),
            peers.port,
        )
    };
    loop {
        let target = at(&leading, &holder, &replicas);
        let credentials = signed.borrow_and_update().clone();
        let (Some(address), Some(credentials)) = (target, credentials) else {
            if !changed(&mut leading, &mut holder, &mut replicas, &mut signed).await {
                return;
            }
            continue;
        };
        let tls = match connector_to(&credentials, &peers.id) {
            Ok(tls) => tls,
            Err(error) => {
                say(&format!("the leader cannot be dialled: {error}"));
                if !changed(&mut leading, &mut holder, &mut replicas, &mut signed).await {
                    return;
                }
                continue;
            }
        };
        let dialled = address.to_string();
        let following = follow_peer(&dialled, &peers.name, &tls, |bytes| {
            took(&bytes, &snapshot, &*say);
        });
        tokio::pin!(following);
        loop {
            tokio::select! {
                ended = &mut following => {
                    if ended_with(&ended, &dialled, &snapshot, &*say) {
                        tokio::time::sleep(AGAIN).await;
                        break;
                    }
                    // Refused for good by this leader: dialled again once whom to dial, or
                    // with what, changes.
                    loop {
                        if !changed(&mut leading, &mut holder, &mut replicas, &mut signed).await {
                            return;
                        }
                        if at(&leading, &holder, &replicas) != Some(address)
                            || signed.borrow().as_ref() != Some(&credentials)
                        {
                            break;
                        }
                    }
                    break;
                }
                still = changed(&mut leading, &mut holder, &mut replicas, &mut signed) => {
                    if !still {
                        return;
                    }
                    // Another to dial, or none, or a certificate to dial with anew.
                    if at(&leading, &holder, &replicas) != Some(address)
                        || signed.borrow().as_ref() != Some(&credentials)
                    {
                        break;
                    }
                }
            }
        }
    }
}

/// How long the replica taking the lease waits on each ready replica for its snapshot.
pub(super) const GATHER: Duration = Duration::from_secs(5);

/// Leads, on `leading`, as `elected` says this replica is elected, once it has taken the
/// newest snapshot there is into `snapshot` (04 §2): the one of the highest count among its
/// own and each ready replica's in `replicas` but its own pod `me`'s, each asked for its
/// latest within `wait`; with none of them holding one, it leads from a clean start,
/// loudly. One no longer elected by then does not lead. No longer elected, it no longer
/// leads.
#[allow(clippy::too_many_arguments)]
pub(super) async fn take_over(
    peers: Peers,
    me: String,
    wait: Duration,
    mut elected: watch::Receiver<bool>,
    replicas: watch::Receiver<Arc<[Replica]>>,
    signed: watch::Receiver<Option<Credentials>>,
    snapshot: watch::Sender<Option<Arc<Snapshot>>>,
    leading: watch::Sender<bool>,
    say: Rc<dyn Fn(&str)>,
) {
    loop {
        let now = *elected.borrow_and_update();
        if now && !*leading.borrow() {
            let others: Vec<SocketAddr> = replicas
                .borrow()
                .iter()
                .filter(|replica| replica.ready && replica.pod != me)
                .map(|replica| SocketAddr::new(replica.address, peers.port))
                .collect();
            let credentials = signed.borrow().clone();
            gather(
                &peers,
                &others,
                credentials.as_ref(),
                wait,
                &snapshot,
                &*say,
            )
            .await;
            if *elected.borrow() {
                leading.send_replace(true);
            }
        } else if !now {
            leading.send_replace(false);
        }
        if elected.changed().await.is_err() {
            return;
        }
    }
}

/// Takes into `snapshot` the newest of its own and those `others` serve, dialled with
/// `credentials`, each within `wait`: none newer, it is kept; none at all, it is taken as
/// none of a leader's, a clean start. Says which to `say`.
async fn gather(
    peers: &Peers,
    others: &[SocketAddr],
    credentials: Option<&Credentials>,
    wait: Duration,
    snapshot: &watch::Sender<Option<Arc<Snapshot>>>,
    say: &dyn Fn(&str),
) {
    let mut newest: Option<(Arc<Snapshot>, Option<SocketAddr>)> =
        snapshot.borrow().clone().map(|own| (own, None));
    let tls = credentials.map(|credentials| connector_to(credentials, &peers.id));
    for &other in others {
        let Some(Ok(tls)) = &tls else {
            say("the ready replicas' snapshots cannot be asked for: no certificate to dial with");
            break;
        };
        let Some(theirs) = latest(other, &peers.name, tls, wait, say).await else {
            continue;
        };
        if newest
            .as_ref()
            .is_none_or(|(newest, _)| theirs.count > newest.count)
        {
            newest = Some((theirs, Some(other)));
        }
    }
    match newest {
        Some((newest, from)) => {
            let count = newest.count;
            match from {
                Some(from) => {
                    snapshot.send_replace(Some(newest));
                    say(&format!(
                        "leading from the snapshot of the replica at {from}, of count {count}"
                    ));
                }
                None => say(&format!(
                    "leading from this replica's snapshot, of count {count}"
                )),
            }
        }
        None => {
            snapshot.send_replace(Some(Arc::new(Snapshot::default())));
            say("no ready replica holds a snapshot: leading from a clean start");
        }
    }
}

/// The latest snapshot the replica at `address` serves, if it gives one within `wait` that
/// this one reads; why not is said to `say`.
async fn latest(
    address: SocketAddr,
    name: &str,
    tls: &SslConnector,
    wait: Duration,
    say: &dyn Fn(&str),
) -> Option<Arc<Snapshot>> {
    let dialled = address.to_string();
    let (first, taken) = tokio::sync::oneshot::channel();
    let mut first = Some(first);
    let following = follow_peer(&dialled, name, tls, |bytes| {
        if let Some(first) = first.take() {
            let _ = first.send(bytes);
        }
    });
    let bytes = tokio::select! {
        bytes = taken => bytes.ok(),
        ended = following => {
            say(&format!("the snapshot of the replica at {dialled}: {ended}"));
            None
        }
        () = tokio::time::sleep(wait) => {
            say(&format!("the replica at {dialled} gave no snapshot within {} s", wait.as_secs()));
            None
        }
    }?;
    match Snapshot::decode(&bytes) {
        Ok(theirs) => Some(Arc::new(theirs)),
        Err(error) => {
            say(&format!(
                "the snapshot of the replica at {dialled} is not taken in: {error}"
            ));
            None
        }
    }
}

/// How long a replica that shares snapshots goes holding none before it starts clean:
/// past the Lease's 15 s, so that a leader gone is replaced first (04 §2).
pub(super) const CLEAN_AFTER: Duration = Duration::from_secs(60);

/// Says to `say` how following the leader at `dialled` ended, and whether to dial it
/// again: not once it refused this replica, their config versions sharing none, which no
/// dialling again changes; this replica then starts clean, loudly, where it holds no
/// snapshot yet, and is ready (04 §2).
fn ended_with(
    ended: &Unfollowed,
    dialled: &str,
    snapshot: &watch::Sender<Option<Arc<Snapshot>>>,
    say: &dyn Fn(&str),
) -> bool {
    match ended {
        Unfollowed::Refused(why) => {
            let clean = if clean(snapshot) {
                "; this replica translates afresh, as a control plane started clean does"
            } else {
                ""
            };
            say(&format!(
                "the leader at {dialled} refused this replica: {why}{clean}"
            ));
            false
        }
        other => {
            say(&format!("the leader's snapshots at {dialled}: {other}"));
            true
        }
    }
}

/// After `after`, takes `snapshot` as none of the leader's where it holds none yet, saying
/// so to `say`: a replica that cannot reach the leader at all, as one of a build from
/// before snapshots were shared serves none, starts clean, and is ready (04 §2).
pub(super) async fn clean_after(
    after: Duration,
    snapshot: watch::Sender<Option<Arc<Snapshot>>>,
    say: Rc<dyn Fn(&str)>,
) {
    tokio::time::sleep(after).await;
    if clean(&snapshot) {
        say(&format!(
            "no snapshot of the leader's held after {} s: this replica translates afresh, as \
             a control plane started clean does",
            after.as_secs()
        ));
    }
}

/// `snapshot` taken as none of the leader's, where it holds none: whether it held none.
fn clean(snapshot: &watch::Sender<Option<Arc<Snapshot>>>) -> bool {
    snapshot.send_if_modified(|held| {
        if held.is_some() {
            return false;
        }
        *held = Some(Arc::new(Snapshot::default()));
        true
    })
}

/// Takes the leader's snapshot `bytes` into `snapshot`, saying to `say` why one cannot be.
fn took(bytes: &[u8], snapshot: &watch::Sender<Option<Arc<Snapshot>>>, say: &dyn Fn(&str)) {
    match Snapshot::decode(bytes) {
        Ok(taken) => {
            snapshot.send_replace(Some(Arc::new(taken)));
        }
        Err(error @ SnapshotError::Version { .. }) => {
            say(&format!(
                "the leader's snapshot is not taken in: {error}; this replica translates \
                 afresh, as a control plane started clean does"
            ));
            snapshot.send_replace(Some(Arc::new(Snapshot::default())));
        }
        Err(error) => say(&format!("the leader's snapshot is not taken in: {error}")),
    }
}

/// Whether any of them changed, once one does, that one marked seen: false once any is
/// gone.
async fn changed(
    leading: &mut watch::Receiver<bool>,
    holder: &mut watch::Receiver<Option<String>>,
    replicas: &mut watch::Receiver<Arc<[Replica]>>,
    signed: &mut watch::Receiver<Option<Credentials>>,
) -> bool {
    tokio::select! {
        changed = leading.changed() => changed.is_ok(),
        changed = holder.changed() => changed.is_ok(),
        changed = replicas.changed() => changed.is_ok(),
        changed = signed.changed() => changed.is_ok(),
    }
}

/// The credentials of each certificate the issuer signs on `signed`, published on `out`.
pub(super) async fn credentials(
    mut signed: watch::Receiver<Option<Written>>,
    out: watch::Sender<Option<Credentials>>,
) {
    while let Some(next) = super::next_signed(&mut signed).await {
        out.send_replace(Some(next));
    }
}

#[cfg(test)]
mod tests;
