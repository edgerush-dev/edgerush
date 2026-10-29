//! Mutual TLS on the service, on real sockets: who may call a pod, and whom each end
//! refuses.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use boring::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use edgerush_certificates::testing::Authority;
use edgerush_config::Certificate;
use edgerush_control_service::TlsError;
use edgerush_ratelimit::service::membership::{Agreed, Heartbeat};
use edgerush_ratelimit::service::{Batch, PodId, Replicas, View};
use edgerush_ratelimit_service::grpc::{Client, Error, Server};
use edgerush_ratelimit_service::pod::{Pod, Timing};
use edgerush_ratelimit_service::secure::{self, Caller, PodCredentials, PodTls};
use edgerush_ratelimit_service::synchronous::{self, SyncPod, SyncServer};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tonic::Code;

/// The SPIFFE ID the pods share, that of the data plane they serve, and another
/// workload's, each issued by the one authority.
const PODS: &str = "spiffe://cluster.test/ns/edge/sa/ratelimit";
const PLANE: &str = "spiffe://cluster.test/ns/edge/sa/plane";
const OTHER: &str = "spiffe://cluster.test/ns/edge/sa/other";

/// A pod's TLS, presenting `certificate`, trusting `authority` and serving the data plane
/// `data_plane`.
fn pod(authority: &Authority, certificate: Certificate, data_plane: &str) -> PodTls {
    PodTls::new(&PodCredentials {
        certificate,
        authorities: authority.pem(),
        data_plane: data_plane.to_owned(),
    })
    .unwrap()
}

/// A pod alone in its view, served with the latest of `tls` on a port of its own: its
/// address, and where the connections it refuses are said.
async fn serve(tls: watch::Receiver<Arc<PodTls>>) -> (String, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let pod = Pod::spawn(
        0,
        vec![0],
        1,
        Timing::with_cycle(Duration::from_millis(10)),
        BTreeMap::new(),
    );
    let (refusing, refused) = mpsc::unbounded_channel();
    let server = Server::new(pod);
    tokio::spawn(secure::serve(
        listener,
        tls,
        server,
        move |_peer, why| {
            let _gone = refusing.send(why);
        },
        std::future::pending(),
    ));
    (address, refused)
}

/// A data plane's client of the pod at `address`, presenting `certificate`, trusting
/// `authority` to vouch for the pod and checking it for the pods' ID.
fn plane(address: &str, certificate: Certificate, authority: &Authority) -> Client {
    let tls = secure::data_plane(&certificate, &[authority.pem()], PODS).unwrap();
    Client::secured(address, watch::channel(tls).1).unwrap()
}

/// A pod's client of the pod at `address`, dialling as `tls` does.
fn dialled_by(address: &str, tls: &PodTls) -> Client {
    Client::secured(address, watch::channel(tls.connector().clone()).1).unwrap()
}

fn batch() -> Batch {
    Batch {
        from: 1,
        entries: Vec::new(),
        made: 0,
    }
}

fn beat() -> Heartbeat {
    Heartbeat {
        from: 1,
        incarnation: 1,
        agreed: Agreed {
            view: View {
                epoch: 0,
                pods: Vec::new(),
            },
            incarnations: Vec::new(),
        },
        hears: Vec::new(),
    }
}

fn replicas() -> Replicas {
    Replicas {
        from: 1,
        entries: Vec::new(),
        handover: false,
        epoch: 1,
    }
}

fn denied<T: std::fmt::Debug>(result: Result<T, Error>) -> bool {
    matches!(result, Err(Error::Status(status)) if status.code() == Code::PermissionDenied)
}

/// Whether a call failed as one refused in its handshake does: by its client, as a pod
/// it cannot reach; by the pod, in TLS 1.3 after the client has sent its request, as a
/// connection reset, whatever code tonic gives that.
fn failed<T: std::fmt::Debug>(result: Result<T, Error>) -> bool {
    matches!(result, Err(Error::Status(status)) if status.code() != Code::PermissionDenied)
}

fn unavailable<T: std::fmt::Debug>(result: Result<T, Error>) -> bool {
    matches!(result, Err(Error::Status(status)) if status.code() == Code::Unavailable)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_may_only_exchange_and_a_pod_only_beat_and_replicate() {
    let authority = Authority::new("workloads");
    let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]), PLANE);
    let (address, _refused) = serve(watch::channel(Arc::new(tls)).1).await;
    let data_plane = plane(
        &address,
        authority.issue(&["data-plane", PLANE]),
        &authority,
    );
    data_plane.exchange(batch()).await.unwrap();
    assert!(denied(data_plane.heartbeat(beat()).await));
    assert!(denied(data_plane.replicate(replicas()).await));
    let other = pod(&authority, authority.issue(&["127.0.0.2", PODS]), PLANE);
    let pod = dialled_by(&address, &other);
    pod.heartbeat(beat()).await.unwrap();
    pod.replicate(replicas()).await.unwrap();
    assert!(denied(pod.exchange(batch()).await));
}

/// The authority vouches only that a caller is one of its workloads: one with another
/// workload's ID, with none, or with two, is refused once its handshake is done, whatever
/// it would call; one the authority does not vouch for, or without a certificate, in the
/// handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_neither_the_pods_nor_the_data_plane_is_refused() {
    let authority = Authority::new("workloads");
    let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]), PLANE);
    let (address, mut refused) = serve(watch::channel(Arc::new(tls)).1).await;
    for names in [
        &["data-plane", OTHER][..],
        &["data-plane"],
        &["data-plane", PODS, PLANE],
    ] {
        // A client apiece: one whose connection failed may fail its next call undialled,
        // and the pod would have no refusal to say.
        let caller = || plane(&address, authority.issue(names), &authority);
        assert!(failed(caller().exchange(batch()).await), "{names:?}");
        assert!(refused.recv().await.is_some());
        assert!(failed(caller().heartbeat(beat()).await), "{names:?}");
        assert!(refused.recv().await.is_some());
    }
    let stranger = Authority::new("stranger");
    let unknown = plane(&address, stranger.issue(&["data-plane", PODS]), &authority);
    assert!(failed(unknown.heartbeat(beat()).await));
    assert!(refused.recv().await.is_some());

    // TLS 1.3 and HTTP/2, as the service's own client, but no certificate at all.
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_3))
        .unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    builder.set_alpn_protos(b"\x02h2").unwrap();
    let anonymous = Client::secured(&address, watch::channel(builder.build()).1).unwrap();
    assert!(failed(anonymous.exchange(batch()).await));
    assert!(refused.recv().await.is_some());
}

/// A data plane, and a pod, reach only a pod that the authority vouches for, for the name
/// dialled, with the pods' ID.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_is_reached_only_with_the_pods_id_and_the_name_dialled() {
    let authority = Authority::new("workloads");
    let stranger = Authority::new("stranger");
    let dialling = pod(&authority, authority.issue(&["127.0.0.2", PODS]), PLANE);
    for (certificate, reached) in [
        (authority.issue(&["127.0.0.1", PODS]), true),
        (authority.issue(&["127.0.0.1", OTHER]), false),
        (authority.issue(&["elsewhere.test", PODS]), false),
        (stranger.issue(&["127.0.0.1", PODS]), false),
    ] {
        let tls = pod(&authority, certificate, PLANE);
        let (address, _refused) = serve(watch::channel(Arc::new(tls)).1).await;
        let data_plane = plane(
            &address,
            authority.issue(&["data-plane", PLANE]),
            &authority,
        );
        let result = data_plane.exchange(batch()).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
        if !reached {
            assert!(unavailable(result));
        }
        let result = dialled_by(&address, &dialling).heartbeat(beat()).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credentials_that_cannot_tell_callers_apart_are_refused() {
    let authority = Authority::new("workloads");
    let with = |names: &[&str], data_plane: &str| {
        PodTls::new(&PodCredentials {
            certificate: authority.issue(names),
            authorities: authority.pem(),
            data_plane: data_plane.to_owned(),
        })
    };
    assert!(with(&["127.0.0.1", PODS], PLANE).is_ok());
    assert!(matches!(
        with(&["127.0.0.1"], PLANE),
        Err(TlsError::Identity(_))
    ));
    assert!(matches!(
        with(&["127.0.0.1", PODS, OTHER], PLANE),
        Err(TlsError::Identity(_))
    ));
    assert!(matches!(
        with(&["127.0.0.1", PODS], PODS),
        Err(TlsError::Identity(_))
    ));
    assert!(matches!(
        with(&["127.0.0.1", PODS], "plane.test"),
        Err(TlsError::Identity(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credentials_replaced_serve_the_next_connection() {
    let authority = Authority::new("workloads");
    let served = authority.issue(&["127.0.0.1", PODS]);
    let (replace, tls) = watch::channel(Arc::new(pod(&authority, served.clone(), PLANE)));
    let (address, mut refused) = serve(tls).await;
    let before = plane(
        &address,
        authority.issue(&["data-plane", PLANE]),
        &authority,
    );
    before.exchange(batch()).await.unwrap();

    replace.send_replace(Arc::new(pod(&authority, served, OTHER)));
    let after = plane(
        &address,
        authority.issue(&["data-plane", OTHER]),
        &authority,
    );
    after.exchange(batch()).await.unwrap();
    let left = plane(
        &address,
        authority.issue(&["data-plane", PLANE]),
        &authority,
    );
    assert!(failed(left.exchange(batch()).await));
    assert!(refused.recv().await.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pods_over_tls_agree_on_who_serves() {
    let authority = Authority::new("workloads");
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let mut pods = Vec::new();
    for (id, listener) in (0..3).zip(listeners) {
        let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]), PLANE);
        let dialling = watch::channel(tls.connector().clone()).1;
        let peers: BTreeMap<PodId, Client> = (0..3)
            .filter(|pod| *pod != id)
            .map(|pod| {
                let address = &addresses[usize::try_from(pod).unwrap()];
                (pod, Client::secured(address, dialling.clone()).unwrap())
            })
            .collect();
        let pod = Pod::spawn(
            id,
            vec![0, 1, 2],
            1,
            Timing::with_cycle(Duration::from_millis(10)),
            peers,
        );
        tokio::spawn(secure::serve(
            listener,
            watch::channel(Arc::new(tls)).1,
            Server::new(pod.clone()),
            |_peer, _why| {},
            std::future::pending(),
        ));
        pods.push(pod);
    }
    let deadline = Instant::now() + Duration::from_secs(10);
    for pod in &pods {
        while !matches!(pod.view().await, Some((view, true)) if view.pods == [0, 1, 2]) {
            assert!(Instant::now() < deadline, "the pods never agreed");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}

/// The code a server's answer to a request on `path`, from `caller` if any, carries in its
/// headers, if it is refused there.
async fn refusal<S>(server: &mut S, path: &str, caller: Option<Caller>) -> Option<Code>
where
    S: tower_service::Service<
            http::Request<tonic::body::Body>,
            Response = http::Response<tonic::body::Body>,
        >,
    S::Error: std::fmt::Debug,
{
    let mut request = http::Request::builder()
        .method("POST")
        .uri(path)
        .header("content-type", "application/grpc")
        .body(tonic::body::Body::empty())
        .unwrap();
    if let Some(caller) = caller {
        request.extensions_mut().insert(caller);
    }
    let response = server.call(request).await.unwrap();
    tonic::Status::from_header_map(response.headers()).map(|status| status.code())
}

const EXCHANGE: &str = "/edgerush.ratelimit.v1.RateLimit/Exchange";
const REPLICATE: &str = "/edgerush.ratelimit.v1.RateLimit/Replicate";
const HEARTBEAT: &str = "/edgerush.ratelimit.v1.RateLimit/Heartbeat";
const DECIDE: &str = "/edgerush.ratelimit.v1.RateLimit/Decide";
const COPY: &str = "/edgerush.ratelimit.v1.RateLimit/Copy";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_mode_lets_each_caller_call_only_its_own_methods() {
    let denied = Some(Code::PermissionDenied);
    let pod = Pod::spawn(
        0,
        vec![0],
        1,
        Timing::with_cycle(Duration::from_millis(10)),
        BTreeMap::new(),
    );
    let mut server = Server::new(pod);
    let sync = SyncPod::spawn(
        0,
        vec![0],
        1,
        synchronous::Timing::with_copy(Duration::from_millis(10)),
        BTreeMap::new(),
    )
    .unwrap();
    let mut sync_server = SyncServer::new(sync);
    for (path, allowed) in [
        (EXCHANGE, Caller::DataPlane),
        (REPLICATE, Caller::Pod),
        (HEARTBEAT, Caller::Pod),
    ] {
        for caller in [Caller::DataPlane, Caller::Pod] {
            let refused = refusal(&mut server, path, Some(caller)).await;
            assert_eq!(
                refused == denied,
                caller != allowed,
                "{path} from {caller:?}"
            );
        }
        // Served plain, a request says no caller, and anyone may call anything.
        assert_ne!(refusal(&mut server, path, None).await, denied, "{path}");
    }
    for (path, allowed) in [
        (DECIDE, Caller::DataPlane),
        (COPY, Caller::Pod),
        (HEARTBEAT, Caller::Pod),
    ] {
        for caller in [Caller::DataPlane, Caller::Pod] {
            let refused = refusal(&mut sync_server, path, Some(caller)).await;
            assert_eq!(
                refused == denied,
                caller != allowed,
                "{path} from {caller:?}"
            );
        }
        assert_ne!(
            refusal(&mut sync_server, path, None).await,
            denied,
            "{path}"
        );
    }
}
