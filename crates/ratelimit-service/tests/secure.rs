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
use edgerush_control_service::{Credentials, connector};
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

/// The pods' authority and the data planes'.
struct Authorities {
    pods: Authority,
    data_planes: Authority,
}

fn authorities() -> Authorities {
    Authorities {
        pods: Authority::new("pods"),
        data_planes: Authority::new("data planes"),
    }
}

impl Authorities {
    /// A pod's TLS, presenting `certificate`.
    fn pod(&self, certificate: Certificate) -> PodTls {
        PodTls::new(&PodCredentials {
            certificate,
            pods: self.pods.pem(),
            data_planes: self.data_planes.pem(),
        })
        .unwrap()
    }
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

/// A client of the pod at `address`, presenting `certificate` and trusting `trusting` to
/// vouch for the pod.
fn client(address: &str, certificate: Certificate, trusting: &Authority) -> Client {
    let tls = connector(&Credentials {
        certificate,
        authorities: trusting.pem(),
    })
    .unwrap();
    Client::secured(address, watch::channel(tls).1).unwrap()
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
    let authorities = authorities();
    let tls = authorities.pod(authorities.pods.issue(&["127.0.0.1"]));
    let (address, _refused) = serve(watch::channel(Arc::new(tls)).1).await;
    let plane = client(
        &address,
        authorities.data_planes.issue(&["data-plane"]),
        &authorities.pods,
    );
    plane.exchange(batch()).await.unwrap();
    assert!(denied(plane.heartbeat(beat()).await));
    assert!(denied(plane.replicate(replicas()).await));
    let pod = client(
        &address,
        authorities.pods.issue(&["127.0.0.2"]),
        &authorities.pods,
    );
    pod.heartbeat(beat()).await.unwrap();
    pod.replicate(replicas()).await.unwrap();
    assert!(denied(pod.exchange(batch()).await));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_neither_authority_vouches_for_is_refused_in_its_handshake() {
    let authorities = authorities();
    let tls = authorities.pod(authorities.pods.issue(&["127.0.0.1"]));
    let (address, mut refused) = serve(watch::channel(Arc::new(tls)).1).await;
    let stranger = Authority::new("stranger");
    let unknown = client(&address, stranger.issue(&["data-plane"]), &authorities.pods);
    assert!(failed(unknown.exchange(batch()).await));
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_refuses_a_pod_the_pods_authorities_do_not_vouch_for() {
    let authorities = authorities();
    // A pod showing what the data planes' authority signed, or what is for another name.
    for certificate in [
        authorities.data_planes.issue(&["127.0.0.1"]),
        authorities.pods.issue(&["elsewhere.test"]),
    ] {
        let (address, _refused) =
            serve(watch::channel(Arc::new(authorities.pod(certificate))).1).await;
        let plane = client(
            &address,
            authorities.data_planes.issue(&["data-plane"]),
            &authorities.pods,
        );
        assert!(unavailable(plane.exchange(batch()).await));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_authority_both_the_pods_and_the_data_planes_is_refused() {
    let authorities = authorities();
    let refused = PodTls::new(&PodCredentials {
        certificate: authorities.pods.issue(&["127.0.0.1"]),
        pods: authorities.pods.pem(),
        data_planes: format!(
            "{}{}",
            authorities.data_planes.pem(),
            authorities.pods.pem()
        ),
    });
    assert!(refused.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credentials_replaced_serve_the_next_connection() {
    let authorities = authorities();
    let served = authorities.pods.issue(&["127.0.0.1"]);
    let (replace, tls) = watch::channel(Arc::new(authorities.pod(served.clone())));
    let (address, mut refused) = serve(tls).await;
    let before = client(
        &address,
        authorities.data_planes.issue(&["data-plane"]),
        &authorities.pods,
    );
    before.exchange(batch()).await.unwrap();

    let others = Authority::new("other data planes");
    replace.send_replace(Arc::new(
        PodTls::new(&PodCredentials {
            certificate: served,
            pods: authorities.pods.pem(),
            data_planes: others.pem(),
        })
        .unwrap(),
    ));
    let after = client(&address, others.issue(&["data-plane"]), &authorities.pods);
    after.exchange(batch()).await.unwrap();
    let left = client(
        &address,
        authorities.data_planes.issue(&["data-plane"]),
        &authorities.pods,
    );
    assert!(failed(left.exchange(batch()).await));
    assert!(refused.recv().await.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pods_over_tls_agree_on_who_serves() {
    let authorities = Arc::new(authorities());
    let mut listeners = Vec::new();
    let mut addresses = Vec::new();
    for _ in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addresses.push(listener.local_addr().unwrap().to_string());
        listeners.push(listener);
    }
    let mut pods = Vec::new();
    for (id, listener) in (0..3).zip(listeners) {
        let tls = authorities.pod(authorities.pods.issue(&["127.0.0.1"]));
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
