//! Mutual TLS on the store's pods, on real sockets: who may call a pod, and whom each end
//! refuses.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use boring::ssl::{SslConnector, SslMethod, SslVerifyMode, SslVersion};
use edgerush_certificates::testing::Authority;
use edgerush_cluster::Message;
use edgerush_cluster_modes::Value;
use edgerush_config::Certificate;
use edgerush_control_service::TlsError;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer, ask, decide_stream};
use edgerush_ratelimit_service::grpc::{Client, Error};
use edgerush_ratelimit_service::secure::{self, Caller, PodCredentials, PodTls};
use std::sync::Arc;
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

/// A pod of the store, alone, served with the latest of `tls` on a port of its own: its
/// address, and where the connections it refuses are said.
async fn serve(tls: watch::Receiver<Arc<PodTls>>) -> (String, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let pod = StorePod::spawn(
        1,
        address.clone(),
        edgerush_cluster::Config::defaults(),
        Dial::Plain,
        Vec::new(),
    );
    let (refusing, refused) = mpsc::unbounded_channel();
    tokio::spawn(secure::serve(
        listener,
        tls,
        StoreServer::new(pod),
        move |_peer, why| {
            let _gone = refusing.send(why);
        },
        std::future::pending(),
    ));
    (address, refused)
}

/// How a data plane dials the pods, presenting `certificate`, trusting `authority` to vouch
/// for them and checking each for the pods' ID.
fn plane(certificate: Certificate, authority: &Authority) -> Dial {
    let tls = secure::data_plane(&certificate, &[authority.pem()], PODS).unwrap();
    Dial::Secured(watch::channel(tls).1)
}

/// A data plane's call: it opens its stream to the pod at `address`, dialling as `dial` does.
async fn decide(address: &str, dial: &Dial) -> Result<(), Error> {
    decide_stream(address, dial).await.map(|_stream| ())
}

/// A pod's call: it asks the pod `client` reaches the control plane's probe, as the one
/// forming the cluster does.
async fn probe(client: &Client) -> Result<(), Error> {
    let asked = Message::<Value>::Probe { life: 0, round: 0 };
    ask::<Value>(client, asked).await.map(|_answer| ())
}

/// A pod's client of the pod at `address`, dialling as `tls` does.
fn dialled_by(address: &str, tls: &PodTls) -> Client {
    Client::secured(address, watch::channel(tls.connector().clone()).1).unwrap()
}

/// A client of the pod at `address` dialling as `dial` does.
fn client(address: &str, dial: &Dial) -> Client {
    match dial {
        Dial::Secured(tls) => Client::secured(address, tls.clone()).unwrap(),
        Dial::Plain => Client::new(address).unwrap(),
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

/// A data plane may only open its stream, and a pod only make the pods' calls: each refused
/// the other's, as not permitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_data_plane_may_only_decide_and_a_pod_only_make_the_pods_calls() {
    let authority = Authority::new("workloads");
    let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]), PLANE);
    let (address, _refused) = serve(watch::channel(Arc::new(tls)).1).await;
    let data_plane = plane(authority.issue(&["data-plane", PLANE]), &authority);
    decide(&address, &data_plane).await.unwrap();
    assert!(denied(probe(&client(&address, &data_plane)).await));
    let other = pod(&authority, authority.issue(&["127.0.0.2", PODS]), PLANE);
    probe(&dialled_by(&address, &other)).await.unwrap();
    let pod = Dial::Secured(watch::channel(other.connector().clone()).1);
    assert!(denied(decide(&address, &pod).await));
}

/// Served plain, a pod takes every call from anyone: the data plane's and the pods' alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_served_plain_takes_any_call() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let pod = StorePod::spawn(
        1,
        address.clone(),
        edgerush_cluster::Config::defaults(),
        Dial::Plain,
        Vec::new(),
    );
    tokio::spawn(secure::serve_plain(
        listener,
        StoreServer::new(pod),
        std::future::pending(),
    ));
    decide(&address, &Dial::Plain).await.unwrap();
    probe(&client(&address, &Dial::Plain)).await.unwrap();
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
        let caller = || plane(authority.issue(names), &authority);
        assert!(failed(decide(&address, &caller()).await), "{names:?}");
        assert!(refused.recv().await.is_some());
        assert!(
            failed(probe(&client(&address, &caller())).await),
            "{names:?}"
        );
        assert!(refused.recv().await.is_some());
    }
    let stranger = Authority::new("stranger");
    let unknown = plane(stranger.issue(&["data-plane", PODS]), &authority);
    assert!(failed(probe(&client(&address, &unknown)).await));
    assert!(refused.recv().await.is_some());

    // TLS 1.3 and HTTP/2, as the service's own client, but no certificate at all.
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_3))
        .unwrap();
    builder.set_verify(SslVerifyMode::NONE);
    builder.set_alpn_protos(b"\x02h2").unwrap();
    let anonymous = Dial::Secured(watch::channel(builder.build()).1);
    assert!(failed(decide(&address, &anonymous).await));
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
        let data_plane = plane(authority.issue(&["data-plane", PLANE]), &authority);
        let result = decide(&address, &data_plane).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
        if !reached {
            assert!(unavailable(result));
        }
        let result = probe(&dialled_by(&address, &dialling)).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
    }
}

/// A pod dialling another checks it for the pods' own ID, apart from what the pod dialled
/// would accept: an impostor carrying another ID, which takes the dialling pod for its data
/// plane and so would answer it, is refused by the dialling pod in its handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_refuses_to_dial_a_pod_with_another_id_even_one_that_would_answer() {
    let authority = Authority::new("workloads");
    // The impostor's own ID is OTHER, and it serves PODS as its data plane: a pod dialling it
    // passes its caller check as a data plane, allowed to open its stream.
    let impostor = pod(&authority, authority.issue(&["127.0.0.1", OTHER]), PODS);
    let (address, _refused) = serve(watch::channel(Arc::new(impostor)).1).await;
    let dialling = pod(&authority, authority.issue(&["127.0.0.2", PODS]), PLANE);
    let dial = Dial::Secured(watch::channel(dialling.connector().clone()).1);
    let result = decide(&address, &dial).await;
    assert!(unavailable(result), "the impostor was reached");
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
    let before = plane(authority.issue(&["data-plane", PLANE]), &authority);
    decide(&address, &before).await.unwrap();

    replace.send_replace(Arc::new(pod(&authority, served, OTHER)));
    let after = plane(authority.issue(&["data-plane", OTHER]), &authority);
    decide(&address, &after).await.unwrap();
    let left = plane(authority.issue(&["data-plane", PLANE]), &authority);
    assert!(failed(decide(&address, &left).await));
    assert!(refused.recv().await.is_some());
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

const STORE_DECIDE: &str = "/edgerush.cluster.v1.Cluster/Decide";
const STORE_PEER: &str = "/edgerush.cluster.v1.Cluster/Peer";
const STORE_COPY: &str = "/edgerush.cluster.v1.Cluster/Copy";
const STORE_HELLO: &str = "/edgerush.cluster.v1.Cluster/Hello";
const STORE_CONTROL: &str = "/edgerush.cluster.v1.Cluster/Control";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_caller_may_call_only_its_own_methods() {
    let denied = Some(Code::PermissionDenied);
    // The data planes' stream theirs, every other method the pods'.
    let store = StorePod::spawn(
        1,
        "127.0.0.1:7000".to_owned(),
        edgerush_cluster::Config::defaults(),
        Dial::Plain,
        Vec::new(),
    );
    let mut store_server = StoreServer::new(store);
    for (path, allowed) in [
        (STORE_DECIDE, Caller::DataPlane),
        (STORE_PEER, Caller::Pod),
        (STORE_COPY, Caller::Pod),
        (STORE_HELLO, Caller::Pod),
        (STORE_CONTROL, Caller::Pod),
    ] {
        for caller in [Caller::DataPlane, Caller::Pod] {
            let refused = refusal(&mut store_server, path, Some(caller)).await;
            assert_eq!(
                refused == denied,
                caller != allowed,
                "{path} from {caller:?}"
            );
        }
        assert_ne!(
            refusal(&mut store_server, path, None).await,
            denied,
            "{path}"
        );
    }
}
