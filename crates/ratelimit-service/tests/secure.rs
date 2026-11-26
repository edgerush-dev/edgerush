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
use edgerush_ratelimit_service::secure::{self, Caller, PodCredentials, PodTls, role};
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tonic::Code;

/// The pods' trust domain, their ControlPlane's.
const TRUST: &str = "edge.cluster.test";
/// The SPIFFE ID the pods share; a data plane's, another DataPlane's in another namespace,
/// and the controller's, all of the trust domain; another workload's of it; and a data
/// plane's of another ControlPlane: each issued by the one authority.
const PODS: &str = "spiffe://edge.cluster.test/ns/edge/sa/edgerush-ratelimit-edge";
const PLANE: &str = "spiffe://edge.cluster.test/ns/edge/sa/edgerush-proxy-api";
const PLANE_ELSEWHERE: &str = "spiffe://edge.cluster.test/ns/shop/sa/edgerush-proxy-front";
const CONTROLLER: &str = "spiffe://edge.cluster.test/ns/edge/sa/edgerush-controller-edge";
const OTHER: &str = "spiffe://edge.cluster.test/ns/edge/sa/other";
const FOREIGN_PLANE: &str = "spiffe://shop.cluster.test/ns/edge/sa/edgerush-proxy-api";

/// A pod's TLS, presenting `certificate`, trusting `authority` and serving the callers of
/// trust domain `domain`.
fn pod_of(authority: &Authority, certificate: Certificate, domain: &str) -> PodTls {
    PodTls::new(&PodCredentials {
        certificate,
        authorities: authority.pem(),
        trust_domain: domain.to_owned(),
    })
    .unwrap()
}

/// A pod's TLS of [`TRUST`], presenting `certificate` and trusting `authority`.
fn pod(authority: &Authority, certificate: Certificate) -> PodTls {
    pod_of(authority, certificate, TRUST)
}

/// A pod of the store, alone, served with the latest of `tls` on a port of its own: its
/// address, and where the connections it refuses are said.
async fn serve(tls: watch::Receiver<Arc<PodTls>>) -> (String, mpsc::UnboundedReceiver<String>) {
    serve_at("127.0.0.1:0".parse().unwrap(), tls).await
}

/// The same, listening at `at`.
async fn serve_at(
    at: std::net::SocketAddr,
    tls: watch::Receiver<Arc<PodTls>>,
) -> (String, mpsc::UnboundedReceiver<String>) {
    let listener = TcpListener::bind(at).await.unwrap();
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

/// How the controller dials the pods, as `plane`.
fn controller(certificate: Certificate, authority: &Authority) -> Dial {
    let tls = secure::controller(&certificate, &[authority.pem()], PODS).unwrap();
    Dial::Secured(watch::channel(tls).1)
}

/// A data plane's call: it opens its stream to the pod at `address`, dialling as `dial` does.
async fn decide(address: &str, dial: &Dial) -> Result<(), Error> {
    decide_stream(address, dial).await.map(|_stream| ())
}

/// The controller's call: it asks the pod `client` reaches what it has applied, as the one
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

/// A data plane may only open its stream, the controller only ask and name, and a pod only
/// make the pods' calls: each refused the others', as not permitted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_caller_may_make_only_its_own_calls_on_sockets() {
    let authority = Authority::new("workloads");
    let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]));
    let (address, _refused) = serve(watch::channel(Arc::new(tls)).1).await;
    let data_plane = plane(authority.issue(&["data-plane", PLANE]), &authority);
    decide(&address, &data_plane).await.unwrap();
    assert!(denied(probe(&client(&address, &data_plane)).await));
    let forming = controller(authority.issue(&["controller", CONTROLLER]), &authority);
    probe(&client(&address, &forming)).await.unwrap();
    assert!(denied(decide(&address, &forming).await));
    let other = pod(&authority, authority.issue(&["127.0.0.2", PODS]));
    assert!(denied(probe(&dialled_by(&address, &other)).await));
    let pod = Dial::Secured(watch::channel(other.connector().clone()).1);
    assert!(denied(decide(&address, &pod).await));
}

/// A pod serves every DataPlane of its ControlPlane, whatever its namespace, by its ID's shape
/// in the trust domain, and none of another's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn any_data_plane_of_the_trust_domain_is_served_and_none_of_another() {
    let authority = Authority::new("workloads");
    let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]));
    let (address, mut refused) = serve(watch::channel(Arc::new(tls)).1).await;
    for id in [PLANE, PLANE_ELSEWHERE] {
        let data_plane = plane(authority.issue(&["data-plane", id]), &authority);
        decide(&address, &data_plane).await.unwrap();
    }
    let foreign = plane(authority.issue(&["data-plane", FOREIGN_PLANE]), &authority);
    assert!(failed(decide(&address, &foreign).await));
    assert!(refused.recv().await.is_some());
}

/// Served plain, a pod takes every call from anyone: the data plane's and the controller's
/// alike.
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
async fn a_caller_neither_a_pod_a_data_plane_nor_the_controller_is_refused() {
    let authority = Authority::new("workloads");
    let tls = pod(&authority, authority.issue(&["127.0.0.1", PODS]));
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
    let unknown = controller(stranger.issue(&["controller", CONTROLLER]), &authority);
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

/// A data plane, and the controller, reach only a pod that the authority vouches for, with
/// the pods' ID; dialled at its IP, as each pod is at its own address, whatever names its
/// certificate carries, as the pods share one made before any has an address.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_dialled_at_an_ip_is_reached_only_with_the_pods_id() {
    let authority = Authority::new("workloads");
    let stranger = Authority::new("stranger");
    for (certificate, reached) in [
        (authority.issue(&["127.0.0.1", PODS]), true),
        (authority.issue(&["ratelimit.edge.svc", PODS]), true),
        (authority.issue(&["127.0.0.1", OTHER]), false),
        (stranger.issue(&["127.0.0.1", PODS]), false),
    ] {
        let tls = pod(&authority, certificate);
        let (address, _refused) = serve(watch::channel(Arc::new(tls)).1).await;
        let data_plane = plane(authority.issue(&["data-plane", PLANE]), &authority);
        let result = decide(&address, &data_plane).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
        if !reached {
            assert!(unavailable(result));
        }
        let forming = controller(authority.issue(&["controller", CONTROLLER]), &authority);
        let result = probe(&client(&address, &forming)).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
    }
}

/// Dialled by name, as data planes first ask the service's Service, a pod is reached only
/// where its certificate carries the name too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_dialled_by_name_is_reached_only_for_the_name_dialled() {
    use std::net::ToSocketAddrs;
    let authority = Authority::new("workloads");
    // Where `localhost` is dialled first, `::1` on some systems and `127.0.0.1` on others:
    // a connection refused there would take the dial's whole time on Windows.
    let first = ("localhost", 0).to_socket_addrs().unwrap().next().unwrap();
    for (certificate, reached) in [
        (authority.issue(&["localhost", PODS]), true),
        (authority.issue(&["127.0.0.1", PODS]), false),
    ] {
        let tls = pod(&authority, certificate);
        let (address, _refused) = serve_at(first, watch::channel(Arc::new(tls)).1).await;
        let port = address.rsplit(':').next().unwrap();
        let named = format!("localhost:{port}");
        let data_plane = plane(authority.issue(&["data-plane", PLANE]), &authority);
        let result = decide(&named, &data_plane).await;
        assert_eq!(result.is_ok(), reached, "{result:?}");
        if !reached {
            assert!(unavailable(result));
        }
    }
}

/// A data plane dialling a pod checks it for the pods' own ID, apart from what the pod dialled
/// would accept: an impostor carrying another ID, which takes the data plane for one of its
/// own and so would answer it, is refused by the data plane in its handshake.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pod_with_another_id_is_refused_even_one_that_would_answer() {
    let authority = Authority::new("workloads");
    let impostor = pod(&authority, authority.issue(&["127.0.0.1", OTHER]));
    let (address, _refused) = serve(watch::channel(Arc::new(impostor)).1).await;
    let data_plane = plane(authority.issue(&["data-plane", PLANE]), &authority);
    let result = decide(&address, &data_plane).await;
    assert!(unavailable(result), "the impostor was reached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credentials_that_cannot_tell_callers_apart_are_refused() {
    let authority = Authority::new("workloads");
    let with = |names: &[&str], domain: &str| {
        PodTls::new(&PodCredentials {
            certificate: authority.issue(names),
            authorities: authority.pem(),
            trust_domain: domain.to_owned(),
        })
    };
    assert!(with(&["127.0.0.1", PODS], TRUST).is_ok());
    for (names, domain) in [
        // No one ID of its own.
        (&["127.0.0.1"][..], TRUST),
        (&["127.0.0.1", PODS, OTHER], TRUST),
        // Its own ID of another domain, or a domain that is none.
        (&["127.0.0.1", PODS], "shop.cluster.test"),
        (&["127.0.0.1", PODS], ""),
        (&["127.0.0.1", PODS], "edge.cluster.test/ns"),
        // Its own ID a data plane's or the controller's, which would then be served as a pod.
        (&["127.0.0.1", PLANE], TRUST),
        (&["127.0.0.1", CONTROLLER], TRUST),
    ] {
        assert!(
            matches!(with(names, domain), Err(TlsError::Identity(_))),
            "{names:?} of {domain}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn credentials_replaced_serve_the_next_connection() {
    let authority = Authority::new("workloads");
    let served = authority.issue(&["127.0.0.1", PODS]);
    let (replace, tls) = watch::channel(Arc::new(pod(&authority, served)));
    let (address, mut refused) = serve(tls).await;
    let before = plane(authority.issue(&["data-plane", PLANE]), &authority);
    decide(&address, &before).await.unwrap();

    // The pods' ID and trust domain both another's from here on.
    const SHOP_PODS: &str = "spiffe://shop.cluster.test/ns/shop/sa/edgerush-ratelimit-shop";
    let shop = authority.issue(&["127.0.0.1", SHOP_PODS]);
    replace.send_replace(Arc::new(pod_of(&authority, shop, "shop.cluster.test")));
    let after = Dial::Secured(
        watch::channel(
            secure::data_plane(
                &authority.issue(&["data-plane", FOREIGN_PLANE]),
                &[authority.pem()],
                SHOP_PODS,
            )
            .unwrap(),
        )
        .1,
    );
    decide(&address, &after).await.unwrap();
    let left = Dial::Secured(
        watch::channel(
            secure::data_plane(
                &authority.issue(&["data-plane", PLANE]),
                &[authority.pem()],
                SHOP_PODS,
            )
            .unwrap(),
        )
        .1,
    );
    assert!(failed(decide(&address, &left).await));
    assert!(refused.recv().await.is_some());
}

/// Whose an ID is goes by its shape in the trust domain alone: `ns`, a namespace, `sa`, and a
/// ServiceAccount named as the operator names a data plane's or a controller's, nothing more.
#[test]
fn whose_an_id_is_goes_by_its_shape_in_the_trust_domain() {
    for (id, whose) in [
        (PLANE, Some(Caller::DataPlane)),
        (PLANE_ELSEWHERE, Some(Caller::DataPlane)),
        (CONTROLLER, Some(Caller::Controller)),
        (PODS, None),
        (OTHER, None),
        (FOREIGN_PLANE, None),
        // Another domain that starts as this one does.
        (
            "spiffe://edge.cluster.test.evil/ns/edge/sa/edgerush-proxy-api",
            None,
        ),
        // A ServiceAccount named with the prefix and nothing after it.
        (
            "spiffe://edge.cluster.test/ns/edge/sa/edgerush-proxy-",
            None,
        ),
        (
            "spiffe://edge.cluster.test/ns/edge/sa/edgerush-controller-",
            None,
        ),
        // No namespace, a part more or less, or the parts' names wrong.
        ("spiffe://edge.cluster.test/ns//sa/edgerush-proxy-api", None),
        (
            "spiffe://edge.cluster.test/ns/edge/sa/edgerush-proxy-api/more",
            None,
        ),
        (
            "spiffe://edge.cluster.test/ns/edge/edgerush-proxy-api",
            None,
        ),
        (
            "spiffe://edge.cluster.test/sa/edge/ns/edgerush-proxy-api",
            None,
        ),
        ("edge.cluster.test/ns/edge/sa/edgerush-proxy-api", None),
    ] {
        assert_eq!(role(id, TRUST), whose, "{id}");
    }
}

/// The IDs the control plane issues are told apart as the service tells them: a test holds
/// the operator's naming and the service's shapes to one.
#[test]
fn the_ids_the_control_plane_issues_are_told_apart_as_issued() {
    use edgerush_planes::objects::{controller_name, data_plane_id, spiffe_id};
    let domain = "edge.cluster.local";
    assert_eq!(
        role(
            &data_plane_id("edge", "cluster.local", "shop", "front"),
            domain
        ),
        Some(Caller::DataPlane)
    );
    assert_eq!(
        role(
            &spiffe_id("edge", "cluster.local", "edge", &controller_name("edge")),
            domain
        ),
        Some(Caller::Controller)
    );
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
const STORE_COORDINATE: &str = "/edgerush.cluster.v1.Cluster/Coordinate";
const STORE_PEER: &str = "/edgerush.cluster.v1.Cluster/Peer";
const STORE_COPY: &str = "/edgerush.cluster.v1.Cluster/Copy";
const STORE_HELLO: &str = "/edgerush.cluster.v1.Cluster/Hello";
const STORE_CONTROL: &str = "/edgerush.cluster.v1.Cluster/Control";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_caller_may_call_only_its_own_methods() {
    let denied = Some(Code::PermissionDenied);
    // The data planes' stream theirs, asking and naming the controller's, every other method
    // the pods'.
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
        (STORE_COORDINATE, Caller::Pod),
        (STORE_PEER, Caller::Pod),
        (STORE_COPY, Caller::Pod),
        (STORE_HELLO, Caller::Pod),
        (STORE_CONTROL, Caller::Controller),
    ] {
        for caller in [Caller::DataPlane, Caller::Pod, Caller::Controller] {
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
