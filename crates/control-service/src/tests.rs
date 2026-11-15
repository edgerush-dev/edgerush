use super::*;
use boring::ssl::{SslAcceptor, SslConnector, SslMethod, SslVerifyMode, SslVersion};
use boring::x509::X509;
use boring::x509::store::X509StoreBuilder;
use edgerush_certificates::Identity;
use edgerush_certificates::testing::Authority;
use edgerush_config::{Certificate, Config};
use edgerush_control_stream::{CONFIG_VERSION, Malformed, Offer, Report, Staging, Versions, wire};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

/// A config told apart by its port, with a certificate.
fn config(port: u16) -> Config {
    let mut config: Config = serde_saphyr::from_str(&format!(
        "listeners:
  http:
    address: \"[::]:{port}\"
    protocol: http
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
routes: []
upstreams: {{}}
"
    ))
    .unwrap();
    config.certificates = [(
        "shop/secure".to_owned(),
        Certificate {
            chain: format!("chain {port}"),
            key: format!("key {port}"),
        },
    )]
    .into();
    config
}

fn credentials(authority: &Authority, names: &[&str]) -> Credentials {
    Credentials {
        certificate: authority.issue(names),
        authorities: authority.pem(),
    }
}

/// A control plane serving on a port of its own, and what it hears.
struct Plane {
    address: String,
    heard: mpsc::UnboundedReceiver<Heard>,
    offers: watch::Sender<Served>,
    gateway: watch::Sender<Arc<[IpAddr]>>,
    tls: watch::Sender<Arc<SslAcceptor>>,
    stop: Option<oneshot::Sender<()>>,
    served: JoinHandle<Result<(), ServeError>>,
}

impl Plane {
    /// One serving any data plane its authorities vouch for, offering `offer`.
    async fn start(credentials: &Credentials, offer: Option<Offer>) -> Self {
        Self::serving(credentials, Served::Anyone(offer.map(Arc::new)))
    }

    /// One serving as `served` says.
    fn serving(credentials: &Credentials, served: Served) -> Self {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        Self::serving_on(listener, credentials, served)
    }

    /// One serving any data plane on `listener`, which may have served another before.
    fn start_on(
        listener: std::net::TcpListener,
        credentials: &Credentials,
        offer: Option<Offer>,
    ) -> Self {
        Self::serving_on(listener, credentials, Served::Anyone(offer.map(Arc::new)))
    }

    fn serving_on(
        listener: std::net::TcpListener,
        credentials: &Credentials,
        served: Served,
    ) -> Self {
        listener.set_nonblocking(true).unwrap();
        let listener = TcpListener::from_std(listener).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (tls, accepting) = watch::channel(Arc::new(acceptor(credentials).unwrap()));
        let (offers, offered) = watch::channel(served);
        let (gateway, addresses) = watch::channel::<Arc<[IpAddr]>>(Arc::from([]));
        let (said, heard) = mpsc::unbounded_channel();
        let (stop, stopped) = oneshot::channel::<()>();
        let served = tokio::spawn(serve(
            listener,
            accepting,
            offered,
            addresses,
            move |_peer: SocketAddr, heard: Heard| {
                let _ = said.send(heard);
            },
            async move {
                let _ = stopped.await;
            },
        ));
        Self {
            address,
            heard,
            offers,
            gateway,
            tls,
            stop: Some(stop),
            served,
        }
    }

    /// The next thing heard, within a few seconds.
    async fn next(&mut self) -> Heard {
        tokio::time::timeout(Duration::from_secs(5), self.heard.recv())
            .await
            .expect("nothing heard")
            .expect("the server is gone")
    }

    /// Offers `offer` to any data plane, in place of what it offered.
    fn offer(&self, offer: Offer) {
        self.offers
            .send_modify(|served| served.offered(None, Arc::new(offer)));
    }

    /// Tells it to stop.
    fn signal(&mut self) {
        let _ = self.stop.take().unwrap().send(());
    }

    /// Waits for it to have stopped.
    async fn join(self) {
        tokio::time::timeout(Duration::from_secs(5), self.served)
            .await
            .expect("the server did not stop")
            .unwrap()
            .unwrap();
    }
}

/// A data plane following the control plane at `address`, each config it runs sent on,
/// failing those `fail` says.
fn follower(
    address: &str,
    name: &str,
    tls: SslConnector,
    fail: impl Fn(&Config) -> bool + Send + Sync + 'static,
) -> (mpsc::UnboundedReceiver<Config>, JoinHandle<Ended>) {
    let (ran, running) = mpsc::unbounded_channel();
    let (address, name) = (address.to_owned(), name.to_owned());
    let following = tokio::spawn(async move {
        let mut staging = Staging::new();
        follow(
            &address,
            &name,
            &tls,
            &mut staging,
            |config| {
                let ran = if fail(&config) {
                    Err("the port is taken".to_owned())
                } else {
                    let _ = ran.send(*config);
                    Ok(())
                };
                std::future::ready(ran)
            },
            |_| {},
            None,
        )
        .await
    });
    (running, following)
}

async fn ended(following: JoinHandle<Ended>) -> Ended {
    tokio::time::timeout(Duration::from_secs(5), following)
        .await
        .expect("the stream did not end")
        .unwrap()
}

async fn ran(running: &mut mpsc::UnboundedReceiver<Config>) -> Config {
    tokio::time::timeout(Duration::from_secs(5), running.recv())
        .await
        .expect("nothing ran")
        .unwrap()
}

/// A data plane's TLS as the test makes it: `certificate` presented, if any, `authorities`
/// trusted, TLS `most` at most.
fn own_connector(
    certificate: Option<&Certificate>,
    authorities: &str,
    most: SslVersion,
) -> SslConnector {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_max_proto_version(Some(most)).unwrap();
    if let Some(certificate) = certificate {
        let identity = Identity::read(certificate).unwrap();
        builder.set_certificate(&identity.leaf).unwrap();
        builder.set_private_key(&identity.key).unwrap();
    }
    let mut trusted = X509StoreBuilder::new().unwrap();
    for authority in X509::stack_from_pem(authorities.as_bytes()).unwrap() {
        trusted.add_cert(authority).unwrap();
    }
    builder.set_verify_cert_store(trusted.build()).unwrap();
    builder.set_verify(SslVerifyMode::PEER);
    builder.set_alpn_protos(b"\x02h2").unwrap();
    builder.build()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_runs_each_offer_and_says_so() {
    let authority = Authority::new("stream");
    let mut versions = Versions::new(7);
    let first = Offer::new(&config(80), None, &mut versions).unwrap();
    let mut plane = Plane::start(
        &credentials(&authority, &["control.test"]),
        Some(first.clone()),
    )
    .await;
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let (mut running, following) = follower(&plane.address, "control.test", tls, |config| {
        config
            .listeners
            .values()
            .any(|listener| listener.address.port() == 82)
    });
    let opened = Heard::Opened {
        held: Default::default(),
        version: CONFIG_VERSION,
        written: CONFIG_VERSION,
    };
    assert_eq!(plane.next().await, opened);
    assert_eq!(ran(&mut running).await, config(80));
    let Heard::Reported(report) = plane.next().await else {
        panic!("no report")
    };
    assert_eq!(report.running.config.as_deref(), Some(first.version()));
    // Each later offer.
    let second = Offer::new(&config(81), Some(&first), &mut versions).unwrap();
    plane.offer(second.clone());
    assert_eq!(ran(&mut running).await, config(81));
    let Heard::Reported(report) = plane.next().await else {
        panic!("no report")
    };
    assert_eq!(report.running.config.as_deref(), Some(second.version()));
    // One that fails to run is said to have.
    let third = Offer::new(&config(82), Some(&second), &mut versions).unwrap();
    plane.offer(third.clone());
    let Heard::Reported(report) = plane.next().await else {
        panic!("no report")
    };
    assert_eq!(report.running.config.as_deref(), Some(second.version()));
    let refused = report.refused.unwrap();
    assert_eq!(refused.config, third.version());
    assert_eq!(refused.why, "the port is taken");
    // Stopping ends the stream, and says so to the data plane.
    plane.signal();
    let stopping = "the control plane is stopping".to_owned();
    assert_eq!(ended(following).await, Ended::Ended(stopping.clone()));
    assert_eq!(plane.next().await, Heard::Closed(Some(stopping)));
    plane.join().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_the_authorities_do_not_vouch_for_is_refused() {
    let authority = Authority::new("stream");
    let other = Authority::new("other");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["control.test"]), Some(offer)).await;
    let another_authoritys = Credentials {
        certificate: other.issue(&["data-plane"]),
        authorities: authority.pem(),
    };
    let without = own_connector(None, &authority.pem(), SslVersion::TLS1_3);
    let tls_1_2 = own_connector(
        Some(&authority.issue(&["data-plane"])),
        &authority.pem(),
        SslVersion::TLS1_2,
    );
    for (what, tls) in [
        (
            "another authority's",
            connector(&another_authoritys).unwrap(),
        ),
        ("none", without),
        ("TLS 1.2", tls_1_2),
    ] {
        let (_, following) = follower(&plane.address, "control.test", tls, |_| false);
        assert!(
            matches!(ended(following).await, Ended::Unreachable(_)),
            "{what}"
        );
        assert!(matches!(plane.next().await, Heard::Refused(_)), "{what}");
    }
    plane.signal();
    plane.join().await;
}

/// The stream's own credentials of `authority`, a ControlPlane's, for `names`: what it
/// issues, and what its workloads trust.
fn signed(authority: &edgerush_certificates::authority::Authority, names: &[&str]) -> Credentials {
    let issued = authority
        .issue(
            &edgerush_certificates::authority::Leaf {
                common_name: names[0].to_owned(),
                dns: names.iter().map(|name| (*name).to_owned()).collect(),
                spiffe_id: format!("spiffe://edge.cluster.local/ns/edge/sa/{}", names[0]),
            },
            now(),
        )
        .unwrap();
    Credentials {
        certificate: Certificate {
            chain: issued.certificate,
            key: issued.key,
        },
        authorities: issued.bundle,
    }
}

/// Now, a few seconds on: the test's own certificates are valid from the second they are
/// made, which may come after the clock was read.
fn now() -> i64 {
    i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    )
    .unwrap()
        + 5
}

/// Two ControlPlanes' authorities that a platform's one root signs: each end of a stream
/// trusts its own ControlPlane's authority alone, an intermediate it anchors at, and what
/// the other's issues is refused though it leads to the same root (05 §2).
#[tokio::test(flavor = "multi_thread")]
async fn a_provided_authority_is_trusted_alone_and_not_its_siblings() {
    use edgerush_certificates::authority::{Authority as Issuing, Kind, Written};
    let root = Authority::new("platform root");
    let provided = |name: &str| {
        let intermediate = root.issue_authority(name);
        Issuing::read(
            &Written {
                certificate: intermediate.chain,
                key: intermediate.key,
                bundle: root.pem(),
            },
            Kind::Provided,
            now(),
        )
        .unwrap()
    };
    let (ours, theirs) = (provided("edge"), provided("shop"));
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&signed(&ours, &["control.test"]), Some(offer)).await;
    // Of its own authority: served.
    let tls = connector(&signed(&ours, &["data-plane"])).unwrap();
    let (mut running, _following) = follower(&plane.address, "control.test", tls, |_| false);
    ran(&mut running).await;
    // Of the other ControlPlane's, of the same root: refused.
    let tls = connector(&signed(&theirs, &["data-plane"])).unwrap();
    let (_, following) = follower(&plane.address, "control.test", tls, |_| false);
    assert!(matches!(ended(following).await, Ended::Unreachable(_)));
    plane.signal();
    plane.join().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_control_plane_the_authorities_do_not_vouch_for_or_of_another_name_is_refused() {
    let authority = Authority::new("stream");
    let other = Authority::new("other");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    // Another authority's certificate, though the data plane's own is trusted.
    let another_authoritys = Credentials {
        certificate: other.issue(&["control.test"]),
        authorities: authority.pem(),
    };
    for (what, credentials) in [
        ("another authority's", another_authoritys),
        ("another name", credentials(&authority, &["elsewhere.test"])),
    ] {
        let mut plane = Plane::start(&credentials, Some(offer.clone())).await;
        let (_, following) = follower(&plane.address, "control.test", tls.clone(), |_| false);
        let Ended::Unreachable(why) = ended(following).await else {
            panic!("{what}: not refused")
        };
        assert!(why.contains("CERTIFICATE_VERIFY_FAILED"), "{what}: {why}");
        // The control plane hears the data plane give up, in its own words.
        assert!(matches!(plane.next().await, Heard::Refused(_)), "{what}");
        plane.signal();
        plane.join().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn certificates_replaced_serve_the_next_connection() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["control.test"]), Some(offer)).await;
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let (mut running, first) = follower(&plane.address, "control.test", tls.clone(), |_| false);
    assert_eq!(ran(&mut running).await, config(80));
    assert!(matches!(plane.next().await, Heard::Opened { .. }));
    assert!(matches!(plane.next().await, Heard::Reported(_)));
    plane.tls.send_replace(Arc::new(
        acceptor(&credentials(&authority, &["renamed.test"])).unwrap(),
    ));
    let (_, refused) = follower(&plane.address, "control.test", tls.clone(), |_| false);
    assert!(matches!(ended(refused).await, Ended::Unreachable(_)));
    let (mut running, second) = follower(&plane.address, "renamed.test", tls, |_| false);
    assert_eq!(ran(&mut running).await, config(80));
    // The stream opened before goes on.
    assert!(!first.is_finished());
    plane.signal();
    ended(first).await;
    ended(second).await;
    plane.join().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_that_dials_again_is_sent_only_what_it_lacks() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(
        &credentials(&authority, &["control.test"]),
        Some(offer.clone()),
    )
    .await;
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let address = plane.address.clone();
    let mut staging = Staging::new();
    let runs = std::cell::Cell::new(0);
    let run = |_| {
        runs.set(runs.get() + 1);
        std::future::ready(Ok(()))
    };
    // What the control plane hears next, the stream followed meanwhile.
    async fn hear(plane: &mut Plane, following: Pin<&mut impl Future<Output = Ended>>) -> Heard {
        tokio::select! {
            ended = following => panic!("{ended}"),
            heard = plane.next() => heard,
        }
    }
    {
        // Followed until the config runs and is reported, then dropped, as a connection
        // lost.
        let following = follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            run,
            |_| {},
            None,
        );
        tokio::pin!(following);
        let heard = hear(&mut plane, following.as_mut()).await;
        assert!(matches!(heard, Heard::Opened { .. }), "{heard:?}");
        let heard = hear(&mut plane, following.as_mut()).await;
        assert!(matches!(heard, Heard::Reported(_)), "{heard:?}");
    }
    assert!(matches!(plane.next().await, Heard::Closed(_)));
    {
        let following = follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            run,
            |_| {},
            None,
        );
        tokio::pin!(following);
        let heard = hear(&mut plane, following.as_mut()).await;
        let Heard::Opened { held, .. } = heard else {
            panic!("{heard:?}")
        };
        assert_eq!(held.config.as_deref(), Some(offer.version()));
        plane.signal();
        let stopping = "the control plane is stopping".to_owned();
        let ended = tokio::time::timeout(Duration::from_secs(5), following)
            .await
            .expect("the stream did not end");
        assert_eq!(ended, Ended::Ended(stopping));
    }
    // Nothing was sent again, and nothing ran again.
    assert_eq!(runs.get(), 1);
    plane.join().await;
}

/// What the data plane says it serves of its listeners goes with the report of the config
/// it runs, is reported on its own whenever it changes, and is reported again as a stream
/// opens anew, for a control plane that has just started.
#[tokio::test(flavor = "multi_thread")]
async fn what_the_data_plane_serves_of_its_listeners_is_reported_as_it_changes() {
    use edgerush_control_stream::{Listening, Unserved};
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(
        &credentials(&authority, &["control.test"]),
        Some(offer.clone()),
    )
    .await;
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let address = plane.address.clone();
    let mut staging = Staging::new();
    let unserved = Listening {
        unserved: vec![Unserved {
            listener: "api".to_owned(),
            why: "its port is taken".to_owned(),
        }],
        draining: Vec::new(),
    };
    let (saying, mut watched) = tokio::sync::watch::channel(Listening::default());
    // Said as the config runs, so that its report says it.
    let said = saying.clone();
    let with = unserved.clone();
    let run = move |_| {
        said.send_replace(with.clone());
        std::future::ready(Ok(()))
    };
    async fn hear(plane: &mut Plane, following: Pin<&mut impl Future<Output = Ended>>) -> Heard {
        tokio::select! {
            ended = following => panic!("{ended}"),
            heard = plane.next() => heard,
        }
    }
    let reported = |heard: Heard| match heard {
        Heard::Reported(report) => report,
        other => panic!("{other:?}"),
    };
    {
        let following = follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            run,
            |_| {},
            Some(&mut watched),
        );
        tokio::pin!(following);
        let heard = hear(&mut plane, following.as_mut()).await;
        assert!(matches!(heard, Heard::Opened { .. }), "{heard:?}");
        let report = reported(hear(&mut plane, following.as_mut()).await);
        assert_eq!(report.running.config.as_deref(), Some(offer.version()));
        assert_eq!(report.listening, unserved, "the run's report");

        let draining = Listening {
            unserved: Vec::new(),
            draining: vec!["admin".to_owned()],
        };
        saying.send_replace(draining.clone());
        let report = reported(hear(&mut plane, following.as_mut()).await);
        assert_eq!(report.listening, draining, "a report of its own");
        assert_eq!(report.running.config.as_deref(), Some(offer.version()));
    }
    assert!(matches!(plane.next().await, Heard::Closed(_)));
    {
        let following = follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            |_| std::future::ready(Ok(())),
            |_| {},
            Some(&mut watched),
        );
        tokio::pin!(following);
        assert!(matches!(
            hear(&mut plane, following.as_mut()).await,
            Heard::Opened { .. }
        ));
        let report = reported(hear(&mut plane, following.as_mut()).await);
        assert_eq!(
            report.listening.draining,
            ["admin"],
            "said again as it opened"
        );
    }
    plane.signal();
    plane.join().await;
}

/// A stream to `plane` that opens with `first`, as no data plane of this build's would: how
/// the control plane refuses it.
async fn refused(plane: &Plane, tls: &SslConnector, first: wire::FromDataPlane) -> tonic::Status {
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{}", plane.address))
        .unwrap()
        .connect_with_connector(client::Dialler::new(&plane.address, "control.test", tls))
        .await
        .unwrap();
    let mut grpc = tonic::client::Grpc::new(channel);
    grpc.ready().await.unwrap();
    let (up, sending) = mpsc::channel(1);
    up.send(first).await.unwrap();
    let response = grpc
        .streaming(
            tonic::Request::new(Received(sending)),
            http::uri::PathAndQuery::from_static(SUBSCRIBE),
            Prost::<wire::FromDataPlane, wire::ToDataPlane>::default(),
        )
        .await;
    // Refused as the stream opens, or with its first message down.
    let refused = async {
        match response {
            Err(status) => status,
            Ok(response) => response.into_inner().message().await.unwrap_err(),
        }
    };
    tokio::time::timeout(Duration::from_secs(5), refused)
        .await
        .expect("not refused")
}

#[tokio::test(flavor = "multi_thread")]
async fn a_stream_that_does_not_open_with_what_the_data_plane_holds_is_refused() {
    let authority = Authority::new("stream");
    let mut plane = Plane::start(&credentials(&authority, &["control.test"]), None).await;
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let status = refused(&plane, &tls, wire::FromDataPlane::from(&Report::default())).await;
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    let why = Malformed::NoHello.to_string();
    assert_eq!(status.message(), why);
    assert_eq!(plane.next().await, Heard::Closed(Some(why)));
    plane.signal();
    plane.join().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_this_control_plane_is_too_old_for_is_refused() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["control.test"]), Some(offer)).await;
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let newer = wire::FromDataPlane {
        said: Some(wire::Said::Hello(wire::Hello {
            held: None,
            config_version: CONFIG_VERSION + 2,
            oldest_config_version: CONFIG_VERSION + 1,
        })),
    };
    let status = refused(&plane, &tls, newer).await;
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    let why = format!(
        "the data plane reads config version {} and after, and this control plane writes {} at most",
        CONFIG_VERSION + 1,
        CONFIG_VERSION
    );
    assert_eq!(status.message(), why);
    assert_eq!(plane.next().await, Heard::Closed(Some(why)));
    plane.signal();
    plane.join().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn certificate_files_are_read_again_and_each_change_handed_on_once() {
    let dir = std::env::temp_dir().join("edgerush-control-service-files");
    std::fs::create_dir_all(&dir).unwrap();
    let files = Files {
        certificate: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        authorities: dir.join("ca.crt"),
    };
    // Each file put in place whole, as Kubernetes swaps a mounted Secret's: one written in
    // place could be read half written.
    let put = |path: &std::path::Path, contents: &str| {
        let beside = path.with_extension("partial");
        std::fs::write(&beside, contents).unwrap();
        std::fs::rename(&beside, path).unwrap();
    };
    let write = |chain: &str| {
        put(&files.certificate, chain);
        put(&files.key, "key");
        put(&files.authorities, "authorities");
    };
    write("first");
    let first = files.read().await.unwrap();
    assert_eq!(first.certificate.chain, "first");
    let (seen, mut changes) = mpsc::unbounded_channel();
    let rereading = tokio::spawn(reread(
        files.clone(),
        first,
        Duration::from_millis(20),
        move |changed| {
            let _ = seen.send(
                changed
                    .map(|now| now.certificate.chain.clone())
                    .map_err(Clone::clone),
            );
        },
    ));
    let next = async |changes: &mut mpsc::UnboundedReceiver<_>| {
        tokio::time::timeout(Duration::from_secs(5), changes.recv())
            .await
            .expect("no change handed on")
            .unwrap()
    };
    write("second");
    assert_eq!(next(&mut changes).await, Ok("second".to_owned()));
    std::fs::remove_file(&files.key).unwrap();
    let Err(failed) = next(&mut changes).await else {
        panic!("no failure handed on")
    };
    assert!(failed.path.ends_with("tls.key"), "{failed}");
    // Said once, not on every read; and put back as it was, nothing has changed.
    tokio::time::sleep(Duration::from_millis(100)).await;
    put(&files.key, "key");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(changes.try_recv().is_err());
    write("third");
    assert_eq!(next(&mut changes).await, Ok("third".to_owned()));
    rereading.abort();
}

/// The data plane's certificate files, of `authority`'s certificate for it, the stream's
/// authorities `trusted`, in `dir`.
fn data_plane_files(dir: &str, authority: &Authority, trusted: &Authority) -> Files {
    let dir = std::env::temp_dir()
        .join("edgerush-control-service-subscribe")
        .join(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let files = Files {
        certificate: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        authorities: dir.join("ca.crt"),
    };
    put_files(&files, authority, trusted);
    files
}

/// Each file put in place whole, as Kubernetes swaps a mounted Secret's.
fn put_files(files: &Files, authority: &Authority, trusted: &Authority) {
    let put = |path: &std::path::Path, contents: &str| {
        let beside = path.with_extension("partial");
        std::fs::write(&beside, contents).unwrap();
        std::fs::rename(&beside, path).unwrap();
    };
    let issued = authority.issue(&["data-plane"]);
    put(&files.authorities, &trusted.pem());
    put(&files.key, &issued.key);
    put(&files.certificate, &issued.chain);
}

/// A data plane subscribed to the control plane at `address`, each config it runs and
/// what it says as it goes sent on.
fn subscriber(
    address: &str,
    files: Files,
    credentials: Credentials,
    backoff: Backoff,
) -> (
    mpsc::UnboundedReceiver<Config>,
    mpsc::UnboundedReceiver<Following>,
    JoinHandle<()>,
) {
    let (ran, running) = mpsc::unbounded_channel();
    let (said, told) = mpsc::unbounded_channel();
    let subscription = Subscription {
        address: address.to_owned(),
        files,
        credentials,
        reread: Duration::from_millis(20),
        backoff,
    };
    let subscribing = tokio::spawn(async move {
        let mut staging = Staging::new();
        let _never = subscribe(
            subscription,
            &mut staging,
            |config| {
                let _ = ran.send(*config);
                std::future::ready(Ok(()))
            },
            |_| {},
            None,
            |following| {
                let _ = said.send(following);
            },
        )
        .await;
    });
    (running, told, subscribing)
}

async fn within<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), receiver.recv())
        .await
        .expect("nothing came")
        .unwrap()
}

fn quick() -> Backoff {
    Backoff {
        base: Duration::from_millis(50),
        most: Duration::from_millis(400),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_dials_again_after_its_stream_ends_and_holds_what_it_was_sent() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    // An address where each connection is dropped as it is taken, so that each dial fails
    // at once; the control plane takes the socket over afterwards.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let dropping = listener.try_clone().unwrap();
    dropping.set_nonblocking(true).unwrap();
    let refusing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let refuser = {
        let refusing = Arc::clone(&refusing);
        std::thread::spawn(move || {
            while refusing.load(std::sync::atomic::Ordering::Relaxed) {
                match dropping.accept() {
                    Ok((connection, _)) => drop(connection),
                    Err(_) => std::thread::sleep(Duration::from_millis(2)),
                }
            }
        })
    };
    let files = data_plane_files("again", &authority, &authority);
    let presented = files.read().await.unwrap();
    let backoff = Backoff {
        base: Duration::from_millis(10),
        most: Duration::from_millis(640),
    };
    let (mut running, mut told, subscribing) =
        subscriber(&address.to_string(), files, presented, backoff);
    // Each dial fails, and the bound the waits are drawn below grows to 64 times the first.
    for _ in 0..7 {
        let Following::Ended { ended, .. } = within(&mut told).await else {
            panic!("not ended")
        };
        assert!(matches!(ended, Ended::Unreachable(_)), "{ended:?}");
    }
    refusing.store(false, std::sync::atomic::Ordering::Relaxed);
    refuser.join().unwrap();
    let again = listener.try_clone().unwrap();
    let server = credentials(&authority, &["127.0.0.1"]);
    let mut plane = Plane::start_on(listener, &server, Some(offer.clone()));
    assert_eq!(within(&mut running).await, config(80));
    assert!(matches!(plane.next().await, Heard::Opened { .. }));
    assert!(matches!(plane.next().await, Heard::Reported(_)));
    plane.signal();
    plane.join().await;
    // The stream carried a config, so the wait is drawn below the first bound again.
    let ending = async {
        loop {
            match within(&mut told).await {
                Following::Ended {
                    ended: ended @ Ended::Ended(_),
                    wait,
                } => break (ended, wait),
                Following::Ended { .. } => {}
                other => panic!("{other:?}"),
            }
        }
    };
    let ended = tokio::time::timeout(Duration::from_secs(5), ending)
        .await
        .expect("the stream did not end");
    assert_eq!(
        ended.0,
        Ended::Ended("the control plane is stopping".to_owned())
    );
    assert!(ended.1 < backoff.base, "{:?}", ended.1);
    // The control plane back on its address: the data plane says what it holds, and is
    // sent nothing to run again.
    let mut plane = Plane::start_on(again, &server, Some(offer.clone()));
    let Heard::Opened { held, .. } = plane.next().await else {
        panic!("not opened")
    };
    assert_eq!(held.config.as_deref(), Some(offer.version()));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(running.try_recv().is_err());
    subscribing.abort();
    plane.signal();
    plane.join().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn certificate_files_replaced_are_presented_on_the_next_dial() {
    let authority = Authority::new("stream");
    let untrusted = Authority::new("other");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["127.0.0.1"]), Some(offer)).await;
    // A certificate the control plane does not trust: each dial refused.
    let files = data_plane_files("replaced", &untrusted, &authority);
    let credentials = files.read().await.unwrap();
    let (mut running, mut told, subscribing) =
        subscriber(&plane.address.clone(), files.clone(), credentials, quick());
    for _ in 0..2 {
        let Following::Ended { ended, .. } = within(&mut told).await else {
            panic!("not refused")
        };
        assert!(matches!(ended, Ended::Unreachable(_)), "{ended:?}");
    }
    // Files that cannot be used are said so, and the last kept. Dials go on failing
    // meanwhile, so each wait has a deadline of its own.
    std::fs::write(&files.key, "not a key").unwrap();
    let unusable = async {
        loop {
            match within(&mut told).await {
                Following::Unusable(_) => break,
                Following::Ended { .. } => {}
                Following::Replaced => panic!("unusable files put to use"),
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), unusable)
        .await
        .expect("not said to be unusable");
    put_files(&files, &authority, &authority);
    let replaced = async {
        loop {
            match within(&mut told).await {
                Following::Replaced => break,
                Following::Ended { .. } | Following::Unusable(_) => {}
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), replaced)
        .await
        .expect("not said to be replaced");
    assert_eq!(within(&mut running).await, config(80));
    subscribing.abort();
    plane.signal();
    plane.join().await;
}

/// While a config waits to run — a data plane starting, say, its first EndpointSlices
/// listed — the stream goes on: here, its certificate files are read again meanwhile.
#[tokio::test(flavor = "multi_thread")]
async fn the_stream_goes_on_while_a_config_waits_to_run() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["127.0.0.1"]), Some(offer)).await;
    let files = data_plane_files("waits", &authority, &authority);
    let presented = files.read().await.unwrap();
    let subscription = Subscription {
        address: plane.address.clone(),
        files: files.clone(),
        credentials: presented,
        reread: Duration::from_millis(20),
        backoff: quick(),
    };
    let (started, mut starting) = mpsc::unbounded_channel();
    let (said, mut told) = mpsc::unbounded_channel();
    let release = Arc::new(tokio::sync::Notify::new());
    let released = Arc::clone(&release);
    let subscribing = tokio::spawn(async move {
        let mut staging = Staging::new();
        let _never = subscribe(
            subscription,
            &mut staging,
            |_config| {
                let _ = started.send(());
                let released = Arc::clone(&released);
                async move {
                    released.notified().await;
                    Ok(())
                }
            },
            |_| {},
            None,
            |following| {
                let _ = said.send(following);
            },
        )
        .await;
    });
    within(&mut starting).await;
    put_files(&files, &authority, &authority);
    let replaced = async {
        loop {
            if within(&mut told).await == Following::Replaced {
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), replaced)
        .await
        .expect("not read again while the config waited");
    // Let go, it runs, and is reported.
    release.notify_one();
    assert!(matches!(plane.next().await, Heard::Opened { .. }));
    assert!(matches!(plane.next().await, Heard::Reported(_)));
    subscribing.abort();
    plane.signal();
    plane.join().await;
}

/// A control plane with no gateway's addresses to say, its sender of them gone, serves its
/// stream all the same: the last addresses, none, are all there are.
#[tokio::test(flavor = "multi_thread")]
async fn a_stream_goes_on_once_the_gateways_addresses_can_no_longer_change() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["control.test"]), Some(offer)).await;
    drop(std::mem::replace(
        &mut plane.gateway,
        watch::channel::<Arc<[IpAddr]>>(Arc::from([])).0,
    ));
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let (mut running, following) = follower(&plane.address, "control.test", tls, |_| false);
    assert_eq!(ran(&mut running).await, config(80));
    assert!(matches!(plane.next().await, Heard::Opened { .. }));
    assert!(matches!(plane.next().await, Heard::Reported(_)));
    plane.offer(Offer::new(&config(81), None, &mut Versions::new(8)).unwrap());
    assert_eq!(ran(&mut running).await, config(81));
    assert!(!following.is_finished());
    plane.signal();
    ended(following).await;
    plane.join().await;
}

/// What a data plane is handed, in the order it is handed it.
#[derive(Debug, PartialEq)]
enum Handed {
    Config,
    Gateway(Vec<IpAddr>),
}

/// The gateway's addresses reach a data plane before the config it runs, and again each
/// time they change, while the config is not sent again.
#[tokio::test(flavor = "multi_thread")]
async fn the_gateways_addresses_come_before_the_config_and_again_as_they_change() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::start(&credentials(&authority, &["control.test"]), Some(offer)).await;
    let first: Vec<IpAddr> = vec!["10.244.0.5".parse().unwrap()];
    plane.gateway.send_replace(Arc::from(first.clone()));
    let tls = connector(&credentials(&authority, &["data-plane"])).unwrap();
    let (handed, mut handing) = mpsc::unbounded_channel();
    let address = plane.address.clone();
    let following = tokio::spawn(async move {
        let mut staging = Staging::new();
        let running = handed.clone();
        follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            move |_config| {
                let _ = running.send(Handed::Config);
                std::future::ready(Ok(()))
            },
            move |addresses| {
                let _ = handed.send(Handed::Gateway(addresses));
            },
            None,
        )
        .await
    });
    assert_eq!(within(&mut handing).await, Handed::Gateway(first));
    assert_eq!(within(&mut handing).await, Handed::Config);
    // Once the data plane has reported, so that nothing but the change sends more.
    assert!(matches!(plane.next().await, Heard::Opened { .. }));
    assert!(matches!(plane.next().await, Heard::Reported(_)));
    let more: Vec<IpAddr> = vec!["10.244.0.5".parse().unwrap(), "fd00::6".parse().unwrap()];
    plane.gateway.send_replace(Arc::from(more.clone()));
    assert_eq!(within(&mut handing).await, Handed::Gateway(more));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        handing.try_recv().is_err(),
        "more was sent than the addresses"
    );
    plane.signal();
    ended(following).await;
    plane.join().await;
}

/// The ID of DataPlane `web`, of ControlPlane `edge`, as its certificate carries it.
const WEB: &str = "spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-web";

/// The ID of DataPlane `api`, of the same ControlPlane.
const API: &str = "spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-api";

/// The data planes of these IDs, each offered as given.
fn ids(served: &[(&str, Option<Arc<Offer>>)]) -> Served {
    Served::Ids(
        served
            .iter()
            .map(|(id, offer)| ((*id).to_owned(), offer.clone()))
            .collect(),
    )
}

/// Each data plane is offered what is offered to its own ID, and nothing of another's; one
/// whose ID is not served, or that carries none, is told why. Whom it serves changes only
/// as data planes come and go, each kept with what it was offered.
#[test]
fn a_data_plane_is_offered_its_own_or_told_why_not() {
    let web_offer = Arc::new(Offer::new(&config(80), None, &mut Versions::new(7)).unwrap());
    let api_offer = Arc::new(Offer::new(&config(81), None, &mut Versions::new(8)).unwrap());
    let other_domain = "spiffe://other.cluster.local/ns/edge/sa/edgerush-proxy-web";
    let not_served = |id: &str| Err(Unserved::NotServed(id.to_owned()));
    let both = ids(&[
        (WEB, Some(Arc::clone(&web_offer))),
        (API, Some(Arc::clone(&api_offer))),
    ]);
    assert_eq!(both.offer_to(Some(WEB)), Ok(Some(&web_offer)));
    assert_eq!(both.offer_to(Some(API)), Ok(Some(&api_offer)));
    assert_eq!(both.offer_to(Some(other_domain)), not_served(other_domain));
    assert_eq!(both.offer_to(None), Err(Unserved::NoId));
    assert_eq!(Served::of([Some(WEB)]).offer_to(Some(WEB)), Ok(None));
    let nobody = Served::default();
    assert_eq!(nobody.offer_to(Some(WEB)), not_served(WEB));
    assert_eq!(nobody.offer_to(None), Err(Unserved::NoId));
    assert!(matches!(Served::of([]), Served::Ids(ids) if ids.is_empty()));
    let anyone = Served::Anyone(Some(Arc::clone(&web_offer)));
    for id in [Some(WEB), Some(API), None] {
        assert_eq!(anyone.offer_to(id), Ok(Some(&web_offer)), "{id:?}");
    }
    assert_eq!(Served::of([None]).offer_to(None), Ok(None));
    assert!(matches!(
        Served::of([Some(WEB), None]),
        Served::Anyone(None)
    ));
    // The same data planes: what each was offered stays.
    let mut served = both.clone();
    assert!(!served.serve(Served::of([Some(API), Some(WEB)])));
    assert_eq!(served.offer_to(Some(WEB)), Ok(Some(&web_offer)));
    let mut served = anyone.clone();
    assert!(!served.serve(Served::of([None])));
    assert_eq!(served.offer_to(None), Ok(Some(&web_offer)));
    // One goes, one comes: the one that stays keeps its own, the new one has nothing yet.
    let other = "spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-other";
    let mut served = both.clone();
    assert!(served.serve(Served::of([Some(WEB), Some(other)])));
    assert_eq!(served.offer_to(Some(WEB)), Ok(Some(&web_offer)));
    assert_eq!(served.offer_to(Some(other)), Ok(None));
    assert_eq!(served.offer_to(Some(API)), not_served(API));
    // Any instead, or nobody: nothing kept.
    for next in [Served::of([None]), Served::default()] {
        let mut served = both.clone();
        assert!(served.serve(next.clone()), "{next:?}");
        assert!(served.offer_to(None).ok().flatten().is_none(), "{next:?}");
        assert!(
            served.offer_to(Some(WEB)).ok().flatten().is_none(),
            "{next:?}"
        );
    }
    let mut served = anyone;
    assert!(served.serve(Served::of([Some(WEB)])));
    assert_eq!(served.offer_to(Some(WEB)), Ok(None));
    // An offer for one data plane is its alone.
    let mut served = Served::of([Some(WEB), Some(API)]);
    served.offered(Some(WEB), Arc::clone(&web_offer));
    assert_eq!(served.offer_to(Some(WEB)), Ok(Some(&web_offer)));
    assert_eq!(served.offer_to(Some(API)), Ok(None));
    let mut served = Served::default();
    served.offered(Some(WEB), Arc::clone(&web_offer));
    assert_eq!(served.offer_to(Some(WEB)), Ok(Some(&web_offer)));
    let mut served = Served::default();
    served.offered(None, Arc::clone(&web_offer));
    assert_eq!(served.offer_to(Some(API)), Ok(Some(&web_offer)));
}

/// Two data planes of two IDs dial one control plane: each runs its own config, and a
/// third of another ID, refused, changes nothing for either.
#[tokio::test(flavor = "multi_thread")]
async fn two_data_planes_each_run_their_own_config() {
    let authority = Authority::new("stream");
    let web_offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let api_offer = Offer::new(&config(81), None, &mut Versions::new(8)).unwrap();
    let mut plane = Plane::serving(
        &credentials(&authority, &["control.test"]),
        ids(&[
            (WEB, Some(Arc::new(web_offer))),
            (API, Some(Arc::new(api_offer))),
        ]),
    );
    let carrying = |names: &[&str]| connector(&credentials(&authority, names)).unwrap();
    let (mut web_running, web) = follower(
        &plane.address,
        "control.test",
        carrying(&["data-plane", WEB]),
        |_| false,
    );
    let (mut api_running, api) = follower(
        &plane.address,
        "control.test",
        carrying(&["data-plane", API]),
        |_| false,
    );
    assert_eq!(ran(&mut web_running).await, config(80));
    assert_eq!(ran(&mut api_running).await, config(81));
    let other = "spiffe://edge.cluster.local/ns/edge/sa/edgerush-proxy-other";
    let (_, refused) = follower(
        &plane.address,
        "control.test",
        carrying(&["data-plane", other]),
        |_| false,
    );
    assert_eq!(
        ended(refused).await,
        Ended::Ended(format!("{other} is no DataPlane this control plane serves"))
    );
    // Another config for one reaches it alone.
    let next = Offer::new(&config(82), None, &mut Versions::new(9)).unwrap();
    plane
        .offers
        .send_modify(|served| served.offered(Some(API), Arc::new(next)));
    assert_eq!(ran(&mut api_running).await, config(82));
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        web_running.try_recv().is_err(),
        "the other was sent something"
    );
    assert!(!web.is_finished() && !api.is_finished());
    // What was heard: two opened and reported, the third closed, in whatever order.
    let mut heard = Vec::new();
    while let Ok(Some(said)) =
        tokio::time::timeout(Duration::from_millis(200), plane.heard.recv()).await
    {
        heard.push(said);
    }
    let opened = heard
        .iter()
        .filter(|said| matches!(said, Heard::Opened { .. }))
        .count();
    assert_eq!(opened, 2, "{heard:?}");
    plane.signal();
    ended(web).await;
    ended(api).await;
    plane.join().await;
}

/// A data plane is served by the ID its certificate carries: the one served runs its
/// config, and one of another ID, of another trust domain or of none is refused, told why,
/// and that is heard.
#[tokio::test(flavor = "multi_thread")]
async fn each_data_plane_is_known_by_the_id_its_certificate_carries() {
    let authority = Authority::new("stream");
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    let mut plane = Plane::serving(
        &credentials(&authority, &["control.test"]),
        ids(&[(WEB, Some(Arc::new(offer)))]),
    );
    let carrying = |names: &[&str]| connector(&credentials(&authority, names)).unwrap();
    let (mut running, following) = follower(
        &plane.address,
        "control.test",
        carrying(&["data-plane", WEB]),
        |_| false,
    );
    assert_eq!(ran(&mut running).await, config(80));
    assert!(matches!(plane.next().await, Heard::Opened { .. }));
    assert!(matches!(plane.next().await, Heard::Reported(_)));
    let other_domain = "spiffe://other.cluster.local/ns/edge/sa/edgerush-proxy-web";
    for (names, why) in [
        (
            vec!["data-plane", API],
            format!("{API} is no DataPlane this control plane serves"),
        ),
        (
            vec!["data-plane", other_domain],
            format!("{other_domain} is no DataPlane this control plane serves"),
        ),
        (
            vec!["data-plane"],
            "the data plane's certificate carries no SPIFFE ID, or more than one".to_owned(),
        ),
        (
            vec!["data-plane", WEB, API],
            "the data plane's certificate carries no SPIFFE ID, or more than one".to_owned(),
        ),
    ] {
        let (mut refused_running, refusing) =
            follower(&plane.address, "control.test", carrying(&names), |_| false);
        assert_eq!(
            ended(refusing).await,
            Ended::Ended(why.clone()),
            "{names:?}"
        );
        assert_eq!(plane.next().await, Heard::Closed(Some(why)), "{names:?}");
        assert!(refused_running.try_recv().is_err(), "{names:?}");
        let status = refused(&plane, &carrying(&names), staging_hello()).await;
        assert_eq!(status.code(), tonic::Code::PermissionDenied, "{names:?}");
        assert!(
            matches!(plane.next().await, Heard::Closed(Some(_))),
            "{names:?}"
        );
    }
    // The one served is served on.
    assert!(!following.is_finished());
    plane.signal();
    ended(following).await;
    plane.join().await;
}

/// The hello a data plane that holds nothing opens its stream with.
fn staging_hello() -> wire::FromDataPlane {
    Staging::new().hello()
}

/// A data plane refused, as its DataPlane is not served, dials again on its backoff and is
/// served once it is; and once it is served no longer, its stream is ended, told why, and
/// what is offered to another never reaches it.
#[tokio::test(flavor = "multi_thread")]
async fn a_data_plane_is_served_once_its_data_plane_is_and_no_longer_once_it_is_not() {
    let authority = Authority::new("stream");
    let mut plane = Plane::serving(&credentials(&authority, &["127.0.0.1"]), Served::default());
    let files = data_plane_files("served-by-id", &authority, &authority);
    let issued = authority.issue(&["data-plane", WEB]);
    std::fs::write(&files.certificate, &issued.chain).unwrap();
    std::fs::write(&files.key, &issued.key).unwrap();
    let presented = files.read().await.unwrap();
    let (mut running, mut told, subscribing) =
        subscriber(&plane.address, files, presented, quick());
    let not_served = format!("{WEB} is no DataPlane this control plane serves");
    let Following::Ended { ended, .. } = within(&mut told).await else {
        panic!("not ended")
    };
    assert_eq!(ended, Ended::Ended(not_served.clone()));
    assert_eq!(plane.next().await, Heard::Closed(Some(not_served.clone())));
    // Its DataPlane served: on its next dial, it runs its config.
    let offer = Offer::new(&config(80), None, &mut Versions::new(7)).unwrap();
    plane
        .offers
        .send_replace(ids(&[(WEB, Some(Arc::new(offer)))]));
    assert_eq!(within(&mut running).await, config(80));
    // What was told of the dials before it.
    while told.try_recv().is_ok() {}
    // Another served in its place, with a config of its own.
    let another = Offer::new(&config(81), None, &mut Versions::new(8)).unwrap();
    plane
        .offers
        .send_replace(ids(&[(API, Some(Arc::new(another)))]));
    let ending = async {
        loop {
            if let Following::Ended { ended, .. } = within(&mut told).await
                && ended == Ended::Ended(not_served.clone())
            {
                break;
            }
        }
    };
    tokio::time::timeout(Duration::from_secs(5), ending)
        .await
        .expect("its stream was not ended");
    // Its next dials are refused as well, and nothing of the other's reaches it.
    let Following::Ended { ended, .. } = within(&mut told).await else {
        panic!("not ended")
    };
    assert_eq!(ended, Ended::Ended(not_served));
    assert!(running.try_recv().is_err());
    subscribing.abort();
    plane.signal();
    plane.join().await;
}
