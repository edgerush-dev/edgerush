//! Upstreams reached over TLS.

use super::*;

/// A worker whose one upstream `up`, at `upstream`, is reached over TLS as `tls` says
/// and spoken to in `protocol`.
async fn serving_worker_to_tls(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    tls: edgerush_config::UpstreamTls,
) -> SocketAddr {
    serving_worker_to_tls_with(protocol, tls, everything_config(upstream)).await
}

/// The same, with `gateway` the certificate `tls` may name to show an endpoint that
/// asks who the data plane is.
async fn serving_worker_to_tls_showing(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    tls: edgerush_config::UpstreamTls,
    gateway: &edgerush_config::Certificate,
) -> SocketAddr {
    let mut config = everything_config(upstream);
    config
        .certificates
        .insert("gateway".to_owned(), gateway.clone());
    serving_worker_to_tls_with(protocol, tls, config).await
}

/// A worker whose `config` has its upstream `up` reached over TLS as `tls` says and
/// spoken to in `protocol`.
async fn serving_worker_to_tls_with(
    protocol: UpstreamProtocol,
    tls: edgerush_config::UpstreamTls,
    mut config: Config,
) -> SocketAddr {
    let up = config.upstreams.get_mut("up").unwrap();
    up.protocol = protocol;
    up.tls = Some(tls);
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    front
}

/// An upstream that asks who the data plane is is shown the client certificate its
/// TLS names, in either protocol; without one, it will not speak.
#[tokio::test]
async fn an_upstream_that_asks_is_shown_the_client_certificate() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::tls::testing::certificate;
            let server = certificate(&["backend.test"]);
            let ours = certificate(&["gateway"]);
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = tls_upstream_asking(&server, Agrees::Either, Some(&ours)).await;
                let mut tls = trusting("backend.test", &server);
                tls.client_certificate = Some("gateway".to_owned());
                let front =
                    serving_worker_to_tls_showing(upstream, protocol, tls.clone(), &ours).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(
                    answer.starts_with("HTTP/1.1 200 OK\r\n"),
                    "{protocol:?}: {answer}"
                );

                tls.client_certificate = None;
                let front = serving_worker_to_tls(upstream, protocol, tls).await;
                let answer = h1_answer(front, CLOSING_GET).await;
                assert!(
                    answer.starts_with("HTTP/1.1 502 "),
                    "{protocol:?}: {answer}"
                );
            }
        })
        .await;
}

/// An upstream reached over TLS is spoken to in HTTP/1.1 or in HTTP/2, as configured,
/// once its certificate is found to be the named server's and vouched for by a trusted
/// authority. In HTTP/2 its requests say `https`, which it answers only for.
#[tokio::test]
async fn an_upstream_is_reached_over_tls_in_either_protocol() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = tls_upstream(&certificate, Agrees::Either).await;
                let front = serving_worker_to_tls(
                    upstream,
                    protocol,
                    trusting("backend.test", &certificate),
                )
                .await;
                for _ in 0..2 {
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 200 OK\r\n"),
                        "{protocol:?}: {answer}"
                    );
                    assert!(answer.contains("ok"), "{answer}");
                }
            }
        })
        .await;
}

/// An HTTP/2 upstream reached over TLS is probed as it is sent requests, asking for
/// `https`: its HTTP and gRPC health checks both pass one that answers only for that.
#[tokio::test]
async fn an_http2_upstream_over_tls_is_probed_for_https() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let upstream = tls_upstream(&certificate, Agrees::Either).await;
            let probes = [
                healthz(),
                edgerush_config::Probe::Grpc {
                    service: String::new(),
                },
            ];
            for probe in probes {
                let mut config = everything_config(upstream);
                let up = config.upstreams.get_mut("up").unwrap();
                up.protocol = UpstreamProtocol::Http2;
                up.tls = Some(trusting("backend.test", &certificate));
                up.health_check = Some(every_second_by(probe.clone()));
                let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
                let destination = proxy.checked().next().unwrap();
                let check = destination.health_check().unwrap();
                assert!(
                    crate::health::probe::passes(&destination, check).await,
                    "{probe:?}"
                );
            }
        })
        .await;
}

/// An endpoint whose certificate no trusted authority vouches for, or that is not the
/// named server's, is not spoken to: 502.
#[tokio::test]
async fn an_upstream_that_is_not_who_it_should_be_is_not_spoken_to() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let stranger = crate::tls::testing::certificate(&["backend.test"]);
            for protocol in [UpstreamProtocol::Http1, UpstreamProtocol::Http2] {
                let upstream = tls_upstream(&certificate, Agrees::Either).await;
                for tls in [
                    trusting("backend.test", &stranger),
                    trusting("other.test", &certificate),
                ] {
                    let front = serving_worker_to_tls(upstream, protocol, tls).await;
                    let answer = h1_answer(front, CLOSING_GET).await;
                    assert!(
                        answer.starts_with("HTTP/1.1 502 "),
                        "{protocol:?}: {answer}"
                    );
                }
            }
        })
        .await;
}

/// An HTTP/2 upstream that does not agree on `h2` in the handshake is a failed
/// connection, never one spoken to in HTTP/1.1 instead.
#[tokio::test]
async fn an_http2_upstream_that_will_not_agree_on_h2_is_not_spoken_to_in_http1() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let certificate = crate::tls::testing::certificate(&["backend.test"]);
            let upstream = tls_upstream(&certificate, Agrees::NothingButSpeaksH2).await;
            let front = serving_worker_to_tls(
                upstream,
                UpstreamProtocol::Http2,
                trusting("backend.test", &certificate),
            )
            .await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502 "), "{answer}");
        })
        .await;
}

/// A config whose upstream TLS is the config before's keeps what connections were
/// secured with, and its destinations with it; TLS that changed is a new destination,
/// whose connections are not the old one's; and an authority that is not a certificate
/// is refused, with the upstream named.
#[test]
fn a_reload_keeps_the_tls_to_an_upstream_that_has_not_changed() {
    let certificate = crate::tls::testing::certificate(&["backend.test"]);
    let secured = |tls: edgerush_config::UpstreamTls| {
        let mut config = everything_config("127.0.0.1:9".parse().unwrap());
        config.upstreams.get_mut("up").unwrap().tls = Some(tls);
        compile(&config).unwrap()
    };
    let proxy = Proxy::new(
        secured(trusting("backend.test", &certificate)),
        NonZeroUsize::MIN,
    )
    .unwrap();
    let identity = |proxy: &Proxy| Arc::clone(proxy.current.load().destinations.at(0, 0).unwrap());
    let before = identity(&proxy);
    let connector = Arc::clone(before.secure().unwrap());

    proxy
        .reload(secured(trusting("backend.test", &certificate)))
        .unwrap();
    assert!(Arc::ptr_eq(&before, &identity(&proxy)));
    let kept = Arc::clone(proxy.current.load().secure[0].as_ref().unwrap());
    assert!(
        Arc::ptr_eq(&connector, &kept),
        "an unchanged TLS was built again"
    );

    proxy
        .reload(secured(trusting("other.test", &certificate)))
        .unwrap();
    let after = identity(&proxy);
    assert_ne!(after.key(), before.key());
    assert!(before.is_retired());

    let unusable = edgerush_config::UpstreamTls {
        server_name: "backend.test".to_owned(),
        authorities: vec![
            "-----BEGIN CERTIFICATE-----\nnot base64\n-----END CERTIFICATE-----\n".to_owned(),
        ],
        client_certificate: None,
    };
    let refused = proxy.reload(secured(unusable));
    assert!(
        matches!(&refused, Err(ProxyError::UpstreamTls { upstream, .. }) if upstream == "up"),
        "{refused:?}"
    );
}
