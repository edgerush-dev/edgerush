//! What an upstream is told: who the client is, the request's ID, a rewritten path.

use super::*;

/// Over HTTP/1.1 and HTTP/2 alike, the upstream is told who the client is — the peer of
/// the connection, which no proxy in front vouched for — and not what the client said
/// of it; the scheme and host it asked for; and that it came through the gateway.
#[tokio::test]
async fn the_upstream_is_told_who_the_client_is_whatever_it_spoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let (front, _worker) = serving_config(&forwarding_config(upstream, &[])).await;

            let answer = h1_answer(
                front,
                b"GET /a HTTP/1.1\r\nhost: shop.example.com:8080\r\nconnection: close\r\n\
                      x-forwarded-for: 10.9.9.9\r\nx-forwarded-proto: https\r\n\
                      forwarded: for=10.9.9.9\r\nx-real-ip: 10.9.9.9\r\nvia: 1.0 cdn\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[0].clone();
            for line in [
                "\r\nx-forwarded-for: 127.0.0.1\r\n",
                "\r\nx-forwarded-proto: http\r\n",
                "\r\nx-forwarded-host: shop.example.com:8080\r\n",
                "\r\nvia: 1.0 cdn\r\n",
                "\r\nvia: 1.1 edgerush\r\n",
            ] {
                assert!(head.contains(line), "{line:?} in {head}");
            }
            for gone in ["10.9.9.9", "forwarded:", "x-real-ip", "https"] {
                assert!(!head.contains(gone), "{gone:?} in {head}");
            }

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://shop.example.com/b")
                .header("x-forwarded-for", "10.9.9.9")
                .body(())
                .unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            assert_eq!(within(answer).await.unwrap().status(), StatusCode::OK);
            let head = heads.borrow()[1].clone();
            for line in [
                "\r\nx-forwarded-for: 127.0.0.1\r\n",
                "\r\nx-forwarded-proto: http\r\n",
                "\r\nx-forwarded-host: shop.example.com\r\n",
                "\r\nvia: 2 edgerush\r\n",
            ] {
                assert!(head.contains(line), "{line:?} in {head}");
            }
            assert!(!head.contains("10.9.9.9"), "{head}");
        })
        .await;
}

/// A proxy in front that the listener trusts names the client, and says what scheme and
/// host the client asked for.
#[tokio::test]
async fn a_trusted_proxy_in_front_names_the_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let config = forwarding_config(upstream, &["127.0.0.0/8"]);
            let (front, _worker) = serving_config(&config).await;

            let answer = h1_answer(
                front,
                b"GET /a HTTP/1.1\r\nhost: internal:8080\r\nconnection: close\r\n\
                      x-forwarded-for: 1.2.3.4, 198.51.100.9\r\nx-forwarded-proto: https\r\n\
                      x-forwarded-host: shop.example.com\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let head = heads.borrow()[0].clone();
            for line in [
                "\r\nx-forwarded-for: 198.51.100.9\r\n",
                "\r\nx-forwarded-proto: https\r\n",
                "\r\nx-forwarded-host: shop.example.com\r\n",
            ] {
                assert!(head.contains(line), "{line:?} in {head}");
            }
            assert!(!head.contains("1.2.3.4"), "{head}");
        })
        .await;
}

/// Over HTTP/3 the client is the peer of the path its requests came on, and the scheme
/// `https`.
#[tokio::test]
async fn the_upstream_is_told_who_an_http3_client_is() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (upstream, heads) = recording_upstream("200 OK");
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let mut config = h3_config(upstream, http3);
            config.listeners.get_mut("web").unwrap().forwarding = forwarding_config(upstream, &[])
                .listeners["web"]
                .forwarding
                .clone();
            let (front, _) = serving_h3(&config).await;
            let mut client = Client::connect(front, "a.test").await;

            let mut fields = get("a.test", "/c");
            fields.push(("x-forwarded-for", "10.9.9.9"));
            let id = client.request(&fields, true);
            let answer = client.answer(id).await;
            assert_eq!(answer.final_status(), Some("200"));
            let head = heads.borrow()[0].clone();
            for line in [
                "\r\nx-forwarded-for: 127.0.0.1\r\n",
                "\r\nx-forwarded-proto: https\r\n",
                "\r\nx-forwarded-host: a.test\r\n",
                "\r\nvia: 3 edgerush\r\n",
            ] {
                assert!(head.contains(line), "{line:?} in {head}");
            }
            assert!(!head.contains("10.9.9.9"), "{head}");
        })
        .await;
}

/// Over HTTP/3 the upstream is told only an address the client has shown it holds: a
/// request that arrives from an address that never answers path validation — a source the
/// client forged, or a packet an on-path party copied (RFC 9000 §9.3.2) — is not told as
/// coming from there.
#[tokio::test]
async fn an_http3_request_from_an_address_never_validated_is_not_told_as_its_client() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (upstream, heads) = recording_upstream("200 OK");
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, _) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            // First from the address its handshake proved.
            let answer = client.get("a.test", "/first").await;
            assert_eq!(answer.final_status(), Some("200"));
            assert!(heads.borrow()[0].contains("\r\nx-forwarded-for: 127.0.0.1\r\n"));

            // The next request leaves from another address, which never answers anything:
            // the client takes its own socket up again at once.
            let elsewhere = UdpSocket::bind("127.0.0.2:0").await.unwrap();
            let own = client.swap_socket(elsewhere);
            let _second = client.request(&get("a.test", "/second"), true);
            client.flush().await;
            let elsewhere = client.swap_socket(own);
            // The client goes on from its own address, so whatever the gateway would wait
            // for, it has; bounded, since refusing the request is right as well.
            let until = Instant::now() + Duration::from_secs(3);
            while heads.borrow().len() < 2 && Instant::now() < until {
                client.for_a_while(Duration::from_millis(50)).await;
            }
            if let Some(head) = heads.borrow().get(1) {
                assert!(
                    !head.contains("127.0.0.2"),
                    "an address that never proved itself was told as the client: {head}"
                );
            }
            drop(elsewhere);
        })
        .await;
}

/// Over HTTP/3 a request is taken as a trusted proxy's only from an address that has shown
/// it holds it: a datagram sent from a trusted proxy's address, which needs no answer, does
/// not have its forwarding headers believed nor its trusted-only headers kept.
#[tokio::test]
async fn an_http3_request_from_an_unproven_trusted_address_is_not_believed() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (upstream, heads) = recording_upstream("200 OK");
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let mut config = h3_config(upstream, http3);
            config.listeners.get_mut("web").unwrap().forwarding =
                forwarding_config(upstream, &["127.0.0.2/32"]).listeners["web"]
                    .forwarding
                    .clone();
            let (front, _) = serving_h3(&config).await;
            let mut client = Client::connect(front, "a.test").await;
            // From the address its handshake proved, which is no trusted proxy.
            let answer = client.get("a.test", "/first").await;
            assert_eq!(answer.final_status(), Some("200"));
            assert!(heads.borrow()[0].contains("\r\nx-forwarded-for: 127.0.0.1\r\n"));

            // The next request leaves from the trusted proxy's address, which never answers:
            // the client takes its own socket up again at once.
            let elsewhere = UdpSocket::bind("127.0.0.2:0").await.unwrap();
            let own = client.swap_socket(elsewhere);
            let mut fields = get("a.test", "/second");
            fields.push(("x-forwarded-for", "203.0.113.7"));
            fields.push(("x-real-ip", "203.0.113.7"));
            let _second = client.request(&fields, true);
            client.flush().await;
            let elsewhere = client.swap_socket(own);
            // Bounded: refusing the request would be right as well.
            let until = Instant::now() + Duration::from_secs(3);
            while heads.borrow().len() < 2 && Instant::now() < until {
                client.for_a_while(Duration::from_millis(50)).await;
            }
            if let Some(head) = heads.borrow().get(1) {
                assert!(
                    !head.contains("203.0.113.7"),
                    "an unproven address was trusted as a proxy: {head}"
                );
            }
            drop(elsewhere);
        })
        .await;
}

/// Over HTTP/3 a client that moves to another address, and answers for it there, is told as
/// coming from it once its new path is validated; until then its requests are the last
/// validated address's.
#[tokio::test]
async fn an_http3_client_that_moves_is_told_from_its_new_address_once_validated() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::Client;
            let (upstream, heads) = recording_upstream("200 OK");
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, _) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;
            let answer = client.get("a.test", "/first").await;
            assert_eq!(answer.final_status(), Some("200"));
            assert!(heads.borrow()[0].contains("\r\nx-forwarded-for: 127.0.0.1\r\n"));

            // From here on the client is at another address, and hears the gateway there.
            let elsewhere = UdpSocket::bind("127.0.0.2:0").await.unwrap();
            let _own = client.swap_socket(elsewhere);
            // Found in the turn its datagram is read in, before any answer to path
            // validation can have come back.
            let answer = client.get("a.test", "/moving").await;
            assert_eq!(answer.final_status(), Some("200"));
            let head = heads.borrow()[1].clone();
            assert!(
                head.contains("\r\nx-forwarded-for: 127.0.0.1\r\n"),
                "{head}"
            );

            // Validation needs the client's answer to the gateway's challenge, which goes as
            // the client goes on hearing the gateway. Bounded: each request is answered, and
            // the loop is given 5 s.
            let until = Instant::now() + Duration::from_secs(5);
            loop {
                let answer = client.get("a.test", "/moved").await;
                assert_eq!(answer.final_status(), Some("200"));
                let head = heads.borrow().last().unwrap().clone();
                if head.contains("\r\nx-forwarded-for: 127.0.0.2\r\n") {
                    break;
                }
                assert!(
                    head.contains("\r\nx-forwarded-for: 127.0.0.1\r\n"),
                    "{head}"
                );
                assert!(
                    Instant::now() < until,
                    "the new address was never told, though it answered: {head}"
                );
                client.for_a_while(Duration::from_millis(50)).await;
            }
        })
        .await;
}

/// Every `x-request-id` in `text`, a head or answer in lower case, as the value it holds.
fn request_ids(text: &str) -> Vec<String> {
    text.split("\r\n")
        .filter_map(|line| line.strip_prefix("x-request-id: "))
        .map(str::to_owned)
        .collect()
}

/// Whether `id` is a request ID as the gateway makes them: a UUIDv7 in lower case.
fn is_uuid7(id: &str) -> bool {
    let bytes = id.as_bytes();
    bytes.len() == 36
        && bytes.iter().enumerate().all(|(at, &byte)| match at {
            8 | 13 | 18 | 23 => byte == b'-',
            _ => byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte),
        })
        && bytes[14] == b'7'
        && b"89ab".contains(&bytes[19])
}

/// A listener that generates IDs gives every request its own, in place of whatever the
/// client sent, and tells the client the same one, in place of whatever the upstream
/// answered with (08 §3); over HTTP/1.1 and HTTP/2 alike.
#[tokio::test]
async fn a_request_and_its_client_are_told_one_id_whatever_it_spoke() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK\r\nx-request-id: theirs");
            let (front, _worker) = serving_config(&everything_config(upstream)).await;

            let answer = h1_answer(
                front,
                b"GET /a HTTP/1.1\r\nhost: shop.example.com\r\nconnection: close\r\n\
                      x-request-id: mine\r\nX-Request-ID: again\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let told = request_ids(&answer);
            assert_eq!(told.len(), 1, "{answer}");
            assert!(is_uuid7(&told[0]), "{answer}");
            let head = heads.borrow()[0].clone();
            assert_eq!(request_ids(&head), told, "{head}");

            // `theirs` is the second upstream answer's only as the first one's.
            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let request = Request::get("http://shop.example.com/b")
                .header("x-request-id", "mine")
                .body(())
                .unwrap();
            let (answer, _) = send.send_request(request, true).unwrap();
            let answer = within(answer).await.unwrap();
            assert_eq!(answer.status(), StatusCode::OK);
            let values: Vec<_> = answer.headers().get_all("x-request-id").iter().collect();
            assert_eq!(values.len(), 1, "{:?}", answer.headers());
            let id = values[0].to_str().unwrap();
            assert!(is_uuid7(id), "{id}");
            assert_ne!(id, told[0]);
            let head = heads.borrow()[1].clone();
            assert_eq!(request_ids(&head), [id], "{head}");
        })
        .await;
}

/// The gateway's own answers carry an ID too, each its own: a 404 for no rule, a 400 for
/// a host that is none, a 502 for an upstream that cannot be reached, a redirect, and a
/// gRPC call answered with a status (08 §3).
#[tokio::test]
async fn the_gateways_own_answers_are_told_their_ids() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (_held, refusing) = refusing();
            let mut config = everything_config(refusing);
            config.routes[0].rules = [
                "matches: [{ path: { prefix: /old } }]\n\
                     redirect: { status: 301, path: { replace_prefix: /new }, query: keep }",
                "matches: [{ path: { prefix: /up } }]\n\
                     forward: { backends: [{ upstream: up, weight: 1 }] }",
            ]
            .into_iter()
            .map(|rule| serde_saphyr::from_str(rule).unwrap())
            .collect();
            let (front, _worker) = serving_config(&config).await;

            let mut ids = Vec::new();
            for (request, status) in [
                (
                    &b"GET /none HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n"[..],
                    "404",
                ),
                (
                    b"GET /up HTTP/1.1\r\nhost: a!test\r\nconnection: close\r\n\r\n",
                    "400",
                ),
                (
                    b"GET /up HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
                    "502",
                ),
                (
                    b"GET /old HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\r\n",
                    "301",
                ),
            ] {
                let answer = h1_answer(front, request).await;
                assert!(
                    answer.starts_with(&format!("HTTP/1.1 {status} ")),
                    "{answer}"
                );
                let told = request_ids(&answer);
                assert_eq!(told.len(), 1, "{answer}");
                assert!(is_uuid7(&told[0]), "{answer}");
                ids.extend(told);
            }

            let mut send = h2_library_client(front, &::h2::client::Builder::new()).await;
            let (answer, _) = send.send_request(grpc_call("/none", None), true).unwrap();
            let answer = within(answer).await.unwrap();
            assert!(answer.headers().contains_key("grpc-status"));
            let id = answer.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .to_owned();
            assert!(is_uuid7(&id), "{id}");
            ids.push(id);

            ids.sort();
            ids.dedup();
            assert_eq!(ids.len(), 5, "{ids:?}");
        })
        .await;
}

/// A listener that passes IDs through leaves `X-Request-ID` as it is both ways: the
/// client's reaches the upstream, the upstream's reaches the client, and the gateway's
/// own answers carry none.
#[tokio::test]
async fn a_listener_that_passes_ids_leaves_them_as_they_are() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK\r\nx-request-id: theirs");
            let mut config = everything_config(upstream);
            config.listeners.get_mut("web").unwrap().request_id =
                Some(edgerush_config::RequestId::Pass);
            config.routes[0].rules[0].matches[0] =
                serde_saphyr::from_str("path: { prefix: /up }").unwrap();
            let (front, _worker) = serving_config(&config).await;

            let answer = h1_answer(
                front,
                b"GET /up HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\
                      x-request-id: mine\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            assert_eq!(request_ids(&answer), ["theirs"], "{answer}");
            let head = heads.borrow()[0].clone();
            assert_eq!(request_ids(&head), ["mine"], "{head}");

            let answer = h1_answer(
                front,
                b"GET /none HTTP/1.1\r\nhost: a.test\r\nconnection: close\r\n\
                      x-request-id: mine\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 404 "), "{answer}");
            assert!(request_ids(&answer).is_empty(), "{answer}");
        })
        .await;
}

/// Over HTTP/3 as over TCP: the request and its client are told one ID, the gateway's.
#[tokio::test]
async fn an_http3_request_and_its_client_are_told_one_id() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::downstream::h3::testing::{Client, get};
            let (upstream, heads) = recording_upstream("200 OK\r\nx-request-id: theirs");
            let http3 = edgerush_config::Http3 {
                alt_svc_max_age: 60,
                force_retry: false,
            };
            let (front, _) = serving_h3(&h3_config(upstream, http3)).await;
            let mut client = Client::connect(front, "a.test").await;

            let mut fields = get("a.test", "/c");
            fields.push(("x-request-id", "mine"));
            let stream = client.request(&fields, true);
            let answer = client.answer(stream).await;
            assert_eq!(answer.final_status(), Some("200"));
            let told: Vec<String> = answer
                .heads
                .last()
                .unwrap()
                .iter()
                .filter(|(name, _)| name == "x-request-id")
                .map(|(_, value)| value.clone())
                .collect();
            assert_eq!(told.len(), 1, "{:?}", answer.heads);
            assert!(is_uuid7(&told[0]), "{told:?}");
            let head = heads.borrow()[0].clone();
            assert_eq!(request_ids(&head), told, "{head}");
        })
        .await;
}

/// `everything_config`, its one rule rewriting to `one.example.org` and under `/api`.
fn rewriting_config(upstream: SocketAddr) -> Config {
    let mut config = everything_config(upstream);
    let rewrite = "{ type: url_rewrite, host: one.example.org, path: { replace_prefix: /api } }";
    config.routes[0].rules[0]
        .filters
        .push(serde_saphyr::from_str(rewrite).unwrap());
    config
}

/// An HTTP/2 upstream is sent the rewritten path, and the rewritten host as its
/// `:authority` (18 §4).
#[tokio::test]
async fn an_http2_upstream_is_sent_the_rewritten_path_and_host() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, seen, _gate) = h2_upstream(UpstreamH2::default()).await;
            let mut config = rewriting_config(upstream);
            config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
            let (front, _worker) = serving_config(&config).await;

            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let requests = seen.requests.borrow();
            let (request, _) = &requests[0];
            assert_eq!(request.uri().authority().unwrap(), "one.example.org");
            assert_eq!(request.uri().path_and_query().unwrap(), "/api/a/b?c=d");
            assert!(request.headers().get("host").is_none());
        })
        .await;
}

/// An HTTP/1.1 upstream is sent the rewritten path and `Host`, and so on every try of a
/// request sent again: the rewrite is made once, before the first (18 §4).
#[tokio::test]
async fn every_try_is_sent_the_rewritten_path_and_host() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("503 Service Unavailable");
            let mut config = rewriting_config(upstream);
            config.routes[0].rules[0]
                .forward
                .as_mut()
                .expect("the rule forwards")
                .retry = Some(retrying(1, &[503], &[], 1));
            let (front, _worker) = serving_config(&config).await;

            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
            let heads = heads.borrow();
            assert_eq!(heads.len(), 2, "{heads:?}");
            for head in heads.iter() {
                assert!(head.starts_with("get /api/a/b?c=d http/1.1\r\n"), "{head}");
                assert!(head.contains("\r\nhost: one.example.org\r\n"), "{head}");
                // The host the client asked for is where the upstream is told of it, and
                // nowhere else.
                assert!(
                    head.contains("\r\nx-forwarded-host: shop.example.com\r\n"),
                    "{head}"
                );
                assert_eq!(head.matches("shop.example.com").count(), 1, "{head}");
            }
        })
        .await;
}

/// An HTTP/2 upstream's answer with a field value that starts or ends with whitespace is
/// malformed (RFC 9113 §8.2.1), and a client of it must not take it (§8.1.1): it is
/// answered as an upstream that failed, never passed on.
#[tokio::test]
async fn an_http2_answer_with_whitespace_at_either_end_of_a_field_value_is_not_passed_on() {
    use crate::h2_peer::{self, Peer, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = socket.local_addr().unwrap();
            let _answering = tokio::task::spawn_local(async move {
                let (stream, _) = socket.accept().await.unwrap();
                let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                peer.send(&h2_peer::settings_ack()).await;
                let (request, _) = peer.until(|frame| frame.kind == kind::HEADERS).await;
                let block = h2_peer::block(&[(":status", "200"), ("x-env", " canary")]);
                peer.send(&h2_peer::headers(request.stream, block, true))
                    .await;
                std::future::pending::<()>().await;
            });
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(answer.starts_with("HTTP/1.1 502"), "{answer}");
            assert!(!answer.contains("canary"), "{answer}");
        })
        .await;
}

/// An HTTP/2 `:path` holds the target's path and query and nothing more (RFC 9113
/// §8.3.1): one carrying a fragment is not a valid value, and the request's stream is reset
/// with PROTOCOL_ERROR, as HTTP/1 refuses a target with one — never routed and forwarded cut
/// short (review A12-01, the HTTP/2 side of A05-02).
#[tokio::test]
async fn an_http2_path_with_a_fragment_is_not_forwarded_cut_short() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            use crate::h2_peer::{code, kind};
            let (upstream, heads) = recording_upstream("200 OK");
            let (front, _worker) = serving_config(&forwarding_config(upstream, &[])).await;
            let mut peer = h2_client(front).await;
            for (stream, path) in [(1, "/a#b"), (3, "/a?q#b"), (5, "/#")] {
                h2_get(&mut peer, stream, path).await;
                // Bounded: whatever comes first on the stream, an answer or a reset.
                let (frame, _) = within(peer.until(|frame| {
                    frame.stream == stream
                        && (frame.kind == kind::HEADERS || frame.kind == kind::RST_STREAM)
                }))
                .await;
                let forwarded = heads.borrow().clone();
                assert!(
                    forwarded.is_empty(),
                    "an HTTP/2 :path {path:?} was forwarded cut short (the client got \
                     {frame:?}): {forwarded:?}"
                );
                assert!(
                    frame.kind == kind::RST_STREAM && frame.reset() == code::PROTOCOL_ERROR,
                    "{path:?}: {frame:?}"
                );
            }
        })
        .await;
}

/// Control for A12-01: HTTP/1 refuses a target with a fragment (RFC 9112 §3.2) and
/// forwards nothing.
#[tokio::test]
async fn an_http1_target_with_a_fragment_is_refused() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let (upstream, heads) = recording_upstream("200 OK");
            let (front, _worker) = serving_config(&forwarding_config(upstream, &[])).await;
            let answer = h1_answer(
                front,
                b"GET /a#b HTTP/1.1\r\nhost: example.test\r\nconnection: close\r\n\r\n",
            )
            .await;
            assert!(answer.starts_with("HTTP/1.1 400"), "{answer}");
            assert!(heads.borrow().is_empty(), "{:?}", heads.borrow());
        })
        .await;
}

/// An HTTP/2 upstream's answer whose head has no `:status` is malformed (RFC 9113 §8.3.2,
/// "Clients MUST NOT accept a malformed response"): it is answered as an upstream that
/// failed, never passed on as a 200 (review A04-03).
#[tokio::test]
async fn an_http2_answer_without_a_status_is_not_taken_for_a_200() {
    use crate::h2_peer::{self, Peer, kind};
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let upstream = socket.local_addr().unwrap();
            let _answering = tokio::task::spawn_local(async move {
                let (stream, _) = socket.accept().await.unwrap();
                let (mut peer, _) = Peer::accept_as_server(stream, &[]).await;
                peer.send(&h2_peer::settings_ack()).await;
                let (request, _) = peer.until(|frame| frame.kind == kind::HEADERS).await;
                // A head with a field and no `:status`, ending the stream.
                let block = h2_peer::block(&[("content-type", "text/plain")]);
                peer.send(&h2_peer::headers(request.stream, block, true))
                    .await;
                std::future::pending::<()>().await;
            });
            let (front, _worker) = serving_worker_to_h2(upstream, H1Limits::default()).await;
            let answer = h1_answer(front, CLOSING_GET).await;
            assert!(
                answer.starts_with("HTTP/1.1 502"),
                "an answer with no :status was passed on as {answer:?}"
            );
            assert!(!answer.contains("text/plain"), "{answer}");
        })
        .await;
}
