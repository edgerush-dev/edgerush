//! The HTTP/2 pieces joined to EdgeRush's own HTTP/1 exchange, as step 2 will join them
//! ([15 §5](../../../../../docs/15-http2-and-grpc.md)): an upload's credit follows the
//! exchange writing it upstream, and an upstream's answer goes out within what the client
//! grants, paid for while h2 holds it.

use crate::downstream::h2::body::IncomingH2;
use crate::downstream::h2::testing::{locally, serving, wire, within};
use crate::downstream::h2::writer::{Outgoing, Responder, send_body};
use crate::h2_peer::{self, Peer, flag, kind, setting};
use crate::storage::{LIMIT, Storage};
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Blocks, Sizes};
use crate::upstream::h1::codec::Sending;
use crate::upstream::h1::exchange::{Exchange, H1Body};
use http::{HeaderMap, Method, Response};
use std::cell::RefCell;
use std::rc::Rc;
use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream};

fn blocks() -> Rc<RefCell<Blocks>> {
    Rc::new(RefCell::new(Blocks::new(
        Sizes::default(),
        Storage::new(LIMIT),
    )))
}

fn host() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert("host", "up.test".parse().unwrap());
    headers
}

/// A server whose client is the scripted peer, having asked for `settings`.
async fn scripted(
    settings: &[(u16, u32)],
) -> (
    Peer<DuplexStream>,
    tokio::sync::mpsc::UnboundedReceiver<crate::downstream::h2::testing::Accepted>,
) {
    let (near, far) = wire();
    let mut client = Peer::open_as_client(far, settings).await;
    let mut builder = ::h2::server::Builder::new();
    builder.max_send_buffer_size(65_536);
    let server = within(builder.handshake::<_, Outgoing>(near))
        .await
        .unwrap();
    client
        .until(|f| f.kind == kind::SETTINGS && !f.has(flag::ACK))
        .await;
    client.send(&h2_peer::settings_ack()).await;
    (client, serving(server))
}

/// Reads what the upstream is sent until `mark`, and returns all of it.
async fn read_until(upstream: &mut DuplexStream, mark: &[u8]) -> Vec<u8> {
    let mut seen = Vec::new();
    let mut chunk = [0u8; 4096];
    while !seen.ends_with(mark) {
        let read = within(upstream.read(&mut chunk)).await.unwrap();
        assert!(read > 0, "the exchange closed before {mark:?}");
        seen.extend_from_slice(&chunk[..read]);
    }
    seen
}

/// An upload's credit goes back only as the exchange writes it upstream: behind an
/// upstream that takes nothing, the client can send its window and no more.
#[test]
fn an_upload_s_credit_follows_the_exchange_writing_it_upstream() {
    locally(async {
        let (mut client, mut accepted) = scripted(&[]).await;
        client
            .send(&h2_peer::headers(1, h2_peer::request("POST", "/"), false))
            .await;
        // The whole of the default window, 65,535.
        for size in [16_384, 16_384, 16_384, 16_383] {
            client
                .send(&h2_peer::data(1, &vec![9u8; size], false))
                .await;
        }
        let (request, _respond) = within(accepted.recv()).await.unwrap();
        let body = IncomingH2::new(request.into_body());

        // An upstream with 4 KiB of room, which reads nothing yet.
        let (ours, mut upstream) = tokio::io::duplex(4096);
        let exchange = tokio::task::spawn_local(async move {
            Exchange::new(ours, blocks())
                .send(
                    &Method::POST,
                    &"/".parse().unwrap(),
                    &host(),
                    &[],
                    Sending::Chunked,
                    body,
                    &H1Limits::default(),
                )
                .await
                .map(|(answer, _rest)| answer.head.status)
        });
        let stream_credit = |frames: Vec<h2_peer::Frame>| {
            frames
                .iter()
                .filter(|f| f.kind == kind::WINDOW_UPDATE && f.stream == 1)
                .count()
        };
        assert_eq!(
            stream_credit(client.settled().await),
            0,
            "credit before it went on"
        );

        // The upstream reads, the frames go on, and credit comes back for them.
        let reading = tokio::task::spawn_local(async move {
            let seen = read_until(&mut upstream, b"0\r\n\r\n").await;
            upstream
                .write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 0\r\n\r\n")
                .await
                .unwrap();
            (seen, upstream)
        });
        client
            .until(|f| f.kind == kind::WINDOW_UPDATE && f.stream == 1)
            .await;
        client.send(&h2_peer::data(1, b"end", true)).await;
        let status = within(exchange).await.unwrap().unwrap();
        assert_eq!(status, 200);
        let (seen, _upstream) = within(reading).await.unwrap();
        assert!(
            seen.len() > 65_538,
            "the upload did not all go: {}",
            seen.len()
        );
    });
}

/// An upstream's answer, read by the exchange, goes out within what the client grants,
/// whole once it grants more, and every charge on what h2 held is paid back. The bound on
/// what h2 holds is `writer`'s own test, on a connection that cannot drain.
#[test]
fn an_upstream_answer_goes_out_within_the_client_s_window_and_is_paid_for() {
    locally(async {
        const LENGTH: usize = 256 * 1024;
        let (ours, mut upstream) = tokio::io::duplex(64 * 1024);
        tokio::task::spawn_local(async move {
            read_until(&mut upstream, b"\r\n\r\n").await;
            let head = format!("HTTP/1.1 200 OK\r\ncontent-length: {LENGTH}\r\n\r\n");
            upstream.write_all(head.as_bytes()).await.unwrap();
            upstream.write_all(&vec![5u8; LENGTH]).await.unwrap();
            upstream
        });
        let limits = H1Limits::default();
        let (answer, rest) = within(Exchange::new(ours, blocks()).send(
            &Method::GET,
            &"/".parse().unwrap(),
            &host(),
            &[],
            Sending::None,
            http_body_util::Empty::new(),
            &limits,
        ))
        .await
        .unwrap();
        let body = H1Body::new(
            rest,
            answer.delivery.framing,
            answer.delivery.persistent,
            answer.nominated,
            limits,
        );

        let storage = Storage::new(1 << 20);
        let (mut client, mut accepted) = scripted(&[(setting::INITIAL_WINDOW_SIZE, 16_384)]).await;
        client
            .send(&h2_peer::headers(1, h2_peer::request("GET", "/"), true))
            .await;
        let (_request, respond) = within(accepted.recv()).await.unwrap();
        let mut stream = Responder::new(respond)
            .final_head(Response::new(()), false)
            .unwrap();
        let paid = Rc::clone(&storage);
        let sending =
            tokio::task::spawn_local(async move { send_body(&mut stream, body, &paid).await });

        let delivered = |frames: &[h2_peer::Frame]| -> usize {
            frames
                .iter()
                .filter(|f| f.kind == kind::DATA && f.stream == 1)
                .map(|f| f.payload.len())
                .sum()
        };
        assert_eq!(
            delivered(&client.settled().await),
            16_384,
            "past the client's window"
        );
        assert!(!sending.is_finished());

        client.send(&h2_peer::window_update(0, 1 << 20)).await;
        client.send(&h2_peer::window_update(1, 1 << 20)).await;
        let mut received = 16_384;
        while received < LENGTH {
            let frame = client.next().await;
            if frame.kind == kind::DATA && frame.stream == 1 {
                received += frame.payload.len();
                assert!(storage.used() <= 65_536 + 16_384, "{}", storage.used());
            }
        }
        within(sending).await.unwrap().unwrap();
        assert_eq!(received, LENGTH);
        assert_eq!(storage.used(), 0, "a charge outlived its piece");
    });
}
