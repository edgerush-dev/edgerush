//! A pod's client on real sockets: what becomes of a connection that never answers.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the tests fail them the way the tests would"
)]

use edgerush_cluster::Message;
use edgerush_cluster_synchronous::Value;
use edgerush_ratelimit_service::cluster::link::Dial;
use edgerush_ratelimit_service::cluster::pod::{StorePod, StoreServer, ask};
use edgerush_ratelimit_service::grpc::Client;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

/// A connection to a pod that is open but never answers is let go of, and the next call
/// connects afresh (#127): the client pings each connection and drops one whose ping goes
/// unanswered, rather than keeping it, and every call on it timing out, until it fails of
/// itself — which an open connection need never do.
#[tokio::test]
async fn a_client_lets_go_of_a_connection_that_never_answers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let at = listener.local_addr().unwrap();
    let pod = StorePod::spawn(
        1,
        at.to_string(),
        edgerush_cluster::Config::defaults(),
        Dial::Plain,
        Vec::new(),
    );
    let server = StoreServer::new(pod);
    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .serve_with_incoming(server, TcpIncoming::from(listener))
            .await
            .unwrap();
    });
    // In front of it, a relay that holds the first connection open, read by nothing, for
    // as long as the test runs, and relays every later one.
    let relay = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = relay.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let (_held, _) = relay.accept().await.unwrap();
        while let Ok((mut client, _)) = relay.accept().await {
            tokio::spawn(async move {
                let mut server = tokio::net::TcpStream::connect(at).await.unwrap();
                let _ended = tokio::io::copy_bidirectional(&mut client, &mut server).await;
            });
        }
    });
    let client = Client::new(&address).unwrap();
    let probe = || Message::<Value>::Probe { life: 0, round: 0 };
    let deadline = Instant::now() + Duration::from_secs(8);
    while ask::<Value>(&client, probe()).await.is_err() {
        assert!(
            Instant::now() < deadline,
            "every call went on the connection that never answers"
        );
    }
}
