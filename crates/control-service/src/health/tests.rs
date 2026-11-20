use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Long enough for anything a test waits on to happen on a loaded machine.
const WAIT: Duration = Duration::from_secs(10);

/// The readiness serving on a port of its own.
struct Health {
    address: String,
    /// Whether the replica is ready: not, until a test says so.
    ready: watch::Sender<bool>,
    stop: Option<oneshot::Sender<()>>,
    served: JoinHandle<()>,
}

impl Health {
    async fn start(deadline: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (ready, asked) = watch::channel(false);
        let (stop, stopped) = oneshot::channel();
        let served = tokio::spawn(serve(listener, asked, deadline, async move {
            let _ = stopped.await;
        }));
        Self {
            address,
            ready,
            stop: Some(stop),
            served,
        }
    }

    /// The whole answer to `request`, asked on a connection of its own as a probe asks,
    /// without asking for it to be closed: it is, once answered.
    async fn ask(&self, method: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(&self.address).await.unwrap();
        let request = format!("{method} {path} HTTP/1.1\r\nhost: health.test\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        tokio::time::timeout(WAIT, stream.read_to_string(&mut answer))
            .await
            .expect("the connection was not closed once answered")
            .unwrap();
        answer
    }
}

/// Whether `stream` is closed, by the server, within `within`: nothing more to read.
async fn closed_within(stream: &mut TcpStream, within: Duration) -> bool {
    let mut rest = Vec::new();
    matches!(
        tokio::time::timeout(within, stream.read_to_end(&mut rest)).await,
        Ok(Ok(_))
    )
}

/// A probe is answered 503 while the replica is not ready and 200 once it is, as it is
/// when asked; `HEAD` the same, its head alone. Each connection is closed once answered.
#[tokio::test]
async fn a_probe_is_told_whether_the_replica_is_ready() {
    let health = Health::start(DEADLINE).await;
    let unready = "HTTP/1.1 503 Service Unavailable\r\n";
    let answer = health.ask("GET", PATH).await;
    assert!(answer.starts_with(unready), "{answer}");
    let head = health.ask("HEAD", PATH).await;
    assert!(head.starts_with(unready), "{head}");
    health.ready.send_replace(true);
    let answer = health.ask("GET", PATH).await;
    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
    assert!(answer.ends_with("\r\n\r\nready\n"), "{answer}");
    let head = health.ask("HEAD", PATH).await;
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(head.ends_with("\r\n\r\n"), "{head}");
    // What makes it ready is the caller's: told it no longer is, it says so.
    health.ready.send_replace(false);
    let answer = health.ask("GET", PATH).await;
    assert!(answer.starts_with(unready), "{answer}");
}

/// Anywhere else is not found, and anything but `GET` and `HEAD` not allowed.
#[tokio::test]
async fn only_get_and_head_of_the_path_are_answered() {
    let health = Health::start(DEADLINE).await;
    let elsewhere = health.ask("GET", "/metrics").await;
    assert!(
        elsewhere.starts_with("HTTP/1.1 404 Not Found\r\n"),
        "{elsewhere}"
    );
    let posted = health.ask("POST", PATH).await;
    assert!(
        posted.starts_with("HTTP/1.1 405 Method Not Allowed\r\n"),
        "{posted}"
    );
    assert!(
        posted
            .to_ascii_lowercase()
            .contains("\r\nallow: get, head\r\n"),
        "{posted}"
    );
}

/// A connection that sends nothing is closed at the deadline, and so is one that sends
/// part of a head and no more.
#[tokio::test]
async fn a_connection_is_held_no_longer_than_the_deadline() {
    let health = Health::start(Duration::from_millis(200)).await;
    let mut silent = TcpStream::connect(&health.address).await.unwrap();
    assert!(closed_within(&mut silent, WAIT).await);
    let mut partial = TcpStream::connect(&health.address).await.unwrap();
    partial.write_all(b"GET /ready HTTP/1.1\r\n").await.unwrap();
    assert!(closed_within(&mut partial, WAIT).await);
}

/// However many connections are held open idle, a probe is answered: nothing bounds how
/// many are served at once.
#[tokio::test]
async fn idle_connections_do_not_keep_a_probe_out() {
    // A deadline far off, so that the idle ones stay for the whole test.
    let health = Health::start(Duration::from_secs(600)).await;
    health.ready.send_replace(true);
    let mut idle = Vec::new();
    for _ in 0..64 {
        idle.push(TcpStream::connect(&health.address).await.unwrap());
    }
    let answer = tokio::time::timeout(Duration::from_secs(5), health.ask("GET", PATH))
        .await
        .expect("a probe was kept out by idle connections");
    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
    drop(idle);
}

/// Told to stop, it stops, and its connections with it.
#[tokio::test]
async fn it_stops_when_told() {
    let mut health = Health::start(Duration::from_secs(600)).await;
    let mut open = TcpStream::connect(&health.address).await.unwrap();
    // Taken before it is told to stop: a probe on another connection answered.
    let answer = health.ask("GET", PATH).await;
    assert!(answer.starts_with("HTTP/1.1 503"), "{answer}");
    let _ = health.stop.take().unwrap().send(());
    tokio::time::timeout(WAIT, &mut health.served)
        .await
        .unwrap()
        .unwrap();
    assert!(closed_within(&mut open, WAIT).await);
}
