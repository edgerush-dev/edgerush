use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// Long enough for anything a test waits on to happen on a loaded machine.
const WAIT: Duration = Duration::from_secs(10);

/// The metrics serving on a port of their own.
struct Metrics {
    address: String,
    text: watch::Sender<Bytes>,
    stop: Option<oneshot::Sender<()>>,
    served: JoinHandle<()>,
}

impl Metrics {
    async fn start(text: &'static str, deadline: Duration) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let (text, reading) = watch::channel(Bytes::from_static(text.as_bytes()));
        let (stop, stopped) = oneshot::channel();
        let served = tokio::spawn(serve(listener, reading, deadline, async move {
            let _ = stopped.await;
        }));
        Self {
            address,
            text,
            stop: Some(stop),
            served,
        }
    }

    /// The whole answer to `request`, sent on a connection of its own that it closes.
    async fn ask(&self, method: &str, path: &str) -> String {
        let mut stream = TcpStream::connect(&self.address).await.unwrap();
        let request =
            format!("{method} {path} HTTP/1.1\r\nhost: metrics.test\r\nconnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut answer = String::new();
        tokio::time::timeout(WAIT, stream.read_to_string(&mut answer))
            .await
            .unwrap()
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

/// A scrape is given the latest text, as the Prometheus text format; `HEAD` its head alone.
#[tokio::test]
async fn a_scrape_is_given_the_latest_text() {
    let metrics = Metrics::start("a 1\n", DEADLINE).await;
    let answer = metrics.ask("GET", PATH).await;
    assert!(answer.starts_with("HTTP/1.1 200 OK\r\n"), "{answer}");
    assert!(
        answer
            .to_ascii_lowercase()
            .contains("\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\n"),
        "{answer}"
    );
    assert!(answer.ends_with("\r\n\r\na 1\n"), "{answer}");
    metrics.text.send_replace(Bytes::from_static(b"a 2\n"));
    assert!(metrics.ask("GET", PATH).await.ends_with("\r\n\r\na 2\n"));
    let head = metrics.ask("HEAD", PATH).await;
    assert!(head.starts_with("HTTP/1.1 200 OK\r\n"), "{head}");
    assert!(head.ends_with("\r\n\r\n"), "{head}");
}

/// Anywhere else is not found, and anything but `GET` and `HEAD` not allowed.
#[tokio::test]
async fn only_get_and_head_of_the_path_are_answered() {
    let metrics = Metrics::start("a 1\n", DEADLINE).await;
    let elsewhere = metrics.ask("GET", "/").await;
    assert!(
        elsewhere.starts_with("HTTP/1.1 404 Not Found\r\n"),
        "{elsewhere}"
    );
    let posted = metrics.ask("POST", PATH).await;
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

/// A connection that sends nothing is closed at the deadline, and so is one kept open
/// after a scrape that sends nothing more.
#[tokio::test]
async fn a_connection_that_sends_nothing_is_closed() {
    let metrics = Metrics::start("a 1\n", Duration::from_millis(200)).await;
    let mut silent = TcpStream::connect(&metrics.address).await.unwrap();
    assert!(closed_within(&mut silent, WAIT).await);
    let mut kept = TcpStream::connect(&metrics.address).await.unwrap();
    kept.write_all(b"HEAD /metrics HTTP/1.1\r\nhost: metrics.test\r\n\r\n")
        .await
        .unwrap();
    let mut head = Vec::new();
    while !head.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        tokio::time::timeout(WAIT, kept.read_exact(&mut byte))
            .await
            .unwrap()
            .unwrap();
        head.push(byte[0]);
    }
    assert!(head.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(closed_within(&mut kept, WAIT).await);
}

/// Past `MOST` connections, the next waits in the backlog, answered once one ends.
#[tokio::test]
async fn past_the_most_connections_the_next_waits() {
    let metrics = Metrics::start("a 1\n", Duration::from_secs(600)).await;
    let mut open = Vec::new();
    for _ in 0..MOST {
        open.push(TcpStream::connect(&metrics.address).await.unwrap());
    }
    // Each taken before the next is asked: one asked of each that is answered says so.
    for stream in &mut open {
        stream
            .write_all(b"HEAD /metrics HTTP/1.1\r\nhost: metrics.test\r\n\r\n")
            .await
            .unwrap();
        let mut first = [0; 12];
        tokio::time::timeout(WAIT, stream.read_exact(&mut first))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(&first, b"HTTP/1.1 200");
    }
    let mut waiting = TcpStream::connect(&metrics.address).await.unwrap();
    waiting
        .write_all(b"GET /metrics HTTP/1.1\r\nhost: metrics.test\r\nconnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut answer = String::new();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(300),
            waiting.read_to_string(&mut answer)
        )
        .await
        .is_err(),
        "answered past the most: {answer}"
    );
    drop(open.pop());
    tokio::time::timeout(WAIT, waiting.read_to_string(&mut answer))
        .await
        .unwrap()
        .unwrap();
    assert!(answer.ends_with("\r\n\r\na 1\n"), "{answer}");
}

/// Told to stop, it stops, and its connections with it.
#[tokio::test]
async fn it_stops_when_told() {
    let mut metrics = Metrics::start("a 1\n", Duration::from_secs(600)).await;
    let mut open = TcpStream::connect(&metrics.address).await.unwrap();
    // Taken before it is told to stop.
    open.write_all(b"HEAD /metrics HTTP/1.1\r\nhost: metrics.test\r\n\r\n")
        .await
        .unwrap();
    let mut first = [0; 12];
    tokio::time::timeout(WAIT, open.read_exact(&mut first))
        .await
        .unwrap()
        .unwrap();
    let _ = metrics.stop.take().unwrap().send(());
    tokio::time::timeout(WAIT, &mut metrics.served)
        .await
        .unwrap()
        .unwrap();
    assert!(closed_within(&mut open, WAIT).await);
}
