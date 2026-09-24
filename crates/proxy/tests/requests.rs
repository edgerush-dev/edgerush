//! What a client may send, and what becomes of it: each case is bytes a client writes to a
//! real proxy in front of an upstream that answers with what it saw, and what the client
//! must be answered, whether the connection may go on, and what may and may not reach the
//! upstream.
//!
//! The cases come from a survey of the server-side HTTP/1 tests of nginx, HAProxy, Envoy,
//! hyper, Pingora and linkerd2-proxy ([14 §9](../../../docs/14-downstream-server.md)),
//! each case naming where its idea came from. Nothing is copied from them: the bytes here
//! are written for this table, and the expectations are ours — where one of them answers
//! differently, the case says so and the answer here is the one the specification and the
//! docs give ([14 §4](../../../docs/14-downstream-server.md), [03 §4, §11](../../../docs/03-data-plane.md)).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers around the cases fail them the way the cases would"
)]

use edgerush_config::{Config, compile};
use edgerush_proxy::{Proxy, Worker};
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// A proxy whose one listener sends every request to `upstream`, served by a worker on a
/// thread of its own.
fn proxy_to(upstream: SocketAddr) -> SocketAddr {
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http }}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        backends: [{{ upstream: up, weight: 1 }}]
upstreams:
  up: {{ endpoints: ["{upstream}"] }}
"#
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let local = tokio::task::LocalSet::new();
        let entered = runtime.enter();
        let socket = TcpListener::from_std(socket).unwrap();
        let worker = Worker::new(proxy);
        local.spawn_local(std::rc::Rc::clone(&worker).maintain());
        local.spawn_local(worker.serve(0, socket));
        drop(entered);
        runtime.block_on(local);
    });
    address
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

/// Reads into `buffer` until it holds `mark`, and says where the mark ends; `None` if the
/// connection ended first.
async fn until(stream: &mut TcpStream, buffer: &mut Vec<u8>, mark: &[u8]) -> Option<usize> {
    loop {
        if let Some(at) = find(buffer, mark) {
            return Some(at + mark.len());
        }
        let mut bytes = [0; 8192];
        match stream.read(&mut bytes).await {
            Ok(0) | Err(_) => return None,
            Ok(read) => buffer.extend_from_slice(&bytes[..read]),
        }
    }
}

/// Reads into `buffer` until it holds at least `count` bytes.
async fn at_least(stream: &mut TcpStream, buffer: &mut Vec<u8>, count: usize) -> bool {
    while buffer.len() < count {
        let mut bytes = [0; 8192];
        match stream.read(&mut bytes).await {
            Ok(0) | Err(_) => return false,
            Ok(read) => buffer.extend_from_slice(&bytes[..read]),
        }
    }
    true
}

/// The `content-length` of a head, or 0 where it has none.
fn length_of(head: &[u8]) -> usize {
    String::from_utf8_lossy(head)
        .to_ascii_lowercase()
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0)
}

/// An upstream that answers every request with 200 and, as the body, the request exactly
/// as it arrived: its head and its body, framing and all. A `HEAD` is answered with the
/// length and no body.
fn echo_upstream() -> SocketAddr {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    socket.set_nonblocking(true).unwrap();
    let address = socket.local_addr().unwrap();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let socket = TcpListener::from_std(socket).unwrap();
            loop {
                let (stream, _) = socket.accept().await.unwrap();
                tokio::spawn(echo(stream));
            }
        });
    });
    address
}

async fn echo(mut stream: TcpStream) {
    let mut buffer = Vec::new();
    loop {
        let Some(end) = until(&mut stream, &mut buffer, b"\r\n\r\n").await else {
            return;
        };
        let head: Vec<u8> = buffer.drain(..end).collect();
        let lowered = String::from_utf8_lossy(&head).to_ascii_lowercase();
        let chunked = lowered
            .lines()
            .any(|line| line.starts_with("transfer-encoding:") && line.contains("chunked"));
        let body: Vec<u8> = if chunked {
            // The proxy frames what it sends, so its last chunk and the empty line after
            // the trailers are where they should be.
            let mut from = 0;
            let last = loop {
                match find(&buffer[from..], b"0\r\n") {
                    Some(at) if from + at == 0 || buffer[from + at - 1] == b'\n' => {
                        break from + at;
                    }
                    Some(at) => from += at + 1,
                    None => {
                        let mut bytes = [0; 8192];
                        match stream.read(&mut bytes).await {
                            Ok(0) | Err(_) => return,
                            Ok(read) => buffer.extend_from_slice(&bytes[..read]),
                        }
                    }
                }
            };
            let mut rest = buffer.split_off(last);
            let Some(end) = until(&mut stream, &mut rest, b"\r\n\r\n").await else {
                return;
            };
            let tail: Vec<u8> = rest.drain(..end).collect();
            let mut body = std::mem::replace(&mut buffer, rest);
            body.extend_from_slice(&tail);
            body
        } else {
            let length = length_of(&head);
            if !at_least(&mut stream, &mut buffer, length).await {
                return;
            }
            buffer.drain(..length).collect()
        };
        let mut said = head;
        said.extend_from_slice(&body);
        let mut answer =
            format!("HTTP/1.1 200 OK\r\ncontent-length: {}\r\n\r\n", said.len()).into_bytes();
        if !lowered.starts_with("head ") {
            answer.extend_from_slice(&said);
        }
        if stream.write_all(&answer).await.is_err() {
            return;
        }
    }
}

/// An answer the client is to read.
#[derive(Clone, Copy, Debug)]
enum Answer {
    /// This status, with the body its length says.
    Is(u16),
    /// This status, to a `HEAD`: its length says what a `GET` would have had, and no body
    /// follows.
    Headed(u16),
}

/// What becomes of the connection after the answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum End {
    /// It is still open, and nothing more has arrived.
    Open,
    /// The proxy closed it.
    Closed,
}

struct Case {
    name: &'static str,
    /// Where the idea came from.
    from: &'static str,
    /// What the client writes, each piece a moment after the one before.
    sent: Vec<Vec<u8>>,
    /// What it writes once it has read a `100 Continue`, as a client that asked to be told
    /// waits to be; a body that arrived first could make the `100` unnecessary.
    after_continue: Option<Vec<u8>>,
    answers: Vec<Answer>,
    end: End,
    /// What must have reached the upstream, in this order, in the requests it echoed.
    seen: Vec<Vec<u8>>,
    /// What must not have reached it, compared without regard to case.
    unseen: Vec<&'static str>,
}

fn case(name: &'static str, from: &'static str, sent: &[u8], answers: &[Answer], end: End) -> Case {
    Case {
        name,
        from,
        sent: vec![sent.to_vec()],
        after_continue: None,
        answers: answers.to_vec(),
        end,
        seen: Vec::new(),
        unseen: Vec::new(),
    }
}

impl Case {
    fn seen(mut self, bytes: &[u8]) -> Self {
        self.seen.push(bytes.to_vec());
        self
    }

    fn unseen(mut self, text: &'static str) -> Self {
        self.unseen.push(text);
        self
    }

    fn after_continue(mut self, bytes: &[u8]) -> Self {
        self.after_continue = Some(bytes.to_vec());
        self
    }

    fn in_pieces(mut self, pieces: &[&[u8]]) -> Self {
        self.sent = pieces.iter().map(|piece| piece.to_vec()).collect();
        self
    }
}

use Answer::{Headed, Is};
use End::{Closed, Open};

/// Runs a case, and says what went wrong with it if anything did.
async fn run(proxy: SocketAddr, case: &Case) -> Result<(), String> {
    let mut client = TcpStream::connect(proxy).await.unwrap();
    for (at, piece) in case.sent.iter().enumerate() {
        if at > 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // A proxy that has already answered and closed may refuse the rest.
        if client.write_all(piece).await.is_err() {
            break;
        }
    }
    let mut buffer = Vec::new();
    let mut echoed = Vec::new();
    for (at, answer) in case.answers.iter().enumerate() {
        let Some(end) = until(&mut client, &mut buffer, b"\r\n\r\n").await else {
            return Err(format!(
                "answer {at}: the connection ended; had {:?}",
                String::from_utf8_lossy(&buffer)
            ));
        };
        let head: Vec<u8> = buffer.drain(..end).collect();
        let status: u16 = std::str::from_utf8(&head[9..12])
            .ok()
            .and_then(|code| code.parse().ok())
            .unwrap_or(0);
        let (wanted, bodiless) = match answer {
            Is(status) => (*status, false),
            Headed(status) => (*status, true),
        };
        if status != wanted {
            return Err(format!(
                "answer {at}: {:?}, not {wanted}",
                String::from_utf8_lossy(&head)
            ));
        }
        if !head.starts_with(b"HTTP/1.1 ") {
            return Err(format!("answer {at}: {:?}", String::from_utf8_lossy(&head)));
        }
        let length = if bodiless || (100..200).contains(&status) {
            0
        } else {
            length_of(&head)
        };
        if !at_least(&mut client, &mut buffer, length).await {
            return Err(format!("answer {at}: the body was cut short"));
        }
        let body: Vec<u8> = buffer.drain(..length).collect();
        if status == 100
            && let Some(bytes) = &case.after_continue
        {
            client.write_all(bytes).await.unwrap();
        }
        if status == 200 {
            echoed.extend_from_slice(&body);
        }
    }
    let mut from = 0;
    for bytes in &case.seen {
        match find(&echoed[from..], bytes) {
            Some(at) => from += at + bytes.len(),
            None => {
                return Err(format!(
                    "the upstream did not see {:?} (after what came before); it saw {:?}",
                    String::from_utf8_lossy(bytes),
                    String::from_utf8_lossy(&echoed)
                ));
            }
        }
    }
    let lowered = String::from_utf8_lossy(&echoed).to_ascii_lowercase();
    for text in &case.unseen {
        if lowered.contains(&text.to_ascii_lowercase()) {
            return Err(format!("the upstream saw {text:?}: {lowered:?}"));
        }
    }
    match case.end {
        Closed => {
            // Whatever else arrives before the close, if anything, is a failure.
            let mut rest = Vec::new();
            let read =
                tokio::time::timeout(Duration::from_secs(10), client.read_to_end(&mut rest)).await;
            match read {
                Err(_) => return Err("the connection was not closed".to_owned()),
                // A reset is a close too, and which of the two arrives is the system's.
                Ok(_) if rest.is_empty() && buffer.is_empty() => {}
                Ok(_) => {
                    buffer.extend_from_slice(&rest);
                    return Err(format!(
                        "more arrived before the close: {:?}",
                        String::from_utf8_lossy(&buffer)
                    ));
                }
            }
        }
        Open => {
            let mut byte = [0; 1];
            match tokio::time::timeout(Duration::from_millis(300), client.read(&mut byte)).await {
                Err(_) if buffer.is_empty() => {}
                Err(_) => {
                    return Err(format!(
                        "more arrived: {:?}",
                        String::from_utf8_lossy(&buffer)
                    ));
                }
                Ok(Ok(0) | Err(_)) => return Err("the connection was closed".to_owned()),
                Ok(Ok(_)) => return Err("more arrived".to_owned()),
            }
        }
    }
    Ok(())
}

/// A request for `/` with `Host: a` and these field lines, and nothing else.
fn with_fields(fields: &str) -> Vec<u8> {
    format!("GET / HTTP/1.1\r\nHost: a\r\n{fields}\r\n").into_bytes()
}

fn request_line() -> Vec<Case> {
    let long_target = format!("GET /{} HTTP/1.1\r\nHost: a\r\n\r\n", "a".repeat(9_000));
    vec![
        case("a plain request", "all", b"GET / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"GET / HTTP/1.1\r\nHost: a\r\n\r\n"),
        case("a space in the method", "hyper server.rs:2669", b"GE T / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a delimiter in the method", "Envoy integration_test.cc:1232", b"GE(T / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a method of our own", "Envoy codec_impl_test.cc:1329 (refuses it by default); a method is any token (RFC 9110 §9.1)", b"BAD / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"BAD / HTTP/1.1\r\n"),
        case("QUERY with a body", "Envoy codec_impl_test.cc:1384", b"QUERY /s HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc", &[Is(200)], Open)
            .seen(b"QUERY /s HTTP/1.1\r\n")
            .seen(b"content-length: 3\r\n\r\nabc"),
        case("TRACE goes on", "nginx-tests http_method.t:52 (answers 405 itself)", b"TRACE / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"TRACE / HTTP/1.1\r\n"),
        case("two spaces before the target", "hyper role.rs:1889", b"GET  / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("two spaces before the version", "HAProxy http_request_buffer.vtc:120 and nginx accept it; RFC 9112 §3 has one SP", b"GET /  HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("no method, a leading space", "nginx-tests control_api.t:236", b" / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("nothing but spaces", "nginx-tests control_api.t:237", b"   \r\n\r\n", &[Is(400)], Closed),
        case("one empty line first", "Envoy parser_integration_test.cc:48; RFC 9112 §2.2", b"\r\nGET / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open),
        case("many carriage returns first", "Envoy protocol_integration_test.cc:5243", &[b"\r".repeat(100), b"GET / HTTP/1.1\r\nHost: a\r\n\r\n".to_vec()].concat(), &[Is(400)], Closed),
        case("a bare CR before the version", "Envoy codec_impl_test.cc:5198 (accepts it)", b"GET /\rHTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a CR that no LF follows", "nginx-tests control_api.t:235", b"GET / HTTP/1.1\rX\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("lines ending in LF alone", "HAProxy close_wait_lf.vtc:51 and hyper role.rs:3005 accept them (14 §4)", b"GET / HTTP/1.1\nHost: a\n\n", &[Is(400)], Closed),
        case("a field line ending in LF alone", "hyper role.rs:3005", b"GET / HTTP/1.1\r\nHost: a\n\r\n", &[Is(400)], Closed),
        case("NUL in the target", "nginx-tests control_api.t:260", b"GET /a\x00b HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("NUL in the method", "nginx-tests control_api.t:261", b"GE\x00T / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a control byte in the target", "nginx-tests http_uri.t:80", b"GET /\x02 HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a space in the target", "nginx-tests http_uri.t:79", b"GET / / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("UTF-8 in the target", "nginx-tests control_api.t:265; Envoy codec_impl_test.cc:3745 refuses it (03 §4)", b"GET /\xd0\xb0 HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"GET /%D0%B0 HTTP/1.1\r\n"),
        case("bytes that are not text", "nginx-tests control_api.t:266", b"\xff\xfe\r\n\r\n", &[Is(400)], Closed),
        case("two empty lines", "nginx-tests control_api.t:165", b"\r\n\r\n", &[Is(400)], Closed),
        case("HTTP/0.9", "Envoy codec_impl_test.cc:1085 (accepts it)", b"GET /\r\n\r\n", &[Is(400)], Closed),
        case("HTTP/1.2", "nginx-tests control_api.t:213 (14 §4)", b"GET / HTTP/1.2\r\nHost: a\r\n\r\n", &[Is(505)], Closed),
        case("a minor version of two digits", "nginx-tests control_api.t:214", b"GET / HTTP/1.10\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("HTTP/2.0 on a request line", "nginx-tests control_api.t:215", b"GET / HTTP/2.0\r\nHost: a\r\n\r\n", &[Is(505)], Closed),
        case("a major version of two digits", "nginx-tests control_api.t:245", b"GET / HTTP/11.0\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a version that is not a number", "Envoy codec_impl_test.cc:1085", b"GET / HTTP/A.0\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("HTTPS for HTTP", "Envoy codec_impl_test.cc:1085", b"GET / HTTPS/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("the protocol name in lower case", "RFC 9112 §2.3 (case-sensitive)", b"GET / http/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("no version at all", "HAProxy tcp_to_http_upgrade.vtc:168", b"GET / BAD-VERSION\r\n\r\n", &[Is(400)], Closed),
        case("a line break inside the target", "nginx-tests control_api.t:263", b"GET /x\r\nX: y HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("a request line over its bound", "hyper server.rs:1189", long_target.as_bytes(), &[Is(414)], Closed),
    ]
}

fn targets() -> Vec<Case> {
    vec![
        case("OPTIONS *", "HAProxy h1_request_target_validation.vtc:30 serves it (14 §4)", b"OPTIONS * HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("* with GET", "HAProxy h1_request_target_validation.vtc:58", b"GET * HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("* followed by a path", "HAProxy h1_request_target_validation.vtc:52", b"OPTIONS */x HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed),
        case("an authority with GET", "HAProxy h1_request_target_validation.vtc:99", b"GET a:80 HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("a path with no leading slash", "HAProxy h1_request_target_validation.vtc:106", b"GET admin HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("CONNECT", "nginx-tests http_method.t:61 answers 405, Pingora test_basic.rs:806 too (14 §4)", b"CONNECT a:443 HTTP/1.1\r\nHost: a:443\r\n\r\n", &[Is(400)], Open),
        case("CONNECT from HTTP/1.0", "linkerd2-proxy transparency.rs:937", b"CONNECT a:443 HTTP/1.0\r\n\r\n", &[Is(400)], Closed),
        case("absolute-form", "nginx-tests http_uri.t:65", b"GET http://a/x?q=1 HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"GET /x?q=1 HTTP/1.1\r\nHost: a\r\n"),
        case("absolute-form with no path", "HAProxy h1_host_normalization.vtc:820", b"GET http://a HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"GET / HTTP/1.1\r\n"),
        case("absolute-form names another host than Host", "RFC 9112 §3.2.2: the target's wins; HAProxy h1_host_normalization.vtc:423 and Pingora test_upstream.rs:872 refuse it", b"GET http://a/ HTTP/1.1\r\nHost: b\r\n\r\n", &[Is(200)], Open)
            .seen(b"host: a\r\n")
            .unseen("host: b"),
        case("absolute-form names another port than Host", "HAProxy h1_host_normalization.vtc:437 refuses it", b"GET http://a:80/ HTTP/1.1\r\nHost: a:81\r\n\r\n", &[Is(200)], Open)
            .seen(b"host: a:80\r\n"),
        case("user information in absolute-form", "Pingora test_upstream.rs:872; RFC 9110 §4.2.4", b"GET http://u:p@a/ HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("absolute-form with an empty authority", "nginx-tests http_host.t:57", b"GET http:/// HTTP/1.1\r\nHost: \r\n\r\n", &[Is(400)], Closed),
        case("absolute-form with a port that is not a number", "nginx-tests control_api.t:229", b"GET http://a:abc/ HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("absolute-form with a port too large", "Envoy codec_impl_test.cc:1239", b"GET http://a:1000000/ HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("a scheme in mixed case", "Envoy integration_test.cc:1678", b"GET hTtP://a/ HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open),
        case("a percent sign with no digits after it", "nginx-tests http_uri.t:55", b"GET /foo% HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("a path above the root", "nginx-tests http_variables.t:85", b"GET /../x HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
        case("an encoded slash", "Envoy protocol_integration_test.cc:4226 (03 §4)", b"GET /a%2Fb HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Open),
    ]
}

fn hosts() -> Vec<Case> {
    let refused = |name, from, host: &str| {
        let sent = format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n");
        case(name, from, sent.as_bytes(), &[Is(400)], Open)
    };
    let served = |name, from, host: &str| {
        let sent = format!("GET / HTTP/1.1\r\nHost: {host}\r\n\r\n");
        let seen = format!("Host: {host}\r\n");
        case(name, from, sent.as_bytes(), &[Is(200)], Open).seen(seen.as_bytes())
    };
    vec![
        case(
            "HTTP/1.1 with no Host",
            "Envoy integration_test.cc:1545",
            b"GET / HTTP/1.1\r\n\r\n",
            &[Is(400)],
            Open,
        ),
        case(
            "HTTP/1.0 with no Host",
            "linkerd2-proxy transparency.rs:1384 serves it (03 §4; 12)",
            b"GET / HTTP/1.0\r\n\r\n",
            &[Is(400)],
            Closed,
        ),
        case(
            "an empty Host",
            "nginx-tests http_host.t:57",
            b"GET / HTTP/1.1\r\nHost:\r\n\r\n",
            &[Is(400)],
            Open,
        ),
        case(
            "two Hosts that differ",
            "HAProxy h1_host_normalization.vtc:479",
            b"GET / HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n",
            &[Is(400)],
            Open,
        ),
        case(
            "the same Host twice",
            "Pingora test_upstream.rs:872",
            b"GET / HTTP/1.1\r\nHost: a\r\nHost: a\r\n\r\n",
            &[Is(400)],
            Open,
        ),
        refused(
            "user information in Host",
            "Pingora test_upstream.rs:872",
            "u@a",
        ),
        refused("two ports", "nginx-tests http_host.t:138", "a:80:81"),
        refused("a port too long", "nginx-tests http_host.t:136", "a:98777"),
        refused("a slash in Host", "nginx-tests http_host.t:179", "a/b"),
        refused("a backslash in Host", "nginx-tests http_host.t:179", "a\\b"),
        refused(
            "a quote in Host",
            "Envoy codec_impl_test.cc:1594",
            "h.\"com",
        ),
        refused(
            "a bar in Host",
            "Envoy protocol_integration_test.cc:4149",
            "ho|st",
        ),
        refused("a lone dot", "nginx-tests http_host.t:113", "."),
        refused(
            "an IPv6 address not closed",
            "nginx-tests http_host.t:229",
            "[::1",
        ),
        refused(
            "text before an IPv6 address",
            "nginx-tests http_host.t:218",
            "x[::1]",
        ),
        case(
            "a control byte in Host",
            "nginx-tests http_host.t:265",
            b"GET / HTTP/1.1\r\nHost: a\x02\r\n\r\n",
            &[Is(400)],
            Closed,
        ),
        served(
            "two dots together",
            "nginx-tests http_host.t:184 refuses it; RFC 3986 reg-name allows it",
            "a..b",
        ),
        served("a trailing dot", "nginx-tests http_host.t:95", "a.b."),
        served(
            "an empty port",
            "nginx-tests http_host.t:73; RFC 3986 §3.2.3",
            "a:",
        ),
        served(
            "an IPv6 address and a port",
            "nginx-tests http_host.t:159",
            "[::1]:80",
        ),
        served("upper case", "nginx-tests http_host.t:122", "A.EXAMPLE"),
        case(
            "space around Host",
            "Envoy protocol_integration_test.cc:423",
            b"GET / HTTP/1.1\r\nHost:  a \r\n\r\n",
            &[Is(200)],
            Open,
        ),
    ]
}

fn fields() -> Vec<Case> {
    let refused =
        |name, from, fields: &str| case(name, from, &with_fields(fields), &[Is(400)], Closed);
    let large_name = format!("{}: x\r\n", "a".repeat(70_000));
    let lines = |count: usize| {
        (0..count)
            .map(|at| format!("x-{at}: v\r\n"))
            .collect::<String>()
    };
    vec![
        case(
            "an empty value",
            "Envoy codec_impl_test.cc:521",
            &with_fields("X-E:\r\n"),
            &[Is(200)],
            Open,
        )
        .seen(b"X-E:\r\n"),
        refused("an empty name", "Envoy codec_impl_test.cc:4643", ": v\r\n"),
        refused("no colon", "Envoy integration_test.cc:1201", "foo bar\r\n"),
        refused(
            "a space in a name",
            "Envoy codec_impl_test.cc:4775",
            "fo o: v\r\n",
        ),
        refused("a space before the colon", "RFC 9112 §5.1", "foo : v\r\n"),
        refused("a tab before the colon", "RFC 9112 §5.1", "foo\t: v\r\n"),
        refused(
            "a delimiter in a name",
            "Envoy codec_impl_test.cc:4739",
            "fo[o: v\r\n",
        ),
        refused(
            "a name that starts with a colon",
            "nginx-tests ignore_invalid_headers.t:128",
            ":foo: v\r\n",
        ),
        case(
            "a name that is not ASCII",
            "Envoy codec_impl_test.cc:4811",
            &with_fields("f\u{f6}o: v\r\n"),
            &[Is(400)],
            Closed,
        ),
        refused(
            "a CR in a name",
            "Envoy codec_impl_test.cc:5249",
            "fo\ro: v\r\n",
        ),
        refused(
            "a control byte in a value",
            "Envoy codec_impl_test.cc:1497",
            "X: a\x03b\r\n",
        ),
        refused(
            "NUL in a value",
            "Envoy codec_impl_test.cc:5038",
            "X: a\x00b\r\n",
        ),
        refused(
            "DEL in a value",
            "Envoy protocol_integration_test.cc:6218",
            "X: a\x7fb\r\n",
        ),
        refused(
            "a bare CR in a value",
            "Envoy codec_impl_test.cc:5060 drops it",
            "X: a\rb\r\n",
        ),
        case(
            "a byte above ASCII in a value",
            "RFC 9110 §5.5 (obs-text)",
            &[b"GET / HTTP/1.1\r\nHost: a\r\nX: caf\xe9\r\n\r\n".as_slice()].concat(),
            &[Is(200)],
            Open,
        )
        .seen(b"X: caf\xe9\r\n"),
        refused(
            "a folded line",
            "Envoy codec_impl_test.cc:4949 unfolds it; RFC 9112 §5.2",
            "X: a\r\n b\r\n",
        ),
        case(
            "whitespace before the first field",
            "nginx-tests ignore_invalid_headers.t:129",
            b"GET / HTTP/1.1\r\n X: a\r\nHost: a\r\n\r\n",
            &[Is(400)],
            Closed,
        ),
        case(
            "an underscore in a name",
            "HAProxy restrict_req_hdr_names.vtc:145",
            &with_fields("X_my: on\r\n"),
            &[Is(200)],
            Open,
        )
        .seen(b"X_my: on\r\n"),
        case(
            "whitespace around a value",
            "Envoy codec_impl_test.cc:834; kept as sent (14 §6)",
            &with_fields("X:  \t v \t \r\n"),
            &[Is(200)],
            Open,
        )
        .seen(b"X:  \t v \t \r\n"),
        case(
            "a field twice",
            "HAProxy h1or2_to_h1c.vtc:183",
            &with_fields("X: 1\r\nX: 2\r\n"),
            &[Is(200)],
            Open,
        )
        .seen(b"X: 1\r\nX: 2\r\n"),
        case(
            "a name over the head's bound",
            "hyper server.rs:1496",
            &with_fields(&large_name),
            &[Is(431)],
            Closed,
        ),
        case(
            "more fields than allowed",
            "Envoy codec_impl_test.cc:3521",
            &with_fields(&lines(130)),
            &[Is(431)],
            Closed,
        ),
        case(
            "as many fields as allowed",
            "Envoy codec_impl_test.cc:3612",
            &with_fields(&lines(127)),
            &[Is(200)],
            Open,
        ),
        case(
            "a head in pieces",
            "Pingora v1/server.rs:1896",
            b"",
            &[Is(200)],
            Open,
        )
        .in_pieces(&[b"GET / HT", b"TP/1.1\r\nHo", b"st: a\r\n\r", b"\n"]),
    ]
}

fn lengths() -> Vec<Case> {
    let refused = |name, from, length: &str| {
        let sent = format!("POST / HTTP/1.1\r\nHost: a\r\nContent-Length: {length}\r\n\r\nhello");
        case(name, from, sent.as_bytes(), &[Is(400)], Closed)
    };
    vec![
        refused("a sign", "hyper server.rs:478", "+5"),
        refused("a negative length", "Envoy protocol_integration_test.cc:2911", "-1"),
        refused("not a number", "hyper server.rs:2691", "foo"),
        refused("a fraction", "Pingora v1/server.rs:2197", "1.5"),
        refused("nothing", "HAProxy h1_to_h1.vtc:297", ""),
        refused("a trailing comma", "HAProxy h1_to_h1.vtc:284", "5,"),
        refused("a list of equal lengths", "Pingora common.rs:498 takes it as one (14 §4)", "5, 5"),
        refused("a list of lengths that differ", "Envoy protocol_integration_test.cc:2967", "3, 2"),
        refused("more than a count can hold", "Pingora common.rs:498", "99999999999999999999999"),
        case("the same length twice", "Pingora common.rs:445 takes it as one (14 §4)", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 5\r\n\r\nhello", &[Is(400)], Closed),
        case("two lengths that differ", "hyper role.rs:1946", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nContent-Length: 6\r\n\r\nhello", &[Is(400)], Closed),
        case("leading zeros", "HAProxy h1or2_to_h1c.vtc:183", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 005\r\n\r\nhello", &[Is(200)], Open)
            .seen(b"content-length: 5\r\n\r\nhello"),
        case("a length of zero", "Envoy protocol_integration_test.cc:2565; linkerd2-proxy transparency.rs:1061", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\r\n", &[Is(200)], Open)
            .seen(b"content-length: 0\r\n\r\n"),
        case("a body on GET", "HAProxy h1_to_h1.vtc:159; hyper server.rs:59", b"GET / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc", &[Is(200)], Open)
            .seen(b"content-length: 3\r\n\r\nabc"),
        case("a body on HEAD", "HAProxy h1_to_h1.vtc:208", b"HEAD / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabc", &[Headed(200)], Open),
        case("POST with no framing", "HAProxy h1_to_h1.vtc:237; linkerd2-proxy transparency.rs:1025", b"POST / HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200)], Open)
            .seen(b"POST / HTTP/1.1\r\nHost: a\r\n\r\n")
            .unseen("transfer-encoding"),
        case("what follows a closing request", "HAProxy h1_to_h1.vtc:170", b"GET / HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\nthis is not sent\r\n\r\n", &[Is(200)], Closed),
        case("bytes after a request with no body", "Envoy codec_impl_test.cc:4517", b"POST / HTTP/1.1\r\nHost: a\r\n\r\nfoo", &[Is(200)], Open)
            .unseen("foo"),
        case("the next request right after a body", "Envoy protocol_integration_test.cc:5432; nginx-tests body.t", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\n\r\nabcGET /n HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200), Is(200)], Open)
            .seen(b"\r\n\r\nabc")
            .seen(b"GET /n HTTP/1.1\r\n"),
        case("a body in pieces", "nginx-tests body.t:130", b"", &[Is(200)], Open)
            .in_pieces(&[b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 10\r\n\r\n01234", b"56789"])
            .seen(b"\r\n\r\n0123456789"),
    ]
}

fn codings() -> Vec<Case> {
    let coded = |name, from, coding: &str, answer: u16| {
        let sent = format!(
            "POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: {coding}\r\n\r\n1\r\nq\r\n0\r\n\r\n"
        );
        case(
            name,
            from,
            sent.as_bytes(),
            &[Is(answer)],
            if answer == 200 { Open } else { Closed },
        )
    };
    vec![
        case("chunked", "hyper server.rs:498", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nq\r\n2\r\nwe\r\n0\r\n\r\n", &[Is(200)], Open)
            .seen(b"transfer-encoding: chunked\r\n\r\n")
            .seen(b"we"),
        coded("chunked in mixed case", "Envoy codec_impl_test.cc:744", "Chunked", 200),
        coded("identity", "nginx-tests body_chunked.t:174 and Envoy codec_impl_test.cc:546 answer 501 (14 §4)", "identity", 400),
        coded("a coding that is not chunked", "HAProxy http_transfer_encoding.vtc:253", "gzip", 400),
        coded("a coding before chunked", "Envoy codec_impl_test.cc:565; RFC 9112 §6.1", "gzip, chunked", 501),
        coded("chunked before another coding", "HAProxy http_transfer_encoding.vtc:245", "chunked, gzip", 400),
        coded("chunked twice in one field", "HAProxy http_transfer_encoding.vtc:229", "chunked, chunked", 400),
        coded("an empty element after chunked", "Pingora common.rs:631 refuses it; RFC 9110 §5.6.1 ignores it", "chunked, ", 200),
        case("chunked in two fields", "nginx-tests body_chunked.t:176", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nTransfer-Encoding: chunked\r\n\r\n1\r\nq\r\n0\r\n\r\n", &[Is(400)], Closed),
        case("a coding and a length", "HAProxy http_transfer_encoding.vtc:165; Envoy integration_test.cc:964", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nContent-Length: 31\r\n\r\n0\r\n\r\nGET /smuggled HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(400)], Closed)
            .unseen("smuggled"),
        case("a length and a coding", "Envoy codec_impl_test.cc:2567", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 3\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", &[Is(400)], Closed),
        case("a coding from HTTP/1.0", "HAProxy http_transfer_encoding.vtc:176 reads it chunked; RFC 9112 §6.1", b"POST / HTTP/1.0\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n0\r\n\r\n", &[Is(400)], Closed),
        case("a coding hidden behind a tab", "Envoy integration_test.cc:998", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 0\r\n\ttransfer-encoding: chunked\r\n\r\n", &[Is(400)], Closed),
        case("an empty coding", "RFC 9112 §6.1", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding:\r\n\r\n", &[Is(400)], Closed),
        case("chunk extensions", "hyper decode.rs:741", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n1;a=b\r\nq\r\n0;x\r\n\r\n", &[Is(200)], Open)
            .unseen(";a=b"),
        case("whitespace around a chunk extension", "hyper decode.rs:741; RFC 9112 §7.1.1 (BWS)", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n1 ; a=b\r\nq\r\n0\r\n\r\n", &[Is(200)], Open),
        case("trailers, then the next request", "HAProxy http_transfer_encoding.vtc:275; hyper server.rs:3441", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nTE: trailers\r\n\r\n5\r\nhello\r\n0\r\nx-t: 1\r\nX-T2: \tv2\t \r\n\r\nGET /next HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200), Is(200)], Open)
            .seen(b"0\r\nx-t: 1\r\nx-t2: v2\r\n\r\n")
            .seen(b"GET /next HTTP/1.1\r\n"),
        case("trailers that framing and routing forbid", "HAProxy http_transfer_encoding.vtc:344 refuses them", b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\nTE: trailers\r\n\r\n0\r\ncontent-length: 5\r\nhost: evil\r\n\r\n", &[Is(200)], Open)
            .seen(b"\r\n\r\n0\r\n\r\n")
            .unseen("evil"),
        case("a chunked body in pieces", "Pingora body.rs:2275", b"", &[Is(200)], Open)
            .in_pieces(&[b"POST / HTTP/1.1\r\nHost: a\r\nTransfer-Encoding: chunked\r\n\r\n1", b"\r", b"\nq", b"\r\n0\r", b"\n\r", b"\n"])
            .seen(b"1\r\nq\r\n0\r\n\r\n"),
    ]
}

fn expectations() -> Vec<Case> {
    vec![
        case(
            "100-continue",
            "hyper server.rs:877",
            b"POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n",
            &[Is(100), Is(200)],
            Open,
        )
        .after_continue(b"hello")
        .seen(b"hello"),
        case(
            "100-continue in mixed case",
            "hyper server.rs:910; nginx-tests http_expect_100_continue.t:59",
            b"POST / HTTP/1.1\r\nHost: a\r\nExpect: 100-Continue\r\nContent-Length: 5\r\n\r\n",
            &[Is(100), Is(200)],
            Open,
        )
        .after_continue(b"hello"),
        case(
            "100-continue from HTTP/1.0",
            "hyper server.rs:943; RFC 9110 §15.2",
            b"",
            &[Is(200)],
            Closed,
        )
        .in_pieces(&[
            b"POST / HTTP/1.0\r\nHost: a\r\nExpect: 100-continue\r\nContent-Length: 5\r\n\r\n",
            b"hello",
        ]),
        case(
            "100-continue with no body",
            "hyper server.rs:976",
            b"GET / HTTP/1.1\r\nHost: a\r\nExpect: 100-continue\r\n\r\n",
            &[Is(200)],
            Open,
        ),
        case(
            "an expectation nobody knows",
            "nginx-tests http_expect_100_continue.t:86 wants 417; RFC 9110 §10.1.1 (MAY)",
            b"POST / HTTP/1.1\r\nHost: a\r\nExpect: unknown\r\nContent-Length: 0\r\n\r\n",
            &[Is(200)],
            Open,
        ),
    ]
}

fn connections() -> Vec<Case> {
    let pipelined: String = (0..256)
        .map(|at| format!("GET /{at} HTTP/1.1\r\nHost: a\r\n\r\n"))
        .collect();
    vec![
        case("a second request that closes", "hyper server.rs:691", b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\nGET /2 HTTP/1.1\r\nHost: a\r\nConnection: close\r\n\r\n", &[Is(200), Is(200)], Closed)
            .seen(b"GET /1 ")
            .seen(b"GET /2 "),
        case("HTTP/1.0 asking to be kept", "hyper server.rs:731", b"GET /1 HTTP/1.0\r\nHost: a\r\nConnection: keep-alive\r\n\r\nGET /2 HTTP/1.0\r\nHost: a\r\n\r\n", &[Is(200), Is(200)], Closed),
        case("HTTP/1.0 not asking", "hyper server.rs:1169", b"GET / HTTP/1.0\r\nHost: a\r\n\r\n", &[Is(200)], Closed),
        case("close among other options", "hyper conn.rs:1215", b"GET / HTTP/1.1\r\nHost: a\r\nConnection: foo, close\r\n\r\n", &[Is(200)], Closed),
        case("close in a second Connection field", "hyper conn.rs:1215", b"GET / HTTP/1.1\r\nHost: a\r\nConnection: foo\r\nConnection: Close\r\n\r\n", &[Is(200)], Closed),
        case("hop-by-hop fields", "linkerd2-proxy transparency.rs:440; Pingora test_upstream.rs:981", b"GET / HTTP/1.1\r\nHost: a\r\nx-foo: 1\r\nConnection: x-foo\r\nKeep-Alive: 5\r\nProxy-Connection: a\r\nTE: gzip\r\nUpgrade: h2c\r\n\r\n", &[Is(200)], Open)
            .unseen("x-foo")
            .unseen("keep-alive")
            .unseen("proxy-connection")
            .unseen("\r\nte:")
            .unseen("upgrade"),
        case("Connection naming Host", "Pingora test_upstream.rs:1028", b"GET / HTTP/1.1\r\nHost: a\r\nConnection: Host\r\n\r\n", &[Is(400)], Open),
        case("Connection naming the length", "Pingora test_upstream.rs:1127", b"POST / HTTP/1.1\r\nHost: a\r\nContent-Length: 5\r\nConnection: Content-Length\r\n\r\nhello", &[Is(200)], Open)
            .seen(b"content-length: 5\r\n\r\nhello"),
        case("Connection naming many fields", "Envoy codec_impl_test.cc:1246 and Pingora test_upstream.rs:1073 refuse it", b"GET / HTTP/1.1\r\nHost: a\r\nConnection: a,b,c,d,e,f,g,h,i,j,k,l,m\r\n\r\n", &[Is(200)], Open),
        case("a bad request among good ones", "Envoy integration_test.cc:1466", b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\nGE T / HTTP/1.1\r\n\r\nGET /3 HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200), Is(400)], Closed)
            .unseen("/3"),
        case("an empty line between requests", "Envoy parser_integration_test.cc:48", b"GET /1 HTTP/1.1\r\nHost: a\r\n\r\n\r\nGET /2 HTTP/1.1\r\nHost: a\r\n\r\n", &[Is(200), Is(200)], Open),
        case("256 requests in one write", "Pingora v1/server.rs:4564", pipelined.as_bytes(), &[Is(200); 256], Open)
            .seen(b"GET /0 ")
            .seen(b"GET /255 "),
        case("an upgrade from HTTP/1.0", "Pingora test_upstream.rs:1246; linkerd2-proxy upgrade.rs:299", b"GET / HTTP/1.0\r\nHost: a\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n", &[Is(200)], Closed)
            .unseen("upgrade"),
        case("an upgrade nobody serves", "HAProxy protocol_upgrade.vtc:326; Envoy integration_test.cc:2267", b"GET / HTTP/1.1\r\nHost: a\r\nUpgrade: websocket\r\nConnection: upgrade\r\n\r\n", &[Is(200)], Open)
            .unseen("upgrade"),
        case("HEAD, then GET", "HAProxy http_bodyless_response.vtc:68; nginx-tests proxy_cache.t", b"HEAD / HTTP/1.1\r\nHost: a\r\n\r\nGET /2 HTTP/1.1\r\nHost: a\r\n\r\n", &[Headed(200), Is(200)], Open)
            .seen(b"GET /2 "),
    ]
}

#[test]
fn every_case_is_answered_as_it_should_be() {
    let proxy = proxy_to(echo_upstream());
    let cases: Vec<Case> = [
        request_line(),
        targets(),
        hosts(),
        fields(),
        lengths(),
        codings(),
        expectations(),
        connections(),
    ]
    .into_iter()
    .flatten()
    .collect();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let failures: Vec<String> = runtime.block_on(async move {
        let runs: Vec<_> = cases
            .into_iter()
            .map(|case| {
                tokio::spawn(async move {
                    let outcome = tokio::time::timeout(Duration::from_secs(20), run(proxy, &case))
                        .await
                        .unwrap_or_else(|_| Err("it did not finish".to_owned()));
                    outcome
                        .err()
                        .map(|why| format!("{} ({}): {why}", case.name, case.from))
                })
            })
            .collect();
        let mut failures = Vec::new();
        for run in runs {
            if let Some(failure) = run.await.unwrap() {
                failures.push(failure);
            }
        }
        failures
    });
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
