//! Where this reader and hyper's server answer the same request differently, measured
//! rather than assumed.
//!
//! Each case is sent, as bytes, to hyper's own HTTP/1 server over an in-memory connection,
//! and read by [`HeadReader`] and [`arrival`]. Both answers are asserted, so that a
//! difference cannot be lost by being written down wrongly, and a hyper upgrade that
//! changes one says so. The same cases where the two agree are asserted too: agreeing is
//! also a claim. The reasons for each difference are in
//! [14 §4](../../../../docs/14-downstream-server.md); a difference is classified by the
//! specification and policy, never settled in hyper's favour because it is hyper's.

use super::codec::{Head, HeadReader, arrival};
use crate::upstream::h1::H1Limits;
use http::{Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::body::{Bytes, Incoming};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// What this reader answers: the status of its refusal, or 200 for a request it takes.
fn ours(bytes: &[u8]) -> u16 {
    match HeadReader::default().read(bytes, &H1Limits::default()) {
        Err(error) => error.status().as_u16(),
        Ok(Head::More) => panic!("{:?} is not a whole head", String::from_utf8_lossy(bytes)),
        Ok(Head::Read { head, .. }) => match arrival(&head, &head.fields.view(bytes)) {
            Err(error) => error.status().as_u16(),
            Ok(_) => 200,
        },
    }
}

/// What hyper's server answers: the status it writes, after its service has read the whole
/// body and answered 200, or its own refusal. `0` if it closed without answering.
async fn hypers(bytes: &[u8]) -> u16 {
    let (client, server) = tokio::io::duplex(1024 * 1024);
    let service = service_fn(|request: http::Request<Incoming>| async move {
        let status = match request.into_body().collect().await {
            Ok(_) => StatusCode::OK,
            Err(_) => StatusCode::UNPROCESSABLE_ENTITY,
        };
        let mut response = Response::new(Empty::<Bytes>::new());
        *response.status_mut() = status;
        Ok::<_, Infallible>(response)
    });
    let serving =
        hyper::server::conn::http1::Builder::new().serve_connection(TokioIo::new(server), service);
    let asking = async move {
        let mut client = client;
        client.write_all(bytes).await.unwrap();
        let mut answer = Vec::new();
        let mut piece = [0; 1024];
        while !answer.windows(4).any(|window| window == b"\r\n\r\n") {
            match client.read(&mut piece).await {
                Ok(0) | Err(_) => break,
                Ok(read) => answer.extend_from_slice(&piece[..read]),
            }
        }
        answer
    };
    let (_served, answer) = tokio::time::timeout(Duration::from_secs(5), async {
        tokio::join!(serving, asking)
    })
    .await
    .expect("hyper's server neither answered nor closed");
    // `HTTP/1.1 200 OK`: the code is the three digits after the first space.
    answer
        .get(9..12)
        .and_then(|code| std::str::from_utf8(code).ok())
        .and_then(|code| code.parse().ok())
        .unwrap_or(0)
}

const GET: &str = "GET / HTTP/1.1\r\nhost: a\r\n";

/// A request of `GET`'s head with `fields` after it and `body` after the head.
fn with(fields: &str, body: &str) -> Vec<u8> {
    format!("POST / HTTP/1.1\r\nhost: a\r\n{fields}\r\n{body}").into_bytes()
}

fn many_fields(count: usize) -> Vec<u8> {
    let mut bytes = GET.as_bytes().to_vec();
    for at in 0..count {
        bytes.extend_from_slice(format!("x-{at}: v\r\n").as_bytes());
    }
    bytes.extend_from_slice(b"\r\n");
    bytes
}

/// Every case: what it is, its bytes, this reader's answer and hyper's.
fn cases() -> Vec<(&'static str, Vec<u8>, u16, u16)> {
    let long_line = format!("GET /{} HTTP/1.1\r\nhost: a\r\n\r\n", "a".repeat(9 * 1024));
    let long_head = format!("{GET}x: {}\r\n\r\n", "v".repeat(70 * 1024));
    vec![
        // Where the two agree.
        (
            "a plain request",
            format!("{GET}\r\n").into_bytes(),
            200,
            200,
        ),
        (
            "a counted body",
            with("content-length: 3\r\n", "abc"),
            200,
            200,
        ),
        (
            "a chunked body",
            with("transfer-encoding: chunked\r\n", "3\r\nabc\r\n0\r\n\r\n"),
            200,
            200,
        ),
        (
            "one empty line first",
            format!("\r\n{GET}\r\n").into_bytes(),
            200,
            200,
        ),
        (
            "a coding that is not chunked",
            with("transfer-encoding: gzip\r\n", ""),
            400,
            400,
        ),
        (
            "a coding on HTTP/1.0",
            b"POST / HTTP/1.0\r\ntransfer-encoding: chunked\r\n\r\n0\r\n\r\n".to_vec(),
            400,
            400,
        ),
        (
            "space before a colon",
            format!("{GET}x : v\r\n\r\n").into_bytes(),
            400,
            400,
        ),
        (
            "a folded line",
            format!("{GET}x: v\r\n w\r\n\r\n").into_bytes(),
            400,
            400,
        ),
        (
            "a signed length",
            with("content-length: +3\r\n", "abc"),
            400,
            400,
        ),
        (
            "two lengths that differ",
            with("content-length: 3\r\ncontent-length: 4\r\n", "abc"),
            400,
            400,
        ),
        (
            "a list of lengths that agree, in one field",
            with("content-length: 3, 3\r\n", "abc"),
            400,
            400,
        ),
        (
            "a method that is not a token",
            b"G(T / HTTP/1.1\r\nhost: a\r\n\r\n".to_vec(),
            400,
            400,
        ),
        // Invalid input, which both refuse or where ours refuses what hyper repairs.
        (
            "a length and a coding",
            with(
                "content-length: 3\r\ntransfer-encoding: chunked\r\n",
                "3\r\nabc\r\n0\r\n\r\n",
            ),
            400,
            200,
        ),
        (
            "chunked twice",
            with(
                "transfer-encoding: chunked, chunked\r\n",
                "3\r\nabc\r\n0\r\n\r\n",
            ),
            400,
            200,
        ),
        (
            "a bare newline",
            b"GET / HTTP/1.1\nhost: a\n\n".to_vec(),
            400,
            200,
        ),
        (
            "two empty lines first",
            format!("\r\n\r\n{GET}\r\n").into_bytes(),
            400,
            200,
        ),
        // Choices the specification names.
        (
            "two lengths that agree",
            with("content-length: 3\r\ncontent-length: 3\r\n", "abc"),
            400,
            200,
        ),
        (
            "a coding before chunked",
            with(
                "transfer-encoding: gzip, chunked\r\n",
                "3\r\nabc\r\n0\r\n\r\n",
            ),
            501,
            200,
        ),
        (
            "a version this does not speak",
            b"GET / HTTP/1.2\r\nhost: a\r\n\r\n".to_vec(),
            505,
            400,
        ),
        // Bounds.
        (
            "a request line past 8 KiB",
            long_line.into_bytes(),
            414,
            200,
        ),
        ("a head past 64 KiB", long_head.into_bytes(), 431, 200),
        ("127 fields", many_fields(126), 200, 431),
    ]
}

/// Each case, as both readers answer it.
#[tokio::test]
async fn each_difference_from_hypers_server_is_what_it_is_said_to_be() {
    let mut wrong = Vec::new();
    for (case, bytes, expected_ours, expected_hypers) in cases() {
        let (ours, hypers) = (ours(&bytes), hypers(&bytes).await);
        if (ours, hypers) != (expected_ours, expected_hypers) {
            wrong.push(format!(
                "{case}: ours {ours} (said {expected_ours}), hyper's {hypers} (said {expected_hypers})"
            ));
        }
    }
    assert!(wrong.is_empty(), "\n{}", wrong.join("\n"));
}
