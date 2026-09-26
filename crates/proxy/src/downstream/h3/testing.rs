//! What the HTTP/3 server's tests share: quiche's own client over a real UDP socket, driven
//! until what a test waits for has happened, and failing the test rather than hanging it
//! when it does not.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: the helpers fail a test the way the test would"
)]

use crate::downstream::h3::hq;
use crate::h3_peer::client_config;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tokio::time::Instant;

/// How long a test waits for what it waits for.
pub(crate) const PATIENCE: Duration = Duration::from_secs(10);

/// What came back on one request stream.
#[derive(Debug, Default, Clone)]
pub(crate) struct Answer {
    /// Every head, interim ones first, then the final head, then trailers.
    pub(crate) heads: Vec<Vec<(String, String)>>,
    pub(crate) body: Vec<u8>,
    pub(crate) finished: bool,
    /// The server reset the stream, with this code.
    pub(crate) reset: Option<u64>,
}

impl Answer {
    /// The `:status` of the `index`th head.
    pub(crate) fn status(&self, index: usize) -> Option<&str> {
        self.heads.get(index).and_then(|head| {
            head.iter()
                .find(|(name, _)| name == ":status")
                .map(|(_, value)| value.as_str())
        })
    }

    /// The status of the final head: the first that is not 1xx.
    pub(crate) fn final_status(&self) -> Option<&str> {
        (0..self.heads.len())
            .filter_map(|index| self.status(index))
            .find(|status| !status.starts_with('1'))
    }
}

/// quiche's HTTP/3 client, over a UDP socket of its own.
pub(crate) struct Client {
    socket: UdpSocket,
    local: SocketAddr,
    pub(crate) quic: quiche::Connection,
    pub(crate) h3: Option<quiche::h3::Connection>,
    pub(crate) answers: HashMap<u64, Answer>,
    /// The GOAWAY the server sent, if it sent one.
    pub(crate) goaway: Option<u64>,
    /// Every datagram the server sent, in order.
    pub(crate) received: Vec<Vec<u8>>,
    /// Where datagrams go instead of the server's address, when a test sends them astray.
    pub(crate) send_to: Option<SocketAddr>,
    /// The next datagram from the server is lost on its way: received, and never handed
    /// to quiche.
    pub(crate) lose_next: bool,
    /// The stream the next HTTP/0.9 request goes on.
    hq_next: u64,
    /// HTTP/0.9 answers are left unread, as by a client that takes no more.
    pub(crate) hq_unread: bool,
}

impl Client {
    /// A client of `server` asking for `name`, its handshake not yet begun.
    pub(crate) async fn new(server: SocketAddr, name: &str) -> Self {
        Self::offering(server, name, quiche::h3::APPLICATION_PROTOCOL).await
    }

    /// The same, offering `protocols` in its handshake.
    async fn offering(server: SocketAddr, name: &str, protocols: &[&[u8]]) -> Self {
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let local = socket.local_addr().unwrap();
        let mut scid = [0; 16];
        boring::rand::rand_bytes(&mut scid).unwrap();
        let mut config = client_config();
        config.set_application_protos(protocols).unwrap();
        // A head of a hundred kilobytes goes in one call or not at all: room for it from
        // the start, which a loopback has anyway.
        config.set_initial_congestion_window_packets(1_000);
        let quic = quiche::connect(
            Some(name),
            &quiche::ConnectionId::from_ref(&scid),
            local,
            server,
            &mut config,
        )
        .unwrap();
        Self {
            socket,
            local,
            quic,
            h3: None,
            answers: HashMap::new(),
            goaway: None,
            received: Vec::new(),
            send_to: None,
            lose_next: false,
            hq_next: 0,
            hq_unread: false,
        }
    }

    /// A client of `server` asking for `name`, through its handshake, speaking HTTP/3.
    pub(crate) async fn connect(server: SocketAddr, name: &str) -> Self {
        let mut client = Self::new(server, name).await;
        client.until(|client| client.quic.is_established()).await;
        let config = quiche::h3::Config::new().unwrap();
        client.h3 =
            Some(quiche::h3::Connection::with_transport(&mut client.quic, &config).unwrap());
        client.flush().await;
        client
    }

    /// A client of `server` asking for `name`, through its handshake, speaking HTTP/0.9 as
    /// quic-interop-runner's clients do.
    pub(crate) async fn connect_hq(server: SocketAddr, name: &str) -> Self {
        let mut client = Self::offering(server, name, &[hq::ALPN]).await;
        client.until(|client| client.quic.is_established()).await;
        client
    }

    /// Drives the connection until `done` says so; panics after [`PATIENCE`].
    pub(crate) async fn until(&mut self, done: impl Fn(&Self) -> bool) {
        let started = Instant::now();
        loop {
            self.flush().await;
            if done(self) {
                return;
            }
            assert!(
                started.elapsed() < PATIENCE,
                "waited for something that never happened"
            );
            self.turn(Duration::from_millis(20)).await;
        }
    }

    /// Drives the connection for `span`, whatever happens meanwhile.
    pub(crate) async fn for_a_while(&mut self, span: Duration) {
        let until = Instant::now() + span;
        while Instant::now() < until {
            self.flush().await;
            self.turn(
                until
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(20)),
            )
            .await;
        }
    }

    /// Hears the server for `span` without answering it: a handshake goes no further.
    pub(crate) async fn hear_for(&mut self, span: Duration) {
        let until = Instant::now() + span;
        while Instant::now() < until {
            self.turn(
                until
                    .saturating_duration_since(Instant::now())
                    .min(Duration::from_millis(20)),
            )
            .await;
        }
    }

    /// Reads what comes within `wait`, or quiche's own timeout if that is sooner.
    async fn turn(&mut self, wait: Duration) {
        let wait = self
            .quic
            .timeout()
            .map_or(wait, |timeout| timeout.min(wait));
        let mut buf = vec![0; 65_535];
        match tokio::time::timeout(wait, self.socket.recv_from(&mut buf)).await {
            Ok(Ok((len, from))) => {
                self.received.push(buf[..len].to_vec());
                if std::mem::take(&mut self.lose_next) {
                    return;
                }
                let info = quiche::RecvInfo {
                    from,
                    to: self.local,
                };
                let _ = self.quic.recv(&mut buf[..len], info);
            }
            Ok(Err(_)) => {}
            Err(_) => self.quic.on_timeout(),
        }
        self.events();
    }

    /// Sends what quiche wants sent.
    pub(crate) async fn flush(&mut self) {
        let mut out = vec![0; 1_500];
        loop {
            match self.quic.send(&mut out) {
                Ok((len, info)) => {
                    let to = self.send_to.unwrap_or(info.to);
                    let _ = self.socket.send_to(&out[..len], to).await;
                }
                Err(_) => return,
            }
        }
    }

    /// Goes on from a new port, as a NAT that rebinds the client moves it: quiche on this
    /// side does not know, and the server sees a new address.
    pub(crate) async fn rebind(&mut self) {
        self.socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    }

    /// Sends `datagram` as it is.
    pub(crate) async fn send_raw(&self, datagram: &[u8], to: SocketAddr) {
        self.socket.send_to(datagram, to).await.unwrap();
    }

    /// Reads datagrams for `wait`, handing none of them to quiche.
    pub(crate) async fn read_raw(&mut self, wait: Duration) -> Vec<Vec<u8>> {
        let mut read = Vec::new();
        let mut buf = vec![0; 65_535];
        while let Ok(Ok((len, _))) =
            tokio::time::timeout(wait, self.socket.recv_from(&mut buf)).await
        {
            read.push(buf[..len].to_vec());
        }
        read
    }

    fn events(&mut self) {
        if self.quic.application_proto() == hq::ALPN {
            self.hq_events();
            return;
        }
        let Some(h3) = self.h3.as_mut() else {
            return;
        };
        loop {
            match h3.poll(&mut self.quic) {
                Ok((id, quiche::h3::Event::Headers { list, .. })) => {
                    let head = list
                        .iter()
                        .map(|field| {
                            use quiche::h3::NameValue;
                            (
                                String::from_utf8_lossy(field.name()).into_owned(),
                                String::from_utf8_lossy(field.value()).into_owned(),
                            )
                        })
                        .collect();
                    self.answers.entry(id).or_default().heads.push(head);
                }
                Ok((id, quiche::h3::Event::Data)) => {
                    let mut buf = vec![0; 65_535];
                    while let Ok(read) = h3.recv_body(&mut self.quic, id, &mut buf) {
                        self.answers
                            .entry(id)
                            .or_default()
                            .body
                            .extend_from_slice(&buf[..read]);
                    }
                }
                Ok((id, quiche::h3::Event::Finished)) => {
                    self.answers.entry(id).or_default().finished = true;
                }
                Ok((id, quiche::h3::Event::Reset(code))) => {
                    self.answers.entry(id).or_default().reset = Some(code);
                }
                Ok((id, quiche::h3::Event::GoAway)) => self.goaway = Some(id),
                Ok((_, quiche::h3::Event::PriorityUpdate)) => {}
                Err(_) => return,
            }
        }
    }

    /// Reads each stream as an HTTP/0.9 client does: its bytes are the answer, and its end
    /// the answer's.
    fn hq_events(&mut self) {
        if self.hq_unread {
            return;
        }
        let readable: Vec<u64> = self.quic.readable().collect();
        let mut buf = vec![0; 65_535];
        for id in readable {
            let answer = self.answers.entry(id).or_default();
            loop {
                match self.quic.stream_recv(id, &mut buf) {
                    Ok((read, fin)) => {
                        answer.body.extend_from_slice(&buf[..read]);
                        answer.finished |= fin;
                    }
                    Err(quiche::Error::StreamReset(code)) => {
                        answer.reset = Some(code);
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
    }

    /// Sends `line` on a new stream, ending the stream with it if `end`, as an HTTP/0.9
    /// client asks.
    pub(crate) fn hq_request(&mut self, line: &[u8], end: bool) -> u64 {
        let id = self.hq_next;
        self.hq_next += 4;
        assert_eq!(self.quic.stream_send(id, line, end).unwrap(), line.len());
        id
    }

    /// Sends a request with `fields`, ending the stream with its head if `end`.
    pub(crate) fn request(&mut self, fields: &[(&str, &str)], end: bool) -> u64 {
        let fields: Vec<quiche::h3::Header> = fields
            .iter()
            .map(|(name, value)| quiche::h3::Header::new(name.as_bytes(), value.as_bytes()))
            .collect();
        self.h3
            .as_mut()
            .unwrap()
            .send_request(&mut self.quic, &fields, end)
            .unwrap()
    }

    /// Sends `data` on stream `id`, as far as quiche takes it, driving the connection
    /// until it has taken all of it.
    pub(crate) async fn body(&mut self, id: u64, data: &[u8], end: bool) {
        let mut sent = 0;
        let started = Instant::now();
        loop {
            let h3 = self.h3.as_mut().unwrap();
            match h3.send_body(&mut self.quic, id, &data[sent..], end) {
                Ok(written) => sent += written,
                Err(quiche::h3::Error::Done | quiche::h3::Error::StreamBlocked) => {}
                Err(error) => panic!("send_body: {error:?}"),
            }
            if sent == data.len() && (data.is_empty() || sent > 0) {
                self.flush().await;
                return;
            }
            assert!(
                started.elapsed() < PATIENCE,
                "the server never took the body"
            );
            self.flush().await;
            self.turn(Duration::from_millis(20)).await;
        }
    }

    /// Sends trailers on stream `id`, ending it.
    pub(crate) async fn trailers(&mut self, id: u64, fields: &[(&str, &str)]) {
        let fields: Vec<quiche::h3::Header> = fields
            .iter()
            .map(|(name, value)| quiche::h3::Header::new(name.as_bytes(), value.as_bytes()))
            .collect();
        self.h3
            .as_mut()
            .unwrap()
            .send_additional_headers(&mut self.quic, id, &fields, true, true)
            .unwrap();
        self.flush().await;
    }

    /// A GET for `path` of `authority`, answered in full.
    pub(crate) async fn get(&mut self, authority: &str, path: &str) -> Answer {
        let id = self.request(&get(authority, path), true);
        self.answer(id).await
    }

    /// The answer on stream `id`, once it has finished or been reset.
    pub(crate) async fn answer(&mut self, id: u64) -> Answer {
        self.until(|client| {
            client
                .answers
                .get(&id)
                .is_some_and(|answer| answer.finished || answer.reset.is_some())
        })
        .await;
        self.answers.remove(&id).unwrap_or_default()
    }

    /// Whether the connection has closed, and if the server closed it, with what.
    pub(crate) fn closed_by_server(&self) -> Option<(bool, u64)> {
        self.quic
            .peer_error()
            .map(|error| (error.is_app, error.error_code))
    }
}

/// A GET's head for `path` of `authority`.
pub(crate) fn get<'a>(authority: &'a str, path: &'a str) -> Vec<(&'a str, &'a str)> {
    vec![
        (":method", "GET"),
        (":scheme", "https"),
        (":authority", authority),
        (":path", path),
    ]
}
