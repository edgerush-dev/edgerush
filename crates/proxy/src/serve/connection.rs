//! A client's connection, from its accept to its close: its PROXY header, its TLS, and the
//! HTTP it turns out to speak, over TCP or over QUIC.

use super::{Connection, Ends, Worker, serve_h1, serve_h2, serve_tls};
use crate::downstream::detect::{Protocol, detect};
use crate::downstream::h3::listener::Forwarding;
use crate::downstream::h3::{self, listener as h3_listener};
use crate::drain::Drain;
use crate::forwarding::Client;
use crate::linger::{self, Lent, linger};
use crate::metrics::ProxyHeader;
use crate::proxy_protocol::{self, Header as Said, Read as HeaderRead};
use crate::request_body::RequestBody;
use crate::timers::Alarm;
use crate::tls::Tls;
use crate::upstream::h1::blocks::Block;
use edgerush_config::L4;
use http::Request;
use std::cell::Cell;
use std::future::{Future, poll_fn};
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::time::Instant;

impl Worker {
    /// Serves the connections that come in on `socket` as those of the listener at
    /// position `listener` of [`Proxy::listeners`], HTTP/1.1 and HTTP/2 alike. Never
    /// returns; dropping the future stops accepting, and connections already accepted
    /// carry on.
    ///
    /// It accepts whatever comes, with no bound on how many connections it holds: it is
    /// for tests and single-worker harnesses. The data plane's workers accept through
    /// their own loop, which stops at each worker's cap
    /// ([14 §8](../../docs/14-downstream-server.md)).
    ///
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet`, where the connections it accepts are served;
    /// without one there is nowhere to put them and the first connection panics.
    ///
    /// [`Proxy::listeners`]: super::Proxy::listeners
    pub async fn serve(self: Rc<Self>, listener: usize, socket: TcpListener) {
        // Draining: nothing new is taken, and the socket goes with this.
        while let Some(accepted) = self.accept(&socket).await {
            match accepted {
                Ok(stream) => {
                    let connection = Rc::clone(&self).serve_connection(listener, stream);
                    let _detached = tokio::task::spawn_local(connection);
                }
                Err(error) => {
                    if let Some(pause) = self.proxy.accept_failed(listener, &error) {
                        tokio::time::sleep(pause).await;
                    }
                }
            }
        }
    }

    /// Serves one connection, to its end, as one of the listener at position `listener`
    /// of [`Proxy::listeners`]. It may have been accepted anywhere — by another thread,
    /// which then hands it over as a socket of the standard library — as long as `stream`
    /// was made on the runtime that runs this.
    ///
    /// # Panics
    ///
    /// Runs inside the worker's `LocalSet`, where the engine's own futures go. An
    /// HTTP/2 connection
    /// panics without one, as the engine spawns a future for every stream.
    ///
    /// [`Proxy::listeners`]: super::Proxy::listeners
    pub async fn serve_connection(self: Rc<Self>, listener: usize, stream: TcpStream) {
        // Worth having, not worth refusing a connection over.
        let _unset = stream.set_nodelay(true);
        let (tls, passthrough, reads_header, drain) = {
            let snapshot = self.proxy.current.load();
            let compiled = snapshot.listener(listener);
            // The TLS of the config the connection came in under, which it keeps to its end,
            // and the drain of the client validation it is accepted under, which a reload
            // that replaces the validation starts (03 §3).
            let tls = snapshot.tls.get(listener).cloned().flatten();
            let drain = self.drain_for(listener, tls.as_ref());
            // A `tcp` or `tls` listener's connections are carried, not served (17).
            let passthrough = compiled
                .and_then(|compiled| compiled.l4.as_ref())
                .map(|l4| matches!(l4, L4::Tls(_)));
            let reads_header = compiled.is_some_and(|compiled| compiled.proxy_senders.is_some());
            (tls, passthrough, reads_header, drain)
        };
        // A connection that starts with a PROXY header is served, or carried, after it, in a
        // future of its own: boxed, and settled first, so that nothing here is held across
        // the wait for it and no other connection's future is the larger for it (14 §3).
        if reads_header {
            return Box::pin(self.after_header(listener, stream, tls, passthrough, drain)).await;
        }
        if let Some(by_name) = passthrough {
            let connection = Connection::open(Rc::clone(&self), listener, drain);
            let due = Instant::now() + self.deadlines.first_request;
            // Its own ends, asked of the socket only if a backend is to be told of them.
            return self
                .pass_through(connection, stream, by_name, None, None, due)
                .await;
        }
        let connection = Rc::new(Connection::open(Rc::clone(&self), listener, drain));
        // Who the upstream is told the client is, written once for all its requests. Gone
        // only if the client already is.
        let Ok(client) = stream
            .peer_addr()
            .map(|peer| Rc::new(Client::connected(peer.ip(), peer)))
        else {
            return;
        };
        // Lent rather than given, so that it comes back once the engine is done with it.
        let (lent, back) = Lent::new(stream);
        let due = Instant::now() + self.deadlines.first_request;
        self.serve_settled(connection, tls, client, lent, back, due)
            .await;
    }

    /// Serves a connection as HTTP, from accept to close, once it is known whose it is:
    /// `client`'s, its socket `lent` to the engine and coming back through `back`. `due`:
    /// the end of the stretch from accept in which its first request must come.
    async fn serve_settled(
        &self,
        connection: Rc<Connection>,
        tls: Option<Arc<Tls>>,
        client: Rc<Client>,
        lent: Lent,
        back: linger::Returned,
        due: Instant,
    ) {
        let deadlines = self.deadlines;
        // Set when the engine hands over the first request, which is the end of the one
        // stretch its own deadlines do not cover.
        let asked = Rc::new(Cell::new(false));
        let (ours, ours_asking) = (Rc::clone(&connection), Rc::clone(&asked));
        // Each served by our own server: HTTP/1 by the one of 14, HTTP/2 over h2 (15 step 2).
        // A task is as large as its future's largest state, from accept to close, whatever
        // the connection turns out to be (14 §3). So only plain HTTP/1, which waits between
        // requests holding little, is served inline; HTTP/2 and TLS, which hold much more
        // anyway, are boxed once when the connection is found to be one of them.
        let serving = async move {
            match tls {
                // Told apart by our own detector.
                None => match detect(lent).await {
                    Ok(Some((Protocol::Http1, replay))) => {
                        serve_h1(ours, client, ours_asking, replay).await;
                    }
                    Ok(Some((Protocol::Http2, replay))) => {
                        Box::pin(serve_h2(ours, client, ours_asking, deadlines, replay)).await;
                    }
                    // Closed having said nothing, or failed before saying enough.
                    Ok(None) | Err(_) => {}
                },
                Some(tls) => {
                    Box::pin(serve_tls(ours, client, ours_asking, deadlines, &tls, lent)).await;
                }
            }
        };
        // From accept to the first request, whichever server takes the connection: the
        // detector and the engine's HTTP/2 server wait for bytes with no deadline of their
        // own, so a connection that never says anything, or stops part way through the
        // HTTP/2 preface, is bounded here.
        let cut_off = {
            let mut serving = std::pin::pin!(serving);
            let mut first = std::pin::pin!(tokio::time::sleep_until(due));
            std::future::poll_fn(|cx| {
                // An error here is the end of one connection: the peer went away or spoke
                // nonsense. There is nobody to tell.
                if serving.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(false);
                }
                if !asked.get() && first.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(true);
                }
                Poll::Pending
            })
            .await
        };
        if let Some(stream) = back.take() {
            if cut_off {
                // It has been sent nothing, so there is no answer a reset could take with
                // it, and lingering would only hold the connection longer.
                drop(stream);
            } else {
                // Dropped with bytes the client sent still unread — the rest of an upload
                // the engine never read — the connection would be reset, and the reset can
                // take the answer the client has not read yet with it. So it lingers
                // instead.
                linger(stream, linger::QUIET, linger::MOST).await;
            }
        }
    }

    /// Serves or carries a connection of a listener with senders after the PROXY header it
    /// starts with (20 §3), its ends as a believed header names them. `tls` and
    /// `passthrough` are its listener's, as `serve_connection` found them.
    async fn after_header(
        self: &Rc<Self>,
        listener: usize,
        mut stream: TcpStream,
        tls: Option<Arc<Tls>>,
        passthrough: Option<bool>,
        drain: Rc<Drain>,
    ) {
        // From accept to the first request: one stretch, the header included.
        let due = Instant::now() + self.deadlines.first_request;
        let connection = Connection::open(Rc::clone(self), listener, drain);
        // Gone only if the client already is.
        let Ok(peer) = stream.peer_addr() else {
            return;
        };
        // Whether its sender is believed: settled by who connected, before a byte is read.
        let believed = self
            .proxy
            .current
            .load()
            .listener(listener)
            .and_then(|compiled| compiled.proxy_senders.as_ref())
            .is_some_and(|senders| senders.trusts(peer.ip()));
        let Ok((said, mut after)) = self
            .receive_header(listener, &mut stream, believed, due)
            .await
        else {
            return;
        };
        // Its ends as a sender named them; none, and they are its own, the socket's, which
        // a tunnel asks for only if a backend is to be told of them.
        if let Some(by_name) = passthrough {
            return Rc::clone(self)
                .pass_through(connection, stream, by_name, said, Some(after), due)
                .await;
        }
        let (mut lent, back) = Lent::new(stream);
        // What came after the header is the start of what the engine reads.
        let length = after.len();
        lent.read_first(after.take_frame(0..length, length));
        self.blocks.borrow_mut().give(after);
        let client = said.map_or(peer, |ends| ends.client);
        let client = Rc::new(Client::connected(client.ip(), peer));
        self.serve_settled(Rc::new(connection), tls, client, lent, back, due)
            .await;
    }

    /// Reads the PROXY protocol header a connection of a listener with senders starts with
    /// ([20 §3](../../docs/20-proxy-protocol.md)), by `due`, into a block of the worker's
    /// that then holds whatever came after it, to be read first. A header longer than what
    /// is needed to read it (TLVs) is skipped by count, never held. `believed`: whether the
    /// connection's peer is among the senders, whose header's addresses are its ends. What
    /// came of the header is counted; `Err` is a connection to close.
    async fn receive_header(
        &self,
        listener: usize,
        client: &mut TcpStream,
        believed: bool,
        due: Instant,
    ) -> Result<(Option<Ends>, Block), ()> {
        let counted = |outcome| {
            if let Some(counters) = self.proxy.metrics.listener(listener) {
                counters.proxy_header(outcome);
            }
        };
        let Ok(mut block) = self.blocks.borrow_mut().take() else {
            return Err(());
        };
        let mut alarm = Alarm::new(&self.timers, None);
        let mut said = None;
        // What of the header is still to be read and dropped, once it has been read.
        let mut skip = 0;
        let read = poll_fn(|cx| {
            loop {
                if said.is_none() {
                    match proxy_protocol::read(block.data()) {
                        HeaderRead::More => {}
                        HeaderRead::Whole { header, length } => {
                            let have = block.len().min(length);
                            block.consume(have);
                            skip = length - have;
                            said = Some(header);
                        }
                        HeaderRead::Refused(refusal) if refusal.is_missing() => {
                            return Poll::Ready(Err(ProxyHeader::Missing));
                        }
                        HeaderRead::Refused(_) => {
                            return Poll::Ready(Err(ProxyHeader::Malformed));
                        }
                    }
                }
                if let Some(header) = said
                    && skip == 0
                {
                    return Poll::Ready(Ok(header));
                }
                // A header is decided within its first 232 bytes, and once it is the block
                // holds nothing but what was read since: there is always room.
                let mut buffer = ReadBuf::new(block.room());
                if buffer.remaining() == 0 {
                    return Poll::Ready(Err(ProxyHeader::Malformed));
                }
                match Pin::new(&mut *client).poll_read(cx, &mut buffer) {
                    Poll::Ready(Ok(())) => {
                        let count = buffer.filled().len();
                        if count == 0 {
                            return Poll::Ready(Err(ProxyHeader::Closed));
                        }
                        block.arrived(count);
                        if said.is_some() {
                            let dropped = count.min(skip);
                            block.consume(dropped);
                            skip -= dropped;
                        }
                    }
                    Poll::Ready(Err(_)) => return Poll::Ready(Err(ProxyHeader::Closed)),
                    Poll::Pending => {
                        if alarm.poll_until(cx, due).is_ready() {
                            return Poll::Ready(Err(ProxyHeader::TooSlow));
                        }
                        return Poll::Pending;
                    }
                }
            }
        })
        .await;
        let ends = match read {
            Err(outcome) => {
                counted(outcome);
                self.blocks.borrow_mut().give(block);
                return Err(());
            }
            Ok(_) if !believed => {
                counted(ProxyHeader::Untrusted);
                None
            }
            Ok(Said::Local) => {
                counted(ProxyHeader::Local);
                None
            }
            Ok(Said::Proxied {
                source,
                destination,
            }) => {
                counted(ProxyHeader::Accepted);
                Some(Ends {
                    client: source,
                    local: destination,
                })
            }
        };
        Ok((ends, block))
    }

    /// Sets up HTTP/3 on `socket`, the UDP socket of the listener at position `listener` of
    /// [`Proxy::listeners`], and returns what serves it until the worker drains and its last
    /// connection has gone ([16](../../docs/16-http3.md)). Set up at once, so that whoever
    /// starts the worker knows before anything is served whether the listener has HTTP/3.
    /// The listener's TLS, and whether it has every client prove its address first, are
    /// those of the config in force when a connection comes. `forwarding` is this worker's
    /// share of the listener's [`Forwarding`] group: where a datagram for another worker's
    /// connection is handed, and where this worker's are handed to it.
    ///
    /// # Errors
    ///
    /// A socket with no address of its own, or BoringSSL failing to set up what the
    /// connection IDs are made with.
    ///
    /// # Panics
    ///
    /// What it returns runs inside the worker's `LocalSet`, where every connection and
    /// request is a task.
    ///
    /// [`Proxy::listeners`]: super::Proxy::listeners
    pub fn serve_h3(
        self: Rc<Self>,
        listener: usize,
        socket: UdpSocket,
        forwarding: Forwarding,
    ) -> io::Result<impl Future<Output = ()> + use<>> {
        let deadlines = self.deadlines;
        let settings = h3::Settings {
            first_request: deadlines.first_request,
            keep_alive: deadlines.next_request,
            stream_idle: deadlines.idle,
            drain_within: deadlines.drain,
            ..h3::Settings::default()
        };
        let counting = Arc::clone(&self.proxy);
        let count = Box::new(move |event| {
            if let Some(counters) = counting.metrics.listener(listener) {
                counters.quic(event);
            }
        });
        let room = self.connections.as_ref().map(|loads| h3_listener::Room {
            loads: Arc::clone(loads),
            worker: usize::from(self.position),
            listener,
        });
        let refusing = Rc::clone(&self);
        let refused = Box::new(move |client: Rc<Client>| {
            let status = http::StatusCode::REQUEST_HEADER_FIELDS_TOO_LARGE;
            let protocol = Some(edgerush_telemetry::access_log::Protocol::Http3);
            super::refused(
                &refusing,
                listener,
                &client,
                protocol,
                status,
                "head_too_long",
            );
        });
        let closing = Arc::clone(&self.proxy);
        let closed = Box::new(move |why| {
            if let Some(counters) = closing.metrics.listener(listener) {
                counters.closed(why);
            }
        });
        let shared = h3_listener::Shared::new(
            socket,
            settings,
            &self.proxy.quic,
            self.position,
            Rc::clone(&self.timers),
            Rc::clone(&self.drain),
            Rc::clone(self.blocks.borrow().storage()),
            Rc::clone(&self.handshakes),
            room,
            count,
            refused,
            closed,
        )
        .map_err(io::Error::other)?;
        let reading = Rc::clone(&self);
        let in_force = move || {
            let snapshot = reading.proxy.current.load();
            let tls = snapshot.tls.get(listener).cloned().flatten()?;
            let force_retry = snapshot
                .listener(listener)
                .and_then(|listener| listener.http3)
                .is_some_and(|http3| http3.force_retry);
            // Read on this thread at the client's first packet, as a TCP connection reads it
            // at its accept.
            let drain = reading.drain_for(listener, Some(&tls));
            Some(h3_listener::InForce {
                tls,
                force_retry,
                drain,
            })
        };
        let answering = Rc::clone(&self);
        let respond = Rc::new(
            move |request: Request<RequestBody>, interim, client: Rc<Client>| {
                Rc::clone(&answering).handle(listener, client, request, Some(interim))
            },
        );
        let dating = Rc::clone(&self);
        let date = Rc::new(move || dating.date.get());
        let opening = Rc::clone(&self);
        let opened = move |drain: &Rc<Drain>| {
            Connection::open(Rc::clone(&opening), listener, Rc::clone(drain))
        };
        Ok(h3_listener::serve(
            Rc::new(shared),
            in_force,
            respond,
            date,
            opened,
            forwarding,
        ))
    }
}
