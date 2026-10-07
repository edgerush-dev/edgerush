//! The connections of a `tcp` or `tls` listener, carried to a backend rather than served
//! (17).

use super::logged::Passing;
use super::{Connection, Ends, Worker, balance_of, connect_within};
use crate::balance::Tried;
use crate::drain::Drain;
use crate::l4::hello::{self, Hello};
use crate::metrics::{Socket, Tunnel};
use crate::proxy_protocol;
use crate::random::random;
use crate::routed::Through;
use crate::timers::Alarm;
use crate::tunnel::{Bounds as TunnelBounds, carry};
use crate::upstream::balancing::InFlight;
use crate::upstream::destination::{Aside, ReuseIdentity};
use crate::upstream::dial;
use crate::upstream::h1::blocks::Block;
use crate::upstream::h1::exchange::ExchangeError;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::Instant;

impl Worker {
    /// Carries a connection of a `tcp` or `tls` listener to a backend of its route, byte for
    /// byte, and counts how it ended ([17 §4](../../docs/17-tcp-and-tls-passthrough.md)),
    /// and logs it if its listener logs.
    /// `by_name`: the route is the one whose hostnames cover the name the ClientHello asks
    /// for, rather than the listener's one route. `ends`: the connection's as a sender's
    /// header named them, which a backend that asks is told of; none, and they are the
    /// socket's own. `after`: what the client sent after a PROXY header, read with it,
    /// which goes first. `due`: the end of the stretch from accept in which the ClientHello
    /// must come.
    pub(super) async fn pass_through(
        self: Rc<Self>,
        connection: Connection,
        mut client: TcpStream,
        by_name: bool,
        ends: Option<Ends>,
        after: Option<Block>,
        due: Instant,
    ) {
        let listener = connection.listener;
        let mut logging = self.passing(listener, &client, ends.as_ref(), due);
        let ended = self
            .carry_through(
                &connection,
                &mut client,
                by_name,
                ends,
                after,
                due,
                logging.as_deref_mut(),
            )
            .await;
        if let Some(counters) = self.proxy.metrics.listener(listener) {
            counters.tunnel(ended);
        }
        if let Some(logging) = logging {
            logging.ended(&self, ended);
        }
        drop(connection);
    }

    /// A record for a connection of `listener` from `client`, if the listener logs: from
    /// whom `ends` names, if a PROXY header did, and accepted where the stretch to `due`
    /// began. Boxed: it is held across the tunnel, in a future every connection's task has
    /// room for (14 §3).
    fn passing(
        &self,
        listener: usize,
        client: &TcpStream,
        ends: Option<&Ends>,
        due: Instant,
    ) -> Option<Box<Passing>> {
        if !self.proxy.logs.on() {
            return None;
        }
        let snapshot = self.proxy.current.load();
        let accepted = due
            .checked_sub(self.deadlines.first_request)
            .unwrap_or_else(Instant::now);
        let named = ends.map(|ends| ends.client.ip());
        Passing::start(
            &snapshot,
            listener,
            named,
            client.peer_addr().ok(),
            accepted,
        )
        .map(Box::new)
    }

    /// The tunnel. A `tls` listener reads the ClientHello into a block of the worker's,
    /// which then carries the client's bytes on, so that what was read goes to the backend
    /// first and unchanged; the tunnel gives it back once it has. What came after a PROXY
    /// header is already in a block, which goes on the same way. It drains with
    /// `connection`, and notes what it finds in `logging`.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the tunnel needs, as for `carry`"
    )]
    async fn carry_through(
        &self,
        connection: &Connection,
        client: &mut TcpStream,
        by_name: bool,
        ends: Option<Ends>,
        after: Option<Block>,
        due: Instant,
        mut logging: Option<&mut Passing>,
    ) -> Tunnel {
        let (name, hello) = if by_name {
            let mut block = match after {
                Some(block) => block,
                None => match self.blocks.borrow_mut().take() {
                    Ok(block) => block,
                    Err(_) => return Tunnel::Exhausted,
                },
            };
            match self.read_hello(client, &mut block, due).await {
                Ok(name) => {
                    if let Some(logging) = &mut logging {
                        logging.named(&name);
                    }
                    (Some(name), Some(block))
                }
                Err(ended) => {
                    self.blocks.borrow_mut().give(block);
                    return ended;
                }
            }
        } else {
            // Bytes that came with a header go first; an empty block is given back.
            let after = after.and_then(|block| {
                if block.is_empty() {
                    self.blocks.borrow_mut().give(block);
                    None
                } else {
                    Some(block)
                }
            });
            (None, after)
        };
        let routed = self.pass_route(connection.listener, name.as_deref(), logging.as_deref_mut());
        // Held until the tunnel closes: it is load on its backend for as long as it is open.
        let (endpoint, idle, _counted, route) = match routed {
            Ok(routed) => routed,
            Err(ended) => {
                if let Some(block) = hello {
                    self.blocks.borrow_mut().give(block);
                }
                return ended;
            }
        };
        if let Some(logging) = &mut logging {
            logging.connecting(endpoint.address());
        }
        self.proxy.metrics.socket(Socket::Opened);
        // Boxed: a tunnel's connect is part of its connection's future, which every
        // connection's task makes room for (14 §3).
        let connected = Box::pin(connect_within(
            self.limits.connect,
            dial::connect_counted(endpoint.address(), &self.files),
        ));
        let mut backend = match connected.await {
            Ok(backend) => backend,
            Err(failed) => {
                if let Some(block) = hello {
                    self.blocks.borrow_mut().give(block);
                }
                // Nothing but TCP here: whatever stopped it is the endpoint's to be set aside
                // for, but for the worker's own shortage of sockets (03 §6).
                let why = match &failed {
                    ExchangeError::Unconnected(unconnected) => unconnected.aside(),
                    _ => Some(Aside::Connect),
                };
                let Some(why) = why else {
                    return Tunnel::Exhausted;
                };
                endpoint.set_aside(why);
                return Tunnel::ConnectFailed;
            }
        };
        let _unset = backend.set_nodelay(true);
        // A backend that asks is told who the client is before anything of the client's
        // reaches it, at once, whether or not the client has said anything: in a protocol
        // where the server speaks first, the client waits for it (20 §4). Put ahead of what
        // the tunnel carries, it is not counted as the client's.
        let mut told = 0;
        let hello = match endpoint.proxy_protocol() {
            None => hello,
            Some(version) => {
                // The socket's own ends, where no sender named others: asked of it only
                // here, so that a tunnel nobody is told of pays nothing for them.
                let ends = match ends {
                    Some(ends) => ends,
                    None => match (client.peer_addr(), client.local_addr()) {
                        (Ok(client), Ok(local)) => Ends { client, local },
                        _ => {
                            if let Some(hello) = hello {
                                self.blocks.borrow_mut().give(hello);
                            }
                            return Tunnel::Failed;
                        }
                    },
                };
                let header = proxy_protocol::proxied(version, ends.client, ends.local);
                match self.ahead_of(header.as_bytes(), hello) {
                    Ok(first) => {
                        told = header.as_bytes().len();
                        Some(first)
                    }
                    Err(Some(hello)) => {
                        // What was read leaves no room for the header beside it in one
                        // block: the header goes on its own, and then it.
                        if tokio::io::AsyncWriteExt::write_all(&mut backend, header.as_bytes())
                            .await
                            .is_err()
                        {
                            self.blocks.borrow_mut().give(hello);
                            return Tunnel::Failed;
                        }
                        Some(hello)
                    }
                    Err(None) => return Tunnel::Exhausted,
                }
            }
        };
        let bounds = TunnelBounds {
            idle,
            drain_within: self.deadlines.drain,
            websocket: false,
        };
        let carried = carry(
            client,
            &mut backend,
            hello,
            None,
            &self.blocks,
            bounds,
            &self.timers,
            [&connection.drain, &route],
        )
        .await;
        if let Some(logging) = logging {
            logging.carried(carried, told);
        }
        carried.how.into()
    }

    /// `header`, and after it whatever `hello` holds, in one block, to be the tunnel's first
    /// write; `hello` is given back. `Err(Some(hello))` when the two do not fit in a block,
    /// and `Err(None)` when the worker has no block to give.
    fn ahead_of(&self, header: &[u8], hello: Option<Block>) -> Result<Block, Option<Block>> {
        let mut blocks = self.blocks.borrow_mut();
        let Ok(mut first) = blocks.take() else {
            return Err(hello);
        };
        let held = hello.as_ref().map_or(0, Block::len);
        if header.len() + held > first.room().len() {
            blocks.give(first);
            return Err(hello);
        }
        let room = first.room();
        room[..header.len()].copy_from_slice(header);
        if let Some(hello) = &hello {
            room[header.len()..header.len() + held].copy_from_slice(hello.data());
        }
        first.arrived(header.len() + held);
        if let Some(hello) = hello {
            blocks.give(hello);
        }
        Ok(first)
    }

    /// The route of a connection of a `tcp` or `tls` listener, from the config in force now,
    /// and a backend's endpoint, and the listener's idle bound; the route's name and the
    /// upstream's noted in `logging`.
    fn pass_route(
        &self,
        listener: usize,
        name: Option<&str>,
        logging: Option<&mut Passing>,
    ) -> Result<(Arc<ReuseIdentity>, Duration, InFlight, Rc<Drain>), Tunnel> {
        let snapshot = self.proxy.current.load();
        let Some(compiled) = snapshot.listener(listener) else {
            return Err(Tunnel::Refused);
        };
        let route = compiled
            .l4
            .as_ref()
            .and_then(|l4| l4.route(name))
            .map(|(at, route)| (at.map_or(Through::Tcp, Through::Tls), route));
        let Some((through, route)) = route else {
            return Err(Tunnel::Refused);
        };
        // A backend with no endpoint refuses its share of connections, as TLSRoute has
        // it for a backend that cannot be used.
        let picked = route.backends.pick(random());
        if let Some(logging) = logging {
            let upstream = picked.and_then(|upstream| snapshot.config.upstreams().get(upstream.0));
            logging.routed(&route.name, upstream.map(|upstream| upstream.name.as_str()));
        }
        let Some(upstream) = picked else {
            return Err(Tunnel::NoBackend);
        };
        let destinations = snapshot.destinations.of(upstream.0);
        let picked = balance_of(&self.balancing, &snapshot, upstream.0)
            .and_then(|balance| balance.pick(destinations, &Tried::default()));
        let Some((identity, counted)) = picked
            .and_then(|(at, counted)| Some((snapshot.destinations.at(upstream.0, at)?, counted)))
        else {
            return Err(Tunnel::NoBackend);
        };
        // Taken here, of the snapshot the tunnel was routed by, before anything is waited on.
        let drain = self.route_drain(snapshot.routed.key(listener, through, upstream.0));
        Ok((Arc::clone(identity), compiled.tunnel_idle, counted, drain))
    }

    /// Reads a TLS client's ClientHello into `into`, by `due`, the end of the first-request
    /// stretch, and within [`hello::LIMIT`], for the host name it asks for (17 §3). What
    /// `into` already holds is the start of it.
    async fn read_hello(
        &self,
        client: &mut TcpStream,
        into: &mut Block,
        due: Instant,
    ) -> Result<String, Tunnel> {
        let mut alarm = Alarm::new(&self.timers, None);
        std::future::poll_fn(|cx| {
            loop {
                match hello::read(into.data()) {
                    Hello::Whole(Some(name)) => return Poll::Ready(Ok(name)),
                    // Asking for no name, it asks for no route.
                    Hello::Whole(None) | Hello::Refused(_) => {
                        return Poll::Ready(Err(Tunnel::Refused));
                    }
                    Hello::More => {}
                }
                let mut read = ReadBuf::new(into.room());
                if read.remaining() == 0 {
                    return Poll::Ready(Err(Tunnel::Refused));
                }
                match Pin::new(&mut *client).poll_read(cx, &mut read) {
                    Poll::Ready(Ok(())) => {
                        let count = read.filled().len();
                        // Gone before saying enough to be routed.
                        if count == 0 {
                            return Poll::Ready(Err(Tunnel::Refused));
                        }
                        into.arrived(count);
                    }
                    Poll::Ready(Err(_)) => return Poll::Ready(Err(Tunnel::Failed)),
                    Poll::Pending => {
                        if alarm.poll_until(cx, due).is_ready() {
                            return Poll::Ready(Err(Tunnel::TooSlow));
                        }
                        return Poll::Pending;
                    }
                }
            }
        })
        .await
    }
}
