//! One try at an answer from an endpoint, over HTTP/1 or HTTP/2, and the WebSocket that
//! a 101 or an extended CONNECT switches it to.

use super::{
    Admitted, Body, Directed, Handshake, Timing, Toward, Watch, Worker, connect_within, why_stopped,
};
use crate::downstream::h1::connection::Answered;
use crate::gathered::Gathered;
use crate::head::Forwarded;
use crate::interim::Interim;
use crate::metrics::{Answer, Socket, Stopped};
use crate::raw::RawAnswer;
use crate::request_body::{RequestBody, RequestBodyError};
use crate::timers::{Alarm, Timers};
use crate::tunnel::{Backend, Bounds as TunnelBounds, Carried, Switched};
use crate::upstream::balancing::InFlight;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::h1::blocks::Block;
use crate::upstream::h1::codec::{OutgoingFields, Sending};
use crate::upstream::h1::exchange::{Exchange, ExchangeError, H1Body, Stalled, nothing_to_say};
use crate::upstream::h1::pool::Lease;
use crate::upstream::h2::client::PlaceError;
use crate::upstream::h2::exchange::{self as h2_exchange, Bounds as H2Bounds, Connected};
use crate::upstream::secure::Socket as UpstreamSocket;
use crate::websocket;
use bytes::Bytes;
use edgerush_config::UpstreamProtocol;
use http::{HeaderName, Method, Response, StatusCode, Uri};
use http_body::Body as HttpBody;
use std::cell::Cell;
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use tokio::net::TcpStream;
use tokio::time::Instant;

impl Worker {
    /// Sends a request by EdgeRush's own path and returns the answer's head and body.
    ///
    /// A connection comes out of the pool where there is one to use, and is opened where
    /// there is not; either way the answer's body carries the way back, and the
    /// connection returns only if the body earns it ([13 §6](../../docs/13-http1-upstream.md)).
    ///
    /// # Errors
    ///
    /// Anything the upstream said that cannot be read, or a connection that could not be
    /// opened, failed or closed without answering.
    #[expect(
        clippy::too_many_arguments,
        reason = "each of them is a different thing an exchange needs, and a struct \n                  to hold them would be indirection for a lint rather than for a reader"
    )]
    pub(super) async fn through_h1<F, B>(
        &self,
        identity: &Arc<ReuseIdentity>,
        method: &Method,
        uri: &Uri,
        headers: &F,
        nominated: &[HeaderName],
        sending: Sending,
        body: B,
        interim: Option<Interim>,
        head_by_rule: bool,
        upgrading: bool,
    ) -> Result<(RawAnswer, Box<H1Body<UpstreamSocket, B>>), ExchangeError>
    where
        F: OutgoingFields + ?Sized,
        B: HttpBody<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Bound in its own statement, so the pool is not still borrowed when the connect
        // below is waited on.
        let mut kept = None;
        loop {
            let found = self.pool.borrow_mut().take(identity, &self.limits);
            let Some((mut socket, opened)) = found else {
                break;
            };
            // Quiet when it was put back is not quiet now. Anything readable is an
            // upstream that has said something nobody asked for, and an answer to the
            // last request must never be handed to the next one; the connection goes
            // and another is tried.
            if nothing_to_say(&mut socket) {
                kept = Some((socket, opened));
                break;
            }
            // Anything readable is an upstream saying something nobody asked for, and the
            // socket goes rather than being lent again.
            self.proxy.metrics.socket(Socket::Discarded);
        }
        let (socket, opened) = match kept {
            Some(reused) => {
                self.proxy.metrics.socket(Socket::Reused);
                reused
            }
            None => {
                self.proxy.metrics.socket(Socket::Opened);
                let secure = identity.secure().cloned();
                // Whether TCP got through: only a connect that did not is the endpoint set
                // aside for, not a handshake that failed after it (03 §6).
                let connected = Cell::new(false);
                // One bound for the connection and its handshake together.
                let opening = async {
                    let socket = TcpStream::connect(identity.address()).await?;
                    connected.set(true);
                    // Worth having, not worth refusing an upstream over.
                    let _unset = socket.set_nodelay(true);
                    match secure {
                        None => Ok(UpstreamSocket::Plain(socket)),
                        Some(secure) => secure
                            .connect(socket)
                            .await
                            .map(|secured| UpstreamSocket::Secured(Gathered::new(secured))),
                    }
                };
                let opened = connect_within(self.limits.connect, opening).await;
                if opened.is_err() && !connected.get() {
                    identity.set_aside();
                }
                (opened?, Instant::now())
            }
        };

        let mut exchange = Exchange::new(socket, Rc::clone(&self.blocks), Rc::clone(&self.timers));
        if let Some(interim) = interim {
            exchange = exchange.heard_by(interim);
        }
        if head_by_rule {
            exchange = exchange.head_bounded_elsewhere();
        }
        if upgrading {
            exchange = exchange.upgrading();
        }
        let (answer, rest) = exchange
            .send(method, uri, headers, nominated, sending, body, &self.limits)
            .await?;

        // The request may still be going out; what is left of it goes with the body,
        // which drives it while the client reads the answer. Boxed where it is made, so
        // that only a pointer to it is passed back up to where it is served.
        let lease = Lease::in_use(Arc::clone(identity), opened, &self.pool);
        let head = answer.head;
        let persistent =
            answer.delivery.persistent && !crate::upstream::auth::challenges(head.status, &head);
        let body = Box::new(
            H1Body::new(
                rest,
                answer.delivery.framing,
                persistent,
                answer.nominated,
                Rc::clone(&self.body_limits),
            )
            .returning_to(lease),
        );
        Ok((head.into_answer(), body))
    }

    /// One try at an answer from `endpoint`, by the client its protocol calls for: the
    /// upstream's answer, edited, or the reason there is none, which the caller answers
    /// with (or tries again for).
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    pub(super) async fn attempt<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
    ) -> Result<Answered<Body>, Answer> {
        if let Some(logging) = &directed.logging {
            logging.tried(endpoint.address());
        }
        let timing = timing.for_try();
        if endpoint.protocol() == UpstreamProtocol::Http2 {
            if let Some(handshake) = directed.websocket.as_deref() {
                return self
                    .connect_by_h2(
                        directed, endpoint, head, handshake, admitted, interim, timing,
                    )
                    .await;
            }
            return self
                .respond_by_h2(
                    directed,
                    endpoint,
                    head,
                    head.method(),
                    sending,
                    body,
                    admitted,
                    interim,
                    timing,
                )
                .await;
        }
        let exchanged = pin!(self.by_ours(
            directed,
            endpoint,
            head,
            nominated,
            sending,
            body,
            admitted,
            interim,
            timing.head_by_rule,
        ));
        let answered = by_deadline(&self.timers, timing.deadline, exchanged)
            .await
            .unwrap_or(Err(timing.lapsed()));

        let upstream = self.proxy.metrics.upstream(directed.upstream.slot());
        let (mut answer, body) = match answered {
            Ok(answered) => answered,
            // The worker's own storage running out, or a client's body that cannot be read,
            // is not the upstream failing, and is not counted as though it were
            // ([14 §8](../../docs/14-downstream-server.md)).
            Err(
                answer @ (Answer::Exhausted
                | Answer::BadBody
                | Answer::BodyTimedOut
                | Answer::DeadlineExceeded),
            ) => return Err(answer),
            Err(answer) => {
                if let Some(upstream) = upstream {
                    upstream.failures.inc();
                }
                return Err(answer);
            }
        };
        if let Some(upstream) = upstream {
            upstream.responded(answer.status());
        }
        // A name the answer's own `Connection` gave does not travel on, and is not declared
        // onwards either; then what is about the upstream's connection comes off, and the
        // rule's changes are made (14 §6).
        let nominated = crate::hop_by_hop::nominated(&answer);
        let changes = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref());
        // Read before the hop-by-hop fields come off: RFC 9110 §15.5.22 has a 426 name the
        // protocol it wants, and WebSocket is one the gateway can switch to (19 §2).
        let offers = answer.status() == StatusCode::UPGRADE_REQUIRED
            && directed.upgradable
            && websocket::offered(&answer);
        let edited = answer.filter_declaration(&nominated).and_then(|()| {
            answer.strip();
            changes.map_or(Ok(()), |changes| answer.apply(changes))
        });
        // A 101 says it switched to WebSocket, with the Accept of the client's own key; the
        // backend's was of the gateway's.
        let handshake = directed.websocket.as_deref();
        let upgraded = match handshake.and_then(|handshake| handshake.client.as_ref()) {
            Some(client) if answer.status() == StatusCode::SWITCHING_PROTOCOLS => answer
                .set_field(http::header::UPGRADE, websocket::WEBSOCKET)
                .and_then(|()| {
                    answer.set_field(http::header::SEC_WEBSOCKET_ACCEPT, client.accept_value())
                }),
            _ if offers => answer.set_field(http::header::UPGRADE, websocket::WEBSOCKET),
            _ => Ok(()),
        };
        if edited.is_err() || upgraded.is_err() {
            return Err(Answer::Edits);
        }
        // An extended CONNECT's client is told of the switch with a 200 (RFC 8441 §5), and
        // must never be told 2xx of anything else: to a CONNECT that opens the tunnel.
        if handshake.is_some_and(|handshake| handshake.client.is_none()) {
            return connected_answer(answer.status(), answer.into_parts(), body);
        }
        // Written in this hop's version and not the upstream's: "Intermediaries that
        // process HTTP messages ... MUST send their own HTTP-version in forwarded messages"
        // (RFC 9110 §6.2). Our writer says HTTP/1.1, and so does a map made for HTTP/2.
        Ok(Answered::Raw(answer, body))
    }

    /// The answer of an HTTP/2 upstream, edited as an HTTP/1 upstream's would be: what its
    /// `Connection` named and what is about its connection comes off — h2 lets none of
    /// the latter through, and this does not rest on it — and the rule's changes are made.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn respond_by_h2<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        method: &Method,
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
    ) -> Result<Answered<Body>, Answer> {
        let storage = Rc::clone(self.blocks.borrow().storage());
        let bounds = self.h2_bounds(timing);
        let exchanging = pin!(h2_exchange::exchange(
            &self.h2,
            endpoint,
            method,
            head.uri(),
            head.outgoing(),
            sending,
            body,
            &storage,
            interim,
            bounds,
            timing.told,
            || {
                if let Some(upstream) = self.proxy.metrics.upstream(directed.upstream.slot()) {
                    upstream.retries.inc();
                }
            },
        ));
        let upstream = self.proxy.metrics.upstream(directed.upstream.slot());
        let Some(exchanged) = by_deadline(&self.timers, timing.deadline, exchanging).await else {
            let lapsed = timing.lapsed();
            if lapsed == Answer::UpstreamTimedOut
                && let Some(upstream) = upstream
            {
                upstream.failures.inc();
            }
            return Err(lapsed);
        };
        let (parts, answer) = exchanged.map_err(|error| h2_failed(error, upstream))?;
        if let Some(upstream) = upstream {
            upstream.responded(parts.status);
        }
        let watch = Watch {
            proxy: Arc::clone(&self.proxy),
            upstream: directed.upstream.slot(),
        };
        let mut response = Response::from_parts(parts, Body::H2(Box::new(answer), admitted, watch));
        let headers = response.headers_mut();
        let nominated = crate::hop_by_hop::nominated(&*headers);
        crate::h1::filter_declaration(headers, &nominated);
        crate::hop_by_hop::strip_response(headers);
        if let Some(changes) = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(headers);
        }
        Ok(Answered::Map(response))
    }

    /// By EdgeRush's own path, the one there is.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn by_ours<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        mut admitted: Admitted,
        interim: Option<Interim>,
        head_by_rule: bool,
    ) -> Result<(RawAnswer, Body), Answer> {
        let answer = match self
            .through_h1(
                endpoint,
                head.method(),
                head.uri(),
                head.outgoing(),
                nominated,
                sending,
                body,
                interim,
                head_by_rule,
                directed.websocket.is_some(),
            )
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                self.proxy.metrics.stopped(why_stopped(&error));
                return Err(match error {
                    ExchangeError::Exhausted(_) => Answer::Exhausted,
                    ExchangeError::RequestBody(cause)
                        if matches!(
                            cause.downcast_ref::<RequestBodyError>(),
                            Some(RequestBodyError::TimedOut)
                        ) =>
                    {
                        Answer::BodyTimedOut
                    }
                    ExchangeError::RequestBody(_) => Answer::BadBody,
                    // The client's upload stopping, seen by the exchange's clock for it
                    // rather than the body's own: the same client, as slow either way.
                    ExchangeError::Idle {
                        waiting: Stalled::Client,
                        ..
                    } => Answer::BodyTimedOut,
                    // Only what the upstream was to do before its head. The client's own
                    // upload stopping is not the upstream out of time.
                    ExchangeError::TooSlow { .. }
                    | ExchangeError::Idle {
                        waiting: Stalled::Upstream | Stalled::Answer,
                        ..
                    } => Answer::UpstreamTimedOut,
                    ExchangeError::Unconnected(_) => Answer::Unreachable,
                    _ => Answer::UpstreamFailed,
                });
            }
        };
        let (read, mut body) = answer;
        if let Some(handshake) = &directed.websocket {
            if read.status() == StatusCode::SWITCHING_PROTOCOLS {
                return self.switch(read, *body, handshake, admitted.count());
            }
            // A refused handshake's connection could carry another request, but not one
            // whose credentials may have bound it to this client (13 §6).
            if crate::upstream::auth::carries_credentials(head.outgoing()) {
                body.not_kept();
            }
        }
        // Nothing need ever poll an empty body, so its connection would otherwise sit
        // until the body object was dropped.
        if body.is_end_stream() {
            body.settle();
        }
        // The place goes with the body, which is what is still being worked on. Every
        // other way out of here has dropped it already.
        let watch = Watch {
            proxy: Arc::clone(&self.proxy),
            upstream: directed.upstream.slot(),
        };
        Ok((read, Body::Ours(body, admitted, watch)))
    }

    /// A handshake's 101 (19 §2). Only one that says it switched to the WebSocket asked for,
    /// with the Accept of the gateway's own key, is taken: its connection goes to the server
    /// that read the handshake, left with its interim channel, and the 101 goes on with
    /// nothing after it. Any other is the backend failing, and its connection is closed.
    fn switch(
        &self,
        read: RawAnswer,
        body: H1Body<UpstreamSocket, RequestBody>,
        handshake: &Handshake,
        counted: Option<InFlight>,
    ) -> Result<(RawAnswer, Body), Answer> {
        let Toward::Upgrade(ours) = &handshake.toward else {
            return Err(Answer::UpstreamFailed);
        };
        let switched = handshake
            .server
            .as_ref()
            .filter(|_| websocket::switched(&read, ours));
        let Some((interim, (backend, leftover))) = switched.zip(body.into_switched()) else {
            self.proxy.metrics.stopped(Stopped::Codec);
            return Err(Answer::UpstreamFailed);
        };
        interim.switch(self.switched(Backend::Socket(backend), leftover, handshake, counted));
        Ok((read, Body::Empty))
    }

    /// A switched WebSocket's backend, with what its tunnel needs of this worker, and its
    /// end counted among its listener's tunnels.
    fn switched(
        &self,
        backend: Backend,
        leftover: Option<Block>,
        handshake: &Handshake,
        counted: Option<InFlight>,
    ) -> Switched {
        let proxy = Arc::clone(&self.proxy);
        let listener = handshake.listener;
        Switched {
            backend,
            leftover,
            bounds: TunnelBounds {
                idle: handshake.idle,
                drain_within: self.deadlines.drain,
                websocket: true,
            },
            blocks: Rc::clone(&self.blocks),
            timers: Rc::clone(&self.timers),
            route: self.route_drain(handshake.route),
            ended: Box::new(move |carried: Carried| {
                if let Some(counters) = proxy.metrics.listener(listener) {
                    counters.tunnel(carried.into());
                }
            }),
            counted,
            held: handshake.held.take(),
        }
    }

    /// A WebSocket handshake to an HTTP/2 backend: an extended CONNECT if the connection
    /// announces them, and otherwise the plain request it came as, a GET (19 §4).
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    async fn connect_by_h2<H: Forwarded>(
        &self,
        directed: &Directed,
        endpoint: &Arc<ReuseIdentity>,
        head: &H,
        handshake: &Handshake,
        mut admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
    ) -> Result<Answered<Body>, Answer> {
        let storage = Rc::clone(self.blocks.borrow().storage());
        let bounds = self.h2_bounds(timing);
        let connecting = pin!(h2_exchange::connect(
            &self.h2,
            endpoint,
            head.uri(),
            head.outgoing(),
            &storage,
            bounds,
        ));
        let upstream = self.proxy.metrics.upstream(directed.upstream.slot());
        let Some(connected) = by_deadline(&self.timers, timing.deadline, connecting).await else {
            let lapsed = timing.lapsed();
            if lapsed == Answer::UpstreamTimedOut
                && let Some(upstream) = upstream
            {
                upstream.failures.inc();
            }
            return Err(lapsed);
        };
        let (mut parts, body) = match connected.map_err(|error| h2_failed(error, upstream))? {
            Connected::NotOffered => {
                let answered = self
                    .respond_by_h2(
                        directed,
                        endpoint,
                        head,
                        &Method::GET,
                        Sending::None,
                        RequestBody::None,
                        admitted,
                        interim,
                        timing,
                    )
                    .await?;
                return match (&handshake.client, answered) {
                    (None, Answered::Map(response)) => {
                        let (parts, body) = response.into_parts();
                        connected_answer(parts.status, parts, body)
                    }
                    (_, answered) => Ok(answered),
                };
            }
            Connected::Switched(parts, stream) => {
                let Some(interim) = &handshake.server else {
                    return Err(Answer::UpstreamFailed);
                };
                let counted = admitted.count();
                interim.switch(self.switched(Backend::H2(stream), None, handshake, counted));
                (parts, Body::Empty)
            }
            Connected::Refused(parts, answer) => {
                let watch = Watch {
                    proxy: Arc::clone(&self.proxy),
                    upstream: directed.upstream.slot(),
                };
                (parts, Body::H2(Box::new(answer), admitted, watch))
            }
        };
        if let Some(upstream) = upstream {
            upstream.responded(parts.status);
        }
        let switched = parts.status.is_success();
        let headers = &mut parts.headers;
        let nominated = crate::hop_by_hop::nominated(&*headers);
        crate::h1::filter_declaration(headers, &nominated);
        crate::hop_by_hop::strip_response(headers);
        if let Some(changes) = directed
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref())
        {
            changes.apply(headers);
        }
        // To an HTTP/1.1 client, the switch is its 101, with the Accept of its own key.
        if switched && let Some(client) = &handshake.client {
            parts.status = StatusCode::SWITCHING_PROTOCOLS;
            parts
                .headers
                .insert(http::header::UPGRADE, websocket::WEBSOCKET);
            parts
                .headers
                .insert(http::header::SEC_WEBSOCKET_ACCEPT, client.accept_value());
        } else if switched {
            parts.status = StatusCode::OK;
        }
        Ok(Answered::Map(Response::from_parts(parts, body)))
    }

    /// What an HTTP/2 exchange is held to, for a try held to `timing`.
    fn h2_bounds(&self, timing: Timing) -> H2Bounds {
        H2Bounds {
            final_head: (!timing.head_by_rule).then_some(self.limits.final_head),
            idle: self.limits.idle,
            continue_wait: self.limits.continue_wait,
            interim_heads: self.limits.interim_heads,
            interim_bytes: self.limits.interim_bytes,
        }
    }
}

/// What an HTTP/2 exchange that failed is answered with, counted against `upstream` where
/// it was the upstream's failing: waiting for a place, or the client's own body, is not.
fn h2_failed(
    error: h2_exchange::ExchangeError,
    upstream: Option<&crate::metrics::UpstreamCounters>,
) -> Answer {
    let answer = match error {
        h2_exchange::ExchangeError::Place(PlaceError::Full) => Answer::QueueFull,
        h2_exchange::ExchangeError::Place(PlaceError::TimedOut) => Answer::QueueTimedOut,
        // Waiting for a place, the request was never sent.
        h2_exchange::ExchangeError::Place(PlaceError::Unreachable) => Answer::Unreachable,
        h2_exchange::ExchangeError::RequestBody(cause)
            if matches!(
                cause.downcast_ref::<RequestBodyError>(),
                Some(RequestBodyError::TimedOut)
            ) =>
        {
            Answer::BodyTimedOut
        }
        h2_exchange::ExchangeError::RequestBody(_) => Answer::BadBody,
        h2_exchange::ExchangeError::TooSlow { .. } | h2_exchange::ExchangeError::Idle { .. } => {
            Answer::UpstreamTimedOut
        }
        _ => Answer::UpstreamFailed,
    };
    if matches!(
        answer,
        Answer::Unreachable | Answer::UpstreamFailed | Answer::UpstreamTimedOut
    ) && let Some(upstream) = upstream
    {
        upstream.failures.inc();
    }
    answer
}

/// An extended CONNECT's answer, from its backend's (19 §3, §4): a switch, 101 from an
/// HTTP/1.1 backend, is told as 200 (RFC 8441 §5), with no Accept, which has no key to be of;
/// any other 2xx is answered 502 — to a CONNECT every 2xx opens the tunnel (RFC 9110
/// §9.3.6), and a page the backend served in place of the switch would be read as
/// WebSocket frames — and the backend's answer dropped, its connection with it; anything
/// else goes as it came.
fn connected_answer(
    status: StatusCode,
    mut parts: http::response::Parts,
    body: Body,
) -> Result<Answered<Body>, Answer> {
    if status == StatusCode::SWITCHING_PROTOCOLS {
        parts.status = StatusCode::OK;
        parts.headers.remove(http::header::SEC_WEBSOCKET_ACCEPT);
        parts.headers.remove(http::header::UPGRADE);
        return Ok(Answered::Map(Response::from_parts(parts, body)));
    }
    if status.is_success() {
        return Err(Answer::UpstreamFailed);
    }
    Ok(Answered::Map(Response::from_parts(parts, body)))
}

/// `exchange`'s outcome, or nothing if `deadline` comes first, kept in the worker's
/// `timers`. The exchange is pinned by the caller, where it is, and not moved into a
/// future of its own here: a future moved into another keeps its room in both, and these
/// are the biggest part of a request's.
async fn by_deadline<F: Future>(
    timers: &Rc<Timers>,
    deadline: Option<Instant>,
    mut exchange: Pin<&mut F>,
) -> Option<F::Output> {
    let Some(deadline) = deadline else {
        return Some(exchange.await);
    };
    // Boxed: only a request with a deadline has one, and the rest should not carry room
    // for it.
    let mut alarm = Box::new(Alarm::new(timers, None));
    std::future::poll_fn(|cx| {
        if let Poll::Ready(outcome) = exchange.as_mut().poll(cx) {
            return Poll::Ready(Some(outcome));
        }
        alarm.poll_until(cx, deadline).map(|()| None)
    })
    .await
}
