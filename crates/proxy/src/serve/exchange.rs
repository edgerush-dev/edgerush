//! One try at an answer from an endpoint, over HTTP/1 or HTTP/2, and the WebSocket that
//! a 101 or an extended CONNECT switches it to.

use super::logged::Logging;
use super::{
    Admitted, Body, Directed, Handshake, Timing, Toward, Watch, Worker, connect_by, watched,
    why_stopped,
};
use crate::downstream::h1::connection::Answered;
use crate::gathered::Gathered;
use crate::head::Forwarded;
use crate::interim::Interim;
use crate::metrics::{Answer, Socket, Stopped};
use crate::raw::RawAnswer;
use crate::request_body::{RequestBody, RequestBodyError};
use crate::timers::{Alarm, Timers};
use crate::tunnel::{Backend, Bounds as TunnelBounds, Switched, Tunneled};
use crate::upstream::balancing::InFlight;
use crate::upstream::destination::ReuseIdentity;
use crate::upstream::dial::Unconnected;
use crate::upstream::h1::blocks::Block;
use crate::upstream::h1::codec::{OutgoingFields, Sending, head_len};
use crate::upstream::h1::exchange::{Exchange, ExchangeError, H1Body, Stalled, nothing_to_say};
use crate::upstream::h1::pool::{Close, Lease};
use crate::upstream::h2::client::PlaceError;
use crate::upstream::h2::exchange::{self as h2_exchange, Bounds as H2Bounds, Connected};
use crate::upstream::secure::Socket as UpstreamSocket;
use crate::{way_back, websocket};
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
        admitted: &mut Option<Admitted>,
    ) -> Result<(RawAnswer, Box<H1Body<UpstreamSocket, B>>), ExchangeError>
    where
        F: OutgoingFields + ?Sized,
        B: HttpBody<Data = Bytes> + Unpin,
        B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        // Measured before a connection is taken for it: a head the gateway's own fields
        // took past the bound is never sent, and spends no connection (14 §6).
        let measured = head_len(method, uri, headers, sending);
        if measured > self.limits.head {
            return Err(ExchangeError::TooLong {
                limit: self.limits.head,
            });
        }
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
            // socket goes rather than being lent again, closed in good order (13 §6).
            self.proxy.metrics.socket(Socket::Discarded);
            socket.close();
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
                // aside for, not a handshake that failed after it, and not the worker's own
                // shortage of sockets (03 §6).
                let connected = Cell::new(false);
                // One bound for the connection and its handshake together. The connect is
                // lent the try's place, and carried on with it if the try lets go first.
                let deadline = Instant::now() + self.limits.connect;
                let opening = async {
                    let socket = watched::Connecting::new(
                        identity,
                        deadline,
                        admitted,
                        &self.watcher,
                        &self.files,
                    )
                    .await?;
                    connected.set(true);
                    // Worth having, not worth refusing an upstream over.
                    let _unset = socket.set_nodelay(true);
                    match secure {
                        None => Ok(UpstreamSocket::Plain(socket)),
                        Some(secure) => secure
                            .connect(socket)
                            .await
                            .map(|secured| UpstreamSocket::Secured(Gathered::new(secured)))
                            .map_err(Unconnected::Endpoint),
                    }
                };
                let opened = connect_by(deadline, opening).await;
                if let Err(ExchangeError::Unconnected(unconnected)) = &opened
                    && let Some(why) = unconnected.aside()
                    && !connected.get()
                {
                    identity.set_aside(why);
                }
                (opened?, Instant::now())
            }
        };

        let mut exchange = Exchange::new(socket, Rc::clone(&self.blocks), Rc::clone(&self.timers))
            .head_measured(measured);
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
            // The worker's own storage running out, a client's body that cannot be read, or a
            // head the gateway's own fields took past its bound, is not the upstream failing,
            // and is not counted as though it were ([14 §8](../../docs/14-downstream-server.md)).
            Err(
                answer @ (Answer::Exhausted
                | Answer::BadBody
                | Answer::BodyTimedOut
                | Answer::DeadlineExceeded
                | Answer::Edits),
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
        way_back::upstream_answer(&mut answer, &directed.way(false))?;
        // Written in this hop's version and not the upstream's: "Intermediaries that
        // process HTTP messages ... MUST send their own HTTP-version in forwarded messages"
        // (RFC 9110 §6.2). Our writer says HTTP/1.1, and so does a map made for HTTP/2.
        Ok(Answered::Raw(answer, body))
    }

    /// The answer of an HTTP/2 upstream, edited as an HTTP/1 upstream's is: what is about its
    /// connection comes off — h2 lets none of it through, and this does not rest on it — and
    /// the rule's changes are made. A WebSocket handshake comes here when the upstream's
    /// connection does not take extended CONNECT, sent as the plain GET it came as, and its
    /// answer is told to its client as one that did not switch (19 §4).
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
        way_back::upstream_answer(&mut response, &directed.way(false))?;
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
        admitted: Admitted,
        interim: Option<Interim>,
        head_by_rule: bool,
    ) -> Result<(RawAnswer, Body), Answer> {
        // Lent to a connect the exchange opens, and given back with it.
        let mut lent = Some(admitted);
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
                &mut lent,
            )
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                if let Some(why) = why_stopped(&error) {
                    self.proxy.metrics.stopped(why);
                }
                return Err(match error {
                    // Refused as an edit that does not fit is, the gateway's own failing
                    // (14 §6).
                    ExchangeError::TooLong { .. } => Answer::Edits,
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
                    // The worker had no socket to connect with: its own shortage, as storage
                    // is, not counted against the upstream nor tried again (14 §8).
                    ExchangeError::Unconnected(Unconnected::Short(_)) => Answer::Exhausted,
                    ExchangeError::Unconnected(_) => Answer::Unreachable,
                    _ => Answer::UpstreamFailed,
                });
            }
        };
        // Never gone here: a connect lent it gives it back as it ends, and the exchange
        // waits for that.
        let Some(mut admitted) = lent else {
            return Err(Answer::Exhausted);
        };
        let (read, mut body) = answer;
        if let Some(handshake) = &directed.websocket {
            if read.status() == StatusCode::SWITCHING_PROTOCOLS {
                let logging = directed.logging.as_ref();
                return self.switch(read, *body, handshake, admitted.count(), logging);
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
        logging: Option<&Rc<Logging>>,
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
        let backend = Backend::Socket(backend);
        interim.switch(self.switched(backend, leftover, handshake, counted, logging));
        Ok((read, Body::Empty))
    }

    /// A switched WebSocket's backend, with what its tunnel needs of this worker, and its
    /// end counted among its listener's tunnels and noted in its record, `logging`, which
    /// the tunnel holds until then.
    fn switched(
        &self,
        backend: Backend,
        leftover: Option<Block>,
        handshake: &Handshake,
        counted: Option<InFlight>,
        logging: Option<&Rc<Logging>>,
    ) -> Switched {
        let proxy = Arc::clone(&self.proxy);
        let listener = handshake.listener;
        let logging = logging.map(|logging| {
            logging.switched();
            Rc::clone(logging)
        });
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
            ended: Box::new(move |carried: Tunneled| {
                if let Some(counters) = proxy.metrics.listener(listener) {
                    counters.tunnel(carried.how.into());
                }
                if let Some(logging) = logging {
                    logging.tunneled(carried);
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
        let (parts, body) = match connected.map_err(|error| h2_failed(error, upstream))? {
            Connected::NotOffered => {
                return self
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
                    .await;
            }
            Connected::Switched(parts, stream) => {
                let Some(interim) = &handshake.server else {
                    return Err(Answer::UpstreamFailed);
                };
                let counted = admitted.count();
                let logging = directed.logging.as_ref();
                let switched =
                    self.switched(Backend::H2(stream), None, handshake, counted, logging);
                interim.switch(switched);
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
        let mut response = Response::from_parts(parts, body);
        way_back::upstream_answer(&mut response, &directed.way(true))?;
        Ok(Answered::Map(response))
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
/// it was the upstream's failing: waiting for a place, the client's own body, or the
/// worker's own storage running out, is not.
fn h2_failed(
    error: h2_exchange::ExchangeError,
    upstream: Option<&crate::metrics::UpstreamCounters>,
) -> Answer {
    let answer = match error {
        h2_exchange::ExchangeError::Place(PlaceError::Full) => Answer::QueueFull,
        h2_exchange::ExchangeError::Place(PlaceError::TimedOut) => Answer::QueueTimedOut,
        // Waiting for a place, the request was never sent.
        h2_exchange::ExchangeError::Place(PlaceError::Unreachable) => Answer::Unreachable,
        // No socket to connect with: the worker's own shortage, not the upstream's (14 §8).
        h2_exchange::ExchangeError::Place(PlaceError::Exhausted) => Answer::Exhausted,
        h2_exchange::ExchangeError::RequestBody(cause)
            if matches!(
                cause.downcast_ref::<RequestBodyError>(),
                Some(RequestBodyError::TimedOut)
            ) =>
        {
            Answer::BodyTimedOut
        }
        h2_exchange::ExchangeError::RequestBody(_) => Answer::BadBody,
        h2_exchange::ExchangeError::Exhausted => Answer::Exhausted,
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

/// What an answer that cannot go to the client is answered with in its place: one whose
/// edits could not all be made, as a request's are; and the backend's failing for a page
/// in place of a WebSocket's switch, which is dropped, its connection with it (19 §3, §4).
impl From<way_back::Failed> for Answer {
    fn from(failed: way_back::Failed) -> Self {
        match failed {
            way_back::Failed::Edits => Self::Edits,
            way_back::Failed::NotSwitched => Self::UpstreamFailed,
        }
    }
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
