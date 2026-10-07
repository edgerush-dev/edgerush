//! A request, from its head to its answer: directed by the config in force, admitted, and
//! answered by its upstream or by the data plane itself.

use super::logged::{Logging, logged};
use super::{
    Body, Called, Connection, Directed, Directing, Handshake, Mirrored, Others, Proxy, Redirect,
    Snapshot, TUNNEL_IDLE, Timed, Timing, Toward, Worker, at_endpoint, balance_of,
};
use crate::balance::Tried;
use crate::downstream::h1::connection::{self as h1, Answered};
use crate::forwarding::Client;
use crate::grpc::answer::{Answered as GrpcAnswered, is_grpc_answer};
use crate::grpc::call::Call;
use crate::head::Forwarded;
use crate::interim::Interim;
use crate::map_head::MapHead;
use crate::metrics::Answer;
use crate::random::{random, unguessable};
use crate::request::{Decision, Opening, Rejection, decide};
use crate::request_body::{Counts, RequestBody};
use crate::routed::Through;
use crate::timers::{Alarm, Timers};
use crate::upstream::balancing::Balancing;
use crate::upstream::h1::codec::Sending;
use crate::way_back;
use crate::websocket::{self, Key};
use arc_swap::Guard;
use edgerush_config::{RequestId, UpstreamProtocol};
use edgerush_filters::request_id;
use edgerush_router::Fields;
use http::{HeaderValue, Method, Request, Response, Version};
use http_body::Body as HttpBody;
use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::Instant;

impl Worker {
    /// Answers a request that came in on `listener`. `interim` is where the server that
    /// read it wants the upstream's interim answers, if it passes them on
    /// ([14 §5](../../docs/14-downstream-server.md)).
    ///
    /// Not itself `async`: a future of its own would hold the request as well as the
    /// future it hands it to, and every stream's task is made by copying it.
    pub(super) fn handle(
        self: Rc<Self>,
        listener: usize,
        client: Rc<Client>,
        request: Request<RequestBody>,
        interim: Option<Interim>,
    ) -> impl Future<Output = Answered<Body>> {
        let (head, body) = request.into_parts();
        // What the core adds is kept beside the map, in room the worker lends (14 §6).
        let head = MapHead::lent(head, &self.blocks);
        self.handle_head(listener, client, head, body, interim, None)
    }

    /// The record of a request that came in on `listener` from `client`, if its listener
    /// logs, made from its head as it came (21 §4). A request of a config that logs
    /// nothing looks at one flag.
    fn logging<H: Forwarded>(
        &self,
        listener: usize,
        client: &Rc<Client>,
        head: &H,
    ) -> Option<Rc<Logging>> {
        if !self.proxy.logs.on() {
            return None;
        }
        let worker = self.me.upgrade()?;
        let snapshot = self.proxy.current.load();
        Logging::start(&worker, &snapshot, listener, client, head)
    }

    /// The same for a request's head of whatever kind: a map, or the raw head our own
    /// server reads ([14 §6](../../docs/14-downstream-server.md)). Over HTTP/1 the answer is
    /// counted once the server says whose head went, `owed` keeping it till then.
    pub(super) async fn handle_head<H: Forwarded>(
        self: Rc<Self>,
        listener: usize,
        client: Rc<Client>,
        head: H,
        body: RequestBody,
        interim: Option<Interim>,
        owed: Option<Rc<Connection>>,
    ) -> Answered<Body> {
        let came_in = Instant::now();
        // First, so that every answer the core gives, its own included, carries it; with
        // the snapshot it was decided on, which the request is directed by.
        let (snapshot, id) = self.proxy.identify(listener);
        // Its access-log record, if its listener logs: made where the head is in hand
        // already, and handed back here for the answer to end. Made here, the head would be
        // borrowed in this future as well and kept in it, and the future no longer made in
        // its slot (14 §3).
        let logging = Cell::new(None);
        let mut answered = self
            .respond_to(
                snapshot,
                listener,
                &client,
                head,
                body,
                interim,
                id.as_ref(),
                &logging,
            )
            .await;
        self.proxy.say_last(listener, &mut answered, id);
        let took = u64::try_from(came_in.elapsed().as_nanos()).unwrap_or(u64::MAX);
        let logging = logging.take();
        match owed {
            Some(connection) => {
                connection.owe(answered.status(), took, came_in, logging.as_ref());
            }
            None => {
                if let Some(counters) = self.proxy.metrics.listener(listener) {
                    counters.responded(answered.status(), took);
                }
            }
        }
        // Last, so that it counts what goes of the answer and ends with it.
        if let Some(logging) = logging {
            logging.answered(&answered);
            answered = logged(answered, logging);
        }
        answered
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the request needs, as for `through_h1`"
    )]
    async fn respond_to<H: Forwarded>(
        &self,
        snapshot: Guard<Arc<Snapshot>>,
        listener: usize,
        client: &Rc<Client>,
        mut head: H,
        mut body: RequestBody,
        interim: Option<Interim>,
        id: Option<&HeaderValue>,
        handed: &Cell<Option<Rc<Logging>>>,
    ) -> Answered<Body> {
        // Its record, from its head as it came, and its body counted for it (21 §4).
        // Only ever moved from here on, never borrowed, so that this future keeps it no longer
        // than until `direct` takes it (14 §3).
        let logging = match self.logging(listener, client, &head) {
            Some(logging) => {
                logging.identified(id);
                handed.set(Some(Rc::clone(&logging)));
                body = RequestBody::counted(body, Rc::clone(&logging) as Rc<dyn Counts>);
                Some(logging)
            }
            None => None,
        };
        // A request's trailers go no further than the gateway (03 §11): its body ends
        // where they would come, for the upstream, a retry and a mirror alike.
        body.drop_trailers();
        // How the body is to be sent on, worked out from what arrived and before `direct`
        // takes the hop-by-hop fields off it — and before the body itself is touched,
        // because the path is chosen while there is still nothing to undo.
        let sending = sending_for(&head, &body);
        // A gRPC call is answered as one, the gateway's own answers included; read from the
        // head as the client sent it, before any filter touches it (15 §6).
        let call = Call::of(head.version(), head.method(), head.outgoing(), Instant::now);
        let deadline = call.and_then(|call| call.deadline());
        if deadline.is_some_and(|deadline| deadline <= Instant::now()) {
            return self
                .proxy
                .answer_to(listener, Answer::DeadlineExceeded, call)
                .into();
        }
        // What this request's own `Connection` named, read before routing takes the
        // hop-by-hop fields off the head. Afterwards there is nothing left to read them
        // from and everything it named looks like an ordinary field, so a trailer of that
        // name would travel on ([13 §4](../../docs/13-http1-upstream.md)).
        let nominated = crate::hop_by_hop::nominated(head.outgoing());
        let mut directed = match self.proxy.direct(
            snapshot,
            listener,
            client,
            &mut head,
            id,
            &self.balancing,
            logging,
        ) {
            Ok(Directing::Upstream(directed)) => directed,
            Ok(Directing::Redirect(redirect)) => {
                return self.proxy.redirect(listener, redirect, call).into();
            }
            Ok(Directing::FinalRecipient { options }) => {
                let mut response = self.proxy.answer(listener, Answer::MaxForwards);
                // A map takes every field it is given: nothing here fails.
                let _made = way_back::final_recipient_answer(&mut response, options);
                return response.into();
            }
            Err(answer) => return self.proxy.answer_to(listener, answer, call).into(),
        };
        // A WebSocket that an HTTP/2 or HTTP/3 client asks for holds a backend of its own
        // beside the client's one connection, which may carry a hundred: it counts as one
        // of the worker's connections for as long as it is open, and is refused, before
        // anything goes upstream, while the worker or the listener has no room for one,
        // as a connection would not be accepted then (03 §9).
        if let Some(handshake) = &directed.websocket
            && handshake.on_a_stream()
            && let Some(connections) = &self.connections
        {
            let Some(held) = connections.take(usize::from(self.position), listener) else {
                return self.proxy.answer_to(listener, Answer::NoRoom, call).into();
            };
            handshake.held.set(Some(held));
        }
        let timing = Timing::of(call, directed.rule.as_deref());

        // Credentials can bind the upstream socket to this client, even when the
        // response is successful. Decide after rule filters and before the client
        // dispatches. On HTTP/2 there is no connection of one client's to bind them to,
        // and they are not sent at all (15 §5).
        let multiplexed = directed.endpoint.protocol() == UpstreamProtocol::Http2;
        if crate::upstream::auth::carries_credentials(head.outgoing()) {
            if multiplexed {
                return self
                    .proxy
                    .answer_to(listener, Answer::ConnectionAuth, call)
                    .into();
            }
            if let Err(rejection) = head.close_connection() {
                return self
                    .proxy
                    .answer_to(listener, rejection.into(), call)
                    .into();
            }
        }

        // What asks the backend to switch is the gateway's own, its key included: in place
        // of the client's `Upgrade`, `Connection` (a `close` for credentials among it: a
        // refused handshake's connection is kept from the pool instead) and key (19 §2).
        if let Some(handshake) = &mut directed.websocket {
            handshake.server.clone_from(&interim);
            let asked = match &handshake.toward {
                // An extended CONNECT to an HTTP/1.1 backend is RFC 6455's GET (19 §3).
                Toward::Upgrade(ours) => {
                    if head.method() != Method::GET {
                        head.set_method(Method::GET);
                    }
                    head.set_field(http::header::UPGRADE, websocket::WEBSOCKET)
                        .and_then(|()| {
                            head.set_field(http::header::CONNECTION, websocket::UPGRADE_OPTION)
                        })
                        .and_then(|()| {
                            head.set_field(http::header::SEC_WEBSOCKET_KEY, ours.value())
                        })
                }
                // An extended CONNECT has no key (RFC 8441 §5); its method is the
                // exchange's to write.
                Toward::Connect => head.remove_where(|name| {
                    name.eq_ignore_ascii_case(http::header::SEC_WEBSOCKET_KEY.as_str().as_bytes())
                }),
            };
            if let Err(rejection) = asked {
                return self
                    .proxy
                    .answer_to(listener, rejection.into(), call)
                    .into();
            }
        }

        // Before either client looks for a connection or opens one: a place is what
        // entitles a request to a connection, so it is taken before one is sought. The same
        // bound whichever client carries the request, so that the two are compared doing
        // the same work ([14 §2](../../docs/14-downstream-server.md)).
        let admitted = match self.admit(&directed.upstream, directed.alone) {
            Ok(admitted) => admitted,
            Err(refused) => return self.proxy.answer_to(listener, refused, call).into(),
        };
        let admitted = admitted.counting(directed.counted.take());
        let body = if directed.mirrors.is_empty() {
            body
        } else {
            let mirrors = std::mem::take(&mut directed.mirrors);
            self.mirror(mirrors, directed.alone, &head, &nominated, sending, body)
        };
        let retry = directed.rule.as_ref().and_then(|rule| rule.retry());
        let outcome = match retry {
            // Only a rule that asks pays for keeping the body and the loop around it.
            // Boxed: a future is as big as its biggest state, and every request would
            // otherwise carry room for the retry loop's.
            Some(retry) => {
                Box::pin(self.with_retries(
                    &directed, &mut head, &nominated, sending, body, admitted, interim, timing,
                    retry,
                ))
                .await
            }
            None => {
                let endpoint = Arc::clone(&directed.endpoint);
                self.attempt(
                    &directed, &endpoint, &head, &nominated, sending, body, admitted, interim,
                    timing,
                )
                .await
            }
        };
        if outcome.is_ok()
            && let Some(logging) = &directed.logging
        {
            logging.upstream_answered();
        }
        match outcome {
            // A gRPC call's answer that is gRPC's own ends with one status, whatever becomes
            // of it; any other answer goes on as it came, for the client to read (15 §6).
            // Only the answer that goes to the client: one a retry set aside is not counted.
            Ok(Answered::Map(response))
                if call.is_some() && is_grpc_answer(response.status(), response.headers()) =>
            {
                let (parts, body) = response.into_parts();
                let called = Called {
                    proxy: Arc::clone(&self.proxy),
                    listener,
                    logging: directed.logging.clone(),
                };
                let answered = GrpcAnswered::counted(body, &parts.headers, timing.deadline, called);
                Answered::Map(Response::from_parts(parts, Body::Grpc(Box::new(answered))))
            }
            // Any other answer is cut off where its deadline passes, as one that failed.
            Ok(answered) => match timing.deadline {
                Some(deadline) => timed(answered, deadline, &self.timers),
                None => answered,
            },
            Err(answer) => self.proxy.answer_to(listener, answer, call).into(),
        }
    }
}

impl Proxy {
    /// Says on `answered` what every answer says last (22 §5): that the listener serves
    /// HTTP/3 as well, if it does — every answer carries it, HTTP/3's own too, which keeps
    /// what a client remembers fresh — and the request's ID, `id`, if the listener gives one.
    ///
    /// Whether it serves HTTP/3 is read from the config in force when the answer goes, not
    /// the one the request was directed by: it says what the listener offers now, so a
    /// reload that stops HTTP/3 stops it being advertised at once, on the answers of
    /// requests under way too (03 §4).
    fn say_last(&self, listener: usize, answered: &mut Answered<Body>, id: Option<HeaderValue>) {
        let snapshot = self.current.load();
        let alt_svc = snapshot.alt_svc.get(listener).and_then(Option::as_ref);
        match answered {
            Answered::Raw(answer, _) => way_back::every_answer(answer, alt_svc, id),
            Answered::Map(response) => way_back::every_answer(response, alt_svc, id),
        }
    }

    /// The config in force, and an ID on it for a request that came in on `listener`, if
    /// the listener gives its requests one (08 §3 in the docs). The request is directed by
    /// the same snapshot, so that a reload between the two cannot have it served by both
    /// configs (03 §4); and the ID is worked out once, so that the request and its client
    /// are told the same one.
    ///
    /// Handed back beside the snapshot rather than worked out on a borrow of it by the
    /// caller: a local that is borrowed stays in an async fn's future to the end of its
    /// scope, and that one's layout then keeps the request's future from being made in
    /// its slot, which costs a copy of all of it each request (14 §3).
    fn identify(&self, listener: usize) -> (Guard<Arc<Snapshot>>, Option<HeaderValue>) {
        let snapshot = self.current.load();
        let id = snapshot
            .listener(listener)
            .filter(|listener| listener.request_id == RequestId::Generate)
            .and_then(|_| Some(request_id::value(unix_millis(), unguessable()?)));
        (snapshot, id)
    }

    /// An answer of the data plane's own for a request that may be a gRPC `call`: for one
    /// that is, `200` and the status gRPC gives the cause, with nothing after the head — a
    /// trailers-only answer, which is how gRPC answers a call it fails before any message
    /// (15 §6). Counted by its reason either way.
    fn answer_to(&self, listener: usize, answer: Answer, call: Option<Call>) -> Response<Body> {
        let mut response = self.answer(listener, answer);
        if call.is_some() {
            if let Some(counters) = self.metrics.listener(listener) {
                counters.called(answer.grpc().0 as usize);
            }
            way_back::call_answer(&mut response, answer);
        }
        response
    }

    /// An answer of the data plane's own, counted by its reason.
    pub(super) fn answer(&self, listener: usize, answer: Answer) -> Response<Body> {
        if let Some(counters) = self.metrics.listener(listener) {
            counters.answered(answer);
        }
        let mut response = Response::new(Body::Empty);
        *response.status_mut() = answer.status();
        response.extensions_mut().insert(h1::Local(answer));
        response
    }

    /// A redirect's answer: its status, its `Location` and no body, with its rule's changes to
    /// the answer's headers; counted among the data plane's own. A gRPC call is answered as
    /// one, as it is with the data plane's other answers
    /// ([15 §6](../../docs/15-http2-and-grpc.md)): no gRPC client follows a redirect.
    fn redirect(&self, listener: usize, redirect: Redirect, call: Option<Call>) -> Response<Body> {
        if call.is_some() {
            return self.answer_to(listener, Answer::Redirected, call);
        }
        let mut response = self.answer(listener, Answer::Redirected);
        let changes = redirect
            .rule
            .as_ref()
            .and_then(|rule| rule.response_headers.as_ref());
        // A map takes every field it is given: nothing here fails.
        let _made =
            way_back::redirect_answer(&mut response, redirect.status, redirect.location, changes);
        response
    }

    /// Makes the head of a request that came in on a listener's socket the head of the
    /// request to send, target included, or says what to answer instead. All of it is done
    /// on `snapshot`, the one the request's ID was worked out on, which is let go of here,
    /// before anything is waited for; what is kept for the response is the rule, and only if
    /// it has something to do to the response, and the slot of the upstream's counters.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing directing needs, as for `respond_to`"
    )]
    fn direct<H: Forwarded>(
        &self,
        snapshot: Guard<Arc<Snapshot>>,
        listener: usize,
        client: &Client,
        head: &mut H,
        id: Option<&HeaderValue>,
        balancing: &RefCell<Balancing>,
        logging: Option<Rc<Logging>>,
    ) -> Result<Directing, Answer> {
        let came_on = listener;
        let listener = snapshot
            .listeners
            .get(listener)
            .copied()
            .flatten()
            .and_then(|position| snapshot.config.listeners().get(position))
            .ok_or(Answer::NoRoute)?;
        let decided = match decide(&snapshot.config, listener, head, client, &mut random, id) {
            Err(Rejection::MaxForwards { options }) => {
                return Ok(Directing::FinalRecipient { options });
            }
            decided => decided?,
        };
        let forward = match decided {
            Decision::Forward(forward) => forward,
            Decision::Redirect(redirected) => {
                if let Some(logging) = &logging {
                    let route = snapshot.config.route_name(redirected.id.route);
                    logging.routed(route.unwrap_or_default(), redirected.id.rule, None);
                }
                return Ok(Directing::Redirect(Redirect {
                    rule: redirected
                        .rule
                        .response_headers
                        .is_some()
                        .then(|| Arc::clone(redirected.rule)),
                    status: redirected.status,
                    location: redirected.location,
                }));
            }
        };
        // An upstream the snapshot does not have is not known to happen.
        let upstream = forward.upstream.0;
        if let Some(logging) = &logging {
            let route = snapshot.config.route_name(forward.id.route);
            let name = snapshot.config.upstreams().get(upstream);
            logging.routed(
                route.unwrap_or_default(),
                forward.id.rule,
                name.map(|upstream| upstream.name.as_str()),
            );
        }
        let endpoints = snapshot.endpoints.get(upstream).ok_or(Answer::NoBackend)?;
        let destinations = snapshot.destinations.of(upstream);
        let balance = balance_of(balancing, &snapshot, upstream).ok_or(Answer::NoEndpoints)?;
        let (at, counted) = balance
            .pick(destinations, &Tried::default())
            .ok_or(Answer::NoEndpoints)?;
        let endpoint = endpoints.get(at).ok_or(Answer::NoEndpoints)?;
        let identity = snapshot
            .destinations
            .at(upstream, at)
            .ok_or(Answer::NoEndpoints)?;

        let target = at_endpoint(head.uri(), endpoint).ok_or(Answer::BadTarget)?;
        head.set_uri(target);
        let upgradable = head.version() == Version::HTTP_11;
        head.onward();
        // A handshake goes on as HTTP/1.1's upgrade to a backend spoken to in HTTP/1.1, and
        // as an extended CONNECT to one spoken to in HTTP/2 (19 §2, §4).
        let websocket = forward.websocket.as_ref().and_then(|opening| {
            let client = match opening {
                Opening::Upgrade(key) => Some(Key::read(key.as_bytes())?),
                Opening::Connect => None,
            };
            let toward = match identity.protocol() {
                UpstreamProtocol::Http1 => Toward::Upgrade(Key::of(unguessable()?)),
                UpstreamProtocol::Http2 => Toward::Connect,
            };
            Some(Box::new(Handshake {
                client,
                toward,
                idle: forward
                    .rule
                    .timeouts()
                    .and_then(|timeouts| timeouts.tunnel_idle)
                    .unwrap_or(TUNNEL_IDLE),
                listener: came_on,
                server: None,
                held: Cell::new(None),
                route: snapshot
                    .routed
                    .key(came_on, Through::Http(forward.id.route), upstream),
            }))
        });
        if let Some(counters) = self.metrics.upstream(balance.slot()) {
            counters.requests.inc();
        }
        let mut mirrors = Vec::new();
        // The mirrors that take this request were drawn with it, and a copy made for each
        // placed before a change (18 §5).
        for mirroring in forward.mirrors {
            let Some(mirror) = forward.rule.mirrors.get(mirroring.mirror) else {
                continue;
            };
            let (target, fields) = match mirroring.own {
                Some(copied) => (copied.target, Some(copied.fields)),
                None => (head.uri().clone(), None),
            };
            let upstream = mirror.upstream.0;
            let Some(&slot) = snapshot.upstream_slots.get(upstream) else {
                continue;
            };
            // A mirror could only ever be sent a WebSocket's handshake, never its messages.
            if forward.websocket.is_some() {
                if let Some(counters) = self.metrics.upstream(slot) {
                    counters.mirrors_upgrade.inc();
                }
                continue;
            }
            let destinations = snapshot.destinations.of(upstream);
            let found = balance_of(balancing, &snapshot, upstream).and_then(|balance| {
                let (at, counted) = balance.pick(destinations, &Tried::default())?;
                let authority = snapshot.endpoints.get(upstream)?.get(at)?;
                Some((
                    at_endpoint(&target, authority)?,
                    destinations.get(at)?,
                    counted,
                    balance,
                ))
            });
            let counters = self.metrics.upstream(slot);
            let Some((target, destination, counted, balance)) = found else {
                if let Some(counters) = counters {
                    counters.mirrors_nowhere.inc();
                }
                continue;
            };
            if let Some(counters) = counters {
                counters.requests.inc();
            }
            mirrors.push(Mirrored {
                upstream: balance,
                endpoint: Arc::clone(destination),
                counted: Some(counted),
                target,
                fields,
            });
        }
        let kept = forward.rule.response_headers.is_some()
            || forward.rule.retry().is_some()
            || forward.rule.timeouts().is_some();
        // Only a request that may be sent again keeps where else it could go.
        let others = forward.rule.retry().map(|_| {
            Box::new(Others {
                authorities: endpoints.clone(),
                destinations: destinations.to_vec(),
                first: at,
            })
        });
        Ok(Directing::Upstream(Directed {
            rule: kept.then(|| Arc::clone(forward.rule)),
            upstream: balance,
            endpoint: Arc::clone(identity),
            counted: Some(counted),
            others,
            mirrors,
            websocket,
            upgradable,
            alone: snapshot.upstream_slots.len() < 2,
            logging,
        }))
    }
}

/// `answered`, its body cut off, as a body that failed, if it is still coming at
/// `deadline`. An answer with nothing to come is left as it is: it has nothing to cut.
fn timed(answered: Answered<Body>, deadline: Instant, timers: &Rc<Timers>) -> Answered<Body> {
    let wrap = |body: Body| {
        if body.is_end_stream() {
            return body;
        }
        Body::Timed(Box::new(Timed {
            body,
            deadline,
            alarm: Alarm::new(timers, None),
        }))
    };
    match answered {
        Answered::Raw(answer, body) => Answered::Raw(answer, wrap(body)),
        Answered::Map(response) => Answered::Map(response.map(wrap)),
    }
}

/// How a request's body is to be sent on.
///
/// Read from the request as it arrived rather than from the body's own account of itself:
/// a body that says how long it is may still end with trailers, and over HTTP/2 it always
/// may. What is certain is what the client framed it as
/// ([13 §4](../../docs/13-http1-upstream.md)).
fn sending_for<H: Forwarded>(head: &H, body: &RequestBody) -> Sending {
    // The engine says outright when there is no body, and that is the one thing a length
    // alone would not settle. A client that said its body is a length of nothing goes on
    // saying so: RFC 9110 §8.6 has a sender state a length for a method whose content
    // means something, and a server may answer 411 without one. One that said nothing,
    // as an HTTP/2 request ended by its headers does, is sent no framing either.
    if body.is_end_stream() {
        return match request_length(head.outgoing()) {
            Some(0) => Sending::Length(0),
            _ => Sending::None,
        };
    }
    if matches!(head.version(), Version::HTTP_2 | Version::HTTP_3) {
        // Framed as frames, with trailers allowed after any of them. There is no length
        // here that would still be true by the end.
        return Sending::Chunked;
    }
    if crate::hop_by_hop::is_chunked_request(head.outgoing()) {
        return Sending::Chunked;
    }
    match request_length(head.outgoing()) {
        Some(length) => Sending::Length(length),
        // No length and no coding, over HTTP/1.1, is no body at all.
        None => Sending::None,
    }
}

/// A request's `Content-Length`, where it has exactly one that is a plain number. Hyper
/// has already refused what it will refuse; anything left that does not read as a length
/// is treated as no length, and the body is framed by this end instead.
fn request_length<F: Fields + ?Sized>(headers: &F) -> Option<u64> {
    let mut lengths = headers.values(&http::header::CONTENT_LENGTH);
    let only = lengths.next()?;
    if lengths.next().is_some() {
        return None;
    }
    // What `HeaderValue::to_str` takes, visible ASCII and tabs, before it is read as text.
    if !only
        .iter()
        .all(|&byte| (32..127).contains(&byte) || byte == b'\t')
    {
        return None;
    }
    std::str::from_utf8(only).ok()?.trim().parse().ok()
}

/// Milliseconds since the Unix epoch, for a request's ID: read for each, since the date the
/// worker keeps is of whole seconds.
fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}
