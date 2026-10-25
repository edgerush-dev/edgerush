//! The synchronous mode's service pod (06 §4): the owner of its keys
//! (`edgerush_ratelimit::synchronous`), deciding every request data planes ask about on
//! whichever core its stream is served on, and copying the keys that changed to the other
//! pods once a cycle of its own.
//!
//! Apart from the asynchronous mode's pod ([`crate::pod`]): it shares the codec, the view
//! the pods agree on ([`Membership`]) and the heartbeats that agree it, nothing else.
//!
//! - `Decide`: one long-lived stream a data plane, both ways — requests and give-backs up,
//!   answers and the view down, matched by number. Decided as they come, on the stream's
//!   own task; a data plane never sends a request again, so nothing is remembered of them.
//! - `Copy`: an owner's copies of the keys that changed in a cycle, for every other pod.
//! - `Heartbeat`: as in the asynchronous mode.

pub mod wire;

use crate::grpc::{Boxed, Client, Error, HEARTBEAT, MAX_MESSAGE, Prost, limited};
use crate::wire::{Empty, Heartbeat as WireHeartbeat};
use edgerush_ratelimit::service::membership::{Heartbeat, Membership};
use edgerush_ratelimit::service::{PodId, View};
use edgerush_ratelimit::synchronous::{self as owner, Handles, Owner, Shared};
use futures_core::Stream;
use http::uri::PathAndQuery;
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::hash::RandomState;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, watch};
use tonic::server::NamedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

const DECIDE: &str = "/edgerush.ratelimit.v1.RateLimit/Decide";
const COPY: &str = "/edgerush.ratelimit.v1.RateLimit/Copy";

/// How many answers may wait to go down a stream before its requests wait too.
const ANSWERS_WAITING: usize = 1_024;

/// How many messages of copies may wait for one other pod. Beyond it a copy is dropped:
/// the key's next change copies it again, and a pod taking over from a copy that missed a
/// change forgives it, as it forgives a cycle's.
const COPIES_WAITING: usize = 1_024;

/// How often the keys are swept, and how long a full key goes unused before it is
/// forgotten, as local mode's are.
const SWEEP: Duration = Duration::from_secs(1);

/// How often a stream lets go of its handles to keys forgotten, in requests.
const PRUNE_EVERY: u64 = 1_024;

/// How the pods copy and watch each other.
#[derive(Clone, Copy, Debug)]
pub struct Timing {
    /// The pod's own cycle: the time between its copies of what changed.
    pub copy: Duration,
    /// The time between a pod's heartbeats.
    pub heartbeat: Duration,
    /// How long a pod goes unheard before it is suspected.
    pub silence: Duration,
    /// How long a pod is heard before it is taken in.
    pub settle: Duration,
}

impl Timing {
    /// Copies every `copy`, heartbeats as the asynchronous mode's pods beat.
    #[must_use]
    pub fn with_copy(copy: Duration) -> Self {
        let beat = crate::pod::Timing::with_cycle(copy);
        Self {
            copy,
            heartbeat: beat.heartbeat,
            silence: beat.silence,
            settle: beat.settle,
        }
    }
}

fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

/// What every task of a pod shares.
struct Pod {
    owner: Owner<RandomState>,
    hasher: RandomState,
    start: Instant,
    /// The keys streams changed and handed over since the last copy, for the next.
    changed: Mutex<Vec<Shared>>,
    /// The view the pods agree on, and whether it takes this pod in.
    view: watch::Receiver<(View, bool)>,
    heartbeats: mpsc::Sender<Heartbeat>,
    /// Its own cycle, on which streams hand over the keys they changed.
    cycle: Duration,
    /// Set once it is stopped, as a pod gone.
    stopped: watch::Sender<bool>,
}

impl Pod {
    fn now(&self) -> u64 {
        nanos(self.start.elapsed())
    }

    fn stopped(&self) -> bool {
        *self.stopped.borrow()
    }
}

/// A running synchronous service pod. Cheap to clone; stops when the last handle goes.
#[derive(Clone)]
pub struct SyncPod {
    pod: Arc<Pod>,
}

impl std::fmt::Debug for SyncPod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncPod").finish_non_exhaustive()
    }
}

impl SyncPod {
    /// Starts pod `me` among `members` (itself too), in its life `incarnation`, copying to
    /// and beating at the others through `peers`.
    #[must_use]
    pub fn spawn(
        me: PodId,
        members: Vec<PodId>,
        incarnation: u64,
        timing: Timing,
        peers: BTreeMap<PodId, Client>,
    ) -> Self {
        let start = Instant::now();
        let mut membership = Membership::new(
            me,
            incarnation,
            members,
            nanos(timing.silence),
            nanos(timing.settle),
            0,
        );
        let (publish, view) = watch::channel((membership.view().clone(), membership.serving()));
        let (heartbeats, mut heard) = mpsc::channel::<Heartbeat>(1_024);
        let hasher = RandomState::new();
        let (stopped, mut stopping) = watch::channel(false);
        let pod = Arc::new(Pod {
            owner: Owner::new(hasher.clone(), 1),
            hasher,
            start,
            changed: Mutex::new(Vec::new()),
            view,
            heartbeats,
            cycle: timing.copy,
            stopped,
        });
        let senders: Vec<CopySender> = peers.values().cloned().map(CopySender::spawn).collect();
        let watched = Arc::downgrade(&pod);
        // Membership: heartbeats in and out, the view published.
        tokio::spawn(async move {
            let mut beat = tokio::time::interval(timing.heartbeat);
            beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let now = nanos(start.elapsed());
                let view = tokio::select! {
                    _ = stopping.changed() => return,
                    heard = heard.recv() => match heard {
                        Some(heartbeat) => membership.on_heartbeat(heartbeat, now),
                        None => return,
                    },
                    _ = beat.tick() => {
                        if watched.strong_count() == 0 {
                            return;
                        }
                        let view = membership.on_tick(now);
                        let mine = membership.heartbeat(now);
                        for client in peers.values() {
                            let (client, mine) = (client.clone(), mine.clone());
                            tokio::spawn(async move {
                                let _unheard: Result<Empty, Error> =
                                    client.call(WireHeartbeat::from(mine), HEARTBEAT).await;
                            });
                        }
                        view
                    }
                };
                let agreed = (membership.view().clone(), membership.serving());
                if view.is_some() || publish.borrow().1 != agreed.1 {
                    let _gone = publish.send(agreed);
                }
            }
        });
        // The pod's own cycle: what changed copied to every other pod.
        let copying = Arc::downgrade(&pod);
        tokio::spawn(async move {
            let mut cycle = tokio::time::interval(timing.copy);
            cycle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                cycle.tick().await;
                let Some(pod) = copying.upgrade().filter(|pod| !pod.stopped()) else {
                    return;
                };
                let changed = std::mem::take(
                    &mut *pod
                        .changed
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner),
                );
                if changed.is_empty() {
                    continue;
                }
                let copies: Vec<wire::Copy> = pod
                    .owner
                    .copies(&changed, pod.now())
                    .into_iter()
                    .map(wire::Copy::from)
                    .collect();
                for sender in &senders {
                    sender.send(me, copies.clone());
                }
            }
        });
        // The sweep, as local mode's.
        let swept = Arc::downgrade(&pod);
        tokio::spawn(async move {
            let mut every = tokio::time::interval(SWEEP);
            every.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                every.tick().await;
                let Some(pod) = swept.upgrade() else {
                    return;
                };
                pod.owner.keys().sweep(pod.now(), nanos(SWEEP));
            }
        });
        Self { pod }
    }

    /// The view as the pods agree on it, and whether it takes this pod in.
    #[must_use]
    pub fn view(&self) -> (View, bool) {
        self.pod.view.borrow().clone()
    }

    /// The bytes its keys may hold: past them, its sweep evicts.
    pub fn set_capacity(&self, bytes: u64) {
        self.pod.owner.keys().set_capacity(bytes);
    }

    /// How many keys it holds, owned and copies.
    #[must_use]
    pub fn key_count(&self) -> usize {
        self.pod.owner.keys().key_count()
    }

    /// Stops it, as a pod gone: it beats, copies and answers no more, and its streams end.
    pub fn stop(&self) {
        self.pod.stopped.send_replace(true);
    }

    /// Takes in another pod's copies.
    fn install(&self, copies: Vec<owner::Copy>) {
        self.pod.owner.install(&copies, self.pod.now());
    }

    /// Serves one data plane's stream: its requests decided as they come, its give-backs
    /// given, the view sent first and whenever it changes; until either side ends it.
    ///
    /// The keys its decisions change are handed to the pod's cycle once a cycle of the
    /// stream's own, and as it ends, never request by request: what every stream shares
    /// stays off the request's path. A change waits up to two cycles to be copied.
    async fn serve(
        self,
        requests: Streaming<wire::Upstream>,
        answers: mpsc::Sender<Result<wire::Downstream, Status>>,
    ) {
        let mut handles = Handles::new(self.pod.hasher.clone());
        self.decide_all(&mut handles, requests, answers).await;
        // A key marked changed and handed to no cycle would never be copied again.
        self.hand_over(&mut handles);
    }

    fn hand_over(&self, handles: &mut Handles<RandomState>) {
        let changed = handles.take_changed();
        if !changed.is_empty() {
            self.pod
                .changed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .extend(changed);
        }
    }

    async fn decide_all(
        &self,
        handles: &mut Handles<RandomState>,
        mut requests: Streaming<wire::Upstream>,
        answers: mpsc::Sender<Result<wire::Downstream, Status>>,
    ) {
        let pod = &self.pod;
        // Ends with the view: a stopped pod's membership ends, and its view with it.
        let mut view = pod.view.clone();
        // Sent first below: a change is one after that.
        view.mark_unchanged();
        let mut cycle = tokio::time::interval(pod.cycle);
        cycle.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut served = 0_u64;
        // Seen as it is sent, so that only a change sends it again.
        let send_view = |view: &mut watch::Receiver<(View, bool)>| wire::Downstream {
            message: Some(wire::FromOwner::View(
                view.borrow_and_update().0.clone().into(),
            )),
        };
        if answers.send(Ok(send_view(&mut view))).await.is_err() {
            return;
        }
        loop {
            let message = tokio::select! {
                _ = cycle.tick() => {
                    self.hand_over(handles);
                    continue;
                }
                message = requests.message() => message,
                changed = view.changed() => {
                    if changed.is_err() || answers.send(Ok(send_view(&mut view))).await.is_err() {
                        return;
                    }
                    continue;
                }
            };
            let message = match message {
                Ok(Some(message)) => message,
                Ok(None) => return,
                Err(status) => {
                    let _gone = answers.send(Err(status)).await;
                    return;
                }
            };
            let now = pod.now();
            let answer = match message.message {
                Some(wire::ToOwner::Decide(decide)) => match owner::Decide::try_from(decide) {
                    Ok(decide) => Some(pod.owner.decide(handles, &decide, now)),
                    Err(invalid) => {
                        let _gone = answers
                            .send(Err(Status::invalid_argument(invalid.to_string())))
                            .await;
                        return;
                    }
                },
                Some(wire::ToOwner::GiveBack(back)) => match owner::GiveBack::try_from(back) {
                    Ok(back) => {
                        pod.owner.give_back(handles, &back, now);
                        None
                    }
                    Err(invalid) => {
                        let _gone = answers
                            .send(Err(Status::invalid_argument(invalid.to_string())))
                            .await;
                        return;
                    }
                },
                None => None,
            };
            served += 1;
            if served.is_multiple_of(PRUNE_EVERY) {
                handles.prune();
            }
            if let Some(answer) = answer {
                let down = wire::Downstream {
                    message: Some(wire::FromOwner::Decided(answer.into())),
                };
                if answers.send(Ok(down)).await.is_err() {
                    return;
                }
            }
        }
    }
}

/// Sends one other pod its copies, in order, coalescing what waits to the latest of each
/// key; a copy lost to a failed call is the key's to send again at its next change.
struct CopySender {
    queue: mpsc::Sender<(PodId, Vec<wire::Copy>)>,
}

impl CopySender {
    fn spawn(client: Client) -> Self {
        let (queue, mut waiting) = mpsc::channel::<(PodId, Vec<wire::Copy>)>(COPIES_WAITING);
        tokio::spawn(async move {
            while let Some((from, first)) = waiting.recv().await {
                let mut latest: BTreeMap<Vec<u8>, wire::Copy> = BTreeMap::new();
                for copy in first {
                    latest.insert(copy.key.clone(), copy);
                }
                while let Ok((_, more)) = waiting.try_recv() {
                    for copy in more {
                        latest.insert(copy.key.clone(), copy);
                    }
                }
                let copies = wire::Copies {
                    from,
                    entries: latest.into_values().collect(),
                };
                let _lost: Result<Empty, Error> = client.call(copies, COPY).await;
            }
        });
        Self { queue }
    }

    fn send(&self, from: PodId, copies: Vec<wire::Copy>) {
        let _full_or_stopped = self.queue.try_send((from, copies));
    }
}

/// A channel's receiving end as a stream, which is all tonic needs of a stream's messages.
struct Received<T>(mpsc::Receiver<T>);

impl<T> Stream for Received<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().0.poll_recv(cx)
    }
}

/// The synchronous service as a gRPC server, in front of its pod.
#[derive(Clone)]
pub struct SyncServer {
    pod: SyncPod,
}

impl SyncServer {
    /// Serves `pod`.
    #[must_use]
    pub fn new(pod: SyncPod) -> Self {
        Self { pod }
    }
}

impl NamedService for SyncServer {
    const NAME: &'static str = crate::grpc::SERVICE;
}

struct DecideCall(SyncPod);

impl tonic::server::StreamingService<wire::Upstream> for DecideCall {
    type Response = wire::Downstream;
    type ResponseStream = Received<Result<wire::Downstream, Status>>;
    type Future = Boxed<Result<Response<Self::ResponseStream>, Status>>;

    fn call(&mut self, request: Request<Streaming<wire::Upstream>>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let (answers, answered) = mpsc::channel(ANSWERS_WAITING);
            tokio::spawn(pod.serve(request.into_inner(), answers));
            Ok(Response::new(Received(answered)))
        })
    }
}

struct CopyCall(SyncPod);

impl tonic::server::UnaryService<wire::Copies> for CopyCall {
    type Response = Empty;
    type Future = Boxed<Result<Response<Empty>, Status>>;

    fn call(&mut self, request: Request<wire::Copies>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let copies = request
                .into_inner()
                .entries
                .into_iter()
                .map(owner::Copy::try_from)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|invalid| Status::invalid_argument(invalid.to_string()))?;
            pod.install(copies);
            Ok(Response::new(Empty {}))
        })
    }
}

struct BeatCall(SyncPod);

impl tonic::server::UnaryService<WireHeartbeat> for BeatCall {
    type Response = Empty;
    type Future = Boxed<Result<Response<Empty>, Status>>;

    fn call(&mut self, request: Request<WireHeartbeat>) -> Self::Future {
        let pod = self.0.clone();
        Box::pin(async move {
            let beat = Heartbeat::try_from(request.into_inner())
                .map_err(|invalid| Status::invalid_argument(invalid.to_string()))?;
            let _stopped = pod.pod.heartbeats.send(beat).await;
            Ok(Response::new(Empty {}))
        })
    }
}

impl tower_service::Service<http::Request<tonic::body::Body>> for SyncServer {
    type Response = http::Response<tonic::body::Body>;
    type Error = Infallible;
    type Future = Boxed<Result<Self::Response, Infallible>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let pod = self.pod.clone();
        Box::pin(async move {
            if pod.pod.stopped() {
                return Ok(Status::unavailable("stopped").into_http());
            }
            Ok(match request.uri().path() {
                DECIDE => {
                    let mut grpc = limited(Prost::<wire::Downstream, wire::Upstream>::default());
                    grpc.streaming(DecideCall(pod), request).await
                }
                COPY => {
                    let mut grpc = limited(Prost::<Empty, wire::Copies>::default());
                    grpc.unary(CopyCall(pod), request).await
                }
                HEARTBEAT => {
                    let mut grpc = limited(Prost::<Empty, WireHeartbeat>::default());
                    grpc.unary(BeatCall(pod), request).await
                }
                _ => Status::unimplemented("no such method").into_http(),
            })
        })
    }
}

/// A data plane's stream to one synchronous pod: what it sends up, and what comes down.
#[derive(Debug)]
pub struct DecideStream {
    /// Requests and give-backs, sent up in order.
    pub up: mpsc::Sender<wire::Upstream>,
    /// Answers and views, as they come down.
    pub down: Streaming<wire::Downstream>,
}

/// Opens a stream to the synchronous pod at `address` (`host:port`): connected within a
/// second, and then held open for as long as both sides keep it, with no deadline on the
/// whole.
///
/// # Errors
///
/// When `address` cannot be made into a URI, or the stream cannot be opened.
pub async fn decide_stream(address: &str) -> Result<DecideStream, Error> {
    let channel: Channel = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(Duration::from_secs(1))
        .tcp_nodelay(true)
        .connect_lazy();
    let mut grpc = tonic::client::Grpc::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    grpc.ready()
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let (up, sent) = mpsc::channel(ANSWERS_WAITING);
    let response = grpc
        .streaming(
            Request::new(Received(sent)),
            PathAndQuery::from_static(DECIDE),
            Prost::<wire::Upstream, wire::Downstream>::default(),
        )
        .await?;
    Ok(DecideStream {
        up,
        down: response.into_inner(),
    })
}
