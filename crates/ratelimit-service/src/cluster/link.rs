//! What a pod sends to one peer (cluster-store-transplant-plan.md, "T4d"): a queue for each of
//! the two streams, counted in bytes, and a task for each that dials a connection of its own to
//! the peer once something waits, opens its stream with the pod's hello and sends what waits,
//! cut into pieces.
//!
//! A message past what its queue holds is dropped and the connection kept; a connection lost
//! drops what waited for it, and the next is dialled for the next message, backed off. The
//! store sends again what matters, as it does over the simulation's links, which keep each
//! sender's order and may lose messages. A link closed sends what waits before its streams
//! end, as a drained pod's last changes must go (T4d2).

use super::wire::{self, Empty, Hello, Outgoing, PIECE, Piece};
use crate::grpc::{CALL_TIMEOUT, Error, KEEP_ALIVE, MAX_MESSAGE, Prost};
use crate::secure::{self, Dialler};
use boring::ssl::SslConnector;
use bytes::Bytes;
use futures_core::Stream;
use http::uri::PathAndQuery;
use prost::Message as _;
use std::fmt::Debug;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tonic::Request;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};

/// A channel's receiving end as a stream, which is all tonic needs of a stream's messages.
pub(crate) struct Received<T>(pub(crate) mpsc::Receiver<T>);

impl<T> Stream for Received<T> {
    type Item = T;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<T>> {
        self.get_mut().0.poll_recv(cx)
    }
}

/// The most bytes of copies that wait for one peer (decided): the 2 MiB of copies in parts
/// the store has unanswered to a pod at most, and room for some cycles' copies of one part,
/// which only the rate keys change at bounds.
pub const COPIES_WAITING: usize = 8 << 20;

/// The most bytes of everything else that wait for one peer (decided): a handoff's part,
/// with heartbeats and tables beside it.
pub const REST_WAITING: usize = 2 << 20;

/// How long a stream waits to dial again after a dial that failed, at first and at most:
/// doubled each time until one succeeds.
const FIRST_BACK_OFF: Duration = Duration::from_millis(50);
const MOST_BACK_OFF: Duration = Duration::from_secs(1);

/// The metadata a stream's hello goes in, encoded.
pub(crate) const HELLO: &str = "edgerush-hello-bin";

/// The cluster's service, as gRPC calls it, and its methods.
pub(crate) const SERVICE: &str = "edgerush.cluster.v1.Cluster";
pub(crate) const PEER: &str = "/edgerush.cluster.v1.Cluster/Peer";
pub(crate) const COPY: &str = "/edgerush.cluster.v1.Cluster/Copy";
pub(crate) const GREET: &str = "/edgerush.cluster.v1.Cluster/Hello";
pub(crate) const CONTROL: &str = "/edgerush.cluster.v1.Cluster/Control";
pub(crate) const DECIDE: &str = "/edgerush.cluster.v1.Cluster/Decide";

/// How a pod's connections to others are made.
#[derive(Clone, Debug)]
pub enum Dial {
    /// Plain TCP.
    Plain,
    /// TLS with the latest of the pod's connector, each peer's certificate checked against
    /// the host it is dialled by.
    Secured(watch::Receiver<SslConnector>),
}

/// What a pod's sending dropped, by why, since it started.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Dropped {
    /// Messages past what their queue holds.
    pub full: u64,
    /// Messages waiting on a connection that was lost, or for a peer with no address known.
    pub lost: u64,
    /// Messages longer than a receiver takes.
    pub too_long: u64,
}

/// [`Dropped`], counted as the pod's sending goes.
#[derive(Debug, Default)]
pub(crate) struct Counts {
    full: AtomicU64,
    pub(crate) lost: AtomicU64,
    too_long: AtomicU64,
}

impl Counts {
    pub(crate) fn dropped(&self) -> Dropped {
        Dropped {
            full: self.full.load(Ordering::Relaxed),
            lost: self.lost.load(Ordering::Relaxed),
            too_long: self.too_long.load(Ordering::Relaxed),
        }
    }
}

/// What waits to go on one stream to one peer, by its bytes.
#[derive(Debug)]
struct Queue {
    sender: mpsc::UnboundedSender<Bytes>,
    waiting: Arc<AtomicUsize>,
    most: usize,
}

impl Queue {
    fn new(most: usize) -> (Self, mpsc::UnboundedReceiver<Bytes>, Arc<AtomicUsize>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        let waiting = Arc::new(AtomicUsize::new(0));
        let queue = Self {
            sender,
            waiting: Arc::clone(&waiting),
            most,
        };
        (queue, receiver, waiting)
    }

    /// Queues `encoded` if it fits beside what waits, or nothing waits, so that any message
    /// a receiver takes goes: false if it is dropped.
    fn push(&self, encoded: Bytes) -> bool {
        let length = encoded.len();
        // Only the pod's task pushes, so what waits can only fall between the look and the add.
        let waiting = self.waiting.load(Ordering::Acquire);
        if waiting != 0 && waiting.saturating_add(length) > self.most {
            return false;
        }
        self.waiting.fetch_add(length, Ordering::AcqRel);
        self.sender.send(encoded).is_ok()
    }
}

/// What a pod sends to one peer: its two queues, and the tasks that send them, stopped when
/// it is dropped.
#[derive(Debug)]
pub(crate) struct Link {
    rest: Queue,
    copies: Queue,
    tasks: Tasks,
    counts: Arc<Counts>,
}

/// A link's tasks, stopped when dropped.
#[derive(Debug)]
pub(crate) struct Tasks([JoinHandle<()>; 2]);

impl Tasks {
    /// Waits for both to end.
    pub(crate) async fn ended(&mut self) {
        for task in &mut self.0 {
            let _ended = task.await;
        }
    }
}

impl Drop for Tasks {
    fn drop(&mut self) {
        for task in &self.0 {
            task.abort();
        }
    }
}

impl Link {
    /// Sending to the peer at `address`, as `hello`, from the IP the pod is reached at if its
    /// address is one, its drops counted in `counts`.
    ///
    /// # Errors
    ///
    /// When `address` cannot be dialled: not a URI, or, secured, naming no host.
    pub(crate) fn new(
        address: &str,
        hello: &Hello,
        dial: &Dial,
        counts: &Arc<Counts>,
    ) -> Result<Self, Error> {
        let from = hello.address.parse::<SocketAddr>().ok().map(|own| own.ip());
        let hello = Bytes::from(hello.encode_to_vec());
        let (rest, rest_waiting, rest_count) = Queue::new(REST_WAITING);
        let (copies, copies_waiting, copies_count) = Queue::new(COPIES_WAITING);
        let tasks = Tasks([
            tokio::spawn(send_all(
                channel(address, dial, from)?,
                PEER,
                hello.clone(),
                rest_waiting,
                rest_count,
                Arc::clone(counts),
            )),
            tokio::spawn(send_all(
                channel(address, dial, from)?,
                COPY,
                hello,
                copies_waiting,
                copies_count,
                Arc::clone(counts),
            )),
        ]);
        Ok(Self {
            rest,
            copies,
            tasks,
            counts: Arc::clone(counts),
        })
    }

    /// Queues `outgoing` on its stream, or drops it, counted: past what a receiver takes, or
    /// past what its queue holds.
    pub(crate) fn send<W: prost::Message + Default + Debug>(&self, outgoing: &Outgoing<W>) {
        let (encoded, queue) = match outgoing {
            Outgoing::Peer(message) => (message.encode_to_vec(), &self.rest),
            Outgoing::Copy(message) => (message.encode_to_vec(), &self.copies),
        };
        if encoded.len() > MAX_MESSAGE {
            self.counts.too_long.fetch_add(1, Ordering::Relaxed);
        } else if !queue.push(Bytes::from(encoded)) {
            self.counts.full.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Closes it: each stream sends what waits on it, then ends once the peer has read it all,
    /// and its task with it. The tasks, stopped if dropped before.
    pub(crate) fn close(self) -> Tasks {
        self.tasks
    }
}

/// A connection of its own to the pod at `address`, dialled from `from`, made when first
/// needed and again once lost, pinged as the service's other clients are (#127), so that one
/// to a peer wedged is let go; no timeout on a call, which a stream outlives. Dialled from the
/// IP the pod is reached at, as a pod in Kubernetes does with its only IP, a pod is known by
/// it to every peer.
pub(crate) fn channel(address: &str, dial: &Dial, from: Option<IpAddr>) -> Result<Channel, Error> {
    let endpoint = Endpoint::from_shared(format!("http://{address}"))?
        .connect_timeout(CALL_TIMEOUT)
        .local_address(from)
        .tcp_nodelay(true)
        .http2_keep_alive_interval(KEEP_ALIVE)
        .keep_alive_timeout(KEEP_ALIVE)
        .keep_alive_while_idle(true);
    Ok(match dial {
        Dial::Plain => endpoint.connect_lazy(),
        Dial::Secured(tls) => {
            let name = secure::host(endpoint.uri()).ok_or(Error::NoHost)?;
            let dialler = Dialler::new(address, name, tls.clone(), CALL_TIMEOUT).from(from);
            endpoint.connect_with_connector_lazy(dialler)
        }
    })
}

/// Sends what `queue` holds on `path` through `channel`, as `hello`, one stream at a time:
/// opened once something waits, and again after one is lost, backed off if it never opened.
/// Ends once the link is closed and the stream with what waited has ended, or once the link
/// is dropped.
async fn send_all(
    channel: Channel,
    path: &'static str,
    hello: Bytes,
    mut queue: mpsc::UnboundedReceiver<Bytes>,
    waiting: Arc<AtomicUsize>,
    counts: Arc<Counts>,
) {
    let mut back_off = FIRST_BACK_OFF;
    while let Some(first) = queue.recv().await {
        let (pieces, sent) = mpsc::channel(1);
        let (queued, taken) = (&mut queue, &waiting);
        let forward = async move {
            let mut next = Some(first);
            loop {
                let encoded = match next.take() {
                    Some(encoded) => encoded,
                    None => match queued.recv().await {
                        Some(encoded) => encoded,
                        // Closed: no more pieces, so the stream ends once the peer has read
                        // them.
                        None => {
                            drop(pieces);
                            return;
                        }
                    },
                };
                taken.fetch_sub(encoded.len(), Ordering::AcqRel);
                for piece in wire::pieces(encoded, PIECE) {
                    if pieces.send(piece).await.is_err() {
                        // The stream is gone: its call says so.
                        std::future::pending::<()>().await;
                    }
                }
            }
        };
        let call = stream(channel.clone(), path, hello.clone(), Received(sent));
        tokio::pin!(call);
        let opened = tokio::select! {
            () = forward => {
                let _opened = call.await;
                return;
            }
            opened = &mut call => opened,
        };
        // What waited for a connection lost goes with it.
        while let Ok(encoded) = queue.try_recv() {
            waiting.fetch_sub(encoded.len(), Ordering::AcqRel);
            counts.lost.fetch_add(1, Ordering::Relaxed);
        }
        if opened {
            back_off = FIRST_BACK_OFF;
        } else {
            tokio::time::sleep(back_off).await;
            back_off = back_off.saturating_mul(2).min(MOST_BACK_OFF);
        }
    }
}

/// One stream on `path`, opened with `hello`, carrying `pieces` until it fails or the peer
/// ends it: whether it opened.
async fn stream(
    channel: Channel,
    path: &'static str,
    hello: Bytes,
    pieces: Received<Piece>,
) -> bool {
    let mut request = Request::new(pieces);
    request
        .metadata_mut()
        .insert_bin(HELLO, MetadataValue::from_bytes(&hello));
    let mut grpc = tonic::client::Grpc::new(channel)
        .max_decoding_message_size(MAX_MESSAGE)
        .max_encoding_message_size(MAX_MESSAGE);
    if grpc.ready().await.is_err() {
        return false;
    }
    let Ok(response) = grpc
        .streaming(
            request,
            PathAndQuery::from_static(path),
            Prost::<Piece, Empty>::default(),
        )
        .await
    else {
        return false;
    };
    let mut answers = response.into_inner();
    while let Ok(Some(_)) = answers.message().await {}
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(length: usize) -> Bytes {
        Bytes::from(vec![0; length])
    }

    #[test]
    fn a_queue_takes_what_fits_and_drops_the_rest() {
        let (queue, mut receiver, waiting) = Queue::new(10);
        assert!(queue.push(bytes(6)));
        assert!(queue.push(bytes(4)));
        assert!(!queue.push(bytes(1)));
        assert_eq!(waiting.load(Ordering::Acquire), 10);
        let taken = receiver.try_recv().unwrap();
        waiting.fetch_sub(taken.len(), Ordering::AcqRel);
        assert!(queue.push(bytes(6)));
        assert!(!queue.push(bytes(1)));
    }

    #[test]
    fn an_empty_queue_takes_one_message_past_its_most() {
        let (queue, mut receiver, waiting) = Queue::new(10);
        assert!(queue.push(bytes(25)));
        assert!(!queue.push(bytes(1)));
        let taken = receiver.try_recv().unwrap();
        waiting.fetch_sub(taken.len(), Ordering::AcqRel);
        assert!(queue.push(bytes(1)));
    }
}
