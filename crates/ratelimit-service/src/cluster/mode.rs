//! What a pod of the store on sockets ([`StorePod`]) needs of the pod of both modes it runs,
//! beside the store within it: the data planes' streams it serves ([`plane`]) — the
//! synchronous mode's [`Decide`] and the asynchronous mode's [`Exchange`] — what it counts of
//! itself for its scrape ([`counted`]), and which of its messages a client asks
//! ([`asks_as`]).

use super::link::DECIDE;
use super::pod::{StorePod, serve_plane};
use super::wire::{self, Asking, Invalid, Telling, asynchronous};
use edgerush_cluster::NodeId;
use edgerush_cluster_asynchronous::{Message as AsyncMessage, TurnedAway};
use edgerush_cluster_modes::{Message as BothMessage, Pod as BothPod, Value as BothValue};
use edgerush_cluster_synchronous::Message as SyncMessage;
use std::future::Future;
use std::pin::Pin;

/// The data planes' stream of the asynchronous mode.
pub const EXCHANGE: &str = "/edgerush.cluster.v1.Cluster/Exchange";

/// A data plane's call, served: what goes back to it.
pub type Served = Pin<Box<dyn Future<Output = http::Response<tonic::body::Body>> + Send>>;

/// How a pod serves one of the data planes' streams: the call, served as the stream's.
pub type PlaneServer = fn(StorePod, http::Request<tonic::body::Body>) -> Served;

/// The data planes' stream of `method`, if it is one, as the pod serves it: each mode's.
#[must_use]
pub fn plane(method: &str) -> Option<PlaneServer> {
    match method {
        DECIDE => Some(serve_plane::<Decide>),
        EXCHANGE => Some(serve_plane::<Exchange>),
        _ => None,
    }
}

/// One of the data planes' streams: what goes up it and down it, as the pod's messages.
pub trait Stream: Send + 'static {
    /// What a data plane sends up it.
    type Asking: prost::Message + Default + Send + 'static;
    /// What it is told down it.
    type Telling: prost::Message + Default + Send + 'static;

    /// What a data plane sends up, as the message of `client`, the stream's.
    ///
    /// # Errors
    ///
    /// A message the pod cannot use.
    fn asked_from(asking: Self::Asking, client: NodeId) -> Result<BothMessage, Invalid>;

    /// What the pod tells a client, as the stream carries it down; none for anything else.
    fn telling(message: BothMessage) -> Option<Self::Telling>;

    /// The number of the check `asking` is, if it is one: the pod's task answers it itself,
    /// never its mode. A stream that carries no checks has none.
    fn check(asking: &Self::Asking) -> Option<u64> {
        let _ = asking;
        None
    }

    /// The pod's answer to check `id`, as the stream carries it down.
    fn checked(id: u64) -> Option<Self::Telling> {
        let _ = id;
        None
    }
}

/// The synchronous mode's stream (`/edgerush.cluster.v1.Cluster/Decide`): requests,
/// requests to gather and checks up; answers, tables and checks answered down.
#[derive(Debug, Clone, Copy)]
pub struct Decide;

/// The asynchronous mode's stream ([`EXCHANGE`]): batches up, answers and tables down, an
/// owner's relayed by a pod it forwarded entries to among them.
#[derive(Debug, Clone, Copy)]
pub struct Exchange;

impl Stream for Decide {
    type Asking = Asking;
    type Telling = Telling;

    fn asked_from(asking: Asking, client: NodeId) -> Result<BothMessage, Invalid> {
        wire::asked_from::<BothValue>(asking, client).map(BothMessage::Synchronous)
    }

    /// The synchronous mode's answers, and the store's tables.
    fn telling(message: BothMessage) -> Option<Telling> {
        match message {
            BothMessage::Synchronous(message) => wire::telling(message),
            BothMessage::Store(message) => wire::telling(SyncMessage::<BothValue>::Store(message)),
            BothMessage::Asynchronous(_) => None,
        }
    }

    fn check(asking: &Asking) -> Option<u64> {
        wire::check(asking)
    }

    fn checked(id: u64) -> Option<Telling> {
        Some(wire::checked(id))
    }
}

impl Stream for Exchange {
    type Asking = asynchronous::Batch;
    type Telling = asynchronous::Answering;

    fn asked_from(batch: asynchronous::Batch, _: NodeId) -> Result<BothMessage, Invalid> {
        asynchronous::batch_from(batch)
            .map(|batch| BothMessage::Asynchronous(AsyncMessage::Batch(batch)))
    }

    /// The asynchronous mode's answers, an owner's relayed among them, and the store's tables.
    fn telling(message: BothMessage) -> Option<asynchronous::Answering> {
        match message {
            BothMessage::Asynchronous(message) => asynchronous::answering(message),
            BothMessage::Store(message) => {
                asynchronous::answering(AsyncMessage::<BothValue>::Store(message))
            }
            BothMessage::Synchronous(_) => None,
        }
    }
}

/// What a pod counts of itself: each mode's own counts, its keys of each mode, and what it did
/// not serve, the keys of the other mode's.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BothCounted {
    /// The synchronous mode's requests it decided from its copy, their owner unheard.
    pub decided_from_copy: u64,
    /// What it answered the asynchronous mode's data planes unserved, by why.
    pub turned_away: TurnedAway,
    /// Requests, and entries of batches, it did not serve, their keys of the other mode's.
    pub wrong_kind: u64,
    /// Its synchronous mode's keys.
    pub synchronous: KindCounted,
    /// Its asynchronous mode's keys.
    pub asynchronous: KindCounted,
}

/// What a pod's store counts of one mode's keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KindCounted {
    /// The keys it holds, owned and copies.
    pub keys: u64,
    /// About the bytes they take.
    pub key_bytes: u64,
    /// Keys forgotten, their time come.
    pub forgotten: u64,
    /// Keys evicted past the cap.
    pub evicted: u64,
}

/// The store's number for the synchronous mode's keys, as the value of both modes gives it
/// (`Held::kind`): a key not yet given to a mode counts with them.
const SYNCHRONOUS: usize = 0;
/// The store's number for the asynchronous mode's keys.
const ASYNCHRONOUS: usize = 1;

/// What `pod` counts of itself now.
#[must_use]
pub fn counted(pod: &BothPod) -> BothCounted {
    let store = pod.store();
    let keys = store.keys_by_kind();
    let (forgotten, evicted) = (store.forgotten_by_kind(), store.evicted_by_kind());
    let of = |kind: usize| KindCounted {
        keys: u64::try_from(keys[kind].0).unwrap_or(u64::MAX),
        key_bytes: keys[kind].1,
        forgotten: forgotten[kind],
        evicted: evicted[kind],
    };
    BothCounted {
        decided_from_copy: pod.decided_from_copy(),
        turned_away: pod.turned_away(),
        wrong_kind: pod.wrong_kind(),
        synchronous: of(SYNCHRONOUS),
        asynchronous: of(ASYNCHRONOUS),
    }
}

/// Whether `message`, asked by client `client`, is one a client asks, naming it: a
/// synchronous request or request to gather, not one forwarded; or a batch, which whatever
/// data plane it names is answered on the stream it came up.
#[must_use]
pub const fn asks_as(message: &BothMessage, client: NodeId) -> bool {
    match message {
        BothMessage::Synchronous(
            SyncMessage::Request {
                client: asker,
                via: None,
                ..
            }
            | SyncMessage::Gather { client: asker, .. },
        ) => *asker == client,
        BothMessage::Asynchronous(message) => matches!(message, AsyncMessage::Batch(_)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_cluster::{Held, Message};
    use edgerush_cluster_asynchronous::{Answers, Batch, Value as AsyncValue};
    use edgerush_cluster_synchronous::Value as SyncValue;

    /// A data plane's stream asks the asynchronous mode a batch alone: never what pods send
    /// each other, nor the store's.
    #[test]
    fn a_data_plane_asks_the_asynchronous_mode_a_batch_alone() {
        let batch = Batch {
            from: 1,
            stamp: (1, 1),
            made: 0,
            cycle: 10_000_000,
            entries: Vec::new(),
        };
        let answers = Answers {
            life: 1,
            number: 1,
            made: 0,
            entries: Vec::new(),
            unserved: Vec::new(),
            table: None,
        };
        let client = 1 << 63;
        assert!(asks_as(
            &BothMessage::Asynchronous(AsyncMessage::Batch(batch.clone())),
            client
        ));
        for not in [
            AsyncMessage::Forwarded {
                client,
                via: 2,
                batch,
            },
            AsyncMessage::Relayed {
                client,
                owner: 2,
                answers: answers.clone(),
            },
            AsyncMessage::Answers(answers),
        ] {
            let not = BothMessage::Asynchronous(not);
            assert!(!asks_as(&not, client), "{not:?}");
        }
        let store = BothMessage::Store(Message::TableRequest);
        assert!(!asks_as(&store, client), "{store:?}");
    }

    /// A pod serves each mode's stream, and no other.
    #[test]
    fn a_pod_serves_each_modes_stream() {
        let methods = [DECIDE, EXCHANGE, "/edgerush.cluster.v1.Cluster/Other"];
        assert_eq!(
            methods.map(|method| plane(method).is_some()),
            [true, true, false]
        );
    }

    /// The numbers a pod reads each mode's keys under are the ones its value counts them
    /// under.
    #[test]
    fn each_modes_keys_are_counted_under_its_own_number() {
        let synchronous = BothValue::Synchronous(SyncValue::Bucket { due: 0, given: 0 });
        let asynchronous = BothValue::Asynchronous(AsyncValue::default());
        assert_eq!(synchronous.kind(), SYNCHRONOUS);
        assert_eq!(asynchronous.kind(), ASYNCHRONOUS);
        assert_eq!(BothValue::Fresh.kind(), SYNCHRONOUS);
    }
}
