//! The synchronous mode's rate limits (06 §4): every request decided by the owners of its
//! keys in the rate-limiting service. Each worker holds its own stream to each service pod
//! (`edgerush_ratelimit_service::synchronous`), opened when first needed: a request's keys
//! go to their owners as the pods' view has them, those of different owners asked in
//! parallel, and the answers come back to the worker that asked, with no thread between.
//! Admitted under all, or refused, the owners that admitted given their charges back; an
//! owner that does not answer in time, or whose stream breaks, leaves the request to
//! `when_unreachable`.

use super::{Awaited, KeyedBy, Later, Limited, Refusal, Undecided, with_keys};
use edgerush_config::{CompiledLimit, CompiledSynchronous, WhenUnreachable};
use edgerush_ratelimit::service::{Contract, Decided, PodId, View, longest};
use edgerush_ratelimit_service::synchronous::decide_stream;
use edgerush_ratelimit_service::synchronous::wire;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};

/// How many messages may wait for a stream while it is opened: past them, a request is
/// left to `when_unreachable`, as if the owner did not answer.
const QUEUED: usize = 1_024;

/// The process's side of the synchronous mode: the config its workers ask by.
#[derive(Debug)]
pub(crate) struct Process {
    config: CompiledSynchronous,
}

impl Process {
    /// The process's synchronous mode for `config`.
    pub(crate) fn start(config: CompiledSynchronous) -> Arc<Self> {
        Arc::new(Self { config })
    }

    /// Whether it asks as `config` says, so that a reload saying the same keeps it.
    pub(crate) fn is_for(&self, config: &CompiledSynchronous) -> bool {
        self.config == *config
    }
}

/// An owner's answer, or why there is none: its stream broke, or could not be opened.
type Answer = Result<wire::Decided, ()>;

/// What is sent up a stream while it is being opened.
enum Link {
    /// Not open: the next message opens it.
    Down,
    /// Being opened, with what is to go up once it is.
    Opening(Vec<wire::Upstream>),
    /// Open.
    Up(mpsc::Sender<wire::Upstream>),
}

/// A worker's stream to one service pod, and the requests waiting on its answers.
struct Stream {
    address: String,
    link: RefCell<Link>,
    waiting: RefCell<HashMap<u64, oneshot::Sender<Answer>>>,
}

impl Stream {
    /// Sends `message` up, opening the stream first if it is not open; false if it cannot
    /// go: the stream broke, or too much waits for it.
    fn send(self: &Rc<Self>, message: wire::Upstream, view: &Rc<RefCell<View>>) -> bool {
        let mut link = self.link.borrow_mut();
        match &mut *link {
            Link::Up(up) => match up.try_send(message) {
                Ok(()) => true,
                Err(mpsc::error::TrySendError::Full(_)) => false,
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    *link = Link::Down;
                    drop(link);
                    self.broke();
                    false
                }
            },
            Link::Opening(queued) => {
                if queued.len() >= QUEUED {
                    return false;
                }
                queued.push(message);
                true
            }
            Link::Down => {
                *link = Link::Opening(vec![message]);
                tokio::task::spawn_local(open(Rc::downgrade(self), Rc::downgrade(view)));
                true
            }
        }
    }

    /// Hands every request waiting on it the news that its owner will not answer.
    fn broke(&self) {
        let waiting = std::mem::take(&mut *self.waiting.borrow_mut());
        for (_, waiter) in waiting {
            let _gone = waiter.send(Err(()));
        }
    }
}

/// Opens `stream`, sends what waited for it, and hands out its answers and the pods' view
/// until it ends, or the worker lets go of it.
async fn open(stream: Weak<Stream>, view: Weak<RefCell<View>>) {
    let Some(address) = stream.upgrade().map(|stream| stream.address.clone()) else {
        return;
    };
    let opened = decide_stream(&address).await;
    let Some(held) = stream.upgrade() else {
        return;
    };
    let mut down = match opened {
        Ok(opened) => {
            let queued = match std::mem::replace(
                &mut *held.link.borrow_mut(),
                Link::Up(opened.up.clone()),
            ) {
                Link::Opening(queued) => queued,
                Link::Down | Link::Up(_) => Vec::new(),
            };
            for message in queued {
                if opened.up.try_send(message).is_err() {
                    break;
                }
            }
            opened.down
        }
        Err(_) => {
            *held.link.borrow_mut() = Link::Down;
            held.broke();
            return;
        }
    };
    drop(held);
    loop {
        let message = down.message().await;
        let Some(held) = stream.upgrade() else {
            return;
        };
        match message {
            Ok(Some(wire::Downstream {
                message: Some(wire::FromOwner::Decided(decided)),
            })) => {
                if let Some(waiter) = held.waiting.borrow_mut().remove(&decided.id) {
                    let _gone = waiter.send(Ok(decided));
                }
            }
            Ok(Some(wire::Downstream {
                message: Some(wire::FromOwner::View(said)),
            })) => {
                // The latest the pods agreed, whichever stream says it first.
                if let Some(view) = view.upgrade() {
                    let said = View::from(said);
                    let mut view = view.borrow_mut();
                    if said.epoch > view.epoch {
                        *view = said;
                    }
                }
            }
            Ok(Some(wire::Downstream { message: None })) => {}
            Ok(None) | Err(_) => {
                *held.link.borrow_mut() = Link::Down;
                held.broke();
                return;
            }
        }
    }
}

/// A worker's limiter in the synchronous mode: its streams to the service's pods, the view
/// it reads owners from, and its buffers for a request's keys.
pub(crate) struct SyncLimiter {
    process: Arc<Process>,
    streams: Vec<Rc<Stream>>,
    view: Rc<RefCell<View>>,
    next: Cell<u64>,
    keys: RefCell<Vec<Vec<u8>>>,
    contracts: RefCell<Vec<Contract>>,
}

impl std::fmt::Debug for SyncLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SyncLimiter").finish_non_exhaustive()
    }
}

impl SyncLimiter {
    /// A worker's limiter on `process`: a stream to each pod, all of them opened now, and
    /// every pod taken to serve until the pods say otherwise. Every pod's, not only those a
    /// key has needed: the pods' view comes down each, so that a worker whose owners go
    /// still hears from the rest where their keys went.
    pub(crate) fn new(process: Arc<Process>) -> Rc<Self> {
        let streams: Vec<Rc<Stream>> = process
            .config
            .addresses
            .iter()
            .map(|address| {
                Rc::new(Stream {
                    address: address.clone(),
                    link: RefCell::new(Link::Opening(Vec::new())),
                    waiting: RefCell::new(HashMap::new()),
                })
            })
            .collect();
        let pods = (0..process.config.addresses.len())
            .filter_map(|pod| PodId::try_from(pod).ok())
            .collect();
        let view = Rc::new(RefCell::new(View { epoch: 0, pods }));
        for stream in &streams {
            tokio::task::spawn_local(open(Rc::downgrade(stream), Rc::downgrade(&view)));
        }
        Rc::new(Self {
            process,
            streams,
            view,
            next: Cell::new(0),
            keys: RefCell::new(Vec::new()),
            contracts: RefCell::new(Vec::new()),
        })
    }

    /// Whether it asks for `process`.
    pub(crate) fn is_on(&self, process: &Arc<Process>) -> bool {
        Arc::ptr_eq(&self.process, process)
    }

    /// Asks the owners about a request from `client` under those of `limits` that
    /// `limited` marks: each owner about its keys, all at once, the decision to come.
    pub(crate) fn decide_all<'f>(
        self: &Rc<Self>,
        limits: &[Arc<CompiledLimit>],
        limited: u64,
        client: impl Into<KeyedBy<'f>>,
    ) -> Limited {
        let until = tokio::time::Instant::now() + self.process.config.timeout;
        with_keys(
            &self.keys,
            &self.contracts,
            limits,
            limited,
            client.into(),
            |keys| {
                if keys.is_empty() {
                    return Limited::Admit;
                }
                Limited::Later(Later(Awaited::Owners(self.ask(keys, until))))
            },
        )
    }

    /// Sends each owner its part of a request under `keys`, to be answered by `until`.
    fn ask(self: &Rc<Self>, keys: &[(&[u8], Contract)], until: tokio::time::Instant) -> Asked {
        // Which owner each key has, as the view has it; none when the view names a pod
        // this data plane has no address for.
        let owners: Vec<Option<usize>> = {
            let view = self.view.borrow();
            keys.iter()
                .map(|(key, _)| {
                    let (owner, _) = view.owners(key);
                    owner
                        .and_then(|pod| usize::try_from(pod).ok())
                        .filter(|pod| *pod < self.streams.len())
                })
                .collect()
        };
        let mut parts: Vec<Part> = Vec::new();
        for (at, owner) in owners.iter().enumerate() {
            match parts.iter_mut().find(|part| part.pod == *owner) {
                Some(part) => part.positions.push(at),
                None => parts.push(Part {
                    pod: *owner,
                    positions: vec![at],
                    keys: Vec::new(),
                    answer: None,
                }),
            }
        }
        let several = parts.len() > 1;
        for part in &mut parts {
            let Some(pod) = part.pod else {
                continue;
            };
            let id = self.next.get();
            self.next.set(id.wrapping_add(1));
            let sent: Vec<wire::Key> = part
                .positions
                .iter()
                .filter_map(|at| keys.get(*at))
                .map(|(key, contract)| wire::Key {
                    key: key.to_vec(),
                    contract: Some((*contract).into()),
                })
                .collect();
            // Kept only where a give-back may need them: a request asked of one owner has
            // nothing to give back to another.
            if several {
                part.keys.clone_from(&sent);
            }
            let (answered, answer) = oneshot::channel();
            let stream = &self.streams[pod];
            stream.waiting.borrow_mut().insert(id, answered);
            let decide = wire::Upstream {
                message: Some(wire::ToOwner::Decide(wire::Decide { id, keys: sent })),
            };
            if stream.send(decide, &self.view) {
                part.answer = Some((id, answer));
            } else {
                stream.waiting.borrow_mut().remove(&id);
            }
        }
        Asked {
            limiter: Rc::clone(self),
            parts,
            until,
        }
    }

    /// Which of `limits` refused a request from `client` under those `limited` marks, and
    /// how long it should wait, as its owners `told`: by position among its keys, with how
    /// long each waits.
    pub(crate) fn refusal<'a, 'f>(
        &self,
        limits: &'a [Arc<CompiledLimit>],
        limited: u64,
        client: impl Into<KeyedBy<'f>>,
        told: &[(usize, u64)],
    ) -> Refusal<'a> {
        with_keys(
            &self.keys,
            &self.contracts,
            limits,
            limited,
            client.into(),
            |keys| {
                // A key begins with its limit's name and a zero.
                let at = |part: usize| {
                    let (key, _) = keys.get(part)?;
                    let name = key.split(|&byte| byte == 0).next()?;
                    limits
                        .iter()
                        .position(|limit| limit.name.as_bytes() == name)
                };
                let by = told
                    .iter()
                    .filter_map(|(part, _)| at(*part))
                    .filter_map(|at| 1_u64.checked_shl(u32::try_from(at).ok()?))
                    .fold(0, |by, bit| by | bit);
                let longest = longest(told.iter().copied()).and_then(|(part, wait)| {
                    let limit = limits.get(at(part)?)?;
                    Some((&**limit, Duration::from_nanos(wait)))
                });
                Refusal { by, longest }
            },
        )
    }

    /// Gives one request's charge under `keys` back to the owner at `pod`.
    fn give_back(&self, pod: usize, keys: Vec<wire::Key>) {
        if let Some(stream) = self.streams.get(pod) {
            let back = wire::Upstream {
                message: Some(wire::ToOwner::GiveBack(wire::GiveBack { keys })),
            };
            let _lost = stream.send(back, &self.view);
        }
    }
}

/// One owner's part of a request: its keys' places among the request's, the keys as sent
/// where they may be given back, and its answer to come; no pod, or no answer, for a part
/// that could not be asked.
struct Part {
    pod: Option<usize>,
    positions: Vec<usize>,
    keys: Vec<wire::Key>,
    answer: Option<(u64, oneshot::Receiver<Answer>)>,
}

/// A request asked of its owners, waiting on their answers.
pub(crate) struct Asked {
    limiter: Rc<SyncLimiter>,
    parts: Vec<Part>,
    until: tokio::time::Instant,
}

impl Asked {
    /// The decision: refused if any owner refused, the others' charges given back;
    /// admitted if all admitted; otherwise, some owner out of reach, as `when_unreachable`
    /// says.
    pub(crate) async fn decided(self) -> Result<Decided, Undecided> {
        let mut admitted = Vec::new();
        let mut refused_by = Vec::new();
        let mut unreached = None;
        for part in self.parts {
            let Some((id, answer)) = part.answer else {
                unreached = Some(Undecided::Unavailable);
                continue;
            };
            match tokio::time::timeout_at(self.until, answer).await {
                Ok(Ok(Ok(decided))) if decided.admitted => {
                    admitted.push((part.pod, part.keys));
                }
                Ok(Ok(Ok(decided))) => {
                    let at = usize::try_from(decided.position).unwrap_or(usize::MAX);
                    if let Some(&position) = part.positions.get(at) {
                        refused_by.push((position, decided.wait_ns));
                    }
                }
                Ok(Ok(Err(()))) | Ok(Err(_)) => unreached = Some(Undecided::Unavailable),
                Err(_) => {
                    if let Some(stream) = part.pod.and_then(|pod| self.limiter.streams.get(pod)) {
                        stream.waiting.borrow_mut().remove(&id);
                    }
                    unreached = Some(Undecided::Timeout);
                }
            }
        }
        let give_back = |admitted: Vec<(Option<usize>, Vec<wire::Key>)>| {
            for (pod, keys) in admitted {
                if let Some(pod) = pod
                    && !keys.is_empty()
                {
                    self.limiter.give_back(pod, keys);
                }
            }
        };
        if !refused_by.is_empty() {
            give_back(admitted);
            return Ok(Decided {
                ticket: 0,
                admitted: false,
                refused_by: refused_by.into_boxed_slice(),
            });
        }
        let Some(why) = unreached else {
            return Ok(Decided {
                ticket: 0,
                admitted: true,
                refused_by: Box::default(),
            });
        };
        match self.limiter.process.config.when_unreachable {
            WhenUnreachable::Allow => Ok(Decided {
                ticket: 0,
                admitted: true,
                refused_by: Box::default(),
            }),
            // `local` is refused by the config until it is built: refused as `deny` is.
            WhenUnreachable::Deny | WhenUnreachable::Local => {
                give_back(admitted);
                Err(why)
            }
        }
    }
}
