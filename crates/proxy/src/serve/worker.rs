//! A worker: made on its own thread, swept and timed, drained, accepting, and the places
//! it lends its exchanges.

use super::{Admitted, Deadlines, Proxy, Validation, Worker, h2_settings, unix_now};
use crate::connections::Loads;
use crate::descriptors::Descriptors;
use crate::downstream::h1::connection as h1;
use crate::downstream::h1::date::HttpDate;
use crate::downstream::h1::deadlines::Bounds;
use crate::drain::Drain;
use crate::metrics::{Answer, Said, Socket};
use crate::places::{Places, Refused};
use crate::received::Received;
use crate::slots::WorkerSlots;
use crate::storage::Storage;
use crate::timers::Timers;
use crate::tls::Tls;
use crate::upstream::balancing;
use crate::upstream::h1::H1Limits;
use crate::upstream::h1::blocks::{Blocks, SMALL, Sizes};
use crate::upstream::h1::pool::Pool;
use crate::upstream::h2::client::Client as H2Client;
use std::cell::{Cell, RefCell};
use std::future::{Future, poll_fn};
use std::io;
use std::pin::pin;
use std::rc::{Rc, Weak};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Poll;
use tokio::net::{TcpListener, TcpStream};

impl Worker {
    /// A worker of `proxy`, with upstream connections of its own. Made on the thread that
    /// serves with it, inside that thread's `LocalSet`, and never moved off it.
    #[must_use]
    pub fn new(proxy: Arc<Proxy>) -> Rc<Self> {
        Self::with_limits(proxy, H1Limits::default())
    }

    /// The same, on bounds of the caller's choosing. The stage has no configuration for
    /// these; what it has is one value per worker
    /// ([13 §7](../../docs/13-http1-upstream.md)), which a benchmark may override and
    /// then say that it did.
    #[must_use]
    pub fn with_limits(proxy: Arc<Proxy>, limits: H1Limits) -> Rc<Self> {
        Self::with_deadlines(proxy, limits, Deadlines::default())
    }

    /// The same, as the `position`th of the data plane's workers: what the QUIC
    /// connection IDs it issues say, so that another worker can hand it a datagram of its
    /// connections'. `connections` counts every worker's connections, its own at
    /// `position`, and is what a WebSocket it carries for an HTTP/2 or HTTP/3 client is
    /// counted in.
    #[must_use]
    pub fn at(
        proxy: Arc<Proxy>,
        limits: H1Limits,
        position: u16,
        connections: Arc<Loads>,
    ) -> Rc<Self> {
        Self::made(
            proxy,
            limits,
            Deadlines::default(),
            position,
            Some(connections),
        )
    }

    /// The same, holding client connections to `deadlines`.
    pub(super) fn with_deadlines(
        proxy: Arc<Proxy>,
        limits: H1Limits,
        deadlines: Deadlines,
    ) -> Rc<Self> {
        Self::made(proxy, limits, deadlines, 0, None)
    }

    pub(super) fn made(
        proxy: Arc<Proxy>,
        limits: H1Limits,
        deadlines: Deadlines,
        position: u16,
        connections: Option<Arc<Loads>>,
    ) -> Rc<Self> {
        let body_limits = Rc::new(limits);
        let h1 = h1::Settings {
            limits: Rc::clone(&body_limits),
            bounds: Bounds {
                first_request: deadlines.first_request,
                // 14 §8's ten seconds for a head once it has begun, never longer than the
                // wait for it to begin.
                next_head: Bounds::default().next_head.min(deadlines.next_request),
                keep_alive: deadlines.next_request,
                idle: deadlines.idle,
                ..Bounds::default()
            },
            budget: h1::Budget::default(),
        };
        let blocks = Rc::new(RefCell::new(Blocks::new(
            Sizes::within(&limits, SMALL),
            Storage::new(limits.storage),
        )));
        let received = Received::new(Rc::clone(blocks.borrow().storage()));
        let (validations, validated) = {
            let snapshot = proxy.current.load();
            let validations = (0..proxy.listeners.len())
                .map(|listener| Validation {
                    tls: snapshot.tls.get(listener).cloned().flatten(),
                    drain: Rc::new(Drain::default()),
                })
                .collect();
            (RefCell::new(validations), Cell::new(snapshot.generation))
        };
        let batches = proxy.logs.worker();
        let pool = Rc::new(RefCell::new(Pool::default()));
        // Room among the files is made by closing an idle socket of the worker's.
        let files = Descriptors::new(limits.descriptors);
        let idle = Rc::downgrade(&pool);
        files.evicting(move || {
            idle.upgrade()
                .is_some_and(|pool| pool.try_borrow_mut().is_ok_and(|mut pool| pool.close_one()))
        });
        Rc::new_cyclic(|me| Self {
            proxy,
            pool,
            blocks,
            timers: Timers::new(),
            places: Places::new(limits.exchanges),
            limits,
            deadlines,
            date: Cell::new(HttpDate::from_unix(unix_now())),
            drain: Rc::new(Drain::default()),
            h2: H2Client::new(
                h2_settings(&limits),
                Rc::clone(&received),
                Rc::clone(&files),
            ),
            balancing: RefCell::default(),
            accepted: Cell::new(0),
            body_limits,
            me: Weak::clone(me),
            position,
            slots: WorkerSlots::default(),
            h1,
            connections,
            received,
            handshakes: Rc::default(),
            validations,
            validated,
            routes: RefCell::default(),
            batches,
            said: Said::default(),
            watcher: Rc::default(),
            files,
        })
    }

    /// Starts this worker's drain now: it stops accepting, HTTP/1 connections close once
    /// idle and say so on the answer in hand, and HTTP/2 connections are told to go
    /// (03 §10). A worker's sweep does this by itself once [`Proxy::drain`] has been called.
    pub fn drain(&self) {
        self.drain.start();
        // Every connection's and tunnel's drain too; one made later is started as it is.
        let drains: Vec<Rc<Drain>> = self
            .validations
            .borrow()
            .iter()
            .map(|validation| Rc::clone(&validation.drain))
            .chain(self.routes.borrow().values().map(Rc::clone))
            .collect();
        for drain in drains {
            drain.start();
        }
    }

    /// What a tunnel routed by `key` drains with besides its client's connection: the drain
    /// of its listener, route and upstream, which a reload that takes any of them away
    /// starts (03 §10). None, for a tunnel whose route has no key, is the worker's own.
    pub(super) fn route_drain(&self, key: Option<u64>) -> Rc<Drain> {
        let Some(key) = key else {
            return Rc::clone(&self.drain);
        };
        if let Some(drain) = self.routes.borrow().get(&key) {
            return Rc::clone(drain);
        }
        let drain = Rc::new(Drain::default());
        // A key the config in force no longer has — taken away while a WebSocket's
        // handshake was under way — is drained at once, as is one of a worker that drains.
        if self.drain.is_on() || !self.proxy.current.load().routed.holds(key) {
            drain.start();
            return drain;
        }
        self.routes.borrow_mut().insert(key, Rc::clone(&drain));
        drain
    }

    /// What a connection accepted on `listener` with `tls` drains with: the drain of the
    /// client validation it is accepted under (03 §3). The connection read the snapshot on
    /// this thread, no earlier than the worker last did, so a validation that does not hold
    /// for it is one a reload replaced, and the connections accepted under it drain now.
    pub(super) fn drain_for(&self, listener: usize, tls: Option<&Arc<Tls>>) -> Rc<Drain> {
        let (drain, replaced) = {
            let mut validations = self.validations.borrow_mut();
            let Some(validation) = validations.get_mut(listener) else {
                return Rc::clone(&self.drain);
            };
            let replaced = (!validation.holds_for(tls)).then(|| self.replace(validation, tls));
            (Rc::clone(&validation.drain), replaced)
        };
        // Started once nothing is borrowed: a waker may run what it wakes.
        if let Some(replaced) = replaced {
            replaced.start();
        }
        drain
    }

    /// Brings the validations and the routes up to the snapshot in force, once a reload has
    /// come: the connections of a listener whose client validation it replaced drain (03 §3),
    /// and those of one whose certificates alone changed do not; the tunnels of a listener,
    /// route and upstream it took away drain (03 §10). For the sweep.
    pub(super) fn revalidate(&self) {
        let snapshot = self.proxy.current.load();
        if snapshot.generation == self.validated.get() {
            return;
        }
        self.validated.set(snapshot.generation);
        let mut replaced: Vec<Rc<Drain>> = {
            let mut validations = self.validations.borrow_mut();
            validations
                .iter_mut()
                .enumerate()
                .filter_map(|(listener, validation)| {
                    let tls = snapshot.tls.get(listener).and_then(Option::as_ref);
                    if validation.holds_for(tls) {
                        // The same front: what it served before is let go of.
                        validation.tls = tls.cloned();
                        return None;
                    }
                    Some(self.replace(validation, tls))
                })
                .collect()
        };
        // A route's drain no tunnel holds is let go of too; the next tunnel makes another.
        self.routes.borrow_mut().retain(|key, drain| {
            if !snapshot.routed.holds(*key) {
                replaced.push(Rc::clone(drain));
                return false;
            }
            Rc::strong_count(drain) > 1
        });
        for drain in replaced {
            drain.start();
        }
    }

    /// Puts a validation for `tls` in the place of `validation`, and gives back the drain of
    /// the one replaced, for the caller to start once nothing is borrowed.
    fn replace(&self, validation: &mut Validation, tls: Option<&Arc<Tls>>) -> Rc<Drain> {
        let drain = Rc::new(Drain::default());
        // A worker that drains drains whatever it accepts from then on.
        if self.drain.is_on() {
            drain.start();
        }
        let replaced = Validation {
            tls: tls.cloned(),
            drain,
        };
        std::mem::replace(validation, replaced).drain
    }

    /// Until this worker drains.
    pub async fn draining(&self) {
        self.drain.started().await;
    }

    /// The next connection on `socket`, or `None` once this worker drains: a draining
    /// worker takes nothing new (03 §10).
    pub async fn accept(&self, socket: &TcpListener) -> Option<io::Result<TcpStream>> {
        // After a batch, everything else the worker has ready goes first: accepting all a
        // backlog holds would put a flood of new connections ahead of the requests of the
        // ones it has (03 §3).
        if self.accepted.get() >= self.limits.accept_batch {
            self.accepted.set(0);
            tokio::task::yield_now().await;
        }
        self.accepted.set(self.accepted.get() + 1);
        let mut draining = pin!(self.drain.notified());
        std::future::poll_fn(|cx| {
            if self.drain.poll_on(draining.as_mut(), cx).is_ready() {
                return Poll::Ready(None);
            }
            socket
                .poll_accept(cx)
                .map(|accepted| Some(accepted.map(|(stream, _)| stream)))
        })
        .await
    }

    /// Looks over the connections this worker is keeping, for as long as it runs.
    ///
    /// One sweep for the worker rather than a timer for every connection, and it is what
    /// clears out a destination that a reload took away and that nothing will ask for
    /// again ([13 §3](../../docs/13-http1-upstream.md)). It waits on the worker's timers
    /// too, beside the sweep. Spawned into the worker's `LocalSet` beside its listeners; a
    /// worker without it keeps what it should drop, and none of its deadlines ever comes.
    pub async fn maintain(self: Rc<Self>) {
        let mut timing = pin!(Rc::clone(&self.timers).run());
        let mut sweeping = pin!(self.sweep());
        poll_fn(|context| {
            if let Poll::Ready(never) = timing.as_mut().poll(context) {
                match never {}
            }
            sweeping.as_mut().poll(context)
        })
        .await;
    }

    /// The sweep itself, for as long as the worker runs.
    async fn sweep(self: Rc<Self>) {
        let every = self.limits.sweep;
        loop {
            // Or at once, when the data plane is about to end and wants the access-log
            // records the worker holds.
            let finishing = {
                let mut slept = pin!(tokio::time::sleep(every));
                let mut asked = pin!(self.batches.asked_to_finish());
                poll_fn(|context| {
                    if asked.as_mut().poll(context).is_ready() {
                        return Poll::Ready(true);
                    }
                    slept.as_mut().poll(context).map(|()| false)
                })
                .await
            };
            self.date.set(HttpDate::from_unix(unix_now()));
            if self.proxy.draining.load(Ordering::Acquire) && !self.drain.is_on() {
                self.drain();
            }
            self.revalidate();
            // Borrowed for the sweep and let go of before anything is waited on again.
            let swept = self.pool.borrow_mut().sweep(&self.limits);
            // And what a burst left parked of the blocks, down to what a quiet worker keeps.
            self.blocks.borrow_mut().sweep();
            self.slots.sweep();
            self.h2.sweep();
            let metrics = &self.proxy.metrics;
            for _discarded in 0..swept {
                metrics.socket(Socket::Discarded);
            }
            // What this worker holds at the moment it last looked. A sweep already walks
            // everything these ask about, so nothing is counted on the request path for
            // them ([13 §7](../../docs/13-http1-upstream.md)).
            let storage = self.blocks.borrow().storage().used();
            metrics.worker().holding(
                &self.said,
                self.places.held(),
                self.idle_connections(),
                storage,
            );
            // What its listeners logged since the last sweep goes to be written, so that a
            // quiet worker holds no record for longer than a sweep (21 §4).
            self.batches.hand_over(&self.proxy.logs);
            if finishing {
                self.batches.finished();
            }
        }
    }

    /// How many connections this worker is keeping. For tests and, later, a gauge.
    #[must_use]
    pub fn idle_connections(&self) -> usize {
        self.pool.borrow().idle()
    }

    /// How many HTTP/2 connections to upstreams this worker has, open or being opened.
    /// For tests and, later, a gauge.
    #[must_use]
    pub fn h2_connections(&self) -> usize {
        self.h2.connections()
    }

    /// Takes a place among the exchanges this worker has in hand for one with `upstream`,
    /// if one is going to it: none once every place is held, and none for an upstream that
    /// holds its fair share once the worker is short of them
    /// ([03 §9](../../docs/03-data-plane.md)). `alone` says the config it was directed by
    /// has no other upstream.
    ///
    /// Nothing waits here. A request arriving at a worker that is already full is
    /// answered, because holding it would cost the very memory the bound is for.
    pub(super) fn admit(
        &self,
        upstream: &balancing::Upstream,
        alone: bool,
    ) -> Result<Admitted, Answer> {
        match self.places.take(upstream.places(), alone) {
            Ok(place) => Ok(Admitted {
                _place: place,
                counted: None,
            }),
            Err(Refused::Full) => Err(Answer::TooBusy),
            Err(Refused::OverShare) => Err(Answer::OverShare),
        }
    }

    /// What this worker serves: the data plane the whole process shares.
    #[must_use]
    pub fn proxy(&self) -> &Arc<Proxy> {
        &self.proxy
    }
}
