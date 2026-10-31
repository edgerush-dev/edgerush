//! Thread-per-core: for every worker a thread with a single-threaded runtime, a socket of
//! its own on every listener's port, and upstream connections of its own. One data plane
//! is shared by all of them — the config they serve and the counters they add to are the
//! process's, not a worker's.
//!
//! A worker accepts what the kernel gives its sockets and then decides whose connection it
//! is ([`edgerush_proxy::connections`]). One that is another worker's is handed over as a socket that no
//! runtime knows yet, so that it is the other worker's runtime that watches it from its
//! first byte on: once for a connection, and nothing crosses threads for a request.
//!
//! Everything a worker runs lives in a `LocalSet` of its own, so that a connection and all
//! the engine spawns for it stay on the one thread and need not be `Send`.

use crate::essential::{Essential, Told, watched};
use crate::limits::Connections;
use edgerush_proxy::connections::{Held, Loads};
use edgerush_proxy::{AcceptPause, Forwarding, H1Limits, Proxy};
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::runtime::Builder;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::mpsc::{self, Receiver, Sender, UnboundedReceiver, UnboundedSender};
use tokio::task::LocalSet;

/// The most connections a worker holds before it stops accepting ([14 §8]): open ones,
/// ones on their way to it and ones lingering to their close. Past it, connections wait in
/// the kernel's backlog. Fewer where the open files are fewer ([`crate::limits`]).
///
/// [14 §8]: ../../../docs/14-downstream-server.md
pub(crate) const CONNECTIONS_PER_WORKER: usize = 32_768;

/// How many connections can be on their way to a worker that is busy with something else.
/// With more than that, a connection stays with the worker that accepted it.
const ON_THEIR_WAY: usize = 1024;

/// Who decides which worker a connection belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Accept {
    /// The worker that accepted it, by what every worker holds.
    Balanced,
    /// The kernel alone, by the hash it deals connections out by: the control to measure
    /// balancing against.
    Kernel,
}

/// A connection on its way to the worker that is to serve it.
#[derive(Debug)]
struct HandedOver {
    /// The position of the listener whose socket it came in on.
    listener: usize,
    /// Of the standard library's kind: known to no runtime.
    stream: std::net::TcpStream,
    held: Held,
}

/// A listener's sockets for a worker that runs, posted to it by [`Workers::add`].
#[derive(Debug)]
struct Added {
    /// The listener's slot ([`Proxy::listeners`]).
    slot: usize,
    name: String,
    tcp: std::net::TcpListener,
    /// Its UDP socket, where it serves HTTP/3, and the worker's share of its QUIC inboxes.
    udp: Option<(std::net::UdpSocket, Forwarding)>,
    /// Where the worker says whether it serves them.
    said: std::sync::mpsc::Sender<Result<(), String>>,
}

/// The workers that run, for the main loop: what they hold, and where to post each the
/// sockets of a listener added.
#[derive(Debug)]
pub(crate) struct Workers {
    pub(crate) loads: Arc<Loads>,
    /// By position of the worker.
    adding: Vec<UnboundedSender<Added>>,
}

impl Workers {
    /// Has every worker accept on its sockets of `sockets`, one entry a worker in their
    /// order, as the listener `name` whose slot is `slot`, and serve HTTP/3 on its UDP
    /// socket where it has one. Returns once every worker has said whether it does: what
    /// each could not do. The listener has its share of the workers' room from now on.
    pub(crate) fn add(&self, slot: usize, name: &str, sockets: Vec<Sockets>) -> Vec<String> {
        self.loads.listener_added();
        let (said, saying) = std::sync::mpsc::channel();
        let mut forwarding = Forwarding::group(sockets.len()).into_iter();
        let mut posted = 0;
        let mut failed = Vec::new();
        for (position, (worker, (tcp, udp))) in self.adding.iter().zip(sockets).enumerate() {
            let added = Added {
                slot,
                name: name.to_owned(),
                tcp,
                udp: udp.zip(forwarding.next()),
                said: said.clone(),
            };
            match worker.send(added) {
                Ok(()) => posted += 1,
                Err(_) => failed.push(format!("worker {position} takes no listeners")),
            }
        }
        // Every worker's answer, or none from one that went without giving it.
        drop(said);
        let answers: Vec<Result<(), String>> = saying.iter().collect();
        if answers.len() < posted {
            let unsaid = posted - answers.len();
            failed.push(format!("{unsaid} workers went without saying"));
        }
        failed.extend(answers.into_iter().filter_map(Result::err));
        failed
    }
}

/// What a worker works with. Its share of the data plane is an `Rc` and goes no further
/// than the thread it was made on, which is why this is put together there and not here.
#[derive(Debug, Clone)]
struct Worker {
    position: usize,
    plane: Rc<edgerush_proxy::Worker>,
    loads: Arc<Loads>,
    /// Where to hand a connection over to each worker, by position.
    workers: Arc<[Sender<HandedOver>]>,
    accept: Accept,
}

/// A worker's sockets for one listener: TCP, and UDP where the listener serves HTTP/3.
pub(crate) type Sockets = (std::net::TcpListener, Option<std::net::UdpSocket>);

/// Starts a worker on a thread of its own for every entry of `sockets` — the sockets of
/// every listener, in the order of the listeners — all serving the one `proxy`, each
/// accepting only while it holds fewer than `connections` allows, an HTTP/3 connection
/// counting as it says. Returns the workers once every one has set up what it serves. What
/// a worker cannot do without, and the worker itself, tell `tell` when they end
/// ([`crate::essential`]).
///
/// # Errors
///
/// A runtime or a thread that cannot be started, a socket that a runtime does not take, or
/// a listener whose HTTP/3 cannot be set up. Workers that were started before it stay.
pub(crate) fn start(
    proxy: &Arc<Proxy>,
    sockets: Vec<Vec<Sockets>>,
    accept: Accept,
    limits: H1Limits,
    connections: Connections,
    tell: &std::sync::mpsc::Sender<Told>,
) -> Result<Workers, NotStarted> {
    let started = sockets.len();
    // Each worker says, once it has set up what it serves, whether it could.
    let (ready, readied) = std::sync::mpsc::channel::<Result<(), NotStarted>>();
    // By slot, as the data plane numbers its listeners: the first are the sockets'.
    let names = proxy.listeners();
    // Every worker has a socket for every listener, the same ones in the same order.
    let listeners = sockets.first().map_or(0, Vec::len);
    let loads = Loads::new(
        sockets.len(),
        connections.each,
        connections.quic,
        names.len(),
    );
    loads.listening(listeners);
    let (workers, handed_over): (Vec<_>, Vec<_>) = sockets
        .iter()
        .map(|_| mpsc::channel::<HandedOver>(ON_THEIR_WAY))
        .unzip();
    let workers: Arc<[Sender<HandedOver>]> = workers.into();
    let (adding, added): (Vec<_>, Vec<_>) = sockets
        .iter()
        .map(|_| mpsc::unbounded_channel::<Added>())
        .unzip();
    // For every listener, the workers' shares of its QUIC inboxes: a datagram one worker
    // receives for another's connection is handed to it (16 §3).
    let mut forwarding: Vec<std::vec::IntoIter<Forwarding>> = (0..listeners)
        .map(|_| Forwarding::group(sockets.len()).into_iter())
        .collect();

    let each = sockets.into_iter().zip(handed_over).zip(added);
    for (position, ((sockets, handed_over), added)) in each.enumerate() {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        // Sockets are handed to the runtime that is entered. Here, and not on the worker's
        // thread, so that a socket the runtime will not take stops the harness starting.
        let entered = runtime.enter();
        let mut streams = Vec::with_capacity(sockets.len());
        let mut datagrams = Vec::new();
        for (listener, (tcp, udp)) in sockets.into_iter().enumerate() {
            streams.push(TcpListener::from_std(tcp)?);
            let share = forwarding.get_mut(listener).and_then(Iterator::next);
            if let (Some(udp), Some(share)) = (udp, share) {
                datagrams.push((listener, UdpSocket::from_std(udp)?, share));
            }
        }
        let sockets = streams;
        drop(entered);
        // The QUIC connection IDs a worker issues carry its number in two bytes (16 §3).
        let number = u16::try_from(position)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "more than 65,536 workers"))?;
        let (proxy, loads, senders) = (Arc::clone(proxy), Arc::clone(&loads), Arc::clone(&workers));
        let (tell, ready, names) = (tell.clone(), ready.clone(), Arc::clone(&names));
        // A runtime without threads of its own runs on the thread that waits on it, and a
        // LocalSet belongs to one thread, so it is made on that one.
        thread::Builder::new()
            .name(format!("worker-{position}"))
            .spawn(move || {
                // The worker itself: a panic outside its tasks unwinds the thread.
                let mut thread = Essential::new(format!("worker {position}"), &tell);
                let essential =
                    |what: String| Essential::new(format!("worker {position}'s {what}"), &tell);
                let name = |listener: usize| {
                    names
                        .get(listener)
                        .and_then(Option::as_deref)
                        .unwrap_or_default()
                };
                let local = LocalSet::new();
                let entered = runtime.enter();
                // The upstream connections of this worker and of no other, made where
                // they are used: nothing about them can leave this thread.
                let plane = edgerush_proxy::Worker::at(proxy, limits, number, Arc::clone(&loads));
                // One sweep for the worker, beside its listeners, for as long as it runs.
                local.spawn_local(watched(
                    Rc::clone(&plane).maintain(),
                    essential("sweep and timers".to_owned()),
                ));
                let worker = Worker {
                    position,
                    plane,
                    loads,
                    workers: senders,
                    accept,
                };
                for (listener, socket) in sockets.into_iter().enumerate() {
                    local.spawn_local(watched(
                        worker.clone().accept(listener, socket),
                        essential(format!("accepting on listener \"{}\"", name(listener))),
                    ));
                }
                for (listener, socket, share) in datagrams {
                    match Rc::clone(&worker.plane).serve_h3(listener, socket, share) {
                        Ok(serving) => {
                            local.spawn_local(watched(
                                serving,
                                essential(format!("HTTP/3 on listener \"{}\"", name(listener))),
                            ));
                        }
                        Err(error) => {
                            let listener = name(listener).to_owned();
                            let _told = ready.send(Err(NotStarted::Http3 { listener, error }));
                            thread.finished();
                            return;
                        }
                    }
                }
                local.spawn_local(watched(
                    worker.clone().receive(handed_over),
                    essential("connections handed over".to_owned()),
                ));
                // Not one it cannot do without: a listener it cannot take is said to be.
                local.spawn_local(worker.adding(added, tell.clone()));
                drop(entered);
                let _told = ready.send(Ok(()));
                // Accepting has no end but a drain, so neither has the LocalSet that waits on
                // it before then.
                runtime.block_on(local);
                thread.finished();
            })?;
    }
    drop(ready);
    // Every worker that is running has said so, or why it is not; one that panicked first
    // has said nothing, and its thread's guard tells the main loop.
    for worker in readied.iter().take(started) {
        worker?;
    }
    Ok(Workers { loads, adding })
}

/// Why the workers did not all start.
#[derive(Debug, thiserror::Error)]
pub(crate) enum NotStarted {
    #[error("cannot start the runtime: {0}")]
    Runtime(#[from] io::Error),
    #[error("listener \"{listener}\" cannot serve HTTP/3: {error}")]
    Http3 { listener: String, error: io::Error },
}

impl Worker {
    async fn accept(self, listener: usize, socket: TcpListener) {
        loop {
            // A worker at its cap leaves what comes in the backlog until one of its
            // connections ends, rather than take on what it has no room for; and a listener
            // holding its share, while the workers' room is short (03 §9). Counted once a
            // pause, so that a listener that is not accepting can be told from a slow one.
            if !self.loads.has_room(self.position) {
                self.plane
                    .proxy()
                    .accept_paused(listener, AcceptPause::WorkerCap);
            } else if !self.loads.listener_has_room(listener) {
                self.plane
                    .proxy()
                    .accept_paused(listener, AcceptPause::Share);
            }
            self.loads.room(self.position, listener).await;
            // Draining: nothing new is taken, and the socket goes with this.
            let Some(accepted) = self.plane.accept(&socket).await else {
                return;
            };
            match accepted {
                Ok(stream) => self.place(listener, stream),
                Err(error) => {
                    if let Some(pause) = self.plane.proxy().accept_failed(listener, &error) {
                        tokio::time::sleep(pause).await;
                    }
                }
            }
        }
    }

    /// Serves the connection here or hands it over. Whatever goes wrong with handing it
    /// over, the connection is not lost to it: it is served here then.
    fn place(&self, listener: usize, stream: TcpStream) {
        let held = match self.accept {
            Accept::Balanced => self.loads.place(self.position, listener),
            Accept::Kernel => self.loads.hold(self.position, listener),
        };
        let Some(other) = self
            .workers
            .get(held.worker())
            .filter(|_| held.worker() != self.position)
        else {
            return self.serve(listener, stream, held);
        };
        // Out of this runtime's hands before it goes into another's.
        let Ok(stream) = stream.into_std() else {
            // Not known to happen; the connection went with the stream.
            return;
        };
        let handed_over = HandedOver {
            listener,
            stream,
            held,
        };
        let Err(TrySendError::Full(back) | TrySendError::Closed(back)) =
            other.try_send(handed_over)
        else {
            return;
        };
        let HandedOver { stream, held, .. } = back;
        drop(held);
        if let Ok(stream) = TcpStream::from_std(stream) {
            self.serve(listener, stream, self.loads.hold(self.position, listener));
        }
    }

    /// Takes on the listeners posted to it, until the main loop has gone.
    async fn adding(
        self,
        mut added: UnboundedReceiver<Added>,
        tell: std::sync::mpsc::Sender<Told>,
    ) {
        while let Some(Added {
            slot,
            name,
            tcp,
            udp,
            said,
        }) = added.recv().await
        {
            let _gone = said.send(self.listen(slot, &name, tcp, udp, &tell));
        }
    }

    /// Accepts on `tcp` as the listener `name` whose slot is `slot`, and serves HTTP/3 on
    /// `udp` if there is one, beside what it serves already: the first even if the second
    /// cannot be, so that no connection the kernel gives this worker waits unaccepted.
    fn listen(
        &self,
        slot: usize,
        name: &str,
        tcp: std::net::TcpListener,
        udp: Option<(std::net::UdpSocket, Forwarding)>,
        tell: &std::sync::mpsc::Sender<Told>,
    ) -> Result<(), String> {
        let position = self.position;
        let essential = |what: String| Essential::new(format!("worker {position}'s {what}"), tell);
        let cannot = |what: &str, error: io::Error| {
            format!("worker {position} cannot {what} listener \"{name}\": {error}")
        };
        let socket = TcpListener::from_std(tcp).map_err(|error| cannot("accept on", error))?;
        let _detached = tokio::task::spawn_local(watched(
            self.clone().accept(slot, socket),
            essential(format!("accepting on listener \"{name}\"")),
        ));
        let Some((udp, share)) = udp else {
            return Ok(());
        };
        let serving = UdpSocket::from_std(udp)
            .and_then(|udp| Rc::clone(&self.plane).serve_h3(slot, udp, share))
            .map_err(|error| cannot("serve HTTP/3 on", error))?;
        let _detached = tokio::task::spawn_local(watched(
            serving,
            essential(format!("HTTP/3 on listener \"{name}\"")),
        ));
        Ok(())
    }

    async fn receive(self, mut handed_over: Receiver<HandedOver>) {
        while let Some(HandedOver {
            listener,
            stream,
            held,
        }) = handed_over.recv().await
        {
            if let Ok(stream) = TcpStream::from_std(stream) {
                self.serve(listener, stream, held);
            }
        }
    }

    /// Serves the connection on this worker's runtime, which the caller is on. It counts
    /// as the worker's to its end.
    fn serve(&self, listener: usize, stream: TcpStream, held: Held) {
        let plane = Rc::clone(&self.plane);
        let _detached = tokio::task::spawn_local(async move {
            plane.serve_connection(listener, stream).await;
            drop(held);
        });
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use crate::bind::{Port, datagrams, listen};
    use edgerush_config::{Compiled, Config, compile};
    use edgerush_proxy::connections::QUIC_MOST;
    use std::io::{Read, Write};
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    /// A cap of `each` connections a worker, an HTTP/3 connection counting as three.
    fn capped(each: usize) -> Connections {
        Connections {
            each,
            set_by: "the test",
            quic: QUIC_MOST,
        }
    }

    /// Workers whose one listener `web` has nowhere to send a request: every request is
    /// answered with 503 by the worker that serves its connection.
    fn workers(count: usize, accept: Accept) -> (SocketAddr, Arc<Loads>) {
        let (address, loads, _, _told) = capped_workers(count, accept, CONNECTIONS_PER_WORKER);
        (address, loads)
    }

    /// The same, each holding no more than `connections`, and the data plane they serve.
    fn capped_workers(
        count: usize,
        accept: Accept,
        connections: usize,
    ) -> (
        SocketAddr,
        Arc<Loads>,
        Arc<Proxy>,
        std::sync::mpsc::Receiver<Told>,
    ) {
        let yaml = r#"
listeners:
  web: { address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: / }
        forward: { backends: [{ upstream: nowhere, weight: 1 }] }
upstreams:
  nowhere: { load_balancer: p2c, endpoints: { listed: [] } }
"#;
        let config: Config = serde_saphyr::from_str(yaml).unwrap();
        let workers = NonZeroUsize::new(count).unwrap();
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), workers).unwrap());
        let mut address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut sockets = Vec::new();
        for _ in 0..count {
            let socket = listen(address, Port::Shared).unwrap();
            address = socket.local_addr().unwrap();
            sockets.push(vec![(socket, None)]);
        }
        let (tell, told) = std::sync::mpsc::channel();
        let loads = start(
            &proxy,
            sockets,
            accept,
            H1Limits::default(),
            capped(connections),
            &tell,
        )
        .unwrap()
        .loads;
        (address, loads, proxy, told)
    }

    fn eventually(loads: &Loads, what: impl Fn(&[usize]) -> bool) -> Vec<usize> {
        let began = Instant::now();
        loop {
            let now = loads.now();
            if what(&now) || began.elapsed() > Duration::from_secs(10) {
                return now;
            }
            thread::sleep(Duration::from_millis(5));
        }
    }

    /// Sends a request and reads the whole answer — of the worker's own, so without a
    /// body — leaving the connection ready for the next: the status line of the answer.
    fn request(connection: &mut std::net::TcpStream) -> String {
        connection
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        connection
            .write_all(b"GET / HTTP/1.1\r\nhost: balance.test\r\n\r\n")
            .unwrap();
        let mut answer = Vec::new();
        let mut byte = [0];
        while !answer.ends_with(b"\r\n\r\n") {
            connection.read_exact(&mut byte).unwrap();
            answer.push(byte[0]);
        }
        let answer = String::from_utf8_lossy(&answer);
        answer.lines().next().unwrap().to_owned()
    }

    /// A worker at its cap leaves the next connection in the backlog, unserved, until one of
    /// its own ends; then it is served.
    #[test]
    fn a_connection_past_the_cap_waits_until_another_ends() {
        let (address, loads, proxy, _told) = capped_workers(1, Accept::Balanced, 1);
        let mut first = std::net::TcpStream::connect(address).unwrap();
        assert_eq!(request(&mut first), "HTTP/1.1 503 Service Unavailable");

        // Connected by the kernel, and not accepted: its request goes unanswered.
        let mut second = std::net::TcpStream::connect(address).unwrap();
        second
            .write_all(b"GET / HTTP/1.1\r\nhost: balance.test\r\n\r\n")
            .unwrap();
        second
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut byte = [0];
        let unanswered = second.read(&mut byte);
        assert!(unanswered.is_err(), "answered past the cap: {unanswered:?}");
        assert_eq!(loads.now(), [1]);
        let scrape = proxy.metrics();
        let paused =
            "edgerush_listener_accept_paused_total{listener=\"web\",reason=\"worker_cap\"} 1";
        assert!(scrape.contains(paused), "{scrape}");

        drop(first);
        second
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut answer = Vec::new();
        while !answer.ends_with(b"\r\n\r\n") {
            second.read_exact(&mut byte).unwrap();
            answer.push(byte[0]);
        }
        assert!(
            answer.starts_with(b"HTTP/1.1 503 "),
            "{}",
            String::from_utf8_lossy(&answer)
        );
    }

    /// One worker holding no more than `connections`, with two listeners, `a` and `b`, each
    /// with nowhere to send a request: their addresses, in that order. The config's
    /// addresses only need to differ; the sockets are the test's own.
    fn two_listeners(connections: usize) -> (SocketAddr, SocketAddr, Arc<Loads>, Arc<Proxy>) {
        let yaml = r#"
listeners:
  a: { address: "127.0.0.1:1", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
  b: { address: "127.0.0.1:2", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }
routes:
  - name: everything
    listeners: [a, b]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: / }
        forward: { backends: [{ upstream: nowhere, weight: 1 }] }
upstreams:
  nowhere: { load_balancer: p2c, endpoints: { listed: [] } }
"#;
        let config: Config = serde_saphyr::from_str(yaml).unwrap();
        let compiled = compile(&config).unwrap();
        // The sockets go in the order the data plane has the listeners in.
        let names: Vec<String> = compiled
            .listeners()
            .iter()
            .map(|l| l.name.clone())
            .collect();
        let proxy = Arc::new(Proxy::new(compiled, NonZeroUsize::MIN).unwrap());
        let mut addresses = std::collections::HashMap::new();
        let mut sockets = Vec::new();
        for name in &names {
            let socket = listen("127.0.0.1:0".parse().unwrap(), Port::Own).unwrap();
            addresses.insert(name.clone(), socket.local_addr().unwrap());
            sockets.push((socket, None));
        }
        // What ends while a test runs is not what it looks at.
        let (tell, _told) = std::sync::mpsc::channel();
        let loads = start(
            &proxy,
            vec![sockets],
            Accept::Balanced,
            H1Limits::default(),
            capped(connections),
            &tell,
        )
        .unwrap()
        .loads;
        (addresses["a"], addresses["b"], loads, proxy)
    }

    /// Whether a request on `connection` is answered within half a second.
    fn answered_soon(connection: &mut std::net::TcpStream) -> bool {
        connection
            .write_all(b"GET / HTTP/1.1\r\nhost: balance.test\r\n\r\n")
            .unwrap();
        connection
            .set_read_timeout(Some(Duration::from_millis(500)))
            .unwrap();
        let mut byte = [0];
        connection.read(&mut byte).is_ok()
    }

    /// Once the worker is short of connections, the listener holding its share leaves the
    /// next connection in its backlog while the other listener is served; it is not let in
    /// until a connection of its own ends and the worker is no longer short (03 §9).
    #[test]
    fn a_listener_at_its_share_leaves_the_next_in_its_backlog_and_the_other_is_served() {
        // Short from 7 of 8; with two listeners, a share is 4.
        let (a, b, loads, proxy) = two_listeners(8);
        let mut held: Vec<std::net::TcpStream> = (0..7)
            .map(|_| {
                let mut connection = std::net::TcpStream::connect(a).unwrap();
                assert_eq!(request(&mut connection), "HTTP/1.1 503 Service Unavailable");
                connection
            })
            .collect();
        assert_eq!(loads.now(), [7]);

        let mut waiting = std::net::TcpStream::connect(a).unwrap();
        assert!(!answered_soon(&mut waiting), "answered past its share");
        let scrape = proxy.metrics();
        let paused = "edgerush_listener_accept_paused_total{listener=\"a\",reason=\"share\"} 1";
        assert!(scrape.contains(paused), "{scrape}");
        let mut other = std::net::TcpStream::connect(b).unwrap();
        assert_eq!(request(&mut other), "HTTP/1.1 503 Service Unavailable");

        // Still short, and `a` still holds more than its share.
        drop(other);
        eventually(&loads, |now| now == [7]);
        let mut byte = [0];
        assert!(waiting.read(&mut byte).is_err(), "let in over its share");

        held.pop();
        waiting
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let mut answer = Vec::new();
        while !answer.ends_with(b"\r\n\r\n") {
            waiting.read_exact(&mut byte).unwrap();
            answer.push(byte[0]);
        }
        assert!(
            answer.starts_with(b"HTTP/1.1 503 "),
            "{}",
            String::from_utf8_lossy(&answer)
        );
    }

    #[test]
    fn a_handful_of_connections_is_spread_over_the_workers_whoever_accepts_them() {
        let (address, loads) = workers(4, Accept::Balanced);
        let mut connections: Vec<_> = (0..8)
            .map(|_| std::net::TcpStream::connect(address).unwrap())
            .collect();
        let spread = eventually(&loads, |now| now.iter().sum::<usize>() == 8);
        // Two workers that look at the same moment may pick the same third.
        assert!(
            spread.iter().all(|held| (1..=3).contains(held)),
            "{spread:?}"
        );

        // Handed over or not, every connection is served, and more than once.
        for connection in &mut connections {
            assert_eq!(request(connection), "HTTP/1.1 503 Service Unavailable");
            assert_eq!(request(connection), "HTTP/1.1 503 Service Unavailable");
        }

        connections.truncate(3);
        let left = eventually(&loads, |now| now.iter().sum::<usize>() == 3);
        assert_eq!(left.iter().sum::<usize>(), 3, "{left:?}");
        drop(connections);
        assert_eq!(eventually(&loads, |now| now == [0; 4]), [0; 4]);
    }

    #[test]
    fn one_connection_after_another_each_finds_the_worker_with_the_least() {
        let (address, loads) = workers(4, Accept::Balanced);
        let mut connections = Vec::new();
        for so_far in 1..=8 {
            connections.push(std::net::TcpStream::connect(address).unwrap());
            let now = eventually(&loads, |now| now.iter().sum::<usize>() == so_far);
            let (least, most) = (now.iter().min().unwrap(), now.iter().max().unwrap());
            assert!(most - least <= 1, "after {so_far}: {now:?}");
        }
    }

    #[test]
    fn left_to_the_kernel_every_connection_stays_where_it_was_accepted() {
        let (address, loads) = workers(4, Accept::Kernel);
        let mut connections: Vec<_> = (0..8)
            .map(|_| std::net::TcpStream::connect(address).unwrap())
            .collect();
        let held = eventually(&loads, |now| now.iter().sum::<usize>() == 8);
        assert_eq!(held.iter().sum::<usize>(), 8, "{held:?}");
        for connection in &mut connections {
            assert_eq!(request(connection), "HTTP/1.1 503 Service Unavailable");
        }
    }

    /// `count` workers holding no more than `connections` each, serving `web` (as
    /// [`capped_workers`] has it) and a listener `api` with no routes, whose sockets are
    /// opened and posted to them once they run: the addresses of `web` and of `api`, TCP and
    /// UDP, and what the workers hold and the data plane.
    fn added_later(
        count: usize,
        port: Port,
        connections: usize,
    ) -> (SocketAddr, SocketAddr, Arc<Loads>, Arc<Proxy>) {
        let config = |listeners: &str| -> Compiled {
            let yaml = format!(
                r#"
listeners:
  web: {{ address: "127.0.0.1:1", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate }}
{listeners}
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - {{ name: "*", falls_through: true }}
    rules:
      - matches:
          - path: {{ prefix: / }}
        forward: {{ backends: [{{ upstream: nowhere, weight: 1 }}] }}
upstreams:
  nowhere: {{ load_balancer: p2c, endpoints: {{ listed: [] }} }}
"#
            );
            let config: Config = serde_saphyr::from_str(&yaml).unwrap();
            compile(&config).unwrap()
        };
        let workers = NonZeroUsize::new(count).unwrap();
        let proxy = Arc::new(Proxy::new(config(""), workers).unwrap());
        let mut web: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut sockets = Vec::new();
        for _ in 0..count {
            let socket = listen(web, port).unwrap();
            web = socket.local_addr().unwrap();
            sockets.push(vec![(socket, None)]);
        }
        let (tell, _told) = std::sync::mpsc::channel();
        let running = start(
            &proxy,
            sockets,
            Accept::Balanced,
            H1Limits::default(),
            capped(connections),
            &tell,
        )
        .unwrap();

        // What the main loop does at a reload that adds a listener.
        let mut address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut sockets = Vec::new();
        for _ in 0..count {
            let tcp = listen(address, port).unwrap();
            address = tcp.local_addr().unwrap();
            sockets.push((tcp, Some(datagrams(address, port).unwrap())));
        }
        let api = "  api: { address: \"127.0.0.1:2\", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }";
        let opened = edgerush_proxy::Opened {
            name: "api".to_owned(),
            quic: Some(address.port()),
        };
        assert_eq!(proxy.reload_with(config(api), &[opened]).unwrap(), [1]);
        assert_eq!(running.add(1, "api", sockets), [""; 0]);
        (web, address, running.loads, proxy)
    }

    /// A datagram of a QUIC version nobody speaks, as large as an Initial: what an HTTP/3
    /// listener answers with a version negotiation, whatever else it is set up with.
    fn unknown_version() -> Vec<u8> {
        let mut datagram = vec![0xc0, 0x1a, 0x2a, 0x3a, 0x4a, 8];
        datagram.extend([1; 8]);
        datagram.push(8);
        datagram.extend([2; 8]);
        datagram.resize(1200, 0);
        datagram
    }

    /// Whether the UDP socket at `address` answers a QUIC client with a version negotiation.
    fn negotiates(address: SocketAddr) -> bool {
        let client = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        client.send_to(&unknown_version(), address).unwrap();
        let mut answer = [0; 1500];
        let Ok((read, _)) = client.recv_from(&mut answer) else {
            return false;
        };
        // A long header whose version is 0.
        read > 5 && answer[0] & 0x80 != 0 && answer[1..5] == [0; 4]
    }

    /// A listener added while a worker runs is accepted on, as the listener its slot names,
    /// and its UDP socket is read.
    #[test]
    fn a_worker_that_runs_takes_on_a_listener_added_to_it() {
        let (_, address, loads, proxy) = added_later(1, Port::Own, CONNECTIONS_PER_WORKER);
        let mut connection = std::net::TcpStream::connect(address).unwrap();
        // No route of `api`'s: the data plane's own 404, counted as the listener's.
        assert_eq!(request(&mut connection), "HTTP/1.1 404 Not Found");
        assert_eq!(loads.now(), [1]);
        let scrape = proxy.metrics();
        let counted = "edgerush_listener_responses_total{listener=\"api\",class=\"4xx\"} 1\n";
        assert!(scrape.contains(counted), "{scrape}");
        assert!(negotiates(address), "no version negotiation");
    }

    /// A listener added has its share of a worker's room as the others do: once the worker
    /// is short, the listener holding its share leaves the next connection in its backlog,
    /// and the one added is served (03 §9).
    #[test]
    fn a_listener_added_has_its_share_of_a_workers_room() {
        // Short from 7 of 8; with two listeners, a share is 4.
        let (web, api, loads, _proxy) = added_later(1, Port::Own, 8);
        let _held: Vec<std::net::TcpStream> = (0..7)
            .map(|_| {
                let mut connection = std::net::TcpStream::connect(web).unwrap();
                assert_eq!(request(&mut connection), "HTTP/1.1 503 Service Unavailable");
                connection
            })
            .collect();
        assert_eq!(loads.now(), [7]);
        let mut waiting = std::net::TcpStream::connect(web).unwrap();
        assert!(!answered_soon(&mut waiting), "answered past its share");
        let mut added = std::net::TcpStream::connect(api).unwrap();
        assert_eq!(request(&mut added), "HTTP/1.1 404 Not Found");
    }

    /// Every worker takes on its sockets of a listener added: none of the connections the
    /// kernel deals out among them waits unaccepted.
    #[cfg(unix)]
    #[test]
    fn every_worker_takes_on_a_listener_added_to_them() {
        let (_, address, _loads, _proxy) = added_later(4, Port::Shared, CONNECTIONS_PER_WORKER);
        let mut connections: Vec<_> = (0..16)
            .map(|_| std::net::TcpStream::connect(address).unwrap())
            .collect();
        for connection in &mut connections {
            assert_eq!(request(connection), "HTTP/1.1 404 Not Found");
        }
    }

    /// What a worker cannot do without tells when it ends: as the data plane drains, the
    /// worker's accepting ends, and says so by the listener's name.
    #[test]
    fn a_workers_accepting_tells_when_it_ends() {
        let (_address, _loads, proxy, told) =
            capped_workers(1, Accept::Balanced, CONNECTIONS_PER_WORKER);
        assert!(told.try_recv().is_err(), "something ended while serving");
        proxy.drain();
        let ended = loop {
            match told.recv_timeout(Duration::from_secs(10)).unwrap() {
                Told::Ended(ended) => break ended,
                Told::Stop => {}
            }
        };
        assert_eq!(
            ended.to_string(),
            "worker 0's accepting on listener \"web\" ended"
        );
    }
}
