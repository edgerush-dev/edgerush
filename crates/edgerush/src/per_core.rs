//! Thread-per-core: for every worker a thread with a single-threaded runtime, a socket of
//! its own on every listener's port, and upstream connections of its own. One data plane
//! is shared by all of them — the config they serve and the counters they add to are the
//! process's, not a worker's.
//!
//! A worker accepts what the kernel gives its sockets and then decides whose connection it
//! is ([`crate::balance`]). One that is another worker's is handed over as a socket that no
//! runtime knows yet, so that it is the other worker's runtime that watches it from its
//! first byte on: once for a connection, and nothing crosses threads for a request.
//!
//! Everything a worker runs lives in a `LocalSet` of its own, so that a connection and all
//! the engine spawns for it stay on the one thread and need not be `Send`.

use crate::balance::{Held, Loads};
use edgerush_proxy::Proxy;
use std::io;
use std::rc::Rc;
use std::sync::Arc;
use std::thread;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::Builder;
use tokio::sync::mpsc::{self, Receiver, Sender, error::TrySendError};
use tokio::task::LocalSet;

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

/// Starts a worker on a thread of its own for every entry of `sockets` — one socket for
/// every listener, in the order of the listeners — all serving the one `proxy`. Returns
/// what the workers hold, for whoever wants to look.
///
/// # Errors
///
/// A runtime or a thread that cannot be started, or a socket that a runtime does not take.
/// Workers that were started before it stay.
pub(crate) fn start(
    proxy: &Arc<Proxy>,
    sockets: Vec<Vec<std::net::TcpListener>>,
    accept: Accept,
) -> io::Result<Arc<Loads>> {
    let loads = Loads::new(sockets.len());
    let (workers, handed_over): (Vec<_>, Vec<_>) = sockets
        .iter()
        .map(|_| mpsc::channel::<HandedOver>(ON_THEIR_WAY))
        .unzip();
    let workers: Arc<[Sender<HandedOver>]> = workers.into();

    for (position, (sockets, handed_over)) in sockets.into_iter().zip(handed_over).enumerate() {
        let runtime = Builder::new_current_thread().enable_all().build()?;
        // Sockets are handed to the runtime that is entered. Here, and not on the worker's
        // thread, so that a socket the runtime will not take stops the harness starting.
        let entered = runtime.enter();
        let sockets = sockets
            .into_iter()
            .map(TcpListener::from_std)
            .collect::<io::Result<Vec<_>>>()?;
        drop(entered);
        let (proxy, loads, senders) = (Arc::clone(proxy), Arc::clone(&loads), Arc::clone(&workers));
        // A runtime without threads of its own runs on the thread that waits on it, and a
        // LocalSet belongs to one thread, so it is made on that one.
        thread::Builder::new()
            .name(format!("worker-{position}"))
            .spawn(move || {
                let local = LocalSet::new();
                let entered = runtime.enter();
                // The upstream connections of this worker and of no other, made where
                // they are used: nothing about them can leave this thread.
                let plane = edgerush_proxy::Worker::new(proxy);
                // One sweep for the worker, beside its listeners, for as long as it runs.
                local.spawn_local(Rc::clone(&plane).maintain());
                let worker = Worker {
                    position,
                    plane,
                    loads,
                    workers: senders,
                    accept,
                };
                for (listener, socket) in sockets.into_iter().enumerate() {
                    local.spawn_local(worker.clone().accept(listener, socket));
                }
                local.spawn_local(worker.receive(handed_over));
                drop(entered);
                // Accepting has no end, so neither has the LocalSet that waits on it.
                runtime.block_on(local);
            })?;
    }
    Ok(loads)
}

impl Worker {
    async fn accept(self, listener: usize, socket: TcpListener) {
        loop {
            match socket.accept().await {
                Ok((stream, _)) => self.place(listener, stream),
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
            Accept::Balanced => self.loads.place(self.position),
            Accept::Kernel => self.loads.hold(self.position),
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
            self.serve(listener, stream, self.loads.hold(self.position));
        }
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
    use crate::bind::{Port, listen};
    use edgerush_config::{Config, compile};
    use std::io::{Read, Write};
    use std::net::SocketAddr;
    use std::num::NonZeroUsize;
    use std::time::{Duration, Instant};

    /// Workers whose one listener `web` has nowhere to send a request: every request is
    /// answered with 503 by the worker that serves its connection.
    fn workers(count: usize, accept: Accept) -> (SocketAddr, Arc<Loads>) {
        let yaml = r#"
listeners:
  web: { address: "127.0.0.1:0", protocol: http }
routes:
  - name: everything
    listeners: [web]
    hostnames:
      - { name: "*", falls_through: true }
    rules:
      - matches:
          - path: { prefix: / }
        backends: [{ upstream: nowhere, weight: 1 }]
upstreams:
  nowhere: { endpoints: [] }
"#;
        let config: Config = serde_saphyr::from_str(yaml).unwrap();
        let workers = NonZeroUsize::new(count).unwrap();
        let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), workers).unwrap());
        let mut address: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let mut sockets = Vec::new();
        for _ in 0..count {
            let socket = listen(address, Port::Shared).unwrap();
            address = socket.local_addr().unwrap();
            sockets.push(vec![socket]);
        }
        (address, start(&proxy, sockets, accept).unwrap())
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
}
