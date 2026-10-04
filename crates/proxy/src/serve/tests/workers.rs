//! Workers and data planes made for the tests, and how a test waits on them.

use super::*;

/// Where the futures hyper's HTTP/2 client spawns go: this worker's `LocalSet`.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct OnThisWorker;

impl<F: Future<Output = ()> + 'static> Executor<F> for OnThisWorker {
    fn execute(&self, future: F) {
        let _detached = tokio::task::spawn_local(future);
    }
}

/// Deadlines short enough that the tests of them run on real sockets and real time: a
/// stopped clock jumps ahead whenever every task waits on a socket, which a loopback
/// round trip does, and that would fire a deadline nobody reached.
pub(super) const SHORT: Deadlines = Deadlines {
    first_request: Duration::from_millis(300),
    next_request: Duration::from_millis(700),
    idle: Duration::from_millis(500),
    drain: Duration::from_millis(900),
};

/// How late a deadline may be seen to fire on a loaded machine.
pub(super) const SLACK: Duration = Duration::from_millis(400);

/// How early a deadline may be seen to fire. A server's clock starts at what the client
/// sees only afterwards — the accept, before the client's first write; the end of the
/// answer, before the client has read it and written again — so a deadline kept to the
/// microsecond looks that much early from the client's side. Far less than any wrong
/// deadline would be.
pub(super) const EARLY: Duration = Duration::from_millis(50);

/// Serves `socket` with `worker`, whose timers are waited on beside it as its
/// maintenance would wait on them.
pub(super) fn serving(worker: &Rc<Worker>, socket: TcpListener) -> tokio::task::JoinHandle<()> {
    let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
    tokio::task::spawn_local(Rc::clone(worker).serve(0, socket))
}

/// Serves a worker for `upstream` on a listener of its own, and says where.
pub(super) async fn serving_worker(upstream: SocketAddr) -> SocketAddr {
    let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    front
}

/// A worker serving `config`'s `web` listener over HTTP/3 on a UDP socket of its own,
/// and the proxy, for a test to give a new config.
pub(super) async fn serving_h3(config: &edgerush_config::Config) -> (SocketAddr, Arc<Proxy>) {
    let proxy = Arc::new(Proxy::new(compile(config).unwrap(), NonZeroUsize::MIN).unwrap());
    let worker = Worker::with_deadlines(Arc::clone(&proxy), H1Limits::default(), SHORT);
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _timing = tokio::task::spawn_local(Rc::clone(&worker.timers).run());
    let alone = Forwarding::group(1).remove(0);
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve_h3(0, socket, alone).unwrap());
    (front, proxy)
}

/// The same as [`serving_worker`], with the worker, for a test that looks inside it.
pub(super) async fn serving_worker_and(upstream: SocketAddr) -> (SocketAddr, Rc<Worker>) {
    let proxy = Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// Bounded, so that what nobody finishes fails the test rather than hangs it.
pub(super) async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(10), future)
        .await
        .expect("did not finish")
}

/// A worker serving `config`, its sweep run every 50 ms, far sooner than an idle
/// connection is let go of: what the sweep starts is told apart from the keep-alive.
pub(super) async fn serving_swept(config: Compiled) -> (SocketAddr, Rc<Worker>) {
    let limits = H1Limits {
        sweep: Duration::from_millis(50),
        ..H1Limits::default()
    };
    let proxy = Proxy::new(config, NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = tokio::task::spawn_local(Rc::clone(&worker).serve(0, socket));
    let _sweeping = tokio::task::spawn_local(Rc::clone(&worker).maintain());
    (front, worker)
}

/// A worker serving `yaml`'s one listener, a passthrough one, with the short deadlines.
pub(super) async fn passing(yaml: &str) -> (SocketAddr, Rc<Worker>) {
    let config: Config = serde_saphyr::from_str(yaml).unwrap();
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// Whether the listener `listener` has counted one tunnel as ended by `outcome`, soon.
pub(super) async fn tunnel_ended(worker: &Worker, listener: &str, outcome: &str) {
    let line = format!(
        "edgerush_listener_tunnels_total{{listener=\"{listener}\",outcome=\"{outcome}\"}} 1\n"
    );
    until(|| worker.proxy().metrics().contains(&line)).await;
}

/// `reading`, which fails the test rather than hang it if the tunnel never passes an
/// end on.
pub(super) async fn bounded<T>(reading: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), reading)
        .await
        .expect("the tunnel never ended")
}

/// A worker whose one upstream `up`, at `upstream`, is spoken to in HTTP/2.
pub(super) async fn serving_worker_to_h2(
    upstream: SocketAddr,
    limits: H1Limits,
) -> (SocketAddr, Rc<Worker>) {
    let mut config = everything_config(upstream);
    config.upstreams.get_mut("up").unwrap().protocol = UpstreamProtocol::Http2;
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// A worker serving `config`, on a socket of its own.
pub(super) async fn serving_config(config: &Config) -> (SocketAddr, Rc<Worker>) {
    let proxy = Proxy::new(compile(config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), H1Limits::default(), SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// A worker for `up` at `upstream` in `protocol`, whose one rule retries as `retry`
/// says if it says anything, with `limits` as its bounds.
pub(super) async fn serving_worker_with(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    retry: Option<edgerush_config::Retry>,
    limits: H1Limits,
) -> (SocketAddr, Rc<Worker>) {
    serving_worker_forwarding(upstream, protocol, limits, |forward| forward.retry = retry).await
}

/// A worker for `up` at `upstream` in `protocol`, whose one rule's forwarding is as
/// `change` leaves it, with `limits` as its bounds.
pub(super) async fn serving_worker_forwarding(
    upstream: SocketAddr,
    protocol: UpstreamProtocol,
    limits: H1Limits,
    change: impl FnOnce(&mut edgerush_config::Forward),
) -> (SocketAddr, Rc<Worker>) {
    let mut config = everything_config(upstream);
    config.upstreams.get_mut("up").unwrap().protocol = protocol;
    change(
        config.routes[0].rules[0]
            .forward
            .as_mut()
            .expect("the rule forwards"),
    );
    let proxy = Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap();
    let worker = Worker::with_deadlines(Arc::new(proxy), limits, SHORT);
    let socket = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let front = socket.local_addr().unwrap();
    let _serving = serving(&worker, socket);
    (front, worker)
}

/// A proxy of one listener that sends everything to `upstream`, by EdgeRush's own
/// path because that is the path with a bound on it.
pub(super) fn sending_to(upstream: SocketAddr) -> Arc<Proxy> {
    Arc::new(Proxy::new(everything_to(upstream), NonZeroUsize::MIN).unwrap())
}

/// Waits for something the test is about to depend on, and fails rather than hangs
/// if it never happens.
pub(super) async fn until(mut settled: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !settled() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("waited for something that never happened");
}
