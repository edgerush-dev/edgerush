//! The engine alone: hyper's server answering every request itself, set up the way the
//! proxy's frontend is — a current-thread runtime and a `SO_REUSEPORT` listener for each
//! worker, HTTP/1.1 and HTTP/2 on one port, and everything a connection spawns kept on
//! its worker. For the macro benchmark, to tell what hyper costs from what the request
//! core and an upstream add to it (`bench/run.sh frontend`). Not part of EdgeRush.
//!
//! ```text
//! cargo run --release -p edgerush-proxy --example bare_hyper -- 127.0.0.1:8080 2
//! ```

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::rt::Executor;
use hyper::service::service_fn;
use hyper::{Request, Response};
use hyper_util::rt::TokioIo;
use hyper_util::server::conn::auto;
use std::convert::Infallible;
use std::future::Future;
use std::net::SocketAddr;

/// Spawns what the engine spawns onto the worker that serves the connection.
#[derive(Clone, Copy)]
struct OnThisWorker;

impl<F: Future<Output = ()> + 'static> Executor<F> for OnThisWorker {
    fn execute(&self, future: F) {
        let _detached = tokio::task::spawn_local(future);
    }
}

fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    let address: SocketAddr = args
        .next()
        .and_then(|address| address.parse().ok())
        .unwrap_or_else(|| SocketAddr::from(([127, 0, 0, 1], 8080)));
    let workers: usize = args
        .next()
        .and_then(|workers| workers.parse().ok())
        .unwrap_or(1);
    let threads: Vec<_> = (0..workers)
        .map(|_| std::thread::spawn(move || serve(address)))
        .collect();
    eprintln!("bare hyper on {address} ({workers} workers)");
    for thread in threads {
        match thread.join() {
            Ok(served) => served?,
            Err(_) => return Err(std::io::Error::other("a worker panicked")),
        }
    }
    Ok(())
}

fn serve(address: SocketAddr) -> std::io::Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let socket = tokio::net::TcpSocket::new_v4()?;
        #[cfg(unix)]
        socket.set_reuseport(true)?;
        socket.bind(address)?;
        let listener = socket.listen(4096)?;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let _nodelay = stream.set_nodelay(true);
            let _detached = tokio::task::spawn_local(async move {
                let service = service_fn(|_: Request<hyper::body::Incoming>| async {
                    Ok::<_, Infallible>(Response::new(Full::new(Bytes::from_static(b"ok"))))
                });
                let _closed = auto::Builder::new(OnThisWorker)
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    })
}
