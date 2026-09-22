//! The floor an own HTTP/1 server could reach for an idle connection with tokio: accept,
//! wait for readability holding nothing, borrow a block from the worker only while a head
//! is arriving, answer, give the block back, and wait again. Step 0 of
//! [14 §9](../../../docs/14-downstream-server.md): it measures what can be released, and
//! is not a server — there is no parser beyond finding the end of a head, and every
//! request gets the same two-byte answer. Not part of EdgeRush.
//!
//! ```text
//! cargo run --release -p edgerush-proxy --example idle_floor -- 127.0.0.1:8080 2
//! ```

use std::cell::RefCell;
use std::io;
use std::net::SocketAddr;
use std::rc::Rc;
use tokio::net::TcpStream;

/// What a head is read into, lent from the worker while one is arriving.
const BLOCK: usize = 16 * 1024;

/// The same answer to everything.
const ANSWER: &[u8] = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\n\r\nok";

/// Free blocks, kept by one worker.
type Blocks = Rc<RefCell<Vec<Vec<u8>>>>;

fn main() -> io::Result<()> {
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
    eprintln!("idle floor on {address} ({workers} workers)");
    for thread in threads {
        match thread.join() {
            Ok(served) => served?,
            Err(_) => return Err(io::Error::other("a worker panicked")),
        }
    }
    Ok(())
}

fn serve(address: SocketAddr) -> io::Result<()> {
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
        let blocks: Blocks = Rc::default();
        let mut told = false;
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let _nodelay = stream.set_nodelay(true);
            let connection = connection(stream, Rc::clone(&blocks));
            if !told {
                eprintln!(
                    "a connection's future: {} bytes",
                    std::mem::size_of_val(&connection)
                );
                told = true;
            }
            let _detached = tokio::task::spawn_local(connection);
        }
    })
}

/// One connection: idle holding nothing, active holding one block.
async fn connection(stream: TcpStream, blocks: Blocks) {
    loop {
        if stream.readable().await.is_err() {
            return;
        }
        let mut block = blocks.borrow_mut().pop().unwrap_or_else(|| vec![0; BLOCK]);
        let answered = head_and_answer(&stream, &mut block).await;
        blocks.borrow_mut().push(block);
        match answered {
            Ok(Turn::Answered | Turn::Nothing) => {}
            Ok(Turn::Gone) | Err(_) => return,
        }
    }
}

/// How a turn with a block went.
enum Turn {
    /// A head was read and answered.
    Answered,
    /// Readiness was stale and nothing had arrived: the block goes back before waiting,
    /// or an idle connection would wait holding it.
    Nothing,
    /// The peer has gone, or sent more than a head may be.
    Gone,
}

/// Reads a head into `block` and answers it.
async fn head_and_answer(stream: &TcpStream, block: &mut [u8]) -> io::Result<Turn> {
    let mut filled = 0;
    loop {
        match stream.try_read(&mut block[filled..]) {
            Ok(0) => return Ok(Turn::Gone),
            Ok(read) => {
                filled += read;
                if block[..filled].windows(4).any(|end| end == b"\r\n\r\n") {
                    break;
                }
                if filled == block.len() {
                    return Ok(Turn::Gone);
                }
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                if filled == 0 {
                    return Ok(Turn::Nothing);
                }
                stream.readable().await?;
            }
            Err(error) => return Err(error),
        }
    }
    let mut written = 0;
    while written < ANSWER.len() {
        match stream.try_write(&ANSWER[written..]) {
            Ok(wrote) => written += wrote,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                stream.writable().await?;
            }
            Err(error) => return Err(error),
        }
    }
    Ok(Turn::Answered)
}
