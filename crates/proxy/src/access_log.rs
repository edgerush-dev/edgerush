//! Where access-log records go once a worker has written them
//! ([21 §4](../../docs/21-access-logs.md)): a batch of each worker's for each place its
//! listeners log to, and one thread, the logger, that writes the batches.
//!
//! A worker writes a record's line into its batch with no lock, no atomic and no
//! allocation, and never waits for the logger. A batch that is full, or that the worker's
//! sweep finds with something in it, goes to the logger over a channel and comes back
//! empty once written. A worker has at most [`BATCHES`]; one with none free drops the
//! record and counts it, rather than wait (08 §2).
//!
//! The logger alone writes, a batch at a time, and may block on a write for as long as it
//! likes: no request waits on it. It starts with the first config that logs, so that a
//! data plane whose configs never log has no thread for it.
//!
//! A file stays open once a config has named it, until the process ends: a request begun
//! under a config that named it may make its record after a reload that does not, and its
//! batch reach the logger up to a sweep later. A config that names it again writes to it
//! as before.

use crate::metrics::LogsDropped;
use edgerush_config::{AccessLog, Compiled};
use edgerush_telemetry::{Counter, Sharded};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError, Weak};
use std::thread;
use std::time::{Duration, Instant};
use tokio::sync::Notify;

/// How large a batch grows before it goes to the logger: a write of this or a little more,
/// a few hundred records.
pub(crate) const BATCH: usize = 64 * 1024;

/// How many batches a worker has at most, those being written included: what a logger that
/// has stalled costs a worker before it drops records, [`BATCHES`] × [`BATCH`].
pub(crate) const BATCHES: usize = 4;

/// What the data plane counts dropped records in, by why: shared with the logger thread.
pub(crate) type Dropped = Arc<Sharded<[Counter; LogsDropped::ALL.len()]>>;

/// A place records are written to, as the data plane numbers it: a place keeps its number
/// for as long as the process runs, and a number is never given to another place.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct Sink(u32);

/// What the logger is told.
enum Message {
    /// Write to `target` as `sink` from now on.
    Open(Sink, Target),
    /// A worker's batch for `sink`, to be written and given back.
    Batch {
        sink: Sink,
        bytes: Vec<u8>,
        back: Sender<Vec<u8>>,
    },
    /// Open every file again by its path: it has been moved aside for rotation.
    Reopen,
    /// Says on the channel once everything sent before it has been written.
    Finished(Sender<()>),
}

/// Where a sink's records go, as the logger holds it.
enum Target {
    Stdout,
    File {
        path: PathBuf,
        file: File,
    },
    /// What a test writes to, to see what arrives or to hold the logger up.
    #[cfg(test)]
    Test(Box<dyn Write + Send>),
}

impl Target {
    fn open(log: &AccessLog) -> io::Result<Self> {
        Ok(match log {
            AccessLog::Stdout => Self::Stdout,
            AccessLog::File(path) => Self::File {
                file: appending(path)?,
                path: path.clone(),
            },
        })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            // Every batch ends with a line's end, so standard output's line buffer has
            // nothing left in it once this returns.
            Self::Stdout => io::stdout().lock().write_all(bytes),
            Self::File { file, .. } => file.write_all(bytes),
            #[cfg(test)]
            Self::Test(writer) => writer.write_all(bytes),
        }
    }

    /// Opens the file again by its path. One that cannot be opened is written on where it
    /// was, which is no worse than not having been asked.
    fn reopen(&mut self) {
        if let Self::File { path, file } = self
            && let Ok(reopened) = appending(path)
        {
            *file = reopened;
        }
    }
}

fn appending(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

/// The data plane's access logs: one for the process, whatever its workers.
pub(crate) struct Logs {
    /// Every sink a config has named, by what each is. Reloads alone come here, one at a
    /// time.
    named: Mutex<Named>,
    /// The logger, from the first config that logs.
    logger: OnceLock<SyncSender<Message>>,
    /// Every worker's word for the end, while the worker is there to be told.
    workers: Mutex<Vec<Weak<Finish>>>,
    /// Whether the running config logs anything at all: what a request of one that does
    /// not looks at, and no further.
    on: AtomicBool,
    /// Room in the channel for every batch every worker can have, so that handing one over
    /// never finds it full.
    room: usize,
    dropped: Dropped,
}

impl std::fmt::Debug for Logs {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Logs")
            .field("logging", &self.logger.get().is_some())
            .finish_non_exhaustive()
    }
}

#[derive(Default)]
struct Named {
    sinks: HashMap<AccessLog, Sink>,
    next: u32,
}

/// What a config's listeners log to, its new files opened, before it is known whether the
/// config will run: dropped, it closes them again.
pub(crate) struct Prepared {
    /// By position in the data plane's listeners.
    by_listener: Vec<Option<Sink>>,
    /// What no config has named before, and the sink each will be.
    opened: Vec<(AccessLog, Sink, Target)>,
    next: u32,
}

impl Prepared {
    /// Where the records of the listener at each position go, if anywhere.
    pub(crate) fn by_listener(&self) -> Vec<Option<Sink>> {
        self.by_listener.clone()
    }
}

/// Why a config's access log cannot be written.
#[derive(Debug)]
pub(crate) struct CannotOpen {
    pub(crate) listener: String,
    pub(crate) error: io::Error,
}

impl Logs {
    /// The logs of a data plane of `workers`, counting what they drop in `dropped`.
    pub(crate) fn new(workers: usize, dropped: Dropped) -> Self {
        Self {
            named: Mutex::default(),
            logger: OnceLock::new(),
            workers: Mutex::default(),
            on: AtomicBool::new(false),
            room: workers.max(1) * BATCHES + 8,
            dropped,
        }
    }

    /// Opens what `config` logs to that no config has named before, for the listeners of
    /// the data plane, `listeners`. A place already written to keeps its sink and is not
    /// opened again.
    pub(crate) fn prepare(
        &self,
        config: &Compiled,
        listeners: &[String],
    ) -> Result<Prepared, CannotOpen> {
        let named = lock(&self.named);
        let mut next = named.next;
        let mut sinks = HashMap::new();
        let mut opened = Vec::new();
        for listener in config.listeners() {
            let Some(log) = &listener.access_log else {
                continue;
            };
            if sinks.contains_key(log) {
                continue;
            }
            let sink = if let Some(sink) = named.sinks.get(log) {
                *sink
            } else {
                let target = Target::open(log).map_err(|error| CannotOpen {
                    listener: listener.name.clone(),
                    error,
                })?;
                let sink = Sink(next);
                next += 1;
                opened.push((log.clone(), sink, target));
                sink
            };
            sinks.insert(log.clone(), sink);
        }
        let by_listener = listeners
            .iter()
            .map(|name| {
                let listener = config.listeners().iter().find(|l| l.name == *name)?;
                sinks.get(listener.access_log.as_ref()?).copied()
            })
            .collect();
        Ok(Prepared {
            by_listener,
            opened,
            next,
        })
    }

    /// Makes what `prepared` opened written to, now that its config is to run, and before
    /// any request of it can make a record: the logger, started if this is the first config
    /// that logs, writes to each from now on.
    pub(crate) fn commit(&self, prepared: Prepared) {
        let logging = prepared.by_listener.iter().any(Option::is_some);
        self.on.store(logging, Ordering::Relaxed);
        let mut named = lock(&self.named);
        named.next = prepared.next;
        if prepared.opened.is_empty() {
            return;
        }
        let logger = self.logger();
        for (log, sink, target) in prepared.opened {
            named.sinks.insert(log, sink);
            let _sent = logger.send(Message::Open(sink, target));
        }
    }

    /// The logger, started now if it was not.
    fn logger(&self) -> &SyncSender<Message> {
        self.logger.get_or_init(|| {
            let (to, from) = mpsc::sync_channel(self.room);
            let dropped = Arc::clone(&self.dropped);
            // A thread that cannot be started leaves a channel nobody reads, and every
            // record is counted as dropped.
            let _started = thread::Builder::new()
                .name("access-log".to_owned())
                .spawn(move || write(&from, &dropped));
            to
        })
    }

    /// A worker's batches, which it is told to hand over at the end.
    pub(crate) fn worker(&self) -> Batches {
        let finish = Arc::new(Finish::default());
        let mut workers = lock(&self.workers);
        workers.retain(|worker| worker.strong_count() > 0);
        workers.push(Arc::downgrade(&finish));
        drop(workers);
        let (back, returned) = mpsc::channel();
        Batches {
            open: RefCell::new(Vec::new()),
            free: RefCell::new(Vec::new()),
            made: Cell::new(0),
            back,
            returned,
            finish,
        }
    }

    /// Whether the running config has any listener log. Set as a config is put in force,
    /// before any request of it.
    pub(crate) fn on(&self) -> bool {
        self.on.load(Ordering::Relaxed)
    }

    /// Tells the logger to open its files again, for whoever moved them aside.
    pub(crate) fn reopen(&self) {
        if let Some(logger) = self.logger.get() {
            let _sent = logger.send(Message::Reopen);
        }
    }

    /// Has every worker hand over what it holds and the logger write all of it, waiting
    /// `within` at most. Whether everything was written in time.
    pub(crate) fn finish(&self, within: Duration) -> bool {
        let until = Instant::now() + within;
        let workers: Vec<Arc<Finish>> = lock(&self.workers)
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        for worker in &workers {
            worker.asked.notify_one();
        }
        while workers
            .iter()
            .any(|worker| !worker.done.load(Ordering::Acquire))
        {
            if Instant::now() >= until {
                return false;
            }
            thread::sleep(Duration::from_millis(5));
        }
        let Some(logger) = self.logger.get() else {
            return true;
        };
        let (finished, written) = mpsc::channel();
        logger.send(Message::Finished(finished)).is_ok()
            && written
                .recv_timeout(until.saturating_duration_since(Instant::now()))
                .is_ok()
    }

    /// Hands `bytes` to the logger to write as `sink`'s; if it cannot take them, they are
    /// counted as dropped and the batch given back as it is.
    fn hand(&self, sink: Sink, mut bytes: Vec<u8>, back: &Sender<Vec<u8>>) -> Option<Vec<u8>> {
        if let Some(logger) = self.logger.get() {
            let batch = Message::Batch {
                sink,
                bytes,
                back: back.clone(),
            };
            match logger.try_send(batch) {
                Ok(()) => return None,
                Err(TrySendError::Full(message) | TrySendError::Disconnected(message)) => {
                    let Message::Batch { bytes: refused, .. } = message else {
                        return None;
                    };
                    bytes = refused;
                }
            }
        }
        count(&self.dropped, LogsDropped::Behind, records(&bytes));
        bytes.clear();
        Some(bytes)
    }
}

/// A worker's word for the end: asked to hand over what it holds, and done once it has.
#[derive(Debug, Default)]
struct Finish {
    asked: Notify,
    done: AtomicBool,
}

/// A worker's batches: one open for each sink its records go to, and those it has free.
/// Its own, on its own thread.
#[derive(Debug)]
pub(crate) struct Batches {
    open: RefCell<Vec<(Sink, Vec<u8>)>>,
    free: RefCell<Vec<Vec<u8>>>,
    /// How many it has made, at most [`BATCHES`].
    made: Cell<usize>,
    /// Where the logger gives them back, written and emptied.
    back: Sender<Vec<u8>>,
    returned: Receiver<Vec<u8>>,
    finish: Arc<Finish>,
}

impl Batches {
    /// Writes a record to `sink` with `write`, which appends its line: into the batch open
    /// for `sink`, handed over once full. With no batch free, the record is dropped and
    /// counted, and `write` is not called.
    pub(crate) fn record(&self, logs: &Logs, sink: Sink, write: impl FnOnce(&mut Vec<u8>)) {
        let Ok(mut open) = self.open.try_borrow_mut() else {
            count(&logs.dropped, LogsDropped::Behind, 1);
            return;
        };
        let at = match open.iter().position(|(open, _)| *open == sink) {
            Some(at) => at,
            None => {
                let Some(batch) = self.take() else {
                    count(&logs.dropped, LogsDropped::Behind, 1);
                    return;
                };
                open.push((sink, batch));
                open.len() - 1
            }
        };
        let Some((_, batch)) = open.get_mut(at) else {
            return;
        };
        write(batch);
        if batch.len() >= BATCH {
            let (sink, bytes) = open.swap_remove(at);
            drop(open);
            self.give(logs, sink, bytes);
        }
    }

    /// Hands every batch with something in it to the logger: the worker's sweep does, so
    /// that a quiet worker's records are not held for long.
    pub(crate) fn hand_over(&self, logs: &Logs) {
        let handed: Vec<(Sink, Vec<u8>)> = {
            let Ok(mut open) = self.open.try_borrow_mut() else {
                return;
            };
            let mut handed = Vec::new();
            let mut at = 0;
            while at < open.len() {
                if open.get(at).is_some_and(|(_, bytes)| !bytes.is_empty()) {
                    handed.push(open.swap_remove(at));
                } else {
                    at += 1;
                }
            }
            handed
        };
        for (sink, bytes) in handed {
            self.give(logs, sink, bytes);
        }
    }

    /// Waits until the worker is asked to hand over what it holds at the end.
    pub(crate) async fn asked_to_finish(&self) {
        self.finish.asked.notified().await;
    }

    /// Says that the worker has handed over what it held at the end.
    pub(crate) fn finished(&self) {
        self.finish.done.store(true, Ordering::Release);
    }

    /// Gives `bytes` to the logger; what it cannot take comes back to be used again.
    fn give(&self, logs: &Logs, sink: Sink, bytes: Vec<u8>) {
        if let Some(refused) = logs.hand(sink, bytes, &self.back)
            && let Ok(mut free) = self.free.try_borrow_mut()
        {
            free.push(refused);
        }
    }

    /// A batch to write into: a free one, one the logger has given back, or a new one while
    /// the worker has made fewer than [`BATCHES`].
    fn take(&self) -> Option<Vec<u8>> {
        let mut free = self.free.try_borrow_mut().ok()?;
        if let Some(batch) = free.pop() {
            return Some(batch);
        }
        free.extend(self.returned.try_iter());
        if let Some(batch) = free.pop() {
            return Some(batch);
        }
        let made = self.made.get();
        if made >= BATCHES {
            return None;
        }
        self.made.set(made + 1);
        // Room for a full batch and one more record of a usual size, so that the record
        // that fills it does not make it grow.
        Some(Vec::with_capacity(BATCH + 4096))
    }
}

/// The logger: writes what it is sent, in the order it was sent, until every sender is gone.
fn write(from: &Receiver<Message>, dropped: &Dropped) {
    let mut targets: HashMap<Sink, Target> = HashMap::new();
    for message in from {
        match message {
            Message::Open(sink, target) => {
                targets.insert(sink, target);
            }
            Message::Batch {
                sink,
                mut bytes,
                back,
            } => {
                let written = targets
                    .get_mut(&sink)
                    .is_some_and(|target| target.write_all(&bytes).is_ok());
                if !written {
                    count(dropped, LogsDropped::Unwritten, records(&bytes));
                }
                bytes.clear();
                // A worker that has gone takes no batches back.
                let _given = back.send(bytes);
            }
            Message::Reopen => {
                for target in targets.values_mut() {
                    target.reopen();
                }
            }
            Message::Finished(finished) => {
                let _flushed = io::stdout().lock().flush();
                let _said = finished.send(());
            }
        }
    }
}

/// How many records `bytes` holds: one a line.
fn records(bytes: &[u8]) -> u64 {
    let lines = bytes.iter().filter(|byte| **byte == b'\n').count();
    u64::try_from(lines).unwrap_or(u64::MAX)
}

fn count(dropped: &Dropped, why: LogsDropped, records: u64) {
    if let Some(counter) = dropped.local().get(why as usize) {
        counter.add(records);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
impl Logs {
    /// A sink that writes to `writer`, as a config's file would.
    fn writing_to(&self, writer: Box<dyn Write + Send>) -> Sink {
        let mut named = lock(&self.named);
        let sink = Sink(named.next);
        named.next += 1;
        let _sent = self
            .logger()
            .send(Message::Open(sink, Target::Test(writer)));
        sink
    }

    fn dropped(&self, why: LogsDropped) -> u64 {
        self.dropped.sum(|shard| shard[why as usize].get())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::serve::{Proxy, ProxyError};
    use edgerush_config::{Config, compile};
    use std::num::NonZeroUsize;
    use std::time::{SystemTime, UNIX_EPOCH};

    const WITHIN: Duration = Duration::from_secs(10);

    /// A directory of the test's own for the files it logs to, removed with it.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(test: &str) -> Self {
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let name = format!("edgerush-{test}-{}-{nanos}", std::process::id());
            let directory = std::env::temp_dir().join(name);
            std::fs::create_dir(&directory).unwrap();
            Self(directory)
        }

        fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            // Only what this test made; a file the logger still holds open may stay.
            let _removed = std::fs::remove_dir_all(&self.0);
        }
    }

    fn logs() -> Logs {
        Logs::new(1, Arc::new(Sharded::new(NonZeroUsize::MIN)))
    }

    /// An HTTP listener's line of a config, logging as `log` says (`""` for not at all).
    fn listener(name: &str, log: &str) -> String {
        // Never bound: a port of each name's own, as no two listeners may share one.
        let port = 8000 + name.bytes().map(u16::from).sum::<u16>();
        format!(
            "  {name}: {{ address: \"127.0.0.1:{port}\", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate{log} }}\n"
        )
    }

    /// `, access_log: { file: … }` for `path`, quoted so that no backslash is read as an
    /// escape.
    fn file(path: &Path) -> String {
        format!(", access_log: {{ file: '{}' }}", path.display())
    }

    fn compiled(listeners: &str) -> Compiled {
        let yaml = format!("listeners:\n{listeners}routes: []\nupstreams: {{}}\n");
        compile(&serde_saphyr::from_str::<Config>(&yaml).unwrap()).unwrap()
    }

    fn names(names: &[&str]) -> Vec<String> {
        names.iter().map(ToString::to_string).collect()
    }

    /// The sink of each of `listeners`, `config` in force.
    fn running(logs: &Logs, config: &Compiled, listeners: &[String]) -> Vec<Option<Sink>> {
        let prepared = logs.prepare(config, listeners).unwrap();
        let sinks = prepared.by_listener();
        logs.commit(prepared);
        sinks
    }

    /// What a worker does when asked at the end: hands over what it holds and says so.
    fn finish(logs: &Logs, batches: &Batches) {
        batches.hand_over(logs);
        batches.finished();
        assert!(logs.finish(WITHIN));
    }

    fn read(path: &Path) -> String {
        std::fs::read_to_string(path).unwrap()
    }

    #[test]
    fn records_reach_the_file_their_listener_names() {
        let scratch = Scratch::new("logged");
        let path = scratch.file("access.log");
        let logs = logs();
        let sinks = running(
            &logs,
            &compiled(&(listener("web", &file(&path)) + &listener("quiet", ""))),
            &names(&["web", "quiet"]),
        );
        let [Some(web), None] = sinks[..] else {
            panic!("{sinks:?}");
        };
        let batches = logs.worker();
        batches.record(&logs, web, |out| out.extend_from_slice(b"one\n"));
        batches.record(&logs, web, |out| out.extend_from_slice(b"two\n"));
        // Nothing is written before the batch is handed over.
        assert_eq!(read(&path), "");
        finish(&logs, &batches);
        assert_eq!(read(&path), "one\ntwo\n");
    }

    /// A batch that fills goes to the logger there and then, sweep or no sweep.
    #[test]
    fn a_full_batch_goes_at_once() {
        let scratch = Scratch::new("full");
        let path = scratch.file("access.log");
        let logs = logs();
        let sinks = running(
            &logs,
            &compiled(&listener("web", &file(&path))),
            &names(&["web"]),
        );
        let Some(web) = sinks[0] else { panic!() };
        let batches = logs.worker();
        let line = [b'x'; 99];
        let mut written = 0;
        while written <= BATCH {
            batches.record(&logs, web, |out| {
                out.extend_from_slice(&line);
                out.push(b'\n');
            });
            written += 100;
        }
        batches.record(&logs, web, |out| out.extend_from_slice(b"last\n"));
        batches.finished();
        assert!(logs.finish(WITHIN));
        let read = read(&path);
        assert_eq!(
            read.len(),
            written,
            "the full batch, and not the last record"
        );
        // And the last one with the next hand-over.
        finish(&logs, &batches);
        assert!(super::tests::read(&path).ends_with("last\n"));
    }

    /// A writer that holds the logger in its first write until it is let go.
    struct Held {
        go: Receiver<()>,
        held: bool,
        got: Arc<Mutex<Vec<u8>>>,
    }

    impl Write for Held {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if !self.held {
                let _go = self.go.recv();
                self.held = true;
            }
            lock(&self.got).extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// A logger that cannot keep up costs records, counted, and never a wait: with every
    /// batch it has with the logger, a worker drops what comes next.
    #[test]
    fn a_stalled_logger_costs_records_and_never_a_wait() {
        let logs = logs();
        let (go, held) = mpsc::channel();
        let got = Arc::new(Mutex::new(Vec::new()));
        let sink = logs.writing_to(Box::new(Held {
            go: held,
            held: false,
            got: Arc::clone(&got),
        }));
        let batches = logs.worker();
        let line = [b'x'; 99];
        let records = (BATCHES + 2) * BATCH / 100;
        for _ in 0..records {
            batches.record(&logs, sink, |out| {
                out.extend_from_slice(&line);
                out.push(b'\n');
            });
        }
        let dropped = logs.dropped(LogsDropped::Behind);
        assert!(dropped > 0);
        go.send(()).unwrap();
        finish(&logs, &batches);
        let written = lock(&got).iter().filter(|byte| **byte == b'\n').count();
        assert!(written >= BATCHES * BATCH / 100, "{written}");
        assert_eq!(written as u64 + dropped, records as u64);
        assert_eq!(logs.dropped(LogsDropped::Unwritten), 0);
    }

    struct Failing;

    impl Write for Failing {
        fn write(&mut self, _: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("no room"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    /// Records the logger could not write are counted, one a line.
    #[test]
    fn records_that_cannot_be_written_are_counted() {
        let logs = logs();
        let sink = logs.writing_to(Box::new(Failing));
        let batches = logs.worker();
        for _ in 0..3 {
            batches.record(&logs, sink, |out| out.extend_from_slice(b"lost\n"));
        }
        finish(&logs, &batches);
        assert_eq!(logs.dropped(LogsDropped::Unwritten), 3);
        assert_eq!(logs.dropped(LogsDropped::Behind), 0);
    }

    /// What rotation does: the file is moved aside, the logger told, and what comes after
    /// goes to a new file at the path, what came before staying where it was moved.
    #[test]
    fn a_reopened_file_takes_the_records_after_it() {
        let scratch = Scratch::new("reopened");
        let path = scratch.file("access.log");
        let aside = scratch.file("access.log.1");
        let logs = logs();
        let sinks = running(
            &logs,
            &compiled(&listener("web", &file(&path))),
            &names(&["web"]),
        );
        let Some(web) = sinks[0] else { panic!() };
        let batches = logs.worker();
        batches.record(&logs, web, |out| out.extend_from_slice(b"before\n"));
        finish(&logs, &batches);
        std::fs::rename(&path, &aside).unwrap();
        logs.reopen();
        batches.record(&logs, web, |out| out.extend_from_slice(b"after\n"));
        finish(&logs, &batches);
        assert_eq!(read(&aside), "before\n");
        assert_eq!(read(&path), "after\n");
    }

    /// A place already logged to keeps its sink through a reload and is not opened again;
    /// one that is new is opened; one a reload stops naming is still written to, for the
    /// requests begun before it.
    #[test]
    fn a_reload_opens_only_what_is_new_and_closes_nothing() {
        let scratch = Scratch::new("reloaded");
        let first = scratch.file("first.log");
        let second = scratch.file("second.log");
        let logs = logs();
        let listeners = names(&["web", "api"]);
        let before = running(
            &logs,
            &compiled(&(listener("web", &file(&first)) + &listener("api", ""))),
            &listeners,
        );
        let after = running(
            &logs,
            &compiled(&(listener("web", &file(&first)) + &listener("api", &file(&second)))),
            &listeners,
        );
        assert_eq!(after[0], before[0]);
        assert!(after[1].is_some() && after[1] != after[0]);
        let gone = running(
            &logs,
            &compiled(&(listener("web", "") + &listener("api", &file(&second)))),
            &listeners,
        );
        assert_eq!(gone, [None, after[1]]);
        let batches = logs.worker();
        let (Some(old), Some(new)) = (before[0], after[1]) else {
            panic!()
        };
        batches.record(&logs, old, |out| out.extend_from_slice(b"begun before\n"));
        batches.record(&logs, new, |out| out.extend_from_slice(b"api\n"));
        finish(&logs, &batches);
        assert_eq!(read(&first), "begun before\n");
        assert_eq!(read(&second), "api\n");
        // Named again, the first file is written to as before.
        let again = running(
            &logs,
            &compiled(&listener("web", &file(&first))),
            &names(&["web"]),
        );
        assert_eq!(again[0], before[0]);
    }

    /// A config whose access log cannot be opened does not run: not at the start, and not
    /// at a reload, after which the one before it runs on.
    #[test]
    fn a_log_that_cannot_be_opened_keeps_its_config_from_running() {
        let scratch = Scratch::new("refused");
        let nowhere = scratch.file("no such directory").join("access.log");
        let refusal = |error: ProxyError| match error {
            ProxyError::AccessLog { listener, .. } => listener,
            other => panic!("{other:?}"),
        };
        let unopenable = compiled(&listener("web", &file(&nowhere)));
        let error = Proxy::new(unopenable, NonZeroUsize::MIN).unwrap_err();
        assert_eq!(refusal(error), "web");

        let path = scratch.file("access.log");
        let proxy =
            Proxy::new(compiled(&listener("web", &file(&path))), NonZeroUsize::MIN).unwrap();
        let unopenable = compiled(&listener("web", &file(&nowhere)));
        assert_eq!(refusal(proxy.reload(unopenable).unwrap_err()), "web");
        assert!(proxy.metrics().contains("edgerush_config_reloads_total 0"));
    }

    /// A data plane whose configs log nothing has no thread for it.
    #[test]
    fn nothing_starts_until_a_config_logs() {
        let logs = logs();
        let listeners = names(&["web"]);
        running(&logs, &compiled(&listener("web", "")), &listeners);
        assert!(logs.logger.get().is_none());
        let batches = logs.worker();
        batches.finished();
        assert!(logs.finish(WITHIN), "an end with nothing to write");
        running(
            &logs,
            &compiled(&listener("web", ", access_log: stdout")),
            &listeners,
        );
        assert!(logs.logger.get().is_some());
    }

    #[test]
    fn dropped_records_are_counted_by_why_on_a_scrape() {
        let scratch = Scratch::new("scraped");
        let path = scratch.file("access.log");
        let proxy =
            Proxy::new(compiled(&listener("web", &file(&path))), NonZeroUsize::MIN).unwrap();
        let scrape = proxy.metrics();
        assert!(
            scrape.contains("edgerush_access_log_dropped_total{reason=\"behind\"} 0\n"),
            "{scrape}"
        );
        assert!(scrape.contains("edgerush_access_log_dropped_total{reason=\"unwritten\"} 0\n"));
    }
}
