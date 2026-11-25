//! EdgeRush rate limiting.
//!
//! A pure crate: it decides whether a request is admitted and does nothing else — no
//! sockets, clocks, randomness or async runtime. The time of every decision is passed in,
//! in nanoseconds from any fixed origin, which keeps it deterministic and easy to test.
//!
//! It holds [`reference`](mod@reference): the admission models of docs/06 §3, each written
//! as plainly as it can be, so that what each one promises can be read, tested and compared
//! before any of them is built for the request path; and [`service`](mod@service): the
//! asynchronous mode's protocol, the data plane's participant and the owner's applying of a
//! report, which the proxy and the cluster store's pods run; and [`local`](mod@local): local
//! mode's limiter, each process on what it sees alone. The synchronous mode's owner is the cluster store's
//! (`edgerush_cluster_synchronous`).

/// How far, in nanoseconds, a worker's clock may lag the latest charge of a key without a
/// request within the limit being refused on a data plane: a bucket admits a request whose
/// arrival time is up to its burst and this much refill ahead (06 §4). Workers read their own
/// clocks, so one may decide a moment after another that read a later time; a key at 100 a
/// second refills a request every 10 ms, so microseconds of lag refuse nothing below about
/// 100,000 a second, and this covers the rest. The burst grows by it times the rate: 0.01 of a
/// request at 100 a second, 100 at a million. The rate is unchanged.
pub const SLACK: u64 = 100_000;

/// How long, in nanoseconds, a worker may go on holding a handle to a key its process has let
/// go of: workers prune their handles a part at a time, one of [`local::HANDLE_PARTS`] every
/// `HANDLES_PRUNED / HANDLE_PARTS`, local mode's and the service modes' alike. Past its cap,
/// a process evicts leaving room for the keys made over this long, which is the most its
/// workers can hold of keys it has let go of beside those it keeps (06 §8, #602).
pub const HANDLES_PRUNED: u64 = 1_000_000_000;

pub mod local;

/// Whether the machine has the native 128-bit compare-and-swap a key's word needs, local
/// mode's and the asynchronous mode's alike (#350), found at run time where the build does
/// not assume it: the proxy does not start their limits without it rather than put the
/// fallback's lock on the request path. Under loom, whose atomics are its own, always.
#[must_use]
pub fn supported() -> bool {
    #[cfg(not(loom))]
    {
        portable_atomic::AtomicU128::is_lock_free()
    }
    #[cfg(loom)]
    {
        true
    }
}
mod chunked;
mod cutoff;
mod rate;
pub mod reference;
pub mod service;
mod word;
