use super::*;
use std::cell::Cell;
use tokio::time::Instant;

/// Tries that fail `failures` times, then take the connection, counted in `tries`.
fn failing(
    failures: u32,
    tries: &Cell<u32>,
) -> impl FnMut() -> std::future::Ready<io::Result<u32>> + '_ {
    move || {
        tries.set(tries.get() + 1);
        std::future::ready(if tries.get() > failures {
            Ok(tries.get())
        } else {
            Err(io::Error::other("too many open files"))
        })
    }
}

/// Out of open files, every try fails at once: each is followed by a pause, not the next
/// try straight away, so a second of them is ten tries, not a core spun.
#[tokio::test(start_paused = true)]
async fn each_failure_is_followed_by_a_pause() {
    let tries = Cell::new(0);
    let start = Instant::now();
    let taken = next(failing(49, &tries)).await;
    assert_eq!(taken, 50);
    assert_eq!(start.elapsed(), PAUSE * 49);
}

#[tokio::test(start_paused = true)]
async fn a_connection_taken_is_handed_on_at_once() {
    let tries = Cell::new(0);
    let start = Instant::now();
    assert_eq!(next(failing(0, &tries)).await, 1);
    assert_eq!(start.elapsed(), Duration::ZERO);
}

/// Dropped in its pause, as a listener's loop drops it to stop or to see a connection end,
/// it tries no more.
#[tokio::test(start_paused = true)]
async fn dropped_in_its_pause_it_tries_no_more() {
    let tries = Cell::new(0);
    let waited = tokio::time::timeout(PAUSE * 3 + PAUSE / 2, next(failing(1_000, &tries)));
    assert!(waited.await.is_err());
    assert_eq!(tries.get(), 4);
    tokio::time::sleep(PAUSE * 10).await;
    assert_eq!(tries.get(), 4);
}
