//! Active health checks of upstream endpoints ([03 §6](../../../docs/03-data-plane.md)).
//!
//! One checker for the process, on a thread and runtime of its own — as HAProxy and
//! Pingora run one per process — so that a worker saturated with requests does not starve
//! it: a check must still run when the data path is busiest, which is when it matters.
//! What it finds it writes on each destination, which every worker reads when it picks an
//! endpoint; a reload that keeps a destination keeps what was found about it.
//!
//! Each endpoint is probed every interval, the first probe spread over the first interval
//! so that a config with many endpoints does not probe them all at once, and each later
//! one moved by up to a tenth of the interval either way. Results count in a row, as
//! HAProxy's `rise` and `fall` and Envoy's thresholds count them: so many failures make a
//! serving endpoint unhealthy, so many passes make it serve again. At most [`AT_ONCE`]
//! probes are out at a time, and never two for one endpoint.

mod probe;

use crate::random::random;
use crate::serve::Proxy;
use crate::upstream::destination::ReuseIdentity;
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

/// The most probes out at once.
pub(crate) const AT_ONCE: usize = 64;

/// How often the checker looks for probes that are due.
const TICK: Duration = Duration::from_millis(100);

/// What the checker knows of one destination.
struct Tracked {
    destination: Arc<ReuseIdentity>,
    next: Instant,
    /// Results the other way from its state, in a row.
    against: u32,
    probing: bool,
}

/// Probes the endpoints of the running config for as long as it runs. For a thread of its
/// own, in a `LocalSet`.
pub(crate) async fn check(proxy: Arc<Proxy>) {
    let tracked: Rc<RefCell<HashMap<u64, Tracked>>> = Rc::default();
    let out = Rc::new(Cell::new(0_usize));
    loop {
        tokio::time::sleep(TICK).await;
        let now = Instant::now();
        let current: Vec<Arc<ReuseIdentity>> = proxy.checked().collect();
        {
            let mut tracked = tracked.borrow_mut();
            tracked.retain(|key, _| current.iter().any(|destination| destination.key() == *key));
            for destination in &current {
                let Some(check) = destination.health_check() else {
                    continue;
                };
                let interval = Duration::from_secs(check.interval_seconds);
                tracked.entry(destination.key()).or_insert_with(|| Tracked {
                    destination: Arc::clone(destination),
                    // Spread over the first interval.
                    next: now + interval.mul_f64(unit()),
                    against: 0,
                    probing: false,
                });
            }
        }
        let due: Vec<u64> = tracked
            .borrow()
            .iter()
            .filter(|(_, tracked)| !tracked.probing && tracked.next <= now)
            .map(|(key, _)| *key)
            .collect();
        for key in due {
            if out.get() >= AT_ONCE {
                break;
            }
            let destination = {
                let mut tracked = tracked.borrow_mut();
                let Some(entry) = tracked.get_mut(&key) else {
                    continue;
                };
                let Some(check) = entry.destination.health_check() else {
                    continue;
                };
                let interval = Duration::from_secs(check.interval_seconds);
                // A tenth of the interval either way.
                entry.next = now + interval.mul_f64(0.9 + 0.2 * unit());
                entry.probing = true;
                Arc::clone(&entry.destination)
            };
            out.set(out.get() + 1);
            let (tracked, out) = (Rc::clone(&tracked), Rc::clone(&out));
            let _probing = tokio::task::spawn_local(async move {
                let Some(check) = destination.health_check() else {
                    return;
                };
                let passed = probe::passes(&destination, check).await;
                out.set(out.get() - 1);
                if let Some(entry) = tracked.borrow_mut().get_mut(&key) {
                    entry.probing = false;
                    record(entry, passed);
                }
            });
        }
    }
}

/// A probe's result: counted against the destination's state when it differs, and the
/// state turned once enough have come in a row.
fn record(entry: &mut Tracked, passed: bool) {
    let Some(check) = entry.destination.health_check() else {
        return;
    };
    let (healthy, against) = turn(
        entry.destination.is_healthy(),
        entry.against,
        passed,
        check.healthy_threshold,
        check.unhealthy_threshold,
    );
    entry.against = against;
    entry.destination.set_healthy(healthy);
}

/// The state after a probe, and the results against it in a row: `rise` passes in a row
/// make an unhealthy endpoint healthy, `fall` failures a healthy one unhealthy, and one
/// result the same way as the state starts the count again.
fn turn(healthy: bool, against: u32, passed: bool, rise: u32, fall: u32) -> (bool, u32) {
    if passed == healthy {
        return (healthy, 0);
    }
    let against = against + 1;
    let needed = if healthy { fall } else { rise };
    if against >= needed {
        (passed, 0)
    } else {
        (healthy, against)
    }
}

/// A number in `[0, 1)`.
fn unit() -> f64 {
    (random() >> 11) as f64 / (1_u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::turn;

    /// Results count in a row: so many failures turn a healthy endpoint, so many passes an
    /// unhealthy one, and one result the other way starts the count again.
    #[test]
    fn a_state_turns_after_enough_results_in_a_row() {
        let (rise, fall) = (2, 3);
        let mut state = (true, 0);
        for (passed, expected) in [
            (false, (true, 1)),
            (false, (true, 2)),
            (true, (true, 0)),
            (false, (true, 1)),
            (false, (true, 2)),
            (false, (false, 0)),
            (true, (false, 1)),
            (false, (false, 0)),
            (true, (false, 1)),
            (true, (true, 0)),
        ] {
            state = turn(state.0, state.1, passed, rise, fall);
            assert_eq!(state, expected);
        }
    }
}
