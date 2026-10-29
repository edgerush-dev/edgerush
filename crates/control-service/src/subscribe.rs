//! The data plane's side, for as long as it runs: the stream dialled, followed until it
//! ends, and dialled again after a wait ([03 §10](../../../docs/03-data-plane.md)).

use crate::{Credentials, Ended, Files, TlsError, connector, follow, reread};
use edgerush_config::Config;
use edgerush_control_stream::Staging;
use std::convert::Infallible;
use std::future::Future;
use std::hash::{BuildHasher, RandomState};
use std::time::Duration;
use tokio::sync::watch;

/// How long to wait before dialling again: a delay drawn at random below a bound, which
/// doubles after each up to a most, and starts again once a stream has carried a config.
/// Drawn so, a fleet that lost its control plane at once does not dial it again at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Backoff {
    /// The first bound.
    pub base: Duration,
    /// The bound it doubles up to.
    pub most: Duration,
}

impl Default for Backoff {
    /// Envoy's xDS client's: 0.5 s, doubled up to 30 s.
    fn default() -> Self {
        Self {
            base: Duration::from_millis(500),
            most: Duration::from_secs(30),
        }
    }
}

/// The bound a [`Backoff`] has reached.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Waiting {
    backoff: Backoff,
    bound: Duration,
}

impl Waiting {
    fn new(backoff: Backoff) -> Self {
        Self {
            backoff,
            bound: backoff.base,
        }
    }

    /// The next wait, drawn from `random` below the bound, which then doubles.
    fn next(&mut self, random: u64) -> Duration {
        let bound = u64::try_from(self.bound.as_micros())
            .unwrap_or(u64::MAX)
            .max(1);
        self.bound = (self.bound * 2).min(self.backoff.most);
        Duration::from_micros(random % bound)
    }

    fn reset(&mut self) {
        self.bound = self.backoff.base;
    }
}

/// What a data plane following its control plane says as it goes, for its caller to log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Following {
    /// The stream ended, or could not be opened; it is dialled again after `wait`.
    Ended {
        /// Why.
        ended: Ended,
        /// How long until it is dialled again.
        wait: Duration,
    },
    /// The certificate files changed, and the next dial presents and trusts what they hold.
    Replaced,
    /// The certificate files cannot be read or used; the last are kept.
    Unusable(String),
}

/// Where a data plane's control plane is, and what it presents to it.
#[derive(Debug, Clone)]
pub struct Subscription {
    /// The control plane's address, `host:port`; its certificate must name the host.
    pub address: String,
    /// The data plane's certificate files.
    pub files: Files,
    /// What they held as it started.
    pub credentials: Credentials,
    /// How often they are read again.
    pub reread: Duration,
    /// How long it waits before dialling again.
    pub backoff: Backoff,
}

/// Follows the control plane for as long as it is polled: each config given to run handed
/// to `run`, which runs it or says why it cannot, from what `staging` holds, the addresses
/// of the gateway's own pods to `gateway` each time they come, and what happens along the
/// way to `told`.
///
/// # Errors
///
/// A [`TlsError`], at once, if the credentials it starts with cannot be used.
pub async fn subscribe<R>(
    subscription: Subscription,
    staging: &mut Staging,
    mut run: impl FnMut(Box<Config>) -> R,
    mut gateway: impl FnMut(Vec<std::net::IpAddr>),
    told: impl Fn(Following),
) -> Result<Infallible, TlsError>
where
    R: Future<Output = Result<(), String>>,
{
    let Subscription {
        address,
        files,
        credentials,
        reread: every,
        backoff,
    } = subscription;
    let (tls, dialled) = watch::channel(connector(&credentials)?);
    let name = host(&address).to_owned();
    let rereading = reread(files, credentials, every, |changed| {
        match changed.map(connector) {
            Ok(Ok(replaced)) => {
                tls.send_replace(replaced);
                told(Following::Replaced);
            }
            Ok(Err(error)) => told(Following::Unusable(error.to_string())),
            Err(error) => told(Following::Unusable(error.to_string())),
        }
    });
    let following = async {
        let mut waiting = Waiting::new(backoff);
        let random = RandomState::new();
        let mut attempt: u64 = 0;
        loop {
            let tls = dialled.borrow().clone();
            let mut carried = false;
            let ended = follow(
                &address,
                &name,
                &tls,
                staging,
                |config| {
                    carried = true;
                    run(config)
                },
                &mut gateway,
            )
            .await;
            if carried {
                waiting.reset();
            }
            attempt += 1;
            let wait = waiting.next(random.hash_one(attempt));
            told(Following::Ended { ended, wait });
            tokio::time::sleep(wait).await;
        }
    };
    tokio::select! {
        never = following => never,
        () = rereading => unreachable_rereading(),
    }
}

/// [`reread`] runs for as long as it is polled.
fn unreachable_rereading() -> Result<Infallible, TlsError> {
    Err(TlsError::Setup(
        "the certificate files stopped being read".to_owned(),
    ))
}

/// The host of `address`, `host:port`, without an IPv6 address's brackets.
fn host(address: &str) -> &str {
    let host = address
        .rsplit_once(':')
        .map_or(address, |(host, _port)| host);
    host.strip_prefix('[')
        .and_then(|host| host.strip_suffix(']'))
        .unwrap_or(host)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn each_wait_is_drawn_below_a_bound_that_doubles_up_to_the_most_and_starts_again() {
        let backoff = Backoff {
            base: Duration::from_millis(500),
            most: Duration::from_secs(3),
        };
        let mut waiting = Waiting::new(backoff);
        // Each bound, exactly: a draw just below it is the wait, and one of it is none.
        let bound = |waiting: &Waiting| {
            let mut probe = *waiting;
            let micros = u64::try_from(waiting.bound.as_micros()).unwrap();
            assert_eq!(probe.next(micros), Duration::ZERO);
            micros
        };
        for expected in [500_000, 1_000_000, 2_000_000, 3_000_000, 3_000_000] {
            assert_eq!(bound(&waiting), expected);
            assert_eq!(
                waiting.next(expected - 1),
                Duration::from_micros(expected - 1)
            );
        }
        waiting.reset();
        assert_eq!(bound(&waiting), 500_000);
        assert_eq!(Backoff::default().base, Duration::from_millis(500));
        assert_eq!(Backoff::default().most, Duration::from_secs(30));
    }

    #[test]
    fn the_host_is_the_address_without_its_port() {
        assert_eq!(
            host("edgerush-control.edgerush.svc:9443"),
            "edgerush-control.edgerush.svc"
        );
        assert_eq!(host("10.0.0.1:9443"), "10.0.0.1");
        assert_eq!(host("[::1]:9443"), "::1");
    }
}
