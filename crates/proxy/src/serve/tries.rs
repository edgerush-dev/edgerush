//! The tries a request is given: again where its rule's retry says and its budget allows,
//! and a copy to each of its mirrors.

use super::{Admitted, Body, Directed, Mirrored, Timing, Worker, wants_again};
use crate::balance::Tried;
use crate::downstream::h1::connection::Answered;
use crate::head::Forwarded;
use crate::interim::Interim;
use crate::metrics::Answer;
use crate::mirror;
use crate::random::random;
use crate::request_body::RequestBody;
use crate::retry::replay::Tee;
use crate::upstream::h1::codec::{OutgoingFields, Sending};
use edgerush_config::CompiledRetry;
use http::{HeaderMap, HeaderName, HeaderValue, Request};
use http_body::Body as HttpBody;
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::time::Instant;

impl Worker {
    /// Tries, and tries again while the rule's retry says to, the budget allows and the
    /// body was kept whole or none of it went (03 §6). Each try goes to an endpoint drawn
    /// afresh; each waits its backoff first; none goes past the request's deadline. What
    /// decides is the answer's head alone — its status, or a gRPC status a trailers-only
    /// head carries — so nothing of an answer has gone to the client when a request is
    /// sent again.
    #[expect(
        clippy::too_many_arguments,
        reason = "each is a different thing the exchange needs, as for `through_h1`"
    )]
    pub(super) async fn with_retries<H: Forwarded>(
        &self,
        directed: &Directed,
        head: &mut H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
        admitted: Admitted,
        interim: Option<Interim>,
        timing: Timing,
        retry: &CompiledRetry,
    ) -> Result<Answered<Body>, Answer> {
        let deadline = timing.deadline;
        let (tee, recorded) = Tee::new(body, self.blocks.borrow().storage());
        let mut body = RequestBody::Recorded(Box::new(tee));
        let mut admitted = admitted;
        // The first try's, and a later one's only where the first let go of the body
        // untouched (below).
        let listened = interim.clone();
        let mut interim = interim;
        let mut endpoint = Arc::clone(&directed.endpoint);
        directed.upstream.budget(|budget| {
            budget.deposit(Instant::now());
        });
        let upstream = || self.proxy.metrics.upstream(directed.upstream.slot());
        let mut retried = 0;
        // Where the tries have gone, for the next to keep away from (03 §6).
        let mut tried = Tried::default();
        if let Some(others) = directed.others.as_deref() {
            tried.add(others.first);
        }
        loop {
            let outcome = self
                .attempt(
                    directed,
                    &endpoint,
                    head,
                    nominated,
                    sending,
                    body,
                    admitted,
                    interim.take(),
                    timing,
                )
                .await;
            if retried >= retry.attempts || !wants_again(retry, &outcome) {
                return outcome;
            }
            // A body none of which went is the body itself, given back: its client may
            // still be waiting for leave to send it, which only the try that asks for the
            // body can give, so what the client hears goes with it. A body sent again from
            // what was kept went whole before, and its client has heard all it will.
            let (again, hearing) = if let Some(replayed) = recorded.replay() {
                (RequestBody::Replayed(replayed), None)
            } else if let Some(untouched) = recorded.given_back() {
                (untouched, listened.clone())
            } else {
                if let Some(upstream) = upstream() {
                    upstream.retries_unkept.inc();
                }
                return outcome;
            };
            // What may still refuse it first, and the budget last: the budget pays for the
            // retries that are sent, as tower's is charged by linkerd's policy as it sends.
            let wait = backoff(retry, retried);
            if deadline.is_some_and(|deadline| Instant::now() + wait >= deadline) {
                if let Some(upstream) = upstream() {
                    upstream.retries_deadline.inc();
                }
                return outcome;
            }
            // A place for the next try before this one's is given back with its answer:
            // a worker at its bound keeps the answer it has rather than lose it.
            let Ok(next) = self.admit(&directed.upstream, directed.alone) else {
                if let Some(upstream) = upstream() {
                    upstream.retries_busy.inc();
                }
                return outcome;
            };
            let Some((target, drawn, at, counted)) = directed.draw(head.uri(), &tried) else {
                if let Some(upstream) = upstream() {
                    upstream.retries_nowhere.inc();
                }
                return outcome;
            };
            if !directed
                .upstream
                .budget(|budget| budget.withdraw(Instant::now()))
            {
                if let Some(upstream) = upstream() {
                    upstream.retries_over_budget.inc();
                }
                return outcome;
            }
            tried.add(at);
            drop(outcome);
            tokio::time::sleep(wait).await;
            if let Some(upstream) = upstream() {
                upstream.retries.inc();
            }
            head.set_uri(target);
            endpoint = drawn;
            body = again;
            interim = hearing;
            admitted = next.counting(Some(counted));
            retried += 1;
        }
    }

    /// Sends a copy of the request to each of `mirrors`, each its own exchange on a task
    /// of its own that nothing waits for, and gives back the body the request is to be
    /// sent with, which copies itself to them as it goes (03 §6). Each copy takes a place
    /// as any exchange does; a worker without one to give sends none. Its answer is read
    /// and thrown away.
    pub(super) fn mirror<H: Forwarded>(
        &self,
        mirrors: Vec<Mirrored>,
        alone: bool,
        head: &H,
        nominated: &[HeaderName],
        sending: Sending,
        body: RequestBody,
    ) -> RequestBody {
        let given_up = |slot: usize,
                        counter: fn(
            &crate::metrics::UpstreamCounters,
        ) -> &edgerush_telemetry::Counter| {
            if let Some(upstream) = self.proxy.metrics.upstream(slot) {
                counter(upstream).inc();
            }
        };
        // Credentials bound to the client's connection are not the mirror's to use, and
        // without them the copy would not be the request.
        use crate::upstream::auth::carries_credentials;
        let upstream_carries = carries_credentials(head.outgoing());
        let mirrors: Vec<Mirrored> = mirrors
            .into_iter()
            .filter(|mirror| {
                let bound = mirror
                    .fields
                    .as_ref()
                    .map_or(upstream_carries, carries_credentials);
                if bound {
                    given_up(mirror.upstream.slot(), |counters| {
                        &counters.mirrors_credentials
                    });
                }
                !bound
            })
            .collect();
        if mirrors.is_empty() {
            return body;
        }
        let Some(worker) = self.me.upgrade() else {
            return body;
        };
        let placed: Vec<_> = mirrors
            .into_iter()
            .filter_map(|mut mirror| match self.admit(&mirror.upstream, alone) {
                Ok(admitted) => {
                    let counted = mirror.counted.take();
                    Some((mirror, admitted.counting(counted)))
                }
                Err(_) => {
                    given_up(mirror.upstream.slot(), |counters| &counters.mirrors_busy);
                    None
                }
            })
            .collect();
        if placed.is_empty() {
            return body;
        }
        // The request as it goes upstream, for the copies of it; made only if one is.
        let mut going = None;
        let (tee, copies) = mirror::Tee::new(body, placed.len(), self.blocks.borrow().storage());
        for ((mut mirror, admitted), (copy, kept)) in placed.into_iter().zip(copies) {
            let headers = match mirror.fields.take() {
                // A copy made before the changes after it, and so before what the rest of
                // the way does.
                Some(own) => own,
                None => going
                    .get_or_insert_with(|| {
                        let mut headers = HeaderMap::new();
                        head.outgoing().each_field(|name, value| {
                            if let (Ok(name), Ok(value)) =
                                (HeaderName::from_bytes(name), HeaderValue::from_bytes(value))
                            {
                                headers.append(name, value);
                            }
                        });
                        headers
                    })
                    .clone(),
            };
            let (mut parts, ()) = Request::new(()).into_parts();
            parts.method = head.method().clone();
            parts.uri = mirror.target;
            parts.headers = headers;
            // Nobody is there to be told to go on: a copy is sent without asking.
            parts.headers.remove(http::header::EXPECT);
            let nominated = nominated.to_vec();
            let worker = Rc::clone(&worker);
            let _copying = tokio::task::spawn_local(async move {
                let directed = Directed {
                    rule: None,
                    upstream: Rc::clone(&mirror.upstream),
                    alone,
                    endpoint: Arc::clone(&mirror.endpoint),
                    counted: None,
                    others: None,
                    mirrors: Vec::new(),
                    websocket: None,
                    upgradable: false,
                    logging: None,
                };
                // A copy given up on is let go of at once, its exchange and place with it:
                // what sends it may be waiting for room its upstream will never give, and
                // would learn of it only at its idle bound.
                {
                    let mut copying = pin!(async {
                        let answered = worker
                            .attempt(
                                &directed,
                                &mirror.endpoint,
                                &parts,
                                &nominated,
                                sending,
                                RequestBody::Copy(copy),
                                admitted,
                                None,
                                Timing::FIXED,
                            )
                            .await;
                        // Read to its end, so that its connection can carry another request.
                        let mut body = match answered {
                            Ok(Answered::Raw(_, body)) => body,
                            Ok(Answered::Map(response)) => response.into_body(),
                            Err(_) => Body::Empty,
                        };
                        while let Some(Ok(_)) =
                            std::future::poll_fn(|cx| Pin::new(&mut body).poll_frame(cx)).await
                        {
                        }
                    });
                    let mut given_up = pin!(kept.given_up());
                    std::future::poll_fn(|cx| {
                        if copying.as_mut().poll(cx).is_ready()
                            || given_up.as_mut().poll(cx).is_ready()
                        {
                            return Poll::Ready(());
                        }
                        Poll::Pending
                    })
                    .await;
                }
                if kept.fell_behind()
                    && let Some(upstream) = worker.proxy.metrics.upstream(mirror.upstream.slot())
                {
                    upstream.mirrors_behind.inc();
                }
            });
        }
        RequestBody::Mirrored(Box::new(tee))
    }
}

/// The wait before try `tried + 1`: the base doubled for each try before, no more than the
/// most, and as much again at random — linkerd's backoff, with its jitter of one.
fn backoff(retry: &CompiledRetry, tried: u32) -> Duration {
    let doubled = retry
        .backoff_base
        .saturating_mul(1 << tried.min(16))
        .min(retry.backoff_max);
    let jitter = (random() >> 11) as f64 / (1_u64 << 53) as f64;
    doubled + doubled.mul_f64(jitter)
}
