//! Backend protection's annotations on a Service
//! ([25 §2](../../../docs/25-backend-protection.md)): a bound on each pod's requests in
//! flight from each data plane, and a queue for when every pod is full, compiled into every
//! upstream of an HTTP or gRPC route that reaches the Service.
//!
//! **Read whole or not at all.** Annotations that do not read, a value that does not parse,
//! a queue stated by half, or a key under `gateway.edgerush.dev/` that is none of these,
//! leave the Service unprotected and are said, as a finding on the Service, for each data
//! plane whose routes reach it: a typo that left a backend unguarded without a word would
//! be found only once the backend was buried.

use crate::Object;
use crate::http_route::milliseconds;
use edgerush_config::{Protection, ProtectionQueue};
use k8s_openapi::api::core::v1::Service;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::num::{NonZeroU32, NonZeroU64};

/// The annotation a Service bounds each of its pods' requests in flight from each data
/// plane with: a whole number above zero.
pub const MAX_IN_FLIGHT_PER_POD_ANNOTATION: &str = "gateway.edgerush.dev/max-in-flight-per-pod";

/// The annotation a Service says how many requests may wait for a pod with room with, every
/// pod full: a whole number above zero, stated with the queue's timeout or not at all.
pub const QUEUE_SIZE_ANNOTATION: &str = "gateway.edgerush.dev/queue-size";

/// The annotation a Service says how long a request may wait for a pod with room with: a
/// Gateway API duration above zero (GEP-2257), `2s`, stated with the queue's size or not at
/// all.
pub const QUEUE_TIMEOUT_ANNOTATION: &str = "gateway.edgerush.dev/queue-timeout";

/// What EdgeRush's own annotations begin with.
const PREFIX: &str = "gateway.edgerush.dev/";

/// The protection `service`'s annotations ask for, none where they ask for none, or why
/// they cannot be read, each thing wrong with them said.
pub(crate) fn asked(service: &Service) -> Result<Option<Protection>, String> {
    let Some(annotations) = service.metadata.annotations.as_ref() else {
        return Ok(None);
    };
    let mut wrong = Vec::new();
    for key in annotations.keys() {
        if key.starts_with(PREFIX)
            && ![
                MAX_IN_FLIGHT_PER_POD_ANNOTATION,
                QUEUE_SIZE_ANNOTATION,
                QUEUE_TIMEOUT_ANNOTATION,
            ]
            .contains(&key.as_str())
        {
            wrong.push(format!(
                "`{key}` is not an annotation EdgeRush reads on a Service"
            ));
        }
    }
    let read = |key: &str| annotations.get(key).map(String::as_str);
    let mut count_at = |key: &'static str| {
        read(key).and_then(|value| {
            let counted = count(value);
            if counted.is_none() {
                wrong.push(format!(
                    "`{key}` is `{value}`, not a whole number from 1 to {}",
                    u32::MAX
                ));
            }
            counted
        })
    };
    let max_in_flight = count_at(MAX_IN_FLIGHT_PER_POD_ANNOTATION);
    let size = count_at(QUEUE_SIZE_ANNOTATION);
    let timeout = read(QUEUE_TIMEOUT_ANNOTATION).and_then(|value| {
        let timeout = milliseconds(value).ok().and_then(NonZeroU64::new);
        if timeout.is_none() {
            wrong.push(format!(
                "`{QUEUE_TIMEOUT_ANNOTATION}` is `{value}`, not a Gateway API duration above \
                 zero, such as `2s` or `500ms`"
            ));
        }
        timeout
    });
    let stated = |key| read(key).is_some();
    match (
        stated(QUEUE_SIZE_ANNOTATION),
        stated(QUEUE_TIMEOUT_ANNOTATION),
    ) {
        (true, false) => wrong.push(format!(
            "`{QUEUE_SIZE_ANNOTATION}` is stated without `{QUEUE_TIMEOUT_ANNOTATION}`; a queue \
             takes both"
        )),
        (false, true) => wrong.push(format!(
            "`{QUEUE_TIMEOUT_ANNOTATION}` is stated without `{QUEUE_SIZE_ANNOTATION}`; a queue \
             takes both"
        )),
        _ => {}
    }
    let queued = stated(QUEUE_SIZE_ANNOTATION) || stated(QUEUE_TIMEOUT_ANNOTATION);
    if queued && !stated(MAX_IN_FLIGHT_PER_POD_ANNOTATION) {
        wrong.push(format!(
            "a queue is stated without `{MAX_IN_FLIGHT_PER_POD_ANNOTATION}`, which says when a \
             pod is full"
        ));
    }
    if !wrong.is_empty() {
        return Err(wrong.join("; "));
    }
    Ok(max_in_flight.map(|max_in_flight| Protection {
        max_in_flight,
        queue: size
            .zip(timeout)
            .map(|(size, timeout_ms)| ProtectionQueue { size, timeout_ms }),
    }))
}

/// A count written as Kubernetes users write one: decimal digits alone, no sign, no space,
/// above zero.
fn count(value: &str) -> Option<NonZeroU32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return None;
    }
    value.parse().ok()
}

/// The Services a data plane's HTTP and gRPC routes reach whose protection cannot be read,
/// each with why, as the routes are translated: each is said once, however many reach it.
#[derive(Debug, Default)]
pub(crate) struct Unprotected {
    reached: RefCell<BTreeMap<Object, String>>,
}

impl Unprotected {
    /// The protection `service`'s annotations ask for: none where they ask for none, and
    /// none, said, where they cannot be read.
    pub(crate) fn of(&self, service: &Service) -> Option<Protection> {
        asked(service).unwrap_or_else(|why| {
            self.reached
                .borrow_mut()
                .insert(Object::of("Service", &service.metadata), why);
            None
        })
    }

    /// Each Service said, by namespace and name, and why.
    pub(crate) fn said(self) -> BTreeMap<Object, String> {
        self.reached.into_inner()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Service of `annotations`, written as YAML's flow mapping.
    fn service(annotations: &str) -> Service {
        serde_saphyr::from_str(&format!(
            "metadata: {{ name: search, namespace: shop, annotations: {annotations} }}"
        ))
        .unwrap()
    }

    fn protection(max_in_flight: u32, queue: Option<(u32, u64)>) -> Protection {
        Protection {
            max_in_flight: NonZeroU32::new(max_in_flight).unwrap(),
            queue: queue.map(|(size, timeout_ms)| ProtectionQueue {
                size: NonZeroU32::new(size).unwrap(),
                timeout_ms: NonZeroU64::new(timeout_ms).unwrap(),
            }),
        }
    }

    /// A Service of none of protection's annotations asks for none, whatever else it
    /// carries; one of the cap asks for it, with a queue where both its size and its
    /// timeout are stated, the timeout read as Gateway API's durations are.
    #[test]
    fn annotations_read_as_25_has_them() {
        let read = |annotations: &str| asked(&service(annotations));
        assert_eq!(asked(&Service::default()), Ok(None));
        assert_eq!(
            read("{ team: search, kubernetes.io/description: shop }"),
            Ok(None)
        );
        assert_eq!(
            read("{ gateway.edgerush.dev/max-in-flight-per-pod: '20' }"),
            Ok(Some(protection(20, None)))
        );
        assert_eq!(
            read(
                "{ gateway.edgerush.dev/max-in-flight-per-pod: '20', \
                 gateway.edgerush.dev/queue-size: '100', \
                 gateway.edgerush.dev/queue-timeout: 1m2s500ms }"
            ),
            Ok(Some(protection(20, Some((100, 62_500)))))
        );
        assert_eq!(
            read(&format!(
                "{{ gateway.edgerush.dev/max-in-flight-per-pod: '{}' }}",
                u32::MAX
            )),
            Ok(Some(protection(u32::MAX, None)))
        );
    }

    /// Whatever is wrong is said, each thing, and the Service asks for nothing: a count not
    /// a whole number above zero, a timeout not a duration above zero, a queue by half or
    /// with no cap, a key of EdgeRush's that is none of protection's.
    #[test]
    fn annotations_that_do_not_read_say_why_and_protect_nothing() {
        let wrong = |annotations: &str| asked(&service(annotations)).unwrap_err();
        for value in [
            "'0'",
            "'-1'",
            "'+20'",
            "' 20'",
            "'20 '",
            "'2e1'",
            "''",
            "'4294967296'",
        ] {
            let why = wrong(&format!(
                "{{ gateway.edgerush.dev/max-in-flight-per-pod: {value} }}"
            ));
            assert!(
                why.contains("max-in-flight-per-pod") && why.contains("not a whole number"),
                "{value}: {why}"
            );
        }
        for value in ["0s", "2", "2 s", "1.5s", "123456s", "1h1m1s1ms1h", "-2s"] {
            let why = wrong(&format!(
                "{{ gateway.edgerush.dev/max-in-flight-per-pod: '20', \
                 gateway.edgerush.dev/queue-size: '10', \
                 gateway.edgerush.dev/queue-timeout: '{value}' }}"
            ));
            assert!(why.contains("not a Gateway API duration"), "{value}: {why}");
        }
        assert!(
            wrong(
                "{ gateway.edgerush.dev/max-in-flight-per-pod: '20', \
                 gateway.edgerush.dev/queue-size: '10' }"
            )
            .contains("`gateway.edgerush.dev/queue-size` is stated without")
        );
        assert!(
            wrong(
                "{ gateway.edgerush.dev/max-in-flight-per-pod: '20', \
                 gateway.edgerush.dev/queue-timeout: 2s }"
            )
            .contains("`gateway.edgerush.dev/queue-timeout` is stated without")
        );
        assert!(
            wrong(
                "{ gateway.edgerush.dev/queue-size: '10', gateway.edgerush.dev/queue-timeout: 2s }"
            )
            .contains("a queue is stated without")
        );
        assert_eq!(
            wrong(
                "{ gateway.edgerush.dev/max-inflight-per-pod: '20', \
                 gateway.edgerush.dev/max-in-flight-per-pod: '0' }"
            ),
            "`gateway.edgerush.dev/max-inflight-per-pod` is not an annotation EdgeRush reads on \
             a Service; `gateway.edgerush.dev/max-in-flight-per-pod` is `0`, not a whole number \
             from 1 to 4294967295"
        );
    }

    /// A Service whose annotations do not read is said once, however often it is reached,
    /// and one that reads is not said.
    #[test]
    fn a_service_is_said_once() {
        let unprotected = Unprotected::default();
        let bad = service("{ gateway.edgerush.dev/max-in-flight-per-pod: none }");
        let good = service("{ gateway.edgerush.dev/max-in-flight-per-pod: '3' }");
        assert_eq!(unprotected.of(&bad), None);
        assert_eq!(unprotected.of(&bad), None);
        assert_eq!(unprotected.of(&good), Some(protection(3, None)));
        let said = unprotected.said();
        assert_eq!(said.len(), 1);
        let (object, why) = said.iter().next().unwrap();
        assert_eq!(
            (object.kind, object.namespace.as_str(), object.name.as_str()),
            ("Service", "shop", "search")
        );
        assert!(why.contains("is `none`"), "{why}");
    }
}
