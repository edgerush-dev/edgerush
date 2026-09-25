//! What the gateway knows of a request that is a gRPC call ([15 §6](../../../docs/15-http2-and-grpc.md)).

use super::status::is_grpc;
use super::timeout;
use edgerush_router::Fields;
use http::Version;
use http::header::CONTENT_TYPE;
use tokio::time::Instant;

/// A gRPC call, and when it must be over by.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Call {
    deadline: Option<Instant>,
}

impl Call {
    /// The call a request makes, if it is one: HTTP/2 with a gRPC content type, read from
    /// the head as the client sent it, before any filter could change it. Its deadline is
    /// what its `grpc-timeout` says, counted from `now`, which is asked only then; a
    /// value that is not a timeout, or one said twice, is no deadline — as Envoy reads it
    /// — and goes on as it came.
    pub(crate) fn of<F: Fields + ?Sized>(
        version: Version,
        fields: &F,
        now: impl FnOnce() -> Instant,
    ) -> Option<Self> {
        if version != Version::HTTP_2 {
            return None;
        }
        let mut types = fields.values(&CONTENT_TYPE);
        let content_type = types.next()?;
        if types.next().is_some() || !is_grpc(content_type) {
            return None;
        }
        let name = http::HeaderName::from_static("grpc-timeout");
        let mut timeouts = fields.values(&name);
        let deadline = match (timeouts.next(), timeouts.next()) {
            (Some(value), None) => timeout::parse(value).map(|left| now() + left),
            _ => None,
        };
        Some(Self { deadline })
    }

    /// When the call must be over by, if it said.
    pub(crate) fn deadline(&self) -> Option<Instant> {
        self.deadline
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::HeaderMap;
    use std::time::Duration;

    fn head(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut map = HeaderMap::new();
        for (name, value) in pairs {
            map.append(
                http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        map
    }

    #[test]
    fn a_call_is_http2_with_a_grpc_content_type() {
        let now = Instant::now();
        let grpc = head(&[("content-type", "application/grpc")]);
        assert!(Call::of(Version::HTTP_2, &grpc, || now).is_some());
        assert!(Call::of(Version::HTTP_11, &grpc, || now).is_none());
        let web = head(&[("content-type", "application/grpc-web")]);
        assert!(Call::of(Version::HTTP_2, &web, || now).is_none());
        assert!(Call::of(Version::HTTP_2, &head(&[]), || now).is_none());
        let twice = head(&[
            ("content-type", "application/grpc"),
            ("content-type", "application/grpc"),
        ]);
        assert!(Call::of(Version::HTTP_2, &twice, || now).is_none());
    }

    #[test]
    fn its_deadline_is_its_timeout_from_now_when_it_says_one_it_can_be_read() {
        let now = Instant::now();
        let with = |timeouts: &[&str]| {
            let mut pairs = vec![("content-type", "application/grpc")];
            pairs.extend(timeouts.iter().map(|timeout| ("grpc-timeout", *timeout)));
            Call::of(Version::HTTP_2, &head(&pairs), || now)
                .unwrap()
                .deadline()
        };
        assert_eq!(with(&["250m"]), Some(now + Duration::from_millis(250)));
        assert_eq!(with(&["0n"]), Some(now));
        assert_eq!(with(&[]), None);
        assert_eq!(with(&["soon"]), None);
        assert_eq!(with(&["1S", "2S"]), None);
    }
}
