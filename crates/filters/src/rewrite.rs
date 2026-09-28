//! Rewrites: Gateway API's `URLRewrite`, a change to what a request asks the upstream for.
//!
//! A rewrite is applied after routing, to the head that goes upstream: the request has been
//! routed on its normalised path and its host, and the upstream is sent another path, host
//! or both. It never routes the request again. The path is changed by [`PathModifier`], so
//! what it makes is in normal form without a check per request; the host is made once, as
//! the `Host` value it becomes.

use crate::path_modifier::PathModifier;
use crate::redirect::is_precise_host;
use http::HeaderValue;

/// A rewrite, checked: a host, a path change, or both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UrlRewrite {
    host: Option<HeaderValue>,
    path: Option<PathModifier>,
}

impl UrlRewrite {
    /// A rewrite to `host` (a DNS name in lower case, the `Host` the upstream is sent, with
    /// no port), a path change, or both.
    ///
    /// # Errors
    ///
    /// Returns a [`RewriteError`] for a rewrite of nothing, or a host that is not a DNS name
    /// in lower case (Gateway API's `PreciseHostname`).
    pub fn new(host: Option<&str>, path: Option<PathModifier>) -> Result<Self, RewriteError> {
        if host.is_none() && path.is_none() {
            return Err(RewriteError::Nothing);
        }
        let host = host
            .map(|host| {
                if !is_precise_host(host) {
                    return Err(RewriteError::Host(host.to_owned()));
                }
                HeaderValue::from_str(host).map_err(|_| RewriteError::Host(host.to_owned()))
            })
            .transpose()?;
        Ok(Self { host, path })
    }

    /// The `Host` the upstream is sent, if the rewrite changes it.
    #[must_use]
    pub fn host(&self) -> Option<&HeaderValue> {
        self.host.as_ref()
    }

    /// The path change, if the rewrite makes one.
    #[must_use]
    pub fn path(&self) -> Option<&PathModifier> {
        self.path.as_ref()
    }
}

/// Why a rewrite was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RewriteError {
    /// A rewrite that changes nothing.
    #[error("`url_rewrite` needs a `host`, a `path` or both")]
    Nothing,
    /// A host that is not a DNS name in lower case.
    #[error("rewrite host `{0}` is not a DNS name in lower case")]
    Host(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_rewrite_changes_the_host_the_path_or_both() {
        let host = UrlRewrite::new(Some("one.example.org"), None).unwrap();
        assert_eq!(host.host().unwrap(), "one.example.org");
        assert!(host.path().is_none());
        let full = PathModifier::full("/one").unwrap();
        let path = UrlRewrite::new(None, Some(full.clone())).unwrap();
        assert!(path.host().is_none());
        assert_eq!(path.path(), Some(&full));
        assert!(UrlRewrite::new(Some("a.example"), Some(full)).is_ok());
    }

    #[test]
    fn a_rewrite_of_nothing_or_to_no_host_is_refused() {
        assert_eq!(UrlRewrite::new(None, None), Err(RewriteError::Nothing));
        for bad in [
            "One.example.org",
            "a.example:8080",
            "*.example",
            "192.0.2.1",
            "",
        ] {
            assert_eq!(
                UrlRewrite::new(Some(bad), None),
                Err(RewriteError::Host(bad.to_owned())),
                "{bad}"
            );
        }
    }
}
