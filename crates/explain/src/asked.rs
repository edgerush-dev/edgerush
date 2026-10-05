//! A request as `edgerush explain` and `edgerush test` state it
//! ([22 §2](../../../docs/22-explain-and-test.md)): every field that changes what the core
//! does, none filled in by default, and the head it comes as — what its client would send:
//! over HTTP/1.x an origin-form target and a `Host` field, over HTTP/2 and 3 the URL's
//! scheme, host and path as `:scheme`, `:authority` and `:path`.

use edgerush_config::{CompiledListener, Protocol};
use edgerush_proxy::head::set_protocol;
use http::header::{HOST, HeaderName, HeaderValue};
use http::request::Parts;
use http::uri::{Authority, PathAndQuery, Scheme};
use http::{Method, Request, Uri, Version};
use std::net::IpAddr;

/// A request to a listener of `http` or `https`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Asked {
    /// The address it came from.
    pub client: IpAddr,
    /// The HTTP version it came by.
    pub protocol: Version,
    /// Its method, as sent.
    pub method: Method,
    /// Its scheme, the listener's.
    pub scheme: Scheme,
    /// Its host, with the port if one is given.
    pub authority: Authority,
    /// Its path and query, as written.
    pub target: PathAndQuery,
    /// Its field lines, in order.
    pub headers: Vec<(HeaderName, HeaderValue)>,
    /// An extended CONNECT's `:protocol`.
    pub connect_protocol: Option<String>,
}

/// Why a request cannot be made of what was said.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Invalid {
    /// The client is not an IP address.
    #[error("'{0}' is not an IP address")]
    Client(String),
    /// The protocol is not an HTTP version the gateway serves.
    #[error("'{0}' is not a protocol: \"1.0\", \"1.1\", \"2\" or \"3\"")]
    Protocol(String),
    /// The method is not a token.
    #[error("'{0}' is not a method")]
    Method(String),
    /// The URL is not absolute.
    #[error("'{0}' is not a URL with a scheme, a host and a path")]
    Url(String),
    /// A header line has no colon.
    #[error("'{0}' is not a header line, a name and a value: 'Name: value'")]
    HeaderLine(String),
    /// A header name is not a token.
    #[error("'{0}' is not a header name")]
    HeaderName(String),
    /// A header value has what no field value may.
    #[error("'{value}' is not a value for header {name}")]
    HeaderValue {
        /// The header's name.
        name: String,
        /// The value given.
        value: String,
    },
    /// `Host` among the headers: the URL gives it.
    #[error("Host is the URL's to give: write it in the URL")]
    Host,
    /// The URL's scheme is not the listener's.
    #[error("the scheme of listener {listener} is {wanted}, not {given}")]
    Scheme {
        /// The listener.
        listener: String,
        /// Its scheme.
        wanted: &'static str,
        /// The URL's.
        given: String,
    },
    /// HTTP/3 to a listener that does not serve it.
    #[error("listener {0} does not serve HTTP/3")]
    NoHttp3(String),
    /// A connect protocol on anything but a CONNECT over HTTP/2 or HTTP/3.
    #[error("a connect protocol is for a CONNECT over HTTP/2 or HTTP/3 alone")]
    ConnectProtocol,
}

/// The HTTP version `text` names.
pub fn protocol(text: &str) -> Result<Version, Invalid> {
    match text {
        "1.0" => Ok(Version::HTTP_10),
        "1.1" => Ok(Version::HTTP_11),
        "2" => Ok(Version::HTTP_2),
        "3" => Ok(Version::HTTP_3),
        _ => Err(Invalid::Protocol(text.to_owned())),
    }
}

/// The method `text` names, as sent: case matters, as it does on the wire.
pub fn method(text: &str) -> Result<Method, Invalid> {
    Method::from_bytes(text.as_bytes()).map_err(|_| Invalid::Method(text.to_owned()))
}

/// The scheme, host and target of an absolute URL, as written: the path is not normalised,
/// so that what the core does to it is what is explained. A URL without a path has `/`.
pub fn url(text: &str) -> Result<(Scheme, Authority, PathAndQuery), Invalid> {
    let invalid = || Invalid::Url(text.to_owned());
    let parts = text.parse::<Uri>().map_err(|_| invalid())?.into_parts();
    let scheme = parts.scheme.ok_or_else(invalid)?;
    let authority = parts.authority.ok_or_else(invalid)?;
    let target = match parts.path_and_query {
        Some(target) if target.as_str().starts_with('/') => target,
        // A query with no path before it: the path, which reads as `/`, is empty.
        Some(target) if target.as_str().starts_with('?') => {
            PathAndQuery::try_from(format!("/{}", target.as_str())).map_err(|_| invalid())?
        }
        Some(_) => return Err(invalid()),
        None => PathAndQuery::from_static("/"),
    };
    Ok((scheme, authority, target))
}

/// A header line as a flag gives it, `Name: value`: the value without the white space
/// around it, as a server reads a field line.
pub fn header_line(line: &str) -> Result<(HeaderName, HeaderValue), Invalid> {
    let (name, value) = line
        .split_once(':')
        .ok_or_else(|| Invalid::HeaderLine(line.to_owned()))?;
    header(name, value.trim_matches([' ', '\t']))
}

/// A header, by its name and value. `Host` is refused: the URL gives it.
pub fn header(name: &str, value: &str) -> Result<(HeaderName, HeaderValue), Invalid> {
    let parsed = header_name(name)?;
    if parsed == HOST {
        return Err(Invalid::Host);
    }
    Ok((parsed, header_value(name, value)?))
}

/// A field line of an answer, by its name and value: any an answer may have.
pub fn answer_header(name: &str, value: &str) -> Result<(HeaderName, HeaderValue), Invalid> {
    Ok((header_name(name)?, header_value(name, value)?))
}

fn header_name(name: &str) -> Result<HeaderName, Invalid> {
    HeaderName::from_bytes(name.as_bytes()).map_err(|_| Invalid::HeaderName(name.to_owned()))
}

fn header_value(name: &str, value: &str) -> Result<HeaderValue, Invalid> {
    HeaderValue::from_str(value).map_err(|_| Invalid::HeaderValue {
        name: name.to_owned(),
        value: value.to_owned(),
    })
}

impl Asked {
    /// The head this request comes as to `listener`, an `http` or `https` one.
    ///
    /// # Errors
    ///
    /// If its scheme is not the listener's, it asks for HTTP/3 where none is served, or a
    /// connect protocol where it means nothing.
    pub fn head(&self, listener: &CompiledListener) -> Result<Parts, Invalid> {
        let wanted = match listener.protocol {
            Protocol::Https => Scheme::HTTPS,
            Protocol::Http | Protocol::Tcp | Protocol::Tls => Scheme::HTTP,
        };
        if self.scheme != wanted {
            return Err(Invalid::Scheme {
                listener: listener.name.clone(),
                wanted: if wanted == Scheme::HTTPS {
                    "https"
                } else {
                    "http"
                },
                given: self.scheme.to_string(),
            });
        }
        if self.protocol == Version::HTTP_3 && listener.http3.is_none() {
            return Err(Invalid::NoHttp3(listener.name.clone()));
        }
        let multiplexed = matches!(self.protocol, Version::HTTP_2 | Version::HTTP_3);
        if self.connect_protocol.is_some() && !(multiplexed && self.method == Method::CONNECT) {
            return Err(Invalid::ConnectProtocol);
        }

        let mut request = Request::builder()
            .method(self.method.clone())
            .version(self.protocol);
        request = if multiplexed {
            let uri = Uri::builder()
                .scheme(self.scheme.clone())
                .authority(self.authority.clone())
                .path_and_query(self.target.clone())
                .build();
            match uri {
                Ok(uri) => request.uri(uri),
                Err(_) => return Err(Invalid::Url(self.authority.to_string())),
            }
        } else {
            request
                .uri(self.target.clone())
                .header(HOST, self.authority.as_str())
        };
        for (name, value) in &self.headers {
            request = request.header(name, value);
        }
        let (mut parts, ()) = request
            .body(())
            .map_err(|_| Invalid::Url(self.authority.to_string()))?
            .into_parts();
        if let Some(protocol) = &self.connect_protocol {
            set_protocol(&mut parts, protocol);
        }
        Ok(parts)
    }

    /// The URL as stated.
    pub fn url(&self) -> String {
        format!("{}://{}{}", self.scheme, self.authority, self.target)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use edgerush_config::{Certificate, Compiled, Config, compile};
    use edgerush_proxy::head::Head;

    const LISTENERS: &str = r#"
listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
  secure: { address: "[::]:8443", protocol: https, proxy_protocol: off, tls: { certificates: [site] }, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
  quic: { address: "[::]:9443", protocol: https, proxy_protocol: off, tls: { certificates: [site] }, http3: {}, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
routes: []
upstreams: {}
"#;

    fn compiled() -> Compiled {
        let mut config: Config = serde_saphyr::from_str(LISTENERS).unwrap();
        let site = Certificate {
            chain: String::new(),
            key: String::new(),
        };
        config.certificates.insert("site".to_owned(), site);
        compile(&config).unwrap()
    }

    fn asked(protocol: &str, url: &str) -> Asked {
        let (scheme, authority, target) = super::url(url).unwrap();
        Asked {
            client: "203.0.113.7".parse().unwrap(),
            protocol: super::protocol(protocol).unwrap(),
            method: Method::GET,
            scheme,
            authority,
            target,
            headers: vec![header_line("X-Debug: 1").unwrap()],
            connect_protocol: None,
        }
    }

    fn head(listener: &str, asked: &Asked) -> Result<Parts, Invalid> {
        let compiled = compiled();
        let listener = compiled
            .listeners()
            .iter()
            .find(|l| l.name == listener)
            .unwrap();
        asked.head(listener)
    }

    #[test]
    fn each_field_is_read_as_written() {
        assert_eq!(protocol("1.0"), Ok(Version::HTTP_10));
        assert_eq!(protocol("3"), Ok(Version::HTTP_3));
        assert_eq!(
            protocol("HTTP/1.1"),
            Err(Invalid::Protocol("HTTP/1.1".to_owned()))
        );
        assert_eq!(method("PURGE").unwrap().as_str(), "PURGE");
        assert_eq!(method("get").unwrap().as_str(), "get");
        assert!(method("GE T").is_err());

        let (scheme, authority, target) = url("http://Shop.Example.com:8080/a/../b?q=1").unwrap();
        assert_eq!(
            (scheme.as_str(), authority.as_str(), target.as_str()),
            ("http", "Shop.Example.com:8080", "/a/../b?q=1")
        );
        // A URL without a path has `/`, and one with a query and no path has it before it.
        assert_eq!(url("http://a.example").unwrap().2.as_str(), "/");
        assert_eq!(url("http://a.example?q=1").unwrap().2.as_str(), "/?q=1");
        for invalid in [
            "/just/a/path",
            "shop.example.com/x",
            "http:///x",
            "http://a b/",
        ] {
            assert_eq!(
                url(invalid),
                Err(Invalid::Url(invalid.to_owned())),
                "{invalid}"
            );
        }
    }

    #[test]
    fn a_header_line_is_a_name_and_a_value_and_host_is_the_urls() {
        let (name, value) = header_line("X-Env:  canary \t").unwrap();
        assert_eq!((name.as_str(), value.as_bytes()), ("x-env", &b"canary"[..]));
        assert_eq!(header_line("X-Empty:").unwrap().1.as_bytes(), b"");
        assert_eq!(
            header_line("X-Env canary"),
            Err(Invalid::HeaderLine("X-Env canary".to_owned()))
        );
        assert_eq!(
            header_line("X Env: canary"),
            Err(Invalid::HeaderName("X Env".to_owned()))
        );
        assert_eq!(header_line("Host: other.example"), Err(Invalid::Host));
        assert_eq!(header("hOsT", "other.example"), Err(Invalid::Host));
        assert!(matches!(
            header("X-Env", "a\nb"),
            Err(Invalid::HeaderValue { .. })
        ));
    }

    #[test]
    fn over_http1_the_target_is_origin_form_and_the_host_a_field() {
        for version in ["1.0", "1.1"] {
            let head = head("web", &asked(version, "http://Shop.Example.com:8080/a?q=1")).unwrap();
            assert_eq!(head.uri.to_string(), "/a?q=1");
            let fields: Vec<(&str, &[u8])> = head
                .headers
                .iter()
                .map(|(name, value)| (name.as_str(), value.as_bytes()))
                .collect();
            assert_eq!(
                fields,
                [
                    ("host", &b"Shop.Example.com:8080"[..]),
                    ("x-debug", &b"1"[..])
                ]
            );
            assert_eq!(head.version, protocol(version).unwrap());
        }
    }

    #[test]
    fn over_http2_and_3_the_host_is_the_authority() {
        for (listener, version, url) in [
            ("web", "2", "http://shop.example.com/a?q=1"),
            ("secure", "2", "https://shop.example.com/a?q=1"),
            ("quic", "3", "https://shop.example.com/a?q=1"),
        ] {
            let head = head(listener, &asked(version, url)).unwrap();
            assert_eq!(head.uri.to_string(), url);
            assert!(!head.headers.contains_key(HOST));
            assert_eq!(head.headers.len(), 1);
            assert_eq!(head.version, protocol(version).unwrap());
        }
    }

    #[test]
    fn a_request_the_listener_could_not_take_as_it_is_is_refused() {
        assert_eq!(
            head("web", &asked("1.1", "https://shop.example.com/")).err(),
            Some(Invalid::Scheme {
                listener: "web".to_owned(),
                wanted: "http",
                given: "https".to_owned()
            })
        );
        assert!(matches!(
            head("secure", &asked("1.1", "http://shop.example.com/")),
            Err(Invalid::Scheme {
                wanted: "https",
                ..
            })
        ));
        assert_eq!(
            head("secure", &asked("3", "https://shop.example.com/")).err(),
            Some(Invalid::NoHttp3("secure".to_owned()))
        );
    }

    #[test]
    fn a_connect_protocol_is_for_an_extended_connect_alone() {
        let mut connect = asked("2", "http://shop.example.com/chat");
        connect.method = Method::CONNECT;
        connect.connect_protocol = Some("websocket".to_owned());
        let parts = head("web", &connect).unwrap();
        assert_eq!(Head::protocol(&parts), Some("websocket"));

        let mut over_http1 = connect.clone();
        over_http1.protocol = Version::HTTP_11;
        assert_eq!(
            head("web", &over_http1).err(),
            Some(Invalid::ConnectProtocol)
        );
        let mut not_connect = connect;
        not_connect.method = Method::GET;
        assert_eq!(
            head("web", &not_connect).err(),
            Some(Invalid::ConnectProtocol)
        );
    }
}
