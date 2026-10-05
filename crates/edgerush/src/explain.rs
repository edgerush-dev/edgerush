//! `edgerush explain`: the command line around `edgerush_explain`
//! ([22 §4](../../../docs/22-explain-and-test.md)) — its flags, reading the config, and
//! printing what the library says.

use crate::config_file::{self, Rejected};
use edgerush_config::Protocol;
use edgerush_explain::{Asked, Invalid, Unexplained, asked};
use std::io::Write;
use std::path::PathBuf;

pub(crate) const USAGE: &str = "\
Usage: edgerush explain --config <FILE> --listener <NAME> [REQUEST]

Says where a request to a listener goes and why every other match did not take it, as the
data plane would decide it, without running one. The config is read as the harness reads
it; the files of its certificates are not.

Request, every part of it required for an http or https listener:
      --client <ADDRESS>         The IP address it came from
      --protocol <VERSION>       The HTTP version it came by: 1.0, 1.1, 2 or 3
      --method <METHOD>          Its method, as sent
      --url <URL>                Its scheme (the listener's), host and port, path and query
      --header <'NAME: VALUE'>   A field line, in order: once for each, or not at all
      --connect-protocol <NAME>  An extended CONNECT's protocol, over HTTP/2 or HTTP/3

Connection to a tls listener, one of the two (a tcp listener's takes nothing):
      --sni <NAME>               The name its ClientHello asks for
      --no-sni                   A ClientHello that asks for no name

Options:
      --config <FILE>            The config, in YAML
      --listener <NAME>          The listener the request comes to
  -h, --help                     Print help
";

/// Runs `edgerush explain` with the arguments after its name. Returns the exit status: 0
/// for a request explained, whatever became of it; 2 for one that could not be.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match parse(args) {
        Ok(Parsed::Help) => write!(stdout, "{USAGE}").map(|()| 0),
        Ok(Parsed::Explain(options)) => match run(&options) {
            Ok(text) => write!(stdout, "{text}").map(|()| 0),
            Err(failure) => writeln!(stderr, "error: {failure}").map(|()| crate::EXIT_USAGE),
        },
        Err(error) => write!(stderr, "error: {error}\n\n{USAGE}").map(|()| crate::EXIT_USAGE),
    };
    written.unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    /// Boxed: much the larger of the two, and passed by value.
    Explain(Box<Options>),
}

/// The command line, as given.
#[derive(Debug, Default, PartialEq, Eq)]
struct Options {
    config: PathBuf,
    listener: String,
    client: Option<String>,
    protocol: Option<String>,
    method: Option<String>,
    url: Option<String>,
    headers: Vec<String>,
    connect_protocol: Option<String>,
    sni: Option<String>,
    no_sni: bool,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'{0}' is required")]
    Required(&'static str),
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut config = None;
    let mut listener = None;
    let mut options = Options::default();
    while let Some(arg) = args.next() {
        let flag: &'static str = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--no-sni" => {
                if std::mem::replace(&mut options.no_sni, true) {
                    return Err(UsageError::Twice("--no-sni"));
                }
                continue;
            }
            "--config" => "--config",
            "--listener" => "--listener",
            "--client" => "--client",
            "--protocol" => "--protocol",
            "--method" => "--method",
            "--url" => "--url",
            "--header" => "--header",
            "--connect-protocol" => "--connect-protocol",
            "--sni" => "--sni",
            _ => return Err(UsageError::Unexpected(arg)),
        };
        let value = args.next().ok_or(UsageError::NoValue(flag))?;
        let once = match flag {
            "--config" => &mut config,
            "--listener" => &mut listener,
            "--client" => &mut options.client,
            "--protocol" => &mut options.protocol,
            "--method" => &mut options.method,
            "--url" => &mut options.url,
            "--connect-protocol" => &mut options.connect_protocol,
            "--sni" => &mut options.sni,
            _ => {
                options.headers.push(value);
                continue;
            }
        };
        if once.replace(value).is_some() {
            return Err(UsageError::Twice(flag));
        }
    }
    options.config = config
        .map(PathBuf::from)
        .ok_or(UsageError::Required("--config <FILE>"))?;
    options.listener = listener.ok_or(UsageError::Required("--listener <NAME>"))?;
    Ok(Parsed::Explain(Box::new(options)))
}

/// Why a request could not be explained.
#[derive(Debug, thiserror::Error)]
enum Failed {
    #[error("config {} cannot be read:\n{rejected}", path.display())]
    Config { path: PathBuf, rejected: Rejected },
    #[error("there is no listener {0}")]
    NoListener(String),
    #[error("'{0}' is required for {1} listener")]
    Required(&'static str, &'static str),
    #[error("'{0}' does not apply to {1} listener")]
    Unwanted(&'static str, &'static str),
    #[error("'--sni' and '--no-sni' are given together: a ClientHello asks for a name or none")]
    SniTwice,
    #[error(transparent)]
    Invalid(#[from] Invalid),
    #[error(transparent)]
    Unexplained(#[from] Unexplained),
}

fn run(options: &Options) -> Result<String, Failed> {
    let snapshot = config_file::offline(&options.config).map_err(|rejected| Failed::Config {
        path: options.config.clone(),
        rejected,
    })?;
    let listener = snapshot
        .listener(&options.listener)
        .ok_or_else(|| Failed::NoListener(options.listener.clone()))?;
    // The listener's kind, as the errors below name it.
    let kind = match listener.protocol {
        Protocol::Http => "an http",
        Protocol::Https => "an https",
        Protocol::Tcp => "a tcp",
        Protocol::Tls => "a tls",
    };
    // What the listener's kind takes, and nothing else.
    let http = [
        ("--client", options.client.is_some()),
        ("--protocol", options.protocol.is_some()),
        ("--method", options.method.is_some()),
        ("--url", options.url.is_some()),
        ("--header", !options.headers.is_empty()),
        ("--connect-protocol", options.connect_protocol.is_some()),
    ];
    let sni = [
        ("--sni", options.sni.is_some()),
        ("--no-sni", options.no_sni),
    ];
    let unwanted = |given: &[(&'static str, bool)]| {
        given
            .iter()
            .find(|(_, given)| *given)
            .map_or(Ok(()), |(flag, _)| Err(Failed::Unwanted(flag, kind)))
    };
    match listener.protocol {
        Protocol::Http | Protocol::Https => unwanted(&sni)?,
        Protocol::Tcp => {
            unwanted(&http)?;
            unwanted(&sni)?;
            return Ok(snapshot.explain_connection(listener, None)?.text());
        }
        Protocol::Tls => {
            unwanted(&http)?;
            let name = match (&options.sni, options.no_sni) {
                (Some(name), false) => Some(name.as_str()),
                (None, true) => None,
                (Some(_), true) => return Err(Failed::SniTwice),
                (None, false) => return Err(Failed::Required("--sni <NAME>' or '--no-sni", kind)),
            };
            return Ok(snapshot.explain_connection(listener, name)?.text());
        }
    }
    let required = |given: &Option<String>, flag| given.clone().ok_or(Failed::Required(flag, kind));
    let (scheme, authority, target) = asked::url(&required(&options.url, "--url")?)?;
    let client = required(&options.client, "--client")?;
    let asked = Asked {
        client: client.parse().map_err(|_| Invalid::Client(client))?,
        protocol: asked::protocol(&required(&options.protocol, "--protocol")?)?,
        method: asked::method(&required(&options.method, "--method")?)?,
        scheme,
        authority,
        target,
        headers: options
            .headers
            .iter()
            .map(|line| asked::header_line(line))
            .collect::<Result<_, _>>()?,
        connect_protocol: options.connect_protocol.clone(),
    };
    Ok(snapshot.explain(listener, &asked)?.text())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// Every kind of verdict and outcome: precedence by host, path and method; a header
    /// against a regex; a query decoded; a hostname that does not fall through; a redirect,
    /// a rewrite, a mirror, timeouts and a retry; a WebSocket; a rule with nowhere to send;
    /// a listener that passes request IDs on; and a `tcp` listener.
    const CONFIG: &str = r#"listeners:
  web: { address: "[::]:8080", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [Forwarded, X-Real-IP, "X-Forwarded-*"] }, request_id: generate }
  passing: { address: "[::]:8081", protocol: http, proxy_protocol: off, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: pass }
  db: { address: "[::]:5432", protocol: tcp, proxy_protocol: off }
routes:
  - name: shop
    listeners: [web]
    hostnames:
      - { name: shop.example.com, falls_through: true }
    rules:
      - matches:
          - path: { prefix: /cart }
        filters:
          - type: request_header_modifier
            set: [{ name: X-Gateway, value: edgerush }]
            remove: [x-debug]
          - { type: request_mirror, upstream: shadow, fraction: { numerator: 1, denominator: 10 } }
        forward:
          backends:
            - { upstream: cart, weight: 9 }
            - { upstream: cart-canary, weight: 1 }
          timeouts: { request_ms: 5000, backend_request_ms: 1000 }
          retry: { attempts: 2, http_statuses: [502, 503], on_timeout: true, backoff_base_ms: 25, backoff_max_ms: 250 }
      - matches:
          - path: { exact: /closed }
        redirect: { status: 301, path: { replace_full: /open }, query: keep }
      - matches:
          - path: { prefix: /search }
            query: [{ name: q, value: { exact: "a b" } }]
        forward: { backends: [{ upstream: api, weight: 1 }] }
      - matches:
          - path: { regex: "/v[0-9]+/.*" }
            headers: [{ name: X-Env, value: { regex: "canary|beta" } }]
          - path: { prefix: /admin }
            method: POST
        filters:
          - { type: url_rewrite, host: api.internal, path: { replace_full: /api } }
        forward: { backends: [{ upstream: api, weight: 1 }] }
      - matches:
          - path: { prefix: /chat }
        forward: { backends: [{ upstream: chat, weight: 1 }] }
  - name: wild
    listeners: [web]
    hostnames: [{ name: "*.example.com", wildcard: any_labels, falls_through: false }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: fallback, weight: 1 }] }
  - name: passed
    listeners: [passing]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: / } }]
        forward: { backends: [{ upstream: fallback, weight: 1 }] }
  - name: everything-else
    listeners: [web]
    hostnames: [{ name: "*", falls_through: true }]
    rules:
      - matches: [{ path: { prefix: /static } }]
        forward: { backends: [{ upstream: fallback, weight: 0 }] }
upstreams:
  api: { load_balancer: p2c, endpoints: [] }
  cart: { load_balancer: p2c, endpoints: [] }
  cart-canary: { load_balancer: p2c, endpoints: [] }
  chat: { load_balancer: p2c, endpoints: [] }
  fallback: { load_balancer: p2c, endpoints: [] }
  shadow: { load_balancer: p2c, endpoints: [] }
tcp_routes:
  - { name: postgres, listeners: [db], backends: [{ upstream: fallback, weight: 1 }] }
"#;

    /// A file of the test's own in the system's temporary directory, gone with the test.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(test: &str, content: &str) -> Self {
            let name = format!("edgerush-{}-explain-{test}.yaml", std::process::id());
            let path = std::env::temp_dir().join(name);
            fs::write(&path, content).unwrap();
            Self(path)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _gone_already = fs::remove_file(&self.0);
        }
    }

    /// Runs the command on `config`, saved under the test's name, with `args` after
    /// `--config`; its exit status, stdout and stderr.
    fn explain_on(config: &str, test: &str, args: &[&str]) -> (u8, String, String) {
        let file = Scratch::new(test, config);
        let path = file.0.to_str().unwrap().to_owned();
        let all = ["--config", path.as_str()]
            .into_iter()
            .chain(args.iter().copied());
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(all.map(str::to_owned), &mut stdout, &mut stderr);
        (
            status,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    /// The flags of a request from 203.0.113.7 to `listener`.
    fn request<'a>(
        listener: &'a str,
        protocol: &'a str,
        method: &'a str,
        url: &'a str,
    ) -> Vec<&'a str> {
        vec![
            "--listener",
            listener,
            "--client",
            "203.0.113.7",
            "--protocol",
            protocol,
            "--method",
            method,
            "--url",
            url,
        ]
    }

    #[test]
    fn a_request_explained_exits_0_with_its_text() {
        let args = request("web", "1.1", "GET", "http://shop.example.com/closed");
        let (status, text, stderr) = explain_on(CONFIG, "explained", &args);
        assert_eq!((status, stderr.as_str()), (0, ""));
        assert!(
            text.starts_with(
                "web (http)  GET http://shop.example.com/closed  HTTP/1.1  from 203.0.113.7

"
            ),
            "{text}"
        );
        assert!(
            text.ends_with(
                "
redirect  301 to /open
"
            ),
            "{text}"
        );
    }

    #[test]
    fn certificate_files_are_never_opened() {
        let secure = "  secure: { address: \"[::]:8443\", protocol: https, proxy_protocol: off, tls: { certificates: [site] }, forwarding: { trusted_proxies: [], trusted_only_headers: [] }, request_id: generate }\n  db: {";
        let mut https = CONFIG.replace("  db: {", secure);
        https.push_str("certificates:\n  site: { chain_file: /nowhere/site.pem, key_file: /nowhere/site.key }\n");
        let args = request("secure", "1.1", "GET", "https://shop.example.com/");
        let (status, text, stderr) = explain_on(&https, "certificates", &args);
        assert_eq!((status, stderr.as_str()), (0, ""));
        assert!(
            text.starts_with("secure (https)  GET https://shop.example.com/"),
            "{text}"
        );
    }

    #[test]
    fn what_cannot_be_explained_exits_2_and_says_why() {
        let failed = |test: &str, config: &str, args: &[&str]| {
            let (status, stdout, stderr) = explain_on(config, test, args);
            assert_eq!(status, 2, "{test}: {stdout}");
            assert!(stdout.is_empty(), "{test}");
            stderr
        };
        let url = "http://shop.example.com/";
        assert_eq!(
            failed(
                "no_listener",
                CONFIG,
                &request("nowhere", "1.1", "GET", url)
            ),
            "error: there is no listener nowhere\n"
        );
        // A tcp listener takes a connection and nothing of a request.
        assert_eq!(
            failed("tcp", CONFIG, &request("db", "1.1", "GET", url)),
            "error: '--client' does not apply to a tcp listener\n"
        );
        let mut no_client = request("web", "1.1", "GET", url);
        no_client.drain(2..4);
        assert_eq!(
            failed("no_client", CONFIG, &no_client),
            "error: '--client' is required for an http listener\n"
        );
        assert_eq!(
            failed(
                "bad_url",
                CONFIG,
                &request("web", "1.1", "GET", "shop.example.com/")
            ),
            "error: 'shop.example.com/' is not a URL with a scheme, a host and a path\n"
        );
        let mut host = request("web", "1.1", "GET", url);
        host.extend(["--header", "Host: elsewhere"]);
        assert_eq!(
            failed("host", CONFIG, &host),
            "error: Host is the URL's to give: write it in the URL\n"
        );
        assert_eq!(
            failed("http3", CONFIG, &request("web", "3", "GET", url)),
            "error: listener web does not serve HTTP/3\n"
        );
        let invalid = failed(
            "invalid",
            "listeners: {}\n",
            &request("web", "1.1", "GET", url),
        );
        assert!(invalid.starts_with("error: config "), "{invalid}");
        assert!(invalid.contains("cannot be read"), "{invalid}");
    }

    #[test]
    fn a_config_file_that_is_not_there_exits_2() {
        let missing = std::env::temp_dir().join("edgerush-explain-no-such-file.yaml");
        let args = ["--config", missing.to_str().unwrap(), "--listener", "web"];
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(
            args.iter().map(ToString::to_string),
            &mut stdout,
            &mut stderr,
        );
        assert_eq!(status, 2);
        assert!(stdout.is_empty());
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .contains("cannot be read")
        );
    }

    #[test]
    fn the_command_line_is_read_flag_by_flag() {
        let parsed = |args: &[&str]| parse(args.iter().map(ToString::to_string));
        assert_eq!(parsed(&["--listener", "web", "-h"]), Ok(Parsed::Help));
        assert_eq!(
            parsed(&["--listener", "web"]),
            Err(UsageError::Required("--config <FILE>"))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml"]),
            Err(UsageError::Required("--listener <NAME>"))
        );
        assert_eq!(
            parsed(&["--config", "a.yaml", "--config", "b.yaml"]),
            Err(UsageError::Twice("--config"))
        );
        assert_eq!(parsed(&["--url"]), Err(UsageError::NoValue("--url")));
        assert_eq!(
            parsed(&["--cookie", "a=1"]),
            Err(UsageError::Unexpected("--cookie".to_owned()))
        );
        let headers = [
            "--config",
            "a.yaml",
            "--header",
            "A: 1",
            "--listener",
            "web",
            "--header",
            "B: 2",
            "--header",
            "A: 3",
        ];
        let Ok(Parsed::Explain(options)) = parsed(&headers) else {
            panic!("parsed");
        };
        assert_eq!(options.headers, ["A: 1", "B: 2", "A: 3"]);
        assert_eq!(options.listener, "web");
    }

    #[test]
    fn a_tls_listener_takes_a_name_or_none_and_nothing_else() {
        let tls = CONFIG.replace(
            "  db: {",
            "  sni: { address: \"[::]:443\", protocol: tls, proxy_protocol: off }\n  db: {",
        ) + "tls_routes:\n  - { name: api, listeners: [sni], hostnames: [{ name: api.example.com, falls_through: true }], backends: [{ upstream: api, weight: 1 }] }\n";
        let run = |test: &str, args: &[&str]| explain_on(&tls, test, args);
        let (status, text, _) = run(
            "tls_named",
            &["--listener", "sni", "--sni", "api.example.com"],
        );
        assert_eq!(status, 0);
        assert!(
            text.starts_with("sni (tls)  SNI api.example.com\n\n→ api  chosen\n"),
            "{text}"
        );
        let (status, text, _) = run("tls_unnamed", &["--listener", "sni", "--no-sni"]);
        assert_eq!(status, 0);
        assert!(
            text.starts_with("sni (tls)  no SNI\n\n  a ClientHello that asks for no name"),
            "{text}"
        );
        assert!(text.ends_with("\nrefused   no_route\n"), "{text}");
        let (status, text, _) = run("tls_tcp", &["--listener", "db"]);
        assert_eq!(status, 0);
        assert!(text.starts_with("db (tcp)\n\n→ "), "{text}");

        let failed = |test: &str, args: &[&str]| {
            let (status, stdout, stderr) = run(test, args);
            assert_eq!((status, stdout.as_str()), (2, ""), "{test}");
            stderr
        };
        assert_eq!(
            failed("tls_neither", &["--listener", "sni"]),
            "error: '--sni <NAME>' or '--no-sni' is required for a tls listener\n"
        );
        assert_eq!(
            failed(
                "tls_both",
                &["--listener", "sni", "--sni", "a.test", "--no-sni"]
            ),
            "error: '--sni' and '--no-sni' are given together: a ClientHello asks for a name or none\n"
        );
        assert_eq!(
            failed(
                "tls_http_flag",
                &["--listener", "sni", "--no-sni", "--method", "GET"]
            ),
            "error: '--method' does not apply to a tls listener\n"
        );
        assert_eq!(
            failed("tls_sni_on_tcp", &["--listener", "db", "--sni", "a.test"]),
            "error: '--sni' does not apply to a tcp listener\n"
        );
        let mut http = request("web", "1.1", "GET", "http://shop.example.com/");
        http.push("--no-sni");
        assert_eq!(
            failed("tls_sni_on_http", &http),
            "error: '--no-sni' does not apply to an http listener\n"
        );
        let parsed = |args: &[&str]| parse(args.iter().map(ToString::to_string));
        assert_eq!(
            parsed(&["--no-sni", "--no-sni"]),
            Err(UsageError::Twice("--no-sni"))
        );
        assert_eq!(parsed(&["--sni"]), Err(UsageError::NoValue("--sni")));
    }

    #[test]
    fn help_is_the_usage_on_stdout() {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(["--help".to_owned()].into_iter(), &mut stdout, &mut stderr);
        assert_eq!(
            (status, String::from_utf8(stdout).unwrap()),
            (0, USAGE.to_owned())
        );
        assert!(stderr.is_empty());
    }
}
