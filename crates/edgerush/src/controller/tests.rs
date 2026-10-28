use super::*;
use edgerush_certificates::testing::Authority;
use edgerush_config::{Config, HarnessFile};
use edgerush_control_stream::Staging;
use edgerush_kubernetes::Timing;
use edgerush_kubernetes::testing::{Answer, Fake, answer, token};
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::rc::Rc;
use std::time::Duration;

fn parsed(args: &[&str]) -> Result<Parsed, UsageError> {
    parse(args.iter().map(ToString::to_string))
}

#[test]
fn it_needs_its_data_plane_and_watches_the_cluster_or_each_namespace_given() {
    assert_eq!(
        parsed(&["--data-plane", "p"]),
        Ok(Parsed::Control(Options {
            data_plane: PathBuf::from("p"),
            scope: vec![Scope::Cluster],
            write_config: None,
            stream: None,
            status: None,
            leader: None,
        }))
    );
    assert_eq!(
        parsed(&[
            "--namespace",
            "shop",
            "--data-plane",
            "p",
            "--namespace",
            "blog",
            "--write-config",
            "c"
        ]),
        Ok(Parsed::Control(Options {
            data_plane: PathBuf::from("p"),
            scope: vec![
                Scope::Namespace("shop".to_owned()),
                Scope::Namespace("blog".to_owned())
            ],
            write_config: Some(PathBuf::from("c")),
            stream: None,
            status: None,
            leader: None,
        }))
    );
    assert_eq!(parsed(&["--help"]), Ok(Parsed::Help));
    assert_eq!(
        parsed(&["--namespace", "shop"]),
        Err(UsageError::Required("--data-plane <FILE>"))
    );
    assert_eq!(
        parsed(&["--data-plane", "p", "--data-plane", "q"]),
        Err(UsageError::Twice("--data-plane"))
    );
    assert_eq!(
        parsed(&["--write-config", "c", "--write-config", "d"]),
        Err(UsageError::Twice("--write-config"))
    );
    assert_eq!(
        parsed(&["--namespace", "shop", "--namespace", "shop"]),
        Err(UsageError::NamespaceTwice("shop".to_owned()))
    );
    assert_eq!(
        parsed(&["--namespace", "Shop"]),
        Err(UsageError::Namespace(
            "Shop".to_owned(),
            ApiError::Namespace("Shop".to_owned())
        ))
    );
    assert_eq!(
        parsed(&["--namespace"]),
        Err(UsageError::NoValue("--namespace"))
    );
    assert_eq!(parsed(&["p"]), Err(UsageError::Unexpected("p".to_owned())));
}

/// A file of the tests' own, `name`, holding `contents`.
fn file(name: &str, contents: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("edgerush-controller-tests");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    std::fs::write(&path, contents).unwrap();
    path
}

/// A data plane of class `edge`, with an `http` listener at `http` and an `https` one at
/// `https`.
fn plane_at(http: &str, https: &str) -> String {
    format!(
        r#"
ingress_class: edge
cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: false
ingress_https_redirect_max_age: 30
listeners:
  http:
    address: "{http}"
    protocol: http
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
  https:
    address: "{https}"
    protocol: https
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
    tls: {{ certificates: [], without_sni: none }}
"#
    )
}

/// Outside a cluster, there is no API server to follow the cluster through, and it says so.
#[test]
fn outside_a_cluster_it_does_not_start() {
    let plane = file("outside-plane.yaml", &plane_at("[::]:8080", "[::]:8443"));
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let status = command(
        ["--data-plane", plane.to_str().unwrap()]
            .into_iter()
            .map(ToOwned::to_owned),
        &mut stdout,
        &mut stderr,
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert_eq!(status, crate::EXIT_USAGE, "{stderr}");
    assert!(
        stderr.starts_with("error: the API server cannot be reached as a pod reaches it: "),
        "{stderr}"
    );
}

#[test]
fn settings_that_cannot_be_read_stop_it_before_it_looks_for_a_cluster() {
    let plane = file("unreadable-plane.yaml", "ingress_class: [");
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let status = command(
        ["--data-plane", plane.to_str().unwrap()]
            .into_iter()
            .map(ToOwned::to_owned),
        &mut stdout,
        &mut stderr,
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert_eq!(status, crate::EXIT_USAGE, "{stderr}");
    assert!(stderr.contains("is not what was expected"), "{stderr}");
}

/// A fake API server of IngressClass `edge`, Ingress `shop/web` on `shop.example`,
/// Service `shop/web` and no Secrets, each kind's watch open and quiet.
async fn cluster() -> Fake {
    Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            return Answer {
                status: 200,
                chunks: vec!["\n".into()],
                pause: Duration::from_secs(60),
            };
        }
        let list = |kind: &str, version: &str, item: &str| {
            answer(
                200,
                &format!(
                    r#"{{"apiVersion":"{version}","kind":"{kind}List","metadata":{{"resourceVersion":"5"}},"items":[{item}]}}"#
                ),
            )
        };
        match path.split('?').next().unwrap_or_default() {
            "/apis/networking.k8s.io/v1/ingressclasses" => list(
                "IngressClass",
                "networking.k8s.io/v1",
                r#"{"metadata":{"name":"edge"},"spec":{"controller":"edgerush.dev/ingress-controller"}}"#,
            ),
            "/apis/networking.k8s.io/v1/ingresses" => list(
                "Ingress",
                "networking.k8s.io/v1",
                r#"{"metadata":{"name":"web","namespace":"shop"},"spec":{"ingressClassName":"edge","rules":[{"host":"shop.example","http":{"paths":[{"path":"/","pathType":"Prefix","backend":{"service":{"name":"web","port":{"number":80}}}}]}}]}}"#,
            ),
            "/api/v1/services" => list(
                "Service",
                "v1",
                r#"{"metadata":{"name":"web","namespace":"shop"},"spec":{"ports":[{"name":"http","port":80}]}}"#,
            ),
            "/api/v1/secrets" => list("Secret", "v1", ""),
            _ => answer(500, ""),
        }
    })
    .await
}

/// Runs the control plane against `fake` for `plane` until `done` holds of what it said,
/// or fails the test. What it said, a line each.
async fn run_until(
    fake: &Fake,
    name: &str,
    plane: &str,
    write_config: Option<PathBuf>,
    serving: Option<Serving>,
    done: impl Fn(&[String]) -> bool,
) -> Vec<String> {
    let api = fake.client(&token(name, "secret"), Timing::default());
    let plane: Plane = serde_saphyr::from_str(plane).unwrap();
    let lines: Rc<RefCell<Vec<String>>> = Rc::default();
    let heard = Rc::clone(&lines);
    let options = Options {
        data_plane: PathBuf::new(),
        scope: vec![Scope::Cluster],
        write_config,
        stream: None,
        status: None,
        leader: None,
    };
    let stop = {
        let lines = Rc::clone(&lines);
        async move {
            tokio::time::timeout(Duration::from_secs(10), async {
                while !done(&lines.borrow()) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap_or_else(|_| panic!("never done: {:?}", lines.borrow()));
            Ok(())
        }
    };
    tokio::task::LocalSet::new()
        .run_until(control_until(
            api,
            options,
            plane,
            serving,
            None,
            stop,
            move |line| {
                heard
                    .borrow_mut()
                    .extend(line.lines().map(ToOwned::to_owned));
            },
        ))
        .await
        .unwrap();
    lines.take()
}

/// What it waits on is said, then each translation, which `--write-config` writes whole as
/// the development harness reads a file.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_translation_is_said_and_written_as_a_harness_file() {
    let fake = cluster().await;
    let written = std::env::temp_dir()
        .join("edgerush-controller-tests")
        .join("written.yaml");
    let _gone = std::fs::remove_file(&written);
    let lines = run_until(
        &fake,
        "controller-writes",
        &plane_at("[::]:8080", "[::]:8443"),
        Some(written.clone()),
        None,
        |lines| lines.iter().any(|line| line.starts_with("written to ")),
    )
    .await;
    assert_eq!(
        lines[0],
        "waiting to have listed ingressclasses in the cluster, ingresses in the cluster, \
         services in the cluster, secrets in the cluster"
    );
    assert!(
        lines.iter().any(|line| line.starts_with(
            "1 routes and 1 upstreams translated; 0 left out; 0 paths without their backend;"
        )),
        "{lines:?}"
    );
    let file: HarnessFile =
        serde_saphyr::from_str(&std::fs::read_to_string(&written).unwrap()).unwrap();
    let config: Config = file.into_config(BTreeMap::new(), None);
    let routes: Vec<&str> = config
        .routes
        .iter()
        .map(|route| route.name.as_str())
        .collect();
    assert_eq!(routes, ["ingress/shop/web/shop.example"]);
    assert_eq!(
        config.upstreams.keys().collect::<Vec<_>>(),
        ["shop/web/http"]
    );
}

/// A config the data plane would refuse is said, with why, and not written.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_config_the_data_plane_would_refuse_is_said_and_not_written() {
    let fake = cluster().await;
    let written = std::env::temp_dir()
        .join("edgerush-controller-tests")
        .join("refused.yaml");
    let _gone = std::fs::remove_file(&written);
    let lines = run_until(
        &fake,
        "controller-refuses",
        &plane_at("[::]:8080", "[::]:8080"),
        Some(written.clone()),
        None,
        |lines| lines.iter().any(|line| line.contains("already taken")),
    )
    .await;
    assert!(
        lines
            .iter()
            .any(|line| line == "the data plane would refuse the config, which is not handed on:"),
        "{lines:?}"
    );
    assert!(!written.exists(), "a refused config written");
}

#[test]
fn the_stream_is_given_whole_or_not_at_all() {
    let stream = [
        "--listen",
        "[::]:9443",
        "--certificate",
        "c",
        "--key",
        "k",
        "--authorities",
        "a",
    ];
    let mut args = vec!["--data-plane", "p"];
    args.extend(stream);
    assert_eq!(
        parsed(&args),
        Ok(Parsed::Control(Options {
            data_plane: PathBuf::from("p"),
            scope: vec![Scope::Cluster],
            write_config: None,
            stream: Some(Box::new(Stream {
                listen: "[::]:9443".parse().unwrap(),
                files: Files {
                    certificate: PathBuf::from("c"),
                    key: PathBuf::from("k"),
                    authorities: PathBuf::from("a"),
                },
            })),
            status: None,
            leader: None,
        }))
    );
    // Each of the four left out.
    for left_out in [0, 2, 4, 6] {
        let mut args = vec!["--data-plane", "p"];
        for (i, pair) in stream.chunks(2).enumerate() {
            if i * 2 != left_out {
                args.extend(pair);
            }
        }
        assert_eq!(parsed(&args), Err(UsageError::Together), "{args:?}");
    }
    assert_eq!(
        parsed(&["--listen", "9443"]),
        Err(UsageError::Listen("9443".to_owned()))
    );
    assert_eq!(
        parsed(&["--listen", "[::]:1", "--listen", "[::]:2"]),
        Err(UsageError::Twice("--listen"))
    );
}

#[test]
fn certificates_that_cannot_be_read_stop_it_before_it_looks_for_a_cluster() {
    let plane = file("stream-plane.yaml", &plane_at("[::]:8080", "[::]:8443"));
    let missing = std::env::temp_dir()
        .join("edgerush-controller-tests")
        .join("missing.crt");
    let missing = missing.to_str().unwrap();
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let status = command(
        [
            "--data-plane",
            plane.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--certificate",
            missing,
            "--key",
            missing,
            "--authorities",
            missing,
        ]
        .into_iter()
        .map(ToOwned::to_owned),
        &mut stdout,
        &mut stderr,
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert_eq!(status, crate::EXIT_USAGE, "{stderr}");
    assert!(
        stderr.starts_with("error: the stream's certificates: ") && stderr.contains("missing.crt"),
        "{stderr}"
    );
    // Files that can be read, but not used.
    let junk = file("junk.crt", "not a certificate");
    let junk = junk.to_str().unwrap();
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let status = command(
        [
            "--data-plane",
            plane.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
            "--certificate",
            junk,
            "--key",
            junk,
            "--authorities",
            junk,
        ]
        .into_iter()
        .map(ToOwned::to_owned),
        &mut stdout,
        &mut stderr,
    );
    let stderr = String::from_utf8(stderr).unwrap();
    assert_eq!(status, crate::EXIT_USAGE, "{stderr}");
    assert!(
        stderr.starts_with("error: the stream's certificates: the certificate: "),
        "{stderr}"
    );
}

/// The stream's files, of `authority`'s certificate for `name`, in `dir`.
fn stream_files(dir: &str, authority: &Authority, name: &str) -> Files {
    let dir = std::env::temp_dir()
        .join("edgerush-controller-tests")
        .join(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let files = Files {
        certificate: dir.join("tls.crt"),
        key: dir.join("tls.key"),
        authorities: dir.join("ca.crt"),
    };
    let issued = authority.issue(&[name]);
    std::fs::write(&files.certificate, issued.chain).unwrap();
    std::fs::write(&files.key, issued.key).unwrap();
    std::fs::write(&files.authorities, authority.pem()).unwrap();
    files
}

/// A data plane that dials the control plane is sent each config translated, and what it
/// says of it is said; certificate files replaced are put to use, or said not to be.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_config_translated_is_sent_to_a_data_plane_and_what_it_says_is_said() {
    let fake = cluster().await;
    let authority = Authority::new("stream");
    let files = stream_files("served", &authority, "control.test");
    let credentials = files.read().await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let serving = Serving {
        listener,
        files: files.clone(),
        credentials,
        reread: Duration::from_millis(20),
        versions: Versions::new(7),
    };
    // The data plane, dialling as one does.
    let (ran, mut running) = tokio::sync::mpsc::unbounded_channel();
    let tls = edgerush_control_service::connector(&Credentials {
        certificate: authority.issue(&["data-plane"]),
        authorities: authority.pem(),
    })
    .unwrap();
    let following = tokio::spawn(async move {
        let mut staging = Staging::new();
        edgerush_control_service::follow(&address, "control.test", &tls, &mut staging, |config| {
            let _ = ran.send(*config);
            std::future::ready(Ok(()))
        })
        .await
    });
    // Files that cannot be used, then another authority's, once the data plane runs its
    // config; and then a data plane that trusts that authority alone, which is served.
    let replacing = files.clone();
    let address = serving.listener.local_addr().unwrap().to_string();
    tokio::spawn(async move {
        let config = running.recv().await.unwrap();
        let routes: Vec<String> = config
            .routes
            .iter()
            .map(|route| route.name.clone())
            .collect();
        assert_eq!(routes, ["ingress/shop/web/shop.example"]);
        std::fs::write(&replacing.key, "not a key").unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let renamed = Authority::new("renamed");
        let issued = renamed.issue(&["control.test"]);
        std::fs::write(&replacing.certificate, issued.chain).unwrap();
        std::fs::write(&replacing.key, issued.key).unwrap();
        std::fs::write(&replacing.authorities, renamed.pem()).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        let tls = edgerush_control_service::connector(&Credentials {
            certificate: renamed.issue(&["data-plane"]),
            authorities: renamed.pem(),
        })
        .unwrap();
        let mut staging = Staging::new();
        edgerush_control_service::follow(&address, "control.test", &tls, &mut staging, |_| {
            std::future::ready(Ok(()))
        })
        .await
    });
    let runs = |lines: &[String]| {
        lines
            .iter()
            .filter(|line| {
                line.starts_with("data plane 127.0.0.1:") && line.contains(": runs config 7.")
            })
            .count()
    };
    let lines = run_until(
        &fake,
        "controller-serves",
        &plane_at("[::]:8080", "[::]:8443"),
        None,
        Some(serving),
        |lines| runs(lines) == 2,
    )
    .await;
    assert!(
        lines
            .iter()
            .any(|line| line == "the stream's certificates are replaced"),
        "{lines:?}"
    );
    let said = |start: &str| lines.iter().any(|line| line.starts_with(start));
    assert!(said("offered as config 7."), "{lines:?}");
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("data plane 127.0.0.1:")
                && line.ends_with(": opened at config version 1, holding no config")),
        "{lines:?}"
    );
    assert!(
        lines
            .iter()
            .any(|line| line.starts_with("data plane 127.0.0.1:")
                && line.contains(": runs config 7.")),
        "{lines:?}"
    );
    assert!(
        said("the stream's certificates cannot be used, and the last are kept: "),
        "{lines:?}"
    );
    // Stopping ends the data plane's stream.
    let ended = tokio::time::timeout(Duration::from_secs(5), following)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        ended,
        edgerush_control_service::Ended::Ended("the control plane is stopping".to_owned())
    );
}

#[test]
fn ingress_statuses_say_the_data_planes_service_or_addresses_stated_never_both() {
    let status = |args: &[&str]| {
        let mut all = vec!["--data-plane", "p"];
        all.extend(args);
        parsed(&all).map(|parsed| match parsed {
            Parsed::Control(options) => options.status,
            Parsed::Help => panic!("help"),
        })
    };
    assert_eq!(status(&[]), Ok(None));
    assert_eq!(
        status(&["--status-service", "edge/edgerush"]),
        Ok(Some(Addresses::Service {
            namespace: "edge".to_owned(),
            name: "edgerush".to_owned(),
        }))
    );
    assert_eq!(
        status(&[
            "--status-address",
            "203.0.113.7",
            "--status-address",
            "lb.example"
        ]),
        Ok(Some(Addresses::Stated(vec![
            address("203.0.113.7"),
            address("lb.example")
        ])))
    );
    assert_eq!(
        status(&[
            "--status-service",
            "edge/edgerush",
            "--status-address",
            "203.0.113.7"
        ]),
        Err(UsageError::Status)
    );
    let leader = |args: &[&str]| {
        let mut all = vec!["--data-plane", "p"];
        all.extend(args);
        parsed(&all).map(|parsed| match parsed {
            Parsed::Control(options) => options.leader,
            Parsed::Help => panic!("help"),
        })
    };
    assert_eq!(leader(&[]), Ok(None));
    assert_eq!(
        leader(&["--leader-lease", "edge/edgerush-control"]),
        Ok(Some(("edge".to_owned(), "edgerush-control".to_owned())))
    );
    assert_eq!(
        leader(&["--leader-lease", "edge"]),
        Err(UsageError::LeaderLease("edge".to_owned()))
    );
    assert_eq!(
        leader(&["--leader-lease", "edge/a", "--leader-lease", "edge/b"]),
        Err(UsageError::Twice("--leader-lease"))
    );
    for named in ["edgerush", "Edge/edgerush", "edge/", "edge/a/b"] {
        assert_eq!(
            status(&["--status-service", named]),
            Err(UsageError::StatusService(named.to_owned())),
            "{named}"
        );
    }
    assert_eq!(
        status(&["--status-service", "edge/a", "--status-service", "edge/b"]),
        Err(UsageError::Twice("--status-service"))
    );
}

#[test]
fn what_is_said_of_a_status_names_the_ingress_and_its_addresses() {
    let ingress = edgerush_translate::Object {
        kind: "Ingress",
        namespace: "shop".to_owned(),
        name: "web".to_owned(),
    };
    let addresses = [address("203.0.113.7"), address("lb.example")];
    let said = |addresses: &[_], written| {
        told(
            &Said::Status {
                ingress: &ingress,
                addresses,
                written,
            },
            None,
        )
    };
    assert_eq!(
        said(&addresses, Ok(())),
        "status of Ingress shop/web: 203.0.113.7, lb.example"
    );
    assert_eq!(said(&[], Ok(())), "status of Ingress shop/web: no address");
    let refused = edgerush_kubernetes::ApiError::Answered {
        code: 403,
        message: "forbidden".to_owned(),
    };
    let not = said(&addresses[..1], Err(&refused));
    assert!(
        not.starts_with("status of Ingress shop/web cannot be written (203.0.113.7): "),
        "{not}"
    );
}
