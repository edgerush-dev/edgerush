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
        Ok(Parsed::Control(Box::new(Options {
            data_plane: PathBuf::from("p"),
            scope: vec![Scope::Cluster],
            write_config: None,
            stream: None,
            own_names: Vec::new(),
            status: None,
            leader: None,
            webhook: None,
        })))
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
        Ok(Parsed::Control(Box::new(Options {
            data_plane: PathBuf::from("p"),
            scope: vec![
                Scope::Namespace("shop".to_owned()),
                Scope::Namespace("blog".to_owned())
            ],
            write_config: Some(PathBuf::from("c")),
            stream: None,
            own_names: Vec::new(),
            status: None,
            leader: None,
            webhook: None,
        })))
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
    listeners: Listeners,
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
        own_names: Vec::new(),
        status: None,
        leader: None,
        webhook: None,
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
            listeners,
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
        Listeners {
            stream: None,
            webhook: None,
        },
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
        Listeners {
            stream: None,
            webhook: None,
        },
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
        Ok(Parsed::Control(Box::new(Options {
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
            own_names: Vec::new(),
            status: None,
            leader: None,
            webhook: None,
        })))
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
fn own_names_go_with_the_stream_each_once_and_each_a_name() {
    let stream = [
        "--data-plane",
        "p",
        "--listen",
        "[::]:9443",
        "--certificate",
        "c",
        "--key",
        "k",
        "--authorities",
        "a",
    ];
    let with = |names: &[&str]| {
        let mut args = stream.to_vec();
        args.extend(names);
        parsed(&args)
    };
    let Ok(Parsed::Control(options)) = with(&[
        "--own-name",
        "Peers.Edge.svc.cluster.local.",
        "--own-name",
        "edgerush-control.edge.svc.cluster.local",
    ]) else {
        panic!("not parsed")
    };
    assert_eq!(
        options.own_names,
        [
            "peers.edge.svc.cluster.local",
            "edgerush-control.edge.svc.cluster.local"
        ]
    );
    assert_eq!(
        with(&["--own-name", "a.b", "--own-name", "A.B."]),
        Err(UsageError::OwnNameTwice("A.B.".to_owned()))
    );
    assert!(matches!(
        with(&["--own-name", "10.0.0.1"]),
        Err(UsageError::OwnName(name, _)) if name == "10.0.0.1"
    ));
    assert_eq!(
        parsed(&["--data-plane", "p", "--own-name", "peers.edge"]),
        Err(UsageError::OwnNameWithoutStream)
    );
}

#[test]
fn the_webhook_is_given_whole_or_not_at_all() {
    let webhook = [
        "--webhook-listen",
        "[::]:8443",
        "--webhook-certificate",
        "c",
        "--webhook-key",
        "k",
    ];
    let mut args = vec!["--data-plane", "p"];
    args.extend(webhook);
    assert_eq!(
        parsed(&args),
        Ok(Parsed::Control(Box::new(Options {
            data_plane: PathBuf::from("p"),
            scope: vec![Scope::Cluster],
            write_config: None,
            stream: None,
            own_names: Vec::new(),
            status: None,
            leader: None,
            webhook: Some(Box::new(Webhook {
                listen: "[::]:8443".parse().unwrap(),
                files: CertificateFiles {
                    certificate: PathBuf::from("c"),
                    key: PathBuf::from("k"),
                },
            })),
        })))
    );
    // Each of the three left out.
    for left_out in [0, 2, 4] {
        let mut args = vec!["--data-plane", "p"];
        for (i, pair) in webhook.chunks(2).enumerate() {
            if i * 2 != left_out {
                args.extend(pair);
            }
        }
        assert_eq!(parsed(&args), Err(UsageError::WebhookTogether), "{args:?}");
    }
    assert_eq!(
        parsed(&["--webhook-listen", "8443"]),
        Err(UsageError::WebhookListen("8443".to_owned()))
    );
    for twice in [
        ["--webhook-listen", "[::]:1", "--webhook-listen", "[::]:2"],
        ["--webhook-certificate", "a", "--webhook-certificate", "b"],
        ["--webhook-key", "a", "--webhook-key", "b"],
    ] {
        assert_eq!(
            parsed(&twice),
            Err(UsageError::Twice(twice[0])),
            "{twice:?}"
        );
    }
}

#[test]
fn a_webhook_certificate_that_cannot_be_used_stops_it_before_it_looks_for_a_cluster() {
    let plane = file("webhook-plane.yaml", &plane_at("[::]:8080", "[::]:8443"));
    let missing = std::env::temp_dir()
        .join("edgerush-controller-tests")
        .join("missing-webhook.crt");
    let junk = file("junk-webhook.crt", "not a certificate");
    for (path, said) in [
        (missing.as_path(), "missing-webhook.crt"),
        (junk.as_path(), "the certificate: "),
    ] {
        let path = path.to_str().unwrap();
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(
            [
                "--data-plane",
                plane.to_str().unwrap(),
                "--webhook-listen",
                "127.0.0.1:0",
                "--webhook-certificate",
                path,
                "--webhook-key",
                path,
            ]
            .into_iter()
            .map(ToOwned::to_owned),
            &mut stdout,
            &mut stderr,
        );
        let stderr = String::from_utf8(stderr).unwrap();
        assert_eq!(status, crate::EXIT_USAGE, "{stderr}");
        assert!(
            stderr.starts_with("error: the webhook's certificate: ") && stderr.contains(said),
            "{stderr}"
        );
    }
}

/// A review of an Ingress, of class `edge` and annotated as `annotations` says.
fn review_of(uid: &str, annotations: &str) -> Vec<u8> {
    format!(
        r#"{{"apiVersion":"admission.k8s.io/v1","kind":"AdmissionReview","request":{{"uid":"{uid}","kind":{{"group":"networking.k8s.io","version":"v1","kind":"Ingress"}},"operation":"CREATE","object":{{"metadata":{{"name":"web","namespace":"shop","annotations":{annotations}}},"spec":{{"ingressClassName":"edge","rules":[{{"host":"shop.example","http":{{"paths":[{{"path":"/","pathType":"Prefix","backend":{{"service":{{"name":"web","port":{{"number":80}}}}}}}}]}}}}]}}}}}}}}"#
    )
    .into_bytes()
}

fn classes(listed: bool) -> Listed<IngressClass> {
    Listed {
        listed,
        objects: vec![
            serde_json::from_str(
                r#"{"metadata":{"name":"edge"},"spec":{"controller":"edgerush.dev/ingress-controller"}}"#,
            )
            .unwrap(),
        ],
    }
}

#[test]
fn a_review_is_answered_once_the_classes_are_listed_and_what_is_none_is_a_bad_request() {
    let plane: Plane = serde_saphyr::from_str(&plane_at("[::]:8080", "[::]:8443")).unwrap();
    let answered = |listed: bool, body: &[u8]| {
        let answer = review(&plane, &classes(listed), body);
        (answer.status, String::from_utf8(answer.body).unwrap())
    };
    let (status, body) = answered(false, &review_of("u-1", "{}"));
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body, "the IngressClasses are not listed yet");
    let (status, body) = answered(true, &review_of("u-2", "{}"));
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["response"]["uid"], "u-2");
    assert_eq!(body["response"]["allowed"], true);
    let (status, body) = answered(true, &review_of("u-3", r#"{"x":"y"}"#));
    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(body["response"]["allowed"], false);
    assert_eq!(
        body["response"]["status"]["message"],
        "not supported yet: annotations"
    );
    let (status, body) = answered(true, b"not a review");
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(body.starts_with("not an AdmissionReview"), "{body}");
}

/// The webhook's files, of `authority`'s certificate for `name`, in `dir`, each put in
/// place whole, as Kubernetes swaps a mounted Secret's.
fn webhook_files(dir: &str, authority: &Authority, name: &str) -> CertificateFiles {
    let dir = std::env::temp_dir()
        .join("edgerush-controller-tests")
        .join(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let files = CertificateFiles {
        certificate: dir.join("tls.crt"),
        key: dir.join("tls.key"),
    };
    put_webhook_files(&files, authority, name);
    files
}

fn put_webhook_files(files: &CertificateFiles, authority: &Authority, name: &str) {
    let put = |path: &Path, contents: &str| {
        let beside = path.with_extension("partial");
        std::fs::write(&beside, contents).unwrap();
        std::fs::rename(&beside, path).unwrap();
    };
    let issued = authority.issue(&[name]);
    put(&files.key, &issued.key);
    put(&files.certificate, &issued.chain);
}

/// What the webhook at `address`, named `name`, answers `body` POSTed to it over HTTP/1.1,
/// `authority` trusted: its status line and its body.
async fn posted(address: &str, authority: &Authority, name: &str, body: &[u8]) -> (String, String) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut builder = boring::ssl::SslConnector::builder(boring::ssl::SslMethod::tls()).unwrap();
    let mut trusted = boring::x509::store::X509StoreBuilder::new().unwrap();
    for certificate in boring::x509::X509::stack_from_pem(authority.pem().as_bytes()).unwrap() {
        trusted.add_cert(certificate).unwrap();
    }
    builder.set_verify_cert_store(trusted.build()).unwrap();
    let connector = builder.build();
    let socket = tokio::net::TcpStream::connect(address).await.unwrap();
    let mut stream = tokio_boring::connect(connector.configure().unwrap(), name, socket)
        .await
        .unwrap();
    let mut request = format!(
        "POST /validate HTTP/1.1\r\nhost: {name}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    )
    .into_bytes();
    request.extend_from_slice(body);
    stream.write_all(&request).await.unwrap();
    let mut read = Vec::new();
    let mut buffer = [0; 4096];
    while let Ok(count @ 1..) = stream.read(&mut buffer).await {
        read.extend_from_slice(&buffer[..count]);
    }
    let read = String::from_utf8(read).unwrap();
    let (head, body) = read.split_once("\r\n\r\n").unwrap();
    (head.lines().next().unwrap().to_owned(), body.to_owned())
}

/// The webhook answers each review as the IngressClasses it follows say, refusing an
/// Ingress of the class that does not translate, on a thread of its own: a review is
/// answered while the control plane's own thread is held up. Its certificate files
/// replaced are put to use for the next connection. On one thread, as `edgerush controller`
/// runs, the reviews asked from another.
#[tokio::test(flavor = "current_thread")]
async fn the_webhook_answers_reviews_and_its_certificate_replaced_serves_the_next() {
    let fake = cluster().await;
    let authority = Authority::new("webhook");
    let files = webhook_files("webhook", &authority, "webhook.test");
    let certificate = files.read().await.unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let hooking = Hooking {
        api: fake.client(
            &token("controller-webhook-follows", "secret"),
            Timing::default(),
        ),
        listener,
        files: files.clone(),
        certificate,
        reread: Duration::from_millis(20),
    };
    // The control plane runs until the test is done with it, and holds up its own thread
    // once when asked.
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let finished = Arc::clone(&done);
    let hold = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let holding = Arc::clone(&hold);
    let reviewer = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let reviewing = std::thread::spawn(move || {
        reviewer.block_on(async move {
            // Until its IngressClasses are listed, it has nothing to judge by.
            let admitted = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let (status, body) = posted(
                        &address,
                        &authority,
                        "webhook.test",
                        &review_of("u-1", "{}"),
                    )
                    .await;
                    if status != "HTTP/1.1 503 Service Unavailable" {
                        return (status, body);
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("never answered but 503");
            let refused = posted(
                &address,
                &authority,
                "webhook.test",
                &review_of("u-2", r#"{"x":"y"}"#),
            )
            .await;
            // The control plane's thread held up for 1.5 s: a review is answered all the same.
            holding.store(true, std::sync::atomic::Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(200)).await;
            let asked = std::time::Instant::now();
            let (status, _) = posted(
                &address,
                &authority,
                "webhook.test",
                &review_of("u-3", "{}"),
            )
            .await;
            let answered_in = asked.elapsed();
            assert_eq!(status, "HTTP/1.1 200 OK");
            assert!(
                answered_in < Duration::from_millis(800),
                "answered in {answered_in:?}, the control plane's thread held up"
            );
            put_webhook_files(&files, &authority, "renamed.test");
            let renamed = tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let socket = tokio::net::TcpStream::connect(&address).await.unwrap();
                    let mut builder =
                        boring::ssl::SslConnector::builder(boring::ssl::SslMethod::tls()).unwrap();
                    let mut trusted = boring::x509::store::X509StoreBuilder::new().unwrap();
                    for certificate in
                        boring::x509::X509::stack_from_pem(authority.pem().as_bytes()).unwrap()
                    {
                        trusted.add_cert(certificate).unwrap();
                    }
                    builder.set_verify_cert_store(trusted.build()).unwrap();
                    let connector = builder.build();
                    let connected = tokio_boring::connect(
                        connector.configure().unwrap(),
                        "renamed.test",
                        socket,
                    )
                    .await;
                    if connected.is_ok() {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            });
            renamed
                .await
                .expect("the certificate replaced never served");
            finished.store(true, std::sync::atomic::Ordering::SeqCst);
            (admitted, refused)
        })
    });
    let lines = run_until(
        &fake,
        "controller-webhook",
        &plane_at("[::]:8080", "[::]:8443"),
        None,
        Listeners {
            stream: None,
            webhook: Some(hooking),
        },
        move |_| {
            if hold.swap(false, std::sync::atomic::Ordering::SeqCst) {
                std::thread::sleep(Duration::from_millis(1500));
            }
            done.load(std::sync::atomic::Ordering::SeqCst)
        },
    )
    .await;
    let (admitted, refused) = reviewing.join().unwrap();
    assert_eq!(admitted.0, "HTTP/1.1 200 OK");
    let admitted: serde_json::Value = serde_json::from_str(&admitted.1).unwrap();
    assert_eq!(admitted["response"]["allowed"], true, "{admitted}");
    assert_eq!(refused.0, "HTTP/1.1 200 OK");
    let refused: serde_json::Value = serde_json::from_str(&refused.1).unwrap();
    assert_eq!(refused["response"]["uid"], "u-2");
    assert_eq!(refused["response"]["allowed"], false, "{refused}");
    assert_eq!(
        refused["response"]["status"]["message"],
        "not supported yet: annotations"
    );
    assert!(
        lines
            .iter()
            .any(|line| line == "the webhook's certificate is replaced"),
        "{lines:?}"
    );
    assert!(
        !lines.iter().any(|line| line.contains("for the webhook")),
        "{lines:?}"
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
    // The gateway's own pods, a headless Service's, as a nameserver stood in for answers.
    let nameserver =
        crate::resolver::tests::nameserver(crate::resolver::tests::replying(|kind, _| {
            Some(match kind {
                edgerush_dns::Kind::A => edgerush_dns::testing::Reply::Addresses(
                    vec!["10.244.0.6".parse().unwrap(), "10.244.0.5".parse().unwrap()],
                    30,
                ),
                edgerush_dns::Kind::Aaaa => edgerush_dns::testing::Reply::NoSuchName(30),
            })
        }))
        .await;
    let serving = Serving {
        listener,
        files: files.clone(),
        credentials,
        reread: Duration::from_millis(20),
        versions: Versions::new(7),
        own: Own {
            names: vec!["peers.edge.svc.cluster.local".to_owned()],
            nameservers: vec![nameserver],
            pace: crate::resolver::Pace::default(),
        },
    };
    // The data plane, dialling as one does.
    let (ran, mut running) = tokio::sync::mpsc::unbounded_channel();
    let (named, mut naming) = tokio::sync::mpsc::unbounded_channel();
    let tls = edgerush_control_service::connector(&Credentials {
        certificate: authority.issue(&["data-plane"]),
        authorities: authority.pem(),
    })
    .unwrap();
    let following = tokio::spawn(async move {
        let mut staging = Staging::new();
        edgerush_control_service::follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            |config| {
                let _ = ran.send(*config);
                std::future::ready(Ok(()))
            },
            move |addresses| {
                let _ = named.send(addresses);
            },
        )
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
        edgerush_control_service::follow(
            &address,
            "control.test",
            &tls,
            &mut staging,
            |_| std::future::ready(Ok(())),
            |_| {},
        )
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
        Listeners {
            stream: Some(serving),
            webhook: None,
        },
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
    // The gateway's own addresses were sent, whatever the stream sent first.
    let mut gateway = Vec::new();
    while let Ok(addresses) = naming.try_recv() {
        gateway = addresses;
    }
    assert_eq!(
        gateway,
        [
            "10.244.0.5".parse::<std::net::IpAddr>().unwrap(),
            "10.244.0.6".parse().unwrap()
        ]
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
