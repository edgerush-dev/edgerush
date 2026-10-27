use super::*;
use edgerush_config::{Config, HarnessFile};
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
        .run_until(control_until(api, options, plane, stop, move |line| {
            heard
                .borrow_mut()
                .extend(line.lines().map(ToOwned::to_owned));
        }))
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
