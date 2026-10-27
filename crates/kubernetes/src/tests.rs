use super::*;
use crate::testing::*;
use k8s_openapi::ByteString;
use k8s_openapi::api::core::v1::{Secret, Service};
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

/// A namespace's scope.
fn namespace(name: &str) -> Scope {
    Scope::Namespace(name.to_owned())
}

#[tokio::test]
async fn a_list_is_asked_for_with_the_token_and_read() {
    let fake = Fake::start(&["127.0.0.1"], |_| answer(200, &list("5", Some("a/b=")))).await;
    let client = fake.client(&token("list", "secret\n"), Timing::default());
    let page = client
        .list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap();
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].metadata.name.as_deref(), Some("web-1"));
    assert_eq!(page.metadata.resource_version.as_deref(), Some("5"));
    assert_eq!(page.metadata.continue_.as_deref(), Some("a/b="));
    client
        .list::<EndpointSlice>(&namespace("shop"), Some("a/b="))
        .await
        .unwrap();

    let seen = fake.seen();
    assert_eq!(
        seen[0].path,
        "/apis/discovery.k8s.io/v1/namespaces/shop/endpointslices?limit=500"
    );
    // The token goes as written in its file, without the line's end; a list's token is
    // percent-encoded.
    assert_eq!(seen[0].authorization.as_deref(), Some("Bearer secret"));
    assert_eq!(
        seen[1].path,
        "/apis/discovery.k8s.io/v1/namespaces/shop/endpointslices?limit=500&continue=a%2Fb%3D"
    );
    // Both over one connection.
    assert_eq!(fake.connections.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn a_watch_is_asked_from_its_version_and_read_an_event_a_line() {
    let added = format!(r#"{{"type":"ADDED","object":{SLICE}}}"#);
    let bookmark = r#"{"type":"BOOKMARK","object":{"kind":"EndpointSlice","apiVersion":"discovery.k8s.io/v1","metadata":{"resourceVersion":"9"}}}"#;
    // Events split across chunks however the server sends them.
    let whole = format!("{added}\n\n{bookmark}\n");
    let (first, rest) = whole.split_at(added.len() - 7);
    let (second, third) = rest.split_at(20);
    let chunks = [first, second, third]
        .map(|part| Bytes::copy_from_slice(part.as_bytes()))
        .to_vec();
    let fake = Fake::start(&["127.0.0.1"], move |_| Answer {
        status: 200,
        chunks: chunks.clone(),
        pause: Duration::from_millis(5),
    })
    .await;
    let client = fake.client(&token("watch", "secret"), Timing::default());
    let mut events = client
        .watch::<EndpointSlice>(&namespace("shop"), "7+x")
        .await
        .unwrap();
    match events.next().await {
        Some(Ok(WatchEvent::Added(slice))) => {
            assert_eq!(slice.metadata.name.as_deref(), Some("web-1"));
        }
        other => panic!("{other:?}"),
    }
    match events.next().await {
        Some(Ok(WatchEvent::Bookmark {
            resource_version, ..
        })) => assert_eq!(resource_version, "9"),
        other => panic!("{other:?}"),
    }
    assert!(events.next().await.is_none());
    assert_eq!(
        fake.seen()[0].path,
        "/apis/discovery.k8s.io/v1/namespaces/shop/endpointslices?watch=true&allowWatchBookmarks=true&timeoutSeconds=300&resourceVersion=7%2Bx"
    );
}

#[tokio::test]
async fn another_status_is_said_with_its_message_and_410_is_gone() {
    let status = r#"{"kind":"Status","apiVersion":"v1","metadata":{},"status":"Failure","message":"too old resource version: 7 (12)","reason":"Expired","code":410}"#;
    let fake = Fake::start(&["127.0.0.1"], move |path| {
        if path.contains("watch") {
            answer(410, status)
        } else {
            answer(403, "not JSON")
        }
    })
    .await;
    let client = fake.client(&token("status", "secret"), Timing::default());
    let gone = client
        .watch::<EndpointSlice>(&namespace("shop"), "7")
        .await
        .unwrap_err();
    assert_eq!(
        gone,
        ApiError::Answered {
            code: 410,
            message: "too old resource version: 7 (12)".to_owned()
        }
    );
    assert!(gone.is_gone());
    let refused = client
        .list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap_err();
    assert_eq!(
        refused,
        ApiError::Answered {
            code: 403,
            message: String::new()
        }
    );
    assert!(!refused.is_gone());
}

#[tokio::test]
async fn a_certificate_not_for_the_address_or_not_trusted_is_refused() {
    let elsewhere = Fake::start(&["127.0.0.2", "kubernetes.default.svc"], |_| {
        answer(200, &list("1", None))
    })
    .await;
    let client = elsewhere.client(&token("elsewhere", "secret"), Timing::default());
    assert!(
        matches!(
            client.list::<EndpointSlice>(&namespace("shop"), None).await,
            Err(ApiError::Connect(_))
        ),
        "a certificate for another address"
    );

    let fake = Fake::start(&["127.0.0.1"], |_| answer(200, &list("1", None))).await;
    let other = edgerush_certificates::testing::certificate(&["127.0.0.1"]);
    let untrusting = ApiServer::new(
        "127.0.0.1",
        fake.address.port(),
        &other.chain,
        token("untrusting", "secret"),
        Timing::default(),
    )
    .unwrap();
    assert!(
        matches!(
            untrusting
                .list::<EndpointSlice>(&namespace("shop"), None)
                .await,
            Err(ApiError::Connect(_))
        ),
        "a certificate no trusted authority signed"
    );
    assert!(fake.seen().is_empty());
}

#[tokio::test]
async fn the_token_is_read_again_once_it_is_old_and_kept_if_it_cannot_be() {
    let fake = Fake::start(&["127.0.0.1"], |_| answer(200, &list("1", None))).await;
    let path = token("reread", "first");
    let every_time = Timing {
        token_reread: Duration::ZERO,
        ..Timing::default()
    };
    let client = fake.client(&path, every_time);
    client
        .list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap();
    std::fs::write(&path, "second").unwrap();
    client
        .list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap();
    std::fs::remove_file(&path).unwrap();
    client
        .list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap();

    let held = fake.client(&token("held", "first"), Timing::default());
    held.list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap();
    token("held", "second");
    held.list::<EndpointSlice>(&namespace("shop"), None)
        .await
        .unwrap();

    let tokens: Vec<Option<String>> = fake.seen().into_iter().map(|s| s.authorization).collect();
    let bearer = |token: &str| Some(format!("Bearer {token}"));
    assert_eq!(
        tokens,
        [
            bearer("first"),
            bearer("second"),
            bearer("second"),
            bearer("first"),
            bearer("first")
        ]
    );

    let none = fake.client(&token("none", "  \n"), Timing::default());
    assert!(matches!(
        none.list::<EndpointSlice>(&namespace("shop"), None).await,
        Err(ApiError::Token(_))
    ));
}

#[tokio::test]
async fn answers_are_bounded_in_size_and_time() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("namespaces/large/") && path.contains("watch") {
            // An event that never ends its line.
            answer(200, &"x".repeat(MOST_EVENT_BYTES + 1))
        } else if path.contains("namespaces/large/") {
            Answer {
                status: 200,
                chunks: vec![Bytes::from(vec![b' '; 1 << 20]); (MOST_PAGE_BYTES >> 20) + 1],
                pause: Duration::ZERO,
            }
        } else if path.contains("watch") {
            Answer {
                status: 200,
                chunks: vec![Bytes::from_static(b"\n"), Bytes::from_static(b"\n")],
                pause: Duration::from_secs(5),
            }
        } else {
            Answer {
                status: 200,
                chunks: vec![Bytes::from_static(b"{"), Bytes::from_static(b"}")],
                pause: Duration::from_secs(5),
            }
        }
    })
    .await;
    let quick = Timing {
        answer: Duration::from_millis(500),
        quiet: Duration::from_millis(300),
        ..Timing::default()
    };
    let client = fake.client(&token("bounded", "secret"), quick);
    assert_eq!(
        client
            .list::<EndpointSlice>(&namespace("large"), None)
            .await
            .unwrap_err(),
        ApiError::TooLarge("a list's page", MOST_PAGE_BYTES)
    );
    let mut events = client
        .watch::<EndpointSlice>(&namespace("large"), "1")
        .await
        .unwrap();
    assert!(matches!(
        events.next().await,
        Some(Err(ApiError::TooLarge("a watch's event", MOST_EVENT_BYTES)))
    ));
    assert_eq!(
        client
            .list::<EndpointSlice>(&namespace("slow"), None)
            .await
            .unwrap_err(),
        ApiError::TimedOut
    );
    let mut quiet = client
        .watch::<EndpointSlice>(&namespace("slow"), "1")
        .await
        .unwrap();
    assert_eq!(quiet.next().await.unwrap().unwrap_err(), ApiError::TimedOut);
}

#[test]
fn the_server_is_what_the_environment_names() {
    assert_eq!(
        server(Some("10.96.0.1"), Some("443")),
        Ok(("10.96.0.1".to_owned(), 443))
    );
    for (host, port) in [
        (None, Some("443")),
        (Some(""), Some("443")),
        (Some("10.96.0.1"), None),
    ] {
        assert_eq!(
            server(host, port),
            Err(ApiError::NoServer),
            "{host:?} {port:?}"
        );
    }
    for port in ["0", "x", "70000", ""] {
        assert_eq!(
            server(Some("10.96.0.1"), Some(port)),
            Err(ApiError::Port(port.to_owned()))
        );
    }
    let authority = |host: &str| {
        let certificate = edgerush_certificates::testing::certificate(&[host]);
        ApiServer::new(
            host,
            443,
            &certificate.chain,
            PathBuf::new(),
            Timing::default(),
        )
        .unwrap()
        .authority
    };
    assert_eq!(authority("fd00::1"), "[fd00::1]:443");
    assert_eq!(authority("10.96.0.1"), "10.96.0.1:443");
}

#[test]
fn a_namespace_is_a_label_and_nothing_else() {
    for namespace in [
        "",
        "Shop",
        "../x",
        "shop/x",
        "-shop",
        "shop-",
        &"a".repeat(64),
    ] {
        assert_eq!(
            kind::collection::<EndpointSlice>(&Scope::Namespace(namespace.to_owned())),
            Err(ApiError::Namespace(namespace.to_owned()))
        );
    }
    assert!(kind::collection::<EndpointSlice>(&self::namespace("shop-2")).is_ok());
}

#[test]
fn each_kind_is_asked_for_at_its_own_path_in_the_cluster_or_a_namespace() {
    let shop = namespace("shop");
    for (path, expected) in [
        (
            kind::collection::<Ingress>(&Scope::Cluster),
            "/apis/networking.k8s.io/v1/ingresses",
        ),
        (
            kind::collection::<Ingress>(&shop),
            "/apis/networking.k8s.io/v1/namespaces/shop/ingresses",
        ),
        (
            kind::collection::<IngressClass>(&Scope::Cluster),
            "/apis/networking.k8s.io/v1/ingressclasses",
        ),
        // The core group has a path of its own.
        (
            kind::collection::<Service>(&Scope::Cluster),
            "/api/v1/services",
        ),
        (
            kind::collection::<Secret>(&shop),
            "/api/v1/namespaces/shop/secrets",
        ),
        (
            kind::collection::<EndpointSlice>(&Scope::Cluster),
            "/apis/discovery.k8s.io/v1/endpointslices",
        ),
    ] {
        assert_eq!(path.as_deref(), Ok(expected));
    }
    assert_eq!(
        kind::collection::<IngressClass>(&shop),
        Err(ApiError::NotNamespaced("IngressClass"))
    );
}

/// A Secret as `kubectl apply` leaves it: a certificate, its key, a key besides, and the
/// whole again in kubectl's annotation.
const SECRET: &str = r#"{"metadata":{"name":"tls","namespace":"shop","resourceVersion":"3","labels":{"app":"web"},"annotations":{"kubectl.kubernetes.io/last-applied-configuration":"{\"data\":{\"password\":\"aHVudGVyMg==\"}}"},"managedFields":[{"manager":"kubectl"}]},"type":"kubernetes.io/tls","data":{"tls.crt":"Y2VydA==","tls.key":"a2V5","password":"aHVudGVyMg=="}}"#;

/// [`SECRET`] as it is kept.
fn trimmed_secret() -> Secret {
    Secret {
        metadata: ObjectMeta {
            name: Some("tls".to_owned()),
            namespace: Some("shop".to_owned()),
            resource_version: Some("3".to_owned()),
            ..ObjectMeta::default()
        },
        type_: Some("kubernetes.io/tls".to_owned()),
        data: Some(BTreeMap::from([
            ("tls.crt".to_owned(), ByteString(b"cert".to_vec())),
            ("tls.key".to_owned(), ByteString(b"key".to_vec())),
        ])),
        ..Secret::default()
    }
}

#[tokio::test]
async fn secrets_are_asked_for_without_helms_and_tokens_and_kept_as_certificates_alone() {
    let fake = Fake::start(&["127.0.0.1"], |path| {
        if path.contains("watch=true") {
            answer(
                200,
                &format!("{{\"type\":\"MODIFIED\",\"object\":{SECRET}}}\n"),
            )
        } else {
            answer(
                200,
                &format!(r#"{{"metadata":{{"resourceVersion":"3"}},"items":[{SECRET}]}}"#),
            )
        }
    })
    .await;
    let client = fake.client(&token("secrets", "secret"), Timing::default());
    let page = client.list::<Secret>(&Scope::Cluster, None).await.unwrap();
    assert_eq!(page.items, [trimmed_secret()]);
    let mut events = client.watch::<Secret>(&Scope::Cluster, "3").await.unwrap();
    assert_eq!(
        events.next().await.unwrap().unwrap(),
        WatchEvent::Modified(trimmed_secret())
    );

    let seen = fake.seen();
    let selector = "fieldSelector=type%21%3Dhelm.sh%2Frelease.v1%2Ctype%21%3Dkubernetes.io%2Fservice-account-token";
    assert_eq!(seen[0].path, format!("/api/v1/secrets?limit=12&{selector}"));
    assert_eq!(
        seen[1].path,
        format!(
            "/api/v1/secrets?watch=true&allowWatchBookmarks=true&timeoutSeconds=300&{selector}&resourceVersion=3"
        )
    );
}

#[tokio::test]
async fn every_kind_is_kept_without_its_managed_fields() {
    let ingress = r#"{"metadata":{"name":"web","namespace":"shop","resourceVersion":"2","annotations":{"a":"b"},"managedFields":[{"manager":"kubectl"}]},"spec":{}}"#;
    let fake = Fake::start(&["127.0.0.1"], move |_| {
        answer(
            200,
            &format!(r#"{{"metadata":{{"resourceVersion":"2"}},"items":[{ingress}]}}"#),
        )
    })
    .await;
    let client = fake.client(&token("managed", "secret"), Timing::default());
    let page = client.list::<Ingress>(&Scope::Cluster, None).await.unwrap();
    let metadata = &page.items[0].metadata;
    assert_eq!(metadata.managed_fields, None);
    // An Ingress's annotations are read: they stay.
    assert_eq!(
        metadata.annotations,
        Some(BTreeMap::from([("a".to_owned(), "b".to_owned())]))
    );
    assert_eq!(
        fake.seen()[0].path,
        "/apis/networking.k8s.io/v1/ingresses?limit=500"
    );
}

#[test]
fn authorities_that_are_not_certificates_are_refused() {
    for authorities in ["", "not PEM"] {
        assert!(matches!(
            ApiServer::new(
                "10.96.0.1",
                443,
                authorities,
                PathBuf::new(),
                Timing::default()
            ),
            Err(ApiError::Authorities(_))
        ));
    }
}
