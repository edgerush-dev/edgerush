use super::*;
use edgerush_config::HarnessFile;
use proptest::prelude::*;
use serde_json::{Value, json};

/// A version of RateLimitPolicy `namespace/name` that applied, with a limit of each of
/// `limits`, `(name, rate)`, on the Ingress `web`.
fn policy(
    namespace: &str,
    name: &str,
    uid: Option<String>,
    generation: Option<i64>,
    limits: &[(String, String)],
) -> AcceptedPolicy {
    let limits: Vec<Value> = limits
        .iter()
        .map(|(limit, rate)| {
            json!({ "name": limit, "key": "Client", "model": { "bucket": { "rate": rate, "burst": 1 } } })
        })
        .collect();
    AcceptedPolicy {
        namespace: namespace.to_owned(),
        name: name.to_owned(),
        uid,
        generation,
        spec: serde_json::from_value(json!({
            "targetRefs": [{ "group": "networking.k8s.io", "kind": "Ingress", "name": "web" }],
            "limits": limits,
        }))
        .unwrap(),
    }
}

/// DataPlane `namespace/name` of `generation`, serving Ingress on `http`, with a global
/// limit of each of `limits`.
fn data_plane(
    namespace: &str,
    name: &str,
    generation: i64,
    http: i32,
    limits: &[String],
) -> DataPlane {
    let global: Vec<Value> = limits
        .iter()
        .map(|limit| json!({ "name": limit, "key": "Client", "model": { "bucket": { "rate": "10/s", "burst": 1 } } }))
        .collect();
    serde_json::from_value(json!({
        "metadata": {
            "name": name,
            "namespace": namespace,
            "uid": format!("{name}-uid"),
            "generation": generation,
            "creationTimestamp": "2026-11-20T10:00:00Z",
            "labels": { "team": namespace },
        },
        "spec": {
            "controlPlaneRef": { "name": "edge" },
            "ingress": { "httpPort": http, "httpsPort": 443 },
            "rateLimiting": { "mode": "local", "global": global },
        },
    }))
    .unwrap()
}

/// A config told apart by its port, with a certificate of each of `certificates`,
/// `(name, chain, key)`, and limits of `burst`, if any.
fn config(port: u16, certificates: &[(String, String, String)], burst: Option<u64>) -> Config {
    let mut config: Config = serde_saphyr::from_str(&format!(
        "listeners:
  http:
    address: \"[::]:{port}\"
    protocol: http
    proxy_protocol: off
    forwarding: {{ trusted_proxies: [], trusted_only_headers: [Forwarded] }}
    request_id: generate
routes: []
upstreams: {{}}
"
    ))
    .unwrap();
    config.certificates = certificates
        .iter()
        .map(|(name, chain, key)| {
            let certificate = Certificate {
                chain: chain.clone(),
                key: key.clone(),
            };
            (name.clone(), certificate)
        })
        .collect();
    config.limits = burst.map(|burst| {
        serde_saphyr::from_str(&format!(
            "global: {{ rate_limits: [per-client] }}
limits:
  - {{ name: per-client, key: client, model: {{ bucket: {{ rate: 10/s, burst: {burst} }} }} }}
"
        ))
        .unwrap()
    });
    config
}

/// This build's config version, as a snapshot is written for a replica of the same build.
fn this() -> ConfigVersion {
    ConfigVersion::served(CONFIG_VERSION).unwrap()
}

fn names() -> impl Strategy<Value = String> {
    "[a-z][a-z0-9-]{0,11}"
}

fn policies() -> impl Strategy<Value = Vec<AcceptedPolicy>> {
    proptest::collection::vec(
        (
            names(),
            names(),
            proptest::option::of(names()),
            proptest::option::of(1i64..1_000),
            proptest::collection::vec(
                (
                    names(),
                    prop_oneof![Just("10/s"), Just("5/minute"), Just("1/fortnight")],
                ),
                0..4,
            ),
        )
            .prop_map(|(namespace, name, uid, generation, limits)| {
                let limits: Vec<(String, String)> = limits
                    .into_iter()
                    .map(|(limit, rate)| (limit, rate.to_owned()))
                    .collect();
                policy(&namespace, &name, uid, generation, &limits)
            }),
        0..4,
    )
}

fn data_planes() -> impl Strategy<Value = Option<DataPlane>> {
    proptest::option::of(
        (
            names(),
            names(),
            1i64..1_000,
            1i32..65_536,
            proptest::collection::vec(names(), 0..3),
        )
            .prop_map(|(namespace, name, generation, http, limits)| {
                data_plane(&namespace, &name, generation, http, &limits)
            }),
    )
}

/// Configs whose certificates hold anything at all, escapes and control characters among
/// it.
fn configs() -> impl Strategy<Value = Option<Config>> {
    proptest::option::of(
        (
            1u16..,
            proptest::collection::vec((names(), any::<String>(), any::<String>()), 0..3),
            proptest::option::of(1u64..1_000),
        )
            .prop_map(|(port, certificates, burst)| config(port, &certificates, burst)),
    )
}

fn snapshots() -> impl Strategy<Value = Snapshot> {
    let planes = proptest::collection::btree_map(
        names(),
        (policies(), data_planes(), data_planes(), configs()).prop_map(
            |(policies, accepted, served, config)| Record {
                policies,
                accepted,
                served,
                config,
            },
        ),
        0..4,
    );
    (any::<u64>(), planes).prop_map(|(count, planes)| Snapshot { count, planes })
}

/// `snapshot`, encoded, with `edit` made to its JSON.
fn edited(snapshot: &Snapshot, edit: impl FnOnce(&mut Value)) -> Vec<u8> {
    let mut written: Value = serde_json::from_slice(&snapshot.encode(this()).unwrap()).unwrap();
    edit(&mut written);
    serde_json::to_vec(&written).unwrap()
}

/// A snapshot of two data planes: one whose policy's edit is refused, the generation of its
/// DataPlane accepted served, and whose config now is refused whole, the last handed on
/// served; one whose later generation is held back.
fn two_planes() -> Snapshot {
    let limits = [("per-client".to_owned(), "10/s".to_owned())];
    Snapshot {
        count: 7,
        planes: BTreeMap::from([
            (
                "shop/web".to_owned(),
                Record {
                    policies: vec![policy(
                        "shop",
                        "shop",
                        Some("shop-uid".to_owned()),
                        Some(1),
                        &limits,
                    )],
                    accepted: Some(data_plane("shop", "web", 3, 80, &[])),
                    served: None,
                    config: Some(config(
                        8080,
                        &[(
                            "shop".to_owned(),
                            "-----BEGIN CERTIFICATE-----
shop
-----END CERTIFICATE-----
"
                            .to_owned(),
                            "-----BEGIN PRIVATE KEY-----
shop
-----END PRIVATE KEY-----
"
                            .to_owned(),
                        )],
                        Some(1),
                    )),
                },
            ),
            (
                "shop/api".to_owned(),
                Record {
                    policies: vec![],
                    accepted: Some(data_plane(
                        "shop",
                        "api",
                        5,
                        8080,
                        &["per-client".to_owned()],
                    )),
                    served: Some(data_plane("shop", "api", 4, 8080, &[])),
                    config: None,
                },
            ),
        ]),
    }
}

proptest! {
    /// Whatever a snapshot holds, it is read back as it was written.
    #[test]
    fn a_snapshot_is_read_as_it_was_written(snapshot in snapshots()) {
        let read = Snapshot::decode(&snapshot.encode(this()).unwrap()).unwrap();
        prop_assert_eq!(read, snapshot);
    }
}

/// Written for a replica of any version in the window, as a leader writes for an older one
/// through an upgrade, it says that version and is read back as it was.
#[test]
fn a_snapshot_is_written_at_the_version_its_reader_reads() {
    let versions: Vec<ConfigVersion> = (OLDEST_CONFIG_VERSION..=CONFIG_VERSION)
        .filter_map(ConfigVersion::served)
        .collect();
    assert!(!versions.is_empty());
    for version in versions {
        let bytes = two_planes().encode(version).unwrap();
        let written: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(written["config_version"], json!(version.get()));
        assert_eq!(Snapshot::decode(&bytes).unwrap(), two_planes());
    }
}

/// One of a version before the oldest read, or after this build's, is refused whole, saying
/// its version.
#[test]
fn a_snapshot_outside_the_window_is_refused() {
    for version in [OLDEST_CONFIG_VERSION - 1, CONFIG_VERSION + 1] {
        let bytes = edited(&two_planes(), |written| {
            written["config_version"] = json!(version);
        });
        match Snapshot::decode(&bytes) {
            Err(SnapshotError::Version { version: said }) => assert_eq!(said, version),
            other => panic!("version {version} taken in: {other:?}"),
        }
    }
}

/// A snapshot holds what its version writes and nothing else: a field it does not know, or
/// one it requires missing, its count or a record's policies, is no snapshot of its
/// version (one of none, missing, is none).
#[test]
fn a_snapshot_not_of_its_versions_shape_is_refused() {
    let added = edited(&two_planes(), |written| {
        written["planes"]["shop/web"]["later"] = json!(true);
    });
    let missing = edited(&two_planes(), |written| {
        written["planes"]["shop/web"]
            .as_object_mut()
            .unwrap()
            .remove("policies");
    });
    let uncounted = edited(&two_planes(), |written| {
        written.as_object_mut().unwrap().remove("count");
    });
    for bytes in [added, missing, uncounted] {
        assert!(matches!(
            Snapshot::decode(&bytes),
            Err(SnapshotError::Unreadable(_))
        ));
    }
}

#[test]
fn what_is_no_snapshot_is_refused() {
    for bytes in [
        &b""[..],
        b"not json",
        b"[]",
        b"{}",
        b"{\"config_version\": \"1\"}",
    ] {
        assert!(
            matches!(Snapshot::decode(bytes), Err(SnapshotError::Unreadable(_))),
            "{:?} taken in",
            String::from_utf8_lossy(bytes)
        );
    }
}

/// A DataPlane's status says what is made of it now, not what the snapshot was taken of: it
/// is not carried.
#[test]
fn a_data_planes_status_is_not_carried() {
    let mut with_status = data_plane("shop", "web", 3, 80, &[]);
    with_status.status = serde_json::from_value(json!({
        "conditions": [{
            "type": "Accepted", "status": "True", "reason": "Accepted", "message": "",
            "lastTransitionTime": "2026-11-20T10:00:00Z", "observedGeneration": 3,
        }],
    }))
    .unwrap();
    let snapshot = Snapshot {
        count: 1,
        planes: BTreeMap::from([(
            "shop/web".to_owned(),
            Record {
                accepted: Some(with_status),
                ..Record::default()
            },
        )]),
    };
    let written: Value = serde_json::from_slice(&snapshot.encode(this()).unwrap()).unwrap();
    assert_eq!(
        written["planes"]["shop/web"]["accepted"].get("status"),
        None
    );
    let read = Snapshot::decode(&snapshot.encode(this()).unwrap()).unwrap();
    assert_eq!(
        read.planes["shop/web"].accepted,
        Some(data_plane("shop", "web", 3, 80, &[]))
    );
}

/// The fuzz target's seed is a snapshot this build reads, so that fuzzing starts from one.
#[test]
fn the_fuzz_seed_is_a_snapshot() {
    let seed = include_bytes!("../../../../fuzz/seeds/snapshot/two-planes");
    assert_eq!(Snapshot::decode(seed).unwrap(), two_planes());
}

/// The config a snapshot carries is the whole model, every feature of it, and its
/// certificates, keys included, and its limits, which the config's own JSON leaves out.
#[test]
fn a_config_is_carried_whole_with_its_certificates_and_limits() {
    // Read as the development harness reads it, its certificates' files left out.
    let file: HarnessFile = serde_saphyr::from_str(
        &include_str!("../../../../examples/all-features.yaml").replace(
            "
", "
",
        ),
    )
    .unwrap();
    let mut whole = file.into_config(BTreeMap::new(), None);
    whole.certificates = config(
        80,
        &[("shop".to_owned(), "chain".to_owned(), "key".to_owned())],
        None,
    )
    .certificates;
    whole.limits = config(80, &[], Some(5)).limits;
    let snapshot = Snapshot {
        count: 1,
        planes: BTreeMap::from([(
            "shop/web".to_owned(),
            Record {
                config: Some(whole.clone()),
                ..Record::default()
            },
        )]),
    };
    let written: Value = serde_json::from_slice(&snapshot.encode(this()).unwrap()).unwrap();
    let carried = &written["planes"]["shop/web"]["config"];
    assert_eq!(
        carried["certificates"]["shop"],
        json!({ "chain": "chain", "key": "key" })
    );
    assert_eq!(carried["config"].get("certificates"), None);
    let read = Snapshot::decode(&snapshot.encode(this()).unwrap()).unwrap();
    assert_eq!(read.planes["shop/web"].config, Some(whole));
}

/// What prints a snapshot, an error or a log, does not print a key it carries.
#[test]
fn a_snapshot_printed_shows_no_key() {
    let printed = format!("{:?}", two_planes());
    assert!(printed.contains("-----BEGIN CERTIFICATE-----"));
    assert!(!printed.contains("PRIVATE KEY"), "{printed}");
}
