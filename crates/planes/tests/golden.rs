//! Golden tests: a resource in, the objects the operator makes for it out
//! ([10](../../../docs/10-testing.md)).
//!
//! Each directory under `tests/golden/` is a case: `resource.yaml`, a ControlPlane and,
//! where the case is of DataPlanes, the DataPlanes of it after it, as `kubectl apply` would
//! take them, and as the API server would store them, a name, namespace and UID each;
//! where the ControlPlane's watch scope is a selector, `selected.txt`, the namespaces it
//! selects, a line each; and `objects.yaml`, the objects expected for the ControlPlane, or
//! else for each DataPlane in turn, as YAML documents in the order they are made. The
//! install's image is `edgerush:golden`. A case of DataPlanes has `service.yaml` too, each
//! data plane's Service its control plane writes, for the ports `bound.txt` says each
//! config binds, a line each of the port and `tcp`, `udp` or both (none without the file).
//!
//! Each object says the kind and metadata it is written with, no two objects of a case are
//! one object, of one kind, namespace and name, and between them the cases make every kind
//! the operator makes.
//!
//! A case whose objects have changed on purpose is written again, and then read over, with
//! `EDGERUSH_WRITE_GOLDEN=1 cargo test -p edgerush-planes --test golden`.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    reason = "a case that cannot be read fails the test"
)]

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use edgerush_planes::control_plane::ControlPlane;
use edgerush_planes::data_plane::DataPlane;
use edgerush_planes::objects::{
    Install, Object, ProxyService, Transports, for_control_plane, for_data_plane,
};
use serde::Deserialize;

/// A resource of a case.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Resource {
    ControlPlane(Box<ControlPlane>),
    DataPlane(Box<DataPlane>),
}

fn read(path: &Path) -> String {
    fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("{}: {error}", path.display()))
        .replace("\r\n", "\n")
}

/// The objects written as the case keeps them: YAML documents, `---` between them.
fn written(objects: &[Object]) -> String {
    objects
        .iter()
        .map(|object| serde_saphyr::to_string(object).unwrap())
        .collect::<Vec<_>>()
        .join("---\n")
}

/// The ports of `bound.txt`.
fn bound(lines: &str) -> BTreeMap<u16, Transports> {
    lines
        .lines()
        .map(|line| {
            let mut words = line.split_whitespace();
            let port = words.next().unwrap().parse().unwrap();
            let mut transports = Transports::default();
            for word in words {
                match word {
                    "tcp" => transports.tcp = true,
                    "udp" => transports.udp = true,
                    _ => panic!("bound.txt: {line}"),
                }
            }
            (port, transports)
        })
        .collect()
}

#[test]
fn every_case_makes_its_objects() {
    let write = std::env::var_os("EDGERUSH_WRITE_GOLDEN").is_some();
    let install = Install {
        cluster_domain: "cluster.local".to_owned(),
        image: "edgerush:golden".to_owned(),
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut cases: Vec<_> = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.is_dir())
        .collect();
    cases.sort();
    assert!(!cases.is_empty(), "no cases under {}", root.display());
    let mut kinds = BTreeSet::new();
    for case in cases {
        let name = case.file_name().unwrap().to_string_lossy().into_owned();
        let resources: Vec<Resource> =
            serde_saphyr::from_multiple(&read(&case.join("resource.yaml"))).unwrap();
        let selected_at = case.join("selected.txt");
        let selected: BTreeSet<String> = if selected_at.exists() {
            read(&selected_at).lines().map(str::to_owned).collect()
        } else {
            BTreeSet::new()
        };
        let (control_plane, data_planes) = match resources.as_slice() {
            [Resource::ControlPlane(cp), rest @ ..] => {
                let data_planes: Vec<&DataPlane> = rest
                    .iter()
                    .map(|resource| match resource {
                        Resource::DataPlane(dp) => &**dp,
                        Resource::ControlPlane(_) => {
                            panic!("{name}: resource.yaml has one ControlPlane, first")
                        }
                    })
                    .collect();
                (cp, data_planes)
            }
            _ => panic!("{name}: resource.yaml is a ControlPlane, then perhaps DataPlanes"),
        };
        if !data_planes.is_empty() {
            let bound_at = case.join("bound.txt");
            let bound = if bound_at.exists() {
                bound(&read(&bound_at))
            } else {
                BTreeMap::new()
            };
            let made = data_planes
                .iter()
                .map(|dp| {
                    let service = ProxyService::of(dp)
                        .unwrap_or_else(|error| panic!("{name}: {error}"))
                        .service(&bound);
                    match service {
                        Some(service) => serde_saphyr::to_string(&service).unwrap(),
                        None => "# No port, so no Service.
"
                        .to_owned(),
                    }
                })
                .collect::<Vec<_>>()
                .join("---\n");
            let expected_at = case.join("service.yaml");
            if write {
                fs::write(&expected_at, &made).unwrap();
            } else {
                assert_eq!(
                    read(&expected_at),
                    made,
                    "{name} makes another Service than its service.yaml says; if that is                      meant, write it again with EDGERUSH_WRITE_GOLDEN=1 cargo test -p                      edgerush-planes --test golden, and read it over"
                );
            }
        }
        let objects: Vec<Object> = if data_planes.is_empty() {
            for_control_plane(control_plane, &selected, &install)
                .unwrap_or_else(|error| panic!("{name}: {error}"))
        } else {
            data_planes
                .iter()
                .flat_map(|dp| {
                    for_data_plane(dp, control_plane, &selected, &install)
                        .unwrap_or_else(|error| panic!("{name}: {error}"))
                })
                .collect()
        };
        // What the operator applies each by: its kind and metadata, as it is written; and
        // none another's, which two DataPlanes of one ControlPlane would fight over.
        let mut seen = BTreeSet::new();
        for object in &objects {
            let metadata = object.metadata();
            assert!(
                seen.insert((
                    object.kind(),
                    metadata.namespace.clone(),
                    metadata.name.clone()
                )),
                "{name}: two objects are {} {:?}/{:?}",
                object.kind(),
                metadata.namespace,
                metadata.name
            );
            let as_written = serde_json::to_value(object).unwrap();
            assert_eq!(as_written["kind"], object.kind(), "{name}");
            assert_eq!(
                as_written["metadata"],
                serde_json::to_value(object.metadata()).unwrap(),
                "{name}"
            );
            kinds.insert(object.kind());
        }
        let made = written(&objects);
        let expected_at = case.join("objects.yaml");
        if write {
            fs::write(&expected_at, &made).unwrap();
            continue;
        }
        assert_eq!(
            read(&expected_at),
            made,
            "{name} makes other objects than its objects.yaml says; if that is meant, write \
             them again with EDGERUSH_WRITE_GOLDEN=1 cargo test -p edgerush-planes --test \
             golden, and read them over"
        );
    }
    assert_eq!(
        kinds.len(),
        11,
        "a kind the operator makes is in no case: {kinds:?}"
    );
}
