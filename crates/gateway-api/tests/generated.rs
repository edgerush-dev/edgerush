//! The modules of `src/` are the ones `edgerush-crdgen` writes from the CRDs in `crds/`. To
//! write them again, after `fetch.sh` or a change to the generator, from the repository root:
//! `EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-gateway-api --test generated`.

#![allow(
    clippy::expect_used,
    reason = "test set-up: a CRD that cannot be read fails the test the way it would"
)]

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

/// Each kind's CRD in `crds/`, the version read, and the module written from it.
const KINDS: [(&str, &str, &str); 4] = [
    (
        "gateway.networking.k8s.io_gatewayclasses.yaml",
        "v1",
        "gateway_class.rs",
    ),
    (
        "gateway.networking.k8s.io_gateways.yaml",
        "v1",
        "gateway.rs",
    ),
    (
        "gateway.networking.k8s.io_httproutes.yaml",
        "v1",
        "http_route.rs",
    ),
    (
        "gateway.networking.k8s.io_referencegrants.yaml",
        "v1",
        "reference_grant.rs",
    ),
];

fn path(dir: &str, file: &str) -> String {
    format!("{}/{dir}/{file}", env!("CARGO_MANIFEST_DIR"))
}

/// The module `edgerush-crdgen` writes from the CRD `file`, at `version`.
fn written(file: &str, version: &str) -> String {
    let yaml = std::fs::read_to_string(path("crds", file)).expect("the CRD is there");
    let crd: CustomResourceDefinition = serde_saphyr::from_str(&yaml).expect("the CRD reads");
    let release = std::fs::read_to_string(path("crds", "VERSION")).expect("crds/VERSION is there");
    let body = edgerush_crdgen::generate(&crd, version).expect("the CRD makes a module");
    format!(
        "//! `{kind}`, `{group}/{version}`, as Gateway API {release} defines it in its standard\n\
         //! channel. Written by `edgerush-crdgen` from `crds/{file}`: not to be edited (the\n\
         //! crate's docs say how it is written again).\n\n{body}",
        kind = crd.spec.names.kind,
        group = crd.spec.group,
        release = release.trim(),
    )
}

#[test]
fn the_modules_are_the_ones_the_crds_make() {
    let write = std::env::var_os("EDGERUSH_WRITE_TYPES").is_some();
    let mut stale = Vec::new();
    for (crd, version, module) in KINDS {
        let made = written(crd, version);
        let at = path("src", module);
        if write {
            std::fs::write(&at, &made).expect("the module is written");
            continue;
        }
        let kept = std::fs::read_to_string(&at)
            .unwrap_or_default()
            .replace("\r\n", "\n");
        if kept != made {
            stale.push(module);
        }
    }
    assert!(
        stale.is_empty(),
        "{stale:?} are not what the CRDs make; write them again with \
         EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-gateway-api --test generated"
    );
}
