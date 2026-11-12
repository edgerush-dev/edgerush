//! The CRDs in `install/` and the modules of `src/` written from them by `edgerush-crdgen`.
//!
//! The DataPlane's `spec.gateway` holds the Gateway's own fields, copied from Gateway API's
//! Gateway CRD in `crates/gateway-api/crds`; this fails where the copy differs. To write the
//! copy and the modules again, after a Gateway API release is fetched or a CRD is changed,
//! from the repository root:
//! `EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test generated`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: a CRD that cannot be read fails the test the way it would"
)]

use std::collections::BTreeMap;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, JSONSchemaProps,
};

/// Each kind's CRD in `install/` and the module written from it, at `v1alpha1`.
const KINDS: [(&str, &str); 2] = [
    (
        "gateway.edgerush.dev_controlplanes.yaml",
        "control_plane.rs",
    ),
    ("gateway.edgerush.dev_dataplanes.yaml", "data_plane.rs"),
];

/// The DataPlane's CRD.
const DATA_PLANE: &str = "gateway.edgerush.dev_dataplanes.yaml";

/// The Gateway's fields the DataPlane's `spec.gateway` holds.
const GATEWAY_FIELDS: [&str; 3] = ["allowedListeners", "listeners", "tls"];

/// The last line before the copy of the Gateway's fields, and the line after it.
const COPY_STARTS_AFTER: &str = "                  # Not to be edited.";
const COPY_ENDS_BEFORE: &str = "                  # End of Gateway API's own.";

/// How deep the Gateway's fields sit in the DataPlane's CRD: under `spec.gateway.properties`.
const COPY_INDENT: &str = "                  ";

fn path(dir: &str, file: &str) -> String {
    format!("{}/{dir}/{file}", env!("CARGO_MANIFEST_DIR"))
}

fn read(at: &str) -> String {
    std::fs::read_to_string(at)
        .unwrap_or_default()
        .replace("\r\n", "\n")
}

fn crd(yaml: &str) -> CustomResourceDefinition {
    serde_saphyr::from_str(yaml).expect("the CRD reads")
}

/// The properties of `spec` in `crd`'s `version`.
fn spec(crd: &CustomResourceDefinition, version: &str) -> BTreeMap<String, JSONSchemaProps> {
    let schema = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .and_then(|v| v.schema.as_ref())
        .and_then(|s| s.open_api_v3_schema.as_ref())
        .expect("the version has a schema");
    schema
        .properties
        .as_ref()
        .and_then(|p| p.get("spec"))
        .and_then(|s| s.properties.clone())
        .expect("the schema has a spec")
}

/// The Gateway's fields as Gateway API's Gateway CRD has them.
fn gateway_fields() -> BTreeMap<String, JSONSchemaProps> {
    let yaml = read(&format!(
        "{}/../gateway-api/crds/gateway.networking.k8s.io_gateways.yaml",
        env!("CARGO_MANIFEST_DIR")
    ));
    let mut spec = spec(&crd(&yaml), "v1");
    GATEWAY_FIELDS
        .iter()
        .map(|field| {
            let schema = spec.remove(*field).expect("the Gateway has the field");
            ((*field).to_owned(), schema)
        })
        .collect()
}

/// The Gateway's fields as the DataPlane's CRD holds them.
fn copied_fields(yaml: &str) -> BTreeMap<String, JSONSchemaProps> {
    spec(&crd(yaml), "v1alpha1")
        .remove("gateway")
        .and_then(|gateway| gateway.properties)
        .unwrap_or_default()
}

/// The DataPlane's CRD, `yaml`, with the Gateway's fields written between the markers.
fn with_gateway_fields(yaml: &str) -> String {
    let start = yaml
        .find(COPY_STARTS_AFTER)
        .map(|at| at + COPY_STARTS_AFTER.len() + 1)
        .expect("the DataPlane's CRD marks where the copy starts");
    let end = yaml
        .find(COPY_ENDS_BEFORE)
        .expect("the DataPlane's CRD marks where the copy ends");
    let written = serde_saphyr::to_string(&gateway_fields()).expect("the fields are written");
    let indented: String = written
        .lines()
        .map(|line| format!("{COPY_INDENT}{line}\n"))
        .collect();
    format!("{}{indented}{}", &yaml[..start], &yaml[end..])
}

/// The module `edgerush-crdgen` writes from the CRD `file`.
fn module(file: &str) -> String {
    let crd = crd(&read(&path("install", file)));
    let body = edgerush_crdgen::generate(&crd, "v1alpha1").expect("the CRD makes a module");
    format!(
        "//! `{kind}`, `{group}/v1alpha1`. Written by `edgerush-crdgen` from\n\
         //! `install/{file}`: not to be edited (the crate's docs say how it is written again).\n\n{body}",
        kind = crd.spec.names.kind,
        group = crd.spec.group,
    )
}

#[test]
fn the_data_planes_gateway_fields_are_the_gateways_and_the_modules_the_crds_make() {
    let write = std::env::var_os("EDGERUSH_WRITE_TYPES").is_some();
    let data_plane = path("install", DATA_PLANE);
    if write {
        let yaml = with_gateway_fields(&read(&data_plane));
        std::fs::write(&data_plane, yaml).expect("the CRD is written");
    }
    let copy_is_the_gateways = copied_fields(&read(&data_plane)) == gateway_fields();

    let mut stale = Vec::new();
    for (crd, module_file) in KINDS {
        let made = module(crd);
        let at = path("src", module_file);
        if write {
            std::fs::write(&at, &made).expect("the module is written");
            continue;
        }
        if read(&at) != made {
            stale.push(module_file);
        }
    }
    assert!(
        copy_is_the_gateways,
        "the DataPlane's spec.gateway is not the Gateway's own fields; write it again with \
         EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test generated"
    );
    assert!(
        stale.is_empty(),
        "{stale:?} are not what the CRDs make; write them again with \
         EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test generated"
    );
}
