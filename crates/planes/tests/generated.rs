//! The CRDs in `install/` and the modules of `src/` written from them by `edgerush-crdgen`.
//!
//! The DataPlane's `spec.gateway` holds the Gateway's own fields, copied from Gateway API's
//! Gateway CRD in `crates/gateway-api/crds`, and each `PlatformDefaults` entry's `dataPlane`
//! the DataPlane's own, but for those that come from the class and the Gateway; this fails
//! where a copy differs. To write the copies and the modules again, after a Gateway API
//! release is fetched or a CRD is changed, from the repository root:
//! `EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test generated`.

#![allow(
    clippy::expect_used,
    clippy::panic,
    reason = "test set-up: a CRD that cannot be read fails the test the way it would"
)]

use std::collections::BTreeMap;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, JSONSchemaProps, JSONSchemaPropsOrArray,
};

/// Each kind's CRD in `install/` and the module written from it, at `v1alpha1`.
const KINDS: [(&str, &str); 3] = [
    (
        "gateway.edgerush.dev_controlplanes.yaml",
        "control_plane.rs",
    ),
    ("gateway.edgerush.dev_dataplanes.yaml", "data_plane.rs"),
    (
        "gateway.edgerush.dev_platformdefaults.yaml",
        "platform_defaults.rs",
    ),
];

/// The DataPlane's CRD.
const DATA_PLANE: &str = "gateway.edgerush.dev_dataplanes.yaml";

/// The PlatformDefaults' CRD.
const PLATFORM_DEFAULTS: &str = "gateway.edgerush.dev_platformdefaults.yaml";

/// The Gateway's fields the DataPlane's `spec.gateway` holds.
const GATEWAY_FIELDS: [&str; 3] = ["allowedListeners", "listeners", "tls"];

/// The DataPlane's fields a `PlatformDefaults` entry's `dataPlane` does not hold: they come
/// from the class and the Gateway, and a generated DataPlane serves its Gateway alone.
const NOT_IN_AN_ENTRY: [&str; 4] = ["controlPlaneRef", "gateway", "gatewayRef", "ingress"];

/// Where a copy is written in a CRD: after the line `starts_after`, before `ends_before`,
/// each line indented by `indent`, as deep as the copy's properties sit.
struct Markers {
    starts_after: &'static str,
    ends_before: &'static str,
    indent: &'static str,
}

/// The Gateway's fields in the DataPlane's CRD, under `spec.gateway.properties`.
const GATEWAY_COPY: Markers = Markers {
    starts_after: "                  # Not to be edited.",
    ends_before: "                  # End of Gateway API's own.",
    indent: "                  ",
};

/// The DataPlane's fields in the PlatformDefaults' CRD, under
/// `spec.gatewayClasses.items.dataPlane.properties`.
const DATA_PLANE_COPY: Markers = Markers {
    starts_after: "                        # Not to be edited.",
    ends_before: "                        # End of the DataPlane's own.",
    indent: "                        ",
};

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

/// The Gateway's fields as the DataPlane's CRD, `yaml`, holds them.
fn copied_gateway_fields(yaml: &str) -> BTreeMap<String, JSONSchemaProps> {
    spec(&crd(yaml), "v1alpha1")
        .remove("gateway")
        .and_then(|gateway| gateway.properties)
        .unwrap_or_default()
}

/// The DataPlane's fields an entry holds, as the DataPlane's CRD, `yaml`, has them.
fn data_plane_fields(yaml: &str) -> BTreeMap<String, JSONSchemaProps> {
    let mut fields = spec(&crd(yaml), "v1alpha1");
    for field in NOT_IN_AN_ENTRY {
        assert!(
            fields.remove(field).is_some(),
            "the DataPlane has no {field}"
        );
    }
    fields
}

/// The DataPlane's fields as the PlatformDefaults' CRD, `yaml`, holds them.
fn copied_data_plane_fields(yaml: &str) -> BTreeMap<String, JSONSchemaProps> {
    let entry = spec(&crd(yaml), "v1alpha1")
        .remove("gatewayClasses")
        .and_then(|classes| classes.items)
        .expect("the PlatformDefaults has gatewayClasses");
    let JSONSchemaPropsOrArray::Schema(entry) = entry else {
        panic!("an entry of gatewayClasses is one schema");
    };
    entry
        .properties
        .and_then(|mut properties| properties.remove("dataPlane"))
        .and_then(|data_plane| data_plane.properties)
        .unwrap_or_default()
}

/// `yaml`, a CRD, with `fields` written between `markers`.
fn with_copy(yaml: &str, markers: &Markers, fields: &BTreeMap<String, JSONSchemaProps>) -> String {
    let start = yaml
        .find(markers.starts_after)
        .map(|at| at + markers.starts_after.len() + 1)
        .expect("the CRD marks where the copy starts");
    let end = yaml
        .find(markers.ends_before)
        .expect("the CRD marks where the copy ends");
    let written = serde_saphyr::to_string(fields).expect("the fields are written");
    let indented: String = written
        .lines()
        .map(|line| format!("{}{line}\n", markers.indent))
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
fn the_copies_are_what_they_copy_and_the_modules_what_the_crds_make() {
    let write = std::env::var_os("EDGERUSH_WRITE_TYPES").is_some();
    let data_plane = path("install", DATA_PLANE);
    let platform_defaults = path("install", PLATFORM_DEFAULTS);
    if write {
        let yaml = with_copy(&read(&data_plane), &GATEWAY_COPY, &gateway_fields());
        std::fs::write(&data_plane, yaml).expect("the CRD is written");
        // From the DataPlane's CRD as just written.
        let fields = data_plane_fields(&read(&data_plane));
        let yaml = with_copy(&read(&platform_defaults), &DATA_PLANE_COPY, &fields);
        std::fs::write(&platform_defaults, yaml).expect("the CRD is written");
    }
    let copy_is_the_gateways = copied_gateway_fields(&read(&data_plane)) == gateway_fields();
    let copy_is_the_data_planes = copied_data_plane_fields(&read(&platform_defaults))
        == data_plane_fields(&read(&data_plane));

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
        copy_is_the_data_planes,
        "the PlatformDefaults' gatewayClasses[].dataPlane is not the DataPlane's own fields; \
         write it again with EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test \
         generated"
    );
    assert!(
        stale.is_empty(),
        "{stale:?} are not what the CRDs make; write them again with \
         EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-planes --test generated"
    );
}
