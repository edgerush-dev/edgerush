//! The analyzer's cases are kopium's (NOTICE), each schema as kopium's test has it, with
//! what this crate makes of it: an enumerated string a `String`, names as `case` spells
//! them. Then whole CRDs written.

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    CustomResourceDefinition, JSONSchemaProps,
};

use crate::output::Container;
use crate::{Error, analyzer, generate};

fn analyzed(schema: &str, kind: &str) -> Vec<Container> {
    let schema: JSONSchemaProps = serde_saphyr::from_str(schema).unwrap();
    analyzer::analyze(&schema, kind).unwrap().named().unwrap()
}

fn types(c: &Container) -> Vec<(&str, &str)> {
    c.members
        .iter()
        .map(|m| (m.json.as_str(), m.type_.as_str()))
        .collect()
}

#[test]
fn map_of_struct() {
    let structs = analyzed(
        r#"
        description: AgentStatus defines the observed state of Agent
        properties:
          validationsInfo:
            additionalProperties:
              items:
                properties:
                  id:
                    type: string
                  message:
                    type: string
                  status:
                    type: string
                required: [id, message, status]
                type: object
              type: array
            type: object
        type: object
        "#,
        "Agent",
    );
    assert_eq!(structs[0].name, "Agent");
    assert_eq!(structs[0].level, 0);
    assert_eq!(
        types(&structs[0]),
        [(
            "validationsInfo",
            "Option<BTreeMap<String, Vec<AgentValidationsInfo>>>"
        )]
    );
    assert_eq!(structs[1].name, "AgentValidationsInfo");
    assert_eq!(structs[1].level, 1);
    assert_eq!(
        types(&structs[1]),
        [
            ("id", "String"),
            ("message", "String"),
            ("status", "String")
        ]
    );
}

#[test]
fn map_of_map() {
    let structs = analyzed(
        r#"
        properties:
          instances:
            additionalProperties:
              additionalProperties:
                type: string
              type: object
            type: object
        type: object
        "#,
        "ClusterStatusTopology",
    );
    assert_eq!(structs.len(), 1);
    assert_eq!(
        types(&structs[0]),
        [(
            "instances",
            "Option<BTreeMap<String, BTreeMap<String, String>>>"
        )]
    );
}

#[test]
fn maps_of_scalars() {
    let structs = analyzed(
        r#"
        properties:
          weights:
            additionalProperties: { type: number }
            type: object
          options:
            additionalProperties: { type: boolean }
            type: object
          counts:
            additionalProperties: { type: integer, format: int32 }
            type: object
        type: object
        "#,
        "Maps",
    );
    assert_eq!(
        types(&structs[0]),
        [
            ("counts", "Option<BTreeMap<String, i32>>"),
            ("options", "Option<BTreeMap<String, bool>>"),
            ("weights", "Option<BTreeMap<String, f64>>"),
        ]
    );
}

#[test]
fn free_form_objects_are_values_or_maps() {
    let structs = analyzed(
        r#"
        properties:
          selector:
            type: object
            x-kubernetes-preserve-unknown-fields: true
          values:
            additionalProperties:
              type: object
            type: object
          items:
            x-kubernetes-preserve-unknown-fields: true
          list:
            items:
              x-kubernetes-preserve-unknown-fields: true
            type: array
        required: [items]
        type: object
        "#,
        "Free",
    );
    assert_eq!(
        structs.len(),
        1,
        "no struct for a free-form object (kopium #348)"
    );
    assert_eq!(
        types(&structs[0]),
        [
            ("items", "serde_json::Value"),
            ("list", "Option<Vec<serde_json::Value>>"),
            ("selector", "Option<BTreeMap<String, serde_json::Value>>"),
            ("values", "Option<BTreeMap<String, serde_json::Value>>"),
        ]
    );
}

#[test]
fn int_or_string() {
    let structs = analyzed(
        r#"
        properties:
          port:
            x-kubernetes-int-or-string: true
          ports:
            items:
              x-kubernetes-int-or-string: true
            type: array
        required: [port]
        type: object
        "#,
        "Server",
    );
    assert_eq!(
        types(&structs[0]),
        [
            ("port", "IntOrString"),
            ("ports", "Option<Vec<IntOrString>>")
        ]
    );
}

#[test]
fn string_or_string_array() {
    let structs = analyzed(
        r#"
        type: object
        required: [ambassadorId]
        properties:
          ambassadorId:
            items: { type: string }
            oneOf: [{ type: string }, { type: array }]
          other:
            items: { type: string }
            oneOf: [{ type: string }, { type: array }]
        "#,
        "Host",
    );
    assert_eq!(
        types(&structs[0]),
        [
            ("ambassadorId", "serde_json::Value"),
            ("other", "Option<serde_json::Value>")
        ]
    );
}

#[test]
fn an_enumerated_string_is_a_string_with_its_values() {
    let structs = analyzed(
        r#"
        properties:
          operator:
            enum: [In, NotIn, Exists, DoesNotExist]
            type: string
          optional:
            enum: [A, B]
            type: string
        required: [operator]
        type: object
        "#,
        "MatchExpressions",
    );
    assert_eq!(structs.len(), 1, "no enum is made");
    assert_eq!(
        types(&structs[0]),
        [("operator", "String"), ("optional", "Option<String>")]
    );
    assert_eq!(
        structs[0].members[0].values,
        ["In", "NotIn", "Exists", "DoesNotExist"]
    );
    assert_eq!(structs[0].members[1].values, ["A", "B"]);
}

#[test]
fn an_enumerated_value_not_a_string_is_an_error() {
    let schema: JSONSchemaProps =
        serde_saphyr::from_str("properties: { code: { type: string, enum: [1] } }\ntype: object")
            .unwrap();
    assert!(analyzer::analyze(&schema, "Codes").is_err());
}

#[test]
fn nested_properties_in_additional_properties() {
    let structs = analyzed(
        r#"
        properties:
          jwtTokensByRole:
            additionalProperties:
              properties:
                items:
                  items:
                    properties:
                      exp: { format: int64, type: integer }
                      iat: { format: int64, type: integer }
                      id: { type: string }
                    required: [iat]
                    type: object
                  type: array
              type: object
            type: object
        type: object
        "#,
        "AppProjectStatus",
    );
    assert_eq!(
        types(&structs[0]),
        [(
            "jwtTokensByRole",
            "Option<BTreeMap<String, AppProjectStatusJwtTokensByRole>>"
        )]
    );
    assert_eq!(structs[1].name, "AppProjectStatusJwtTokensByRole");
    assert_eq!(structs[1].level, 1);
    assert_eq!(
        types(&structs[1]),
        [("items", "Option<Vec<AppProjectStatusJwtTokensByRoleItems>>")]
    );
    assert_eq!(structs[2].name, "AppProjectStatusJwtTokensByRoleItems");
    assert_eq!(structs[2].level, 2);
    assert_eq!(
        types(&structs[2]),
        [
            ("exp", "Option<i64>"),
            ("iat", "i64"),
            ("id", "Option<String>")
        ]
    );
}

#[test]
fn array_of_array() {
    let structs = analyzed(
        r#"
        properties:
          mounts:
            items:
              items: { type: string }
              type: array
            type: array
          grid:
            items:
              items:
                properties:
                  x: { type: integer }
                type: object
              type: array
            type: array
        type: object
        "#,
        "KubeadmConfig",
    );
    assert_eq!(
        types(&structs[0]),
        [
            ("grid", "Option<Vec<Vec<KubeadmConfigGrid>>>"),
            ("mounts", "Option<Vec<Vec<String>>>"),
        ]
    );
    assert_eq!(structs[1].name, "KubeadmConfigGrid");
    assert_eq!(types(&structs[1]), [("x", "Option<i64>")]);
}

#[test]
fn names_keep_a_kinds_capitals() {
    let structs = analyzed(
        r#"
        properties:
          openAPIV3Schema:
            properties:
              items: { type: string }
            type: object
        required: [openAPIV3Schema]
        type: object
        "#,
        "HTTPRoute",
    );
    assert_eq!(
        types(&structs[0]),
        [("openAPIV3Schema", "HTTPRouteOpenAPIV3Schema")]
    );
    assert_eq!(structs[0].members[0].name, "open_apiv3_schema");
    assert_eq!(structs[1].name, "HTTPRouteOpenAPIV3Schema");
}

#[test]
fn conditions_and_object_references_are_k8s_openapis() {
    let structs = analyzed(
        r#"
        properties:
          conditions:
            items:
              properties:
                lastTransitionTime: { type: string, format: date-time }
                message: { type: string }
                observedGeneration: { type: integer, format: int64 }
                reason: { type: string }
                status: { type: string }
                type: { type: string }
              required: [lastTransitionTime, message, reason, status, type]
              type: object
            type: array
          target:
            properties:
              apiVersion: { type: string }
              fieldPath: { type: string }
              kind: { type: string }
              name: { type: string }
              namespace: { type: string }
              resourceVersion: { type: string }
              uid: { type: string }
            type: object
        type: object
        "#,
        "Thing",
    );
    assert_eq!(structs.len(), 1);
    assert_eq!(
        types(&structs[0]),
        [
            ("conditions", "Option<Vec<Condition>>"),
            ("target", "Option<ObjectReference>")
        ]
    );
}

#[test]
fn a_field_of_no_type_is_an_error() {
    let schema: JSONSchemaProps =
        serde_saphyr::from_str("properties: { odd: { description: none } }\ntype: object").unwrap();
    assert!(analyzer::analyze(&schema, "Odd").is_err());
}

/// A CRD as the Gateway API publishes one, small.
const WIDGETS: &str = r#"
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names: { kind: Widget, listKind: WidgetList, plural: widgets, singular: widget }
  scope: Namespaced
  versions:
  - name: v1beta1
    served: true
    storage: false
    schema:
      openAPIV3Schema: { type: object }
  - name: v1
    served: true
    storage: true
    subresources: { status: {} }
    schema:
      openAPIV3Schema:
        description: A widget. See https://example.com/widgets.
        type: object
        properties:
          apiVersion: { type: string }
          kind: { type: string }
          metadata: { type: object }
          spec:
            description: What the widget should be.
            type: object
            required: [parts]
            properties:
              partCount:
                description: How many parts it may have.
                type: integer
                format: int32
                default: 4
              parts:
                default: [{ kind: Gear }]
                description: |-
                  Its parts.

                  ```yaml
                  parts: [{ kind: Gear }]
                  ```
                type: array
                items:
                  type: object
                  required: [kind]
                  properties:
                    kind:
                      description: The kind of part.
                      type: string
                      enum: [Gear, Spring]
                    labels:
                      type: object
                      additionalProperties: { type: string }
                      default: { colour: blue }
          status:
            type: object
            properties:
              conditions:
                default: []
                type: array
                items:
                  type: object
                  required: [lastTransitionTime, message, reason, status, type]
                  properties:
                    lastTransitionTime: { type: string, format: date-time }
                    message: { type: string }
                    reason: { type: string }
                    status: { type: string }
                    type: { type: string }
        required: [spec]
"#;

fn crd(yaml: &str) -> CustomResourceDefinition {
    serde_saphyr::from_str(yaml).unwrap()
}

/// The module written for [`WIDGETS`], compiled: an object read as an older release might
/// have one, without the fields the schema requires.
#[allow(
    dead_code,
    missing_docs,
    unreachable_pub,
    reason = "only read back by the test"
)]
mod widgets {
    include!("tests/widgets.rs");
}

#[test]
fn a_required_field_missing_is_read_empty() {
    let widget: widgets::Widget =
        serde_saphyr::from_str("metadata: { name: w }\nspec: { parts: [{}] }").unwrap();
    assert_eq!(widget.spec.parts.len(), 1);
    assert_eq!(widget.spec.parts[0].kind, "");
    let widget: widgets::Widget = serde_saphyr::from_str("metadata: { name: w }").unwrap();
    assert!(
        widget.spec.parts.is_empty(),
        "a spec left out is empty, no default made in it"
    );
}

/// The module for [`WIDGETS`] is `src/tests/widgets.rs`, as written, and compiles (above).
#[test]
fn a_crd_is_written_as_a_module() {
    let written = generate(&crd(WIDGETS), "v1").unwrap();
    // `EDGERUSH_WRITE_TYPES=1 cargo test -p edgerush-crdgen` writes it again, to be read over.
    if std::env::var_os("EDGERUSH_WRITE_TYPES").is_some() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/src/tests/widgets.rs");
        std::fs::write(path, &written).unwrap();
    }
    let compiled = include_str!("tests/widgets.rs").replace("\r\n", "\n");
    assert_eq!(compiled, written);
}

#[test]
fn a_field_left_out_holds_the_schemas_default_its_own_nested_ones_with_it() {
    let widget: widgets::Widget =
        serde_saphyr::from_str("metadata: { name: w }\nspec: {}\nstatus: {}").unwrap();
    assert_eq!(widget.spec.part_count, 4);
    assert_eq!(widget.spec.parts.len(), 1);
    assert_eq!(widget.spec.parts[0].kind, "Gear");
    // The default list's item leaves `labels` out: its own default is applied.
    let blue = std::collections::BTreeMap::from([("colour".to_owned(), "blue".to_owned())]);
    assert_eq!(widget.spec.parts[0].labels, blue);
    // A field the object states keeps what it states; the status takes no default.
    let widget: widgets::Widget = serde_saphyr::from_str(
        "metadata: { name: w }\nspec: { partCount: 2, parts: [{ kind: Spring, labels: {} }] }\nstatus: {}",
    )
    .unwrap();
    assert_eq!(widget.spec.part_count, 2);
    assert_eq!(widget.spec.parts[0].kind, "Spring");
    assert!(widget.spec.parts[0].labels.is_empty());
    assert_eq!(widget.status.unwrap().conditions, None);
}

#[test]
fn a_cluster_kind_has_cluster_scope() {
    let written = generate(&crd(&WIDGETS.replace("Namespaced", "Cluster")), "v1").unwrap();
    assert!(written.contains("type Scope = k8s_openapi::ClusterResourceScope;"));
}

#[test]
fn what_cannot_be_written_is_said() {
    assert_eq!(
        generate(&crd(WIDGETS), "v2"),
        Err(Error::NoVersion("v2".to_owned()))
    );
    assert_eq!(
        generate(&crd(&WIDGETS.replace("Namespaced", "Galactic")), "v1"),
        Err(Error::Scope("Galactic".to_owned()))
    );
    // `spec.status` becomes `WidgetStatus`, as the root's `status` does.
    let clashing = WIDGETS.replace(
        "            required: [parts]\n            properties:\n",
        "            required: [parts]\n            properties:\n              status:\n                type: object\n                properties: { x: { type: string } }\n",
    );
    assert_eq!(
        generate(&crd(&clashing), "v1"),
        Err(Error::Clash("WidgetStatus".to_owned()))
    );
    // A default of a field its schema does not have cannot be written.
    let odd = WIDGETS.replace(
        "default: [{ kind: Gear }]",
        "default: [{ kind: Gear, size: 3 }]",
    );
    assert_eq!(
        generate(&crd(&odd), "v1"),
        Err(Error::Schema(
            "the default of default_widget_spec_parts: WidgetParts has no field size".to_owned()
        ))
    );
    let odd = WIDGETS.replace("default: 4", "default: four");
    assert_eq!(
        generate(&crd(&odd), "v1"),
        Err(Error::Schema(
            "the default of default_widget_spec_part_count: \"four\" is not an integer".to_owned()
        ))
    );
}
