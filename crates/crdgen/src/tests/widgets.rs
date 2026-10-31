use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, ObjectMeta};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// A widget. See <https://example.com/widgets>.
#[derive(Clone, Debug, Default, PartialEq, Deserialize)]
pub struct Widget {
    /// The object's metadata.
    #[serde(default)]
    pub metadata: ObjectMeta,
    /// What the widget should be.
    #[serde(default)]
    pub spec: WidgetSpec,
    /// `status`, which the schema does not describe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<WidgetStatus>,
}

/// What the widget should be.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct WidgetSpec {
    /// How many parts it may have.
    #[serde(rename = "partCount")]
    #[serde(default = "default_widget_spec_part_count")]
    pub part_count: i32,
    /// Its parts.
    ///
    /// ```text
    /// parts: [{ kind: Gear }]
    /// ```
    #[serde(default = "default_widget_spec_parts")]
    pub parts: Vec<WidgetParts>,
}

/// `WidgetParts`, which the schema does not describe.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct WidgetParts {
    /// The kind of part.
    ///
    /// The schema's values: `Gear`, `Spring`. Another is read as it is.
    #[serde(default)]
    pub kind: String,
    /// `labels`, which the schema does not describe.
    #[serde(default = "default_widget_parts_labels")]
    pub labels: BTreeMap<String, String>,
}

/// `WidgetStatus`, which the schema does not describe.
#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]
pub struct WidgetStatus {
    /// `conditions`, which the schema does not describe.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conditions: Option<Vec<Condition>>,
}

/// The schema's default.
#[rustfmt::skip]
fn default_widget_spec_part_count() -> i32 {
    4
}

/// The schema's default.
#[rustfmt::skip]
fn default_widget_spec_parts() -> Vec<WidgetParts> {
    vec![WidgetParts { kind: String::from("Gear"), labels: default_widget_parts_labels() }]
}

/// The schema's default.
#[rustfmt::skip]
fn default_widget_parts_labels() -> BTreeMap<String, String> {
    BTreeMap::from([(String::from("colour"), String::from("blue"))])
}

impl k8s_openapi::Resource for Widget {
    const API_VERSION: &'static str = "example.com/v1";
    const GROUP: &'static str = "example.com";
    const KIND: &'static str = "Widget";
    const VERSION: &'static str = "v1";
    const URL_PATH_SEGMENT: &'static str = "widgets";
    type Scope = k8s_openapi::NamespaceResourceScope;
}

impl k8s_openapi::ListableResource for Widget {
    const LIST_KIND: &'static str = "WidgetList";
}

impl k8s_openapi::Metadata for Widget {
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut ObjectMeta {
        &mut self.metadata
    }
}
