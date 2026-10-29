//! Rust types for a Kubernetes custom resource, written from its CRD's OpenAPI schema, so
//! that EdgeRush reads the Gateway API's kinds with types made from the CRDs that define
//! them ([02 §7](../../../docs/02-architecture.md)). Adapted from kopium (NOTICE).
//!
//! A pure crate: a CRD in, a module's source out. The caller reads the CRD and writes the
//! source where it is kept.
//!
//! What is written: a struct for every object in the schema, named after the kind and the
//! fields on the way to it (`HTTPRouteRulesMatchesPath`), with serde's derives; a field the
//! schema does not require is an `Option`. **An enumerated string is a `String`**, its
//! values said in its docs, not a Rust enum as kopium makes it: a value from a newer
//! release or from the experimental channel is then read as it is, for translation to
//! judge, where an enum would fail the object and, with it, the whole list it came in. A
//! field the types do not have is passed over, and one they require but the object lacks
//! is read as its empty value, as Go's types read it, for the same reason. Kubernetes' standard
//! conditions and object references are k8s-openapi's types. The kind's own struct holds
//! its `metadata` and implements k8s-openapi's `Resource`, `ListableResource` and
//! `Metadata`, as the list and watch client asks.

mod analyzer;
mod case;
mod output;

use std::collections::BTreeSet;
use std::fmt::Write as _;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceDefinition;

use crate::output::{Container, doc_lines};

/// Why a module could not be written.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// The CRD defines no such version.
    #[error("the CRD defines no version {0}")]
    NoVersion(String),
    /// The version has no OpenAPI schema.
    #[error("version {0} has no schema")]
    NoSchema(String),
    /// The CRD's scope is neither `Namespaced` nor `Cluster`.
    #[error("the CRD's scope {0} is neither Namespaced nor Cluster")]
    Scope(String),
    /// Something in the schema that no type is written for.
    #[error("{0}")]
    Schema(String),
    /// Two structs that came to have one name.
    #[error("two structs are named {0}")]
    Clash(String),
}

/// The module source for `crd`'s `version`: its types, then its root's resource traits.
///
/// # Errors
///
/// As [`Error`] says.
pub fn generate(crd: &CustomResourceDefinition, version: &str) -> Result<String, Error> {
    let found = crd
        .spec
        .versions
        .iter()
        .find(|v| v.name == version)
        .ok_or_else(|| Error::NoVersion(version.to_owned()))?;
    let schema = found
        .schema
        .as_ref()
        .and_then(|s| s.open_api_v3_schema.as_ref())
        .ok_or_else(|| Error::NoSchema(version.to_owned()))?;
    let scope = match crd.spec.scope.as_str() {
        "Namespaced" => "NamespaceResourceScope",
        "Cluster" => "ClusterResourceScope",
        other => return Err(Error::Scope(other.to_owned())),
    };
    let names = &crd.spec.names;
    let kind = names.kind.as_str();
    let mut structs = analyzer::analyze(schema, kind)
        .and_then(output::Output::named)
        .map_err(Error::Schema)?;
    let root = match structs.first() {
        Some(c) if c.level == 0 => structs.remove(0),
        _ => {
            return Err(Error::Schema(format!(
                "the schema of {kind} is not an object"
            )));
        }
    };
    let spec = format!("{kind}Spec");
    for c in &mut structs {
        c.name = trimmed(&c.name, &spec, kind);
        for m in &mut c.members {
            m.type_ = trimmed(&m.type_, &spec, kind);
        }
    }
    let mut seen = BTreeSet::new();
    for c in &structs {
        if c.name == kind || !seen.insert(c.name.as_str()) {
            return Err(Error::Clash(c.name.clone()));
        }
    }

    let mut src = String::new();
    write_uses(&mut src, &root, &structs);
    write_root(&mut src, &root, kind);
    for c in &structs {
        write_struct(&mut src, c);
    }
    let api_version = if crd.spec.group.is_empty() {
        version.to_owned()
    } else {
        format!("{}/{version}", crd.spec.group)
    };
    let list_kind = names
        .list_kind
        .clone()
        .unwrap_or_else(|| format!("{kind}List"));
    // Infallible: writing to a String does not fail.
    let _ = write!(
        src,
        "impl k8s_openapi::Resource for {kind} {{
    const API_VERSION: &'static str = {api_version:?};
    const GROUP: &'static str = {group:?};
    const KIND: &'static str = {kind:?};
    const VERSION: &'static str = {version:?};
    const URL_PATH_SEGMENT: &'static str = {plural:?};
    type Scope = k8s_openapi::{scope};
}}

impl k8s_openapi::ListableResource for {kind} {{
    const LIST_KIND: &'static str = {list_kind:?};
}}

impl k8s_openapi::Metadata for {kind} {{
    type Ty = ObjectMeta;

    fn metadata(&self) -> &ObjectMeta {{
        &self.metadata
    }}

    fn metadata_mut(&mut self) -> &mut ObjectMeta {{
        &mut self.metadata
    }}
}}
",
        group = crd.spec.group,
        plural = names.plural,
    );
    Ok(src)
}

/// `text` with every name that begins `{kind}Spec` and goes on beginning `{kind}` alone, as
/// kopium names them: `HTTPRouteSpecRules` is `HTTPRouteRules`. `{kind}Spec` itself stays.
fn trimmed(text: &str, spec: &str, kind: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find(spec) {
        let (before, from) = rest.split_at(at);
        out.push_str(before);
        let after = &from[spec.len()..];
        let starts_word = before.is_empty() || !before.ends_with(|c: char| c.is_alphanumeric());
        if starts_word && after.starts_with(|c: char| c.is_ascii_uppercase()) {
            out.push_str(kind);
        } else {
            out.push_str(spec);
        }
        rest = after;
    }
    out.push_str(rest);
    out
}

/// The `use`s the types need, those of k8s-openapi only where they appear.
fn write_uses(src: &mut String, root: &Container, structs: &[Container]) {
    let uses = |name: &str| {
        std::iter::once(root)
            .chain(structs)
            .flat_map(|c| &c.members)
            .any(|m| {
                m.type_
                    .split(|c: char| !c.is_alphanumeric())
                    .any(|word| word == name)
            })
    };
    let mut meta = vec!["ObjectMeta"];
    if uses("Condition") {
        meta.insert(0, "Condition");
    }
    let meta = if meta.len() == 1 {
        meta.join("")
    } else {
        format!("{{{}}}", meta.join(", "))
    };
    let _ = writeln!(
        src,
        "use k8s_openapi::apimachinery::pkg::apis::meta::v1::{meta};"
    );
    if uses("ObjectReference") {
        src.push_str("use k8s_openapi::api::core::v1::ObjectReference;\n");
    }
    if uses("IntOrString") {
        src.push_str("use k8s_openapi::apimachinery::pkg::util::intstr::IntOrString;\n");
    }
    src.push_str("use serde::{Deserialize, Serialize};\n");
    if uses("BTreeMap") {
        src.push_str("use std::collections::BTreeMap;\n");
    }
    src.push('\n');
}

/// The kind's own struct: its metadata, then the root's fields but the three every object
/// has. It is read, not written: written, it would lack `apiVersion` and `kind`.
fn write_root(src: &mut String, root: &Container, kind: &str) {
    write_docs(src, "", root.docs.as_deref(), &[], kind);
    src.push_str("#[derive(Clone, Debug, Default, PartialEq, Deserialize)]\n");
    let _ = writeln!(src, "pub struct {kind} {{");
    src.push_str(
        "    /// The object's metadata.\n    #[serde(default)]\n    pub metadata: ObjectMeta,\n",
    );
    for m in root
        .members
        .iter()
        .filter(|m| !["apiVersion", "kind", "metadata"].contains(&m.json.as_str()))
    {
        write_member(src, m);
    }
    src.push_str("}\n\n");
}

fn write_struct(src: &mut String, c: &Container) {
    write_docs(src, "", c.docs.as_deref(), &[], &c.name);
    src.push_str("#[derive(Clone, Debug, Default, PartialEq, Deserialize, Serialize)]\n");
    let _ = writeln!(src, "pub struct {} {{", c.name);
    for m in &c.members {
        write_member(src, m);
    }
    src.push_str("}\n\n");
}

fn write_member(src: &mut String, m: &output::Member) {
    write_docs(src, "    ", m.docs.as_deref(), &m.values, &m.json);
    // A rename on a line of its own: together with the rest, the attribute would run past
    // what rustfmt keeps on one line, and the generated code is written as rustfmt has it.
    let (renames, rest): (Vec<&String>, Vec<&String>) =
        m.serde.iter().partition(|a| a.starts_with("rename"));
    for group in [renames, rest] {
        if !group.is_empty() {
            let group: Vec<&str> = group.into_iter().map(String::as_str).collect();
            let _ = writeln!(src, "    #[serde({})]", group.join(", "));
        }
    }
    // rustfmt's width, past which it puts the type on a line of its own.
    let line = format!("    pub {}: {},", m.name, m.type_);
    if line.len() <= 100 {
        let _ = writeln!(src, "{line}");
    } else {
        let _ = writeln!(src, "    pub {}:\n        {},", m.name, m.type_);
    }
}

/// A doc comment from the schema's description, or one naming `what` where it has none;
/// an enumerated string's values after it.
fn write_docs(src: &mut String, indent: &str, docs: Option<&str>, values: &[String], what: &str) {
    match docs.map(str::trim).filter(|d| !d.is_empty()) {
        Some(docs) => src.push_str(&doc_lines(indent, docs)),
        None => {
            let _ = writeln!(
                src,
                "{indent}/// `{what}`, which the schema does not describe."
            );
        }
    }
    if !values.is_empty() {
        let listed: Vec<String> = values.iter().map(|v| format!("`{v}`")).collect();
        let _ = writeln!(
            src,
            "{indent}///\n{indent}/// The schema's values: {}. Another is read as it is.",
            listed.join(", ")
        );
    }
}

#[cfg(test)]
mod tests;
