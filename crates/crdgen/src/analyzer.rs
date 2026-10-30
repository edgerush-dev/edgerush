//! A schema walked into the structs it needs and their fields. Adapted from kopium's
//! `analyzer.rs` (NOTICE); the one change of substance is that an enumerated string is a
//! `String`, its values kept for its docs.

use std::collections::BTreeMap;

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::{
    JSON, JSONSchemaProps, JSONSchemaPropsOrArray, JSONSchemaPropsOrBool,
};

use crate::case::upper_camel;
use crate::output::{Container, Member, Output};

/// The root's fields that are not walked: every object has them, and the generated root
/// writes them itself.
const ROOT_FIELDS: [&str; 3] = ["metadata", "apiVersion", "kind"];

/// The structs `schema` needs, each named after `kind` and the fields on the way to it.
pub(crate) fn analyze(schema: &JSONSchemaProps, kind: &str) -> Result<Output, String> {
    let mut out = Output::default();
    analyze_(schema, kind, 0, &mut out)?;
    Ok(out)
}

/// Adds the struct `schema` is, if it is one, and then those of its fields.
///
/// - `stack`: the name so far, the kind's and each field's on the way down;
/// - `level`: how deep, 0 at the root.
fn analyze_(
    schema: &JSONSchemaProps,
    stack: &str,
    level: u8,
    out: &mut Output,
) -> Result<(), String> {
    let none = BTreeMap::new();
    let props = schema.properties.as_ref().unwrap_or(&none);
    // How deep each array field's items are nested, found as its type is: an array of
    // arrays of objects is two.
    let mut depths: BTreeMap<String, u8> = BTreeMap::new();

    if schema.type_.as_deref() == Some("object") {
        // An object has properties or additionalProperties, not both.
        if let Some(JSONSchemaPropsOrBool::Schema(s)) = schema.additional_properties.as_ref() {
            if let Some(extra) = &s.properties {
                // A map whose values are objects with properties: a struct for its values.
                out.insert(container(extra, stack, &mut depths, level, schema)?);
            } else if s.type_.as_deref() == Some("object")
                && (s.properties.is_some() || s.additional_properties.is_some())
            {
                // A map of maps: walk on to its values. An object value with neither is
                // free-form, `serde_json::Value`, and has no struct (kopium issue #348).
                analyze_(s, stack, level, out)?;
            } else if s.type_.is_some() {
                // A map of scalars, written in place.
                return Ok(());
            }
        } else {
            if props.is_empty() && schema.x_kubernetes_preserve_unknown_fields == Some(true) {
                // Free-form, written in place as a map.
                return Ok(());
            }
            out.insert(container(props, stack, &mut depths, level, schema)?);
        }
    }

    let fields = match schema.additional_properties.as_ref() {
        Some(JSONSchemaPropsOrBool::Schema(s)) => s.properties.as_ref().unwrap_or(&none),
        _ => props,
    };
    out.extend(walk(fields, stack, &depths, level)?);
    Ok(())
}

/// The structs `props`' fields need, each walked in turn.
fn walk(
    props: &BTreeMap<String, JSONSchemaProps>,
    stack: &str,
    depths: &BTreeMap<String, u8>,
    level: u8,
) -> Result<Output, String> {
    let mut out = Output::default();
    for (key, value) in props {
        if level == 0 && ROOT_FIELDS.contains(&key.as_str()) {
            continue;
        }
        let next = format!("{stack}{}", upper_camel(key));
        match value.type_.as_deref() {
            // k8s-openapi's type, not a struct of our own.
            Some("object") if is_object_ref(value) => {}
            Some("object") => {
                // A map of arrays: the struct is that of the arrays' items.
                if let Some(JSONSchemaPropsOrBool::Schema(s)) = &value.additional_properties
                    && s.type_.as_deref() == Some("array")
                    && let Some(JSONSchemaPropsOrArray::Schema(items)) = &s.items
                {
                    analyze_(items, &next, level + 1, &mut out)?;
                } else {
                    analyze_(value, &next, level + 1, &mut out)?;
                }
            }
            Some("array") => {
                if let Some(&depth) = depths.get(key) {
                    let mut inner = value;
                    for _ in 0..depth {
                        inner = match &inner.items {
                            Some(JSONSchemaPropsOrArray::Schema(s)) => s,
                            Some(_) => {
                                return Err(format!("array {key} has items of several types"));
                            }
                            None => return Err(format!("array {key} has no items")),
                        };
                    }
                    analyze_(inner, &next, level + 1, &mut out)?;
                }
            }
            // Scalars, enumerated or not, and int-or-string: no struct.
            _ => {}
        }
    }
    Ok(out)
}

/// The struct `props` are the fields of, every field given its type.
fn container(
    props: &BTreeMap<String, JSONSchemaProps>,
    stack: &str,
    depths: &mut BTreeMap<String, u8>,
    level: u8,
    schema: &JSONSchemaProps,
) -> Result<Container, String> {
    let required = schema.required.as_deref().unwrap_or_default();
    let mut members = Vec::with_capacity(props.len());
    for (key, value) in props {
        let type_ = match value.type_.as_deref() {
            Some("object") => object_type(value, stack, key)?,
            Some("string") => "String".to_owned(),
            Some("boolean") => "bool".to_owned(),
            Some("number") => number_type(value).to_owned(),
            Some("integer") => integer_type(value).to_owned(),
            Some("array") => {
                let (array, depth) = array_type(value, stack, key, 1)?;
                if key == "conditions" && is_conditions(value) {
                    "Vec<Condition>".to_owned()
                } else if is_object_ref_list(value) {
                    "Vec<ObjectReference>".to_owned()
                } else {
                    depths.insert(key.clone(), depth);
                    array
                }
            }
            None => {
                if value.x_kubernetes_int_or_string == Some(true) {
                    "IntOrString".to_owned()
                } else if value.x_kubernetes_preserve_unknown_fields == Some(true)
                    || value
                        .one_of
                        .as_deref()
                        .is_some_and(|items| items.iter().all(|item| item.type_.is_some()))
                {
                    "serde_json::Value".to_owned()
                } else {
                    return Err(format!("field {key} of {stack} has no type"));
                }
            }
            Some(other) => {
                return Err(format!("field {key} of {stack} is of unknown type {other}"));
            }
        };
        let values = match (&value.type_, &value.enum_) {
            (Some(t), Some(values)) if t == "string" => string_values(values, stack, key)?,
            _ => Vec::new(),
        };
        // A required field missing is read as its empty value, as Go's types read it, so
        // that an object of an older release, or another channel, without it is still read
        // and a list with it in is not failed whole; translation judges the empty value.
        let (type_, serde) = if required.contains(key) {
            (type_, vec!["default".to_owned()])
        } else {
            (
                format!("Option<{type_}>"),
                vec![
                    "default".to_owned(),
                    "skip_serializing_if = \"Option::is_none\"".to_owned(),
                ],
            )
        };
        members.push(Member {
            name: key.clone(),
            json: key.clone(),
            default: value.default.as_ref().map(|default| default.0.clone()),
            type_,
            serde,
            docs: value.description.clone(),
            values,
        });
    }
    Ok(Container {
        name: stack.to_owned(),
        members,
        level,
        docs: schema.description.clone(),
    })
}

/// The values an enumerated string may take.
fn string_values(values: &[JSON], stack: &str, key: &str) -> Result<Vec<String>, String> {
    values
        .iter()
        .map(|value| match &value.0 {
            serde_json::Value::String(s) => Ok(s.clone()),
            other => Err(format!(
                "field {key} of {stack} enumerates a value not a string: {other}"
            )),
        })
        .collect()
}

/// The type of a map's values.
fn map_value_type(
    additional: &JSONSchemaPropsOrBool,
    stack: &str,
    key: &str,
) -> Result<Option<String>, String> {
    let JSONSchemaPropsOrBool::Schema(s) = additional else {
        return Ok(None);
    };
    Ok(Some(match s.type_.as_deref() {
        Some("string") => "String".to_owned(),
        Some("array") => array_type(s, stack, key, 1)?.0,
        Some("object") => {
            // A free-form object value (kopium issue #348).
            if s.properties.is_none() && s.additional_properties.is_none() {
                "serde_json::Value".to_owned()
            } else {
                object_type(s, stack, key)?
            }
        }
        Some("boolean") => "bool".to_owned(),
        Some("number") => number_type(s).to_owned(),
        Some("integer") => integer_type(s).to_owned(),
        None if s.x_kubernetes_int_or_string == Some(true) => "IntOrString".to_owned(),
        None if s.x_kubernetes_preserve_unknown_fields == Some(true) => {
            "serde_json::Value".to_owned()
        }
        None => return Err(format!("map {key} of {stack} has values of no type")),
        Some(other) => {
            return Err(format!(
                "map {key} of {stack} has values of unknown type {other}"
            ));
        }
    }))
}

/// An array's type, and how deep its items are nested, found by walking down through
/// arrays of arrays to the items' own type.
fn array_type(
    value: &JSONSchemaProps,
    stack: &str,
    key: &str,
    depth: u8,
) -> Result<(String, u8), String> {
    let s = match &value.items {
        Some(JSONSchemaPropsOrArray::Schema(s)) => s,
        Some(_) => return Err(format!("array {key} of {stack} has items of several types")),
        None => return Err(format!("array {key} of {stack} has no items")),
    };
    if s.type_.is_none() && s.x_kubernetes_preserve_unknown_fields == Some(true) {
        return Ok(("Vec<serde_json::Value>".to_owned(), depth));
    }
    let items = match s.type_.as_deref() {
        Some("object") => object_type(s, stack, key)?,
        Some("string") => "String".to_owned(),
        Some("boolean") => "bool".to_owned(),
        Some("number") => number_type(s).to_owned(),
        Some("integer") => integer_type(s).to_owned(),
        Some("array") => {
            let (inner, depth) = array_type(s, stack, key, depth + 1)?;
            return Ok((format!("Vec<{inner}>"), depth));
        }
        None if s.x_kubernetes_int_or_string == Some(true) => "IntOrString".to_owned(),
        None => return Err(format!("array {key} of {stack} has items of no type")),
        Some(other) => {
            return Err(format!(
                "array {key} of {stack} has items of unknown type {other}"
            ));
        }
    };
    Ok((format!("Vec<{items}>"), depth))
}

/// Whether an array is of Kubernetes' standard conditions, which k8s-openapi has a type for.
fn is_conditions(value: &JSONSchemaProps) -> bool {
    let Some(JSONSchemaPropsOrArray::Schema(items)) = &value.items else {
        return false;
    };
    items.properties.as_ref().is_some_and(|p| {
        ["type", "status", "reason", "message", "lastTransitionTime"]
            .iter()
            .all(|field| p.contains_key(*field))
    })
}

fn is_object_ref_list(value: &JSONSchemaProps) -> bool {
    match &value.items {
        Some(JSONSchemaPropsOrArray::Schema(items)) => is_object_ref(items),
        _ => false,
    }
}

/// Whether an object is core's `ObjectReference`, which k8s-openapi has a type for: its
/// seven fields and no others.
fn is_object_ref(value: &JSONSchemaProps) -> bool {
    value.properties.as_ref().is_some_and(|p| {
        p.len() == 7
            && [
                "apiVersion",
                "fieldPath",
                "kind",
                "name",
                "namespace",
                "resourceVersion",
                "uid",
            ]
            .iter()
            .all(|field| p.contains_key(*field))
    })
}

/// An object's type: a map, free-form, `ObjectReference`, or the struct named after it.
fn object_type(value: &JSONSchemaProps, stack: &str, key: &str) -> Result<String, String> {
    let map_values = if let Some(additional) = &value.additional_properties {
        map_value_type(additional, stack, key)?
    } else if value.properties.is_none() && value.x_kubernetes_preserve_unknown_fields == Some(true)
    {
        Some("serde_json::Value".to_owned())
    } else {
        None
    };
    Ok(if let Some(values) = map_values {
        format!("BTreeMap<String, {values}>")
    } else if is_object_ref(value) {
        "ObjectReference".to_owned()
    } else {
        format!("{stack}{}", upper_camel(key))
    })
}

fn number_type(value: &JSONSchemaProps) -> &'static str {
    match value.format.as_deref() {
        Some("float") => "f32",
        _ => "f64",
    }
}

/// An integer's type from its format; Kubernetes' Go types are signed, and so are these.
fn integer_type(value: &JSONSchemaProps) -> &'static str {
    match value.format.as_deref() {
        Some("int8") => "i8",
        Some("int16") => "i16",
        Some("int32") => "i32",
        Some("uint8") => "u8",
        Some("uint16") => "u16",
        Some("uint32") => "u32",
        Some("uint64") => "u64",
        _ => "i64",
    }
}
