//! The schema's defaults, written into the types: a field the schema gives a default is
//! never left empty, but holds what the API server would have stored, so that an object read
//! from a file decodes as the same object read from the cluster. The status is left as it
//! is: it is a controller's to write.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use serde_json::Value;

use crate::case;
use crate::output::Container;

/// Rewrites each member of `structs` and of `root` that the schema gives a default, outside
/// the status (structs named `{kind}Status...`, and the root's `status`): its type plain, no
/// longer optional, and read with a function that returns the default. Those functions'
/// source.
pub(crate) fn apply(
    root: &mut Container,
    structs: &mut [Container],
    kind: &str,
) -> Result<String, String> {
    let status = format!("{kind}Status");
    let mut defaulted: Vec<(String, String, Value)> = Vec::new();
    for c in std::iter::once(&mut *root).chain(structs.iter_mut()) {
        if c.name.starts_with(&status) {
            continue;
        }
        let container = c.name.clone();
        for m in &mut c.members {
            let Some(value) = m.default.take() else {
                continue;
            };
            if c.level == 0 && m.json == "status" {
                continue;
            }
            let function = function_name(&container, &m.name);
            if let Some(inner) = unwrapped(&m.type_, "Option<") {
                m.type_ = inner.to_owned();
            }
            m.serde.retain(|a| a.starts_with("rename"));
            m.serde.push(format!("default = \"{function}\""));
            defaulted.push((function, m.type_.clone(), value));
        }
    }
    let by_name: BTreeMap<&str, &Container> =
        structs.iter().map(|c| (c.name.as_str(), c)).collect();
    let mut src = String::new();
    for (function, type_, value) in &defaulted {
        let literal = literal(value, type_, &by_name)
            .map_err(|why| format!("the default of {function}: {why}"))?;
        // Infallible: writing to a String does not fail.
        let _ = write!(
            src,
            "/// The schema's default.\n#[rustfmt::skip]\nfn {function}() -> {type_} {{\n    {literal}\n}}\n\n"
        );
    }
    Ok(src)
}

/// The function a member's default is read with: `default_http_route_rules_matches`.
fn function_name(container: &str, member: &str) -> String {
    format!(
        "default_{}_{}",
        case::snake(container),
        member.trim_start_matches("r#").trim_end_matches('_')
    )
}

/// `ty` without its `prefix` and closing `>`, where it has them.
fn unwrapped<'a>(ty: &'a str, prefix: &str) -> Option<&'a str> {
    ty.strip_prefix(prefix)?.strip_suffix('>')
}

/// `value` as a Rust expression of type `ty`, among `structs`. A struct's member the value
/// leaves out takes its own default, where it has one, as the API server applies nested
/// defaults, and is otherwise empty.
fn literal(
    value: &Value,
    ty: &str,
    structs: &BTreeMap<&str, &Container>,
) -> Result<String, String> {
    if let Some(inner) = unwrapped(ty, "Option<") {
        return Ok(if value.is_null() {
            "None".to_owned()
        } else {
            format!("Some({})", literal(value, inner, structs)?)
        });
    }
    if let Some(inner) = unwrapped(ty, "Vec<") {
        let items = value
            .as_array()
            .ok_or_else(|| format!("{value} is not a list"))?;
        if items.is_empty() {
            return Ok("Vec::new()".to_owned());
        }
        let items: Vec<String> = items
            .iter()
            .map(|item| literal(item, inner, structs))
            .collect::<Result<_, _>>()?;
        return Ok(format!("vec![{}]", items.join(", ")));
    }
    if let Some(inner) = unwrapped(ty, "BTreeMap<String, ") {
        let entries = value
            .as_object()
            .ok_or_else(|| format!("{value} is not a map"))?;
        if entries.is_empty() {
            return Ok("BTreeMap::new()".to_owned());
        }
        let entries: Vec<String> = entries
            .iter()
            .map(|(key, item)| {
                Ok(format!(
                    "(String::from({key:?}), {})",
                    literal(item, inner, structs)?
                ))
            })
            .collect::<Result<_, String>>()?;
        return Ok(format!("BTreeMap::from([{}])", entries.join(", ")));
    }
    match ty {
        "String" => value
            .as_str()
            .map(|s| format!("String::from({s:?})"))
            .ok_or_else(|| format!("{value} is not a string")),
        "bool" => value
            .as_bool()
            .map(|b| b.to_string())
            .ok_or_else(|| format!("{value} is not a boolean")),
        "i8" | "i16" | "i32" | "i64" | "u8" | "u16" | "u32" | "u64" => value
            .as_i64()
            .map(|n| n.to_string())
            .or_else(|| value.as_u64().map(|n| n.to_string()))
            .ok_or_else(|| format!("{value} is not an integer")),
        "f32" | "f64" => value
            .as_f64()
            .map(|n| format!("{n:?}"))
            .ok_or_else(|| format!("{value} is not a number")),
        name => {
            let c = structs
                .get(name)
                .ok_or_else(|| format!("no default can be written of type {name}"))?;
            let fields = value
                .as_object()
                .ok_or_else(|| format!("{value} is not an object"))?;
            if let Some(unknown) = fields
                .keys()
                .find(|key| !c.members.iter().any(|m| &m.json == *key))
            {
                return Err(format!("{name} has no field {unknown}"));
            }
            let mut written = Vec::new();
            let mut left_out = false;
            for m in &c.members {
                if let Some(item) = fields.get(&m.json) {
                    written.push(format!("{}: {}", m.name, literal(item, &m.type_, structs)?));
                } else if let Some(function) = m.serde.iter().find_map(|a| {
                    a.strip_prefix("default = \"")
                        .and_then(|f| f.strip_suffix('"'))
                }) {
                    written.push(format!("{}: {function}()", m.name));
                } else {
                    left_out = true;
                }
            }
            if left_out {
                written.push("..Default::default()".to_owned());
            }
            Ok(format!("{name} {{ {} }}", written.join(", ")))
        }
    }
}
