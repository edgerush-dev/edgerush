//! Names as Rust spells them: a schema's field names are JSON's, `camelCase` mostly.

/// A type's name from a field's: each word's first letter in upper case, the rest as
/// written, anything but ASCII letters and digits dropped as a break between words.
/// `backendRefs` is `BackendRefs`, and a kind's own name, `HTTPRoute`, stays as it is.
pub(crate) fn upper_camel(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut word_starts = true;
    for c in name.chars() {
        if c.is_ascii_alphanumeric() {
            if word_starts {
                out.push(c.to_ascii_uppercase());
                word_starts = false;
            } else {
                out.push(c);
            }
        } else {
            word_starts = true;
        }
    }
    out
}

/// A field's name in Rust: words in lower case joined by `_`. A word begins at an upper
/// case letter after a lower case one or a digit, and at the last of a run of upper case
/// letters when a lower case one follows, so that `URLRewrite` is `url_rewrite`; anything
/// but ASCII letters and digits is a break.
pub(crate) fn snake(name: &str) -> String {
    let chars: Vec<char> = name.chars().collect();
    let mut out = String::with_capacity(name.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if !c.is_ascii_alphanumeric() {
            if !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
            continue;
        }
        if c.is_ascii_uppercase() && i > 0 {
            let before = chars[i - 1];
            let lower_follows = chars.get(i + 1).is_some_and(char::is_ascii_lowercase);
            let breaks = before.is_ascii_lowercase()
                || before.is_ascii_digit()
                || (before.is_ascii_uppercase() && lower_follows);
            if breaks && !out.is_empty() && !out.ends_with('_') {
                out.push('_');
            }
        }
        out.push(c.to_ascii_lowercase());
    }
    while out.ends_with('_') {
        out.pop();
    }
    out
}

/// Rust's keywords, strict and reserved, in edition 2024.
const KEYWORDS: [&str; 52] = [
    "abstract", "as", "async", "await", "become", "box", "break", "const", "continue", "crate",
    "do", "dyn", "else", "enum", "extern", "false", "final", "fn", "for", "gen", "if", "impl",
    "in", "let", "loop", "macro", "match", "mod", "move", "mut", "override", "priv", "pub", "ref",
    "return", "self", "Self", "static", "struct", "super", "trait", "true", "try", "type",
    "typeof", "unsafe", "unsized", "use", "virtual", "where", "while", "yield",
];

/// The keywords no raw identifier may be.
const NOT_RAW: [&str; 4] = ["crate", "self", "Self", "super"];

/// `name` as an identifier: as it is, raw (`r#type`) when it is a keyword, with a `_`
/// after it when it is one that cannot be raw, and with a `_` before it when it does not
/// begin as an identifier may. None when it is empty.
pub(crate) fn identifier(name: &str) -> Option<String> {
    let first = name.chars().next()?;
    if NOT_RAW.contains(&name) {
        return Some(format!("{name}_"));
    }
    if KEYWORDS.contains(&name) {
        return Some(format!("r#{name}"));
    }
    if first.is_ascii_digit() {
        return Some(format!("_{name}"));
    }
    Some(name.to_owned())
}

#[cfg(test)]
mod tests {
    use super::{identifier, snake, upper_camel};

    #[test]
    fn upper_camel_capitalises_each_word_and_keeps_the_rest() {
        assert_eq!(upper_camel("backendRefs"), "BackendRefs");
        assert_eq!(upper_camel("HTTPRoute"), "HTTPRoute");
        assert_eq!(upper_camel("x-kubernetes-thing"), "XKubernetesThing");
        assert_eq!(upper_camel("snake_case_name"), "SnakeCaseName");
        assert_eq!(upper_camel("v1"), "V1");
    }

    #[test]
    fn snake_splits_at_case_changes_and_acronyms() {
        assert_eq!(snake("backendRefs"), "backend_refs");
        assert_eq!(snake("gatewayClassName"), "gateway_class_name");
        assert_eq!(snake("URLRewrite"), "url_rewrite");
        assert_eq!(snake("caCertificateRefs"), "ca_certificate_refs");
        assert_eq!(snake("requestHeaderModifier"), "request_header_modifier");
        assert_eq!(snake("http2"), "http2");
        assert_eq!(snake("maxAge"), "max_age");
        assert_eq!(snake("IPAddress"), "ip_address");
        assert_eq!(snake("x-kubernetes"), "x_kubernetes");
        assert_eq!(snake("already_snake"), "already_snake");
        assert_eq!(snake("value2Name"), "value2_name");
        assert_eq!(snake("-trailing-"), "trailing");
    }

    #[test]
    fn identifier_escapes_what_rust_would_not_take() {
        assert_eq!(identifier("name").as_deref(), Some("name"));
        assert_eq!(identifier("type").as_deref(), Some("r#type"));
        assert_eq!(identifier("gen").as_deref(), Some("r#gen"));
        assert_eq!(identifier("self").as_deref(), Some("self_"));
        assert_eq!(identifier("2xx").as_deref(), Some("_2xx"));
        assert_eq!(identifier(""), None);
    }
}
