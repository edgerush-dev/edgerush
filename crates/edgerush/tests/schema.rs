//! The config file's JSON Schema, `schema/config.schema.json`, is the one the config model's
//! types make: every section, field and choice, each described by its rustdoc. A change to
//! the model that the published schema does not have fails here.
//!
//! To write it again after such a change, from the repository root:
//! `EDGERUSH_WRITE_SCHEMA=1 cargo test -p edgerush --test schema`.

#![allow(
    clippy::expect_used,
    reason = "test set-up: the helper that makes the schema fails the tests the way they would"
)]

use edgerush_config::HarnessFile;
use schemars::generate::SchemaSettings;
use schemars::transform::RecursiveTransform;
use std::path::Path;

const PUBLISHED: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../schema/config.schema.json"
);

/// The schema as the types make it, with the rustdoc's references to the design docs, which
/// are not published with it, taken out of every description.
fn schema() -> String {
    let settings = SchemaSettings::draft2020_12().with_transform(RecursiveTransform(
        |schema: &mut schemars::Schema| {
            let described = schema
                .get("description")
                .and_then(|description| description.as_str())
                .map(without_doc_references);
            if let Some(description) = described {
                schema.insert("description".to_owned(), description.into());
            }
        },
    ));
    let schema = settings
        .into_generator()
        .into_root_schema_for::<HarnessFile>();
    let mut text = serde_json::to_string_pretty(&schema).expect("a schema is JSON");
    text.push('\n');
    text
}

/// `text` without its references to the design docs: a parenthesis that links into them,
/// `([03 §6](../../../docs/03-data-plane.md))`, or says where in them, `(17 in the docs)`,
/// with the white space before it. Every other parenthesis stays, an RFC's section included.
fn without_doc_references(text: &str) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(open) = rest.find('(') {
        let (before, from) = rest.split_at(open);
        let Some(close) = closing(from) else { break };
        let inner = &from[1..close];
        let reference = inner.ends_with(" in the docs")
            || (inner.starts_with('[') && inner.contains("docs/") && inner.ends_with(".md)"));
        if reference {
            kept.push_str(before.trim_end());
        } else {
            kept.push_str(&rest[..open + close + 1]);
        }
        rest = &from[close + 1..];
    }
    kept.push_str(rest);
    kept
}

/// Where the parenthesis that `text` opens with is closed, if it is.
fn closing(text: &str) -> Option<usize> {
    let mut depth = 0_usize;
    for (at, byte) in text.bytes().enumerate() {
        match byte {
            b'(' => depth += 1,
            b')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(at);
                }
            }
            _ => {}
        }
    }
    None
}

#[test]
fn the_published_schema_is_the_one_the_types_make() {
    let made = schema();
    if std::env::var_os("EDGERUSH_WRITE_SCHEMA").is_some() {
        std::fs::write(PUBLISHED, &made).unwrap();
        return;
    }
    let published = std::fs::read_to_string(Path::new(PUBLISHED))
        .unwrap_or_default()
        .replace("\r\n", "\n");
    assert!(
        published == made,
        "schema/config.schema.json is not the schema the config types make; write it again \
         with EDGERUSH_WRITE_SCHEMA=1 cargo test -p edgerush --test schema"
    );
}

#[test]
fn no_description_points_into_the_design_docs() {
    let made = schema();
    assert!(
        !made.contains("docs/"),
        "a link into the design docs is left"
    );
    assert!(
        !made.contains(" in the docs"),
        "a reference to the design docs is left"
    );
}

#[test]
fn doc_references_are_taken_out_and_nothing_else() {
    let cases = [
        (
            "a resource of their own ([07 §1](../../../docs/07-config-and-dsl.md)), which",
            "a resource of their own, which",
        ),
        ("closed (17 in the docs). An hour", "closed. An hour"),
        (
            "is written (08 §2, 21 in the docs). Left",
            "is written. Left",
        ),
        (
            "whose to believe\n(20 in the docs). Every",
            "whose to believe. Every",
        ),
        (
            "goes ([08 §2](../../../docs/08-observability.md)): one JSON line",
            "goes: one JSON line",
        ),
        (
            "remember it (the `ma` of `Alt-Svc`, RFC 7838 §3.1). A day",
            "remember it (the `ma` of `Alt-Svc`, RFC 7838 §3.1). A day",
        ),
        (
            "(RFC 9113 §3.3): from the first byte",
            "(RFC 9113 §3.3): from the first byte",
        ),
        (
            "one (`10.0.0.0/16`), two (17 in the docs)",
            "one (`10.0.0.0/16`), two",
        ),
        ("left open (as here", "left open (as here"),
        ("nothing to take", "nothing to take"),
    ];
    for (text, meant) in cases {
        assert_eq!(without_doc_references(text), meant, "{text:?}");
    }
}
