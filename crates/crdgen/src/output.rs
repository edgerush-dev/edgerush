//! What the analyzer finds: the structs a schema needs and their fields, named as Rust
//! names them. Adapted from kopium's `output.rs` (NOTICE).

use crate::case;

/// Every struct found, in the order found: the schema's root first.
#[derive(Default, Debug)]
pub(crate) struct Output(Vec<Container>);

impl Output {
    /// Adds `value`, unless a struct of the same name and fields is there already. A struct
    /// of the same name but other fields is told apart by an `X` for each it clashes with
    /// (kopium issue #66); identical structs under one name are one (kopium issue #298).
    pub(crate) fn insert(&mut self, mut value: Container) {
        let mut clashes = 0;
        for c in &self.0 {
            if c == &value {
                return;
            }
            if c.name == value.name {
                clashes += 1;
            }
        }
        for _ in 0..clashes {
            value.name.push('X');
        }
        self.0.push(value);
    }

    /// Adds every struct `extras` found, as they were found.
    pub(crate) fn extend(&mut self, extras: Output) {
        self.0.extend(extras.0);
    }

    /// The structs, every field named as Rust names it.
    pub(crate) fn named(mut self) -> Result<Vec<Container>, String> {
        for c in &mut self.0 {
            c.rename()?;
        }
        Ok(self.0)
    }
}

/// A struct found by the analyzer.
#[derive(Default, Debug, PartialEq)]
pub(crate) struct Container {
    /// Its name: the kind's, then each field's on the way down, `HTTPRouteSpecRules`.
    pub(crate) name: String,
    /// How deep in the schema it was found: 0 is the root.
    pub(crate) level: u8,
    /// Its fields.
    pub(crate) members: Vec<Member>,
    /// The schema's description of it.
    pub(crate) docs: Option<String>,
}

/// A field of a struct.
#[derive(Default, Debug, PartialEq)]
pub(crate) struct Member {
    /// Its name: JSON's until [`Container::rename`], Rust's after.
    pub(crate) name: String,
    /// Its name in JSON, as the schema has it.
    pub(crate) json: String,
    /// Its type, as Rust spells it: `Option<Vec<HTTPRouteSpecRules>>`.
    pub(crate) type_: String,
    /// What goes inside its `#[serde(..)]`: `default` and `skip_serializing_if` when
    /// optional, `rename` when Rust's name is not JSON's.
    pub(crate) serde: Vec<String>,
    /// The schema's description of it.
    pub(crate) docs: Option<String>,
    /// The values an enumerated string may take, said in its docs: the field is a `String`
    /// all the same, so that a value from a newer release or another channel is read.
    pub(crate) values: Vec<String>,
}

impl Container {
    /// Names every field as Rust does, `snake_case`, with a `rename` back to JSON's where
    /// they differ; a name that two fields come to share gets a `_x` for each before it
    /// (kopium issue #165).
    fn rename(&mut self) -> Result<(), String> {
        let mut seen: Vec<String> = Vec::with_capacity(self.members.len());
        for m in &mut self.members {
            let snake = case::snake(&m.name);
            let mut name = case::identifier(&snake).ok_or_else(|| {
                format!("field `{}` of {} has no name in Rust", m.name, self.name)
            })?;
            while seen.contains(&name) {
                name.push_str("_x");
            }
            seen.push(name.clone());
            if name.trim_start_matches("r#") != m.name {
                m.serde.push(format!("rename = \"{}\"", m.name));
            }
            m.name = name;
        }
        Ok(())
    }
}

/// `input` as doc comment lines at `indent`, tabs as spaces. A fenced block is marked `text`, so that
/// rustdoc does not take a schema's example for Rust to test, and a bare URL is made a
/// link, as rustdoc asks.
pub(crate) fn doc_lines(indent: &str, input: &str) -> String {
    let mut out = String::new();
    let mut fenced = false;
    // Tabs as four spaces, as rustdoc renders them and clippy asks.
    let input = input.trim_end().replace('\t', "    ");
    for line in input.lines() {
        let trimmed = line.trim_start();
        let line = if let Some(info) = trimmed.strip_prefix("```") {
            let opens = !fenced;
            fenced = !fenced;
            if opens && info.trim() != "text" {
                format!("{}```text", &line[..line.len() - trimmed.len()])
            } else {
                line.to_owned()
            }
        } else if fenced {
            line.to_owned()
        } else {
            linked(line)
        };
        out.push_str(indent);
        if line.is_empty() {
            out.push_str("///\n");
        } else {
            out.push_str("/// ");
            out.push_str(line.trim_end());
            out.push('\n');
        }
    }
    out
}

/// `line` with each bare `http://` or `https://` URL put in angle brackets: an automatic
/// link, as rustdoc's `bare_urls` asks. A URL already in brackets, or in a code span, is
/// left as it is.
fn linked(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut rest = line;
    let mut in_code = false;
    while let Some(at) = rest.find(['`', 'h']) {
        let (before, from) = rest.split_at(at);
        out.push_str(before);
        if let Some(after) = from.strip_prefix('`') {
            in_code = !in_code;
            out.push('`');
            rest = after;
            continue;
        }
        let is_url = from.starts_with("http://") || from.starts_with("https://");
        // Already a link: automatic, or a Markdown link's target.
        let bracketed = out.ends_with('<') || out.ends_with("](");
        if !is_url || in_code || bracketed {
            out.push('h');
            rest = &from[1..];
            continue;
        }
        let end = from
            .find(|c: char| c.is_whitespace() || c == '>' || c == ')')
            .unwrap_or(from.len());
        // A sentence's full stop or comma after a URL is the sentence's.
        let url = from[..end].trim_end_matches(['.', ',', ';', ':']);
        out.push('<');
        out.push_str(url);
        out.push('>');
        rest = &from[url.len()..];
    }
    out.push_str(rest);
    out
}

#[cfg(test)]
mod tests {
    use super::{Container, Member, Output, doc_lines};

    fn member(name: &str, type_: &str) -> Member {
        Member {
            name: name.to_owned(),
            json: name.to_owned(),
            type_: type_.to_owned(),
            ..Member::default()
        }
    }

    #[test]
    fn identical_structs_are_one_and_clashing_names_are_told_apart() {
        let mut out = Output::default();
        let a = || Container {
            name: "Thing".to_owned(),
            members: vec![member("a", "String")],
            ..Container::default()
        };
        out.insert(a());
        out.insert(a());
        out.insert(Container {
            name: "Thing".to_owned(),
            members: vec![member("b", "String")],
            ..Container::default()
        });
        let names: Vec<_> = out.0.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["Thing", "ThingX"]);
    }

    #[test]
    fn rename_avoids_producing_name_clashes() {
        let mut c = Container {
            name: "Clashing".to_owned(),
            members: vec![
                member("fooBar", "String"),
                member("foo_bar", "String"),
                member("foo-bar", "String"),
                member("type", "String"),
            ],
            ..Container::default()
        };
        assert_eq!(c.rename(), Ok(()));
        let names: Vec<_> = c.members.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["foo_bar", "foo_bar_x", "foo_bar_x_x", "r#type"]);
        assert_eq!(c.members[0].serde, ["rename = \"fooBar\""]);
        assert_eq!(c.members[1].serde, ["rename = \"foo_bar\""]);
        assert_eq!(c.members[2].serde, ["rename = \"foo-bar\""]);
        assert!(c.members[3].serde.is_empty(), "r#type is read as `type`");
    }

    #[test]
    fn a_field_without_a_name_in_rust_is_an_error() {
        let mut c = Container {
            name: "Odd".to_owned(),
            members: vec![member("--", "String")],
            ..Container::default()
        };
        assert!(c.rename().is_err());
    }

    #[test]
    fn fenced_blocks_are_text_and_bare_urls_links() {
        let docs = "See https://example.com/a. Or <https://example.com/b>.\n\n\
                    ```yaml\nkey: http://not.a.link\n```\nThen `http://in.code`.";
        assert_eq!(
            doc_lines("    ", docs),
            "    /// See <https://example.com/a>. Or <https://example.com/b>.\n    ///\n    \
             /// ```text\n    /// key: http://not.a.link\n    /// ```\n    \
             /// Then `http://in.code`.\n"
        );
    }

    #[test]
    fn a_url_in_parentheses_is_a_link_and_a_links_target_is_kept() {
        assert_eq!(
            doc_lines(
                "",
                "Names (https://example.com/names#names). [Docs](https://example.com/d)."
            ),
            "/// Names (<https://example.com/names#names>). [Docs](https://example.com/d).\n"
        );
    }

    #[test]
    fn tabs_are_four_spaces() {
        assert_eq!(doc_lines("", "a:\n\tb: c"), "/// a:\n///     b: c\n");
    }

    #[test]
    fn a_fence_already_text_is_kept() {
        assert_eq!(
            doc_lines("", "```text\nx\n```"),
            "/// ```text\n/// x\n/// ```\n"
        );
    }
}
