//! The conformance runner, `conformance/` at the repository root, pins the Gateway API
//! release whose suite it runs; the CRDs in `crds/` are one release's too (`crds/VERSION`,
//! written by `fetch.sh`). The suite refuses CRDs of a release other than its own, so the
//! two move together (docs/04 §7).

#![allow(
    clippy::expect_used,
    reason = "test set-up: a file that cannot be read fails the test the way it would"
)]

use std::path::Path;

/// The modules `conformance/go.mod` requires that are the suite's release.
const MODULES: [&str; 2] = [
    "sigs.k8s.io/gateway-api",
    "sigs.k8s.io/gateway-api/conformance",
];

/// Each version `go_mod` requires `module` at, from a `require` line or block.
fn required(go_mod: &str, module: &str) -> Vec<String> {
    go_mod
        .lines()
        .filter_map(|line| {
            let line = line.trim();
            let line = line.strip_prefix("require ").unwrap_or(line);
            let mut words = line.split_whitespace();
            (words.next() == Some(module))
                .then(|| words.next().map(str::to_owned))
                .flatten()
        })
        .collect()
}

#[test]
fn the_conformance_runner_pins_the_release_of_the_crds() {
    let here = Path::new(env!("CARGO_MANIFEST_DIR"));
    let crds = std::fs::read_to_string(here.join("crds/VERSION")).expect("crds/VERSION");
    let go_mod =
        std::fs::read_to_string(here.join("../../conformance/go.mod")).expect("conformance/go.mod");
    for module in MODULES {
        assert_eq!(
            required(&go_mod, module),
            [crds.trim()],
            "conformance/go.mod requires {module} at a release other than the CRDs' \
             (crds/VERSION); move both together"
        );
    }
}

#[test]
fn a_require_is_read_from_a_line_or_a_block() {
    let go_mod = "module m\n\nrequire sigs.k8s.io/gateway-api v1.6.0\n\nrequire (\n\
                  \tsigs.k8s.io/gateway-api/conformance v1.7.0 // indirect\n\
                  \tsigs.k8s.io/gateway-api-inference-extension v0.1.0\n)\n";
    assert_eq!(required(go_mod, "sigs.k8s.io/gateway-api"), ["v1.6.0"]);
    assert_eq!(
        required(go_mod, "sigs.k8s.io/gateway-api/conformance"),
        ["v1.7.0"]
    );
}
