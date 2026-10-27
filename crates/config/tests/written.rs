//! A config written as YAML is a file the development harness reads, and reads back as
//! the same config, but for its certificates and limits, which a config file never carries:
//! what `edgerush controller --write-config` writes.

use edgerush_config::{Config, HarnessFile};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

#[test]
fn every_example_config_written_reads_back_the_same() {
    // The config a harness file states, without certificates or limits.
    let read = |yaml: &str| -> Config {
        let file: HarnessFile = serde_saphyr::from_str(yaml).unwrap();
        file.into_config(BTreeMap::new(), None)
    };
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    for path in [
        "examples/all-features.yaml",
        "examples/dev-harness.yaml",
        "examples/grpc.yaml",
        "examples/docker-compose/config/edgerush.yaml",
    ] {
        let config = read(&fs::read_to_string(root.join(path)).unwrap());
        let written = serde_saphyr::to_string(&config).unwrap();
        assert_eq!(read(&written), config, "{path}, written:\n{written}");
    }
}
