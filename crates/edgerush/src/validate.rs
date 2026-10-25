//! `edgerush validate --from-manifests`: the command line around `edgerush_translate`
//! ([04 §3](../../../docs/04-control-plane.md)) — its arguments, reading the manifests and
//! the data plane's settings, and saying what translating them came to, with no cluster.

use edgerush_config::compile;
use edgerush_translate::{Cluster, Plane, translate};
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::networking::v1::Ingress;
use serde::Deserialize;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub(crate) const USAGE: &str = "\
Usage: edgerush validate --from-manifests <PATH> --data-plane <FILE>

Translates Kubernetes manifests as the control plane would for one data plane, compiles the
config as the data plane would, and says what was left out and why, without a cluster.
Ingresses of another class, or of none, are another data plane's: they are counted, not
translated.
Exits 0 when everything was translated and the config compiles, 1 when something was left
out, a path is shadowed by another Ingress's or the config would be refused, and 2 when
the files could not be read.

Options:
      --from-manifests <PATH>  Manifests in YAML: a file, or a directory read for every
                               .yaml and .yml file in it and below. A `List`, as
                               `kubectl get -o yaml` writes one, is read for its items;
                               kinds that are not translated are passed over
      --data-plane <FILE>      The data plane's settings, in YAML: `ingress_class`,
                               `listeners`, `cluster_domain` and
                               `ingress_hosts_fall_through`
  -h, --help                   Print help
";

/// Exit status for manifests that did not all translate, or a config that would be refused.
const EXIT_LEFT_OUT: u8 = 1;

/// Runs `edgerush validate` with the arguments after its name. Returns the exit status.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let written = match parse(args) {
        Ok(Parsed::Help) => write!(stdout, "{USAGE}").map(|()| 0),
        Ok(Parsed::Validate(options)) => match run(&options) {
            Ok((text, clean)) => {
                write!(stdout, "{text}").map(|()| if clean { 0 } else { EXIT_LEFT_OUT })
            }
            Err(failure) => writeln!(stderr, "error: {failure}").map(|()| crate::EXIT_USAGE),
        },
        Err(error) => write!(stderr, "error: {error}\n\n{USAGE}").map(|()| crate::EXIT_USAGE),
    };
    written.unwrap_or(1)
}

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Validate(Options),
}

#[derive(Debug, PartialEq, Eq)]
struct Options {
    manifests: PathBuf,
    data_plane: PathBuf,
}

#[derive(Debug, PartialEq, Eq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'{0}' is required")]
    Required(&'static str),
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut manifests = None;
    let mut data_plane = None;
    while let Some(arg) = args.next() {
        let (option, slot) = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--from-manifests" => ("--from-manifests", &mut manifests),
            "--data-plane" => ("--data-plane", &mut data_plane),
            _ => return Err(UsageError::Unexpected(arg)),
        };
        let value = args.next().ok_or(UsageError::NoValue(option))?;
        if slot.replace(PathBuf::from(value)).is_some() {
            return Err(UsageError::Twice(option));
        }
    }
    Ok(Parsed::Validate(Options {
        manifests: manifests.ok_or(UsageError::Required("--from-manifests <PATH>"))?,
        data_plane: data_plane.ok_or(UsageError::Required("--data-plane <FILE>"))?,
    }))
}

/// Why the manifests could not be validated.
#[derive(Debug, thiserror::Error)]
enum Failed {
    #[error("{} cannot be read: {error}", path.display())]
    Unreadable { path: PathBuf, error: io::Error },
    #[error("{} is not what was expected: {error}", path.display())]
    Parse {
        path: PathBuf,
        error: Box<serde_saphyr::Error>,
    },
    #[error("{} holds no .yaml or .yml file", .0.display())]
    NoManifests(PathBuf),
}

/// A manifest of a kind translation reads, a `List` of them, or anything else, passed over.
#[derive(Deserialize)]
#[serde(tag = "kind")]
enum Manifest {
    Ingress(Box<Ingress>),
    Service(Box<Service>),
    List {
        items: Vec<Manifest>,
    },
    #[serde(other)]
    Other,
}

impl Manifest {
    fn into_cluster(self, cluster: &mut Cluster) {
        match self {
            Self::Ingress(ingress) => cluster.ingresses.push(*ingress),
            Self::Service(service) => cluster.services.push(*service),
            Self::List { items } => {
                for item in items {
                    item.into_cluster(cluster);
                }
            }
            Self::Other => {}
        }
    }
}

/// Translates and compiles. The text, and whether nothing was left out or shadowed and the
/// config compiles.
fn run(options: &Options) -> Result<(String, bool), Failed> {
    let plane: Plane = read(&options.data_plane)?;
    let mut cluster = Cluster::default();
    for file in files(&options.manifests)? {
        for manifest in read_all::<Manifest>(&file)? {
            manifest.into_cluster(&mut cluster);
        }
    }
    let translation = translate(&plane, &cluster);
    let mut text = String::new();
    for finding in &translation.report {
        // Writing to a `String` cannot fail.
        let _ = writeln!(text, "{finding}");
    }
    for shadowed in &translation.shadowed {
        let _ = writeln!(text, "{shadowed}");
    }
    let config = &translation.config;
    let _ = writeln!(
        text,
        "{} routes and {} upstreams translated; {} left out; {} paths shadowed; {} Ingresses \
         of other classes or none passed over",
        config.routes.len(),
        config.upstreams.len(),
        translation.report.len(),
        translation.shadowed.len(),
        translation.passed_over
    );
    let refused = match compile(config) {
        Ok(_) => false,
        Err(problems) => {
            let _ = writeln!(text, "the data plane would refuse the config:");
            for problem in problems {
                let _ = writeln!(text, "  {problem}");
            }
            true
        }
    };
    let clean = translation.report.is_empty() && translation.shadowed.is_empty();
    Ok((text, clean && !refused))
}

/// The files of manifests at `path`: itself, or every `.yaml` and `.yml` file in it and
/// below, in order of their paths.
fn files(path: &Path) -> Result<Vec<PathBuf>, Failed> {
    let unreadable = |error| Failed::Unreadable {
        path: path.to_owned(),
        error,
    };
    if !fs::metadata(path).map_err(unreadable)?.is_dir() {
        return Ok(vec![path.to_owned()]);
    }
    let mut found = Vec::new();
    let mut directories = vec![path.to_owned()];
    while let Some(directory) = directories.pop() {
        let entries = fs::read_dir(&directory).map_err(|error| Failed::Unreadable {
            path: directory.clone(),
            error,
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| Failed::Unreadable {
                path: directory.clone(),
                error,
            })?;
            let path = entry.path();
            if path.is_dir() {
                directories.push(path);
            } else if path
                .extension()
                .is_some_and(|extension| extension == "yaml" || extension == "yml")
            {
                found.push(path);
            }
        }
    }
    if found.is_empty() {
        return Err(Failed::NoManifests(path.to_owned()));
    }
    found.sort();
    Ok(found)
}

fn read<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Failed> {
    let bytes = fs::read(path).map_err(|error| Failed::Unreadable {
        path: path.to_owned(),
        error,
    })?;
    // As a config file is read: no lines quoted around a mistake.
    let options = serde_saphyr::options! { with_snippet: false };
    serde_saphyr::from_slice_with_options(&bytes, options).map_err(|error| Failed::Parse {
        path: path.to_owned(),
        error: Box::new(error),
    })
}

fn read_all<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<Vec<T>, Failed> {
    let bytes = fs::read(path).map_err(|error| Failed::Unreadable {
        path: path.to_owned(),
        error,
    })?;
    let options = serde_saphyr::options! { with_snippet: false };
    serde_saphyr::from_slice_multiple_with_options(&bytes, options).map_err(|error| Failed::Parse {
        path: path.to_owned(),
        error: Box::new(error),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(args: &[&str]) -> Result<Parsed, UsageError> {
        parse(args.iter().map(ToString::to_string))
    }

    #[test]
    fn it_needs_its_manifests_and_its_data_plane_once_each() {
        assert_eq!(
            parsed(&["--from-manifests", "m", "--data-plane", "p"]),
            Ok(Parsed::Validate(Options {
                manifests: PathBuf::from("m"),
                data_plane: PathBuf::from("p"),
            }))
        );
        assert_eq!(parsed(&["--help"]), Ok(Parsed::Help));
        assert_eq!(
            parsed(&["--data-plane", "p"]),
            Err(UsageError::Required("--from-manifests <PATH>"))
        );
        assert_eq!(
            parsed(&["--from-manifests", "m"]),
            Err(UsageError::Required("--data-plane <FILE>"))
        );
        assert_eq!(
            parsed(&["--from-manifests"]),
            Err(UsageError::NoValue("--from-manifests"))
        );
        assert_eq!(
            parsed(&["--from-manifests", "m", "--from-manifests", "n"]),
            Err(UsageError::Twice("--from-manifests"))
        );
        assert_eq!(parsed(&["m"]), Err(UsageError::Unexpected("m".to_owned())));
    }

    /// Kinds that are not translated are passed over, and a `List`'s items are read as
    /// manifests of their own, as `kubectl get -o yaml` writes a cluster's objects.
    #[test]
    fn a_list_is_read_for_its_items_and_other_kinds_are_passed_over() {
        let yaml = "\
apiVersion: apps/v1
kind: Deployment
metadata: { name: web, namespace: shop }
spec: { replicas: 3 }
---
apiVersion: v1
kind: List
items:
  - { apiVersion: v1, kind: Service, metadata: { name: web, namespace: shop } }
  - { apiVersion: v1, kind: ConfigMap, metadata: { name: settings, namespace: shop } }
  - apiVersion: v1
    kind: List
    items:
      - { apiVersion: networking.k8s.io/v1, kind: Ingress, metadata: { name: web, namespace: shop } }
";
        let mut cluster = Cluster::default();
        for manifest in serde_saphyr::from_multiple::<Manifest>(yaml).unwrap() {
            manifest.into_cluster(&mut cluster);
        }
        let names = |metadata: &k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta| {
            metadata.name.clone().unwrap()
        };
        assert_eq!(
            cluster
                .services
                .iter()
                .map(|s| names(&s.metadata))
                .collect::<Vec<_>>(),
            ["web"]
        );
        assert_eq!(
            cluster
                .ingresses
                .iter()
                .map(|i| names(&i.metadata))
                .collect::<Vec<_>>(),
            ["web"]
        );
    }
}
