//! `edgerush validate --from-manifests`: the command line around `edgerush_translate`
//! ([04 §3](../../../docs/04-control-plane.md)) — its arguments, reading the manifests and
//! the data plane's settings, and saying what translating them came to, with no cluster.

use edgerush_config::compile;
use edgerush_gateway_api::gateway::Gateway;
use edgerush_gateway_api::gateway_class::GatewayClass;
use edgerush_gateway_api::http_route::HTTPRoute;
use edgerush_gateway_api::reference_grant::ReferenceGrant;
use edgerush_translate::{Cluster, Plane, Translation, translate};
use k8s_openapi::api::core::v1::{Namespace, Secret, Service};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use serde::Deserialize;
use std::fmt::Write as _;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::{fs, io};

pub(crate) const USAGE: &str = "\
Usage: edgerush validate --from-manifests <PATH> --data-plane <FILE> [--check-secrets]

Translates Kubernetes manifests as the control plane would for one data plane, compiles the
config as the data plane would, and says what was left out and why, without a cluster.
Ingresses of another class, or of none, are another data plane's: they are counted, not
translated.
Exits 0 when everything was translated and the config compiles, 1 when something was left
out, a path's backend is not there, a host or entry `tls` lists is not served as listed, a
path is shadowed by another Ingress's or the config would be refused, and 2 when the files
could not be read.

Options:
      --from-manifests <PATH>  Manifests in YAML: a file, or a directory read for every
                               .yaml and .yml file in it and below. A `List`, as
                               `kubectl get -o yaml` writes one, is read for its items;
                               kinds that are not translated are passed over. The data
                               plane's IngressClass must be among them, naming
                               `edgerush.dev/ingress-controller`
      --data-plane <FILE>      The data plane's settings, in YAML: `ingress_class`,
                               `listeners`, `cluster_domain`,
                               `ingress_hosts_fall_through`,
                               `ingress_https_redirect`,
                               `ingress_https_redirect_max_age` and,
                               optionally, `backend_authorities` and
                               `data_plane`, the data plane's own settings,
                               written into the config as they are
      --check-secrets          Check the Secrets the Ingresses' `tls` names: each must
                               be among the manifests, its certificate and key usable
                               and covering the host, as the control plane checks them.
                               Without it they are taken as they will be in the
                               cluster, secure the hosts as named, and are not looked at
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
    check_secrets: bool,
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
    let mut check_secrets = false;
    while let Some(arg) = args.next() {
        let (option, slot) = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--check-secrets" if check_secrets => return Err(UsageError::Twice("--check-secrets")),
            "--check-secrets" => {
                check_secrets = true;
                continue;
            }
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
        check_secrets,
    }))
}

/// Why the manifests could not be validated, or a file read.
#[derive(Debug, thiserror::Error)]
pub(crate) enum Failed {
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
    IngressClass(Box<IngressClass>),
    Ingress(Box<Ingress>),
    Service(Box<Service>),
    Secret(Box<Secret>),
    GatewayClass(Box<GatewayClass>),
    Gateway(Box<Gateway>),
    HTTPRoute(Box<HTTPRoute>),
    ReferenceGrant(Box<ReferenceGrant>),
    Namespace(Box<Namespace>),
    List {
        items: Vec<Manifest>,
    },
    #[serde(other)]
    Other,
}

impl Manifest {
    fn into_cluster(self, cluster: &mut Cluster) {
        match self {
            Self::IngressClass(class) => cluster.ingress_classes.push(*class),
            Self::Ingress(ingress) => cluster.ingresses.push(*ingress),
            Self::Service(service) => cluster.services.push(*service),
            Self::Secret(secret) => cluster.secrets.push(*secret),
            Self::GatewayClass(class) => cluster.gateway_classes.push(*class),
            Self::Gateway(gateway) => cluster.gateways.push(*gateway),
            Self::HTTPRoute(route) => cluster.http_routes.push(*route),
            Self::ReferenceGrant(grant) => cluster.reference_grants.push(*grant),
            Self::Namespace(namespace) => cluster.namespaces.push(*namespace),
            Self::List { items } => {
                for item in items {
                    item.into_cluster(cluster);
                }
            }
            Self::Other => {}
        }
    }
}

/// Translates and compiles. The text, and whether nothing was left out, without its
/// backend, nothing of a `tls` left unserved and no path shadowed, and the config compiles.
fn run(options: &Options) -> Result<(String, bool), Failed> {
    let plane: Plane = read(&options.data_plane)?;
    let mut cluster = Cluster {
        secrets_assumed: !options.check_secrets,
        ..Cluster::default()
    };
    for file in files(&options.manifests)? {
        for manifest in read_all::<Manifest>(&file)? {
            manifest.into_cluster(&mut cluster);
        }
    }
    let translation = translate(&plane, &cluster);
    let mut text = described(&translation);
    let config = &translation.config;
    if cluster.secrets_assumed && !config.certificates.is_empty() {
        let _ = writeln!(
            text,
            "TLS Secrets were taken as they will be in the cluster, not checked: \
             --check-secrets checks them"
        );
    }
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
    let clean = translation.report.is_empty()
        && translation.unresolved.is_empty()
        && translation.tls.is_empty()
        && translation.shadowed.is_empty()
        && translation.taken.is_empty();
    Ok((text, clean && !refused))
}

/// What a translation came to, a line each: what was left out, the paths without their
/// backend, what of a `tls` is not served as listed and the paths shadowed, then how many
/// of each. As `validate` prints it and the control plane logs it.
pub(crate) fn described(translation: &Translation) -> String {
    let mut text = String::new();
    for finding in &translation.report {
        // Writing to a `String` cannot fail.
        let _ = writeln!(text, "{finding}");
    }
    for unresolved in &translation.unresolved {
        let _ = writeln!(text, "{unresolved}");
    }
    for tls in &translation.tls {
        let _ = writeln!(text, "{tls}");
    }
    for shadowed in &translation.shadowed {
        let _ = writeln!(text, "{shadowed}");
    }
    for taken in &translation.taken {
        let _ = writeln!(text, "{taken}");
    }
    let config = &translation.config;
    let _ = writeln!(
        text,
        "{} routes and {} upstreams translated; {} left out; {} paths without their backend; \
         {} TLS hosts or entries not served as listed; {} paths shadowed; {} Ingresses of other classes \
         or none passed over",
        config.routes.len(),
        config.upstreams.len(),
        translation.report.len(),
        translation.unresolved.len(),
        translation.tls.len(),
        translation.shadowed.len(),
        translation.passed_over
    );
    text
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

/// The file at `path`, in YAML, as a config file is read.
pub(crate) fn read<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T, Failed> {
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
                check_secrets: false,
            }))
        );
        assert_eq!(
            parsed(&[
                "--check-secrets",
                "--from-manifests",
                "m",
                "--data-plane",
                "p"
            ]),
            Ok(Parsed::Validate(Options {
                manifests: PathBuf::from("m"),
                data_plane: PathBuf::from("p"),
                check_secrets: true,
            }))
        );
        assert_eq!(
            parsed(&["--check-secrets", "--check-secrets"]),
            Err(UsageError::Twice("--check-secrets"))
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

    /// The Gateway API's kinds are read, an object of the experimental channel among them, and
    /// Namespaces, for their labels.
    #[test]
    fn the_gateway_apis_kinds_are_read() {
        let yaml = "\
apiVersion: gateway.networking.k8s.io/v1
kind: GatewayClass
metadata: { name: edgerush }
spec: { controllerName: edgerush.dev/gateway-controller }
---
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata: { name: web, namespace: edge }
spec: { gatewayClassName: edgerush, listeners: [{ name: http, port: 80, protocol: HTTP }] }
---
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata: { name: web, namespace: shop }
spec: { rules: [{ filters: [{ type: ExternalAuth, externalAuth: { protocol: HTTP } }] }] }
---
apiVersion: gateway.networking.k8s.io/v1
kind: ReferenceGrant
metadata: { name: from-edge, namespace: shop }
spec: { from: [{ group: gateway.networking.k8s.io, kind: HTTPRoute, namespace: edge }], to: [{ group: '', kind: Service }] }
---
apiVersion: v1
kind: Namespace
metadata: { name: shop, labels: { team: shop } }
";
        let mut cluster = Cluster::default();
        for manifest in serde_saphyr::from_multiple::<Manifest>(yaml).unwrap() {
            manifest.into_cluster(&mut cluster);
        }
        let counts = (
            cluster.gateway_classes.len(),
            cluster.gateways.len(),
            cluster.http_routes.len(),
            cluster.reference_grants.len(),
            cluster.namespaces.len(),
        );
        assert_eq!(counts, (1, 1, 1, 1, 1));
    }
}
