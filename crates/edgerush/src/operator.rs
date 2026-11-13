//! `edgerush operator`: every `ControlPlane` and `DataPlane` in the cluster turned into the
//! objects that run it, and kept so ([05](../../../docs/05-operator.md)).

use crate::controller::{identity, outside, own_namespace_name, told_to_stop};
use edgerush_kubernetes::lease;
use edgerush_kubernetes::{ApiServer, Retry};
use edgerush_operator::{Settings, operate};
use edgerush_planes::objects::Install;
use std::io::{self, Write};
use tokio::runtime::Builder;

const USAGE: &str = "\
Usage: edgerush operator --image <IMAGE> [OPTIONS]

Run the operator, in a cluster. Every ControlPlane and DataPlane in the cluster is
followed, and each accepted has the objects that run it made, by server-side apply under
the field manager edgerush-operator: the fields it states are kept so, a hand edit of one
undone and an object deleted made again, and what others write of them left. An object of
a name it would use that it did not make, as its labels and owner say, is left alone.
Each resource has the operator's finalizer before anything is made for it; deleted, what
no owner reference reaches is deleted by its labels, and the finalizer taken away.
A ControlPlane is accepted where it holds its name, the oldest of a name in the cluster,
and watches its own namespace alone; a DataPlane where a ControlPlane of its namespace
holds the name it names, and it is that ControlPlane's oldest. Each says so in its status:
Accepted, and Programmed once what it asks for is made and its workload has a replica
available.
The replicas elect a leader by the Lease edgerush-operator in the pod's own namespace,
which alone writes; every one follows the cluster, ready to lead.
Runs until Ctrl-C, or SIGTERM on Unix, and then exits 0; exits 2 when the command line
cannot be used, or it is not in a cluster.

Options:
      --image <IMAGE>        The image the planes run: the operator's own, one image for
                             every role
      --cluster-domain <DOMAIN>
                             The cluster's DNS domain, which the control plane is told;
                             cluster.local when left out
  -h, --help                 Print help
";

/// The Lease the operator's replicas elect a leader by, in their own namespace.
const LEASE: &str = "edgerush-operator";

#[derive(Debug, PartialEq, Eq)]
enum Parsed {
    Help,
    Operate(Install),
}

#[derive(Debug, PartialEq, thiserror::Error)]
enum UsageError {
    #[error("unexpected argument '{0}'")]
    Unexpected(String),
    #[error("'{0}' needs a value")]
    NoValue(&'static str),
    #[error("'{0}' is given twice")]
    Twice(&'static str),
    #[error("'{0}' is required")]
    Required(&'static str),
    #[error("'--image' is empty")]
    Image,
    #[error("'--cluster-domain {0}': {1}")]
    ClusterDomain(String, edgerush_config::DnsNameError),
}

fn parse(mut args: impl Iterator<Item = String>) -> Result<Parsed, UsageError> {
    let mut image = None;
    let mut cluster_domain = None;
    while let Some(arg) = args.next() {
        let option = match arg.as_str() {
            "-h" | "--help" => return Ok(Parsed::Help),
            "--image" => "--image",
            "--cluster-domain" => "--cluster-domain",
            _ => return Err(UsageError::Unexpected(arg)),
        };
        let value = args.next().ok_or(UsageError::NoValue(option))?;
        let slot = if option == "--image" {
            if value.is_empty() {
                return Err(UsageError::Image);
            }
            &mut image
        } else {
            edgerush_config::DnsName::host(&value)
                .map_err(|error| UsageError::ClusterDomain(value.clone(), error))?;
            &mut cluster_domain
        };
        if slot.replace(value).is_some() {
            return Err(UsageError::Twice(option));
        }
    }
    Ok(Parsed::Operate(Install {
        image: image.ok_or(UsageError::Required("--image"))?,
        cluster_domain: cluster_domain.unwrap_or_else(|| "cluster.local".to_owned()),
    }))
}

/// Runs `edgerush operator` with the arguments after its name. Returns the exit status.
pub(crate) fn command(
    args: impl Iterator<Item = String>,
    stdout: &mut impl Write,
    stderr: &mut impl Write,
) -> u8 {
    let install = match parse(args) {
        Ok(Parsed::Help) => return write!(stdout, "{USAGE}").map_or(1, |()| 0),
        Ok(Parsed::Operate(install)) => install,
        Err(error) => {
            return write!(stderr, "error: {error}\n\n{USAGE}").map_or(1, |()| crate::EXIT_USAGE);
        }
    };
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return writeln!(stderr, "error: {error}").map_or(1, |()| 1),
    };
    // The election's own client, so that its writes never wait behind the follows'.
    let started = ApiServer::in_cluster()
        .and_then(|api| Ok((api, ApiServer::in_cluster()?)))
        .map_err(|error| outside(&error))
        .and_then(|clients| Ok((clients, own_namespace_name()?)));
    let ((api, elector), namespace) = match started {
        Ok(started) => started,
        Err(failure) => {
            return writeln!(stderr, "error: {failure}").map_or(1, |()| crate::EXIT_USAGE);
        }
    };
    let settings = Settings {
        install,
        namespace,
        lease: LEASE.to_owned(),
        identity: identity(),
        timing: lease::Timing::default(),
        retry: Retry::default(),
    };
    let stopped = tokio::task::LocalSet::new().block_on(&runtime, async move {
        tokio::select! {
            () = operate(api, elector, settings, |line: &str| {
                let _nobody_reads = writeln!(io::stderr(), "{line}");
            }) => Ok(()),
            stopped = told_to_stop() => stopped,
        }
    });
    match stopped {
        Ok(()) => 0,
        Err(error) => writeln!(stderr, "error: {error}").map_or(1, |()| 1),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(args: &[&str]) -> Result<Parsed, UsageError> {
        parse(args.iter().map(ToString::to_string))
    }

    #[test]
    fn the_image_is_required_and_the_cluster_domain_defaults() {
        assert_eq!(
            parsed(&["--image", "edgerush:1"]),
            Ok(Parsed::Operate(Install {
                image: "edgerush:1".to_owned(),
                cluster_domain: "cluster.local".to_owned(),
            }))
        );
        assert_eq!(
            parsed(&["--cluster-domain", "corp.example", "--image", "edgerush:1"]),
            Ok(Parsed::Operate(Install {
                image: "edgerush:1".to_owned(),
                cluster_domain: "corp.example".to_owned(),
            }))
        );
        assert_eq!(parsed(&[]), Err(UsageError::Required("--image")));
        assert_eq!(parsed(&["--help", "--bogus"]), Ok(Parsed::Help));
    }

    #[test]
    fn what_cannot_be_used_is_a_usage_error() {
        assert_eq!(
            parsed(&["--image", "a", "--image", "b"]),
            Err(UsageError::Twice("--image"))
        );
        assert_eq!(parsed(&["--image", ""]), Err(UsageError::Image));
        assert_eq!(parsed(&["--image"]), Err(UsageError::NoValue("--image")));
        assert_eq!(
            parsed(&["--bogus"]),
            Err(UsageError::Unexpected("--bogus".to_owned()))
        );
        assert!(matches!(
            parsed(&["--image", "a", "--cluster-domain", "not a domain"]),
            Err(UsageError::ClusterDomain(..))
        ));
        assert_eq!(
            parsed(&[
                "--image",
                "a",
                "--cluster-domain",
                "a.b",
                "--cluster-domain",
                "a.b"
            ]),
            Err(UsageError::Twice("--cluster-domain"))
        );
    }

    #[test]
    fn help_is_printed_and_a_usage_error_says_why() {
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(["--help".to_owned()].into_iter(), &mut stdout, &mut stderr);
        assert_eq!(status, 0);
        assert_eq!(String::from_utf8(stdout).unwrap(), USAGE);
        let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
        let status = command(std::iter::empty(), &mut stdout, &mut stderr);
        assert_eq!(status, crate::EXIT_USAGE);
        assert!(
            String::from_utf8(stderr)
                .unwrap()
                .starts_with("error: '--image' is required")
        );
    }
}
