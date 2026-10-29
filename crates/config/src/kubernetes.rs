//! A Kubernetes Service named as a cluster names it in DNS, for an upstream whose endpoints
//! are followed through the Service's EndpointSlices ([03 §6](../../../docs/03-data-plane.md)).
//!
//! The port is the Service's, and is read from what EndpointSlices carry. A slice lists
//! each of its Service's ports by the Service port's name, beside the port each pod listens
//! on, but not by the Service port's number. So a port is named, or, when it is a number,
//! stands for the Service's only port, which Kubernetes lets go unnamed. A port name always
//! has a letter in it and a Service with several ports names all of them, so neither
//! reading can be mistaken for the other.

use std::fmt;
use std::num::NonZeroU16;

/// A Kubernetes Service and one of its ports: `<service>.<namespace>.svc.<cluster domain>:<port>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KubernetesService {
    /// The Service's name.
    pub service: String,
    /// The namespace it is in.
    pub namespace: String,
    /// The cluster's DNS domain, as written: `cluster.local` unless the cluster says
    /// otherwise. Kept for what is shown, not for finding the Service.
    pub cluster_domain: String,
    /// Which of its ports.
    pub port: ServicePort,
}

/// A Kubernetes Service, without a port: `<service>.<namespace>.svc.<cluster domain>`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ServiceName {
    /// The Service's name.
    pub service: String,
    /// The namespace it is in.
    pub namespace: String,
    /// The cluster's DNS domain, as written. Kept for what is shown.
    pub cluster_domain: String,
}

/// One of a Service's ports, as its EndpointSlices know it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ServicePort {
    /// The port of this name.
    Named(String),
    /// The Service's only port, left unnamed, which listens on this number. EndpointSlices
    /// do not carry it: it is kept for what is shown.
    Only(NonZeroU16),
}

/// Why a name is not a Kubernetes Service's.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ServiceNameError {
    /// No `:` and port after the name.
    #[error("no port: `<service>.<namespace>.svc.<cluster domain>:<port>`")]
    NoPort,
    /// Not `<service>.<namespace>.svc.<cluster domain>`.
    #[error("not a Service's name: `<service>.<namespace>.svc.<cluster domain>`")]
    NotAService,
    /// A label that is not a DNS label: 1 to 63 of `a-z`, `0-9` and `-`, neither first nor
    /// last a `-`.
    #[error("`{0}` is not a DNS label: 1 to 63 of a-z, 0-9 and -, not starting or ending in -")]
    Label(String),
    /// A Service name that does not start with a letter, as Kubernetes requires.
    #[error("Service name `{0}` does not start with a letter")]
    ServiceStart(String),
    /// A name longer than DNS takes.
    #[error("the name is longer than 253 characters")]
    TooLong,
    /// A port that is neither a number from 1 to 65535, written without a leading zero, nor
    /// a port name.
    #[error(
        "port `{0}` is neither a number from 1 to 65535 nor a port name: 1 to 15 of a-z, 0-9 \
         and -, with a letter, not starting or ending in - and without --"
    )]
    Port(String),
}

impl KubernetesService {
    /// Reads `<service>.<namespace>.svc.<cluster domain>:<port>`, in one spelling: lower
    /// case, no trailing dot.
    ///
    /// # Errors
    ///
    /// Returns a [`ServiceNameError`] saying what is wrong with it.
    pub fn parse(text: &str) -> Result<Self, ServiceNameError> {
        let (name, port) = text.rsplit_once(':').ok_or(ServiceNameError::NoPort)?;
        let ServiceName {
            service,
            namespace,
            cluster_domain,
        } = ServiceName::parse(name)?;
        Ok(Self {
            service,
            namespace,
            cluster_domain,
            port: service_port(port).ok_or_else(|| ServiceNameError::Port(port.to_owned()))?,
        })
    }
}

impl ServiceName {
    /// Reads `<service>.<namespace>.svc.<cluster domain>`, in one spelling: lower case, no
    /// trailing dot.
    ///
    /// # Errors
    ///
    /// Returns a [`ServiceNameError`] saying what is wrong with it.
    pub fn parse(name: &str) -> Result<Self, ServiceNameError> {
        // A port written after it, as an upstream's Service takes one.
        if name.contains(':') {
            return Err(ServiceNameError::NotAService);
        }
        if name.len() > 253 {
            return Err(ServiceNameError::TooLong);
        }
        let labels: Vec<&str> = name.split('.').collect();
        let [service, namespace, svc, domain @ ..] = labels.as_slice() else {
            return Err(ServiceNameError::NotAService);
        };
        if *svc != "svc" || domain.is_empty() {
            return Err(ServiceNameError::NotAService);
        }
        for label in &labels {
            if !is_label(label) {
                return Err(ServiceNameError::Label((*label).to_owned()));
            }
        }
        if !service.starts_with(|c: char| c.is_ascii_lowercase()) {
            return Err(ServiceNameError::ServiceStart((*service).to_owned()));
        }
        Ok(Self {
            service: (*service).to_owned(),
            namespace: (*namespace).to_owned(),
            cluster_domain: domain.join("."),
        })
    }
}

impl fmt::Display for ServiceName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.svc.{}",
            self.service, self.namespace, self.cluster_domain
        )
    }
}

impl fmt::Display for KubernetesService {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}.{}.svc.{}:",
            self.service, self.namespace, self.cluster_domain
        )?;
        match &self.port {
            ServicePort::Named(name) => f.write_str(name),
            ServicePort::Only(number) => write!(f, "{number}"),
        }
    }
}

/// An RFC 1123 label, as Kubernetes names namespaces and Services.
fn is_label(label: &str) -> bool {
    (1..=63).contains(&label.len())
        && label
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !label.starts_with('-')
        && !label.ends_with('-')
}

/// A port number without a leading zero, or a port name as Kubernetes takes one (an
/// IANA service name: 1 to 15 characters, a letter among them, no `--`).
fn service_port(port: &str) -> Option<ServicePort> {
    if !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit()) {
        if port.starts_with('0') {
            return None;
        }
        return port.parse().ok().map(ServicePort::Only);
    }
    let named = (1..=15).contains(&port.len())
        && port
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && port.bytes().any(|b| b.is_ascii_lowercase())
        && !port.starts_with('-')
        && !port.ends_with('-')
        && !port.contains("--");
    named.then(|| ServicePort::Named(port.to_owned()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn named(service: &str, namespace: &str, domain: &str, port: &str) -> KubernetesService {
        KubernetesService {
            service: service.to_owned(),
            namespace: namespace.to_owned(),
            cluster_domain: domain.to_owned(),
            port: ServicePort::Named(port.to_owned()),
        }
    }

    #[test]
    fn a_service_without_a_port_is_read_and_shown_as_written() {
        assert_eq!(
            ServiceName::parse("edgerush.edge.svc.cluster.local"),
            Ok(ServiceName {
                service: "edgerush".to_owned(),
                namespace: "edge".to_owned(),
                cluster_domain: "cluster.local".to_owned(),
            })
        );
        assert_eq!(
            ServiceName::parse("edgerush.edge.svc.cluster.local")
                .unwrap()
                .to_string(),
            "edgerush.edge.svc.cluster.local"
        );
        for (text, error) in [
            (
                "edgerush.edge.svc.cluster.local:http",
                ServiceNameError::NotAService,
            ),
            ("edgerush.edge.svc", ServiceNameError::NotAService),
            (
                "edgerush.edge.svc.Cluster.local",
                ServiceNameError::Label("Cluster".to_owned()),
            ),
            (
                "1edgerush.edge.svc.cluster.local",
                ServiceNameError::ServiceStart("1edgerush".to_owned()),
            ),
        ] {
            assert_eq!(ServiceName::parse(text), Err(error), "{text}");
        }
    }

    #[test]
    fn a_named_port_is_read_with_the_service_and_namespace() {
        assert_eq!(
            KubernetesService::parse("web.shop.svc.cluster.local:http"),
            Ok(named("web", "shop", "cluster.local", "http"))
        );
    }

    #[test]
    fn a_number_is_the_services_only_unnamed_port() {
        let service = KubernetesService::parse("web.shop.svc.cluster.local:8080").unwrap();
        assert_eq!(
            service.port,
            ServicePort::Only(NonZeroU16::new(8080).unwrap())
        );
    }

    #[test]
    fn the_cluster_domain_is_whatever_follows_svc() {
        let service = KubernetesService::parse("web.shop.svc.edge.example.internal:http").unwrap();
        assert_eq!(service.cluster_domain, "edge.example.internal");
        assert_eq!(
            KubernetesService::parse("web.shop.svc.k8s:http").map(|s| s.cluster_domain),
            Ok("k8s".to_owned())
        );
    }

    #[test]
    fn it_is_shown_as_it_was_written() {
        for text in [
            "web.shop.svc.cluster.local:http",
            "a1.b-2.svc.cluster.local:65535",
            "web.shop.svc.edge.example.internal:grpc-web",
        ] {
            assert_eq!(KubernetesService::parse(text).unwrap().to_string(), text);
        }
    }

    #[test]
    fn a_name_that_is_not_a_services_is_refused() {
        for text in [
            "web.shop.svc:http",
            "web.shop:http",
            "web:http",
            "web.shop.pod.cluster.local:http",
            "web.shop.svc.cluster.local.:http",
            ".shop.svc.cluster.local:http",
        ] {
            let refused = KubernetesService::parse(text);
            assert!(
                matches!(
                    refused,
                    Err(ServiceNameError::NotAService | ServiceNameError::Label(_))
                ),
                "{text}: {refused:?}"
            );
        }
        assert_eq!(
            KubernetesService::parse("web.shop.svc.cluster.local"),
            Err(ServiceNameError::NoPort)
        );
    }

    #[test]
    fn labels_are_lower_case_dns_labels() {
        for label in ["Web", "-web", "web-", "we_b", &"a".repeat(64)] {
            let text = format!("{label}.shop.svc.cluster.local:http");
            assert_eq!(
                KubernetesService::parse(&text),
                Err(ServiceNameError::Label(label.to_owned())),
                "{text}"
            );
        }
        assert!(
            KubernetesService::parse(&format!("{}.shop.svc.cluster.local:http", "a".repeat(63)))
                .is_ok()
        );
        assert_eq!(
            KubernetesService::parse("web.Shop.svc.cluster.local:http"),
            Err(ServiceNameError::Label("Shop".to_owned()))
        );
    }

    #[test]
    fn a_service_name_starts_with_a_letter_and_a_namespace_need_not() {
        assert_eq!(
            KubernetesService::parse("1web.shop.svc.cluster.local:http"),
            Err(ServiceNameError::ServiceStart("1web".to_owned()))
        );
        assert!(KubernetesService::parse("web.1shop.svc.cluster.local:http").is_ok());
    }

    #[test]
    fn a_name_longer_than_dns_takes_is_refused() {
        let domain = vec!["a".repeat(60); 4].join(".");
        let text = format!("web.shop.svc.{domain}:http");
        assert_eq!(
            KubernetesService::parse(&text),
            Err(ServiceNameError::TooLong)
        );
    }

    #[test]
    fn a_port_is_a_number_or_a_port_name_in_one_spelling() {
        for port in [
            "0",
            "080",
            "65536",
            "",
            "-http",
            "http-",
            "ht--tp",
            "HTTP",
            "h_t",
            "a-very-long-port",
            "123-456",
        ] {
            let text = format!("web.shop.svc.cluster.local:{port}");
            assert_eq!(
                KubernetesService::parse(&text),
                Err(ServiceNameError::Port(port.to_owned())),
                "{text}"
            );
        }
        for port in [
            "1",
            "65535",
            "http",
            "h2c",
            "grpc-web",
            "a-very-long-por",
            "8080a",
        ] {
            let text = format!("web.shop.svc.cluster.local:{port}");
            assert!(KubernetesService::parse(&text).is_ok(), "{text}");
        }
    }
}
