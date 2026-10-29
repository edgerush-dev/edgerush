//! The admission webhook ([04 §3](../../../docs/04-control-plane.md)): an `AdmissionReview`
//! the API server sends, read; the Ingress it asks about, judged; and the review answered.
//! Pure: the listener that takes reviews is elsewhere.
//!
//! An Ingress is judged alone, translated with no other Ingress, Service or Secret, and
//! refused only where it does not translate into something the proxy accepts. So nothing
//! is said of what hangs on other objects, and nothing answered names one: anyone who can
//! reach the webhook can send it a review, not only the API server. For the same reason
//! what is read is untrusted. A review that is not one is an error, which the listener
//! answers as a bad request; the API server, for its part, then admits the object as it
//! would through an outage. An error never repeats what it was sent.

use edgerush_translate::{Cluster, Plane, translate};
use k8s_openapi::api::networking::v1::{Ingress, IngressClass};
use serde::Deserialize;
use serde_json::json;

/// The version of the review read and written: the one the API server sends a webhook
/// registered with `admissionReviewVersions: [v1]`.
const API_VERSION: &str = "admission.k8s.io/v1";
const KIND: &str = "AdmissionReview";

/// A review, as read.
#[derive(Debug, Clone, PartialEq)]
pub struct Review {
    /// The request's own id, which its answer carries.
    pub uid: String,
    /// What it asks about.
    pub asked: Asked,
}

/// What a review asks about.
#[derive(Debug, Clone, PartialEq)]
pub enum Asked {
    /// An Ingress, as it is to be made or changed to.
    Ingress(Box<Ingress>),
    /// Anything else, or nothing (a deletion carries no object): not the webhook's to judge.
    Other,
}

/// Why a review could not be read.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ReviewError {
    /// It is not JSON, or not a review's.
    #[error("not an AdmissionReview: malformed at line {line}, column {column}")]
    Malformed {
        /// Where, as serde_json counts.
        line: usize,
        /// Where on that line.
        column: usize,
    },
    /// It is a review of another version, or not a review.
    #[error("not an AdmissionReview of admission.k8s.io/v1")]
    Version,
    /// It is a review with nothing asked.
    #[error("an AdmissionReview with no request")]
    NoRequest,
}

impl From<serde_json::Error> for ReviewError {
    fn from(error: serde_json::Error) -> Self {
        Self::Malformed {
            line: error.line(),
            column: error.column(),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Received {
    api_version: String,
    kind: String,
    request: Option<Request>,
}

#[derive(Deserialize)]
struct Request {
    uid: String,
    kind: GroupVersionKind,
    object: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct GroupVersionKind {
    group: String,
    version: String,
    kind: String,
}

/// The review `body` holds.
///
/// # Errors
///
/// If it is not an `admission.k8s.io/v1` review with a request, or the Ingress it asks
/// about is not one.
pub fn read(body: &[u8]) -> Result<Review, ReviewError> {
    let received: Received = serde_json::from_slice(body)?;
    if received.api_version != API_VERSION || received.kind != KIND {
        return Err(ReviewError::Version);
    }
    let request = received.request.ok_or(ReviewError::NoRequest)?;
    let kind = &request.kind;
    let ingress =
        kind.group == "networking.k8s.io" && kind.version == "v1" && kind.kind == "Ingress";
    let asked = match request.object {
        Some(object) if ingress => Asked::Ingress(Box::new(serde_json::from_value(object)?)),
        _ => Asked::Other,
    };
    Ok(Review {
        uid: request.uid,
        asked,
    })
}

/// Why `ingress` is refused, if it is: of the class of the data plane `plane` describes,
/// as `classes` say, it does not translate into something the proxy accepts. Its Secrets
/// are taken as they will be there, as `edgerush validate`'s are unless it is asked to
/// check them.
#[must_use]
pub fn judge(plane: &Plane, classes: &[IngressClass], ingress: &Ingress) -> Option<String> {
    let cluster = Cluster {
        ingress_classes: classes.to_vec(),
        ingresses: vec![ingress.clone()],
        services: Vec::new(),
        secrets: Vec::new(),
        secrets_assumed: true,
        ..Cluster::default()
    };
    // Alone, whatever is said of an Ingress is said of this one. What is said of an
    // IngressClass is that none is the data plane's, and what is said of the config as a
    // whole is of the data plane's own settings: neither is this Ingress's fault.
    let problems: Vec<String> = translate(plane, &cluster)
        .report
        .iter()
        .filter(|finding| finding.object.kind == "Ingress")
        .map(|finding| finding.problem.to_string())
        .collect();
    (!problems.is_empty()).then(|| problems.join("; "))
}

/// The answer to the review `uid`: admitted, or refused for the reason given.
#[must_use]
pub fn answer(uid: &str, refused: Option<&str>) -> Vec<u8> {
    let mut response = json!({ "uid": uid, "allowed": refused.is_none() });
    if let Some(refused) = refused {
        // Without a code, the API server says 403, Forbidden, as for any refusal.
        response["status"] = json!({ "message": refused });
    }
    json!({ "apiVersion": API_VERSION, "kind": KIND, "response": response })
        .to_string()
        .into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use serde_json::Value;

    /// A review as the API server sends one, of an Ingress made.
    fn review_of(kind: Value, object: Value) -> Value {
        json!({
            "apiVersion": "admission.k8s.io/v1",
            "kind": "AdmissionReview",
            "request": {
                "uid": "705ab4f5-6393-11e8-b7cc-42010a800002",
                "kind": kind,
                "resource": { "group": "networking.k8s.io", "version": "v1", "resource": "ingresses" },
                "requestKind": kind,
                "requestResource": { "group": "networking.k8s.io", "version": "v1", "resource": "ingresses" },
                "name": "web",
                "namespace": "shop",
                "operation": "CREATE",
                "userInfo": { "username": "admin", "uid": "014fbff9a07c", "groups": ["system:authenticated"] },
                "object": object,
                "oldObject": null,
                "dryRun": false,
                "options": { "apiVersion": "meta.k8s.io/v1", "kind": "CreateOptions" }
            }
        })
    }

    fn ingress_kind() -> Value {
        json!({ "group": "networking.k8s.io", "version": "v1", "kind": "Ingress" })
    }

    /// Ingress `web` in `shop`, of class `edge`: its `/` on `shop.example` to Service
    /// `shop`'s port 80, the host secured by Secret `shop-tls`.
    fn web() -> Value {
        json!({
            "apiVersion": "networking.k8s.io/v1",
            "kind": "Ingress",
            "metadata": { "name": "web", "namespace": "shop" },
            "spec": {
                "ingressClassName": "edge",
                "tls": [{ "hosts": ["shop.example"], "secretName": "shop-tls" }],
                "rules": [{ "host": "shop.example", "http": { "paths": [{
                    "path": "/", "pathType": "Prefix",
                    "backend": { "service": { "name": "shop", "port": { "number": 80 } } }
                }] } }]
            }
        })
    }

    fn bytes(value: &Value) -> Vec<u8> {
        serde_json::to_vec(value).unwrap()
    }

    #[test]
    fn an_ingress_asked_about_is_read_with_the_requests_uid() {
        let review = read(&bytes(&review_of(ingress_kind(), web()))).unwrap();
        assert_eq!(review.uid, "705ab4f5-6393-11e8-b7cc-42010a800002");
        let Asked::Ingress(ingress) = review.asked else {
            panic!("not read as an Ingress: {:?}", review.asked);
        };
        assert_eq!(ingress.metadata.name.as_deref(), Some("web"));
        assert_eq!(ingress.metadata.namespace.as_deref(), Some("shop"));
        let rules = ingress.spec.unwrap().rules.unwrap();
        assert_eq!(rules[0].host.as_deref(), Some("shop.example"));
    }

    #[test]
    fn another_kind_group_or_version_or_no_object_is_not_judged() {
        let service = json!({ "group": "", "version": "v1", "kind": "Service" });
        let beta = json!({ "group": "networking.k8s.io", "version": "v1beta1", "kind": "Ingress" });
        let class =
            json!({ "group": "networking.k8s.io", "version": "v1", "kind": "IngressClass" });
        let elsewhere = json!({ "group": "example.com", "version": "v1", "kind": "Ingress" });
        for (kind, object) in [
            (service, json!({ "metadata": { "name": "shop" } })),
            (beta, web()),
            (elsewhere, web()),
            (class, json!({ "metadata": { "name": "edgerush" } })),
            // A deletion's.
            (ingress_kind(), Value::Null),
        ] {
            let review = read(&bytes(&review_of(kind.clone(), object))).unwrap();
            assert_eq!(review.asked, Asked::Other, "{kind}");
            assert_eq!(review.uid, "705ab4f5-6393-11e8-b7cc-42010a800002");
        }
        // No object at all, rather than null.
        let mut review = review_of(ingress_kind(), web());
        review["request"].as_object_mut().unwrap().remove("object");
        assert_eq!(read(&bytes(&review)).unwrap().asked, Asked::Other);
    }

    #[test]
    fn what_is_not_a_v1_review_with_a_request_is_an_error_that_repeats_none_of_it() {
        let mut beta = review_of(ingress_kind(), web());
        beta["apiVersion"] = json!("admission.k8s.io/v1beta1");
        let mut other = review_of(ingress_kind(), web());
        other["kind"] = json!("TokenReview");
        let mut no_request = review_of(ingress_kind(), web());
        no_request.as_object_mut().unwrap().remove("request");
        let mut no_uid = review_of(ingress_kind(), web());
        no_uid["request"].as_object_mut().unwrap().remove("uid");
        let mut not_an_ingress = review_of(ingress_kind(), web());
        not_an_ingress["request"]["object"]["spec"]["rules"] = json!("secret-token-in-a-rule");
        for (body, error) in [
            (b"secret-token".to_vec(), "malformed"),
            (b"{\"apiVersion\": \"secret-token\"".to_vec(), "malformed"),
            (bytes(&beta), "admission.k8s.io/v1"),
            (bytes(&other), "admission.k8s.io/v1"),
            (bytes(&no_request), "no request"),
            (bytes(&no_uid), "malformed"),
            (bytes(&not_an_ingress), "malformed"),
        ] {
            let told = read(&body).unwrap_err().to_string();
            assert!(told.contains(error), "{told}");
            assert!(!told.contains("secret-token"), "{told}");
        }
    }

    /// The data plane: class `edge`, plain HTTP and HTTPS.
    fn plane() -> Plane {
        serde_saphyr::from_str(
            r#"
ingress_class: edge
cluster_domain: cluster.local
ingress_hosts_fall_through: false
ingress_https_redirect: true
ingress_https_redirect_max_age: 30
listeners:
  http:
    address: "[::]:8080"
    protocol: http
    proxy_protocol: off
    forwarding: { trusted_proxies: [], trusted_only_headers: [Forwarded] }
    request_id: generate
  https:
    address: "[::]:8443"
    protocol: https
    proxy_protocol: off
    forwarding: { trusted_proxies: [], trusted_only_headers: [Forwarded] }
    request_id: generate
    tls: { certificates: [], without_sni: none }
"#,
        )
        .unwrap()
    }

    /// IngressClass `name`, of `controller`, the cluster's default or not.
    fn class(name: &str, controller: &str, default: bool) -> IngressClass {
        serde_json::from_value(json!({
            "metadata": {
                "name": name,
                "annotations": { "ingressclass.kubernetes.io/is-default-class": default.to_string() }
            },
            "spec": { "controller": controller }
        }))
        .unwrap()
    }

    fn ours() -> Vec<IngressClass> {
        vec![class("edge", "edgerush.dev/ingress-controller", false)]
    }

    fn ingress(change: impl FnOnce(&mut Value)) -> Ingress {
        let mut ingress = web();
        change(&mut ingress);
        serde_json::from_value(ingress).unwrap()
    }

    #[test]
    fn an_ingress_of_the_class_that_translates_is_admitted_whatever_else_is_missing() {
        // Its Service and its Secret are not there: alone, they never are.
        assert_eq!(judge(&plane(), &ours(), &ingress(|_| {})), None);
        // A path it has twice, the second never taking a request: shadowed, not refused.
        let twice = ingress(|ingress| {
            let path = ingress["spec"]["rules"][0]["http"]["paths"][0].clone();
            ingress["spec"]["rules"][0]["http"]["paths"] = json!([path.clone(), path]);
        });
        assert_eq!(judge(&plane(), &ours(), &twice), None);
    }

    #[test]
    fn an_ingress_of_the_class_that_does_not_translate_is_refused_with_why() {
        let annotated = ingress(|ingress| {
            ingress["metadata"]["annotations"] =
                json!({ "nginx.ingress.kubernetes.io/rewrite-target": "/" });
        });
        let climbing = ingress(|ingress| {
            ingress["spec"]["rules"][0]["http"]["paths"][0]["path"] = json!("/a/../admin");
        });
        let address = ingress(|ingress| {
            ingress["spec"]["rules"][0]["host"] = json!("192.0.2.1");
            ingress["spec"]["tls"] = json!([]);
        });
        let by_annotation = ingress(|ingress| {
            ingress["spec"]
                .as_object_mut()
                .unwrap()
                .remove("ingressClassName");
            ingress["metadata"]["annotations"] = json!({ "kubernetes.io/ingress.class": "edge" });
        });
        for (ingress, why) in [
            (annotated, "not supported yet: annotations"),
            (
                climbing,
                "the data plane would refuse it: route `ingress/shop/web/shop.example`, rules[0], \
                 matches[0]: path: path contains a `.` or `..` segment",
            ),
            (address, "invalid: a host that is an IP address"),
            (
                by_annotation,
                "not supported yet: the `kubernetes.io/ingress.class` annotation",
            ),
        ] {
            assert_eq!(judge(&plane(), &ours(), &ingress).as_deref(), Some(why));
        }
    }

    #[test]
    fn an_ingress_not_of_the_class_is_admitted_however_it_is() {
        let bad = |class: Option<&str>| {
            ingress(|ingress| {
                ingress["metadata"]["annotations"] = json!({ "x": "y" });
                match class {
                    Some(class) => ingress["spec"]["ingressClassName"] = json!(class),
                    None => {
                        ingress["spec"]
                            .as_object_mut()
                            .unwrap()
                            .remove("ingressClassName");
                    }
                }
            })
        };
        let theirs = class("nginx", "k8s.io/ingress-nginx", true);
        let both = vec![ours()[0].clone(), theirs.clone()];
        // Of another class.
        assert_eq!(judge(&plane(), &both, &bad(Some("nginx"))), None);
        // Of none, where the default class is another's.
        assert_eq!(judge(&plane(), &both, &bad(None)), None);
        // Of the class, where it is not there or is another controller's: nothing is ours.
        assert_eq!(judge(&plane(), &[theirs], &bad(Some("edge"))), None);
        let taken = vec![class("edge", "k8s.io/ingress-nginx", false)];
        assert_eq!(judge(&plane(), &taken, &bad(Some("edge"))), None);
        // Of none, where the default class is ours, among others: judged.
        let default = vec![
            class("nginx", "k8s.io/ingress-nginx", false),
            class("edge", "edgerush.dev/ingress-controller", true),
        ];
        assert_eq!(
            judge(&plane(), &default, &bad(None)).as_deref(),
            Some("not supported yet: annotations")
        );
    }

    #[test]
    fn an_answer_admits_or_refuses_with_why_and_the_reviews_uid() {
        let admitted: Value = serde_json::from_slice(&answer("u-1", None)).unwrap();
        assert_eq!(
            admitted,
            json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": { "uid": "u-1", "allowed": true }
            })
        );
        let refused = answer("u-2", Some("not supported yet: annotations"));
        let refused: Value = serde_json::from_slice(&refused).unwrap();
        assert_eq!(
            refused,
            json!({
                "apiVersion": "admission.k8s.io/v1",
                "kind": "AdmissionReview",
                "response": {
                    "uid": "u-2",
                    "allowed": false,
                    "status": { "message": "not supported yet: annotations" }
                }
            })
        );
    }

    proptest! {
        /// Whatever a uid or a reason holds, quotes, escapes and control characters among
        /// it, the answer carries it as it is.
        #[test]
        fn an_answer_carries_what_it_is_given_as_it_is(
            uid in any::<String>(),
            refused in proptest::option::of(any::<String>()),
        ) {
            let sent: Value = serde_json::from_slice(&answer(&uid, refused.as_deref())).unwrap();
            let response = &sent["response"];
            prop_assert_eq!(&response["uid"], &json!(uid));
            prop_assert_eq!(&response["allowed"], &json!(refused.is_none()));
            prop_assert_eq!(response["status"]["message"].as_str(), refused.as_deref());
        }

        /// A review's uid is read as it was sent, whatever it holds.
        #[test]
        fn a_reviews_uid_is_read_as_it_was_sent(uid in any::<String>()) {
            let mut review = review_of(ingress_kind(), web());
            review["request"]["uid"] = json!(uid);
            prop_assert_eq!(read(&bytes(&review)).unwrap().uid, uid);
        }
    }
}
