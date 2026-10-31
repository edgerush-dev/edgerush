//! What the data-plane pods a replica serves run ([04 §6](../../../docs/04-control-plane.md)):
//! for each, by its address, the generation of the data plane's Gateway in the config it
//! runs, and the latest config if it refused it. What `Programmed` is said from, once the
//! leader has merged every replica's. Pure: the versions offered and what each pod says are
//! passed in.

use edgerush_kubernetes::{ApiError, ApiServer};
use k8s_openapi::api::core::v1::ConfigMap;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{ObjectMeta, OwnerReference};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::net::IpAddr;
use std::time::Duration;
use tokio::sync::watch;

/// What one pod runs.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Pod {
    /// The generation of the Gateway in the config it runs: none before it runs one, or
    /// where that config serves no Gateway.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runs: Option<i64>,
    /// Why it refused the latest config, if it did.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub refused: Option<String>,
}

/// How many of the versions offered last are kept, though no pod says it runs them: a pod
/// may take one, and say so, after later ones are offered.
const KEPT: usize = 8;

/// What the pods a replica serves run, as they have said it.
#[derive(Debug, Default)]
pub struct Runs {
    /// The generation of the Gateway in each config version offered and still of use: the
    /// last [`KEPT`] offered, which a pod may run before it says so, and those a pod runs.
    /// None where the config serves no Gateway.
    versions: BTreeMap<String, Option<i64>>,
    /// The versions offered, the latest last: the last [`KEPT`].
    offered: VecDeque<String>,
    /// What each pod says it runs: the version, and the latest it refused, if it did.
    said: BTreeMap<IpAddr, Said>,
}

/// What a pod says: the config version it runs, and the latest it refused, if it did, with
/// why.
#[derive(Debug)]
struct Said {
    running: Option<String>,
    refused: Option<(String, String)>,
}

impl Runs {
    /// The config `version` offered, the Gateway at `generation` in it.
    pub fn offered(&mut self, version: &str, generation: Option<i64>) {
        self.versions.insert(version.to_owned(), generation);
        self.offered.push_back(version.to_owned());
        if self.offered.len() > KEPT {
            self.offered.pop_front();
        }
        self.prune();
    }

    /// `pod` says it runs `running`, and refused `refused` (the version, and why), if it
    /// did.
    pub fn reported(&mut self, pod: IpAddr, running: Option<&str>, refused: Option<(&str, &str)>) {
        self.said.insert(
            pod,
            Said {
                running: running.map(str::to_owned),
                refused: refused.map(|(version, why)| (version.to_owned(), why.to_owned())),
            },
        );
        self.prune();
    }

    /// `pod`'s stream has ended: it is another replica's to say, if any's.
    pub fn closed(&mut self, pod: IpAddr) {
        self.said.remove(&pod);
        self.prune();
    }

    /// What each pod runs, by its address: a refusal only of the latest config, as one
    /// of an earlier is no longer what it is offered.
    #[must_use]
    pub fn pods(&self) -> BTreeMap<IpAddr, Pod> {
        self.said
            .iter()
            .map(|(pod, Said { running, refused })| {
                let runs = running
                    .as_ref()
                    .and_then(|version| self.versions.get(version).copied().flatten());
                let refused = refused
                    .as_ref()
                    .filter(|(version, _)| self.offered.back() == Some(version))
                    .map(|(_, why)| why.clone());
                (*pod, Pod { runs, refused })
            })
            .collect()
    }

    /// Forgets the versions no pod runs, but the last [`KEPT`] offered.
    fn prune(&mut self) {
        let (offered, said) = (&self.offered, &self.said);
        self.versions.retain(|version, _| {
            offered.contains(version)
                || said
                    .values()
                    .any(|said| said.running.as_ref() == Some(version))
        });
    }
}

/// The label of each replica's report, its value the Lease the replicas share: what the
/// leader lists them by.
pub const REPORT_LABEL: &str = "gateway.edgerush.dev/acks";

/// The key of a report's data that says what each pod runs, as JSON.
const PODS: &str = "pods.json";

/// The least time between two writes of a report.
const PACE: Duration = Duration::from_secs(1);

/// What each pod runs, by address.
pub type Pods = BTreeMap<IpAddr, Pod>;

/// A replica that reports what its pods run: in a ConfigMap of its namespace, owned by its
/// Pod, so that the report goes with it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reporter {
    /// The replicas' namespace.
    pub namespace: String,
    /// The Lease the replicas share.
    pub lease: String,
    /// Its Pod's name.
    pub pod: String,
    /// Its Pod's UID, where it is told it: without it, the report is owned by none.
    pub uid: Option<String>,
}

impl Reporter {
    /// The name of its report.
    #[must_use]
    pub fn name(&self) -> String {
        format!("{}-acks-{}", self.lease, self.pod)
    }

    /// Its report, saying `pods`.
    ///
    /// # Errors
    ///
    /// If `pods` cannot be written as JSON, which they always can.
    pub fn report(&self, pods: &Pods) -> Result<ConfigMap, serde_json::Error> {
        Ok(ConfigMap {
            metadata: ObjectMeta {
                name: Some(self.name()),
                namespace: Some(self.namespace.clone()),
                labels: Some(BTreeMap::from([(
                    REPORT_LABEL.to_owned(),
                    self.lease.clone(),
                )])),
                owner_references: self.uid.as_ref().map(|uid| {
                    vec![OwnerReference {
                        api_version: "v1".to_owned(),
                        kind: "Pod".to_owned(),
                        name: self.pod.clone(),
                        uid: uid.clone(),
                        ..OwnerReference::default()
                    }]
                }),
                ..ObjectMeta::default()
            },
            data: Some(BTreeMap::from([(
                PODS.to_owned(),
                serde_json::to_string(pods)?,
            )])),
            ..ConfigMap::default()
        })
    }

    /// Writes its report each time `pods` changes, at most once a second, for as long as
    /// it is polled: made where there is none, replaced as of the version read where it
    /// says otherwise. A write that fails is said to `failed` and tried again.
    pub async fn publish(
        &self,
        api: &ApiServer,
        mut pods: watch::Receiver<Pods>,
        failed: impl Fn(&ApiError),
    ) {
        loop {
            let wanted = pods.borrow_and_update().clone();
            match self.write(api, &wanted).await {
                Ok(()) => {
                    if pods.changed().await.is_err() {
                        return;
                    }
                }
                Err(error) => failed(&error),
            }
            tokio::time::sleep(PACE).await;
        }
    }

    /// Has its report say `pods`.
    async fn write(&self, api: &ApiServer, pods: &Pods) -> Result<(), ApiError> {
        let mut report = self
            .report(pods)
            .map_err(|error| ApiError::Unreadable(error.to_string()))?;
        let name = self.name();
        match api.object::<ConfigMap>(&self.namespace, &name).await? {
            None => api.create(&self.namespace, &report).await.map(|_| ()),
            Some(current) if current.data == report.data => Ok(()),
            Some(current) => {
                report.metadata.resource_version = current.metadata.resource_version;
                api.replace(&self.namespace, &name, &report)
                    .await
                    .map(|_| ())
            }
        }
    }
}

/// What a replica's report says each pod runs; none where it says it unreadably.
#[must_use]
pub fn read(report: &ConfigMap) -> Option<Pods> {
    let pods = report.data.as_ref()?.get(PODS)?;
    serde_json::from_str(pods).ok()
}

#[cfg(test)]
mod tests {
    use super::{Pod, Pods, Reporter, Runs, read};
    use std::net::IpAddr;

    fn pod(last: u8) -> IpAddr {
        IpAddr::from([10, 0, 0, last])
    }

    #[test]
    fn each_pod_runs_the_generation_of_the_config_it_says() {
        let mut runs = Runs::default();
        runs.offered("a-1", Some(3));
        runs.reported(pod(1), Some("a-1"), None);
        runs.reported(pod(2), None, None);
        runs.offered("a-2", Some(4));
        assert_eq!(
            runs.pods().into_iter().collect::<Vec<_>>(),
            [
                (
                    pod(1),
                    Pod {
                        runs: Some(3),
                        refused: None
                    }
                ),
                (pod(2), Pod::default()),
            ]
        );
        runs.reported(pod(1), Some("a-2"), None);
        assert_eq!(runs.pods()[&pod(1)].runs, Some(4));
        // A config of no Gateway runs none.
        runs.offered("a-3", None);
        runs.reported(pod(2), Some("a-3"), None);
        assert_eq!(runs.pods()[&pod(2)].runs, None);
    }

    #[test]
    fn a_refusal_is_said_only_of_the_latest_config() {
        let mut runs = Runs::default();
        runs.offered("a-1", Some(3));
        runs.offered("a-2", Some(4));
        runs.reported(pod(1), Some("a-1"), Some(("a-2", "an unknown field")));
        assert_eq!(
            runs.pods()[&pod(1)],
            Pod {
                runs: Some(3),
                refused: Some("an unknown field".to_owned())
            }
        );
        runs.offered("a-3", Some(5));
        assert_eq!(
            runs.pods()[&pod(1)].refused,
            None,
            "refused what is no longer offered"
        );
    }

    #[test]
    fn a_pod_whose_stream_ends_is_said_no_more_and_versions_run_by_none_are_forgotten() {
        let mut runs = Runs::default();
        runs.offered("a-1", Some(3));
        runs.reported(pod(1), Some("a-1"), None);
        runs.offered("a-2", Some(4));
        for n in 3..=10 {
            runs.offered(&format!("a-{n}"), Some(n));
        }
        assert_eq!(
            runs.versions.len(),
            9,
            "the last eight kept, and a-1, run by a pod"
        );
        assert!(!runs.versions.contains_key("a-2"));
        runs.closed(pod(1));
        assert!(runs.pods().is_empty());
        assert_eq!(runs.versions.len(), 8);
        assert!(!runs.versions.contains_key("a-1"));
    }

    #[test]
    fn a_report_is_owned_by_its_pod_and_read_back_as_it_was_written() {
        let reporter = Reporter {
            namespace: "edge".to_owned(),
            lease: "edgerush-control".to_owned(),
            pod: "edgerush-control-7c9-x2".to_owned(),
            uid: Some("0b6c".to_owned()),
        };
        let pods: Pods = [
            (
                pod(1),
                Pod {
                    runs: Some(4),
                    refused: None,
                },
            ),
            (
                "fd00::7".parse().unwrap(),
                Pod {
                    runs: None,
                    refused: Some("an unknown field".to_owned()),
                },
            ),
        ]
        .into_iter()
        .collect();
        let report = reporter.report(&pods).unwrap();
        assert_eq!(
            report.metadata.name.as_deref(),
            Some("edgerush-control-acks-edgerush-control-7c9-x2")
        );
        let owner = &report.metadata.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (owner.kind.as_str(), owner.name.as_str(), owner.uid.as_str()),
            ("Pod", "edgerush-control-7c9-x2", "0b6c")
        );
        assert_eq!(
            report.metadata.labels.as_ref().unwrap()["gateway.edgerush.dev/acks"],
            "edgerush-control"
        );
        assert_eq!(read(&report), Some(pods));
        let unowned = Reporter {
            uid: None,
            ..reporter
        }
        .report(&Pods::new())
        .unwrap();
        assert_eq!(unowned.metadata.owner_references, None);
        assert_eq!(
            read(&k8s_openapi::api::core::v1::ConfigMap::default()),
            None
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_report_is_made_replaced_as_of_its_version_or_left_as_it_says() {
        use edgerush_kubernetes::Timing;
        use edgerush_kubernetes::testing::{Fake, answer, token};
        let reporter = Reporter {
            namespace: "edge".to_owned(),
            lease: "control".to_owned(),
            pod: "one".to_owned(),
            uid: None,
        };
        let path = "/api/v1/namespaces/edge/configmaps/control-acks-one";
        let saying = |runs: i64, version: &str| {
            let mut report = reporter
                .report(&Pods::from([(
                    pod(1),
                    Pod {
                        runs: Some(runs),
                        refused: None,
                    },
                )]))
                .unwrap();
            report.metadata.resource_version = Some(version.to_owned());
            serde_json::to_string(&report).unwrap()
        };
        // Its report says generation 4 in the namespace `edge`; there is none in `blog`.
        let (now, made) = (saying(4, "11"), saying(5, "12"));
        let fake =
            Fake::start_by_method(&["127.0.0.1"], move |method, asked| match (method, asked) {
                ("GET", p) if p == path => answer(200, &now),
                ("GET", _) => answer(404, r#"{"kind":"Status","code":404,"message":"not found"}"#),
                (_, _) => answer(201, &made),
            })
            .await;
        let api = fake.client(&token("acks", "secret"), Timing::default());
        let runs = |runs: i64| {
            Pods::from([(
                pod(1),
                Pod {
                    runs: Some(runs),
                    refused: None,
                },
            )])
        };
        reporter.write(&api, &runs(4)).await.unwrap();
        reporter.write(&api, &runs(5)).await.unwrap();
        let elsewhere = Reporter {
            namespace: "blog".to_owned(),
            ..reporter.clone()
        };
        elsewhere.write(&api, &runs(5)).await.unwrap();
        let writes: Vec<(String, String)> = fake
            .seen()
            .iter()
            .filter(|seen| seen.method != "GET")
            .map(|seen| (seen.method.clone(), seen.path.clone()))
            .collect();
        assert_eq!(
            writes,
            [
                ("PUT".to_owned(), path.to_owned()),
                (
                    "POST".to_owned(),
                    "/api/v1/namespaces/blog/configmaps".to_owned()
                ),
            ],
            "written though it said the same, or not as asked"
        );
        let put = fake
            .seen()
            .into_iter()
            .find(|seen| seen.method == "PUT")
            .unwrap();
        let put: k8s_openapi::api::core::v1::ConfigMap = serde_json::from_str(&put.body).unwrap();
        assert_eq!(put.metadata.resource_version.as_deref(), Some("11"));
        assert_eq!(read(&put), Some(runs(5)));
    }
}
