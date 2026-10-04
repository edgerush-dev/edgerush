//! A worker's access-log records at the end of the data plane ([21 §4] in the docs).
//!
//! [21 §4]: ../../../../../docs/21-access-logs.md

use super::*;
use std::time::{SystemTime, UNIX_EPOCH};

/// At the end, a worker hands over the records it holds at once, not at its next sweep, and
/// they are written before [`Proxy::finish_logs`] returns.
#[tokio::test]
async fn the_end_has_every_worker_hand_over_its_records_at_once() {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory =
        std::env::temp_dir().join(format!("edgerush-end-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let path = directory.join("access.log");
    let yaml = format!(
        r#"
listeners:
  web: {{ address: "127.0.0.1:0", protocol: http, proxy_protocol: off, forwarding: {{ trusted_proxies: [], trusted_only_headers: [] }}, request_id: generate, access_log: {{ file: '{}' }} }}
routes: []
upstreams: {{}}
"#,
        path.display()
    );
    let config: Config = serde_saphyr::from_str(&yaml).unwrap();
    let proxy = Arc::new(Proxy::new(compile(&config).unwrap(), NonZeroUsize::MIN).unwrap());
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            // A sweep that would not come again for a minute: only being asked brings it.
            let limits = H1Limits {
                sweep: Duration::from_secs(60),
                ..H1Limits::default()
            };
            let worker = Worker::with_limits(Arc::clone(&proxy), limits);
            let _maintained = tokio::task::spawn_local(Rc::clone(&worker).maintain());
            tokio::task::yield_now().await;
            let sink = proxy.current.load().logs[0].unwrap();
            worker
                .batches
                .record(&proxy.logs, sink, |out| out.extend_from_slice(b"held\n"));
            let started = Instant::now();
            let finishing = Arc::clone(&proxy);
            let written =
                tokio::task::spawn_blocking(move || finishing.finish_logs(Duration::from_secs(10)))
                    .await
                    .unwrap();
            assert!(written);
            assert!(started.elapsed() < Duration::from_secs(5));
        })
        .await;
    assert_eq!(std::fs::read_to_string(&path).unwrap(), "held\n");
    // Only what this test made.
    let _removed = std::fs::remove_dir_all(&directory);
}
