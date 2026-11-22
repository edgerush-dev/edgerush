use super::*;

#[test]
fn a_start_says_what_became_of_the_limit_where_there_is_one() {
    let raised = OpenFiles::Raised {
        was: Some(1024),
        now: Some(524_288),
    };
    assert_eq!(
        raised.described().as_deref(),
        Some("open files: soft limit 1024 raised to 524288")
    );
    let same = OpenFiles::Raised {
        was: Some(4096),
        now: Some(4096),
    };
    assert_eq!(
        same.described().as_deref(),
        Some("open files: soft limit 4096")
    );
    let unlimited = OpenFiles::Raised {
        was: None,
        now: None,
    };
    assert_eq!(
        unlimited.described().as_deref(),
        Some("open files: soft limit none")
    );
    let kept = OpenFiles::Kept {
        now: Some(1024),
        why: std::io::ErrorKind::PermissionDenied,
    };
    assert_eq!(
        kept.described().as_deref(),
        Some("open files: soft limit 1024, not raised (permission denied)")
    );
    assert_eq!(OpenFiles::NoLimit.described(), None);
}

#[test]
fn the_limit_is_the_soft_one_the_process_runs_under() {
    let raised = OpenFiles::Raised {
        was: Some(1024),
        now: Some(524_288),
    };
    assert_eq!(raised.limit(), Some(524_288));
    let kept = OpenFiles::Kept {
        now: Some(1024),
        why: std::io::ErrorKind::PermissionDenied,
    };
    assert_eq!(kept.limit(), Some(1024));
    assert_eq!(OpenFiles::NoLimit.limit(), None);
}

/// The soft limit is the hard limit once it has been raised, or as near as the kernel
/// lets it be where the hard limit is none.
#[cfg(unix)]
#[test]
fn raising_takes_the_soft_limit_to_the_hard_one() {
    use rustix::process::{Resource, getrlimit};
    let opened = raise();
    let now = getrlimit(Resource::Nofile);
    assert!(matches!(opened, OpenFiles::Raised { .. }), "{opened:?}");
    match now.maximum {
        Some(hard) => assert_eq!(now.current, Some(hard)),
        None => assert!(now.current >= Some(1 << 20), "{now:?}"),
    }
    assert_eq!(opened.limit(), now.current);
}
