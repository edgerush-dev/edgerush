//! Fuzzes the reading of the admission webhook's reviews and the writing of their answers
//! ([04 §3](../../../docs/04-control-plane.md)): any bytes at all, as anyone in the cluster
//! can send the webhook any.
//!
//! Reading must not fail, and an error must be told without failing. A review read is
//! answered, refused, and the answer must be JSON that carries the review's uid as it was
//! read.
//!
//! `cargo fuzz run admission_review corpus/admission_review seeds/admission_review`.

#![no_main]

use edgerush_controller::admission::{answer, read};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let review = match read(data) {
        Ok(review) => review,
        Err(error) => {
            let _told = error.to_string();
            return;
        }
    };
    let sent: serde_json::Value =
        serde_json::from_slice(&answer(&review.uid, Some(&review.uid))).expect("an answer is JSON");
    assert_eq!(sent["response"]["uid"].as_str(), Some(review.uid.as_str()));
});
