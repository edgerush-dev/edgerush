//! Fuzzes both HTTP/1 clients against each other and against the specification: one
//! input becomes a scripted upstream, and EdgeRush's own path and the engine's client are
//! each driven from it over a socket that is nothing but memory.
//!
//! **Neither client is the other's oracle.** What each of them did is held against a
//! reference framer written from RFC 9112 and RFC 9110, and against a lifecycle model
//! that says whether the connection survived; matching hyper is neither proof of
//! correctness nor a reason to accept an ambiguous frame. So what is asserted here is
//! asserted of ours: it may refuse whatever it likes and may meet a bound or a deadline
//! of its own, but it may never present a message the specification does not read there,
//! never read past a bound it set itself, and never keep a connection that cannot carry
//! another exchange.
//!
//! Of the engine's client, nothing is asserted beyond its reaching a classified outcome.
//! Reading something the grammar does not have is a difference of the first kind
//! ([13 §5](../../../docs/13-http1-upstream.md)), not a promise this project can make on
//! hyper's behalf.
//!
//! Every run is bounded — input bytes, steps, operations and simulated time — so a script
//! that stalls produces an outcome that says so instead of hanging the fuzzer.

#![no_main]

use edgerush_proxy::upstream::h1::H1Limits;
use edgerush_proxy::upstream::h1::differential::{Asking, Got, Path, Verdict, check, twice};
use edgerush_proxy::upstream::h1::reference::{self, Reading};
use edgerush_proxy::upstream::h1::script::{Budget, Script};
use libfuzzer_sys::fuzz_target;
use std::time::Duration;

/// What a run may spend. Small, because a fuzzer's value is in how many inputs it gets
/// through, and because every one of these is a bound a scripted peer cannot hold the
/// run past.
fn budget() -> Budget {
    Budget {
        steps: 8,
        said: 512,
        ops: 512,
        time: Duration::from_secs(120),
    }
}

/// Small bounds, so that an input a fuzzer will actually produce can reach them. The
/// rest stay as they are: a bound nothing here can reach is one this target has nothing
/// to say about.
fn limits() -> H1Limits {
    H1Limits {
        head: 256,
        fields: 8,
        chunk_line: 32,
        trailers: 128,
        trailer_fields: 4,
        interim_heads: 2,
        interim_bytes: 256,
        ..H1Limits::default()
    }
}

/// The request to send, chosen by one byte of the input: the answer is the subject, but
/// whether a connection may be used again turns on whether the request all went, so a
/// target that only ever sent bodyless requests would leave half of that untested.
fn asking(choice: u8) -> Asking {
    match choice % 5 {
        0 => Asking::Nothing,
        1 => Asking::Head,
        2 => Asking::Counted(b"ab".to_vec()),
        3 => Asking::Chunked(vec![b"a".to_vec(), b"b".to_vec()]),
        // A request that never finishes, so that the answer arrives over an upload that
        // is still going.
        _ => Asking::Endless(vec![b"a".to_vec()]),
    }
}

fuzz_target!(|bytes: &[u8]| {
    let Some((choice, rest)) = bytes.split_first() else {
        return;
    };
    let (budget, limits) = (budget(), limits());
    let ask = asking(*choice);
    let script = Script::decode(rest, &budget);

    // What ours did, held against both oracles. A disagreement is a finding: either the
    // message was read wrongly, or a bound was read past, or a connection was kept that
    // nothing should keep.
    let ours = check(Path::Ours, &script, &ask, budget, limits);
    assert!(
        !matches!(ours.verdict, Verdict::Disagrees(_)),
        "ours: {:?} against {:?}",
        ours,
        script.steps()
    );

    // The engine's client on the same script. Nothing is asserted of what it read; that
    // it reaches an outcome at all is what a bounded run promises.
    let theirs = check(Path::Theirs, &script, &ask, budget, limits);
    assert!(
        matches!(
            theirs.got,
            Got::Answer(_) | Got::Refused(_) | Got::Spent(_) | Got::Cancelled
        ),
        "theirs: {theirs:?}"
    );

    // And a second exchange on whatever the first one left. This is the only place a
    // boundary read wrongly shows: an answer here means the bytes held a second message
    // where the first one ended, and if they did not, the first exchange poisoned the
    // connection for the next one.
    let again = twice(Path::Ours, &script, &ask, budget, limits);
    assert!(
        !matches!(again.first, Got::Answer(ref seen) if seen.kept && again.second.is_none()),
        "a connection was kept and then not used: {again:?}"
    );
    if let Some(Got::Answer(second)) = &again.second {
        let spoken = script.within(again.tape.reached);
        let Reading::Read(one) = reference::read(&spoken.bytes, ask.asked(), spoken.ended) else {
            panic!("a second exchange on bytes that are not a message: {again:?}");
        };
        let Reading::Read(two) =
            reference::read(&spoken.bytes[one.boundary..], ask.asked(), spoken.ended)
        else {
            panic!("a second answer where the bytes hold one message: {again:?}");
        };
        assert_eq!(second.status, two.status, "{again:?}");
        assert_eq!(second.body, two.body, "{again:?}");
    }
});
