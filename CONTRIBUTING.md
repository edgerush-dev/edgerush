# Contributing to EdgeRush

Thank you for looking. EdgeRush is a hobby project with no deadlines: the data plane runs,
and the Kubernetes parts are still to come ([README.md](README.md) says what works today).
Help of any size is welcome: a bug report, a fuzz run, a review, a fix.

So is help from an AI assistant, from a question to a whole pull request; this repository
is itself written with one. [Its rules](#using-an-ai-assistant) are short.

## Before you start

- **Small fixes** (a bug, a missing test, a clearer error) can go straight to a pull
  request.
- **Anything larger** (a feature, a change of behaviour, a new dependency) starts as an
  issue, so that it is agreed before you spend time on it. Work follows a staged plan, the
  request path first, then rate limiting, then the control plane and the operator, and
  something that belongs to a later stage may have to wait for it.
- **Security problems** are reported privately, never in an issue: see
  [SECURITY.md](SECURITY.md).

The code often cites the design documents, as in "03 §6". They are not published yet;
where one stands between you and a change, ask in the issue.

## Building and checking

[README.md](README.md#building) has what a build needs: BoringSSL is built from source and
wants cmake and libclang, and NASM as well on Windows. Then

```sh
./check.sh
```

runs every check the project has (formatting, the dependencies' advisories and licences,
clippy, a build without test features, the tests, the docs, and the benchmarks and fuzz
targets building) and stops at the first that fails. A pull request is ready when it
passes. Judge it by its exit status, not by reading the output. CI runs the same script on
Linux and on Windows; Linux is the platform that counts, but Windows has to stay green as
well.

## What a change needs

- **Tests, in the same commit.** A bug fix starts with a test that fails without it. Break
  the change and watch each new test fail: a test that cannot fail proves nothing. Match
  the test to the code: unit and property tests (`proptest`) for logic, comparison with
  the slow reference implementations for the router and the filters, integration tests
  with real sockets for the proxy, a fuzz target for anything that reads untrusted input.
- **Never weaken, skip or delete a test** to make something pass. If a test is wrong, say
  why in the commit and fix it on purpose.
- **A benchmark for code on the request path**, written with it.
- **Docs.** Public items get rustdoc. Comments say why, not what. A change users can see
  updates the README.

The code is held to a few rules. Lints enforce what they can; review holds the rest.

- No `unsafe`: the workspace forbids it.
- No `unwrap`, `expect` or `panic!` outside tests, except for an invariant proven where it
  is used, with a comment saying why. Libraries return typed errors (`thiserror`).
- The pure crates, `edgerush-router`, `edgerush-config` and `edgerush-filters`, do no I/O:
  no sockets, clocks, randomness or async runtime. Those are passed in, which keeps the
  crates deterministic and easy to test.
- On the request path: no locks, no allocation that can be avoided, no string formatting,
  no lookups by name, nothing that blocks. A feature that is not configured costs nothing.
- Dependencies are a cost. Propose a new one in an issue first, with its licence and how
  well it is kept up; `deny.toml` says what `cargo deny` lets in.
- Clear over clever, and nothing the current stage does not need: no abstraction, option
  or generality ahead of its use.

## Commits and pull requests

- One logical change per commit: the code, its tests and its docs together, with no
  refactoring or reformatting of other code mixed in.
- Every commit builds and passes the tests.
- A subject in the imperative of at most 72 characters, and a body that says why when it
  is not obvious. `git log` has plenty of examples.
- A pull request usually lands squashed into one commit, its message tidied on the way,
  so push review fixes as new commits; there is no need to force-push. One whose commits
  are each a clean, separate change can land as they are, rebased. `main` has no merge
  commits, and CI must pass before anything is merged.
- Contributions are made under the project's licence, Apache-2.0 (its section 5). No CLA
  or sign-off is needed.

## Using an AI assistant

Welcome, for code, tests, reviews and writing. What it produces is held to the same bar as
anything else, and the pull request is yours:

- Understand every line you submit, and be ready to explain and change it.
- Run `./check.sh` yourself; an assistant saying that it passes is not the same thing.
- Credit the assistant with a `Co-Authored-By:` trailer naming the model, as this
  repository's own commits do.
- Keep issues and descriptions short and accurate: what you saw and what you changed, not
  what an assistant guessed.
- Comments are for people: nothing in the code is addressed to an assistant.
- Agents find their way here from [AGENTS.md](AGENTS.md), which adds what tends to trip
  them up in this repository; Claude Code reads it through `CLAUDE.md`.

## Conduct

Everyone taking part follows the [code of conduct](CODE_OF_CONDUCT.md). Reports go to
hello@edgerush.dev.
