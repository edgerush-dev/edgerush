# Security policy

## Supported versions

EdgeRush has no releases yet and is not for production. Only `main` is supported; fixes
land there.

## Reporting a vulnerability

Please do not open a public issue. Report it privately, either way:

- on GitHub: [report a vulnerability](https://github.com/edgerush-dev/edgerush/security/advisories/new)
  (under the Security tab), or
- by email to security@edgerush.dev.

Include the commit you tested (`git rev-parse HEAD`), the config, the steps or the bytes
that set it off, and what an attacker gains. A failing test or a fuzz input is the best
report of all.

## What happens next

EdgeRush is kept in spare time, so there is no fixed response time. Once a report is
confirmed, the fix is worked out in private and lands on `main`, and a GitHub security
advisory then describes it, crediting you unless you would rather not be named.

## Scope

All code in this repository. `vendor/quiche` and `vendor/h2` are patched copies of
[quiche](https://github.com/cloudflare/quiche) and [h2](https://github.com/hyperium/h2): a
problem that is in the upstream crate too should also be reported to that project, by its
own security policy.

The README lists what is not built yet, such as authentication and rate limiting; their
absence is not a vulnerability.
