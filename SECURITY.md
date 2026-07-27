# Security Policy

## Supported versions

Security fixes target the current `main` branch. No released version is
currently maintained separately.

## Reporting a vulnerability

Please do not open a public issue containing exploit details, credentials,
account data, or information that could put funds at risk.

Use GitHub's private vulnerability reporting flow from the repository's
**Security** tab. Include:

- affected commit or version;
- reproduction steps or a minimal proof of concept;
- expected and observed behavior;
- likely impact, especially whether live order submission or accounting is
  affected;
- any suggested mitigation.

If private reporting is unavailable, open a minimal public issue asking a
maintainer to establish a private channel. Do not include sensitive details in
that issue.

Maintainers will acknowledge a complete report when practical, validate it,
coordinate a fix, and credit the reporter if requested. Please allow time for a
fix before public disclosure.

## Operational safety

Never attach private keys, API credentials, wallet secrets, live journals, or
real account snapshots to a report. Reproduce findings in paper mode with
synthetic identifiers whenever possible.
