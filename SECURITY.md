# Security policy

## Supported versions

Security fixes are released for the most recent minor version of pgtern.

| Version | Supported |
|---------|-----------|
| 0.3.x   | Yes       |
| < 0.3   | No        |

## Reporting a vulnerability

Please report suspected vulnerabilities privately. Use GitHub's
**Security → Report a vulnerability** feature on this repository. Do not open a public issue
or pull request for a vulnerability.

Include, where you can:

- the affected version and PostgreSQL server version;
- a description of the impact and the conditions needed to trigger it;
- steps or a minimal example that reproduce the problem.

You can expect an acknowledgement within 5 business days. We will keep you informed while
the issue is investigated, and we will credit you in the release notes if you wish.

## Scope

In scope: authentication (SCRAM, MD5, cleartext), TLS and certificate verification,
parameter encoding and SQL injection through the library's own escaping, handling of
untrusted server messages, and credential leakage in logs or error messages.
