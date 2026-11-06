# Security and TLS

- **Password checks.** The server must prove it knows the password before a SCRAM login is accepted.
- **No plaintext passwords by accident.** A password is not sent in plaintext unless you choose `sslmode=disable`. A server that declines TLS under `prefer` cannot collect it.
- **Certificate verification.** `verify-ca` and `verify-full` require a root certificate (`sslrootcert`). They fail rather than fall back to the system store.
- **Channel binding.** Over TLS, the login is bound to that connection with SCRAM-SHA-256-PLUS (`tls-server-end-point`, RFC 5929), so a login relayed through another connection fails.
- **Stricter login.** Set `require_auth=scram-sha-256` to refuse any server that does not use SCRAM, including servers that allow trust authentication.

Under `allow` and `prefer`, the client refuses MD5 and cleartext passwords on an unencrypted connection. Under `disable` it sends them, because encryption was explicitly turned off.

Report vulnerabilities privately, as described in the [security policy](../more/security-policy.md).
