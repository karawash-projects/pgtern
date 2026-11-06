# Connecting

`pgtern.connect` accepts URL and key=value connection strings, the standard `PG*` environment variables, Unix sockets, and a list of hosts for failover.

```python
conn = pgtern.connect("postgresql://app@db1.internal,db2.internal/app?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem")
```

## TLS modes

`disable`, `allow`, `prefer`, `require`, `verify-ca` and `verify-full`, following the PostgreSQL conventions. See [Security and TLS](../operations/security.md) for what each one protects.

## Failover and session targets

Failover moves to the next host after any startup failure, including a timeout during TLS negotiation.

- `target_session_attrs`: `read-write`, `read-only`, `primary`, `standby` or `prefer-standby`.
- `hostaddr`: connect to an address while keeping the host name for certificate checks.
- `connect_timeout`: seconds allowed for each host attempt, from opening the socket to the end of startup. The default is 30. `0` means no limit.

## Passwords

Passwords can come from a `.pgpass` file (`PGPASSFILE` or `~/.pgpass`). A password file that other users can read is ignored.
