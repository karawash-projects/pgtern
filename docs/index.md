# pgtern

**A PostgreSQL client for Python, with zero runtime dependencies.**

pgtern speaks the PostgreSQL wire protocol directly, in pure Python and the standard library only. There are no C extensions and no build step, so `pip install pgtern` is the whole setup. It offers a DB-API style interface in two forms: blocking, and `asyncio`.

```python
import pgtern
from pgtern.rows import dict_row

with pgtern.connect("postgresql://app:secret@localhost/app", row_factory=dict_row) as conn:
    with conn.transaction():
        conn.execute("INSERT INTO notes (body) VALUES (%s)", ["hello"])
    print(conn.execute("SELECT id, body FROM notes ORDER BY id").fetchall())
```

## Why pgtern

- **No dependencies to manage.** The package is a small, readable, pure-Python wheel. There is no dependency tree to pin or scan.
- **Installs anywhere Python runs.** No compiler and no system libraries are needed.
- **Secure by default.** SCRAM-SHA-256 is verified before a login is accepted, and a password is not sent in plaintext unless you choose `sslmode=disable`.
- **Blocking and async, one API.** The same names and behaviour in `pgtern` and `pgtern.aio`.
- **Built for modern deployments.** Statements use unnamed prepared statements only, so they work with transaction-mode connection poolers.

## Where next

- [Installation](getting-started/installation.md): install the package and check the version.
- [Connecting](getting-started/connecting.md): DSNs, TLS and failover.
- [Queries and transactions](guide/queries.md): run statements safely.
- [Connection pools](guide/pools.md): share connections across a service.
- [Security and TLS](operations/security.md): what the client verifies and protects.
