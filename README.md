# pgtern

[![CI](https://github.com/karawash-projects/pgtern/actions/workflows/ci.yml/badge.svg)](https://github.com/karawash-projects/pgtern/actions/workflows/ci.yml)
[![License: Apache 2.0](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/karawash-projects/pgtern/blob/main/LICENSE)

pgtern is a PostgreSQL client for Python. It speaks the PostgreSQL wire protocol directly and uses only the Python standard library, so there are no runtime dependencies, no C extensions and no build step. It provides a DB-API style interface in two forms: blocking, and `asyncio`.

> **Status: Beta.** pgtern is at version 0.x. The API may change before 1.0; changes are listed in the [CHANGELOG](https://github.com/karawash-projects/pgtern/blob/main/CHANGELOG.md).

```python
import pgtern
from pgtern.rows import dict_row

with pgtern.connect("postgresql://app:secret@localhost/app", row_factory=dict_row) as conn:
    with conn.transaction():
        conn.execute("INSERT INTO notes (body) VALUES (%s)", ["hello"])
    print(conn.execute("SELECT id, body FROM notes ORDER BY id").fetchall())
```

```python
import asyncio
from pgtern import aio

async def main():
    async with aio.AsyncConnectionPool("postgresql://app:secret@localhost/app", min_size=2, max_size=10) as pool:
        async with pool.connection() as conn:
            cur = await conn.execute("SELECT %s::int", [1])
            print(cur.fetchall())

asyncio.run(main())
```

## Features

- No runtime dependencies. The package is a pure-Python wheel for Python 3.11 and newer.
- Blocking and `asyncio` APIs with the same names and behaviour, including pools, transactions, `COPY`, notifications and pipelines.
- SCRAM-SHA-256 authentication, and SCRAM-SHA-256-PLUS channel binding over TLS.
- TLS modes `disable`, `allow`, `prefer`, `require`, `verify-ca` and `verify-full`.
- Connection pools with liveness checks, idle and lifetime limits, and a fork-safe reset.
- Parameters are always sent separately from the SQL text. A helper module composes identifiers and literals safely.
- Opt-in binary result format for common types, and text format for everything else.
- Server-side cursors for large results, and batched `executemany`.
- Query hooks for logging, timing and tracing. They never receive bound parameter values.
- Exceptions organised by SQLSTATE class.

## Installation

```
pip install pgtern
```

pgtern requires Python 3.11 or newer.

## Connecting

`pgtern.connect` accepts URL and key=value connection strings, the standard `PG*` environment variables, Unix sockets, and a list of hosts for failover.

```python
conn = pgtern.connect("postgresql://app@db1.internal,db2.internal/app?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem")
```

TLS follows the PostgreSQL conventions. Under `allow` and `prefer`, the client refuses to send MD5 or cleartext passwords over an unencrypted connection. Under `disable` it sends them, because encryption was explicitly turned off. Set `require_auth=scram-sha-256` to refuse any server that does not use SCRAM, including servers that allow trust authentication.

Failover moves to the next host after any startup failure, including a timeout during TLS negotiation. Passwords can come from a `.pgpass` file (`PGPASSFILE` or `~/.pgpass`). A password file that other users can read is ignored.

Other supported options:

- `target_session_attrs`: `read-write`, `read-only`, `primary`, `standby` or `prefer-standby`.
- `hostaddr`: connect to an address while keeping the host name for certificate checks.
- `sslpassword` and `sslcrl`.
- `connect_timeout`: seconds allowed for each host attempt, from opening the socket to the end of startup. The default is 30. `0` means no limit.

## Queries and transactions

```python
with pgtern.connect(dsn) as conn:
    conn.execute("CREATE TABLE IF NOT EXISTS accounts (id int PRIMARY KEY, balance numeric)")

    with conn.transaction():               # commits on success, rolls back on error
        conn.execute("UPDATE accounts SET balance = balance - %s WHERE id = %s", [10, 1])
        conn.execute("UPDATE accounts SET balance = balance + %s WHERE id = %s", [10, 2])

    cur = conn.execute("SELECT id, balance FROM accounts WHERE balance > %s", [0])
    for row in cur:
        print(row)
```

Placeholders are `%s`. Values are sent as parameters, never interpolated into the SQL text. To build identifiers and literals safely, use the `sql` module:

```python
from pgtern import sql

with pgtern.connect(dsn) as conn:
    query = sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier("accounts"))
    conn.execute(query, [1])
```

## Row formats

Choose how rows are returned with a row factory: `tuple_row` (the default), `dict_row` or `namedtuple_row`. Pass one to `connect`, or to a single cursor.

## Bulk loading

`executemany` sends the whole batch as one pipelined transaction. In autocommit mode, a failing row rolls back the entire batch. Inside a transaction, it aborts that transaction as usual.

```python
with pgtern.connect(dsn, autocommit=True) as conn:
    conn.cursor().executemany("INSERT INTO events (kind, payload) VALUES (%s, %s)", rows)
```

For the largest loads, use `COPY`:

```python
with pgtern.connect(dsn) as conn:
    with conn.cursor().copy("COPY events (kind, payload) FROM STDIN") as copy:
        for kind, payload in rows:
            copy.write_row((kind, payload))
```

Export uses `COPY ... TO STDOUT`, read with `for chunk in copy` or `copy.rows()`.

## Streaming large results

A named cursor runs on the server and fetches rows in batches, so a result never has to fit in memory:

```python
with pgtern.connect(dsn) as conn:
    with conn.transaction():
        with conn.cursor("big_report") as cur:
            cur.execute("SELECT * FROM events")
            cur.itersize = 1000
            for row in cur:
                process(row)
```

Server-side cursors support `scrollable`, `withhold` and `scroll()`.

## Notifications

```python
with pgtern.connect(dsn, autocommit=True) as conn:   # LISTEN takes effect at once only in autocommit
    conn.execute("LISTEN orders")
    for note in conn.notifies(timeout=30):
        print(note.channel, note.payload)
```

The async connection provides the same notifications through `async for`. `LISTEN` needs a session that stays on one server connection, so use a direct connection or session-mode pooling for it.

## Pipelines

Pipeline mode sends many statements before reading any reply:

```python
with pgtern.connect(dsn) as conn:
    with conn.pipeline() as pipe:
        a = pipe.execute("INSERT INTO queue (job) VALUES (%s)", ["a"])
        b = pipe.execute("SELECT count(*) FROM queue")
    print(b.fetchone())    # results are ready once the block ends
```

Replies are read in order at each sync point: `pipe.sync()`, the end of the block, or every `batch_size` statements. A sync point raises the first failure in its batch, so a failed statement cannot pass silently. Under autocommit, a sync point is also a commit point, so the statements in one batch succeed or fail together. If the block exits with an exception under autocommit, the batch is rolled back. Statements after a failure raise `pgtern.errors.PipelineAborted`. Pipelines support text format only, and `COPY` cannot run inside one.

## Values and types

- Python `int` parameters are sent as `int4`, `int8` or `numeric`, depending on size. Use an explicit cast such as `%s::smallint` when a function takes `smallint`.
- `timestamptz` values are returned in UTC, in both text and binary format, with the same `tzinfo`.
- Intervals with months or years decode to `pgtern.Interval(months, delta)`, because a month has no fixed length. Other intervals decode to `timedelta`.
- `time` values of `24:00:00` raise `DataError`, because Python has no such time.
- Dates and times follow the session's `DateStyle`, and text follows its `client_encoding`. Both are read from the server after every change. Non-ASCII text under a non-UTF-8 encoding raises `NotSupportedError` before anything is sent.

Register loaders for PostgreSQL types and dumpers for Python classes:

```python
from enum import Enum
from pgtern import adapters

class Mood(Enum):
    SAD = "sad"
    HAPPY = "happy"

adapters.register_loader("mood", adapters.enum_loader(Mood))           # CREATE TYPE mood AS ENUM (...)
adapters.register_loader("span", adapters.range_loader(int))           # CREATE TYPE span AS RANGE (subtype = int4)
adapters.register_dumper(Mood, lambda m: m.value)
```

Loaders are looked up by type name once per connection and cached. A registered loader applies to connections opened afterwards, and to pooled connections when they are next returned to the pool.

## Binary format

Opt in per statement with `binary=True`. It covers integers, floats, booleans, text, `bytea`, `uuid`, dates, times, intervals, `json` and `jsonb`, `numeric` results, and one-dimensional arrays of these. Other types use text.

## Pools

```python
from pgtern.pool import ConnectionPool

pool = ConnectionPool(dsn, min_size=4, max_size=20, timeout=5)
with pool.connection() as conn:
    conn.execute("SELECT 1")
```

A pool keeps `min_size` connections open and grows up to `max_size` on demand. The async pool keeps `min_size` open with a background task, and retires connections that have been idle longer than `max_idle` while the pool is above `min_size`. A connection is checked for liveness when it is handed out, rolled back if it was returned in the middle of a transaction, and replaced if the server has closed it. A caller that waits longer than `timeout` seconds receives `pgtern.PoolTimeout`.

## Forking

Do not use a connection across `fork()`. A forked child that uses an inherited connection gets `OperationalError`. A pool resets itself in the child: on first use it discards the parent's connections without closing them, which would otherwise end the parent's sessions, and then opens its own.

## Query hooks

A hook sees every statement, its row count and the connection target. It never sees bound parameter values:

```python
from contextlib import contextmanager
import time
import pgtern

@contextmanager
def timing(info):
    started = time.perf_counter()
    try:
        yield
    finally:
        print(f"{info.statement!r} took {time.perf_counter() - started:.3f}s ({info.rowcount} rows)")

remove = pgtern.add_query_hook(timing)
```

A hook that fails on entry or exit is logged and ignored, so it never changes the result of a statement.

## Limits and safety

- `max_result_bytes` is a connection attribute that caps the bytes one statement may return. It is `None` (no cap) by default. A result over the cap is read and discarded, the connection stays usable, and the statement raises `DataError`. Results are held in memory, so use a server-side cursor for large ones.
- Using a connection from inside a `COPY` block, or from inside a notice or notify handler, raises `ProgrammingError` at once.
- A password is not sent in plaintext unless you choose `sslmode=disable`, so a server that declines TLS under `prefer` cannot collect it.
- Over TLS, the login is bound to that connection with SCRAM-SHA-256-PLUS, so a login relayed through another connection fails.
- `verify-ca` and `verify-full` require a root certificate (`sslrootcert`). They fail rather than fall back to the system store.

## Errors

Every exception derives from `pgtern.Error`. Each common SQLSTATE has its own class, such as `UniqueViolation` or `DeadlockDetected`, and each class also belongs to its broader category, such as `IntegrityError`:

```python
with pgtern.connect(dsn) as conn:
    try:
        conn.execute("INSERT INTO accounts (id) VALUES (%s)", [1])
    except pgtern.IntegrityError:
        conn.rollback()  # a failed transaction must be rolled back before the block ends
```

## Compatibility

- **PostgreSQL:** 14, 15, 16, 17 and 18.
- **Python:** 3.11, 3.12 and 3.13 are tested in continuous integration.
- **Platforms:** tested on Linux. Unix sockets, `fork()` and `.pgpass` permission checks behave differently on Windows, so check those yourself there.

## Development

```
pip install -e ".[dev]"
eval "$(scripts/start-test-postgres.sh)"    # throwaway server with SCRAM, MD5, cleartext, trust and TLS users
pytest
scripts/start-test-postgres.sh stop
```

The unit tests run without a server. The integration tests need `PGTERN_TEST_DSN`. Optional suites read `PGTERN_TEST_POOLER_DSN`, which points at a transaction-mode connection pooler, and `PGTERN_TEST_RESTART_CMD`, a command that restarts the test server.

## Contributing

See [CONTRIBUTING.md](https://github.com/karawash-projects/pgtern/blob/main/CONTRIBUTING.md). Report security issues privately as described in [SECURITY.md](https://github.com/karawash-projects/pgtern/blob/main/SECURITY.md).

## Changelog

Release history is in [CHANGELOG.md](https://github.com/karawash-projects/pgtern/blob/main/CHANGELOG.md).

## License

Apache License 2.0. See [LICENSE](https://github.com/karawash-projects/pgtern/blob/main/LICENSE).
