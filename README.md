# pgtern

**A PostgreSQL client for Python with zero dependencies.**

pgtern speaks the PostgreSQL wire protocol directly, in pure Python and the standard library only. There is no C library dependency, no C extension and no build step, so `pip install pgtern` is the whole setup. It offers a familiar DB-API style interface in two flavours: blocking, and `asyncio`.

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

## Why pgtern

- **Nothing to audit but the code you run.** No transitive dependencies means no dependency tree to pin, scan or keep patched. The package is a small, readable, pure-Python wheel.
- **Installs anywhere Python runs.** No compiler, no system libraries, no architecture-specific binaries. Containers, minimal images and locked-down hosts all get the same package.
- **Encrypted by default, verifiable.** SCRAM-SHA-256 authentication, TLS whenever the server offers it, and certificate and hostname verification with `sslmode=verify-full`.
- **Blocking and async, one API.** The same concepts, names and behaviour in `pgtern` and `pgtern.aio`, including pools, transactions, COPY and notifications.
- **Fast bulk writes.** `executemany` sends a whole batch as a single pipelined transaction. In measurements on PostgreSQL 16 it ran about five times faster than a row-by-row loop, both blocking and async.
- **Built for modern deployments.** It uses unnamed prepared statements only, so it cooperates with transaction-mode connection poolers. Pools check connections before handing them out and replace any the server has closed.

## Install

```
pip install pgtern
```

Requires Python 3.11 or newer.

## Connecting

`pgtern.connect` accepts URL and key=value DSNs, the standard `PG*` environment variables, Unix sockets, and a list of hosts for failover.

```python
conn = pgtern.connect("postgresql://app@db1.internal,db2.internal/app?sslmode=verify-full&sslrootcert=/etc/ssl/ca.pem")
```

TLS modes follow the PostgreSQL convention: `disable`, `allow`, `prefer`, `require`, `verify-ca` and `verify-full`. Authentication supports SCRAM-SHA-256, MD5, cleartext and trust.

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

Parameters use `%s` placeholders and are always sent separately from the SQL text. The `sql` module composes identifiers and literals safely:

```python
import pgtern
from pgtern import sql

with pgtern.connect(dsn) as conn:
    query = sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier("accounts"))
    conn.execute(query, [1])
```

## Rows

Choose how rows come back with a row factory: `tuple_row` (the default), `dict_row`, or `namedtuple_row`. Pass one to `connect` or to a single cursor.

## Bulk loading

`executemany` sends the whole batch as one pipelined transaction. In autocommit mode a failing row rolls back the entire batch; inside a transaction it aborts that transaction, as usual.

```python
with pgtern.connect(dsn, autocommit=True) as conn:
    conn.cursor().executemany("INSERT INTO events (kind, payload) VALUES (%s, %s)", rows)
```

For the fastest loads, use `COPY`:

```python
with pgtern.connect(dsn) as conn:
    with conn.cursor().copy("COPY events (kind, payload) FROM STDIN") as copy:
        for kind, payload in rows:
            copy.write_row((kind, payload))
```

Exporting works the same way with `COPY ... TO STDOUT`, read through `for chunk in copy` or `copy.rows()`.

## Streaming large results

A named cursor runs on the server and fetches in batches, so a result never has to fit in memory:

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

## Real-time notifications

```python
with pgtern.connect(dsn) as conn:
    conn.execute("LISTEN orders")
    for note in conn.notifies(timeout=30):
        print(note.channel, note.payload)
```

The asyncio connection supports `async for` over the same notifications. Session features such as `LISTEN` run on a direct or session-mode connection.

## Binary format and pipelines

Opt into binary transfer per statement with `binary=True`. It covers integers, floats, booleans, text, bytea, uuid, dates and times, intervals, json/jsonb, numeric results and one-dimensional arrays of these. Other types keep using text.

Pipeline mode sends many statements before reading any reply:

```python
with pgtern.connect(dsn) as conn:
    with conn.pipeline() as pipe:
        a = pipe.execute("INSERT INTO queue (job) VALUES (%s)", ["a"])
        b = pipe.execute("SELECT count(*) FROM queue")
    print(b.fetchone())    # results are ready once the block ends
```

Statements are read in order at each sync point (`pipe.sync()`, the end of the block, or every `batch_size` statements). A sync point raises the first failure in its batch, so a failed statement cannot pass silently. With `autocommit=True` a sync point is a commit point, so the statements in one batch succeed or fail together. Statements after a failure are skipped and raise `pgtern.errors.PipelineAborted`.

## Custom types

Register how a PostgreSQL type becomes a Python value, and how a Python class is sent:

```python
from enum import Enum
from pgtern import adapters

class Mood(Enum):
    SAD = "sad"
    HAPPY = "happy"

adapters.register_loader("mood", adapters.enum_loader(Mood))                  # CREATE TYPE mood AS ENUM (...)
adapters.register_loader("span", adapters.range_loader(int))                  # CREATE TYPE span AS RANGE (subtype = int4)
adapters.register_dumper(Mood, lambda m: m.value)
```

Loaders are looked up by type name once per connection and cached. Registering a loader applies to connections opened afterwards, and to pooled connections once they are returned to the pool.

## Pools

```python
from pgtern.pool import ConnectionPool

pool = ConnectionPool(dsn, min_size=4, max_size=20, timeout=5)
with pool.connection() as conn:
    conn.execute("SELECT 1")
```

Pools keep `min_size` connections warm and grow to `max_size` on demand. A connection is checked for liveness each time it is handed out, rolled back if it comes back mid-transaction, and replaced when the server has closed it. Waiting callers receive `pgtern.PoolTimeout` after `timeout` seconds.

## Observability

Query hooks see every statement and its row count, without ever seeing bound parameter values:

```python
from contextlib import contextmanager
import pgtern
import time

@contextmanager
def timing(info):
    started = time.perf_counter()
    try:
        yield
    finally:
        print(f"{info.statement!r} took {time.perf_counter() - started:.3f}s ({info.rowcount} rows)")

remove = pgtern.add_query_hook(timing)
```

The same hook point works for OpenTelemetry spans, metrics, and audit logging.

## Errors

Every error is a subclass of `pgtern.Error`, and each common SQLSTATE has its own class, such as `UniqueViolation` or `DeadlockDetected`. Catch the family you need, or the specific case:

```python
with pgtern.connect(dsn) as conn:
    try:
        conn.execute("INSERT INTO accounts (id) VALUES (%s)", [1])
    except pgtern.IntegrityError:
        conn.rollback()  # a failed transaction must be rolled back before the block ends
```

## Compatibility

- **PostgreSQL:** 14, 15, 16, 17 and 18.
- **Python:** 3.11 and newer. The continuous integration workflow runs the test suite on every supported version.
- **Platforms:** anywhere CPython runs.

## Development

```
pip install -e ".[dev]"
eval "$(scripts/start-test-postgres.sh)"    # throwaway server with SCRAM, MD5, cleartext, trust and TLS users
pytest
scripts/start-test-postgres.sh stop
```

Without a server the unit tests still run. The integration tests need `PGTERN_TEST_DSN`; the optional suites read `PGTERN_TEST_POOLER_DSN` (a transaction-mode connection pooler) and `PGTERN_TEST_RESTART_CMD` (a command that restarts the server). See `CONTRIBUTING.md` for the full workflow.

## Documentation and support

- Release history: [CHANGELOG.md](CHANGELOG.md)
- Contributing: [CONTRIBUTING.md](CONTRIBUTING.md)
- Security reports: [SECURITY.md](SECURITY.md)

## License

Apache License 2.0. See [LICENSE](LICENSE).
