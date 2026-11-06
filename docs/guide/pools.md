# Connection pools

```python
from pgtern.pool import ConnectionPool

pool = ConnectionPool(dsn, min_size=4, max_size=20, timeout=5)
with pool.connection() as conn:
    conn.execute("SELECT 1")
```

A pool keeps `min_size` connections open and grows up to `max_size` on demand.

The async pool keeps `min_size` connections open with a background task. It retires connections that have been idle longer than `max_idle` while the pool is above `min_size`.

A connection is checked for liveness when it is handed out. It is rolled back if it was returned in the middle of a transaction, and replaced if the server has closed it. A caller that waits longer than `timeout` seconds receives `pgtern.PoolTimeout`.
