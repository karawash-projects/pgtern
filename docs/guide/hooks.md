# Query hooks

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
