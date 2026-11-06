# First query

```python
import pgtern
from pgtern.rows import dict_row

with pgtern.connect("postgresql://app:secret@localhost/app", row_factory=dict_row) as conn:
    row = conn.execute("SELECT %s::int AS n", [1]).fetchone()
    print(row["n"])
```

Choose a row format with a row factory: `tuple_row` (the default), `dict_row` or `namedtuple_row`. Pass one to `connect`, or to a single cursor.

The asynchronous version uses the same names:

```python
import asyncio
from pgtern import aio

async def main():
    async with await aio.connect("postgresql://app:secret@localhost/app") as conn:
        cur = await conn.execute("SELECT %s::int", [1])
        print(cur.fetchall())

asyncio.run(main())
```
