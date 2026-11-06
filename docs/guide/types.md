# Values and types

- Python `int` parameters are sent as `int4`, `int8` or `numeric`, depending on size. Use an explicit cast such as `%s::smallint` when a function takes `smallint`.
- `timestamptz` values are returned in UTC, in both text and binary format, with the same `tzinfo`.
- Intervals with months or years decode to `pgtern.Interval(months, delta)`, because a month has no fixed length. Other intervals decode to `timedelta`.
- A `time` value of `24:00:00` raises `DataError`, because Python has no such time.
- Dates and times follow the session's `DateStyle`, and text follows its `client_encoding`. Both are read from the server after every change. Non-ASCII text under a non-UTF-8 encoding raises `NotSupportedError` before anything is sent.

## Custom loaders and dumpers

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
