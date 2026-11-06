# Binary format

Opt in per statement with `binary=True`:

```python
with pgtern.connect(dsn) as conn:
    value = conn.execute("SELECT %s::timestamptz", [some_datetime], binary=True).fetchone()[0]
```

Binary transfer covers integers, floats, booleans, text, `bytea`, `uuid`, dates, times, intervals, `json` and `jsonb`, `numeric` results, and one-dimensional arrays of these. Other types use text.
