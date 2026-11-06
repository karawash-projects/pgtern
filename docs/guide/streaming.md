# Streaming large results

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
