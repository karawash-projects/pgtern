# Bulk loading

## executemany

`executemany` sends the whole batch as one pipelined transaction. In autocommit mode, a failing row rolls back the entire batch. Inside a transaction, it aborts that transaction as usual.

```python
with pgtern.connect(dsn, autocommit=True) as conn:
    conn.cursor().executemany("INSERT INTO events (kind, payload) VALUES (%s, %s)", rows)
```

## COPY

For the largest loads, use `COPY`:

```python
with pgtern.connect(dsn) as conn:
    with conn.cursor().copy("COPY events (kind, payload) FROM STDIN") as copy:
        for kind, payload in rows:
            copy.write_row((kind, payload))
```

Export uses `COPY ... TO STDOUT`, read with `for chunk in copy` or `copy.rows()`. `COPY` cannot run inside a [pipeline](pipelines.md) or an `executemany` batch.
