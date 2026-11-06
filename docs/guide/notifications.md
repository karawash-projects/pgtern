# Notifications

`LISTEN` only takes effect in autocommit mode, so connect with `autocommit=True`:

```python
with pgtern.connect(dsn, autocommit=True) as conn:
    conn.execute("LISTEN orders")
    for note in conn.notifies(timeout=30):
        print(note.channel, note.payload)
```

The async connection provides the same notifications through `async for`.

`LISTEN` needs a session that stays on one server connection, so use a direct connection or session-mode pooling for it.
