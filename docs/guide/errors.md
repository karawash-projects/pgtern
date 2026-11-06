# Errors

Every exception derives from `pgtern.Error`. Each common SQLSTATE has its own class, such as `UniqueViolation` or `DeadlockDetected`, and each belongs to a broader category, such as `IntegrityError`:

```python
with pgtern.connect(dsn) as conn:
    try:
        conn.execute("INSERT INTO accounts (id) VALUES (%s)", [1])
    except pgtern.IntegrityError:
        conn.rollback()  # a failed transaction must be rolled back before the block ends
```
