# Queries and transactions

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

Placeholders are `%s`. Values are sent as parameters and are never interpolated into the SQL text.

## Building SQL safely

Use the `sql` module to compose identifiers and literals:

```python
from pgtern import sql

with pgtern.connect(dsn) as conn:
    query = sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier("accounts"))
    conn.execute(query, [1])
```
