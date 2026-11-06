# Pipelines

Pipeline mode sends many statements before reading any reply:

```python
with pgtern.connect(dsn) as conn:
    with conn.pipeline() as pipe:
        a = pipe.execute("INSERT INTO queue (job) VALUES (%s)", ["a"])
        b = pipe.execute("SELECT count(*) FROM queue")
    print(b.fetchone())    # results are ready once the block ends
```

Replies are read in order at each sync point: `pipe.sync()`, the end of the block, or every `batch_size` statements. A sync point raises the first failure in its batch, so a failed statement cannot pass silently.

Under autocommit, a sync point is also a commit point, so the statements in one batch succeed or fail together. If the block exits with an exception under autocommit, the batch is rolled back.

Statements after a failure raise `pgtern.errors.PipelineAborted`. Pipelines support text format only, and `COPY` cannot run inside one.
