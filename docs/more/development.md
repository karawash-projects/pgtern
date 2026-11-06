# Development

```
pip install -e ".[dev]"
eval "$(scripts/start-test-postgres.sh)"    # throwaway server with SCRAM, MD5, cleartext, trust and TLS users
pytest
scripts/start-test-postgres.sh stop
```

The unit tests run without a server. The integration tests need `PGTERN_TEST_DSN`. Optional suites read `PGTERN_TEST_POOLER_DSN`, which points at a transaction-mode connection pooler, and `PGTERN_TEST_RESTART_CMD`, a command that restarts the test server.
