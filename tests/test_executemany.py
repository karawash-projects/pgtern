# SPDX-License-Identifier: Apache-2.0
"""executemany runs as one pipelined batch in one transaction."""
import pytest

import pgtern
from pgtern import errors, hooks

pytestmark = pytest.mark.integration


@pytest.fixture
def conn(dsn):
    c = pgtern.connect(dsn, autocommit=True)
    c.execute("DROP TABLE IF EXISTS em_t")
    c.execute("CREATE TABLE em_t (id int PRIMARY KEY, v text)")
    yield c
    c.execute("DROP TABLE IF EXISTS em_t")
    c.close()


def count(c):
    return c.execute("SELECT count(*) FROM em_t").fetchone()[0]


def test_rowcount_is_total(conn):
    cur = conn.cursor()
    cur.executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(10)])
    assert cur.rowcount == 10
    assert count(conn) == 10


def test_large_batch_larger_than_window(conn):
    # 5000 statements: far more than the reply window, so replies are read while sending
    cur = conn.cursor()
    cur.executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "v" * 20) for i in range(5000)])
    assert cur.rowcount == 5000
    assert count(conn) == 5000


def test_autocommit_failure_rolls_back_whole_batch(conn):
    # one Sync at the end, so the batch is all or nothing
    rows = [(i, "x") for i in range(200)] + [(5, "dup")] + [(i, "y") for i in range(300, 400)]
    with pytest.raises(errors.UniqueViolation):
        conn.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", rows)
    assert count(conn) == 0


def test_connection_usable_after_failed_batch(conn):
    with pytest.raises(errors.UniqueViolation):
        conn.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(1, "a"), (1, "b")])
    assert conn.execute("SELECT 1").fetchone() == (1,)
    assert conn.info.transaction_status.name == "IDLE"


def test_bad_parameter_mid_batch_keeps_connection_usable(conn):
    rows = [(i, "x") for i in range(100)] + [(object(), "bad")]
    with pytest.raises(Exception):
        conn.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", rows)
    assert count(conn) == 0
    assert conn.execute("SELECT 1").fetchone() == (1,)


def test_transaction_mode_batch_commits_with_transaction(dsn):
    c = pgtern.connect(dsn)  # autocommit off: one transaction
    try:
        c.execute("DROP TABLE IF EXISTS em_t")
        c.execute("CREATE TABLE em_t (id int PRIMARY KEY, v text)")
        c.commit()
        c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(300)])
        c.rollback()
        assert count(c) == 0
        c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(300)])
        c.commit()
        assert count(c) == 300
    finally:
        c.rollback()
        c.execute("DROP TABLE IF EXISTS em_t")
        c.close()


def test_hooks_see_one_call_per_statement(conn):
    seen = []

    class Ctx:
        def __init__(self, info):
            self.info = info
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            seen.append((self.info.statement, self.info.rowcount, exc[0]))

    remove = hooks.add_query_hook(Ctx)
    try:
        conn.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(3)])
    finally:
        remove()
    assert len(seen) == 3
    assert all(stmt.startswith("INSERT") and rc == 1 and exc is None for stmt, rc, exc in seen)


def test_empty_batch(conn):
    cur = conn.cursor()
    cur.executemany("INSERT INTO em_t VALUES (%s, %s)", [])
    assert cur.rowcount == 0
    assert count(conn) == 0
