# SPDX-License-Identifier: Apache-2.0
"""asyncio executemany: same batch semantics as the blocking API (see test_executemany.py)."""
import asyncio

import pytest

from pgtern import aio, errors, hooks

pytestmark = pytest.mark.integration


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 60))


async def count(c):
    cur = await c.execute("SELECT count(*) FROM em_t")
    return cur.fetchone()[0]


async def setup(c):
    await c.execute("DROP TABLE IF EXISTS em_t")
    await c.execute("CREATE TABLE em_t (id int PRIMARY KEY, v text)")


def test_rowcount_and_large_batch(dsn):
    async def go():
        c = await aio.connect(dsn, autocommit=True)
        try:
            await setup(c)
            cur = c.cursor()
            await cur.executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "v" * 20) for i in range(5000)])
            assert cur.rowcount == 5000
            assert await count(c) == 5000
        finally:
            await c.execute("DROP TABLE IF EXISTS em_t")
            await c.close()
    run(go())


def test_autocommit_failure_rolls_back_whole_batch(dsn):
    async def go():
        c = await aio.connect(dsn, autocommit=True)
        try:
            await setup(c)
            rows = [(i, "x") for i in range(200)] + [(5, "dup")] + [(i, "y") for i in range(300, 400)]
            with pytest.raises(errors.UniqueViolation):
                await c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", rows)
            assert await count(c) == 0
            cur = await c.execute("SELECT 1")
            assert cur.fetchone() == (1,)
        finally:
            await c.execute("DROP TABLE IF EXISTS em_t")
            await c.close()
    run(go())


def test_bad_parameter_mid_batch(dsn):
    async def go():
        c = await aio.connect(dsn, autocommit=True)
        try:
            await setup(c)
            rows = [(i, "x") for i in range(100)] + [(object(), "bad")]
            with pytest.raises(Exception):
                await c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", rows)
            assert await count(c) == 0
            cur = await c.execute("SELECT 1")
            assert cur.fetchone() == (1,)
        finally:
            await c.execute("DROP TABLE IF EXISTS em_t")
            await c.close()
    run(go())


def test_transaction_mode_batch(dsn):
    async def go():
        c = await aio.connect(dsn)
        try:
            await setup(c)
            await c.commit()
            await c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(300)])
            await c.rollback()
            assert await count(c) == 0
            await c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(300)])
            await c.commit()
            assert await count(c) == 300
        finally:
            await c.rollback()
            await c.execute("DROP TABLE IF EXISTS em_t")
            await c.close()
    run(go())


def test_hooks_one_call_per_statement(dsn):
    seen = []

    class Ctx:
        def __init__(self, info):
            self.info = info
        def __enter__(self):
            return self
        def __exit__(self, *exc):
            seen.append((self.info.statement, self.info.rowcount, exc[0]))

    async def go():
        c = await aio.connect(dsn, autocommit=True)
        try:
            await setup(c)
            await c.cursor().executemany("INSERT INTO em_t VALUES (%s, %s)", [(i, "x") for i in range(3)])
        finally:
            await c.execute("DROP TABLE IF EXISTS em_t")
            await c.close()

    remove = hooks.add_query_hook(Ctx)
    try:
        run(go())
    finally:
        remove()
    inserts = [s for s in seen if s[0].startswith("INSERT")]
    assert len(inserts) == 3
    assert all(rc == 1 and exc is None for _, rc, exc in inserts)


def test_empty_batch(dsn):
    async def go():
        c = await aio.connect(dsn, autocommit=True)
        try:
            await setup(c)
            cur = c.cursor()
            await cur.executemany("INSERT INTO em_t VALUES (%s, %s)", [])
            assert cur.rowcount == 0
        finally:
            await c.execute("DROP TABLE IF EXISTS em_t")
            await c.close()
    run(go())
