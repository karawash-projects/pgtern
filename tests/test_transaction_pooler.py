# SPDX-License-Identifier: Apache-2.0
"""Behaviour behind a connection pooler in *transaction* pooling mode.

Run with PGTERN_TEST_POOLER_DSN pointing at a transaction-mode pooler with a small
default_pool_size, so many clients share very few server connections.
"""
from __future__ import annotations

import asyncio
import os
import threading
import uuid

import pytest

import pgtern
from pgtern import aio, errors

DSN = os.environ.get("PGTERN_TEST_POOLER_DSN")
pytestmark = pytest.mark.skipif(not DSN, reason="set PGTERN_TEST_POOLER_DSN (transaction-mode pooler)")


def test_repeated_statements_never_hit_prepared_statement_errors():
    with pgtern.connect(DSN) as c:
        for i in range(300):  # a client library could start server-side preparing after a few runs
            assert c.execute("SELECT %s::int + 1", [i]).fetchone() == (i + 1,)
            c.commit()


def test_many_clients_share_few_server_connections_without_cross_talk():
    errs: list[BaseException] = []
    pids: set[int] = set()

    def worker(n: int) -> None:
        try:
            with pgtern.connect(DSN) as c:
                for i in range(40):
                    with c.transaction():
                        c.execute("SELECT set_config('app.tenant_id', %s, true)", [f"t{n}"])
                        got = c.execute("SELECT current_setting('app.tenant_id', true), pg_backend_pid()").fetchone()
                        assert got[0] == f"t{n}", got  # the transaction-local setting never leaks between clients
                        pids.add(got[1])
                    # outside a transaction block the setting must be gone again
                    assert c.execute("SELECT coalesce(nullif(current_setting('app.tenant_id', true), ''), 'none')").fetchone() == ("none",)
                    c.commit()
        except BaseException as exc:  # noqa: BLE001
            errs.append(exc)

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(24)]
    [t.start() for t in threads]
    [t.join(120) for t in threads]
    assert not errs, errs[0]
    assert len(pids) <= 4  # 24 clients really were multiplexed over the 2-connection server pool


def test_pgtern_pool_over_pooler():
    with pgtern.ConnectionPool(DSN, min_size=2, max_size=10) as pool:
        def work(_):
            with pool.connection() as c:
                return c.execute("SELECT 1").fetchone()[0]
        results = []
        ts = [threading.Thread(target=lambda: results.append(work(0))) for _ in range(30)]
        [t.start() for t in ts]
        [t.join(60) for t in ts]
        assert results == [1] * 30


def test_data_types_and_transactions_through_pooler():
    name = "t_" + uuid.uuid4().hex[:10]
    with pgtern.connect(DSN, autocommit=True) as c:
        c.execute(f"CREATE TABLE {name} (id serial PRIMARY KEY, j jsonb, a int[], t timestamptz, b bytea, n numeric)")
        try:
            import datetime as dt
            from decimal import Decimal
            from pgtern.types import Jsonb
            now = dt.datetime(2026, 11, 1, 12, 30, tzinfo=dt.timezone.utc)
            with c.transaction():
                c.execute(f"INSERT INTO {name} (j, a, t, b, n) VALUES (%s, %s, %s, %s, %s)",
                          [Jsonb({"k": [1, 2]}), [1, 2, 3], now, b"\x00\xff", Decimal("1.25")])
                with pytest.raises(errors.CheckViolation):
                    with c.transaction():
                        c.execute(f"ALTER TABLE {name} ADD CONSTRAINT c CHECK (n < 0)")
            row = c.execute(f"SELECT j, a, t, b, n FROM {name}").fetchone()
            assert row == ({"k": [1, 2]}, [1, 2, 3], now, b"\x00\xff", Decimal("1.25"))
            with c.cursor() as cur:
                with cur.copy(f"COPY {name} (n) FROM STDIN") as cp:
                    cp.write_row((5,))
            assert c.execute(f"SELECT count(*) FROM {name}").fetchone() == (2,)
        finally:
            c.execute(f"DROP TABLE {name}")


def test_server_cursor_inside_transaction_through_pooler():
    with pgtern.connect(DSN) as c:
        cur = c.cursor("pooler_cur")
        cur.execute("SELECT g FROM generate_series(1, 500) g")
        assert sum(r[0] for r in cur) == 500 * 501 // 2
        cur.close()


def test_async_over_pooler():
    async def go():
        async with aio.AsyncConnectionPool(DSN, min_size=1, max_size=12) as pool:
            async def one(i):
                async with pool.connection() as c:
                    async with c.transaction():
                        return (await c.execute("SELECT %s::int", [i])).fetchone()[0]
            assert await asyncio.gather(*(one(i) for i in range(60))) == list(range(60))
    asyncio.run(asyncio.wait_for(go(), 60))
