# SPDX-License-Identifier: Apache-2.0
"""The asyncio API: same behaviour as the blocking one."""
from __future__ import annotations

import asyncio
import time
import uuid

import pytest

import pgtern
from pgtern import aio, errors
from pgtern.rows import dict_row

from conftest import CA, HOST, PORT, SOCKET_DIR, url

pytestmark = pytest.mark.integration


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, 60))


def test_basic_query_types_and_params(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as conn:
            cur = await conn.execute("SELECT %s::int, %s::text, %s::bool, %s::numeric, %s::int[]", [1, "é", True, "1.50", [1, 2]])
            assert (await_none(cur.fetchone())) == (1, "é", True, __import__("decimal").Decimal("1.50"), [1, 2])
            cur = await conn.execute("SELECT 1 AS a UNION ALL SELECT 2")
            assert cur.fetchall() == [(1,), (2,)]
            assert [c.name for c in cur.description] == ["a"]
    run(go())


def await_none(x):
    return x


def test_dict_rows_and_async_iteration(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
            cur = await conn.execute("SELECT g FROM generate_series(1,3) g")
            assert [r["g"] async for r in cur] == [1, 2, 3]
    run(go())


def test_transaction_commit_rollback_and_savepoint(dsn):
    name = "t_" + uuid.uuid4().hex[:10]

    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            await c.execute(f"CREATE TABLE {name} (v int)")
            try:
                async with c.transaction():
                    await c.execute(f"INSERT INTO {name} VALUES (1)")
                    with pytest.raises(errors.DivisionByZero):
                        async with c.transaction():
                            await c.execute(f"INSERT INTO {name} VALUES (2)")
                            await c.execute("SELECT 1/0")
                    await c.execute(f"INSERT INTO {name} VALUES (3)")
                assert (await c.execute(f"SELECT array_agg(v ORDER BY v) FROM {name}")).fetchone() == ([1, 3],)
                with pytest.raises(RuntimeError):
                    async with c.transaction():
                        await c.execute(f"INSERT INTO {name} VALUES (9)")
                        raise RuntimeError
                assert (await c.execute(f"SELECT count(*) FROM {name}")).fetchone() == (2,)
            finally:
                await c.execute(f"DROP TABLE {name}")
    run(go())


def test_implicit_transaction_commit_on_exit_and_rollback_on_error(dsn):
    name = "t_" + uuid.uuid4().hex[:10]

    async def go():
        async with await aio.connect(dsn, autocommit=True) as admin:
            await admin.execute(f"CREATE TABLE {name} (v int)")
            try:
                async with await aio.connect(dsn) as c:
                    await c.execute(f"INSERT INTO {name} VALUES (1)")
                with pytest.raises(RuntimeError):
                    async with await aio.connect(dsn) as c:
                        await c.execute(f"INSERT INTO {name} VALUES (2)")
                        raise RuntimeError
                assert (await admin.execute(f"SELECT array_agg(v) FROM {name}")).fetchone() == ([1],)
            finally:
                await admin.execute(f"DROP TABLE {name}")
    run(go())


def test_server_error_classes_and_connection_survives(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            with pytest.raises(errors.UndefinedTable) as ei:
                await c.execute("SELECT * FROM nope_nope")
            assert ei.value.sqlstate == "42P01"
            assert (await c.execute("SELECT 1")).fetchone() == (1,)
    run(go())


@pytest.mark.parametrize("user,pw", [("md5user", "md5pw"), ("clearuser", "clearpw"), ("postgres", "postgres")])
def test_auth_methods(user, pw):
    async def go():
        async with await aio.connect(url(user, pw)) as c:
            assert (await c.execute("SELECT current_user")).fetchone() == (user,)
    run(go())


def test_trust_and_wrong_password():
    async def go():
        async with await aio.connect(url("trustuser")) as c:
            assert (await c.execute("SELECT 1")).fetchone() == (1,)
        with pytest.raises(errors.InvalidPassword):
            await aio.connect(url("postgres", "wrong"))
    run(go())


def test_tls_modes():
    if not CA:
        pytest.skip("no CA")
    with pgtern.connect(url("postgres", "postgres", sslmode="disable")) as c:
        if c.execute("SHOW ssl").fetchone()[0] != "on":
            pytest.skip("the test server has no TLS")

    async def go():
        async with await aio.connect(url("tlsuser", "tlspw", sslmode="require")) as c:
            assert (await c.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")).fetchone() == (True,)
        async with await aio.connect(url("tlsuser", "tlspw", sslmode="verify-full", sslrootcert=CA).replace(HOST, "localhost")) as c:
            assert (await c.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()")).fetchone() == (True,)
        with pytest.raises(errors.OperationalError):
            await aio.connect(url("tlsuser", "tlspw", sslmode="disable"))
    run(go())


def test_unix_socket():
    if not SOCKET_DIR:
        pytest.skip("no socket dir")

    async def go():
        async with await aio.connect(host=SOCKET_DIR, port=PORT, user="postgres", dbname="postgres", password="postgres") as c:
            assert (await c.execute("SELECT 1")).fetchone() == (1,)
    run(go())


def test_multi_host_failover(dsn):
    async def go():
        async with await aio.connect(f"postgresql://postgres:postgres@127.0.0.1:1,{HOST}:{PORT}/postgres") as c:
            assert c.info.port == int(PORT)
    run(go())


def test_connect_failure_is_operational_error():
    async def go():
        with pytest.raises(errors.OperationalError):
            await aio.connect("postgresql://x@127.0.0.1:1/x", connect_timeout=2)
    run(go())


def test_cancellation_closes_connection_and_does_not_hang(dsn):
    async def go():
        c = await aio.connect(dsn, autocommit=True)
        t = asyncio.create_task(c.execute("SELECT pg_sleep(30)"))
        await asyncio.sleep(0.3)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert c.closed
        async with await aio.connect(dsn, autocommit=True) as other:  # nothing left running
            await asyncio.sleep(0.2)
            # the server may still be sleeping until it notices the closed socket; cancel it explicitly
            await other.execute("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(30)%' AND pid <> pg_backend_pid()")
    run(go())


def test_timeout_wrapper(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            t = time.monotonic()
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(c.execute("SELECT pg_sleep(10)"), 0.3)
            assert time.monotonic() - t < 3
            assert c.closed
    run(go())


def test_concurrent_use_of_one_connection_is_serialized(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            async def one(i):
                cur = await c.execute("SELECT %s::int, pg_sleep(0.01)", [i])
                return cur.fetchone()[0]
            assert await asyncio.gather(*(one(i) for i in range(30))) == list(range(30))
    run(go())


def test_many_connections_in_parallel(dsn):
    async def one(i):
        async with await aio.connect(dsn) as c:
            return (await c.execute("SELECT %s::int", [i])).fetchone()[0]

    async def go():
        assert await asyncio.gather(*(one(i) for i in range(40))) == list(range(40))
    run(go())


def test_notify(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as l, await aio.connect(dsn, autocommit=True) as s:
            await l.execute("LISTEN achan")

            async def send():
                await asyncio.sleep(0.2)
                await s.execute("SELECT pg_notify('achan', 'one')")
                await s.execute("SELECT pg_notify('achan', 'two')")

            t = asyncio.create_task(send())
            got = [n.payload async for n in l.notifies(timeout=3, stop_after=2)]
            await t
            assert got == ["one", "two"]
            assert [n async for n in l.notifies(timeout=0.2)] == []
            assert not l.closed
            assert (await l.execute("SELECT 1")).fetchone() == (1,)
    run(go())


def test_copy_in_out(dsn):
    name = "t_" + uuid.uuid4().hex[:10]

    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            await c.execute(f"CREATE TABLE {name} (a int, b text)")
            try:
                cur = c.cursor()
                async with cur.copy(f"COPY {name} FROM STDIN") as copy:
                    for i in range(1000):
                        await copy.write_row((i, f"v\t{i}"))
                assert cur.rowcount == 1000
                async with cur.copy(f"COPY {name} TO STDOUT") as copy:
                    rows = [r async for r in copy.rows()]
                assert len(rows) == 1000 and rows[5] == ("5", "v\t5")
                with pytest.raises(RuntimeError):
                    async with cur.copy(f"COPY {name} FROM STDIN") as copy:
                        await copy.write_row((1, "x"))
                        raise RuntimeError
                assert (await c.execute(f"SELECT count(*) FROM {name}")).fetchone() == (1000,)
                with pytest.raises(errors.DataError):
                    async with cur.copy(f"COPY {name} FROM STDIN") as copy:
                        await copy.write_row(("not-an-int", "x"))
                assert (await c.execute("SELECT 1")).fetchone() == (1,)
            finally:
                await c.execute(f"DROP TABLE {name}")
    run(go())


def test_server_side_cursor(dsn):
    async def go():
        async with await aio.connect(dsn) as c:
            cur = c.cursor("acur")
            cur.itersize = 10
            await cur.execute("SELECT g FROM generate_series(1, 35) g")
            assert (await cur.fetchone()) == (1,)
            assert len(await cur.fetchmany(4)) == 4
            rest = [r[0] async for r in cur]
            assert rest == list(range(6, 36))
            await cur.close()
    run(go())


def test_hooks_run_for_async_queries(dsn):
    from contextlib import contextmanager
    seen = []

    @contextmanager
    def hook(info):
        seen.append(info.statement)
        yield

    async def go():
        remove = pgtern.add_query_hook(hook)
        try:
            async with await aio.connect(dsn, autocommit=True) as c:
                await c.execute("SELECT %s", ["secret"])
        finally:
            remove()
    run(go())
    assert "SELECT %s" in seen and not any("secret" in s for s in seen)


# ---------------------------------------------------------------- pool
def test_pool_basic_and_limits(dsn):
    async def go():
        async with aio.AsyncConnectionPool(dsn, min_size=1, max_size=3, timeout=1) as pool:
            async def work(i):
                async with pool.connection() as c:
                    return (await c.execute("SELECT %s::int, pg_sleep(0.05)", [i])).fetchone()[0]
            assert await asyncio.gather(*(work(i) for i in range(12))) == list(range(12))
            assert len(pool._idle) <= 3
            held = [await pool.getconn() for _ in range(3)]
            with pytest.raises(pgtern.PoolTimeout):
                await pool.getconn(timeout=0.2)
            for c in held:
                await pool.putconn(c)
    run(go())


def test_pool_rolls_back_on_error_and_replaces_dead(dsn):
    name = "t_" + uuid.uuid4().hex[:10]

    async def go():
        async with await aio.connect(dsn, autocommit=True) as admin:
            await admin.execute(f"CREATE TABLE {name} (v int)")
            try:
                async with aio.AsyncConnectionPool(dsn, min_size=1, max_size=1) as pool:
                    with pytest.raises(RuntimeError):
                        async with pool.connection() as c:
                            await c.execute(f"INSERT INTO {name} VALUES (1)")
                            raise RuntimeError
                    async with pool.connection() as c:
                        assert (await c.execute(f"SELECT count(*) FROM {name}")).fetchone() == (0,)
                        pid = c.info.backend_pid
                    await admin.execute("SELECT pg_terminate_backend(%s)", [pid])
                    await asyncio.sleep(0.3)
                    async with pool.connection() as c:
                        assert c.info.backend_pid != pid
                        assert (await c.execute("SELECT 1")).fetchone() == (1,)
                    # a cancelled task must not leak a slot
                    async def slow():
                        async with pool.connection() as c:
                            await c.execute("SELECT pg_sleep(30)")
                    t = asyncio.create_task(slow())
                    await asyncio.sleep(0.3)
                    t.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await t
                    async with pool.connection(timeout=5) as c:
                        assert (await c.execute("SELECT 1")).fetchone() == (1,)
                    await admin.execute("SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE query LIKE 'SELECT pg_sleep(30)%' AND pid <> pg_backend_pid()")
            finally:
                await admin.execute(f"DROP TABLE {name}")
    run(go())


def test_pool_closed():
    async def go():
        pool = aio.AsyncConnectionPool("postgresql://x@127.0.0.1:1/x", min_size=0, max_size=1)
        with pytest.raises(pgtern.PoolClosed):
            await pool.getconn()
    run(go())
