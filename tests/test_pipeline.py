# SPDX-License-Identifier: Apache-2.0
"""Pipeline mode: ordering, error handling, transaction behaviour, batching, guards."""
from __future__ import annotations

import uuid
from contextlib import contextmanager

import pytest

import pgtern
from pgtern import errors

pytestmark = pytest.mark.integration


@pytest.fixture
def tbl(dsn):
    name = "p_" + uuid.uuid4().hex[:8]
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"CREATE TABLE {name} (v int PRIMARY KEY, s text)")
        yield name
        c.execute(f"DROP TABLE {name}")


def _rows(handle):
    """Rows of a statement, or the name of the error it raised (an INSERT has no rows)."""
    try:
        return handle.fetchall()
    except Exception as exc:  # noqa: BLE001
        return type(exc).__name__


def test_results_arrive_in_order(dsn, tbl):
    statements = [("INSERT INTO %s VALUES (%%s, %%s)" % tbl, [i, f"s{i}"]) for i in range(20)]
    statements += [("SELECT v, s FROM %s WHERE v >= %%s ORDER BY v" % tbl, [15]), ("SELECT 'x'", None)]
    with pgtern.connect(dsn, autocommit=True) as m:
        with m.pipeline() as pipe:
            handles = [pipe.execute(q, p) for q, p in statements]
        ours = [_rows(h) for h in handles]
    assert ours[-2] == [(v, f"s{v}") for v in range(15, 20)]
    assert ours[-1] == [("x",)]
    assert ours[0] == "ProgrammingError"  # an INSERT has no result rows


def test_rowcount_and_description_after_sync(dsn, tbl):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            ins = pipe.execute(f"INSERT INTO {tbl} SELECT g, 'x' FROM generate_series(1, 7) g")
            sel = pipe.execute(f"SELECT v FROM {tbl}")
        assert ins.rowcount == 7
        assert [d.name for d in sel.description] == ["v"]
        assert len(sel.fetchall()) == 7


def test_cursor_is_not_readable_before_sync(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            h = pipe.execute("SELECT 1")
            with pytest.raises(errors.ProgrammingError, match="not synchronized"):
                h.fetchall()
        assert h.fetchall() == [(1,)]


def test_error_aborts_rest_of_batch_and_connection_survives_autocommit(dsn, tbl):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            ok = pipe.execute(f"INSERT INTO {tbl} VALUES (1, 'a')")
            bad = pipe.execute("SELECT 1/0")
            after = pipe.execute("SELECT 42")
            # autocommit: the batch is one implicit transaction, so the earlier insert rolls back too
        with pytest.raises(errors.DivisionByZero):
            bad.fetchall()
        with pytest.raises(errors.PipelineAborted):
            after.fetchall()
        assert ok.rowcount == 1  # the insert before the failure was run, and then rolled back with the batch
        assert c.execute(f"SELECT count(*) FROM {tbl}").fetchone() == (0,)
        assert c.execute("SELECT 7").fetchone() == (7,)


def test_sync_points_commit_in_autocommit(dsn, tbl):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            pipe.execute(f"INSERT INTO {tbl} VALUES (1, 'a')")
            pipe.sync()  # committed here
            pipe.execute(f"INSERT INTO {tbl} VALUES (2, 'b')")
            bad = pipe.execute(f"INSERT INTO {tbl} VALUES (1, 'dup')")
        with pytest.raises(errors.UniqueViolation):
            bad.fetchall()
        assert c.execute(f"SELECT array_agg(v ORDER BY v) FROM {tbl}").fetchone() == ([1],)


def test_non_autocommit_wraps_batch_in_one_transaction(dsn, tbl):
    with pgtern.connect(dsn) as c:
        with c.pipeline() as pipe:
            pipe.execute(f"INSERT INTO {tbl} VALUES (5, 'x')")
            pipe.execute(f"INSERT INTO {tbl} VALUES (6, 'y')")
            bad = pipe.execute("SELECT 1/0")
        with pytest.raises(errors.DivisionByZero):
            bad.fetchall()
        assert c.info.transaction_status.name == "INERROR"
        c.rollback()
        assert c.execute(f"SELECT count(*) FROM {tbl}").fetchone() == (0,)
    with pgtern.connect(dsn) as c:
        with c.pipeline() as pipe:
            pipe.execute(f"INSERT INTO {tbl} VALUES (5, 'x')")
        c.commit()
        assert c.execute(f"SELECT count(*) FROM {tbl}").fetchone() == (1,)


def test_large_pipeline_does_not_deadlock_on_big_results(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline(batch_size=16) as pipe:
            big = pipe.execute("SELECT repeat('x', 100000), g FROM generate_series(1, 200) g")
            handles = [pipe.execute("SELECT %s::int", [i]) for i in range(2000)]
        assert len(big.fetchall()) == 200
        values = [h.fetchall()[0][0] for h in handles]
        assert values == list(range(2000))


def test_batch_size_auto_sync(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline(batch_size=3) as pipe:
            hs = [pipe.execute("SELECT %s::int", [i]) for i in range(7)]
            assert hs[0].rowcount == 1  # the first batch of three was synchronized automatically
        assert [h.fetchall() for h in hs] == [[(i,)] for i in range(7)]


def test_guards_prevent_protocol_desync(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            pipe.execute("SELECT 1")
            with pytest.raises(errors.ProgrammingError, match="pipeline is active"):
                c.execute("SELECT 2")
            with pytest.raises(errors.ProgrammingError, match="pipeline is active"):
                c.transaction().__enter__()
            with pytest.raises(errors.ProgrammingError, match="already active"):
                c.pipeline().__enter__()
        assert c.execute("SELECT 3").fetchone() == (3,)
    with pgtern.connect(dsn) as c:
        with c.pipeline() as pipe:
            pipe.execute("SELECT 1")
            with pytest.raises(errors.ProgrammingError):
                c.commit()
            with pytest.raises(errors.ProgrammingError):
                c.rollback()


def test_pipeline_not_usable_after_block(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            pipe.execute("SELECT 1")
        with pytest.raises(errors.ProgrammingError, match="not active"):
            pipe.execute("SELECT 2")


def test_exception_inside_block_keeps_connection_usable(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with pytest.raises(RuntimeError):
            with c.pipeline() as pipe:
                pipe.execute("SELECT 1/0")
                raise RuntimeError("body failed")
        assert c.execute("SELECT 5").fetchone() == (5,)


def test_hooks_run_for_each_pipelined_statement(dsn):
    seen = []

    @contextmanager
    def hook(info):
        seen.append(info.statement)
        yield

    remove = pgtern.add_query_hook(hook)
    try:
        with pgtern.connect(dsn, autocommit=True) as c:
            with c.pipeline() as pipe:
                pipe.execute("SELECT %s::int", [424242])
                pipe.execute("SELECT 'two'")
    finally:
        remove()
    assert "SELECT %s::int" in seen and "SELECT 'two'" in seen
    assert not any("424242" in s for s in seen)


def test_pipeline_with_adapters_and_unknown_types(dsn):
    from pgtern import adapters
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            h = pipe.execute("SELECT ARRAY[1,2]::int[], now()::date IS NOT NULL")
        assert h.fetchall() == [([1, 2], True)]
    adapters._types._LOADERS.clear()


def test_pipeline_through_pooled_connections(dsn):
    with pgtern.ConnectionPool(dsn, min_size=1, max_size=2) as pool:
        with pool.connection() as c:
            with c.pipeline() as pipe:
                h = [pipe.execute("SELECT %s::int", [i]) for i in range(5)]
            assert [x.fetchall()[0][0] for x in h] == list(range(5))


# ---------------------------------------------------------------- asyncio
import asyncio  # noqa: E402

from pgtern import aio  # noqa: E402


def test_async_pipeline_order_errors_and_batches(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            async with c.pipeline(batch_size=4) as pipe:
                hs = [await pipe.execute("SELECT %s::int", [i]) for i in range(10)]
            assert [h.fetchall() for h in hs] == [[(i,)] for i in range(10)]

            async with c.pipeline() as pipe:
                ok = await pipe.execute("SELECT 1")
                bad = await pipe.execute("SELECT 1/0")
                after = await pipe.execute("SELECT 2")
            assert ok.fetchall() == [(1,)]
            with pytest.raises(errors.DivisionByZero):
                bad.fetchall()
            with pytest.raises(errors.PipelineAborted):
                after.fetchall()
            assert (await c.execute("SELECT 3")).fetchone() == (3,)

            async with c.pipeline() as pipe:
                await pipe.execute("SELECT 1")
                with pytest.raises(errors.ProgrammingError, match="pipeline is active"):
                    await c.execute("SELECT 9")
    asyncio.run(asyncio.wait_for(go(), 60))


def test_async_pipeline_large_and_matches_sync(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            async with c.pipeline(batch_size=32) as pipe:
                big = await pipe.execute("SELECT repeat('y', 50000), g FROM generate_series(1, 100) g")
                hs = [await pipe.execute("SELECT %s::int * 2", [i]) for i in range(1000)]
            assert len(big.fetchall()) == 100
            return [h.fetchall()[0][0] for h in hs]
    got = asyncio.run(asyncio.wait_for(go(), 60))
    assert got == [i * 2 for i in range(1000)]


def test_async_pipeline_cancellation_closes_connection(dsn):
    async def go():
        c = await aio.connect(dsn, autocommit=True)
        async def run():
            async with c.pipeline() as pipe:
                await pipe.execute("SELECT pg_sleep(30)")
        t = asyncio.create_task(run())
        await asyncio.sleep(0.3)
        t.cancel()
        with pytest.raises(asyncio.CancelledError):
            await t
        assert c.closed
    asyncio.run(asyncio.wait_for(go(), 30))
