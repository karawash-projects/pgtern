# SPDX-License-Identifier: Apache-2.0
"""A server restart in the middle of a session: nothing hangs, pools recover.

Needs PGTERN_TEST_RESTART_CMD, a shell command that restarts the server under PGTERN_TEST_DSN and returns
once it accepts connections again.
"""
from __future__ import annotations

import asyncio
import os
import subprocess
import time

import pytest

import pgtern
from pgtern import aio, errors

CMD = os.environ.get("PGTERN_TEST_RESTART_CMD")
DSN = os.environ.get("PGTERN_TEST_DSN")
pytestmark = pytest.mark.skipif(not (CMD and DSN), reason="set PGTERN_TEST_RESTART_CMD and PGTERN_TEST_DSN")


def restart():
    subprocess.run(CMD, shell=True, check=True, timeout=120, capture_output=True)


def test_connection_fails_cleanly_then_new_one_works():
    c = pgtern.connect(DSN)
    assert c.execute("SELECT 1").fetchone() == (1,)
    restart()
    t = time.monotonic()
    with pytest.raises(errors.OperationalError):
        c.execute("SELECT 1")
    assert time.monotonic() - t < 10 and c.closed
    with pgtern.connect(DSN) as c2:
        assert c2.execute("SELECT 1").fetchone() == (1,)


def test_sync_pool_recovers():
    with pgtern.ConnectionPool(DSN, min_size=3, max_size=3, timeout=10) as pool:
        with pool.connection() as c:
            assert c.execute("SELECT 1").fetchone() == (1,)
        restart()
        for _ in range(6):  # every pooled connection is dead; the pool must replace them, not hand them out
            with pool.connection() as c:
                assert c.execute("SELECT 1").fetchone() == (1,)


def test_async_pool_recovers_and_listener_notices():
    async def go():
        async with aio.AsyncConnectionPool(DSN, min_size=3, max_size=3, timeout=10) as pool:
            async with pool.connection() as c:
                assert (await c.execute("SELECT 1")).fetchone() == (1,)
            listener = await aio.connect(DSN, autocommit=True)
            await listener.execute("LISTEN x")
            await asyncio.get_running_loop().run_in_executor(None, restart)
            for _ in range(6):
                async with pool.connection() as c:
                    assert (await c.execute("SELECT 1")).fetchone() == (1,)
            with pytest.raises(errors.OperationalError):
                async for _ in listener.notifies(timeout=5):
                    pass
            assert listener.closed
    asyncio.run(asyncio.wait_for(go(), 120))


def test_blocked_query_is_interrupted_by_restart():
    c = pgtern.connect(DSN)
    import threading
    out = []

    def run():
        try:
            c.execute("SELECT pg_sleep(60)")
            out.append(None)
        except BaseException as exc:  # noqa: BLE001
            out.append(exc)

    t = threading.Thread(target=run)
    t.start()
    time.sleep(0.5)
    restart()
    t.join(30)
    assert not t.is_alive() and isinstance(out[0], errors.OperationalError)
