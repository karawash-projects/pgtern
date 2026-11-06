# SPDX-License-Identifier: Apache-2.0
"""Async pool: a failed open leaves nothing behind, and a forked child can use the pool."""
from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest

import pgtern
from pgtern import aio, errors
from conftest import url

pytestmark = pytest.mark.integration


def _fork_and_run(child_body) -> int:
    pid = os.fork()
    if pid == 0:
        import signal
        signal.alarm(15)  # a child stuck on a socket must die, not outlive the test
        code = 1
        try:
            code = 0 if child_body() else 1
        except BaseException:  # noqa: BLE001 - the parent sees the exit code
            code = 2
        finally:
            os._exit(code)
    _, status = os.waitpid(pid, 0)
    return os.waitstatus_to_exitcode(status)


def test_failed_open_closes_the_connections_it_made(dsn):
    # A role limited to 2 connections: a pool asking for 3 opens two, then fails on the third.
    # Those two must be closed, not left behind.
    role = f"pgtern_limit_{uuid.uuid4().hex[:10]}"
    with pgtern.connect(dsn, autocommit=True) as admin:
        admin.execute(f"CREATE ROLE {role} LOGIN PASSWORD 'pw' CONNECTION LIMIT 2")
    pool = aio.AsyncConnectionPool(url(role, "pw"), min_size=3, max_size=3, timeout=10)
    try:
        with pytest.raises(errors.Error):
            asyncio.run(asyncio.wait_for(pool.open(), 30))
        assert pool._closed and not pool._idle and not pool._out
        # sessions end once their sockets close; give the server a moment, then count
        remaining = None
        for _ in range(40):
            with pgtern.connect(dsn, autocommit=True) as admin:
                remaining = admin.execute(
                    "SELECT count(*) FROM pg_stat_activity WHERE usename = %s", (role,)).fetchone()[0]
            if remaining == 0:
                break
            time.sleep(0.05)
        assert remaining == 0, f"{remaining} sessions left behind by the failed open"
    finally:
        with pgtern.connect(dsn, autocommit=True) as admin:
            admin.execute(f"DROP ROLE IF EXISTS {role}")


@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_async_pool_is_usable_in_a_forked_child(dsn):
    pool = aio.AsyncConnectionPool(dsn, min_size=1, max_size=2)

    async def parent_use() -> None:
        # the parent creates a connection bound to its own event loop, then returns it to the pool
        await pool.open(wait=False)
        conn = await pool.getconn()
        await pool.putconn(conn)

    asyncio.run(parent_use())
    parent_pid = os.getpid()

    def child_body() -> bool:
        async def child() -> bool:
            conn = await pool.getconn()
            try:
                row = (await conn.execute("SELECT 7")).fetchone()
            finally:
                await pool.putconn(conn)
            await pool.close()
            return row == (7,) and os.getpid() != parent_pid and pool._pid == os.getpid()

        return asyncio.run(child())

    try:
        assert _fork_and_run(child_body) == 0
    finally:
        # the parent's connection belongs to a loop that has already closed; closing it can fail harmlessly
        try:
            asyncio.run(pool.close())
        except Exception:  # noqa: BLE001
            pass
