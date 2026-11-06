# SPDX-License-Identifier: Apache-2.0
"""Session settings that change after connect: client_encoding, DateStyle, reentrant use."""
from __future__ import annotations

import datetime as dt

import pytest

import pgtern
from pgtern import errors

pytestmark = pytest.mark.integration


def test_client_encoding_change_is_followed_when_reading(conn):
    # chr(233) is 'é'; the server sends it in the session encoding, which the driver must follow.
    conn.execute("SET client_encoding TO 'LATIN1'")
    try:
        assert conn.execute("SELECT chr(233)").fetchone()[0] == "é"
    finally:
        conn.execute("SET client_encoding TO 'UTF8'")
    assert conn.execute("SELECT chr(233)").fetchone()[0] == "é"


def test_non_ascii_sql_is_refused_under_a_non_utf8_encoding(conn):
    conn.execute("SET client_encoding TO 'LATIN1'")
    try:
        with pytest.raises(errors.NotSupportedError):
            conn.execute("SELECT 'é'")
        # refused before anything was sent: the connection is still usable, in the same session
        assert conn.execute("SELECT 1").fetchone()[0] == 1
    finally:
        conn.execute("SET client_encoding TO 'UTF8'")
    assert conn.execute("SELECT 'é'").fetchone()[0] == "é"


def test_non_ascii_parameter_is_refused_under_a_non_utf8_encoding(conn):
    conn.execute("SET client_encoding TO 'LATIN1'")
    try:
        with pytest.raises(errors.NotSupportedError):
            conn.execute("SELECT %s", ("é",))
        assert conn.execute("SELECT 2").fetchone()[0] == 2
    finally:
        conn.execute("SET client_encoding TO 'UTF8'")


def test_copy_text_follows_the_session_encoding(conn):
    conn.execute("SET client_encoding TO 'LATIN1'")
    try:
        with conn.cursor().copy("COPY (SELECT chr(233) AS c) TO STDOUT") as copy:
            rows = list(copy.rows())
        assert rows == [("é",)]
    finally:
        conn.execute("SET client_encoding TO 'UTF8'")


def test_datestyle_change_is_followed_when_reading(conn):
    conn.execute("SET DateStyle TO 'SQL, DMY'")
    try:
        with pytest.raises(errors.NotSupportedError, match="DateStyle"):
            conn.execute("SELECT '2024-01-02'::date").fetchone()
        # the connection is still synchronized after the refused decode
        assert conn.execute("SELECT 3").fetchone()[0] == 3
    finally:
        conn.execute("SET DateStyle TO 'ISO, MDY'")
    assert conn.execute("SELECT '2024-01-02'::date").fetchone()[0] == dt.date(2024, 1, 2)


def test_reentrant_query_inside_copy_raises_clearly(conn):
    conn.execute("CREATE TEMP TABLE reentrant_copy (n int)")
    with conn.cursor().copy("COPY reentrant_copy FROM STDIN") as copy:
        with pytest.raises(errors.ProgrammingError, match="reentrant|COPY"):
            conn.execute("SELECT 1")
        copy.write_row((7,))
    # the COPY completed normally and the connection is still usable
    assert conn.execute("SELECT n FROM reentrant_copy").fetchone()[0] == 7


def test_reentrant_query_from_notify_handler_raises_clearly(conn):
    seen: list[str] = []

    def handler(note):
        try:
            conn.execute("SELECT 1")
            seen.append("ran")
        except errors.ProgrammingError as exc:
            seen.append(str(exc))

    conn.add_notify_handler(handler)
    conn.execute("LISTEN session_state_reentry")
    conn.commit()
    conn.execute("NOTIFY session_state_reentry, 'go'")
    conn.commit()  # notifications are delivered when the transaction ends
    assert seen == [seen[0]] and "COPY block or a notice/notify handler" in seen[0], seen
    assert conn.execute("SELECT 4").fetchone()[0] == 4


def _run(coro):
    import asyncio
    return asyncio.run(asyncio.wait_for(coro, 60))


def test_async_reentrant_query_inside_copy_raises_without_deadlock(dsn):
    from pgtern import aio

    async def main():
        async with await aio.connect(dsn) as conn:
            cur = conn.cursor()
            await cur.execute("CREATE TEMP TABLE reentrant_async (n int)")
            async with cur.copy("COPY reentrant_async FROM STDIN") as copy:
                with pytest.raises(errors.ProgrammingError, match="COPY block"):
                    await conn.execute("SELECT 1")
                await copy.write_row((9,))
            await cur.execute("SELECT n FROM reentrant_async")
            return cur.fetchone()[0]

    assert _run(main()) == 9


def test_async_transaction_inside_copy_raises(dsn):
    from pgtern import aio

    async def main():
        async with await aio.connect(dsn) as conn:
            cur = conn.cursor()
            await cur.execute("CREATE TEMP TABLE reentrant_tx (n int)")
            async with cur.copy("COPY reentrant_tx FROM STDIN") as copy:
                with pytest.raises(errors.ProgrammingError, match="COPY block"):
                    async with conn.transaction():
                        pass
                await copy.write_row((1,))

    _run(main())


def test_query_hook_that_fails_on_exit_does_not_break_the_query(conn, caplog):
    import contextlib

    @contextlib.contextmanager
    def failing_on_exit(info):
        yield
        raise RuntimeError("hook exit failed")

    remove = pgtern.add_query_hook(lambda info: failing_on_exit(info))
    try:
        with caplog.at_level("ERROR", logger="pgtern.hooks"):
            conn.execute("CREATE TEMP TABLE hook_exit (n int)")
            conn.cursor().executemany("INSERT INTO hook_exit VALUES (%s)", [(1,), (2,), (3,)])
            conn.execute("DROP TABLE hook_exit")
    finally:
        remove()
    assert "failed on exit" in caplog.text
    assert conn.execute("SELECT 5").fetchone()[0] == 5


def test_prefer_standby_accepts_a_primary_when_there_is_no_standby(dsn):
    with pgtern.connect(dsn, target_session_attrs="prefer-standby") as c:
        assert c.execute("SELECT pg_is_in_recovery()").fetchone()[0] is False


def test_standby_requirement_refuses_a_primary(dsn):
    with pytest.raises(errors.OperationalError):
        pgtern.connect(dsn, target_session_attrs="standby")
