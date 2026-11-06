# SPDX-License-Identifier: Apache-2.0
"""Regression tests for the blockers found in external review (pgtern 0.3.0).

Each test reproduces one reported failure. Tests marked ``integration`` need a server;
the others use a local fake server only.
"""
from __future__ import annotations

import asyncio
import shutil
import socket
import ssl
import struct
import subprocess
import threading
import time
import uuid

import pytest

import pgtern
from pgtern import aio, errors
from pgtern.rows import dict_row


# ============================================================ 1. TLS: bytes after 'S' must not be trusted

def _msg(tag: bytes, body: bytes) -> bytes:
    return tag + struct.pack("!I", len(body) + 4) + body


@pytest.fixture(scope="module")
def tls_cert(tmp_path_factory):
    if shutil.which("openssl") is None:
        pytest.skip("needs the openssl command to make a throwaway certificate")
    d = tmp_path_factory.mktemp("cert")
    crt, key = str(d / "crt.pem"), str(d / "key.pem")
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", key,
                    "-out", crt, "-days", "1", "-subj", "/CN=localhost"],
                   check=True, capture_output=True)
    return crt, key


def _mitm_server(cert: tuple[str, str], port_box: list, injected: bytes) -> None:
    """Answers the SSL request with 'S' followed by plaintext the server should never have sent."""
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(5)
    port_box.append(srv.getsockname()[1])
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(*cert)
    while True:
        conn, _ = srv.accept()
        try:
            conn.recv(8)  # SSLRequest
            conn.sendall(b"S" + injected)
            tls = ctx.wrap_socket(conn, server_side=True)
            tls.recv(65536)  # the startup message, over TLS
            time.sleep(2)
        except Exception:  # noqa: BLE001 - the client is expected to give up
            pass
        finally:
            conn.close()


def _start_mitm(cert, injected: bytes) -> str:
    port_box: list[int] = []
    threading.Thread(target=_mitm_server, args=(cert, port_box, injected), daemon=True).start()
    for _ in range(100):
        if port_box:
            break
        time.sleep(0.02)
    return (f"host=localhost port={port_box[0]} user=app password=secret dbname=app "
            f"sslmode=require connect_timeout=3")


INJECTED = _msg(b"R", struct.pack("!I", 0)) + _msg(b"S", b"server_version\x00INJECTED\x00") + _msg(b"Z", b"I")


def test_sync_rejects_plaintext_sent_after_ssl_answer(tls_cert):
    dsn = _start_mitm(tls_cert, INJECTED)
    with pytest.raises(errors.OperationalError):
        pgtern.connect(dsn)


def test_async_rejects_plaintext_sent_after_ssl_answer(tls_cert):
    dsn = _start_mitm(tls_cert, INJECTED)

    async def go():
        conn = await aio.connect(dsn)
        await conn.close()

    with pytest.raises(errors.OperationalError):
        asyncio.run(asyncio.wait_for(go(), 15))


# ============================================================ 2. executemany failure must not shift later results

@pytest.mark.integration
def test_failed_executemany_does_not_shift_later_results(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        from contextlib import contextmanager

        c.execute("DROP TABLE IF EXISTS rg_em; CREATE TABLE rg_em (id int)")
        seen = {"n": 0}

        @contextmanager
        def bad_on_exit(info):
            yield
            seen["n"] += 1
            if seen["n"] == 3:
                raise RuntimeError("hook exit")

        remove = pgtern.add_query_hook(bad_on_exit)
        try:
            with pytest.raises(RuntimeError, match="hook exit"):
                c.cursor().executemany("INSERT INTO rg_em VALUES (%s)", [(i,) for i in range(5)])
        finally:
            remove()
        assert c.execute("SELECT 42").fetchall() == [(42,)]
        assert c.execute("SELECT 43").fetchall() == [(43,)]
        assert c.execute("SELECT 44").fetchall() == [(44,)]
        c.execute("DROP TABLE rg_em")


# ============================================================ 3. pipeline errors must surface

@pytest.mark.integration
def test_pipeline_block_raises_when_a_statement_fails(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS rg_pipe; CREATE TABLE rg_pipe (id int primary key)")
        with pytest.raises(errors.UniqueViolation):
            with c.pipeline() as pipe:
                pipe.execute("INSERT INTO rg_pipe VALUES (1)")
                pipe.execute("INSERT INTO rg_pipe VALUES (1)")
                pipe.execute("INSERT INTO rg_pipe VALUES (2)")
        assert c.execute("SELECT count(*) FROM rg_pipe").fetchone() == (0,)  # the autocommit batch rolled back
        c.execute("DROP TABLE rg_pipe")


@pytest.mark.integration
def test_pipeline_sync_raises_the_first_error(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            pipe.execute("SELECT 1")
            pipe.execute("SELECT 1/0")
            with pytest.raises(errors.DivisionByZero):
                pipe.sync()
        assert c.execute("SELECT 5").fetchone() == (5,)


@pytest.mark.integration
def test_pipeline_error_in_transaction_surfaces_on_commit_path(dsn):
    with pgtern.connect(dsn) as c:
        c.execute("DROP TABLE IF EXISTS rg_pipe2; CREATE TABLE rg_pipe2 (id int primary key)")
        c.commit()
        with pytest.raises(errors.UniqueViolation):
            with c.pipeline() as pipe:
                pipe.execute("INSERT INTO rg_pipe2 VALUES (1)")
                pipe.execute("INSERT INTO rg_pipe2 VALUES (1)")
        c.rollback()
        assert c.execute("SELECT count(*) FROM rg_pipe2").fetchone() == (0,)
        c.execute("DROP TABLE rg_pipe2")
        c.commit()


# ============================================================ 4. commit of an aborted transaction must raise

@pytest.mark.integration
def test_commit_after_failed_statement_raises(dsn):
    with pgtern.connect(dsn) as c:
        c.execute("DROP TABLE IF EXISTS rg_tx; CREATE TABLE rg_tx (id int primary key)")
        c.commit()
        c.execute("INSERT INTO rg_tx VALUES (1)")
        with pytest.raises(errors.IntegrityError):
            c.execute("INSERT INTO rg_tx VALUES (1)")
        with pytest.raises(errors.InFailedSqlTransaction):
            c.commit()
        c.rollback()
        assert c.execute("SELECT count(*) FROM rg_tx").fetchone() == (0,)
        c.execute("DROP TABLE rg_tx")
        c.commit()


@pytest.mark.integration
def test_transaction_block_with_caught_error_raises_on_exit(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS rg_tx2; CREATE TABLE rg_tx2 (id int primary key)")
        with pytest.raises(errors.InFailedSqlTransaction):
            with c.transaction():
                c.execute("INSERT INTO rg_tx2 VALUES (1)")
                try:
                    c.execute("INSERT INTO rg_tx2 VALUES (1)")
                except errors.IntegrityError:
                    pass
        assert c.execute("SELECT count(*) FROM rg_tx2").fetchone() == (0,)
        c.execute("DROP TABLE rg_tx2")


# ============================================================ 5. pooled connections must not leak session state

@pytest.mark.integration
def test_pool_returns_connections_with_clean_session_state(dsn):
    role = "rg_low_" + uuid.uuid4().hex[:6]
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"DROP ROLE IF EXISTS {role}; CREATE ROLE {role}")
        me = c.execute("SELECT current_user").fetchone()[0]
    with pgtern.ConnectionPool(dsn, min_size=1, max_size=1, kwargs={"autocommit": True}) as pool:
        with pool.connection() as c:
            c.execute(f"SET ROLE {role}")
            c.execute("SET search_path = pg_catalog")
            c.execute("CREATE TEMP TABLE rg_leak (x int)")
            c.row_factory = dict_row
        with pool.connection() as c:
            assert c.execute("SELECT current_user").fetchone() == (me,)
            assert c.execute("SHOW search_path").fetchone() != ("pg_catalog",)
            assert c.execute("SELECT to_regclass('pg_temp.rg_leak')").fetchone() == (None,)
            assert c.execute("SELECT 1 AS one").fetchone() == (1,)  # the row factory was reset to tuples
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"DROP ROLE {role}")


# ============================================================ 6. cancellation

@pytest.mark.integration
def test_sync_cancel_from_another_thread_stops_the_query(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        timer = threading.Timer(0.5, c.cancel)
        timer.start()
        started = time.monotonic()
        with pytest.raises(errors.QueryCanceled):
            c.execute("SELECT pg_sleep(20)")
        timer.join()
        assert time.monotonic() - started < 10
        assert c.execute("SELECT 1").fetchone() == (1,)


@pytest.mark.integration
def test_async_timeout_cancels_the_server_side_query(dsn):
    async def go():
        conn = await aio.connect(dsn, autocommit=True)
        with pytest.raises((asyncio.TimeoutError, TimeoutError)):
            await asyncio.wait_for(conn.execute("SELECT pg_sleep(20)"), 0.5)
        assert conn.closed
        other = await aio.connect(dsn, autocommit=True)
        try:
            for _ in range(30):
                cur = await other.execute("SELECT count(*) FROM pg_stat_activity "
                                          "WHERE query LIKE 'SELECT pg_sleep(20)%' AND state = 'active'")
                if cur.fetchone() == (0,):
                    return
                await asyncio.sleep(0.1)
            raise AssertionError("the abandoned query is still running on the server")
        finally:
            await other.close()

    asyncio.run(asyncio.wait_for(go(), 60))


# ============================================================ 7. a connection must not be returned twice

@pytest.mark.integration
def test_pool_rejects_a_connection_returned_twice(dsn):
    pool = pgtern.ConnectionPool(dsn, min_size=0, max_size=2)
    try:
        a = pool.getconn()
        pool.putconn(a)
        with pytest.raises(errors.ProgrammingError):
            pool.putconn(a)
        x, y = pool.getconn(), pool.getconn()
        assert x is not y
    finally:
        pool.close()


@pytest.mark.integration
def test_async_pool_rejects_a_connection_returned_twice(dsn):
    async def go():
        pool = aio.AsyncConnectionPool(dsn, min_size=0, max_size=1)
        await pool.open()
        try:
            a = await pool.getconn()
            await pool.putconn(a)
            with pytest.raises(errors.ProgrammingError):
                await pool.putconn(a)
            b = await pool.getconn()
            with pytest.raises((asyncio.TimeoutError, errors.OperationalError)):
                await pool.getconn(timeout=0.3)  # max_size=1: there must be no second slot
            await pool.putconn(b)
        finally:
            await pool.close()

    asyncio.run(asyncio.wait_for(go(), 30))
