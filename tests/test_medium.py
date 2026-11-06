# SPDX-License-Identifier: Apache-2.0
"""Regression tests for the Medium items of the external review (pgtern 0.3.0).

Tests marked ``integration`` need a server; the others use local fake servers or no server.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import os
import shutil
import socket
import ssl
import struct
import subprocess
import threading
import time

import pytest

import pgtern
from pgtern import _binary, _dsn, _types, aio, errors
from test_regressions import READY, _auth, _fake_server, _handshake


def _with_timeout(fn, seconds: float = 60.0):
    """Run ``fn`` in a thread. A hang fails the test instead of blocking the run."""
    box: dict = {}

    def target() -> None:
        try:
            box["value"] = fn()
        except BaseException as exc:  # noqa: BLE001 - re-raised below
            box["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(seconds)
    assert not thread.is_alive(), f"hung for more than {seconds} seconds"
    if "error" in box:
        raise box["error"]
    return box.get("value")


def _with_param(dsn: str, **params: str) -> str:
    sep = "&" if "?" in dsn else "?"
    return dsn + sep + "&".join(f"{k}={v}" for k, v in params.items())


# ============================================================ 1. connection strings and passwords (no server)

def test_password_is_hidden_from_repr():
    params = _dsn.build_params("host=h user=u password=s3cret dbname=d")
    assert "s3cret" not in repr(params)


def test_one_port_applies_to_every_host():
    params = _dsn.build_params("host=h1,h2,h3 port=6000 user=u dbname=d")
    assert params.hosts == [("h1", 6000), ("h2", 6000), ("h3", 6000)]


def test_mismatched_port_count_is_a_clear_error():
    with pytest.raises(errors.ProgrammingError, match="port"):
        _dsn.build_params("host=h1,h2,h3 port=1,2 user=u dbname=d")


def test_plus_in_a_url_query_value_stays_a_plus():
    params = _dsn.build_params("postgresql://u@h/db?application_name=my+app")
    assert params.application_name == "my+app"


def test_target_session_attrs_is_validated():
    assert _dsn.build_params("host=h user=u dbname=d target_session_attrs=read-only").target_session_attrs == "read-only"
    with pytest.raises(errors.ProgrammingError, match="target_session_attrs"):
        _dsn.build_params("host=h user=u dbname=d target_session_attrs=whenever")


def test_pgpass_supplies_the_password(monkeypatch, tmp_path):
    path = tmp_path / "pgpass"
    path.write_text("# comment\nother:5432:app:app:wrong\n127.0.0.1:5432:app:app:fromfile\n")
    path.chmod(0o600)
    monkeypatch.setenv("PGPASSFILE", str(path))
    monkeypatch.delenv("PGPASSWORD", raising=False)
    params = _dsn.build_params("host=127.0.0.1 port=5432 user=app dbname=app")
    assert _dsn.resolve_password(params, "127.0.0.1", 5432) == "fromfile"


def test_pgpass_that_is_readable_by_others_is_ignored(monkeypatch, tmp_path):
    path = tmp_path / "pgpass"
    path.write_text("127.0.0.1:5432:app:app:fromfile\n")
    path.chmod(0o644)
    monkeypatch.setenv("PGPASSFILE", str(path))
    monkeypatch.delenv("PGPASSWORD", raising=False)
    params = _dsn.build_params("host=127.0.0.1 port=5432 user=app dbname=app")
    assert _dsn.resolve_password(params, "127.0.0.1", 5432) is None


def test_hostaddr_is_used_for_the_connection():
    port = _fake_server(lambda sock: (_handshake(sock), sock.sendall(_auth(0) + READY)))
    dsn = (f"host=name.that.does.not.resolve.invalid hostaddr=127.0.0.1 port={port} "
           f"user=app password=secret dbname=app sslmode=disable connect_timeout=3")
    pgtern.connect(dsn).close()


# ============================================================ 2. values: integers and intervals

def test_ints_are_sent_as_int4_or_wider():
    assert _types.dump(300)[0] == _types.INT4
    assert _types.dump(2**40)[0] == _types.INT8
    assert _types.dump(2**70)[0] == _types.NUMERIC


def test_interval_with_months_keeps_its_months():
    value = _types._dec_interval("1 year 2 mons 3 days 04:05:06")
    assert isinstance(value, pgtern.Interval)
    assert value.months == 14
    assert value.delta == dt.timedelta(days=3, hours=4, minutes=5, seconds=6)


def test_binary_interval_keeps_its_months():
    raw = struct.pack("!qii", 0, 3, 14)  # 0 microseconds, 3 days, 14 months
    value = _binary._dec_interval(raw)
    assert isinstance(value, pgtern.Interval)
    assert (value.months, value.delta) == (14, dt.timedelta(days=3))


def test_interval_without_months_is_still_a_timedelta():
    assert _types._dec_interval("3 days 01:00:00") == dt.timedelta(days=3, hours=1)


# ============================================================ 3. SSL: key password and revocation list

def _openssl(*args: str) -> None:
    if shutil.which("openssl") is None:
        pytest.skip("needs the openssl command")
    subprocess.run(["openssl", *args], check=True, capture_output=True)


def test_sslpassword_opens_an_encrypted_client_key(tmp_path):
    cert, key = str(tmp_path / "c.pem"), str(tmp_path / "k.pem")
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-keyout", key, "-out", cert, "-days", "1",
             "-subj", "/CN=client", "-passout", "pass:hunter2")
    params = _dsn.build_params(f"host=h user=u dbname=d sslmode=require sslcert={cert} sslkey={key} "
                               f"sslpassword=hunter2")
    assert params.sslpassword == "hunter2"
    from pgtern._conn import _ssl_context
    _ssl_context(params, "require")  # must load the encrypted key without prompting


def test_sslcrl_turns_on_revocation_checking(tmp_path):
    ca_key, ca_cert = str(tmp_path / "ca.key"), str(tmp_path / "ca.pem")
    _openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", ca_key, "-out", ca_cert,
             "-days", "1", "-subj", "/CN=test ca")
    (tmp_path / "index.txt").write_text("")
    (tmp_path / "crlnumber").write_text("1000\n")
    conf = tmp_path / "ca.cnf"
    conf.write_text(f"""[ca]
default_ca = CA_default
[CA_default]
database = {tmp_path}/index.txt
crlnumber = {tmp_path}/crlnumber
default_md = sha256
default_crl_days = 1
certificate = {ca_cert}
private_key = {ca_key}
""")
    crl = str(tmp_path / "crl.pem")
    _openssl("ca", "-config", str(conf), "-gencrl", "-out", crl)
    params = _dsn.build_params(f"host=localhost user=u dbname=d sslmode=verify-full sslrootcert={ca_cert} "
                               f"sslcrl={crl}")
    from pgtern._conn import _ssl_context
    ctx = _ssl_context(params, "verify-full")
    assert ctx.verify_flags & ssl.VERIFY_CRL_CHECK_CHAIN


# ============================================================ 4. pipelines and executemany

@pytest.mark.integration
def test_copy_inside_a_pipeline_is_refused(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with c.pipeline() as pipe:
            pipe.execute("SELECT 1")
            with pytest.raises(errors.ProgrammingError):
                with c.cursor().copy("COPY (SELECT 1) TO STDOUT") as copy:
                    copy.read()


@pytest.mark.integration
def test_copy_inside_executemany_is_refused(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with pytest.raises(errors.ProgrammingError):
            _with_timeout(lambda: c.cursor().executemany("COPY rg_nonexistent FROM STDIN", [(1,)]), 20)


@pytest.mark.integration
def test_pipeline_with_large_results_does_not_deadlock(dsn):
    big = "x" * 1_000_000

    def run():
        with pgtern.connect(dsn, autocommit=True) as c:
            with c.pipeline() as pipe:
                cursors = [pipe.execute("SELECT %s", [big]) for _ in range(30)]
            return [cur.fetchone()[0] == big for cur in cursors]

    assert all(_with_timeout(run, 90))


@pytest.mark.integration
def test_executemany_with_large_results_does_not_deadlock(dsn):
    big = "y" * 1_000_000

    def run():
        with pgtern.connect(dsn, autocommit=True) as c:
            return c.cursor().executemany("SELECT %s", [(big,)] * 30)

    _with_timeout(run, 90)


@pytest.mark.integration
def test_autocommit_pipeline_block_error_rolls_back_the_batch(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute("DROP TABLE IF EXISTS rg_ac; CREATE TABLE rg_ac (id int)")
        with pytest.raises(RuntimeError):
            with c.pipeline() as pipe:
                pipe.execute("INSERT INTO rg_ac VALUES (1)")
                raise RuntimeError("the application failed after queuing work")
        assert c.execute("SELECT count(*) FROM rg_ac").fetchone() == (0,)
        assert c.execute("SELECT 1").fetchone() == (1,)
        c.execute("DROP TABLE rg_ac")


def _drop_after_read(sock):
    _handshake(sock)
    sock.sendall(_auth(0) + READY)
    time.sleep(0.3)  # let the client send its pipeline, then drop the connection


def test_cursors_report_the_real_error_when_the_connection_drops_mid_pipeline():
    port = _fake_server(_drop_after_read)
    conn = pgtern.connect(f"host=127.0.0.1 port={port} user=app password=secret dbname=app sslmode=disable "
                          f"connect_timeout=3")
    with conn.pipeline() as pipe:
        cursor = pipe.execute("SELECT 1")
        with pytest.raises(errors.OperationalError):
            pipe.sync()
    with pytest.raises(errors.OperationalError) as exc:
        cursor.fetchone()
    assert "synchronized" not in str(exc.value)


# ============================================================ 5. pools and liveness

@pytest.mark.integration
def test_async_pool_refills_min_size_after_a_connection_dies(dsn):
    async def go():
        pool = aio.AsyncConnectionPool(dsn, min_size=2, max_size=2)
        await pool.open(wait=True)
        try:
            victim = pool._idle[0]
            other = await aio.connect(dsn, autocommit=True)
            try:
                await other.execute("SELECT pg_terminate_backend(%s)", [victim._backend_pid])
            finally:
                await other.close()
            for _ in range(100):
                if len(pool._idle) == 2 and victim not in pool._idle:
                    return
                await asyncio.sleep(0.1)
            raise AssertionError("the async pool did not replace the dead connection")
        finally:
            await pool.close()

    asyncio.run(asyncio.wait_for(go(), 60))


def test_async_pool_accepts_max_idle():
    async def go():
        pool = aio.AsyncConnectionPool("host=127.0.0.1 port=1", min_size=0, max_size=2, max_idle=0.2)
        assert pool.max_idle == 0.2

    asyncio.run(go())


def test_keepalive_is_tuned_so_dead_peers_are_noticed():
    if not hasattr(socket, "TCP_KEEPIDLE"):
        pytest.skip("TCP keepalive tuning is Linux-specific here")
    port = _fake_server(lambda sock: (_handshake(sock), sock.sendall(_auth(0) + READY), time.sleep(2)))
    conn = pgtern.connect(f"host=127.0.0.1 port={port} user=app password=secret dbname=app sslmode=disable "
                          f"connect_timeout=3")
    try:
        assert conn._sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE) <= 60
        assert conn._sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL) <= 30
        assert conn._sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT) <= 5
    finally:
        conn.close()


@pytest.mark.integration
def test_target_session_attrs_refuses_a_server_that_is_not_read_only(dsn):
    with pytest.raises(errors.OperationalError, match="read-only"):
        pgtern.connect(_with_param(dsn, target_session_attrs="read-only"))
    with pgtern.connect(_with_param(dsn, target_session_attrs="read-write")) as c:
        assert c.execute("SELECT 1").fetchone() == (1,)


# ============================================================ 6. fork safety

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


# The test process is multi-threaded (the pool thread, the fake servers), so Python warns about fork();
# that warning is about deadlocks in arbitrary code, and the child here runs only pgtern calls.
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_forked_child_cannot_use_the_parent_connection():
    received: list[bytes] = []

    def script(sock):
        _handshake(sock)
        sock.sendall(_auth(0) + READY)
        while True:
            data = sock.recv(4096)
            if not data:
                return
            received.append(data)

    port = _fake_server(script)
    conn = pgtern.connect(f"host=127.0.0.1 port={port} user=app password=secret dbname=app sslmode=disable "
                          f"connect_timeout=3")

    def child() -> bool:
        try:
            conn.execute("SELECT 1")
            return False
        except errors.OperationalError:
            pass
        conn.close()  # must not send Terminate over the shared socket
        return True

    assert _fork_and_run(child) == 0
    time.sleep(0.3)
    assert not any(b"X" in data for data in received)


@pytest.mark.integration
@pytest.mark.filterwarnings("ignore:This process .* is multi-threaded:DeprecationWarning")
@pytest.mark.skipif(not hasattr(os, "fork"), reason="needs fork")
def test_forked_child_gets_fresh_pool_connections(dsn):
    pool = pgtern.ConnectionPool(dsn, min_size=1, max_size=2)
    try:
        with pool.connection() as c:
            parent_backend = c.info.backend_pid

        def child() -> bool:
            with pool.connection() as c:  # must be a new server session, not the parent's socket
                return c.info.backend_pid != parent_backend and c.execute("SELECT 2").fetchone() == (2,)

        assert _fork_and_run(child) == 0
        with pool.connection() as c:
            assert c.execute("SELECT 3").fetchone() == (3,)
    finally:
        pool.close()


# ============================================================ 7. more value and pool checks against a server

@pytest.mark.integration
def test_int_parameters_multiply_without_overflow(dsn):
    with pgtern.connect(dsn) as c:
        assert c.execute("SELECT %s * %s", [300, 300]).fetchone() == (90000,)
        assert c.execute("SELECT %s::smallint", [7]).fetchone() == (7,)


@pytest.mark.integration
def test_int_parameter_inserts_into_a_text_column(dsn):
    with pgtern.connect(dsn) as c:
        c.execute("DROP TABLE IF EXISTS rg_txt; CREATE TABLE rg_txt (t text)")
        c.execute("INSERT INTO rg_txt VALUES (%s)", [5])
        assert c.execute("SELECT t FROM rg_txt").fetchone() == ("5",)
        c.execute("DROP TABLE rg_txt")
        c.commit()


@pytest.mark.integration
def test_interval_with_months_round_trips_through_the_server(dsn):
    value = pgtern.Interval(1, dt.timedelta(days=2, hours=3))
    with pgtern.connect(dsn) as c:
        assert c.execute("SELECT %s::interval", [value]).fetchone() == (value,)
        assert c.execute("SELECT interval '1 month'").fetchone() == (pgtern.Interval(1, dt.timedelta()),)


@pytest.mark.integration
def test_async_pool_getconn_respects_one_deadline(dsn):
    """A server that accepts the connection and never answers the startup must not hold getconn past its timeout."""
    silent = _fake_server(lambda sock: (_handshake(sock), time.sleep(10)))

    async def go():
        pool = aio.AsyncConnectionPool(f"host=127.0.0.1 port={silent} user=app password=x dbname=app sslmode=disable",
                                       min_size=0, max_size=1, timeout=1.0)
        pool._closed = False
        pool._slots = asyncio.Semaphore(1)
        started = time.monotonic()
        with pytest.raises(pgtern.PoolTimeout):
            await pool.getconn(timeout=1.0)
        assert time.monotonic() - started < 2.0

    asyncio.run(asyncio.wait_for(go(), 30))
