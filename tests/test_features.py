# SPDX-License-Identifier: Apache-2.0
"""LISTEN/NOTIFY, COPY and server-side cursors."""
from __future__ import annotations

import threading
import time

import pytest

import pgtern
from pgtern import errors

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------- LISTEN / NOTIFY
def test_notify_roundtrip(dsn):
    with pgtern.connect(dsn, autocommit=True) as listener, pgtern.connect(dsn, autocommit=True) as sender:
        listener.execute("LISTEN chan_a")
        sender.execute("SELECT pg_notify('chan_a', 'hello')")
        sender.execute("NOTIFY chan_a, 'world'")
        got = list(listener.notifies(timeout=2, stop_after=2))
        assert [(n.channel, n.payload) for n in got] == [("chan_a", "hello"), ("chan_a", "world")]
        assert got[0].pid == sender.info.backend_pid


def test_notify_timeout_returns_nothing(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute("LISTEN chan_b")
        t = time.monotonic()
        assert list(c.notifies(timeout=0.3)) == []
        assert 0.25 <= time.monotonic() - t < 2


def test_notify_arrives_during_other_statements(dsn):
    with pgtern.connect(dsn, autocommit=True) as listener, pgtern.connect(dsn, autocommit=True) as sender:
        listener.execute("LISTEN chan_c")
        sender.execute("NOTIFY chan_c, 'x'")
        time.sleep(0.2)
        listener.execute("SELECT 1")  # delivers the notification while reading this result
        assert [n.payload for n in listener.notifies(timeout=0.5)] == ["x"]


def test_notify_handler(dsn):
    seen = []
    with pgtern.connect(dsn, autocommit=True) as listener, pgtern.connect(dsn, autocommit=True) as sender:
        listener.add_notify_handler(seen.append)
        listener.execute("LISTEN chan_d")
        sender.execute("NOTIFY chan_d, 'y'")
        time.sleep(0.2)
        listener.execute("SELECT 1")
    assert [n.payload for n in seen] == ["y"]


def test_notify_from_another_thread(dsn):
    with pgtern.connect(dsn, autocommit=True) as listener:
        listener.execute("LISTEN chan_e")

        def send():
            time.sleep(0.2)
            with pgtern.connect(dsn, autocommit=True) as s:
                s.execute("NOTIFY chan_e, 'late'")

        threading.Thread(target=send).start()
        assert [n.payload for n in listener.notifies(timeout=3, stop_after=1)] == ["late"]


def test_notify_server_shutdown_error(dsn):
    with pgtern.connect(dsn, autocommit=True) as victim, pgtern.connect(dsn, autocommit=True) as admin:
        victim.execute("LISTEN chan_f")
        admin.execute("SELECT pg_terminate_backend(%s)", [victim.info.backend_pid])
        with pytest.raises(errors.OperationalError):
            list(victim.notifies(timeout=3))
        assert victim.closed


# ---------------------------------------------------------------- COPY
def test_copy_in_rows_and_out(table, conn):
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} (name, n) FROM STDIN") as copy:
            copy.write_row(("a", 1))
            copy.write_row(("tab\there\nnewline\\slash", None))
            copy.write_row(("unicode-é€", 3))
        assert cur.rowcount == 3
    rows = conn.execute(f"SELECT name, n FROM {table} ORDER BY id").fetchall()
    assert rows == [("a", 1), ("tab\there\nnewline\\slash", None), ("unicode-é€", 3)]
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} (name, n) TO STDOUT") as copy:
            assert list(copy.rows()) == [("a", "1"), ("tab\there\nnewline\\slash", None), ("unicode-é€", "3")]
        assert cur.rowcount == 3


def test_copy_in_raw_bytes_and_csv(table, conn):
    with conn.cursor() as cur:
        with cur.copy(f"COPY {table} (name, n) FROM STDIN WITH (FORMAT csv)") as copy:
            copy.write(b'"x,y",1\n')
            copy.write("z,2\n")
    assert conn.execute(f"SELECT name, n FROM {table} ORDER BY id").fetchall() == [("x,y", 1), ("z", 2)]
    with conn.cursor() as cur, cur.copy(f"COPY {table} (name) TO STDOUT WITH (FORMAT csv)") as copy:
        assert b"".join(copy) == b'"x,y"\nz\n'


def test_copy_with_params_and_query(table, conn):
    conn.execute(f"INSERT INTO {table} (name, n) VALUES ('p', 5), ('q', 6)")
    with conn.cursor() as cur, cur.copy("COPY (SELECT name FROM " + table + " WHERE n > %s ORDER BY n) TO STDOUT", [5]) as copy:
        assert list(copy.rows()) == [("q",)]


def test_copy_error_in_python_aborts_and_connection_survives(table, conn):
    with pytest.raises(RuntimeError):
        with conn.cursor() as cur, cur.copy(f"COPY {table} (name) FROM STDIN") as copy:
            copy.write_row(("keep",))
            raise RuntimeError("boom")
    conn.rollback()
    assert conn.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)


def test_copy_server_error_on_done(table, conn):
    with pytest.raises(errors.CheckViolation):
        with conn.cursor() as cur, cur.copy(f"COPY {table} (name, n) FROM STDIN") as copy:
            copy.write_row(("bad", -1))
    assert conn.info.transaction_status.name == "INERROR"
    conn.rollback()
    assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_bad_statement(conn):
    with pytest.raises(errors.ProgrammingError):
        with conn.cursor() as cur, cur.copy("SELECT 1"):
            pass
    with pytest.raises(errors.UndefinedTable):
        with conn.cursor() as cur, cur.copy("COPY nope_nope FROM STDIN"):
            pass
    conn.rollback()
    assert conn.execute("SELECT 1").fetchone() == (1,)


def test_copy_large(table, conn):
    n = 50_000
    with conn.cursor() as cur, cur.copy(f"COPY {table} (name, n) FROM STDIN") as copy:
        for i in range(n):
            copy.write_row((f"name-{i}", i))
    assert conn.execute(f"SELECT count(*), sum(n) FROM {table}").fetchone() == (n, n * (n - 1) // 2)
    with conn.cursor() as cur, cur.copy(f"COPY {table} (name, n) TO STDOUT") as copy:
        assert sum(1 for _ in copy.rows()) == n


def test_copy_execute_gives_clear_error(table, aconn):
    with pytest.raises(errors.NotSupportedError, match="cursor.copy"):
        aconn.execute(f"COPY {table} TO STDOUT")
    assert aconn.execute("SELECT 1").fetchone() == (1,)


def test_copy_hooks_see_statement(table, conn):
    seen = []
    from contextlib import contextmanager

    @contextmanager
    def hook(info):
        seen.append(info.statement)
        yield

    remove = pgtern.add_query_hook(hook)
    try:
        with conn.cursor() as cur, cur.copy(f"COPY {table} (name) FROM STDIN") as copy:
            copy.write_row(("h",))
    finally:
        remove()
    assert any(s.startswith("COPY") for s in seen)


# ---------------------------------------------------------------- server-side cursors
def test_server_cursor_batches(conn):
    with conn.cursor("big") as cur:
        cur.execute("SELECT g, g::text FROM generate_series(1, 1050) g")
        assert [c.name for c in cur.description] == ["g", "text"] or cur.description[0].name == "g"
        assert cur.fetchone() == (1, "1")
        assert len(cur.fetchmany(10)) == 10
        assert cur.rownumber == 11
        rest = cur.fetchall()
        assert len(rest) == 1039 and rest[-1] == (1050, "1050")
        assert cur.fetchone() is None


def test_server_cursor_iteration_and_itersize(conn):
    with conn.cursor("it") as cur:
        cur.itersize = 7
        cur.execute("SELECT g FROM generate_series(1, 30) g")
        assert [r[0] for r in cur] == list(range(1, 31))


def test_server_cursor_params_rowfactory_and_scroll(conn):
    from pgtern.rows import dict_row
    with conn.cursor("sc", row_factory=dict_row, scrollable=True) as cur:
        cur.execute("SELECT g AS v FROM generate_series(1, %s) g", [10])
        assert cur.fetchmany(3) == [{"v": 1}, {"v": 2}, {"v": 3}]
        cur.scroll(-2)
        assert cur.fetchone() == {"v": 2}
        cur.scroll(8, "absolute")
        assert cur.fetchone() == {"v": 9}


def test_server_cursor_sql_composed_and_reuse_error(conn):
    from pgtern import sql
    cur = conn.cursor("comp")
    cur.execute(sql.SQL("SELECT {} AS x").format(sql.Literal(5)))
    assert cur.fetchall() == [(5,)]
    with pytest.raises(errors.ProgrammingError):
        cur.execute("SELECT 1")
    cur.close()
    assert cur.closed


def test_server_cursor_name_collision_and_no_transaction(aconn):
    with pytest.raises(errors.Error):
        aconn.cursor("nt").execute("SELECT 1")  # DECLARE outside a transaction (autocommit)
    with aconn.transaction():
        c1 = aconn.cursor("dup")
        c1.execute("SELECT 1")
        with pytest.raises(errors.DuplicateCursor):
            aconn.cursor("dup").execute("SELECT 2")


def test_server_cursor_withhold_survives_commit(conn):
    cur = conn.cursor("hold", withhold=True)
    cur.execute("SELECT g FROM generate_series(1, 5) g")
    conn.commit()
    assert [r[0] for r in cur.fetchall()] == [1, 2, 3, 4, 5]
    cur.close()


def test_server_cursor_error_leaves_connection_usable(conn):
    cur = conn.cursor("bad")
    with pytest.raises(errors.UndefinedTable):
        cur.execute("SELECT * FROM missing_table_zz")
    conn.rollback()
    assert conn.execute("SELECT 1").fetchone() == (1,)
    cur.close()
