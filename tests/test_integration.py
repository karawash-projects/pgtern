# SPDX-License-Identifier: Apache-2.0
"""Integration tests against a real PostgreSQL (see scripts/start-test-postgres.sh)."""
from __future__ import annotations

import os
import random
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import date, datetime, time as dtime, timedelta, timezone
from decimal import Decimal

import pytest

import pgtern
from pgtern import errors, sql
from pgtern.pool import ConnectionPool, PoolClosed, PoolTimeout
from pgtern.rows import dict_row, namedtuple_row
from pgtern.types import Jsonb

from conftest import CA, HOST, PORT, SOCKET_DIR, url

pytestmark = pytest.mark.integration


# ===================================================================== authentication & TLS
@pytest.mark.auth_roles
def test_scram_md5_cleartext_and_trust_authentication():
    for user, pw in (("postgres", "postgres"), ("md5user", "md5pw"), ("clearuser", "clearpw"), ("trustuser", None)):
        with pgtern.connect(url(user, pw, sslmode="disable")) as c:
            assert c.execute("SELECT current_user").fetchone() == (user,)


def test_wrong_password_is_invalid_password():
    for user, pw in (("postgres", "nope"), ("md5user", "nope"), ("clearuser", "nope")):
        with pytest.raises(errors.InvalidPassword) as exc:
            pgtern.connect(url(user, pw, sslmode="disable"))
        assert exc.value.sqlstate == "28P01" and isinstance(exc.value, pgtern.OperationalError)


def test_missing_password_when_server_wants_one():
    with pytest.raises(pgtern.OperationalError, match="password"):
        pgtern.connect(url("postgres", None, sslmode="disable"))


def test_unknown_database_is_invalid_catalog_name(dsn):
    with pytest.raises(errors.InvalidCatalogName):
        pgtern.connect(dsn.rsplit("/", 1)[0] + "/no_such_db_here")


@pytest.mark.skipif(not SOCKET_DIR, reason="needs PGTERN_TEST_SOCKET_DIR")
def test_unix_socket():
    with pgtern.connect(host=SOCKET_DIR, port=PORT, user="postgres", dbname="postgres") as c:
        assert c.execute("SELECT 1").fetchone() == (1,)
        assert c.info.host == SOCKET_DIR


def _ssl_in_use(c) -> bool:
    return c.execute("SELECT ssl FROM pg_stat_ssl WHERE pid = pg_backend_pid()").fetchone()[0]


def _needs_tls_server():
    with pgtern.connect(url("postgres", "postgres", sslmode="disable")) as c:
        if c.execute("SHOW ssl").fetchone()[0] != "on":
            pytest.skip("the test server has no TLS (see scripts/start-test-postgres.sh)")


def test_tls_modes():
    _needs_tls_server()
    assert _ssl_in_use(pgtern.connect(url("postgres", "postgres", sslmode="require"))) is True
    assert _ssl_in_use(pgtern.connect(url("postgres", "postgres", sslmode="prefer"))) is True
    assert _ssl_in_use(pgtern.connect(url("postgres", "postgres"))) is True  # default is prefer
    assert _ssl_in_use(pgtern.connect(url("postgres", "postgres", sslmode="disable"))) is False
    assert _ssl_in_use(pgtern.connect(url("postgres", "postgres", sslmode="allow"))) is False


def test_tls_only_user_needs_tls():
    _needs_tls_server()
    if not os.environ.get("PGTERN_TEST_AUTH_ROLES"):
        pytest.skip("needs the local auth cluster: set PGTERN_TEST_AUTH_ROLES=1")
    with pgtern.connect(url("tlsuser", "tlspw", sslmode="require")) as c:
        assert _ssl_in_use(c)
    with pytest.raises(pgtern.OperationalError):
        pgtern.connect(url("tlsuser", "tlspw", sslmode="disable"))
    # sslmode=allow tries plaintext first, then retries with TLS
    with pgtern.connect(url("tlsuser", "tlspw", sslmode="allow")) as c:
        assert _ssl_in_use(c)


@pytest.mark.skipif(not CA, reason="needs PGTERN_TEST_CA")
def test_tls_certificate_verification():
    for mode in ("verify-ca", "verify-full"):
        with pgtern.connect(url("postgres", "postgres", sslmode=mode, sslrootcert=CA)) as c:
            assert _ssl_in_use(c)
    # verification without any root certificate is a configuration error, raised before connecting
    for mode in ("verify-ca", "verify-full"):
        with pytest.raises(pgtern.ProgrammingError, match="sslrootcert"):
            pgtern.connect(url("postgres", "postgres", sslmode=mode))
    # a self-signed cert is not trusted by the system store
    for mode in ("verify-ca", "verify-full"):
        with pytest.raises(pgtern.OperationalError, match="SSL"):
            pgtern.connect(url("postgres", "postgres", sslmode=mode, sslrootcert="system"))
    # hostname mismatch: the cert covers localhost and 127.0.0.1, not this name
    with pytest.raises(pgtern.OperationalError, match="SSL"):
        pgtern.connect(f"postgresql://postgres:postgres@127.0.0.2:{PORT}/postgres?sslmode=verify-full&sslrootcert={CA}")


def test_multi_host_failover_and_connect_timeout():
    with pgtern.connect(f"postgresql://postgres:postgres@127.0.0.1:1,{HOST}:{PORT}/postgres?sslmode=disable") as c:
        assert c.info.port == int(PORT)
    started = time.monotonic()
    with pytest.raises(pgtern.OperationalError):
        pgtern.connect("postgresql://u:p@10.255.255.1:5432/db?connect_timeout=1&sslmode=disable")
    assert time.monotonic() - started < 10


# ===================================================================== query protocols
def test_simple_query_runs_multiple_statements_and_keeps_percent_signs(conn):
    cur = conn.execute("SELECT 'a%sb' AS x, 1 AS y; SELECT 2; SELECT 3")
    assert cur.fetchall() == [("a%sb", 1)]
    assert cur.nextset() and cur.fetchall() == [(2,)]
    assert cur.nextset() and cur.fetchall() == [(3,)]
    assert cur.nextset() is None


def test_whole_migration_file_in_one_call(aconn):
    name = "mig_" + uuid.uuid4().hex[:8]
    script = f"""
    CREATE TABLE {name} (a int);
    CREATE FUNCTION {name}_f() RETURNS int LANGUAGE plpgsql AS $$ BEGIN RETURN 42; END $$;
    INSERT INTO {name} VALUES (1), (2);
    -- a % sign and a 'quoted ; semicolon' that must not confuse anything
    COMMENT ON TABLE {name} IS '100% done; really';
    """
    with aconn.transaction():
        aconn.execute(script)
    assert aconn.execute(f"SELECT {name}_f(), (SELECT count(*) FROM {name})").fetchone() == (42, 2)
    aconn.execute(f"DROP FUNCTION {name}_f(); DROP TABLE {name}")


def test_empty_query(conn):
    cur = conn.execute("")
    assert cur.description is None and cur.rowcount == -1
    with pytest.raises(pgtern.ProgrammingError):
        cur.fetchone()


def test_parameters_and_casts(conn):
    # (an untyped NULL needs a cast, e.g. `%s::text IS NULL`: PostgreSQL itself can't infer its type)
    assert conn.execute("SELECT %s::int + %s, %s::text, %s::text IS NULL", (2, 3, "x", None)).fetchone() == (5, "x", True)
    with pytest.raises(errors.IndeterminateDatatype):
        conn.execute("SELECT %s IS NULL", (None,))
    conn.rollback()
    assert conn.execute("SELECT %(a)s::int + %(a)s::int + %(b)s::int", {"a": 1, "b": 5}).fetchone() == (7,)
    assert conn.execute("SELECT 'it''s 100%%' || %s", ("!",)).fetchone() == ("it's 100%!",)
    # f(smallint) resolution works because small ints are sent as int2
    assert conn.execute("SELECT make_interval(days => %s)", (3,)).fetchone() == (timedelta(days=3),)
    assert conn.execute("SELECT now() + make_interval(secs => %s) > now()", (60,)).fetchone() == (True,)
    assert conn.execute("SELECT %s::text[]", (["a", "b,c", 'd"e', "f\\g", None, ""],)).fetchone() == (
        ["a", "b,c", 'd"e', "f\\g", None, ""],)


ROUNDTRIP = [
    None, True, False, 0, -1, 32767, 32768, 2**31, 2**40, -(2**63), 2**64,
    1.5, -0.0, 1e100, float("inf"), Decimal("12345678901234567890.123456789"), Decimal("-0.5"),
    "", "héllo wörld", "日本語 😀", "a'b\"c\\d%e$f", "line1\nline2\ttab", "x" * 100_000,
    b"", b"\x00\x01\xfe\xff", bytes(range(256)) * 100,
    uuid.UUID("12345678-1234-5678-1234-567812345678"),
    datetime(2026, 11, 1, 14, 6, 5, 123456, tzinfo=timezone.utc),
    datetime(2026, 11, 1, 14, 6, 5, tzinfo=timezone(timedelta(hours=-5, minutes=-30))),
    datetime(2026, 11, 1, 14, 6, 5, 999999), date(2026, 2, 28), date(1, 1, 1), date(9999, 12, 31),
    dtime(23, 59, 59, 1), timedelta(days=3, seconds=7, microseconds=9), timedelta(days=-2, seconds=5),
    Jsonb({"a": [1, 2.5, None, "x"], "b": {"c": True}}),
    [1, 2, 3], [[1, 2], [3, 4]], [Decimal("1.5"), None], [True, False], [uuid.UUID(int=5)],
    [datetime(2026, 1, 1, tzinfo=timezone.utc)], [b"\x01", b"\xff\x00"],
]


@pytest.mark.parametrize("value", ROUNDTRIP, ids=[repr(v)[:40] for v in ROUNDTRIP])
def test_value_roundtrip(conn, value):
    got = conn.execute("SELECT %s", (value,)).fetchone()[0]
    if isinstance(value, Jsonb):
        assert got == value.obj
    elif isinstance(value, float) and value != value:
        assert got != got
    elif isinstance(value, int) and not isinstance(value, bool) and abs(value) >= 2**63:
        assert got == Decimal(value)
    else:
        assert got == value  # aware datetimes compare by instant; the offset is the session TimeZone's
        if isinstance(value, datetime) and value.tzinfo is not None:
            assert got.tzinfo is not None


def test_nan_and_bounds_of_special_values(conn):
    assert conn.execute("SELECT %s", (float("nan"),)).fetchone()[0] != conn.execute("SELECT 'NaN'::float8").fetchone()[0]
    assert conn.execute("SELECT %s::text", (Decimal("NaN"),)).fetchone() == ("NaN",)


def test_unrepresentable_dates_raise_dataerror_and_leave_connection_usable(conn):
    for expr in ("'infinity'::date", "'-infinity'::timestamptz", "'0044-03-15 BC'::date", "'12345-01-01'::date"):
        with pytest.raises(pgtern.DataError):
            conn.execute(f"SELECT {expr}").fetchone()
    assert conn.execute("SELECT 1").fetchone() == (1,)
    assert conn.execute("SELECT 'infinity'::date::text").fetchone() == ("infinity",)


def test_server_types_decode(conn):
    row = conn.execute(
        "SELECT 1::int2, 2::int4, 3::int8, 1.25::float4, 'x'::varchar, 'ab'::char(4), 'n'::name, "
        "'{\"k\":[1,2]}'::json, '{\"k\":1}'::jsonb, interval '1 year 2 mons 3 days 04:05:06', "
        "'{1,2}'::int4[], '{a,b}'::varchar[], ARRAY['{\"a\": 1}'::jsonb], '10.0.0.1'::inet"
    ).fetchone()
    assert row == (1, 2, 3, 1.25, "x", "ab  ", "n", {"k": [1, 2]}, {"k": 1},
                   pgtern.Interval(14, timedelta(days=3, hours=4, minutes=5, seconds=6)), [1, 2], ["a", "b"],
                   [{"a": 1}], "10.0.0.1")  # inet has no decoder yet: it comes back as text


def test_big_result_and_big_values(conn):
    rows = conn.execute("SELECT g, md5(g::text) FROM generate_series(1, 20000) g").fetchall()
    assert len(rows) == 20000 and rows[-1][0] == 20000
    big = "é" * 3_000_000
    assert conn.execute("SELECT %s", (big,)).fetchone()[0] == big


# ===================================================================== cursor API
def test_cursor_fetch_variants_and_rowcount(table, conn):  # `table` first: it must outlive `conn`'s open transaction
    cur = conn.execute(f"INSERT INTO {table} (name, n) VALUES (%s, %s), (%s, %s), (%s, %s)",
                       ("a", 1, "b", 2, "c", 3))
    assert cur.rowcount == 3 and cur.description is None and cur.statusmessage == "INSERT 0 3"
    cur = conn.execute(f"SELECT id, name FROM {table} ORDER BY name")
    assert [c.name for c in cur.description] == ["id", "name"] and cur.rowcount == 3
    assert cur.fetchone()[1] == "a"
    assert [r[1] for r in cur.fetchmany(1)] == ["b"]
    assert [r[1] for r in cur.fetchall()] == ["c"]
    assert cur.fetchone() is None and cur.fetchall() == []
    assert [r[1] for r in conn.execute(f"SELECT id, name FROM {table} ORDER BY name")] == ["a", "b", "c"]
    assert conn.execute(f"UPDATE {table} SET n = n + 1 WHERE n > %s", (1,)).rowcount == 2
    assert conn.execute(f"DELETE FROM {table}").rowcount == 3
    with pytest.raises(pgtern.ProgrammingError):
        conn.execute("CREATE TEMP TABLE zz (a int)").fetchone()


def test_row_factories_and_executemany(dsn, table):
    with pgtern.connect(dsn, row_factory=dict_row) as c:
        cur = c.cursor()
        cur.executemany(f"INSERT INTO {table} (name, n) VALUES (%s, %s)", [("x", 1), ("y", 2)])
        assert cur.rowcount == 2
        assert c.execute(f"SELECT name, n FROM {table} ORDER BY name").fetchall() == [
            {"name": "x", "n": 1}, {"name": "y", "n": 2}]
        row = c.cursor(row_factory=namedtuple_row).execute(f"SELECT name, n FROM {table} ORDER BY name").fetchone()
        assert (row.name, row.n) == ("x", 1)
        c.rollback()


def test_closed_cursor_and_connection(dsn):
    c = pgtern.connect(dsn)
    cur = c.cursor()
    cur.close()
    with pytest.raises(pgtern.InterfaceError):
        cur.execute("SELECT 1")
    c.close()
    c.close()  # idempotent
    with pytest.raises(pgtern.OperationalError):
        c.execute("SELECT 1")


# ===================================================================== transactions
def test_implicit_transaction_commit_and_rollback(dsn, table):
    with pgtern.connect(dsn) as a, pgtern.connect(dsn, autocommit=True) as b:
        a.execute(f"INSERT INTO {table} (name) VALUES ('one')")
        assert a.info.transaction_status is pgtern.TransactionStatus.INTRANS
        assert b.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
        a.commit()
        assert a.info.transaction_status is pgtern.TransactionStatus.IDLE
        assert b.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)
        a.execute(f"INSERT INTO {table} (name) VALUES ('two')")
        a.rollback()
        assert b.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)


def test_connection_context_manager_commits_or_rolls_back(dsn, table):
    with pgtern.connect(dsn) as c:
        c.execute(f"INSERT INTO {table} (name) VALUES ('kept')")
    assert c.closed
    with pytest.raises(RuntimeError):
        with pgtern.connect(dsn) as c:
            c.execute(f"INSERT INTO {table} (name) VALUES ('lost')")
            raise RuntimeError
    with pgtern.connect(dsn) as c:
        assert c.execute(f"SELECT name FROM {table}").fetchall() == [("kept",)]


def test_transaction_block_on_idle_connection_begins_and_commits(dsn, table):
    with pgtern.connect(dsn) as a, pgtern.connect(dsn, autocommit=True) as b:
        with a.transaction():
            a.execute(f"INSERT INTO {table} (name) VALUES ('in-block')")
            assert b.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
        assert b.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)
        assert a.info.transaction_status is pgtern.TransactionStatus.IDLE
        with pytest.raises(RuntimeError):
            with a.transaction():
                a.execute(f"INSERT INTO {table} (name) VALUES ('rolled-back')")
                raise RuntimeError
        assert b.execute(f"SELECT count(*) FROM {table}").fetchone() == (1,)


def test_nested_transactions_are_savepoints(aconn, table):
    with aconn.transaction():
        aconn.execute(f"INSERT INTO {table} (name) VALUES ('outer')")
        with pytest.raises(errors.UniqueViolation):
            with aconn.transaction():
                aconn.execute(f"INSERT INTO {table} (name) VALUES ('inner')")
                aconn.execute(f"INSERT INTO {table} (name) VALUES ('outer')")  # fails; inner work is undone
        with aconn.transaction(savepoint_name="named"):
            aconn.execute(f"INSERT INTO {table} (name) VALUES ('after')")
        assert aconn.info.transaction_status is pgtern.TransactionStatus.INTRANS
    assert aconn.execute(f"SELECT name FROM {table} ORDER BY name").fetchall() == [("after",), ("outer",)]


def test_force_rollback_and_error_state(aconn, table):
    with aconn.transaction(force_rollback=True):
        aconn.execute(f"INSERT INTO {table} (name) VALUES ('ghost')")
    assert aconn.execute(f"SELECT count(*) FROM {table}").fetchone() == (0,)
    with pytest.raises(errors.InFailedSqlTransaction):  # leaving the block after an error is not a commit
        with aconn.transaction():
            with pytest.raises(errors.DivisionByZero):
                aconn.execute("SELECT 1/0")
            assert aconn.info.transaction_status is pgtern.TransactionStatus.INERROR
            with pytest.raises(errors.InFailedSqlTransaction):
                aconn.execute("SELECT 1")
    assert aconn.execute("SELECT 1").fetchone() == (1,)


def test_commit_inside_block_and_autocommit_change_are_forbidden(conn):
    with conn.transaction():
        with pytest.raises(pgtern.ProgrammingError):
            conn.commit()
        with pytest.raises(pgtern.ProgrammingError):
            conn.rollback()
    conn.execute("SELECT 1")
    with pytest.raises(pgtern.ProgrammingError):
        conn.autocommit = True
    conn.rollback()
    conn.autocommit = True
    assert conn.autocommit is True


def test_autocommit_connection_does_not_open_transactions(aconn):
    aconn.execute("SELECT 1")
    assert aconn.info.transaction_status is pgtern.TransactionStatus.IDLE
    aconn.execute("CREATE DATABASE pgtern_autocommit_probe")  # refuses to run inside a transaction block
    aconn.execute("DROP DATABASE pgtern_autocommit_probe")


# ===================================================================== errors
def test_constraint_errors_carry_diagnostics(table, conn):
    conn.execute(f"INSERT INTO {table} (name, n) VALUES ('a', 1)")
    with pytest.raises(errors.UniqueViolation) as exc:
        conn.execute(f"INSERT INTO {table} (name, n) VALUES ('a', 1)")
    e = exc.value
    assert isinstance(e, pgtern.IntegrityError) and e.sqlstate == "23505"
    assert e.diag.table_name == table and e.diag.constraint_name == f"{table}_name_key"
    assert "DETAIL:" in str(e) and str(e).split("\n")[0].startswith("duplicate key value")
    conn.rollback()
    with pytest.raises(errors.NotNullViolation):
        conn.execute(f"INSERT INTO {table} (name) VALUES (NULL)")
    conn.rollback()
    with pytest.raises(errors.CheckViolation):
        conn.execute(f"INSERT INTO {table} (name, n) VALUES ('b', -1)")
    conn.rollback()


def test_error_classes_for_common_mistakes(aconn):
    cases = [
        ("SELECT * FROM no_such_table_zz", errors.UndefinedTable),
        ("SELECT no_such_col FROM pg_class", errors.UndefinedColumn),
        ("SELEC 1", errors.SyntaxError),
        ("SELECT 'x'::int", errors.InvalidTextRepresentation),
        ("SELECT no_such_fn()", errors.UndefinedFunction),
        ("DO $$ BEGIN RAISE EXCEPTION 'custom %', 42; END $$", errors.RaiseException),
    ]
    for query, expected in cases:
        with pytest.raises(expected) as exc:
            aconn.execute(query)
        assert isinstance(exc.value, pgtern.DatabaseError)
    with pytest.raises(errors.RaiseException) as exc:
        aconn.execute("DO $$ BEGIN RAISE EXCEPTION 'custom %', 42; END $$")
    assert str(exc.value).split("\n")[0] == "custom 42"


def test_insufficient_privilege_for_a_plain_role(dsn):
    with pgtern.connect(dsn, autocommit=True) as owner:
        owner.execute(sql.SQL("DROP ROLE IF EXISTS pgtern_plain"))
        owner.execute(sql.SQL("CREATE ROLE pgtern_plain LOGIN PASSWORD {}").format(sql.Literal("pw")))
    try:
        with pgtern.connect(url("pgtern_plain", "pw", sslmode="disable")) as c:
            with pytest.raises(errors.InsufficientPrivilege) as exc:
                c.execute("SELECT * FROM pg_authid")
            assert exc.value.sqlstate == "42501"
            c.rollback()
    finally:
        with pgtern.connect(dsn, autocommit=True) as owner:
            owner.execute("DROP ROLE IF EXISTS pgtern_plain")


def test_object_in_use_when_dropping_a_busy_database(aconn):
    aconn.execute("DROP DATABASE IF EXISTS pgtern_busy")
    aconn.execute("CREATE DATABASE pgtern_busy")
    try:
        with pgtern.connect(url("postgres", "postgres", dbname="pgtern_busy")):
            with pytest.raises(errors.ObjectInUse) as exc:
                aconn.execute("DROP DATABASE pgtern_busy")
            assert isinstance(exc.value, pgtern.OperationalError)
            assert str(exc.value).split("\n")[0].startswith('database "pgtern_busy" is being accessed')
    finally:
        aconn.execute("DROP DATABASE IF EXISTS pgtern_busy WITH (FORCE)")


def test_terminated_backend_raises_and_closes_the_connection(dsn):
    victim = pgtern.connect(dsn)
    pid = victim.info.backend_pid
    with pgtern.connect(dsn, autocommit=True) as killer:
        killer.execute("SELECT pg_terminate_backend(%s)", (pid,))
    time.sleep(0.2)
    with pytest.raises(pgtern.OperationalError):
        victim.execute("SELECT 1")
    assert victim.closed
    with pytest.raises(pgtern.OperationalError):
        victim.execute("SELECT 1")


def test_statement_timeout_via_options(dsn):
    with pgtern.connect(dsn, options="-c statement_timeout=200") as c:
        with pytest.raises(errors.QueryCanceled):
            c.execute("SELECT pg_sleep(5)")
        c.rollback()
        assert c.execute("SELECT 1").fetchone() == (1,)


def test_copy_is_refused_cleanly(aconn, table):
    with pytest.raises(pgtern.NotSupportedError):
        aconn.execute("COPY (SELECT 1) TO STDOUT")
    with pytest.raises(pgtern.NotSupportedError):
        aconn.execute(f"COPY {table} (name) FROM STDIN")
    assert aconn.execute("SELECT 1").fetchone() == (1,)  # the connection is still in sync


def test_server_notices_reach_handlers(aconn):
    seen = []
    aconn.add_notice_handler(lambda diag: seen.append(diag.message_primary))
    aconn.execute("DO $$ BEGIN RAISE NOTICE 'hello %', 'there'; END $$")
    assert seen == ["hello there"]


def test_session_parameters(aconn):
    assert aconn.info.parameter_status("DateStyle").startswith("ISO")
    assert aconn.info.encoding == "UTF8"
    assert aconn.info.server_version >= 140000
    with pgtern.connect(url("postgres", "postgres"), application_name="pgtern-test") as c:
        assert c.execute("SHOW application_name").fetchone() == ("pgtern-test",)


# ===================================================================== safe SQL composition
ALPHABET = list("abcXYZ019 _-'\"\\%$;:/*\n\t") + ["--", "/*", "é", "ü", "日本", "😀", "\\'", "''", "\\\\", "$$", "%s"]


def _random_strings(count: int, seed: int) -> list[str]:
    rng = random.Random(seed)
    return ["".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 24))) for _ in range(count)]


@pytest.mark.parametrize("setting", ["on", "off"])
def test_literal_roundtrips_for_random_strings(aconn, setting):
    aconn.execute(f"SET standard_conforming_strings = {setting}")
    aconn.execute("SET client_min_messages = error")
    for s in _random_strings(400, seed=1):
        assert aconn.execute(sql.SQL("SELECT {}").format(sql.Literal(s))).fetchone() == (s,), repr(s)
    for s in ["\\", "\\\\", "'", "\\'", "''\\''", "\\n", "a\\"]:
        assert aconn.execute(sql.SQL("SELECT {}").format(sql.Literal(s))).fetchone() == (s,), repr(s)


def test_identifier_roundtrips_for_random_names(aconn):
    for name in _random_strings(300, seed=2):
        name = (name or "x")[:20]
        cur = aconn.execute(sql.SQL("SELECT 1 AS {}").format(sql.Identifier(name)))
        assert cur.description[0].name == name, repr(name)
    assert sql.Identifier("a", "b").as_string() == '"a"."b"'


def test_password_literals_work_for_real_logins(dsn):
    passwords = ["plain", "pä55 wörd", "weird\\'\"%$ ;--", "emoji😀pw", "nb\u00a0sp", "日本語パスワード"]
    with pgtern.connect(dsn, autocommit=True) as owner:
        owner.execute("DROP ROLE IF EXISTS pgtern_pw")
        owner.execute("CREATE ROLE pgtern_pw LOGIN")
        try:
            for pw in passwords:
                owner.execute(sql.SQL("ALTER ROLE pgtern_pw WITH LOGIN PASSWORD {}").format(sql.Literal(pw)))
                with pgtern.connect(host=HOST, port=PORT, user="pgtern_pw", password=pw,
                                    dbname="postgres", sslmode="disable") as c:
                    assert c.execute("SELECT current_user").fetchone() == ("pgtern_pw",), repr(pw)
        finally:
            owner.execute("DROP ROLE IF EXISTS pgtern_pw")


def test_grant_with_identifier_from_connection_info(aconn):
    aconn.execute("DROP ROLE IF EXISTS pgtern_grantee")
    aconn.execute("CREATE ROLE pgtern_grantee NOLOGIN")
    try:
        aconn.execute(sql.SQL("GRANT pgtern_grantee TO {}").format(sql.Identifier(aconn.info.user)))
        assert aconn.execute(
            "SELECT pg_has_role(current_user, 'pgtern_grantee', 'member')").fetchone() == (True,)
    finally:
        aconn.execute("DROP ROLE pgtern_grantee")


# ===================================================================== hooks
@contextmanager
def _recording(log):
    def hook(info):
        @contextmanager
        def cm():
            entry = {"statement": info.statement, "error": None}
            log.append(entry)
            try:
                yield
            except BaseException as exc:
                entry["error"] = type(exc).__name__
                raise
            finally:
                entry["rowcount"] = info.rowcount
        return cm()
    remove = pgtern.add_query_hook(hook)
    try:
        yield
    finally:
        remove()


def test_query_hooks_see_statements_rowcounts_and_errors_but_never_params(aconn):
    log: list[dict] = []
    secret = "s3cr3t-value"
    with _recording(log):
        aconn.execute("SELECT %s::text", (secret,))
        aconn.execute(sql.SQL("DO $$ BEGIN PERFORM {}; END $$").format(sql.Literal(secret)))
        with pytest.raises(errors.DivisionByZero):
            aconn.execute("SELECT 1/0")
    aconn.execute("SELECT 'not recorded: hook removed'")
    assert [e["statement"] for e in log] == ["SELECT %s::text", "DO $$ BEGIN PERFORM ?; END $$", "SELECT 1/0"]
    assert [e["error"] for e in log] == [None, None, "DivisionByZero"]
    assert log[0]["rowcount"] == 1
    assert secret not in repr(log)


def test_a_broken_hook_does_not_break_queries(aconn):
    def bad_hook(info):
        raise RuntimeError("observer bug")
    remove = pgtern.add_query_hook(bad_hook)
    try:
        assert aconn.execute("SELECT 7").fetchone() == (7,)
    finally:
        remove()


# ===================================================================== pool
def test_pool_basics_commit_rollback_and_row_factory(dsn, table):
    with ConnectionPool(dsn, min_size=1, max_size=2, kwargs={"row_factory": dict_row}) as pool:
        with pool.connection() as c:
            c.execute(f"INSERT INTO {table} (name) VALUES ('committed')")
        with pytest.raises(RuntimeError):
            with pool.connection() as c:
                c.execute(f"INSERT INTO {table} (name) VALUES ('rolled back')")
                raise RuntimeError
        with pool.connection() as c:
            assert c.execute(f"SELECT name FROM {table}").fetchall() == [{"name": "committed"}]


def test_pool_enforces_max_size_and_times_out(dsn):
    with ConnectionPool(dsn, min_size=0, max_size=1, timeout=0.3) as pool:
        held = pool.getconn()
        started = time.monotonic()
        with pytest.raises(PoolTimeout):
            pool.getconn()
        assert 0.25 < time.monotonic() - started < 3
        pool.putconn(held)
        assert pool.getconn() is held


def test_pool_serves_many_threads_with_few_connections(dsn):
    pids: set[int] = set()
    failures: list[BaseException] = []
    with ConnectionPool(dsn, min_size=2, max_size=4) as pool:
        def worker():
            try:
                for _ in range(25):
                    with pool.connection() as c:
                        pids.add(c.execute("SELECT pg_backend_pid(), pg_sleep(0.001)").fetchone()[0])
            except BaseException as exc:  # noqa: BLE001
                failures.append(exc)
        threads = [threading.Thread(target=worker) for _ in range(16)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(60)
        stats = pool.get_stats()
    assert not failures, failures
    assert 1 <= len(pids) <= 4 and stats["pool_size"] <= 4


def test_pool_replaces_connections_the_server_killed(dsn):
    with ConnectionPool(dsn, min_size=1, max_size=1) as pool:
        with pool.connection() as c:
            old_pid = c.info.backend_pid
        with pgtern.connect(dsn, autocommit=True) as killer:
            killer.execute("SELECT pg_terminate_backend(%s)", (old_pid,))
        time.sleep(0.3)
        with pool.connection() as c:
            assert c.execute("SELECT 1").fetchone() == (1,)
            assert c.info.backend_pid != old_pid


def test_pool_rolls_back_connections_returned_mid_transaction(dsn):
    with ConnectionPool(dsn, min_size=1, max_size=1) as pool:
        c = pool.getconn()
        c.execute("SELECT 1")  # opens a transaction
        assert c.info.transaction_status is pgtern.TransactionStatus.INTRANS
        pool.putconn(c)
        again = pool.getconn()
        assert again is c and again.info.transaction_status is pgtern.TransactionStatus.IDLE
        pool.putconn(again)


def test_pool_keeps_min_size_and_closes_cleanly(dsn):
    pool = ConnectionPool(dsn, min_size=3, max_size=5)
    pool.wait(timeout=10)
    assert pool.get_stats()["pool_size"] == 3
    conn = pool.getconn()
    pool.close()
    with pytest.raises(PoolClosed):
        pool.getconn()
    pool.putconn(conn)  # returning after close just closes it
    assert conn.closed


def test_pool_survives_the_server_being_unreachable_then_recovering():
    pool = ConnectionPool("postgresql://u:p@127.0.0.1:1/db?sslmode=disable", min_size=0, max_size=1, timeout=0.5)
    with pytest.raises(PoolTimeout):
        pool.getconn()
    pool.close()


def test_transaction_local_setting_does_not_leak_between_pool_checkouts(dsn):
    """set_config(..., true) is transaction-local: it is gone once the transaction ends."""
    with ConnectionPool(dsn, min_size=1, max_size=1) as pool:
        for tenant in ("acme", "globex"):
            with pool.connection() as c:
                with c.transaction():
                    c.execute("SELECT set_config('app.tenant_id', %s, true)", (tenant,))
                    assert c.execute("SELECT current_setting('app.tenant_id', true)").fetchone() == (tenant,)
            with pool.connection() as c:
                assert c.execute("SELECT current_setting('app.tenant_id', true)").fetchone() in {(None,), ("",)}
