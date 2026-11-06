# SPDX-License-Identifier: Apache-2.0
"""Unit tests: everything that needs no database."""
from __future__ import annotations

import base64
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from uuid import UUID

import pytest

from pgtern import _dsn, _protocol as P, _query, _scram, _types, errors, sql
from pgtern.types import Jsonb


# ------------------------------------------------------------------ SCRAM
def test_scram_rfc7677_vector():
    c = _scram.ScramClient("pencil", username="user", nonce="rOprNGfwEbeRWgbNEkqO")
    assert c.client_first() == b"n,,n=user,r=rOprNGfwEbeRWgbNEkqO"
    server_first = (b"r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
                    b"s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096")
    assert c.client_final(server_first) == (
        b"c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,"
        b"p=dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
    c.verify_server_final(b"v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
    with pytest.raises(errors.OperationalError):
        c.verify_server_final(b"v=" + base64.b64encode(b"x" * 32))


def test_scram_rejects_bad_server_messages():
    c = _scram.ScramClient("pw", nonce="abc")
    with pytest.raises(errors.OperationalError):
        c.client_final(b"r=zzz,s=AAAA,i=4096")  # nonce does not extend ours
    with pytest.raises(errors.OperationalError):
        c.client_final(b"r=abcdef,s=AAAA,i=999999999")  # absurd iteration count
    with pytest.raises(errors.OperationalError):
        c.client_final(b"e=invalid-proof")


# ------------------------------------------------------------------ protocol framing
def test_message_parser_handles_arbitrary_chunking():
    stream = (P._msg(b"S", b"a\0b\0") + P._msg(b"Z", b"I") + P._msg(b"C", b"SELECT 1\0"))
    parser = P.MessageParser()
    seen = []
    for i in range(len(stream)):
        seen += parser.feed(stream[i:i + 1])
    assert seen == [(ord("S"), b"a\0b\0"), (ord("Z"), b"I"), (ord("C"), b"SELECT 1\0")]
    assert P.MessageParser().feed(stream) == seen


def test_message_parser_rejects_garbage_length():
    with pytest.raises(errors.InterfaceError):
        P.MessageParser().feed(b"Z\x00\x00\x00\x01x")


def test_extended_query_layout_and_param_limit():
    data = P.extended_query("select $1", [0], [b"x"])
    assert data.startswith(b"P") and data.endswith(b"S\x00\x00\x00\x04")
    with pytest.raises(errors.ProgrammingError):
        P.extended_query("select 1", [], [b"x"] * 70000)
    with pytest.raises(errors.ProgrammingError):
        P.query_message("select 1\0")


def test_row_description_and_data_row_roundtrip():
    desc = (b"\x00\x02" + b"id\0" + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00"
            + b"name\0" + b"\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\xff\xff\xff\xff\xff\xff\x00\x00")
    cols = P.parse_row_description(desc)
    assert [(c.name, c.type_code) for c in cols] == [("id", 23), ("name", 25)]
    assert P.parse_data_row(b"\x00\x03\x00\x00\x00\x011\xff\xff\xff\xff\x00\x00\x00\x02hi") == [b"1", None, b"hi"]


def test_rowcount_from_command_tag():
    assert P._rowcount("INSERT 0 3") == 3
    assert P._rowcount("UPDATE 7") == 7
    assert P._rowcount("SELECT 2") == 2
    assert P._rowcount("CREATE TABLE") == -1
    assert P._rowcount("BEGIN") == -1


# ------------------------------------------------------------------ DSN
def test_url_with_multiple_hosts_and_escapes():
    p = _dsn.build_params("postgresql://u:p%40ss@h1:5433,h2/db?sslmode=verify-full&sslrootcert=system&connect_timeout=5")
    assert p.hosts == [("h1", 5433), ("h2", 5432)]
    assert (p.user, p.password, p.dbname, p.sslmode, p.connect_timeout) == ("u", "p@ss", "db", "verify-full", 5.0)


def test_url_ipv6_and_unix_socket_host():
    assert _dsn.build_params("postgresql://[::1]:5555/db").hosts == [("::1", 5555)]
    assert _dsn.build_params("postgresql:///db?host=/tmp&port=5433").hosts == [("/tmp", 5433)]


def test_keyword_string_with_quotes():
    p = _dsn.build_params(r"host=/var/run/postgresql port=5433 dbname='my db' password='a\'b' user=bob")
    assert p.hosts == [("/var/run/postgresql", 5433)]
    assert (p.dbname, p.password, p.user) == ("my db", "a'b", "bob")


def test_kwargs_override_and_env_defaults(monkeypatch):
    monkeypatch.setenv("PGUSER", "envuser")
    monkeypatch.setenv("PGPASSWORD", "envpw")
    monkeypatch.setenv("PGSSLMODE", "require")
    p = _dsn.build_params("postgresql://h/db", user="kw")
    assert (p.user, p.password, p.sslmode, p.dbname) == ("kw", "envpw", "require", "db")


def test_bad_options_are_rejected():
    with pytest.raises(errors.ProgrammingError):
        _dsn.build_params("postgresql://h/db?sslmode=bogus")
    with pytest.raises(errors.ProgrammingError):
        _dsn.build_params("postgresql://h/db", frobnicate="1")
    with pytest.raises(errors.ProgrammingError):
        _dsn.build_params("mysql://h/db")


def test_zero_connect_timeout_means_wait_forever():
    assert _dsn.build_params("postgresql://h/db?connect_timeout=0").connect_timeout is None


# ------------------------------------------------------------------ placeholders
def test_translate_positional_and_percent_escape():
    assert _query.translate("a=%s AND b=%s::int AND c LIKE '100%%'", (1, "2")) == (
        "a=$1 AND b=$2::int AND c LIKE '100%'", [1, "2"])


def test_translate_named_reuses_parameter_numbers():
    sql_text, values = _query.translate("%(a)s + %(b)s + %(a)s", {"a": 1, "b": 2})
    assert (sql_text, values) == ("$1 + $2 + $1", [1, 2])


@pytest.mark.parametrize("query, params", [
    ("select %s", ()),            # too few
    ("select 1", (1,)),           # too many
    ("select %d", (1,)),          # unsupported placeholder
    ("select 100%", ()),          # trailing percent
    ("select %(a)s", (1,)),       # named placeholder, sequence params
    ("select %s", {"a": 1}),      # positional placeholder, mapping params
    ("select %(a)s", {"b": 1}),   # missing name
    ("select %s", "x"),           # string is not a parameter sequence
])
def test_translate_errors(query, params):
    with pytest.raises(errors.ProgrammingError):
        _query.translate(query, params)


# ------------------------------------------------------------------ dumping
def test_dump_scalars():
    assert _types.dump(None) == (0, None)
    assert _types.dump(True) == (16, b"t")
    assert _types.dump("x") == (0, b"x")
    assert _types.dump(5) == (23, b"5")  # int4: int2 would overflow in $1 * $2
    assert _types.dump(40000) == (23, b"40000")
    assert _types.dump(2**40) == (20, str(2**40).encode())
    assert _types.dump(2**70)[0] == 1700
    assert _types.dump(1.5) == (701, b"1.5")
    assert _types.dump(float("inf")) == (701, b"Infinity")
    assert _types.dump(Decimal("1.50")) == (1700, b"1.50")
    assert _types.dump(b"\x01\xff") == (17, b"\\x01ff")
    assert _types.dump(UUID(int=1))[0] == 2950
    assert _types.dump(Jsonb({"a": 1})) == (3802, b'{"a": 1}')
    assert _types.dump(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc))[0] == 1184
    assert _types.dump(datetime(2026, 1, 2, 3, 4, 5))[0] == 1114
    assert _types.dump(date(2026, 1, 2)) == (1082, b"2026-01-02")


def test_dump_rejects_bad_values():
    with pytest.raises(errors.DataError):
        _types.dump("a\x00b")
    with pytest.raises(errors.ProgrammingError):
        _types.dump({"a": 1})
    with pytest.raises(errors.ProgrammingError):
        _types.dump(object())


def test_dump_arrays():
    assert _types.dump(["a", "b"]) == (0, b'{"a","b"}')
    assert _types.dump([1, 2]) == (1007, b"{1,2}")
    assert _types.dump([1, 70000])[0] == 1007
    assert _types.dump([]) == (0, b"{}")
    assert _types.dump(['a"b\\c', None]) == (0, b'{"a\\"b\\\\c",NULL}')
    assert _types.dump([[1, 2], [3, 4]]) == (1007, b"{{1,2},{3,4}}")
    assert _types.dump([True, False]) == (1000, b"{t,f}")
    assert _types.dump([Jsonb({"k": "v"})])[0] == 3807
    with pytest.raises(errors.ProgrammingError):
        _types.dump([1, "a"])


# ------------------------------------------------------------------ decoding
def dec(oid: int, text: str):
    return _types.load_row([text.encode()], _types.make_loaders([oid]))[0]


def test_decode_scalars():
    assert dec(16, "t") is True and dec(16, "f") is False
    assert dec(20, "-9") == -9 and dec(23, "7") == 7
    assert dec(1700, "1.50") == Decimal("1.50")
    assert dec(701, "Infinity") == float("inf")
    assert dec(2950, "00000000-0000-0000-0000-000000000001") == UUID(int=1)
    assert dec(3802, '{"a": [1, 2]}') == {"a": [1, 2]}
    assert dec(17, "\\xdeadbeef") == b"\xde\xad\xbe\xef"
    assert dec(25, "héllo") == "héllo"
    assert dec(99999, "whatever") == "whatever"  # unknown OIDs come back as text


def test_decode_dates_and_times():
    assert dec(1082, "2026-11-01") == date(2026, 11, 1)
    assert dec(1114, "2026-11-01 14:06:05.5") == datetime(2026, 11, 1, 14, 6, 5, 500000)
    assert dec(1184, "2026-11-01 14:06:05.123456+00") == datetime(2026, 11, 1, 14, 6, 5, 123456, tzinfo=timezone.utc)
    aware = dec(1184, "2026-11-01 14:06:05-05:30")
    assert aware == datetime(2026, 11, 1, 19, 36, 5, tzinfo=timezone.utc)  # UTC, as the binary format returns it
    assert aware.utcoffset() == timedelta(0)
    assert dec(1083, "14:06:05") == time(14, 6, 5)
    assert dec(1266, "14:06:05+02").utcoffset() == timedelta(hours=2)
    assert dec(1186, "1 year 2 mons 3 days 04:05:06.5") == _types.Interval(
        14, timedelta(days=3, hours=4, minutes=5, seconds=6.5))
    assert dec(1186, "-1 days +02:03:04") == timedelta(days=-1, hours=2, minutes=3, seconds=4)
    assert dec(1186, "00:00:01") == timedelta(seconds=1)


@pytest.mark.parametrize("oid, text", [
    (1082, "infinity"), (1082, "0044-03-15 BC"), (1082, "12345-01-01"),
    (1184, "-infinity"), (1184, "2026-11-01 14:06:05+00 BC"),
])
def test_decode_unrepresentable_dates_raise_dataerror(oid, text):
    with pytest.raises(errors.DataError):
        dec(oid, text)


def test_decode_arrays():
    assert dec(1009, '{a,"b c",NULL,"d\\"e"}') == ["a", "b c", None, 'd"e']
    assert dec(1009, '{"NULL",NULL}') == ["NULL", None]
    assert dec(1007, "{1,2,3}") == [1, 2, 3]
    assert dec(1007, "{{1,2},{3,4}}") == [[1, 2], [3, 4]]
    assert dec(1009, "{}") == []
    assert dec(1009, "[0:1]={x,y}") == ["x", "y"]
    assert dec(3807, '{"{\\"a\\": 1}"}') == [{"a": 1}]
    assert dec(1185, '{"2026-11-01 14:06:05+00"}') == [datetime(2026, 11, 1, 14, 6, 5, tzinfo=timezone.utc)]


def test_dates_refused_when_datestyle_is_not_iso():
    loaders = _types.make_loaders([1082], dates_ok=False)
    with pytest.raises(errors.NotSupportedError):
        _types.load_row([b"01/02/2026"], loaders)


# ------------------------------------------------------------------ errors
def test_error_classes_follow_sqlstate():
    e = errors.error_from_fields({"C": "42501", "M": "permission denied for table x", "S": "ERROR"})
    assert isinstance(e, errors.InsufficientPrivilege) and isinstance(e, errors.ProgrammingError)
    assert e.sqlstate == "42501" and e.diag.message_primary == "permission denied for table x"
    e = errors.error_from_fields({"C": "55006", "M": "tenant is active", "D": "still in use"})
    assert isinstance(e, errors.ObjectInUse) and isinstance(e, errors.OperationalError)
    assert str(e) == "tenant is active\nDETAIL:  still in use"
    assert str(e).split("\n")[0] == "tenant is active"  # the first line is the message alone
    assert isinstance(errors.error_from_fields({"C": "P0001", "M": "x"}), errors.RaiseException)
    assert isinstance(errors.error_from_fields({"C": "23505", "M": "x"}), errors.IntegrityError)


def test_unknown_sqlstates_fall_back_to_their_dbapi_class():
    e = errors.error_from_fields({"C": "42ZZZ", "M": "x"})
    assert type(e) is errors.ProgrammingError and e.sqlstate == "42ZZZ"
    assert type(errors.error_from_fields({"C": "99999", "M": "x"})) is errors.DatabaseError


def test_hierarchy_is_flat():
    assert errors.UniqueViolation.__bases__ == (errors.IntegrityError,)
    assert errors.RaiseException.__bases__ == (errors.ProgrammingError,)
    assert errors.ObjectInUse.__bases__ == (errors.OperationalError,)
    assert not issubclass(errors.UniqueViolation, errors.IntegrityConstraintViolation)


# ------------------------------------------------------------------ sql composition
def test_quote_literal_and_identifier():
    assert sql.quote_literal("it's") == "'it''s'"
    assert sql.quote_literal("a\\b") == " E'a\\\\b'"
    assert sql.quote_literal("a\\'b") == " E'a\\\\''b'"
    assert sql.quote_ident('we"ird') == '"we""ird"'
    with pytest.raises(errors.DataError):
        sql.quote_literal("a\x00b")


def test_composition_and_redaction():
    q = sql.SQL("ALTER ROLE {role} WITH LOGIN PASSWORD {pw}").format(
        role=sql.Identifier("app_role"), pw=sql.Literal("s3cr'et"))
    assert q.as_string() == """ALTER ROLE "app_role" WITH LOGIN PASSWORD 's3cr''et'"""
    assert "s3cr" not in q.as_redacted()
    assert q.as_redacted() == 'ALTER ROLE "app_role" WITH LOGIN PASSWORD ?'
    assert sql.SQL("GRANT {} TO {}").format(sql.Identifier("a"), sql.Identifier("b")).as_string() == 'GRANT "a" TO "b"'
    assert sql.SQL(", ").join([sql.Identifier("a"), sql.Identifier("b")]).as_string() == '"a", "b"'
    assert sql.Identifier("s", "t").as_string() == '"s"."t"'
    assert sql.Placeholder().as_string() == "%s" and sql.Placeholder("n").as_string() == "%(n)s"
    with pytest.raises(TypeError):
        sql.SQL("select {}").format("not composable")


def test_literal_values():
    L = lambda v: sql.Literal(v).as_string()  # noqa: E731
    assert L(None) == "NULL" and L(True) == "true" and L(5) == "5" and L(-5) == " -5"
    assert L("x") == "'x'" and L(b"\x01") == "decode('01', 'hex')"
    assert L([1, 2]) == "ARRAY[1,2]" and L([]) == "'{}'"
    assert L(float("nan")) == "'NaN'::float8"
