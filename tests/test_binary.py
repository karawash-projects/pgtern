# SPDX-License-Identifier: Apache-2.0
"""Binary parameter and result format. The text format is the oracle: every value must decode
to the same thing in both formats."""
from __future__ import annotations

import asyncio
import datetime as dt
import decimal
import math
import uuid
from contextlib import contextmanager

import pytest

import pgtern
from pgtern import adapters, aio, errors
from pgtern.types import Jsonb

pytestmark = pytest.mark.integration

TZ = dt.timezone(dt.timedelta(hours=-5))

CASES = [
    ("int2", "%s::int2", [0, 1, -1, 32767, -32768]),
    ("int4", "%s::int4", [0, 2**31 - 1, -(2**31)]),
    ("int8", "%s::int8", [0, 2**63 - 1, -(2**63), 2**40]),
    ("oid", "%s::oid", [0, 4294967295]),
    ("bool", "%s::bool", [True, False]),
    ("float8", "%s::float8", [0.0, -0.0, 1.5, 1e300, 1e-300, float("inf"), float("-inf")]),
    ("float4", "%s::float4", [0.0, 1.5, -2.25]),
    ("text", "%s::text", ["", "plain", "héllo ✓ 𝄞", "tab\tnl\n", "quote'\"\\"]),
    ("varchar", "%s::varchar", ["", "x"]),
    ("bpchar", "%s::char(4)", ["ab  "]),
    ("bytea", "%s::bytea", [b"", b"\x00\xff\x10", bytes(range(256))]),
    ("uuid", "%s::uuid", [uuid.UUID(int=0), uuid.UUID("12345678-1234-5678-1234-567812345678")]),
    ("date", "%s::date", [dt.date(1900, 1, 1), dt.date(2000, 1, 1), dt.date(2026, 11, 1), dt.date(9999, 12, 31)]),
    ("time", "%s::time", [dt.time(0, 0), dt.time(23, 59, 59, 999999), dt.time(7, 8, 9, 10)]),
    ("timestamp", "%s::timestamp", [dt.datetime(1999, 12, 31, 23, 59, 59, 999999), dt.datetime(2026, 1, 2, 3, 4, 5, 6)]),
    ("timestamptz", "%s::timestamptz", [dt.datetime(2026, 11, 1, 12, 30, 1, 5, tzinfo=TZ),
                                         dt.datetime(1970, 1, 1, tzinfo=dt.timezone.utc)]),
    ("numeric", "%s::numeric", [decimal.Decimal(x) for x in
                                ("0", "0.000", "1.50", "-12.34", "123456789012345678901234567890.12345",
                                 "0.0001", "-0.0012", "10000", "1e20", "-7.5e-7")]),
    ("interval", "%s::interval", [dt.timedelta(days=3), dt.timedelta(seconds=1, microseconds=5),
                                  dt.timedelta(days=-2, seconds=7)]),
    ("jsonb", "%s::jsonb", [Jsonb({"a": [1, 2.5, None, "x"], "b": {"c": True}}), Jsonb([])]),
    ("json", "%s::json", [Jsonb({"a": 1})]),
    ("int4[]", "%s::int4[]", [[1, None, 3], [], [-5]]),
    ("text[]", "%s::text[]", [["a", "b,c", "", None, 'q"uote']]),
    ("int8[] 2d", "%s::int8[]", [[[1, 2], [3, 4]]]),
    ("uuid[]", "%s::uuid[]", [[uuid.UUID(int=1), None]]),
    ("timestamptz[]", "%s::timestamptz[]", [[dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc), None]]),
    ("numeric[]", "%s::numeric[]", [[decimal.Decimal("1.5"), None]]),
]


def _f32(x):
    import struct
    return struct.unpack("!f", struct.pack("!f", x))[0]


def _norm(v):
    if isinstance(v, float) and math.isnan(v):
        return "nan"
    if isinstance(v, dt.datetime) and v.tzinfo is not None:
        return ("instant", v.astimezone(dt.timezone.utc))
    return v


@pytest.mark.parametrize("name,expr,values", CASES, ids=[c[0] for c in CASES])
def test_text_and_binary_agree(dsn, name, expr, values):
    with pgtern.connect(dsn, autocommit=True) as c:
        for value in values:
            q = f"SELECT {expr}"
            text = c.execute(q, [value]).fetchone()[0]
            binary = c.execute(q, [value], binary=True).fetchone()[0]
            if name == "float4":  # binary is the exact float4; text is the shortest repr, which can differ
                assert binary == _f32(value), (value, binary)
                continue
            assert _norm(binary) == _norm(text), (name, value, text, binary)


def test_nan_and_infinity_round_trip_binary(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        for v in (float("nan"), float("inf"), float("-inf")):
            got = c.execute("SELECT %s::float8", [v], binary=True).fetchone()[0]
            assert _norm(got) == _norm(v)
        assert c.execute("SELECT 'NaN'::numeric", binary=True).fetchone()[0].is_nan()


def test_null_in_params_and_results(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        assert c.execute("SELECT %s::int4, %s::text, %s::int4[]", [None, None, None], binary=True).fetchone() == (None, None, None)


def test_integers_out_of_range_for_server_type_error_like_text(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with pytest.raises(errors.NumericValueOutOfRange):
            c.execute("SELECT %s::int2", [40000], binary=True)
        with pytest.raises(errors.NumericValueOutOfRange):
            c.execute("SELECT %s::int2", [40000])
        assert c.execute("SELECT 1", binary=True).fetchone() == (1,)


def test_parameters_are_sent_binary_where_possible_and_text_otherwise(dsn):
    from pgtern import _types
    formats, payloads = _types.binary_param_plan([23, 25, 1700], [5, "s", decimal.Decimal("1.5")])
    assert formats == [1, 1, 0]  # int4 and text are binary; numeric has no binary encoder here, so it stays text
    assert payloads[0] == b"\x00\x00\x00\x05"


def test_text_fallback_for_unknown_python_types_still_works(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        assert c.execute("SELECT %s::text", [decimal.Decimal("2.5")], binary=True).fetchone() == ("2.5",)


def test_user_loader_wins_over_binary(dsn):
    from enum import Enum

    class Mood(Enum):
        HAPPY = "happy"

    name = "mood_" + uuid.uuid4().hex[:6]
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"CREATE TYPE {name} AS ENUM ('happy')")
        try:
            adapters.register_loader(name, adapters.enum_loader(Mood))
            assert c.execute(f"SELECT 'happy'::{name}", binary=True).fetchone()[0] is Mood.HAPPY
        finally:
            adapters._types._LOADERS.pop(name, None)
            c.execute(f"DROP TYPE {name}")


def test_binary_inside_transaction_and_hooks(dsn):
    seen = []

    @contextmanager
    def hook(info):
        seen.append(info.statement)
        yield

    remove = pgtern.add_query_hook(hook)
    try:
        with pgtern.connect(dsn) as c:
            with c.transaction():
                assert c.execute("SELECT %s::int4", [7], binary=True).fetchone() == (7,)
                assert c.info.transaction_status.name == "INTRANS"
            assert c.info.transaction_status.name == "IDLE"
    finally:
        remove()
    assert any("SELECT" in s for s in seen)


def test_binary_error_leaves_connection_usable(dsn):
    with pgtern.connect(dsn) as c:
        with pytest.raises(errors.UndefinedTable):
            c.execute("SELECT * FROM no_such_table_zz WHERE id = %s", [1], binary=True)
        c.rollback()
        assert c.execute("SELECT %s::int4", [2], binary=True).fetchone() == (2,)
        with pytest.raises(errors.SyntaxError):
            c.execute("SELEC %s", [1], binary=True)
        c.rollback()
        assert c.execute("SELECT 3", binary=True).fetchone() == (3,)


def test_binary_with_multi_statement_script_is_refused_clearly(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        with pytest.raises(errors.Error):
            c.execute("SELECT 1; SELECT 2", binary=True)
        assert c.execute("SELECT 4").fetchone() == (4,)


def test_binary_describe_does_not_run_side_effects(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute("CREATE TEMP TABLE bd (v int)")
        c.execute("INSERT INTO bd VALUES (%s)", [1], binary=True)
        assert c.execute("SELECT count(*) FROM bd").fetchone() == (1,)


def test_binary_server_cursor(dsn):
    with pgtern.connect(dsn) as c:
        with c.cursor("bc") as cur:
            cur.itersize = 2
            cur.execute("SELECT g, g::float8 / 4 FROM generate_series(1, 5) g", binary=True)
            assert cur.fetchall() == [(1, 0.25), (2, 0.5), (3, 0.75), (4, 1.0), (5, 1.25)]
        c.rollback()


def test_async_binary(dsn):
    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            cur = await c.execute("SELECT %s::int8, %s::timestamptz, %s::numeric, %s::uuid[]",
                                  [2**50, dt.datetime(2026, 5, 5, tzinfo=dt.timezone.utc),
                                   decimal.Decimal("9.99"), [uuid.UUID(int=9)]], binary=True)
            row = cur.fetchone()
            assert row[0] == 2**50 and row[2] == decimal.Decimal("9.99") and row[3] == [uuid.UUID(int=9)]
            assert row[1] == dt.datetime(2026, 5, 5, tzinfo=dt.timezone.utc)
            with pytest.raises(errors.UndefinedTable):
                await c.execute("SELECT * FROM nope_zz", binary=True)
            assert (await c.execute("SELECT 1", binary=True)).fetchone() == (1,)
    asyncio.run(asyncio.wait_for(go(), 30))


def test_pool_with_binary(dsn):
    with pgtern.ConnectionPool(dsn, min_size=1, max_size=2) as pool:
        with pool.connection() as c:
            assert c.execute("SELECT %s::int8", [2**33], binary=True).fetchone() == (2**33,)


def test_randomized_server_generated_values_agree(dsn):
    """400 server-generated rows (random scales, magnitudes, months, microseconds): text == binary."""
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(
            """CREATE TEMP TABLE rv AS SELECT
                 round(((random() - 0.5) * 10 ^ (n % 30))::numeric, n % 13) AS a,
                 (random() * 4000 - 2000) * interval '1 day'
                   + make_interval(months => (n % 40) - 20) + (n % 7) * interval '1 microsecond' AS b,
                 timestamp '1990-01-01' + (random() * 1e9)::int * interval '1 second' + n * interval '1 microsecond' AS c,
                 timestamptz '1950-01-01' + (random() * 1e9)::int * interval '1 second' AS d
               FROM generate_series(1, 400) n"""
        )
        text = c.execute("SELECT a, b, c, d FROM rv ORDER BY a, b, c, d").fetchall()
        binary = c.execute("SELECT a, b, c, d FROM rv ORDER BY a, b, c, d", binary=True).fetchall()
    assert len(text) == len(binary) == 400
    for t, b in zip(text, binary):
        assert t[0] == b[0], (t[0], b[0])                       # numeric, exact
        assert t[1] == b[1], (t[1], b[1])                       # interval
        assert t[2] == b[2], (t[2], b[2])                       # timestamp
        assert _norm(t[3]) == _norm(b[3]), (t[3], b[3])         # timestamptz, same instant


def test_text_interval_with_mixed_signs_regression():
    from pgtern._types import _dec_interval
    got = _dec_interval("-11 mons +785 days 23:26:23.969766")
    assert got == dt.timedelta(days=-330 + 785, hours=23, minutes=26, seconds=23, microseconds=969766)
