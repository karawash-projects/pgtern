# SPDX-License-Identifier: Apache-2.0
"""Custom type adapters. The server is the oracle for the text format of records and ranges."""
from __future__ import annotations

import asyncio
import enum
import random
import string
import uuid
from dataclasses import dataclass
from decimal import Decimal

import pytest

import pgtern
from pgtern import adapters, aio, errors
from pgtern._composite import format_record, parse_record

pytestmark = pytest.mark.integration

ALPHABET = string.ascii_letters + " ,()\"\\\t\n'é" + "{}[]:"


@pytest.fixture(autouse=True)
def clean_registry():
    yield
    from pgtern import _types
    _types._LOADERS.clear()
    _types._DUMPERS.clear()


@dataclass
class Address:
    street: str
    no: int
    note: str | None


class Mood(enum.Enum):
    SAD = "sad"
    HAPPY = "happy"


@pytest.fixture
def types(dsn):
    name = "t_" + uuid.uuid4().hex[:8]
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"CREATE TYPE {name}_mood AS ENUM ('sad', 'happy')")
        c.execute(f"CREATE TYPE {name}_addr AS (street text, no int, note text)")
        c.execute(f"CREATE TYPE {name}_span AS RANGE (subtype = int4)")
        c.execute(f"CREATE TABLE {name}_t (m {name}_mood, a {name}_addr, s {name}_span, arr {name}_addr[])")
        yield name
        c.execute(f"DROP TABLE {name}_t; DROP TYPE {name}_addr; DROP TYPE {name}_mood; DROP TYPE {name}_span")


# ---------------------------------------------------------------- text format, checked against the server
def _server_record(conn, values):
    cols = ", ".join("%s::text" for _ in values)
    return conn.execute(f"SELECT ROW({cols})::text", list(values)).fetchone()[0]


def test_record_parser_matches_server_on_random_values(dsn):
    rng = random.Random(7)
    with pgtern.connect(dsn, autocommit=True) as c:
        for _ in range(300):
            values = [None if rng.random() < 0.2 else
                      "".join(rng.choice(ALPHABET) for _ in range(rng.randint(0, 6)))
                      for _ in range(rng.randint(1, 4))]
            text = _server_record(c, [v if v is not None else None for v in values]) if all(
                v is not None for v in values) else c.execute(
                "SELECT ROW(" + ", ".join("NULL::text" if v is None else "%s::text" for v in values) + ")::text",
                [v for v in values if v is not None]).fetchone()[0]
            assert parse_record(text) == values, text
            assert format_record(values) == text or parse_record(format_record(values)) == values


def test_format_record_round_trips_through_server(dsn):
    values = ["a,b", "(x)", 'q"uote', "back\\slash", "", None, " lead", "plain"]
    with pgtern.connect(dsn, autocommit=True) as c:
        out = c.execute("SELECT %s::text", [format_record(values)]).fetchone()[0]
        assert parse_record(out) == values
        assert c.execute("SELECT ROW(%s::text, %s::text)::text", ["x", None]).fetchone()[0] == "(x,)"


def test_parse_record_edge_cases():
    assert parse_record("()") == [None]
    assert parse_record("(,)") == [None, None]
    assert parse_record('("",x)') == ["", "x"]
    assert parse_record("(a,)") == ["a", None]
    with pytest.raises(errors.DataError):
        parse_record("nope")


# ---------------------------------------------------------------- loaders and dumpers
def test_enum_and_composite_loaders(dsn, types):
    adapters.register_loader(f"{types}_mood", adapters.enum_loader(Mood))
    adapters.register_loader(f"{types}_addr", adapters.record_loader(Address, (str, int, str)))
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"INSERT INTO {types}_t (m, a, arr) VALUES ('happy', ROW('Main, 5', 5, NULL), "
                  f"ARRAY[ROW('a',1,'x'), ROW('b',2,NULL)]::{types}_addr[])")
        m, a, arr = c.execute(f"SELECT m, a, arr FROM {types}_t").fetchone()
        assert m is Mood.HAPPY
        assert a == Address("Main, 5", 5, None)
        assert arr == [Address("a", 1, "x"), Address("b", 2, None)]


def test_loader_applies_to_schema_qualified_names_and_null(dsn, types):
    adapters.register_loader(f"public.{types}_mood", adapters.enum_loader(Mood))
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"INSERT INTO {types}_t (m) VALUES ('happy')")
        c.execute(f"INSERT INTO {types}_t (m) VALUES (NULL)")
        assert [r[0] for r in c.execute(f"SELECT m FROM {types}_t ORDER BY m NULLS LAST")] == [Mood.HAPPY, None]


def test_no_loader_means_text_as_before(dsn, types):
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"INSERT INTO {types}_t (m, a) VALUES ('sad', ROW('s', 1, 'n'))")
        assert c.execute(f"SELECT m, a FROM {types}_t").fetchone() == ("sad", "(s,1,n)")


def test_range_loader_and_dumper(dsn, types):
    adapters.register_loader(f"{types}_span", adapters.range_loader(int))
    adapters.register_dumper(adapters.Range, adapters.range_dumper())
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"INSERT INTO {types}_t (s) VALUES (%s)", [adapters.Range(1, 10, True, False)])
        got = c.execute(f"SELECT s FROM {types}_t").fetchone()[0]
        assert got == adapters.Range(1, 10, True, False)
        c.execute(f"INSERT INTO {types}_t (s) VALUES (%s)", [adapters.Range(empty=True)])
        assert c.execute(f"SELECT s FROM {types}_t WHERE isempty(s)").fetchone()[0].empty
        c.execute(f"INSERT INTO {types}_t (s) VALUES (%s)", [adapters.Range(None, 4, False, True)])
        unbounded = c.execute(f"SELECT s FROM {types}_t WHERE upper(s) = 4").fetchone()[0]
        assert unbounded == adapters.Range(None, 4, False, True)


def test_dumper_for_custom_class_and_subclass(dsn):
    @dataclass
    class Money:
        cents: int

    class Euro(Money):
        pass

    adapters.register_dumper(Money, lambda m: f"{m.cents / 100:.2f}")
    with pgtern.connect(dsn, autocommit=True) as c:
        assert c.execute("SELECT %s::numeric", [Money(1250)]).fetchone() == (12.5,) or \
            c.execute("SELECT %s::numeric", [Money(1250)]).fetchone()[0] == Decimal("12.50")
        assert c.execute("SELECT %s::text", [Euro(5)]).fetchone() == ("0.05",)
        assert c.execute("SELECT %s::text", [None]).fetchone() == (None,)
    adapters.unregister_dumper(Money)
    with pgtern.connect(dsn, autocommit=True) as c:
        with pytest.raises(errors.Error):
            c.execute("SELECT %s::text", [Money(1)])


def test_record_dumper_round_trip(dsn, types):
    adapters.register_loader(f"{types}_addr", adapters.record_loader(Address, (str, int, str)))
    adapters.register_dumper(Address, adapters.record_dumper(lambda a: (a.street, a.no, a.note)))
    with pgtern.connect(dsn, autocommit=True) as c:
        addr = Address("Oak, \"Lane\"", 7, None)
        c.execute(f"INSERT INTO {types}_t (a) VALUES (%s)", [addr])
        assert c.execute(f"SELECT a FROM {types}_t").fetchone()[0] == addr


def test_loaders_resolved_once_per_connection(dsn, types):
    adapters.register_loader(f"{types}_mood", adapters.enum_loader(Mood))
    seen = []
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"INSERT INTO {types}_t (m) VALUES ('sad')")
        for _ in range(3):
            seen.append(c.execute(f"SELECT m FROM {types}_t").fetchone()[0])
        assert seen == [Mood.SAD] * 3
        assert len(c._loader_cache) >= 1


def test_loader_errors_propagate_and_connection_survives(dsn, types):
    def bad(text):
        raise ValueError(f"no such mood {text}")
    adapters.register_loader(f"{types}_mood", bad)
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"INSERT INTO {types}_t (m) VALUES ('sad')")
        with pytest.raises(ValueError):
            c.execute(f"SELECT m FROM {types}_t").fetchall()
        adapters.unregister_loader(f"{types}_mood")
        c._loader_cache.clear()
        assert c.execute(f"SELECT m FROM {types}_t").fetchone() == ("sad",)


def test_async_adapters(dsn, types):
    adapters.register_loader(f"{types}_mood", adapters.enum_loader(Mood))
    adapters.register_loader(f"{types}_addr", adapters.record_loader(Address, (str, int, str)))

    async def go():
        async with await aio.connect(dsn, autocommit=True) as c:
            await c.execute(f"INSERT INTO {types}_t (m, a) VALUES ('happy', ROW('z', 9, 'q'))")
            cur = await c.execute(f"SELECT m, a FROM {types}_t")
            assert cur.fetchone() == (Mood.HAPPY, Address("z", 9, "q"))
    asyncio.run(asyncio.wait_for(go(), 30))


def test_adapters_with_pool(dsn, types):
    adapters.register_loader(f"{types}_mood", adapters.enum_loader(Mood))
    with pgtern.ConnectionPool(dsn, min_size=1, max_size=2) as pool:
        with pool.connection() as c:
            c.execute(f"INSERT INTO {types}_t (m) VALUES ('sad')")
        with pool.connection() as c:
            assert c.execute(f"SELECT m FROM {types}_t").fetchone()[0] is Mood.SAD
