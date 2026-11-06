# SPDX-License-Identifier: Apache-2.0
"""Regression tests for the Low items of the external review (and the result-size cap and connect budget).

Tests marked ``integration`` need a server (PGTERN_TEST_DSN); the rest run anywhere.
"""
from __future__ import annotations

import asyncio
import socket
import struct
import time
from datetime import datetime, timezone

import pytest

import pgtern
from pgtern import _binary, _dsn, _protocol as P, _query, _scram, _types, adapters, errors, sql
from pgtern import aio


# ============================================================ no server needed

def test_binary_time_24_00_is_refused_not_turned_into_midnight():
    # PostgreSQL accepts 24:00:00; the binary decoder used to return 00:00 for it
    with pytest.raises(errors.DataError):
        _binary._dec_time_bin(struct.pack("!q", 86_400_000_000))
    assert _binary._dec_time_bin(struct.pack("!q", 0)).hour == 0


def test_float4_out_of_range_is_sent_as_text_for_the_server_to_judge():
    assert _binary.encode_scalar(1e300, _types.FLOAT4) is None
    assert _binary.encode_scalar(1.5, _types.FLOAT4) == struct.pack("!f", 1.5)


def test_int_dumper_does_not_capture_bool():
    adapters.register_dumper(int, lambda value: "99")
    try:
        assert _types.dump(True) == (_types.BOOL, b"t")
        assert _types.dump(False) == (_types.BOOL, b"f")
        assert _types.dump(5) == (0, b"99")  # the registered dumper still serves int
    finally:
        adapters.unregister_dumper(int)


def test_bare_percent_b_and_percent_t_are_refused():
    with pytest.raises(errors.ProgrammingError, match="only '%s'"):
        _query.translate("SELECT %b", [1])
    with pytest.raises(errors.ProgrammingError, match="only '%s'"):
        _query.translate("SELECT %t", [1])


def test_too_few_parameters_is_a_programming_error_not_an_index_error():
    with pytest.raises(errors.ProgrammingError, match="more placeholders"):
        _query.translate("SELECT %s, %s", [1])
    with pytest.raises(errors.ProgrammingError, match="more placeholders"):
        _query.translate("COPY t FROM STDIN WITH (x %s, %s)", [1], render=lambda v: repr(v))


def test_array_element_delimiter_comes_from_the_type():
    # box[] uses ';' between elements, and its elements contain ','
    assert _types.parse_array("{(1,1),(0,0);(2,2),(1,1)}", None, ";") == ["(1,1),(0,0)", "(2,2),(1,1)"]


def test_scram_server_nonce_must_strictly_extend_ours():
    client = _scram.ScramClient("pw", username="u", nonce="abc")
    with pytest.raises(errors.OperationalError, match="extend"):
        client.client_final(b"r=abc,s=c2FsdA==,i=4096")  # equal is not an extension
    client = _scram.ScramClient("pw", username="u", nonce="abc")
    with pytest.raises(errors.OperationalError, match="malformed"):
        client.client_final(b"r=abcdef,s=not base64!!,i=4096")


def test_connect_timeout_default_and_zero():
    assert _dsn.build_params("host=example.invalid").connect_timeout == 30.0
    assert _dsn.build_params("host=example.invalid connect_timeout=0").connect_timeout is None
    assert _dsn.build_params("host=example.invalid connect_timeout=2.5").connect_timeout == 2.5


def _data_row(size: int) -> bytes:
    return struct.pack("!H", 1) + struct.pack("!i", size) + b"x" * size


def test_result_size_cap_drops_rows_past_the_budget_and_reports_it():
    collector = P.QueryCollector(max_bytes=20)
    for _ in range(3):  # each row carries 8 bytes of data
        collector.handle(0x44, _data_row(8))
    collector.handle(0x43, b"SELECT 3\0")
    assert isinstance(collector.error, errors.DataError)
    assert "max_result_bytes" in str(collector.error)
    assert len(collector.results[0].rows) < 3


def test_silent_server_is_bounded_by_connect_timeout():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)  # accepts the TCP connection, then never answers the startup message
    port = srv.getsockname()[1]
    dsn = f"host=127.0.0.1 port={port} user=u dbname=d sslmode=disable connect_timeout=1"
    try:
        started = time.monotonic()
        with pytest.raises(errors.OperationalError):
            pgtern.connect(dsn)
        assert time.monotonic() - started < 5
    finally:
        srv.close()


def test_silent_server_is_bounded_by_connect_timeout_async():
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(("127.0.0.1", 0))
    srv.listen(1)
    port = srv.getsockname()[1]
    dsn = f"host=127.0.0.1 port={port} user=u dbname=d sslmode=disable connect_timeout=1"

    async def main():
        with pytest.raises(errors.OperationalError):
            await aio.connect(dsn)

    try:
        started = time.monotonic()
        asyncio.run(asyncio.wait_for(main(), 20))
        assert time.monotonic() - started < 8
    finally:
        srv.close()


# ============================================================ live server


@pytest.mark.integration
def test_binary_time_24_00_from_the_server_is_refused(dsn):
    with pgtern.connect(dsn) as conn:
        with pytest.raises(errors.DataError):
            conn.execute("SELECT '24:00:00'::time", binary=True).fetchall()
        assert conn.execute("SELECT 1").fetchone()[0] == 1


@pytest.mark.integration
def test_timestamptz_is_utc_in_text_and_binary_alike(dsn):
    with pgtern.connect(dsn) as conn:
        conn.execute("SET TimeZone TO 'Asia/Kolkata'")
        try:
            literal = "SELECT '2026-01-01 05:30:00+05:30'::timestamptz"
            text = conn.execute(literal).fetchone()[0]
            binary = conn.execute(literal, binary=True).fetchone()[0]
        finally:
            conn.execute("SET TimeZone TO 'UTC'")
    expected = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert text == binary == expected
    assert text.utcoffset() == binary.utcoffset() == timezone.utc.utcoffset(None)


@pytest.mark.integration
def test_float4_out_of_range_reaches_the_server_as_a_data_error(dsn):
    with pgtern.connect(dsn, autocommit=True) as conn:  # the server error must not abort a transaction
        with pytest.raises(errors.DataError):
            conn.execute("SELECT %s::float4", (1e300,), binary=True).fetchall()
        assert conn.execute("SELECT 2").fetchone()[0] == 2


@pytest.mark.integration
def test_bytes_literal_is_right_when_standard_conforming_strings_is_off(dsn):
    with pgtern.connect(dsn) as conn:
        conn.execute("SET standard_conforming_strings = off")
        try:
            value = conn.execute(sql.SQL("SELECT {}").format(sql.Literal(b"\x01\xff"))).fetchone()[0]
        finally:
            conn.execute("SET standard_conforming_strings = on")
    assert value == b"\x01\xff"


@pytest.mark.integration
def test_array_of_box_splits_on_semicolon_live(dsn):
    adapters.register_loader("box", lambda text: text)
    try:
        with pgtern.connect(dsn) as conn:
            value = conn.execute("SELECT ARRAY[box '((1,1),(0,0))', box '((2,2),(1,1))']").fetchone()[0]
    finally:
        adapters.unregister_loader("box")
    assert value == ["(1,1),(0,0)", "(2,2),(1,1)"]


@pytest.mark.integration
def test_ddl_clears_the_type_loader_cache(dsn):
    with pgtern.connect(dsn) as conn:
        conn._loader_cache[999_999] = lambda text: text
        conn.execute("CREATE TEMP TABLE low_items_ddl (n int)")
        assert 999_999 not in conn._loader_cache


@pytest.mark.integration
def test_result_size_cap_refuses_a_big_result_and_keeps_the_connection(dsn):
    with pgtern.connect(dsn) as conn:
        conn.max_result_bytes = 1000
        with pytest.raises(errors.DataError, match="max_result_bytes"):
            conn.execute("SELECT repeat('x', 5000)").fetchall()
        conn.max_result_bytes = None
        assert conn.execute("SELECT 3").fetchone()[0] == 3


@pytest.mark.integration
def test_result_size_cap_applies_in_async(dsn):
    async def main():
        async with await aio.connect(dsn) as conn:
            conn.max_result_bytes = 1000
            with pytest.raises(errors.DataError, match="max_result_bytes"):
                await conn.execute("SELECT repeat('x', 5000)")
            conn.max_result_bytes = None
            return (await conn.execute("SELECT 4")).fetchone()[0]

    assert asyncio.run(asyncio.wait_for(main(), 60)) == 4


@pytest.mark.integration
def test_prepare_true_is_refused_and_false_is_accepted(dsn):
    with pgtern.connect(dsn) as conn:
        with pytest.raises(errors.NotSupportedError, match="prepare"):
            conn.execute("SELECT 1", prepare=True)
        assert conn.execute("SELECT 1", prepare=False).fetchone()[0] == 1
