# SPDX-License-Identifier: Apache-2.0
"""Binary wire format for the common types (opt-in with ``binary=True``).

Decoders must agree with the text decoders in ``_types`` for every value both support:
tests check this against the server's own text output. Types not listed here keep using text.
"""
from __future__ import annotations

import datetime as _dt
import json
import struct
import uuid
from decimal import Decimal
from typing import Any, Callable

from . import errors
from ._types import (
    BOOL, BPCHAR, BYTEA, CHAR, DATE, ELEMENT_OF, FLOAT4, FLOAT8, INT2, INT4, INT8, INTERVAL, JSON,
    JSONB, NAME, NUMERIC, OID, TEXT, TIME, TIMESTAMP, TIMESTAMPTZ, UUID, VARCHAR, ARRAY_OF,
    Interval, make_interval,
)

_EPOCH = _dt.datetime(2000, 1, 1)
_EPOCH_DATE = _dt.date(2000, 1, 1)
_EPOCH_UTC = _dt.datetime(2000, 1, 1, tzinfo=_dt.timezone.utc)
_I64_MAX = 2**63 - 1
_I64_MIN = -(2**63)

_s_i16 = struct.Struct("!h")
_s_u16 = struct.Struct("!H")
_s_i32 = struct.Struct("!i")
_s_u32 = struct.Struct("!I")
_s_i64 = struct.Struct("!q")
_s_f32 = struct.Struct("!f")
_s_f64 = struct.Struct("!d")


# ---------------------------------------------------------------- decoders (bytes -> Python)
def _dec_numeric(b: bytes) -> Decimal:
    ndigits, weight, sign, dscale = struct.unpack_from("!hhHh", b, 0)
    if sign == 0xC000:
        return Decimal("NaN")
    if sign in (0xD000, 0xF000):
        return Decimal("Infinity") if sign == 0xD000 else Decimal("-Infinity")
    digits = [_s_i16.unpack_from(b, 8 + 2 * i)[0] for i in range(ndigits)]
    n = 0
    lowest = weight - (ndigits - 1) if ndigits else 0
    for i, d in enumerate(digits):
        n += d * (10000 ** (weight - i - lowest))  # base-10000 groups; digit i is worth 10000**(weight-i)
    # value = n * 10**(4*lowest); exact decimal with exactly dscale fraction digits (no context limits)
    shift = 4 * lowest + dscale
    coefficient = n * 10 ** shift if shift >= 0 else n // 10 ** (-shift)
    text = str(coefficient)
    return Decimal((1 if sign == 0x4000 else 0, tuple(int(c) for c in text), -dscale))


def _dec_interval(b: bytes) -> Any:
    usec, days, months = struct.unpack("!qii", b)
    return make_interval(months, _dt.timedelta(days=days, microseconds=usec))


def _dec_timestamp_bin(b: bytes) -> _dt.datetime:
    (usec,) = _s_i64.unpack(b)
    _check_finite(usec)
    return _EPOCH + _dt.timedelta(microseconds=usec)


def _dec_timestamptz_bin(b: bytes) -> _dt.datetime:
    (usec,) = _s_i64.unpack(b)
    _check_finite(usec)
    return (_EPOCH_UTC + _dt.timedelta(microseconds=usec)).astimezone(_dt.timezone.utc)


def _check_finite(usec: int) -> None:
    if usec in (_I64_MAX, _I64_MIN):
        raise errors.DataError("can't decode infinite timestamp in binary format")


def _dec_date_bin(b: bytes) -> _dt.date:
    (days,) = _s_i32.unpack(b)
    if days in (2147483647, -2147483648):
        raise errors.DataError("can't decode infinite date in binary format")
    return _EPOCH_DATE + _dt.timedelta(days=days)


def _dec_time_bin(b: bytes) -> _dt.time:
    (usec,) = _s_i64.unpack(b)
    return (_dt.datetime(2000, 1, 1) + _dt.timedelta(microseconds=usec)).time()


def _dec_text_bin(b: bytes) -> str:
    return b.decode("utf-8")


def _dec_jsonb_bin(b: bytes) -> Any:
    if not b or b[0] != 1:
        raise errors.NotSupportedError("unsupported jsonb binary version")
    return json.loads(b[1:].decode("utf-8"))


def _dec_float4(b: bytes) -> float:
    return _s_f32.unpack(b)[0]


def _dec_float8(b: bytes) -> float:
    return _s_f64.unpack(b)[0]


_SCALAR_BIN: dict[int, Callable[[bytes], Any]] = {
    BOOL: lambda b: b != b"\x00",
    BYTEA: bytes,
    INT2: lambda b: _s_i16.unpack(b)[0],
    INT4: lambda b: _s_i32.unpack(b)[0],
    INT8: lambda b: _s_i64.unpack(b)[0],
    OID: lambda b: _s_u32.unpack(b)[0],
    FLOAT4: _dec_float4,
    FLOAT8: _dec_float8,
    NUMERIC: _dec_numeric,
    UUID: lambda b: uuid.UUID(bytes=b),
    JSON: lambda b: json.loads(b.decode("utf-8")),
    JSONB: _dec_jsonb_bin,
    DATE: _dec_date_bin,
    TIME: _dec_time_bin,
    TIMESTAMP: _dec_timestamp_bin,
    TIMESTAMPTZ: _dec_timestamptz_bin,
    INTERVAL: _dec_interval,
    TEXT: _dec_text_bin,
    VARCHAR: _dec_text_bin,
    BPCHAR: _dec_text_bin,
    NAME: _dec_text_bin,
    CHAR: _dec_text_bin,
}


def decode_array(b: bytes, element: Callable[[bytes], Any]) -> list:
    ndim, _flags, _elem = struct.unpack_from("!iii", b, 0)
    pos = 12
    dims = []
    for _ in range(ndim):
        length, _lb = struct.unpack_from("!ii", b, pos)
        dims.append(length)
        pos += 8
    if ndim == 0:
        return []
    items: list = []
    total = 1
    for d in dims:
        total *= d
    for _ in range(total):
        (length,) = _s_i32.unpack_from(b, pos)
        pos += 4
        if length < 0:
            items.append(None)
        else:
            items.append(element(b[pos:pos + length]))
            pos += length
    # fold the flat list into nested lists, outermost dimension first
    for d in reversed(dims[1:]):
        items = [items[i:i + d] for i in range(0, len(items), d)]
    return items


def scalar_decoder(oid: int) -> Callable[[bytes], Any] | None:
    """Binary decoder for a type (arrays included), or ``None`` if the type has no binary form here."""
    if oid in _SCALAR_BIN:
        return _SCALAR_BIN[oid]
    elem = ELEMENT_OF.get(oid)
    if elem is not None and elem in _SCALAR_BIN:
        inner = _SCALAR_BIN[elem]
        return lambda b: decode_array(b, inner)
    return None


# ---------------------------------------------------------------- encoders (Python -> bytes)
def encode_scalar(value: Any, oid: int) -> bytes | None:
    """Binary form of ``value`` for a parameter the server typed as ``oid``; ``None`` means
    "send it as text instead" (always safe: the text form is what the server parses anyway)."""
    if oid == BOOL and isinstance(value, bool):
        return b"\x01" if value else b"\x00"
    if oid in (INT2, INT4, INT8, OID) and isinstance(value, int) and not isinstance(value, bool):
        fmt, lo, hi = {INT2: ("!h", -2**15, 2**15 - 1), INT4: ("!i", -2**31, 2**31 - 1),
                       INT8: ("!q", -2**63, 2**63 - 1), OID: ("!I", 0, 2**32 - 1)}[oid]
        return struct.pack(fmt, value) if lo <= value <= hi else None
    if oid in (FLOAT4, FLOAT8) and isinstance(value, (int, float)) and not isinstance(value, bool):
        return _s_f32.pack(value) if oid == FLOAT4 else _s_f64.pack(float(value))
    if oid in (TEXT, VARCHAR, BPCHAR, NAME) and isinstance(value, str) and "\x00" not in value:
        return value.encode("utf-8")
    if oid == BYTEA and isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    if oid == UUID and isinstance(value, uuid.UUID):
        return value.bytes
    if oid == DATE and isinstance(value, _dt.date) and not isinstance(value, _dt.datetime):
        return _s_i32.pack((value - _EPOCH_DATE).days)
    if oid == TIMESTAMP and isinstance(value, _dt.datetime) and value.tzinfo is None:
        return _s_i64.pack(_usec(value - _EPOCH))
    if oid == TIMESTAMPTZ and isinstance(value, _dt.datetime) and value.tzinfo is not None:
        return _s_i64.pack(_usec(value.astimezone(_dt.timezone.utc) - _EPOCH_UTC))
    if oid == TIME and isinstance(value, _dt.time) and value.tzinfo is None:
        t = _dt.timedelta(hours=value.hour, minutes=value.minute, seconds=value.second, microseconds=value.microsecond)
        return _s_i64.pack(_usec(t))
    if oid == INTERVAL and isinstance(value, Interval):
        d = value.delta
        return struct.pack("!qii", (d.seconds * 1_000_000) + d.microseconds, d.days, value.months)
    if oid == INTERVAL and isinstance(value, _dt.timedelta):
        return struct.pack("!qii", _usec(value), 0, 0)
    if oid in ARRAY_OF.values() and isinstance(value, list):  # a one-dimensional array of scalars
        elem = ELEMENT_OF.get(oid)
        return _encode_array(value, elem) if elem is not None else None
    return None


def _usec(delta: _dt.timedelta) -> int:
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def _encode_array(items: list, elem: int) -> bytes | None:
    if any(isinstance(v, list) for v in items):
        return None  # multi-dimensional: text
    parts = [struct.pack("!iii", 1, 0, elem), struct.pack("!ii", len(items), 1)]
    for v in items:
        if v is None:
            parts.append(_s_i32.pack(-1))
            continue
        data = encode_scalar(v, elem)
        if data is None:
            return None
        parts.append(_s_i32.pack(len(data)) + data)
    return b"".join(parts)
