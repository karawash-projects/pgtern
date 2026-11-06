# SPDX-License-Identifier: Apache-2.0
"""Python <-> PostgreSQL text-format codecs.

Parameters are sent in text format. Strings go out with the "unknown" type OID
(0) so the server infers the type from context (``%s::int``, ``col = %s``); ints
are sent as int4, int8 or numeric by size (never int2: ``$1 * $2`` on smallint parameters overflows).
Use an explicit cast for a function that only takes smallint. Results are decoded from text format; types without a decoder
come back as ``str``.
"""
from __future__ import annotations

import binascii
import json
from dataclasses import dataclass
import re
import uuid
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from typing import Any, Callable, Sequence

from . import errors
from .types import Json, Jsonb

BOOL, BYTEA, CHAR, NAME, INT8, INT2, INT4, TEXT, OID = 16, 17, 18, 19, 20, 21, 23, 25, 26
JSON, FLOAT4, FLOAT8, BPCHAR, VARCHAR = 114, 700, 701, 1042, 1043
DATE, TIME, TIMESTAMP, TIMESTAMPTZ, INTERVAL, TIMETZ = 1082, 1083, 1114, 1184, 1186, 1266
NUMERIC, UUID, JSONB = 1700, 2950, 3802

ARRAY_OF = {
    BOOL: 1000, BYTEA: 1001, CHAR: 1002, NAME: 1003, INT2: 1005, INT4: 1007, TEXT: 1009,
    OID: 1028, VARCHAR: 1015, INT8: 1016, FLOAT4: 1021, FLOAT8: 1022, BPCHAR: 1014, JSON: 199,
    DATE: 1182, TIME: 1183, TIMESTAMP: 1115, TIMESTAMPTZ: 1185, INTERVAL: 1187, TIMETZ: 1270,
    NUMERIC: 1231, UUID: 2951, JSONB: 3807,
}
ELEMENT_OF = {array: elem for elem, array in ARRAY_OF.items()}

_UNQUOTED_IN_ARRAYS = {BOOL, INT2, INT4, INT8, FLOAT8, NUMERIC}
_INT_ORDER = (INT2, INT4, INT8, NUMERIC)


# ---------------------------------------------------------------- dumping

def render_literal(value: Any) -> str:
    """A value as an SQL literal (for client-side binding)."""
    from .sql import quote_literal
    _oid, data = dump(value)
    return "NULL" if data is None else quote_literal(data.decode("utf-8"))


def dump(value: Any) -> tuple[int, bytes | None]:
    """Return ``(type_oid, text_bytes)`` for a parameter; ``(0, None)`` is SQL NULL."""
    if value is None:
        return 0, None
    if _DUMPERS:
        fn = _dumper_for(type(value))
        if fn is not None:  # user dumpers win; the text goes out untyped so the server infers the type
            text = fn(value)
            return (0, None) if text is None else (0, text.encode("utf-8"))
    if value is True or value is False:
        return BOOL, b"t" if value else b"f"
    if isinstance(value, int):
        if -2147483648 <= value <= 2147483647:
            oid = INT4
        elif -9223372036854775808 <= value <= 9223372036854775807:
            oid = INT8
        else:
            oid = NUMERIC
        return oid, str(int(value)).encode()
    if isinstance(value, str):
        if "\x00" in value:
            raise errors.DataError("PostgreSQL text fields cannot contain NUL (0x00) bytes")
        return 0, str.encode(value, "utf-8")
    if isinstance(value, float):
        if value != value:
            return FLOAT8, b"NaN"
        if value in (float("inf"), float("-inf")):
            return FLOAT8, b"Infinity" if value > 0 else b"-Infinity"
        return FLOAT8, repr(value).encode()
    if isinstance(value, Decimal):
        return NUMERIC, str(value).encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return BYTEA, b"\\x" + binascii.hexlify(bytes(value))
    if isinstance(value, datetime):
        return (TIMESTAMPTZ if value.tzinfo is not None else TIMESTAMP), value.isoformat(" ").encode()
    if isinstance(value, date):
        return DATE, value.isoformat().encode()
    if isinstance(value, time):
        return (TIMETZ if value.tzinfo is not None else TIME), value.isoformat().encode()
    if isinstance(value, Interval):
        d = value.delta
        return INTERVAL, (f"{value.months} months {d.days} days {d.seconds} seconds "
                          f"{d.microseconds} microseconds").encode()
    if isinstance(value, timedelta):
        return INTERVAL, f"{value.days} days {value.seconds} seconds {value.microseconds} microseconds".encode()
    if isinstance(value, uuid.UUID):
        return UUID, str(value).encode()
    if isinstance(value, Json):
        return (JSONB if isinstance(value, Jsonb) else JSON), value.encode().encode()
    if isinstance(value, list):
        return _dump_list(value)
    raise errors.ProgrammingError(
        f"cannot adapt type {type(value).__name__!r}; wrap dicts in Jsonb() and use lists for arrays")


def _unify(oids: set[int]) -> int:
    if not oids:
        return 0
    if len(oids) == 1:
        return next(iter(oids))
    if oids <= set(_INT_ORDER):
        return max(oids, key=_INT_ORDER.index)
    raise errors.ProgrammingError(f"cannot dump a list of mixed types (type OIDs {sorted(oids)})")


def _dump_list(items: list) -> tuple[int, bytes]:
    leaves: list[tuple[int, bytes | None]] = []

    def walk(node: Any) -> Any:
        if isinstance(node, list):
            return [walk(child) for child in node]
        if node is None:
            return None
        pair = dump(node)
        leaves.append(pair)
        return pair

    tree = walk(items)
    elem = _unify({oid for oid, _ in leaves})

    def render(node: Any) -> str:
        if node is None:
            return "NULL"
        if isinstance(node, list):
            return "{" + ",".join(render(child) for child in node) + "}"
        oid, data = node
        text = data.decode()
        if oid in _UNQUOTED_IN_ARRAYS:
            return text
        return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'

    return (ARRAY_OF.get(elem, 0) if elem else 0), render(tree).encode()


# ---------------------------------------------------------------- decoding

_DATE_RE = re.compile(r"^(\d{4,})-(\d\d)-(\d\d)( BC)?$")
_TIMESTAMP_RE = re.compile(
    r"^(\d{4,})-(\d\d)-(\d\d) (\d\d):(\d\d):(\d\d)(?:\.(\d{1,6}))?"
    r"(?:([+-])(\d\d)(?::?(\d\d))?(?::?(\d\d))?)?( BC)?$")
_TIME_RE = re.compile(
    r"^(\d\d):(\d\d):(\d\d)(?:\.(\d{1,6}))?(?:([+-])(\d\d)(?::?(\d\d))?(?::?(\d\d))?)?$")
_INTERVAL_RE = re.compile(
    r"^(?:([+-]?\d+) years?)?\s*(?:([+-]?\d+) mons?)?\s*(?:([+-]?\d+) days?)?\s*"
    r"(?:([+-])?(\d+):(\d\d):(\d\d)(?:\.(\d{1,6}))?)?$")


def _tz(sign: str | None, hh: str | None, mm: str | None, ss: str | None) -> timezone | None:
    if sign is None:
        return None
    delta = timedelta(hours=int(hh or 0), minutes=int(mm or 0), seconds=int(ss or 0))
    if sign == "-":
        delta = -delta
    return timezone.utc if not delta else timezone(delta)


def _dec_bool(s: str) -> bool:
    return s == "t"


def _dec_bytea(s: str) -> bytes:
    if s.startswith("\\x"):
        return binascii.unhexlify(s[2:])
    out = bytearray()  # legacy "escape" output format
    i = 0
    while i < len(s):
        if s[i] != "\\":
            out.append(ord(s[i]))
            i += 1
        elif s[i + 1:i + 2] == "\\":
            out.append(0x5C)
            i += 2
        else:
            out.append(int(s[i + 1:i + 4], 8))
            i += 4
    return bytes(out)


def _dec_date(s: str) -> date:
    m = _DATE_RE.match(s)
    if not m or m.group(4) or int(m.group(1)) > 9999:
        raise errors.DataError(f"can't decode date {s!r}: BC dates, years above 9999 and infinity are not supported")
    try:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    except ValueError as exc:
        raise errors.DataError(f"can't decode date {s!r}: {exc}") from None


def _make_timestamp(s: str, aware: bool) -> datetime:
    m = _TIMESTAMP_RE.match(s)
    if not m or m.group(12) or int(m.group(1)) > 9999:
        raise errors.DataError(
            f"can't decode timestamp {s!r}: BC dates, years above 9999 and infinity are not supported")
    year, month, day, hour, minute, second = (int(m.group(i)) for i in range(1, 7))
    micro = int((m.group(7) or "").ljust(6, "0"))
    tz = _tz(m.group(8), m.group(9), m.group(10), m.group(11)) if aware else None
    try:
        return datetime(year, month, day, hour, minute, second, micro, tzinfo=tz)
    except ValueError as exc:
        raise errors.DataError(f"can't decode timestamp {s!r}: {exc}") from None


def _dec_timestamp(s: str) -> datetime:
    return _make_timestamp(s, False)


def _dec_timestamptz(s: str) -> datetime:
    return _make_timestamp(s, True)


def _make_time(s: str, aware: bool) -> time:
    m = _TIME_RE.match(s)
    if not m:
        raise errors.DataError(f"can't decode time {s!r}")
    micro = int((m.group(4) or "").ljust(6, "0"))
    tz = _tz(m.group(5), m.group(6), m.group(7), m.group(8)) if aware else None
    try:
        return time(int(m.group(1)), int(m.group(2)), int(m.group(3)), micro, tzinfo=tz)
    except ValueError as exc:
        raise errors.DataError(f"can't decode time {s!r}: {exc}") from None


def _dec_time(s: str) -> time:
    return _make_time(s, False)


def _dec_timetz(s: str) -> time:
    return _make_time(s, True)


@dataclass(frozen=True)
class Interval:
    """A PostgreSQL interval that has months or years in it.

    Python's timedelta has no months, and a month is not a fixed number of days, so ``months`` is kept
    apart from ``delta`` (the days and the time). Intervals without months come back as a timedelta.
    """

    months: int = 0
    delta: timedelta = timedelta()


def make_interval(months: int, delta: timedelta) -> timedelta | Interval:
    return Interval(months, delta) if months else delta


def _dec_interval(s: str) -> timedelta | Interval:
    """PostgreSQL ``postgres`` IntervalStyle text."""
    m = _INTERVAL_RE.match(s.strip())
    if not m:
        raise errors.NotSupportedError(f"can't decode interval {s!r}: only IntervalStyle 'postgres' is supported")
    years, months, days = (int(m.group(i) or 0) for i in (1, 2, 3))
    delta = timedelta(days=days)
    if m.group(5) is not None:
        clock = timedelta(hours=int(m.group(5)), minutes=int(m.group(6)), seconds=int(m.group(7)),
                          microseconds=int((m.group(8) or "").ljust(6, "0")))
        delta += -clock if m.group(4) == "-" else clock
    return make_interval(years * 12 + months, delta)


_SCALAR_DECODERS: dict[int, Callable[[str], Any] | None] = {
    BOOL: _dec_bool, BYTEA: _dec_bytea, INT2: int, INT4: int, INT8: int, OID: int,
    FLOAT4: float, FLOAT8: float, NUMERIC: Decimal, UUID: uuid.UUID,
    JSON: json.loads, JSONB: json.loads,
    DATE: _dec_date, TIME: _dec_time, TIMETZ: _dec_timetz, TIMESTAMP: _dec_timestamp,
    TIMESTAMPTZ: _dec_timestamptz, INTERVAL: _dec_interval,
    TEXT: None, VARCHAR: None, BPCHAR: None, NAME: None, CHAR: None,
}
_DATE_OIDS = {DATE, TIME, TIMETZ, TIMESTAMP, TIMESTAMPTZ, INTERVAL}


def parse_array(text: str, element: Callable[[str], Any] | None) -> list:
    """Parse a PostgreSQL array literal (any number of dimensions) with ``element`` per item."""
    if text.startswith("["):  # explicit bounds, e.g. "[0:1]={a,b}"
        text = text[text.index("=") + 1:]
    root: list = []
    stack: list[list] = []
    i, n = 0, len(text)
    while i < n:
        c = text[i]
        if c == "{":
            node: list = []
            (stack[-1] if stack else _Holder(root)).append(node)
            stack.append(node)
            i += 1
        elif c == "}":
            stack.pop()
            i += 1
        elif c == ",":
            i += 1
        elif c == '"':
            i += 1
            buf: list[str] = []
            while text[i] != '"':
                if text[i] == "\\":
                    i += 1
                buf.append(text[i])
                i += 1
            i += 1
            token = "".join(buf)
            stack[-1].append(token if element is None else element(token))
        else:
            j = i
            while j < n and text[j] not in ",}":
                j += 1
            token = text[i:j]
            i = j
            stack[-1].append(None if token == "NULL" else (token if element is None else element(token)))
    return root[0] if root else []


class _Holder(list):
    """Lets the first ``{`` replace the root list contents."""

    def __init__(self, root: list) -> None:
        super().__init__()
        self._root = root

    def append(self, item: Any) -> None:  # type: ignore[override]
        self._root.append(item)


def _decoder_for(oid: int, dates_ok: bool) -> Callable[[str], Any] | None:
    base = ELEMENT_OF.get(oid)
    if base is not None:
        element = _decoder_for(base, dates_ok)
        return lambda s: parse_array(s, element)
    decoder = _SCALAR_DECODERS.get(oid)
    if decoder is not None and oid in _DATE_OIDS and not dates_ok:
        def refuse(s: str) -> Any:
            raise errors.NotSupportedError(
                "date/time values can't be decoded: the connection's DateStyle is not ISO")
        return refuse
    return decoder


class BinaryColumn:
    """Marks a column whose values are binary (decoder takes bytes, not text)."""

    __slots__ = ("fn",)

    def __init__(self, fn: Callable[[bytes], Any]) -> None:
        self.fn = fn


def make_loaders(oids: Sequence[int], dates_ok: bool = True,
                 cache: dict[int, Any] | None = None,
                 formats: Sequence[int] | None = None) -> list[Any]:
    """One loader per column: a text decoder (``str`` in), a :class:`BinaryColumn`, or ``None`` (keep text)."""
    out: list[Any] = []
    for i, oid in enumerate(oids):
        if formats is not None and formats[i] == 1:
            from ._binary import scalar_decoder  # imported here: _binary depends on this module
            decoder = scalar_decoder(oid)
            if decoder is None:
                raise errors.InterfaceError(f"no binary decoder for type oid {oid}")
            out.append(BinaryColumn(decoder))
            continue
        user = cache.get(oid) if cache else None
        out.append(user if user is not None else _decoder_for(oid, dates_ok))
    return out


# ---------------------------------------------------------------- user adapters
_DUMPERS: dict[type, Callable[[Any], str | None]] = {}
_LOADERS: dict[str, Callable[[str], Any]] = {}


def _dumper_for(cls: type) -> Callable[[Any], str | None] | None:
    for klass in cls.__mro__:
        fn = _DUMPERS.get(klass)
        if fn is not None:
            return fn
    return None


def has_loaders() -> bool:
    return bool(_LOADERS)


def loader_for_name(nspname: str, typname: str) -> Callable[[str], Any] | None:
    return _LOADERS.get(f"{nspname}.{typname}") or _LOADERS.get(typname)


def _nn(value: bytes | None) -> bytes:
    if value is None:
        raise errors.InterfaceError("unexpected NULL in a catalog row")
    return value


def resolve_loaders(pg_type_rows: list[list[bytes | None]], cache: dict[int, Any], wanted: Sequence[int]) -> None:
    """Fill ``cache`` (oid -> loader or None) for ``wanted``.

    ``pg_type_rows`` are rows of (oid, nspname, typname, typelem, typcategory) for the wanted
    types and their element types. Arrays get a loader when their element type has one.
    """
    info: dict[int, tuple[str, str, int, str]] = {}
    for oid_b, nsp_b, name_b, elem_b, cat_b in pg_type_rows:
        info[int(_nn(oid_b))] = (_nn(nsp_b).decode(), _nn(name_b).decode(), int(_nn(elem_b)), _nn(cat_b).decode())

    def loader_for(oid: int, depth: int = 0) -> Callable[[str], Any] | None:
        if oid in cache:
            return cache[oid]
        result: Callable[[str], Any] | None = None
        entry = info.get(oid)
        if entry is not None and depth < 4:
            nsp, name, elem, cat = entry
            if cat == "A" and elem:
                element = loader_for(elem, depth + 1)
                if element is not None:
                    result = lambda s, _e=element: parse_array(s, _e)  # noqa: E731
            if result is None:
                result = loader_for_name(nsp, name)
        cache[oid] = result
        return result

    for oid in wanted:
        loader_for(int(oid))



def load_row(raw: Sequence[bytes | None], loaders: Sequence[Any]) -> tuple:
    out: list[Any] = []
    for v, ld in zip(raw, loaders):
        if v is None:
            out.append(None)
        elif type(ld) is BinaryColumn:
            out.append(ld.fn(v))
        else:
            out.append(v.decode() if ld is None else ld(v.decode()))
    return tuple(out)


def binary_param_plan(server_oids: Sequence[int], values: Sequence[Any]) -> tuple[list[int], list[bytes | None]]:
    """Per parameter: (format, bytes). Binary when the server's type has a binary encoder for the
    value; otherwise text, which the server parses exactly as it would without binary mode."""
    from ._binary import encode_scalar
    formats: list[int] = []
    payloads: list[bytes | None] = []
    for oid, value in zip(server_oids, values):
        if value is None:
            formats.append(0)
            payloads.append(None)
            continue
        data = encode_scalar(value, oid)
        if data is not None:
            formats.append(1)
            payloads.append(data)
        else:
            formats.append(0)
            payloads.append(dump(value)[1])
    return formats, payloads
