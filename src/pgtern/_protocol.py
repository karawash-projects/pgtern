# SPDX-License-Identifier: Apache-2.0
"""PostgreSQL frontend/backend protocol 3.0, sans-I/O.

Functions here build or parse bytes and never touch a socket, so the same code
can later be driven by asyncio, Trio or a blocking socket.
"""
from __future__ import annotations

import re
import struct
from typing import NamedTuple, Sequence

from . import errors

_I32 = struct.Struct("!I")
_S32 = struct.Struct("!i")
_U16 = struct.Struct("!H")
_S16 = struct.Struct("!h")
_MAX_MESSAGE = 1 << 30

SSL_REQUEST = struct.pack("!II", 8, 80877103)


def _msg(tag: bytes, payload: bytes) -> bytes:
    return tag + _I32.pack(len(payload) + 4) + payload


def startup_message(params: dict[str, str]) -> bytes:
    body = _I32.pack(196608) + b"".join(
        k.encode() + b"\0" + v.encode() + b"\0" for k, v in params.items()
    ) + b"\0"
    return _I32.pack(len(body) + 4) + body


def query_message(sql: str) -> bytes:
    return _msg(b"Q", _checked(sql).encode() + b"\0")


def _checked(sql: str) -> str:
    if "\x00" in sql:
        raise errors.ProgrammingError("a query cannot contain NUL (0x00) characters")
    return sql


def extended_query(sql: str, oids: Sequence[int], values: Sequence[bytes | None],
                   pformats: Sequence[int] | None = None, rformats: Sequence[int] | None = None,
                   sync: bool = True) -> bytes:
    """Parse + Bind + Describe + Execute + Sync on the unnamed statement and portal.

    Parameters travel in text format unless ``pformats`` says otherwise (one code per
    parameter, 0 text or 1 binary); results likewise with ``rformats`` (one per column).
    """
    if len(values) > 65535:
        raise errors.ProgrammingError("PostgreSQL allows at most 65535 query parameters")
    parse = b"\0" + _checked(sql).encode() + b"\0" + _U16.pack(len(oids)) + struct.pack(f"!{len(oids)}I", *oids)
    pf = b"" if pformats is None else _U16.pack(len(pformats)) + b"".join(_U16.pack(f) for f in pformats)
    parts = [b"\0\0", pf or _U16.pack(0), _U16.pack(len(values))]
    for v in values:
        parts.append(_S32.pack(-1) if v is None else _S32.pack(len(v)) + v)
    rf = b"" if rformats is None else _U16.pack(len(rformats)) + b"".join(_U16.pack(f) for f in rformats)
    parts.append(rf or _U16.pack(0))
    return (
        _msg(b"P", parse)
        + _msg(b"B", b"".join(parts))
        + _msg(b"D", b"P\0")
        + _msg(b"E", b"\0" + _S32.pack(0))
        + (_msg(b"S", b"") if sync else b"")
    )


SYNC = _msg(b"S", b"")

# Bytes a pipeline or executemany may send before it must read the replies it is owed. Without this bound a
# client and a server can each block on a full socket: the server writes big results, the client writes big
# parameters, and neither reads.
FLUSH_BYTES = 256 * 1024

# COPY cannot run in a pipeline or executemany: the server waits for data the batch never sends.
COPY_START = re.compile(r"\s*copy\b", re.IGNORECASE)
# The server buffers its replies until a Sync or a Flush: pipelines use Flush to read mid-batch.
FLUSH = _msg(b"H", b"")


def build_query(sql: str, params, translate, dump) -> bytes:
    """Frontend bytes for a query: simple protocol without parameters, extended with them."""
    if params is None:
        return query_message(sql)
    pg_sql, values = translate(sql, params)
    dumped = [dump(v) for v in values]
    return extended_query(pg_sql, [oid for oid, _ in dumped], [data for _, data in dumped])


def describe_statement(sql: str) -> bytes:
    """Parse the unnamed statement (types inferred by the server) and describe it. Runs nothing."""
    parse = b"\0" + _checked(sql).encode() + b"\0" + _U16.pack(0)
    return _msg(b"P", parse) + _msg(b"D", b"S\0") + _msg(b"S", b"")


def parse_param_description(payload: bytes) -> list[int]:
    (count,) = _U16.unpack_from(payload, 0)
    return list(struct.unpack_from(f"!{count}I", payload, 2))


def password_message(password: bytes) -> bytes:
    return _msg(b"p", password + b"\0")


def sasl_initial_response(mechanism: str, data: bytes) -> bytes:
    return _msg(b"p", mechanism.encode() + b"\0" + _S32.pack(len(data)) + data)


def sasl_response(data: bytes) -> bytes:
    return _msg(b"p", data)


def copy_fail(reason: str) -> bytes:
    return _msg(b"f", reason.encode() + b"\0")


def copy_data(data: bytes) -> bytes:
    return _msg(b"d", data)


def cancel_request(pid: int, secret: int) -> bytes:
    """CancelRequest: sent on a new connection to stop the statement running on backend ``pid``."""
    return struct.pack("!IIII", 16, 80877102, pid, secret)


COPY_DONE = _msg(b"c", b"")
TERMINATE = _msg(b"X", b"")


class Notify(NamedTuple):
    """An asynchronous notification from LISTEN/NOTIFY."""

    channel: str
    payload: str
    pid: int


def parse_notification(payload: bytes) -> Notify:
    pid = _I32.unpack_from(payload, 0)[0]
    channel, body, _ = payload[4:].split(b"\0", 2)
    return Notify(channel.decode("utf-8", "replace"), body.decode("utf-8", "replace"), pid)


class MessageParser:
    """Reassembles backend messages from arbitrary chunks of bytes."""

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes) -> list[tuple[int, bytes]]:
        buf = self._buf
        buf += data
        out: list[tuple[int, bytes]] = []
        pos, n = 0, len(buf)
        while n - pos >= 5:
            (length,) = _I32.unpack_from(buf, pos + 1)
            if length < 4 or length > _MAX_MESSAGE:
                raise errors.InterfaceError(f"invalid message length {length} from the server")
            end = pos + 1 + length
            if end > n:
                break
            out.append((buf[pos], bytes(buf[pos + 5:end])))
            pos = end
        if pos:
            del buf[:pos]
        return out


def parse_error_fields(payload: bytes) -> dict[str, str]:
    """ErrorResponse / NoticeResponse: ``code byte`` + ``cstring`` pairs, ended by a zero byte."""
    fields: dict[str, str] = {}
    i, n = 0, len(payload)
    while i < n and payload[i] != 0:
        code = chr(payload[i])
        end = payload.index(b"\0", i + 1)
        fields[code] = payload[i + 1:end].decode("utf-8", "replace")
        i = end + 1
    return fields


def parse_parameter_status(payload: bytes) -> tuple[str, str]:
    name, value, _ = payload.split(b"\0", 2)
    return name.decode(), value.decode("utf-8", "replace")


class Column(NamedTuple):
    """A DB-API 7-item description entry (only the first four are filled in)."""

    name: str
    type_code: int
    display_size: int | None = None
    internal_size: int | None = None
    precision: int | None = None
    scale: int | None = None
    null_ok: bool | None = None


def parse_row_description(payload: bytes) -> list[Column]:
    (count,) = _U16.unpack_from(payload, 0)
    pos = 2
    cols: list[Column] = []
    for _ in range(count):
        end = payload.index(b"\0", pos)
        name = payload[pos:end].decode("utf-8", "replace")
        _table, _attr, type_oid, size, _mod, _fmt = struct.unpack_from("!IhIhih", payload, end + 1)
        cols.append(Column(name, type_oid, None, size if size >= 0 else None))
        pos = end + 1 + 18
    return cols


def parse_data_row(payload: bytes) -> list[bytes | None]:
    (count,) = _U16.unpack_from(payload, 0)
    pos = 2
    row: list[bytes | None] = []
    for _ in range(count):
        (length,) = _S32.unpack_from(payload, pos)
        pos += 4
        if length < 0:
            row.append(None)
        else:
            row.append(payload[pos:pos + length])
            pos += length
    return row


def _rowcount(tag: str) -> int:
    words = tag.split()
    if words and words[0] in {"INSERT", "SELECT", "UPDATE", "DELETE", "MERGE", "FETCH", "MOVE", "COPY"}:
        try:
            return int(words[-1])
        except ValueError:
            return -1
    return -1


class Result:
    __slots__ = ("columns", "rows", "tag", "rowcount", "formats")

    def __init__(self, columns: list[Column] | None = None) -> None:
        self.columns = columns
        self.rows: list[list[bytes | None]] = []
        self.tag = ""
        self.rowcount = -1
        self.formats: list[int] | None = None  # per column: 1 when the rows are binary


class QueryCollector:
    """Builds the list of results of one query cycle from backend messages."""

    def __init__(self) -> None:
        self.results: list[Result] = []
        self._current: Result | None = None

    def handle(self, tag: int, payload: bytes) -> None:
        if tag == 0x54:  # T RowDescription
            self._current = Result(parse_row_description(payload))
        elif tag == 0x44:  # D DataRow
            if self._current is None:
                self._current = Result()
            self._current.rows.append(parse_data_row(payload))
        elif tag == 0x43:  # C CommandComplete
            res = self._current or Result()
            res.tag = payload.rstrip(b"\0").decode("utf-8", "replace")
            res.rowcount = _rowcount(res.tag)
            self.results.append(res)
            self._current = None
        elif tag == 0x49:  # I EmptyQueryResponse
            self.results.append(Result())
        # 1 ParseComplete, 2 BindComplete, 3 CloseComplete, n NoData, t ParameterDescription: nothing to keep
