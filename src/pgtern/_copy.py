# SPDX-License-Identifier: Apache-2.0
"""COPY support: text-format helpers and the synchronous ``Copy`` object."""
from __future__ import annotations

from contextlib import ExitStack
from typing import Any, Iterator, Sequence

from . import _protocol as P
from . import _types, errors

_ESCAPES = {ord("\\"): "\\\\", ord("\t"): "\\t", ord("\n"): "\\n", ord("\r"): "\\r"}
_UNESCAPE = {b"b": b"\b", b"f": b"\f", b"n": b"\n", b"r": b"\r", b"t": b"\t", b"v": b"\v", b"\\": b"\\"}


def format_row(row: Sequence[Any]) -> bytes:
    """One row in COPY text format (tab separated, ``\\N`` for NULL), newline terminated."""
    fields = []
    for value in row:
        _oid, data = _types.dump(value)
        fields.append("\\N" if data is None else data.decode("utf-8").translate(_ESCAPES))
    return ("\t".join(fields) + "\n").encode("utf-8")


def _unescape(field: bytes, codec: str = "utf-8") -> str | None:
    if field == b"\\N":
        return None
    if b"\\" not in field:
        return field.decode(codec)
    out = bytearray()
    i, n = 0, len(field)
    while i < n:
        c = field[i:i + 1]
        if c != b"\\" or i + 1 >= n:
            out += c
            i += 1
            continue
        nxt = field[i + 1:i + 2]
        if nxt in _UNESCAPE:
            out += _UNESCAPE[nxt]
            i += 2
        elif nxt == b"x":
            j = i + 2
            while j < n and j < i + 4 and chr(field[j]) in "0123456789abcdefABCDEF":
                j += 1
            out.append(int(field[i + 2:j], 16)) if j > i + 2 else out.extend(b"x")
            i = max(j, i + 2)
        elif nxt in b"01234567":
            j = i + 1
            while j < n and j < i + 4 and field[j:j + 1] in (b"0", b"1", b"2", b"3", b"4", b"5", b"6", b"7"):
                j += 1
            out.append(int(field[i + 1:j], 8) & 0xFF)
            i = j
        else:
            out += nxt
            i += 2
    return out.decode(codec)


def parse_rows(buffer: bytearray, final: bool = False, codec: str = "utf-8") -> list[tuple]:
    """Consume complete text-format lines from ``buffer``; return them as tuples of str/None."""
    rows: list[tuple] = []
    while True:
        end = buffer.find(b"\n")
        if end < 0:
            if final and buffer:
                end = len(buffer)
            else:
                return rows
        line = bytes(buffer[:end])
        del buffer[:end + 1]
        rows.append(tuple(_unescape(f, codec) for f in line.split(b"\t")))


class Copy:
    """``with cursor.copy("COPY t FROM STDIN") as copy: copy.write_row(...)`` or
    ``with cursor.copy("COPY t TO STDOUT") as copy: for chunk in copy: ...``."""

    def __init__(self, cursor: Any, query: Any, params: Any = None) -> None:
        self._cursor = cursor
        self._conn = cursor.connection
        self._query = query
        self._params = params
        self._kind = ""
        self._stack = ExitStack()
        self._done = False
        self._buffer = bytearray()
        self._results: list = []

    # ---------------------------------------------------------------- context
    def __enter__(self) -> "Copy":
        conn = self._conn
        conn._reentry_check()
        conn._lock.acquire()
        try:
            self._kind = conn._start_copy(self._query, self._params, self._stack)
            conn._copy_depth += 1
        except BaseException:
            self._stack.close()
            conn._lock.release()
            raise
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        conn = self._conn
        try:
            if conn.closed:
                return
            if self._kind == "in":
                if exc_type is not None:
                    conn._send(P.copy_fail(f"error in Python: {exc_type.__name__}"))
                    try:
                        conn._read_cycle()
                    except errors.Error:
                        pass  # the expected "COPY from stdin failed"
                else:
                    conn._send(P.COPY_DONE)
                    self._results = conn._read_cycle()
            else:
                while not self._done:
                    self._next_chunk()
                self._results = self._finish()
            if exc_type is None:
                self._cursor._set_results(self._results)
        finally:
            try:
                self._stack.__exit__(exc_type, exc, tb)
            finally:
                conn._copy_depth -= 1
                conn._lock.release()

    # ---------------------------------------------------------------- COPY FROM STDIN
    def write(self, data: bytes | bytearray | memoryview | str) -> None:
        if self._kind != "in":
            raise errors.ProgrammingError("write() needs a COPY ... FROM STDIN statement")
        if isinstance(data, str):
            P.check_encodable(self._conn._codec, data)
            data = data.encode("utf-8")
        data = bytes(data)
        for i in range(0, max(len(data), 1), 1 << 20):
            self._conn._send(P.copy_data(data[i:i + (1 << 20)]))

    def write_row(self, row: Sequence[Any]) -> None:
        """Write one row in text format (``COPY ... FROM STDIN`` default, or ``WITH (FORMAT text)``)."""
        self.write(format_row(row))

    # ---------------------------------------------------------------- COPY TO STDOUT
    def _next_chunk(self) -> bytes | None:
        conn = self._conn
        while True:
            tag, payload = conn._next_message()
            if tag == 0x64:  # d CopyData
                return payload
            if tag == 0x63:  # c CopyDone
                self._done = True
                return None
            if tag == 0x45:
                self._done = True
                self._pending_error = P.parse_error_fields(payload)
                return None
            conn._handle_async_message(tag, payload)

    def _finish(self) -> list:
        err = getattr(self, "_pending_error", None)
        results = self._conn._read_cycle()
        if err is not None:
            raise errors.error_from_fields(err)
        return results

    def read(self) -> bytes:
        """The next chunk of COPY data, or ``b""`` at the end."""
        if self._kind != "out":
            raise errors.ProgrammingError("read() needs a COPY ... TO STDOUT statement")
        if self._done:
            return b""
        chunk = self._next_chunk()
        return chunk if chunk is not None else b""

    def __iter__(self) -> Iterator[bytes]:
        while True:
            chunk = self.read()
            if not chunk:
                return
            yield chunk

    def rows(self) -> Iterator[tuple]:
        """Parsed rows (tuples of ``str``/``None``) of a text-format ``COPY ... TO STDOUT``."""
        for chunk in self:
            self._buffer += chunk
            yield from parse_rows(self._buffer, codec=self._conn._codec)
        yield from parse_rows(self._buffer, final=True, codec=self._conn._codec)
