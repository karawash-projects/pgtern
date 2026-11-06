# SPDX-License-Identifier: Apache-2.0
"""Native asyncio API: ``AsyncConnection``, ``AsyncCursor``, ``AsyncConnectionPool``.

Same wire protocol, authentication, TLS and type handling as the blocking driver;
only the I/O is different. If a task is cancelled (or a timeout fires) while a
statement is in flight the connection is closed, because its protocol state is then unknown.

    async with await pgtern.aio.connect("postgresql://...") as conn:
        async with conn.transaction():
            cur = await conn.execute("SELECT %s::int", [1])
            print(await cur.fetchall())
"""
from __future__ import annotations

import asyncio
import collections
import itertools
import logging
import time
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from typing import Any, AsyncIterator, Callable, Iterable, Sequence

from . import _binary
from . import _protocol as P
from . import _query, _types, errors, hooks
from ._conn import (
    _BROKEN, _IDLE, PRIME_SQL, ConnectionInfo, _ssl_context,
)
from ._copy import format_row, parse_rows
from ._cursor import Cursor
from ._dsn import ConnParams, build_params
from ._auth import Authenticator
from .pipeline import _enter_hooks, _encode, _exit_hooks, _Item
from .pool import PoolClosed, PoolTimeout
from .rows import tuple_row
from .sql import SQL, Composable, quote_ident

log = logging.getLogger("pgtern.aio")

__all__ = ["connect", "AsyncConnection", "AsyncCursor", "AsyncServerCursor", "AsyncTransaction",
           "AsyncCopy", "AsyncConnectionPool"]


def _buffered(reader: asyncio.StreamReader) -> int:
    """Bytes the stream has read from the socket but not handed out yet."""
    return len(getattr(reader, "_buffer", b""))  # asyncio keeps them in a private buffer; there is no public accessor


async def _send_cancel_async(host: str, port: int, pid: int, secret: int, tls_mode: str | None,
                             params: ConnParams) -> None:
    """Async twin of ``_conn._send_cancel``."""
    unix = host.startswith("/")
    try:
        if unix:
            reader, writer = await asyncio.open_unix_connection(f"{host}/.s.PGSQL.{port}")
        else:
            reader, writer = await asyncio.open_connection(host, port)
    except OSError as exc:
        raise errors.OperationalError(f'cancel request: connection to server at "{host}", port {port} failed: {exc}') from exc
    try:
        if tls_mode is not None and not unix:
            writer.write(P.SSL_REQUEST)
            await writer.drain()
            if await reader.readexactly(1) != b"S":
                raise errors.OperationalError("the server refused TLS for the cancel request")
            await writer.start_tls(_ssl_context(params, tls_mode), server_hostname=host)
        writer.write(P.cancel_request(pid, secret))
        await writer.drain()
        await reader.read(1)  # the server closes the connection once it has handled the request
    except (OSError, asyncio.IncompleteReadError) as exc:
        raise errors.OperationalError(f"cancel request failed: {exc}") from exc
    finally:
        writer.close()


class AsyncCursor(Cursor):
    """Like :class:`pgtern.Cursor`; fetching is in-memory (so synchronous), executing is awaited."""

    async def execute(self, query: Any, params: Any = None, *, prepare: bool | None = None,  # type: ignore[override]
                      binary: bool | None = None) -> "AsyncCursor":
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        self._set_results(await self.connection._execute(query, params, bool(binary)))
        return self

    async def executemany(self, query: Any, params_seq: Sequence[Any]) -> None:  # type: ignore[override]
        """Run ``query`` once per parameter set as one pipelined batch, in one transaction
        (see ``AsyncConnection._run_many``). ``rowcount`` is the total of the statements' rowcounts."""
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        total = await self.connection._run_many(query, params_seq)
        self._set_results([])
        self._total_rowcount = total

    def copy(self, query: Any, params: Any = None) -> "AsyncCopy":  # type: ignore[override]
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        return AsyncCopy(self, query, params)

    async def close(self) -> None:  # type: ignore[override]
        Cursor.close(self)

    async def __aenter__(self) -> "AsyncCursor":
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()

    def __aiter__(self) -> AsyncIterator:
        return self._aiter()

    async def _aiter(self) -> AsyncIterator:
        while True:
            row = self.fetchone()
            if row is None:
                return
            yield row


class AsyncServerCursor(AsyncCursor):
    """Named server-side cursor (``DECLARE``); rows are fetched in batches with ``await``."""

    itersize = 100

    def __init__(self, connection: Any, name: str, *, row_factory: Callable | None = None,
                 scrollable: bool | None = None, withhold: bool = False) -> None:
        super().__init__(connection, row_factory=row_factory)
        self.name = name
        self._scrollable, self._withhold = scrollable, withhold
        self._binary = False
        self._declared = False
        self._columns = None
        self._fetched = 0
        self._batch = None

    @property
    def description(self):
        return list(self._columns) if self._columns is not None else None

    @property
    def rowcount(self) -> int:
        return self._fetched if self._declared else -1

    @property
    def rownumber(self) -> int | None:
        return self._fetched if self._declared else None

    async def execute(self, query: Any, params: Any = None, *, binary: bool | None = None,  # type: ignore[override]
                      **_: Any) -> "AsyncServerCursor":
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        if self._declared:
            raise errors.ProgrammingError("the server-side cursor was already executed; create a new one")
        self._binary = bool(binary)
        prefix = f"DECLARE {quote_ident(self.name)} "
        if self._scrollable is not None:
            prefix += "SCROLL " if self._scrollable else "NO SCROLL "
        prefix += "CURSOR " + ("WITH HOLD " if self._withhold else "") + "FOR "
        body = query if isinstance(query, Composable) else SQL(query if isinstance(query, str) else bytes(query).decode())
        await self.connection._execute(SQL(prefix) + body, params)
        self._declared = True
        first = (await self.connection._execute(f"FETCH FORWARD 0 FROM {quote_ident(self.name)}", None, self._binary))[0]
        self._columns = first.columns
        self._set_results([first])
        return self

    async def _fetch(self, command: str):
        if not self._declared:
            raise errors.ProgrammingError("the cursor has not been executed")
        result = (await self.connection._execute(f"{command} FROM {quote_ident(self.name)}", None, self._binary))[0]
        if result.columns is None:
            result.columns = self._columns
        self._set_results([result])
        return result

    async def fetchmany(self, size: int | None = None) -> list:  # type: ignore[override]
        result = await self._fetch(f"FETCH FORWARD {int(self.arraysize if size is None else size)}")
        self._fetched += len(result.rows)
        return Cursor.fetchall(self)

    async def fetchone(self) -> Any:  # type: ignore[override]
        rows = await self.fetchmany(1)
        return rows[0] if rows else None

    async def fetchall(self) -> list:  # type: ignore[override]
        result = await self._fetch("FETCH FORWARD ALL")
        self._fetched += len(result.rows)
        return Cursor.fetchall(self)

    async def scroll(self, value: int, mode: str = "relative") -> None:
        if mode not in ("relative", "absolute"):
            raise ValueError(f"bad mode: {mode}; it should be 'relative' or 'absolute'")
        await self._fetch(f"MOVE {mode.upper()} {int(value)}")
        self._fetched = (self._fetched + value) if mode == "relative" else value

    def _aiter(self) -> AsyncIterator:
        return self._server_iter()

    async def _server_iter(self) -> AsyncIterator:
        while True:
            rows = await self.fetchmany(self.itersize)
            for row in rows:
                yield row
            if len(rows) < self.itersize:
                return

    async def close(self) -> None:  # type: ignore[override]
        if self._declared and not self.connection.closed and self.connection._tx_status != "E":
            try:
                await self.connection._execute(f"CLOSE {quote_ident(self.name)}", None)
            except errors.Error:
                pass
        self._declared = False
        Cursor.close(self)


class AsyncTransaction:
    def __init__(self, conn: "AsyncConnection", savepoint_name: str | None = None,
                 force_rollback: bool = False) -> None:
        self._conn, self._name, self._force_rollback = conn, savepoint_name, force_rollback
        self._savepoint: str | None = None

    async def __aenter__(self) -> "AsyncTransaction":
        conn = self._conn
        async with conn._lock:
            conn._check_open()
            if conn._tx_status == _IDLE:
                self._savepoint = None
                await conn._simple("BEGIN")
            else:
                self._savepoint = self._name or f"_pgtern_{next(conn._savepoint_ids)}"
                await conn._simple(f"SAVEPOINT {quote_ident(self._savepoint)}")
            conn._tx_depth += 1
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        conn = self._conn
        async with conn._lock:
            conn._tx_depth = max(0, conn._tx_depth - 1)
            if conn.closed:
                return False
            commit = exc_type is None and not self._force_rollback
            try:
                if self._savepoint is None:
                    await (conn._commit_or_fail() if commit else conn._simple("ROLLBACK"))
                else:
                    sp = quote_ident(self._savepoint)
                    await conn._simple(f"RELEASE {sp}" if commit else f"ROLLBACK TO {sp}; RELEASE {sp}")
            except Exception:
                if exc_type is None:
                    raise
                log.warning("error while ending a failed transaction block", exc_info=True)
        return False


class AsyncCopy:
    """``async with cur.copy("COPY t FROM STDIN") as copy: await copy.write_row(...)``."""

    def __init__(self, cursor: AsyncCursor, query: Any, params: Any = None) -> None:
        self._cursor, self._conn, self._query, self._params = cursor, cursor.connection, query, params
        self._kind = ""
        self._stack = AsyncExitStack()
        self._done = False
        self._buffer = bytearray()
        self._pending_error: dict[str, str] | None = None

    async def __aenter__(self) -> "AsyncCopy":
        conn = self._conn
        await conn._lock.acquire()
        try:
            self._kind = await conn._start_copy(self._query, self._params, self._stack)
        except BaseException:
            await self._stack.aclose()
            conn._lock.release()
            raise
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        conn = self._conn
        try:
            if conn.closed:
                return
            if exc_type is asyncio.CancelledError:
                conn._mark_broken()
                return
            if self._kind == "in":
                if exc_type is not None:
                    await conn._send(P.copy_fail(f"error in Python: {exc_type.__name__}"))
                    try:
                        await conn._read_cycle()
                    except errors.Error:
                        pass
                else:
                    await conn._send(P.COPY_DONE)
                    results = await conn._read_cycle()
                    self._cursor._set_results(results)
            else:
                while not self._done:
                    await self._next_chunk()
                err = self._pending_error
                results = await conn._read_cycle()
                if err is not None:
                    raise errors.error_from_fields(err)
                if exc_type is None:
                    self._cursor._set_results(results)
        finally:
            try:
                await self._stack.__aexit__(exc_type, exc, tb)
            finally:
                conn._lock.release()

    async def write(self, data: bytes | bytearray | memoryview | str) -> None:
        if self._kind != "in":
            raise errors.ProgrammingError("write() needs a COPY ... FROM STDIN statement")
        data = data.encode("utf-8") if isinstance(data, str) else bytes(data)
        for i in range(0, max(len(data), 1), 1 << 20):
            await self._conn._send(P.copy_data(data[i:i + (1 << 20)]))

    async def write_row(self, row: Sequence[Any]) -> None:
        await self.write(format_row(row))

    async def _next_chunk(self) -> bytes | None:
        conn = self._conn
        while True:
            tag, payload = await conn._next_message()
            if tag == 0x64:
                return payload
            if tag == 0x63:
                self._done = True
                return None
            if tag == 0x45:
                self._done = True
                self._pending_error = P.parse_error_fields(payload)
                return None
            conn._handle_async_message(tag, payload)

    async def read(self) -> bytes:
        if self._kind != "out":
            raise errors.ProgrammingError("read() needs a COPY ... TO STDOUT statement")
        if self._done:
            return b""
        chunk = await self._next_chunk()
        return chunk if chunk is not None else b""

    async def __aiter__(self) -> AsyncIterator[bytes]:
        while True:
            chunk = await self.read()
            if not chunk:
                return
            yield chunk

    async def rows(self) -> AsyncIterator[tuple]:
        async for chunk in self:
            self._buffer += chunk
            for row in parse_rows(self._buffer):
                yield row
        for row in parse_rows(self._buffer, final=True):
            yield row


class AsyncConnection:
    """An asyncio connection. Create it with :func:`pgtern.aio.connect`."""

    def __init__(self, params: ConnParams, *, autocommit: bool = False,
                 row_factory: Callable | None = None) -> None:
        self._params = params
        self._autocommit = autocommit
        self.row_factory = row_factory or tuple_row
        self._default_row_factory = self.row_factory
        self._lock = asyncio.Lock()
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._closed = True
        self._parser = P.MessageParser()
        self._pending: collections.deque[tuple[int, bytes]] = collections.deque()
        self._server_params: dict[str, str] = {}
        self._backend_pid = 0
        self._backend_secret = 0
        self._tls_mode: str | None = None
        self._tx_status = _BROKEN
        self._tx_depth = 0
        self._savepoint_ids = itertools.count(1)
        self._host, self._port = "", 0
        self._dates_ok = True
        self._notice_handlers: list[Callable] = []
        self._notify_handlers: list[Callable] = []
        self._notifies: collections.deque[P.Notify] = collections.deque()
        self._loader_cache: dict[int, Any] = {}
        self._pipeline: Any = None
        self.info = ConnectionInfo(self)  # type: ignore[arg-type]
        self.created = time.monotonic()

    # ---------------------------------------------------------------- opening
    @classmethod
    async def connect(cls, conninfo: str = "", *, autocommit: bool = False,
                      row_factory: Callable | None = None, **kwargs: Any) -> "AsyncConnection":
        conn = cls(build_params(conninfo, **kwargs), autocommit=autocommit, row_factory=row_factory)
        await conn._open()
        return conn

    async def _open(self) -> None:
        modes = ["disable", "require"] if self._params.sslmode == "allow" else [self._params.sslmode]
        last: errors.Error | None = None
        for host, port in self._params.hosts:
            for mode in modes:
                try:
                    await asyncio.wait_for(self._connect_host(host, port, mode), self._params.connect_timeout)
                    return
                except errors.Error as exc:
                    last = exc
                except (OSError, asyncio.TimeoutError) as exc:
                    last = errors.OperationalError(
                        f'connection to server at "{host}", port {port} failed: {exc or "timeout expired"}')
                    last.__cause__ = exc
                self._drop()
        assert last is not None
        raise last

    def _drop(self) -> None:
        writer, self._writer, self._reader = self._writer, None, None
        self._closed = True
        self._parser = P.MessageParser()
        self._pending.clear()
        if writer is not None:
            writer.close()

    async def _connect_host(self, host: str, port: int, mode: str) -> None:
        unix = host.startswith("/")
        try:
            if unix:
                reader, writer = await asyncio.open_unix_connection(f"{host}/.s.PGSQL.{port}")
            else:
                reader, writer = await asyncio.open_connection(host, port)
                sock = writer.get_extra_info("socket")
                if sock is not None:
                    import socket as _s
                    sock.setsockopt(_s.IPPROTO_TCP, _s.TCP_NODELAY, 1)
                    sock.setsockopt(_s.SOL_SOCKET, _s.SO_KEEPALIVE, 1)
        except OSError as exc:
            raise errors.OperationalError(f'connection to server at "{host}", port {port} failed: {exc}') from exc
        self._reader, self._writer = reader, writer
        self._host, self._port = host, port
        self._closed = False
        self._parser = P.MessageParser()
        self._pending.clear()
        self._server_params = {}
        self._tls_mode = None
        if not unix and mode != "disable":
            try:
                writer.write(P.SSL_REQUEST)
                await writer.drain()
                answer = await reader.readexactly(1)
                if answer == b"S":
                    if _buffered(reader):
                        # bytes that arrived right behind the 'S' were never encrypted: never hand them to TLS
                        self._drop()
                        raise errors.OperationalError("the server sent unexpected data after its SSL answer")
                    await writer.start_tls(_ssl_context(self._params, mode), server_hostname=host)
                    self._tls_mode = mode
                elif answer == b"N":
                    if mode in ("require", "verify-ca", "verify-full"):
                        raise errors.OperationalError("server does not support SSL, but SSL was required")
                else:
                    raise errors.OperationalError(f"received an invalid response to the SSL request: {answer!r}")
            except (OSError, asyncio.IncompleteReadError) as exc:
                self._drop()
                if mode == "prefer":
                    return await self._connect_host(host, port, "disable")
                raise errors.OperationalError(
                    f'SSL connection to server at "{host}", port {port} failed: {exc}') from exc
        await self._startup()
        self._dates_ok = self._server_params.get("DateStyle", "ISO").upper().startswith("ISO")

    async def _startup(self) -> None:
        p = self._params
        startup = {"user": p.user, "database": p.dbname, "client_encoding": "UTF8", "DateStyle": "ISO"}
        if p.application_name:
            startup["application_name"] = p.application_name
        if p.options:
            startup["options"] = p.options
        await self._send(P.startup_message(startup))
        encrypted = self._tls_mode is not None or self._host.startswith("/")
        auth = Authenticator(p.user, p.password, encrypted=encrypted, allow_plaintext=p.sslmode == "disable",
                             require_scram=p.require_auth == "scram-sha-256")
        while True:
            tag, payload = await self._next_message()
            if tag == 0x52:  # R Authentication*
                reply = auth.step(int.from_bytes(payload[:4], "big"), payload[4:])
                if reply:
                    await self._send(reply)
            elif tag == 0x45:
                raise errors.error_from_fields(P.parse_error_fields(payload))
            elif tag == 0x4B:
                self._backend_pid = int.from_bytes(payload[:4], "big")
                self._backend_secret = int.from_bytes(payload[4:8], "big")
            elif tag == 0x5A:
                self._tx_status = chr(payload[0])
                return
            else:
                self._handle_async_message(tag, payload)

    # ---------------------------------------------------------------- I/O
    def _mark_broken(self) -> None:
        self._closed = True
        self._tx_status = _BROKEN
        writer, self._writer, self._reader = self._writer, None, None
        if writer is not None:
            writer.close()

    async def _send(self, data: bytes) -> None:
        writer = self._writer
        if writer is None:
            raise errors.OperationalError("the connection is closed")
        try:
            writer.write(data)
            await writer.drain()
        except (OSError, ConnectionError) as exc:
            self._mark_broken()
            raise errors.OperationalError(f"connection failed while sending: {exc}") from exc
        except BaseException:
            self._mark_broken()  # cancelled mid-write: the stream is in an unknown state
            raise

    def _write(self, data: bytes) -> None:
        """Buffer bytes for the socket without waiting: pipelines drain at their flush points."""
        writer = self._writer
        if writer is None:
            raise errors.OperationalError("the connection is closed")
        writer.write(data)

    async def _drain(self) -> None:
        writer = self._writer
        if writer is None:
            raise errors.OperationalError("the connection is closed")
        try:
            await writer.drain()
        except (OSError, ConnectionError) as exc:
            self._mark_broken()
            raise errors.OperationalError(f"connection failed while sending: {exc}") from exc
        except BaseException:
            self._mark_broken()
            raise

    async def _next_message(self) -> tuple[int, bytes]:
        while not self._pending:
            reader = self._reader
            if reader is None:
                raise errors.OperationalError("the connection is closed")
            try:
                data = await reader.read(65536)
            except (OSError, ConnectionError) as exc:
                self._mark_broken()
                raise errors.OperationalError(f"connection failed while receiving: {exc}") from exc
            except asyncio.CancelledError:
                # cancelled mid-statement (e.g. a timeout): stop the query on the server, then drop the connection
                await self._cancel_quietly()
                self._mark_broken()
                raise
            except BaseException:
                self._mark_broken()
                raise
            if not data:
                self._mark_broken()
                raise errors.OperationalError("server closed the connection unexpectedly")
            self._pending.extend(self._parser.feed(data))
        return self._pending.popleft()

    def _notice(self, payload: bytes) -> None:
        diag = errors.Diagnostic(P.parse_error_fields(payload))
        for handler in self._notice_handlers:
            handler(diag)
        if not self._notice_handlers:
            log.debug("server notice: %s", diag.message_primary)

    def add_notice_handler(self, handler: Callable) -> None:
        self._notice_handlers.append(handler)

    def add_notify_handler(self, handler: Callable) -> None:
        self._notify_handlers.append(handler)

    def _handle_async_message(self, tag: int, payload: bytes) -> None:
        if tag == 0x4E:
            self._notice(payload)
        elif tag == 0x53:
            name, value = P.parse_parameter_status(payload)
            self._server_params[name] = value
        elif tag == 0x41:
            note = P.parse_notification(payload)
            if self._notify_handlers:
                for handler in self._notify_handlers:
                    handler(note)
            else:
                self._notifies.append(note)

    async def notifies(self, *, timeout: float | None = None, stop_after: int | None = None) -> AsyncIterator[P.Notify]:
        """``async for note in conn.notifies(timeout=5): ...`` (after ``LISTEN``)."""
        count = 0
        while stop_after is None or count < stop_after:
            async with self._lock:
                self._check_open()
                if not self._notifies:
                    try:
                        await asyncio.wait_for(self._wait_notify(), timeout)
                    except asyncio.TimeoutError:
                        pass
                note = self._notifies.popleft() if self._notifies else None
            if note is None:
                return
            count += 1
            yield note

    async def _wait_notify(self) -> None:
        # Never leaves a partial message behind: messages are only taken whole from the parser.
        while not self._notifies:
            tag, payload = await self._next_message_keep_open()
            if tag == 0x45:
                self._mark_broken()
                raise errors.error_from_fields(P.parse_error_fields(payload))
            self._handle_async_message(tag, payload)

    async def _next_message_keep_open(self) -> tuple[int, bytes]:
        """Like _next_message, but a cancelled wait (timeout) does not poison the connection."""
        while not self._pending:
            reader = self._reader
            if reader is None:
                raise errors.OperationalError("the connection is closed")
            try:
                data = await reader.read(65536)
            except (OSError, ConnectionError) as exc:
                self._mark_broken()
                raise errors.OperationalError(f"connection failed while receiving: {exc}") from exc
            if not data:
                self._mark_broken()
                raise errors.OperationalError("server closed the connection unexpectedly")
            self._pending.extend(self._parser.feed(data))
        return self._pending.popleft()

    async def _read_cycle(self) -> list[P.Result]:
        collector = P.QueryCollector()
        error: dict[str, str] | None = None
        unsupported = False
        while True:
            tag, payload = await self._next_message()
            if tag == 0x5A:
                self._tx_status = chr(payload[0])
                break
            if tag == 0x45:
                fields = P.parse_error_fields(payload)
                error = error or fields
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    self._mark_broken()
                    raise errors.error_from_fields(fields)
            elif tag in (0x4E, 0x53, 0x41):
                self._handle_async_message(tag, payload)
            elif tag == 0x47:
                unsupported = True
                await self._send(P.copy_fail("COPY needs cursor.copy()"))
            elif tag in (0x48, 0x57, 0x64, 0x63):
                unsupported = True
            else:
                collector.handle(tag, payload)
        if unsupported:
            raise errors.NotSupportedError("COPY must be run with cursor.copy(), not execute()")
        if error is not None:
            raise errors.error_from_fields(error)
        return collector.results

    def _check_open(self) -> None:
        if self._closed:
            raise errors.OperationalError("the connection is closed")

    def _check_alive(self) -> bool:
        reader, writer = self._reader, self._writer
        if self._closed or reader is None or writer is None:
            return False
        if reader.at_eof() or writer.is_closing() or reader.exception() is not None:
            self._mark_broken()
            return False
        # An idle connection must have nothing buffered. If the server shut it down, its FATAL
        # ErrorResponse is sitting in the buffer (and at_eof() is False until it is read).
        buffered = bytes(getattr(reader, "_buffer", b""))
        if buffered:
            try:
                if any(tag == 0x45 for tag, _ in P.MessageParser().feed(buffered)):
                    self._mark_broken()
                    return False
            except errors.Error:
                self._mark_broken()
                return False
        return True

    # ---------------------------------------------------------------- execution
    async def _simple(self, sql: str) -> list[P.Result]:
        await self._send(P.query_message(sql))
        return await self._read_cycle()

    async def _send_query(self, sql: str, params: Any, *, inline: bool = False) -> None:
        begin = not self._autocommit and self._tx_status == _IDLE
        if inline and params is not None:
            sql, params = _query.translate(sql, params, _types.render_literal)[0], None
        body = P.build_query(sql, params, _query.translate, _types.dump)
        if begin:
            await self._send(P.query_message("BEGIN") + body)
            try:
                await self._read_cycle()
            except BaseException:
                self._mark_broken()
                raise
        else:
            await self._send(body)

    def _query_text(self, query: Any) -> tuple[str, str]:
        if isinstance(query, Composable):
            return query.as_string(), query.as_redacted()
        if isinstance(query, str):
            return query, query
        if isinstance(query, (bytes, bytearray)):
            text = bytes(query).decode()
            return text, text
        raise TypeError(f"the query must be str, bytes or sql.Composable, not {type(query).__name__}")

    async def _start_copy(self, query: Any, params: Any, stack: AsyncExitStack) -> str:
        sql, redacted = self._query_text(query)
        self._check_open()
        if hooks._HOOKS:
            info = hooks.QueryInfo(redacted, self._host, self._port, self._params.dbname, self._params.user)
            for hook in tuple(hooks._HOOKS):
                try:
                    cm = hook(info)
                    if isinstance(cm, AbstractAsyncContextManager):
                        await stack.enter_async_context(cm)
                    else:
                        stack.enter_context(cm)
                except Exception:
                    log.exception("a pgtern query hook failed on entry; ignoring it")
        await self._send_query(sql, params, inline=True)
        error: dict[str, str] | None = None
        while True:
            tag, payload = await self._next_message()
            if error is None and tag in (0x47, 0x48):
                return "in" if tag == 0x47 else "out"
            if tag == 0x45:
                fields = P.parse_error_fields(payload)
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    self._mark_broken()
                    raise errors.error_from_fields(fields)
                error = error or fields
            elif tag == 0x5A:
                self._tx_status = chr(payload[0])
                if error is not None:
                    raise errors.error_from_fields(error)
                raise errors.ProgrammingError("the statement is not a COPY ... FROM STDIN / TO STDOUT")
            else:
                self._handle_async_message(tag, payload)

    async def _prime_oids(self, oids: Iterable[int]) -> None:
        if not _types.has_loaders():
            return
        wanted = set(oids) - self._loader_cache.keys()
        if not wanted:
            return
        ids = ", ".join(str(int(o)) for o in sorted(wanted))
        rows = (await self._simple(PRIME_SQL.format(ids=ids)))[0].rows
        _types.resolve_loaders(rows, self._loader_cache, sorted(wanted))

    async def _prime_loaders(self, results: list[P.Result]) -> None:
        await self._prime_oids(c.type_code for r in results if r.columns for c in r.columns)

    async def _describe(self, sql: str) -> tuple[list[int], list[P.Column] | None]:
        await self._send(P.describe_statement(sql))
        param_oids: list[int] = []
        columns: list[P.Column] | None = None
        error: dict[str, str] | None = None
        while True:
            tag, payload = await self._next_message()
            if tag == 0x5A:
                self._tx_status = chr(payload[0])
                break
            if tag == 0x74:
                param_oids = P.parse_param_description(payload)
            elif tag == 0x54:
                columns = P.parse_row_description(payload)
            elif tag == 0x45:
                fields = P.parse_error_fields(payload)
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    self._mark_broken()
                    raise errors.error_from_fields(fields)
                error = error or fields
            elif tag in (0x31, 0x6E):
                pass
            else:
                self._handle_async_message(tag, payload)
        if error is not None:
            raise errors.error_from_fields(error)
        return param_oids, columns

    async def _run_binary(self, sql: str, params: Any) -> list[P.Result]:
        begin = not self._autocommit and self._tx_status == _IDLE
        if begin:
            await self._simple("BEGIN")
        values: list[Any]
        if params is None:
            pg_sql, values = sql, []
        else:
            pg_sql, values = _query.translate(sql, params)
        param_oids, columns = await self._describe(pg_sql)
        if len(param_oids) != len(values):
            raise errors.ProgrammingError(
                f"the query has {len(param_oids)} parameters but {len(values)} were passed")
        pformats, payloads = _types.binary_param_plan(param_oids, values)
        rformats = None
        if columns is not None:
            await self._prime_oids(c.type_code for c in columns)
            rformats = [1 if _binary.scalar_decoder(c.type_code) is not None and self._loader_cache.get(c.type_code) is None
                        else 0 for c in columns]
        await self._send(P.extended_query(pg_sql, param_oids, payloads, pformats, rformats))
        results = await self._read_cycle()
        for r in results:
            if r.columns is not None:
                r.formats = rformats
        return results

    async def _run_text(self, sql: str, params: Any) -> list[P.Result]:
        await self._send_query(sql, params)
        return await self._read_cycle()

    def _check_no_pipeline(self) -> None:
        if self._pipeline is not None:
            raise errors.ProgrammingError(
                "a pipeline is active on this connection: queue statements with pipe.execute() "
                "or leave the pipeline block first")

    async def _execute(self, query: Any, params: Any, binary: bool = False) -> list[P.Result]:
        sql, redacted = self._query_text(query)
        run = self._run_binary if binary else self._run_text
        async with self._lock:
            self._check_open()
            self._check_no_pipeline()
            if not hooks._HOOKS:
                results = await run(sql, params)
            else:
                info = hooks.QueryInfo(redacted, self._host, self._port, self._params.dbname, self._params.user)
                from contextlib import ExitStack
                with ExitStack() as stack:
                    for hook in tuple(hooks._HOOKS):
                        try:
                            stack.enter_context(hook(info))
                        except Exception:
                            log.exception("a pgtern query hook failed on entry; ignoring it")
                    results = await run(sql, params)
                    info.rowcount = results[-1].rowcount if results else -1
            await self._prime_loaders(results)
            return results

    async def _run_many(self, sql: str, params_seq: Iterable[Any]) -> int:
        """``executemany`` as one pipeline (async twin of ``Connection._run_many``): every statement is
        sent before its reply is read, one transaction per batch (BEGIN..COMMIT under autocommit,
        the implicit transaction otherwise), replies read through a sliding window with Flush."""
        from contextlib import ExitStack

        from .pipeline import _encode  # internal helper; imported here to avoid a cycle

        window = 64
        pg_sql, redacted = self._query_text(sql)
        async with self._lock:
            self._check_open()
            self._check_no_pipeline()
            own_tx = self._autocommit and self._tx_status == _IDLE
            if own_tx or (not self._autocommit and self._tx_status == _IDLE):
                await self._send(P.query_message("BEGIN"))
                await self._read_cycle()
            total = 0
            in_flight: collections.deque = collections.deque()
            failure: BaseException | None = None
            sent_sync = False

            def enter_hooks() -> tuple[ExitStack, Any]:
                stack = ExitStack()
                info = None
                if hooks._HOOKS:
                    info = hooks.QueryInfo(redacted, self._host, self._port, self._params.dbname, self._params.user)
                    for hook in tuple(hooks._HOOKS):
                        try:
                            stack.enter_context(hook(info))
                        except Exception:
                            log.exception("a pgtern query hook failed on entry; ignoring it")
                return stack, info

            async def read_one() -> errors.Error | None:
                nonlocal total
                stack, info = in_flight.popleft()
                results, error = await AsyncPipeline._read_reply(self)
                if results is not None:
                    rc = results[-1].rowcount if results else -1
                    if info is not None:
                        info.rowcount = rc
                    if rc > 0:
                        total += rc
                if error is None:
                    stack.__exit__(None, None, None)
                else:
                    stack.__exit__(type(error), error, None)
                return error

            try:
                pending: list[bytes] = []  # encoded statements not yet handed to the socket
                for params in params_seq:
                    pending.append(_encode(pg_sql, params))  # no Sync: the batch ends with one
                    in_flight.append(enter_hooks())
                    if len(pending) >= window:  # coalesce: one socket write per window, not per statement
                        self._write(b"".join(pending))
                        pending.clear()
                    if len(in_flight) > window:
                        self._write(b"".join(pending) + P.FLUSH)  # Flush: the server sends replies so far
                        pending.clear()
                        await self._drain()
                        while len(in_flight) > window // 2:
                            failure = await read_one()
                            if failure is not None:
                                break  # statements sent after a failure are skipped by the server
                        if failure is not None:
                            break
                if failure is not None:
                    pending.clear()  # the server would skip these anyway
                self._write(b"".join(pending) + P.SYNC)
                pending.clear()
                await self._drain()
                sent_sync = True
                while failure is None and in_flight:
                    failure = await read_one()
            except BaseException as exc:
                failure = exc
                if not self._closed:
                    # discard the replies still unread, up to the batch's ReadyForQuery (see the blocking twin)
                    try:
                        if not sent_sync:
                            await self._send(P.SYNC)
                        while True:
                            tag, payload = await self._next_message()
                            if tag == 0x5A:
                                self._tx_status = chr(payload[0])
                                break
                        if own_tx and self._tx_status != _IDLE:
                            await self._simple("ROLLBACK")
                    except Exception:
                        self._mark_broken()
                raise
            finally:
                while in_flight:
                    stack, _ = in_flight.popleft()
                    if failure is None:
                        stack.__exit__(None, None, None)
                    else:
                        stack.__exit__(type(failure), failure, None)
            while True:
                tag, payload = await self._next_message()
                if tag == 0x5A:
                    self._tx_status = chr(payload[0])
                    break
                if tag == 0x45:
                    fields = P.parse_error_fields(payload)
                    if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                        self._mark_broken()
                        raise errors.error_from_fields(fields)
                else:
                    self._handle_async_message(tag, payload)
            if own_tx:
                await self._simple("ROLLBACK" if failure is not None else "COMMIT")
            if failure is not None:
                raise failure
            return total

    # ---------------------------------------------------------------- public API
    def cursor(self, name: str | None = None, *, row_factory: Callable | None = None,
               scrollable: bool | None = None, withhold: bool = False) -> AsyncCursor:
        if name is not None:
            return AsyncServerCursor(self, name, row_factory=row_factory, scrollable=scrollable, withhold=withhold)
        return AsyncCursor(self, row_factory=row_factory)

    async def execute(self, query: Any, params: Any = None, *, prepare: bool | None = None,
                      binary: bool | None = None) -> AsyncCursor:
        return await self.cursor().execute(query, params, binary=bool(binary))

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    async def set_autocommit(self, value: bool) -> None:
        async with self._lock:
            if bool(value) != self._autocommit and self._tx_status not in (_IDLE, _BROKEN):
                raise errors.ProgrammingError("can't change 'autocommit' now: connection in a transaction")
            self._autocommit = bool(value)

    async def commit(self) -> None:
        async with self._lock:
            self._check_open()
            self._check_no_pipeline()
            if self._tx_depth:
                raise errors.ProgrammingError("explicit commit() is forbidden inside a transaction() block")
            if self._tx_status != _IDLE:
                await self._commit_or_fail()

    async def rollback(self) -> None:
        async with self._lock:
            self._check_open()
            self._check_no_pipeline()
            if self._tx_depth:
                raise errors.ProgrammingError("explicit rollback() is forbidden inside a transaction() block")
            if self._tx_status != _IDLE:
                await self._simple("ROLLBACK")

    def transaction(self, savepoint_name: str | None = None, force_rollback: bool = False) -> AsyncTransaction:
        self._check_no_pipeline()
        return AsyncTransaction(self, savepoint_name, force_rollback)

    def pipeline(self, batch_size: int = 64) -> "AsyncPipeline":
        """Pipeline mode (asyncio). See :mod:`pgtern.pipeline`."""
        return AsyncPipeline(self, batch_size)

    async def cancel(self) -> None:
        """Ask the server to cancel the statement running on this connection (see ``Connection.cancel``)."""
        if self._closed or not self._backend_pid:
            raise errors.OperationalError("the connection is closed")
        await _send_cancel_async(self._host, self._port, self._backend_pid, self._backend_secret,
                                 self._tls_mode, self._params)

    async def _cancel_quietly(self) -> None:
        if self._closed or not self._backend_pid:
            return
        try:
            await asyncio.wait_for(self.cancel(), 2.0)
        except Exception:  # noqa: BLE001 - best effort: the connection is dropped right after
            pass

    async def _commit_or_fail(self) -> None:
        """COMMIT that raises when the server rolled the transaction back instead (it had failed)."""
        results = await self._simple("COMMIT")
        if results and results[-1].tag == "ROLLBACK":
            raise errors.error_from_fields({"S": "ERROR", "C": "25P02", "M": (
                "the transaction was rolled back, not committed: a statement in it had failed")})

    async def _reset_for_pool(self) -> None:
        async with self._lock:
            self._loader_cache.clear()
            self._tx_depth = 0
            if self._closed:
                return
            if self._tx_status != _IDLE:
                await self._simple("ROLLBACK")
            await self._simple("DISCARD ALL")  # roles, search_path, temp tables, LISTEN, advisory locks, cursors
            self.row_factory = self._default_row_factory
            self._notice_handlers.clear()
            self._notify_handlers.clear()
            self._notifies.clear()

    async def close(self) -> None:
        if self._closed:
            return
        writer = self._writer
        self._closed = True
        self._tx_status = _BROKEN
        self._reader = self._writer = None
        if writer is not None:
            try:
                writer.write(P.TERMINATE)
                await writer.drain()
            except (OSError, ConnectionError):
                pass
            writer.close()
            try:
                await writer.wait_closed()
            except (OSError, ConnectionError):
                pass

    async def __aenter__(self) -> "AsyncConnection":
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if not self._closed:
                if exc_type is None:
                    await self.commit()
                elif exc_type is asyncio.CancelledError:
                    self._mark_broken()
                else:
                    try:
                        await self.rollback()
                    except Exception:
                        log.warning("rollback failed while leaving a connection block", exc_info=True)
        finally:
            await self.close()


class AsyncPipeline:
    """asyncio pipeline: ``async with conn.pipeline() as pipe: h = await pipe.execute(...)``.
    Same semantics as :class:`pgtern.pipeline.Pipeline`."""

    def __init__(self, conn: AsyncConnection, batch_size: int = 64) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._conn = conn
        self._batch_size = batch_size
        self._queue: list[_Item] = []
        self._begin_queued = False
        self._open = False

    async def __aenter__(self) -> "AsyncPipeline":
        conn = self._conn
        async with conn._lock:
            conn._check_open()
            if conn._pipeline is not None:
                raise errors.ProgrammingError("a pipeline is already active on this connection")
            conn._pipeline = self
        self._open = True
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if exc_type is None:
                await self.sync()
            elif not self._conn.closed:
                try:
                    await self.sync()
                except Exception:  # noqa: BLE001 - the body's exception is the one to report
                    pass
        finally:
            self._open = False
            if self._conn._pipeline is self:
                self._conn._pipeline = None

    async def execute(self, query: Any, params: Any = None) -> AsyncCursor:
        conn = self._conn
        if self._open and len(self._queue) >= self._batch_size:
            await self.sync()  # before taking the lock: sync() takes it itself
        async with conn._lock:
            conn._check_open()
            if not self._open:
                raise errors.ProgrammingError("the pipeline is not active (use it in an 'async with' block)")
            sql, redacted = conn._query_text(query)
            if not conn._autocommit and conn._tx_status == _IDLE and not self._begin_queued:
                await conn._send(P.extended_query("BEGIN", [], [], sync=False))
                self._queue.append(_Item(None, "BEGIN", "BEGIN", hidden=True))
                self._begin_queued = True
            cursor = AsyncCursor(conn)
            cursor._pipeline_pending = True
            await conn._send(_encode(sql, params))
            self._queue.append(_Item(cursor, sql, redacted))
            return cursor

    async def sync(self) -> None:
        conn = self._conn
        async with conn._lock:
            conn._check_open()
            items, self._queue = self._queue, []
            self._begin_queued = False
            await conn._send(P.SYNC)
            failure: errors.Error | None = None
            # Items are shared with the sync pipeline (typed Cursor); here the cursor is always an AsyncCursor.
            collected: list[tuple[Any, list]] = []
            for item in items:
                if failure is not None:
                    if item.cursor is not None:
                        item.cursor._pipeline_pending = False
                        item.cursor._pipeline_error = errors.PipelineAborted(
                            "not executed: an earlier statement in the pipeline failed")
                    continue
                info, entered = _enter_hooks(conn, item)
                results, error = await self._read_reply(conn)
                if info is not None and results is not None:
                    info.rowcount = results[-1].rowcount if results else -1
                _exit_hooks(entered, error)
                if error is not None:
                    failure = error
                    if item.cursor is not None:
                        item.cursor._pipeline_pending = False
                        item.cursor._pipeline_error = error
                elif item.cursor is not None and results is not None:
                    collected.append((item.cursor, results))
            while True:
                tag, payload = await conn._next_message()
                if tag == 0x5A:
                    conn._tx_status = chr(payload[0])
                    break
                if tag == 0x45:
                    fields = P.parse_error_fields(payload)
                    if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                        conn._mark_broken()
                        raise errors.error_from_fields(fields)
                else:
                    conn._handle_async_message(tag, payload)
            await conn._prime_oids(c.type_code for _, res in collected for r in res if r.columns for c in r.columns)
            for cursor, res in collected:
                cursor._pipeline_pending = False
                cursor._set_results(res)
            if failure is not None:
                raise failure  # the cursors hold it too; raising here means a failure cannot pass silently

    @staticmethod
    async def _read_reply(conn: AsyncConnection) -> tuple[list | None, errors.Error | None]:
        collector = P.QueryCollector()
        while True:
            tag, payload = await conn._next_message()
            if tag == 0x45:
                fields = P.parse_error_fields(payload)
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    conn._mark_broken()
                    raise errors.error_from_fields(fields)
                return None, errors.error_from_fields(fields)
            if tag in (0x43, 0x49):
                collector.handle(tag, payload)
                return collector.results, None
            if tag in (0x4E, 0x53, 0x41):
                conn._handle_async_message(tag, payload)
            else:
                collector.handle(tag, payload)


async def connect(conninfo: str = "", **kwargs: Any) -> AsyncConnection:
    """Open an :class:`AsyncConnection` (same arguments as :func:`pgtern.connect`)."""
    return await AsyncConnection.connect(conninfo, **kwargs)


class AsyncConnectionPool:
    """asyncio connection pool: ``min_size`` kept warm, up to ``max_size`` on demand.

    Connections are liveness-checked on checkout, replaced when dead or older than
    ``max_lifetime``, and rolled back when returned mid-transaction.
    """

    def __init__(self, conninfo: str = "", *, kwargs: dict[str, Any] | None = None, min_size: int = 4,
                 max_size: int | None = None, timeout: float = 30.0, max_lifetime: float = 3600.0,
                 check: Callable | None = None, name: str | None = None) -> None:
        max_size = min_size if max_size is None else max_size
        if min_size < 0 or max_size < 1 or max_size < min_size:
            raise ValueError(f"bad pool sizes: min_size={min_size}, max_size={max_size}")
        self.conninfo, self.kwargs = conninfo, dict(kwargs or {})
        self.min_size, self.max_size = min_size, max_size
        self.timeout, self.max_lifetime = timeout, max_lifetime
        self.name = name or f"apool-{id(self):x}"
        self._check = check
        self._idle: collections.deque[AsyncConnection] = collections.deque()
        self._out: set[AsyncConnection] = set()  # checked out and not yet returned
        self._slots: asyncio.Semaphore | None = None
        self._closed = True
        self._default_autocommit = bool(self.kwargs.get("autocommit", False))

    async def open(self, wait: bool = True) -> None:
        if not self._closed:
            return
        self._closed = False
        self._slots = asyncio.Semaphore(self.max_size)
        if wait:
            conns = [await self._new() for _ in range(self.min_size)]
            self._idle.extend(conns)

    async def _new(self) -> AsyncConnection:
        return await connect(self.conninfo, **self.kwargs)

    async def getconn(self, timeout: float | None = None) -> AsyncConnection:
        if self._closed or self._slots is None:
            raise PoolClosed("the pool is closed")
        wait = self.timeout if timeout is None else timeout
        try:
            await asyncio.wait_for(self._slots.acquire(), wait)
        except asyncio.TimeoutError:
            raise PoolTimeout(f"couldn't get a connection after {wait:.2f} sec") from None
        try:
            while self._idle:
                conn = self._idle.popleft()
                if conn._check_alive() and time.monotonic() - conn.created < self.max_lifetime:
                    if await self._passes_check(conn):
                        self._out.add(conn)
                        return conn
                await conn.close()
            conn = await asyncio.wait_for(self._new(), wait)
            self._out.add(conn)
            return conn
        except BaseException:
            self._slots.release()
            raise

    async def _passes_check(self, conn: AsyncConnection) -> bool:
        if self._check is None:
            return True
        try:
            await self._check(conn)
            return True
        except Exception:
            return False

    async def putconn(self, conn: AsyncConnection) -> None:
        assert self._slots is not None
        if conn not in self._out:  # a second return would also release a second slot
            raise errors.ProgrammingError("this connection was not checked out from the pool, or was already returned")
        self._out.discard(conn)
        try:
            if not self._closed and not conn.closed:
                try:
                    await conn._reset_for_pool()
                    if conn.autocommit != self._default_autocommit:
                        await conn.set_autocommit(self._default_autocommit)
                except Exception:
                    await conn.close()
            if self._closed or conn.closed or time.monotonic() - conn.created >= self.max_lifetime:
                await conn.close()
            else:
                self._idle.append(conn)
        finally:
            self._slots.release()

    @asynccontextmanager
    async def connection(self, timeout: float | None = None) -> AsyncIterator[AsyncConnection]:
        """Check a connection out; commit on success, roll back on error, then return it."""
        conn = await self.getconn(timeout)
        try:
            try:
                yield conn
            except BaseException:
                if conn.closed or isinstance(_current_exc(), asyncio.CancelledError):
                    conn._mark_broken()
                else:
                    try:
                        await conn.rollback()
                    except Exception:
                        await conn.close()
                raise
            else:
                if not conn.closed:
                    await conn.commit()
        finally:
            await self.putconn(conn)

    async def close(self) -> None:
        self._closed = True
        while self._idle:
            await self._idle.popleft().close()

    async def __aenter__(self) -> "AsyncConnectionPool":
        await self.open()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.close()


def _current_exc() -> BaseException | None:
    import sys
    return sys.exc_info()[1]
