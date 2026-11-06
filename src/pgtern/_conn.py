# SPDX-License-Identifier: Apache-2.0
"""Blocking-socket PostgreSQL connection built on the sans-I/O protocol layer."""
from __future__ import annotations

import collections
import enum
import itertools
import logging
import os
import re
import select
import socket
import ssl
import threading
import time
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal

from . import _binary
from . import _protocol as P
from . import _query, _types, errors, hooks
from ._cursor import Cursor, ServerCursor
from ._dsn import ConnParams, build_params, resolve_password
from ._auth import Authenticator
from .rows import tuple_row
from .sql import Composable, quote_ident

if TYPE_CHECKING:
    from .pipeline import Pipeline

log = logging.getLogger("pgtern")

_IDLE, _INTRANS, _INERROR, _BROKEN = "I", "T", "E", "?"
PRIME_SQL = ('SELECT t.oid, n.nspname, t.typname, t.typelem, t.typcategory FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE t.oid IN ({ids}) OR t.oid IN (SELECT typelem FROM pg_type WHERE oid IN ({ids}) AND typelem <> 0)')


class TransactionStatus(enum.Enum):
    IDLE = "I"
    INTRANS = "T"
    INERROR = "E"
    UNKNOWN = "?"


_VERSION_RE = re.compile(r"^(\d+)(?:\.(\d+))?(?:\.(\d+))?")


def _parse_version(text: str) -> int:
    m = _VERSION_RE.match(text)
    if not m:
        return 0
    major, minor, patch = int(m.group(1)), int(m.group(2) or 0), int(m.group(3) or 0)
    return major * 10000 + minor if major >= 10 else major * 10000 + minor * 100 + patch


class ConnectionInfo:
    def __init__(self, conn: "Connection") -> None:
        self._conn = conn

    @property
    def user(self) -> str:
        return self._conn._params.user

    @property
    def dbname(self) -> str:
        return self._conn._params.dbname

    @property
    def host(self) -> str:
        return self._conn._host

    @property
    def port(self) -> int:
        return self._conn._port

    @property
    def server_version(self) -> int:
        return _parse_version(self._conn._server_params.get("server_version", ""))

    @property
    def backend_pid(self) -> int:
        return self._conn._backend_pid

    @property
    def encoding(self) -> str:
        return self._conn._server_params.get("client_encoding", "UTF8")

    @property
    def transaction_status(self) -> TransactionStatus:
        return TransactionStatus(self._conn._tx_status)

    def parameter_status(self, name: str) -> str | None:
        return self._conn._server_params.get(name)


def _ssl_context(params: ConnParams, mode: str) -> ssl.SSLContext:
    if mode in ("require", "prefer", "allow"):  # encrypt without verifying the server
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
    else:
        root = params.sslrootcert
        ctx = ssl.create_default_context(cafile=root if root and root != "system" else None)
        ctx.check_hostname = mode == "verify-full"
        ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    if params.sslcert:
        ctx.load_cert_chain(params.sslcert, params.sslkey, params.sslpassword)
    if params.sslcrl and mode in ("verify-ca", "verify-full"):
        ctx.load_verify_locations(cafile=params.sslcrl)
        ctx.verify_flags |= ssl.VERIFY_CRL_CHECK_CHAIN
    return ctx


def _tune_keepalive(sock: socket.socket) -> None:
    """Notice a half-open TCP peer (failover, NAT timeout, host crash) in about a minute, not two hours."""
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 10)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 3)


def _send_cancel(host: str, port: int, pid: int, secret: int, tls_mode: str | None,
                 params: ConnParams) -> None:
    """Send a CancelRequest on a new connection. TLS is used when the original connection used it,
    so the secret key never travels in clear text."""
    unix = host.startswith("/")
    try:
        if unix:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(params.connect_timeout)
            sock.connect(f"{host}/.s.PGSQL.{port}")
        else:
            sock = socket.create_connection((host, port), timeout=params.connect_timeout)
    except OSError as exc:
        raise errors.OperationalError(f'cancel request: connection to server at "{host}", port {port} failed: {exc}') from exc
    try:
        if tls_mode is not None and not unix:
            sock.sendall(P.SSL_REQUEST)
            if sock.recv(1) != b"S":
                raise errors.OperationalError("the server refused TLS for the cancel request")
            sock = _ssl_context(params, tls_mode).wrap_socket(sock, server_hostname=host)
        sock.sendall(P.cancel_request(pid, secret))
        sock.recv(1)  # the server closes the connection once it has handled the request
    except (OSError, ssl.SSLError) as exc:
        raise errors.OperationalError(f"cancel request failed: {exc}") from exc
    finally:
        sock.close()


class Transaction:
    """``with conn.transaction():`` BEGIN/COMMIT, or SAVEPOINT/RELEASE when already in a transaction."""

    def __init__(self, conn: "Connection", savepoint_name: str | None = None,
                 force_rollback: bool = False) -> None:
        self._conn = conn
        self._name = savepoint_name
        self._force_rollback = force_rollback
        self._savepoint: str | None = None

    def __enter__(self) -> "Transaction":
        conn = self._conn
        with conn._lock:
            conn._check_open()
            if conn._tx_status == _IDLE:
                self._savepoint = None
                conn._simple("BEGIN")
            else:
                self._savepoint = self._name or f"_pgtern_{next(conn._savepoint_ids)}"
                conn._simple(f"SAVEPOINT {quote_ident(self._savepoint)}")
            conn._tx_stack.append(self)
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> Literal[False]:
        conn = self._conn
        with conn._lock:
            if conn._tx_stack and conn._tx_stack[-1] is self:
                conn._tx_stack.pop()
            if conn.closed:
                return False
            commit = exc_type is None and not self._force_rollback
            try:
                if self._savepoint is None:
                    conn._commit_or_fail() if commit else conn._simple("ROLLBACK")
                else:
                    sp = quote_ident(self._savepoint)
                    conn._simple(f"RELEASE {sp}" if commit else f"ROLLBACK TO {sp}; RELEASE {sp}")
            except Exception:
                if exc_type is None:
                    raise
                log.warning("error while ending a failed transaction block", exc_info=True)
        return False


class Connection:
    """A synchronous connection. Create it with :func:`pgtern.connect`."""

    def __init__(self, params: ConnParams, *, autocommit: bool = False,
                 row_factory: Callable | None = None) -> None:
        self._params = params
        self._pid = os.getpid()  # a forked child must never talk on this socket
        self._session_requirement = "any"
        self._autocommit = autocommit
        self.row_factory = row_factory or tuple_row
        self._default_row_factory = self.row_factory
        self._lock = threading.RLock()
        self._sock: socket.socket | None = None
        self._closed = True
        self._parser = P.MessageParser()
        self._pending: collections.deque[tuple[int, bytes]] = collections.deque()
        self._server_params: dict[str, str] = {}
        self._backend_pid = 0
        self._backend_secret = 0
        self._tls_mode: str | None = None
        self._tx_status = _BROKEN
        self._tx_stack: list[Transaction] = []
        self._savepoint_ids = itertools.count(1)
        self._host = ""
        self._port = 0
        self._dates_ok = True
        self._notice_handlers: list[Callable[[errors.Diagnostic], None]] = []
        self._notify_handlers: list[Callable[[P.Notify], None]] = []
        self._loader_cache: dict[int, Any] = {}
        self._pipeline: Any = None
        self._notifies: collections.deque[P.Notify] = collections.deque()
        self.info = ConnectionInfo(self)

    # ---------------------------------------------------------------- opening
    @classmethod
    def connect(cls, conninfo: str = "", *, autocommit: bool = False,
                row_factory: Callable | None = None, **kwargs: Any) -> "Connection":
        conn = cls(build_params(conninfo, **kwargs), autocommit=autocommit, row_factory=row_factory)
        conn._open()
        return conn

    def _open(self) -> None:
        modes = ["disable", "require"] if self._params.sslmode == "allow" else [self._params.sslmode]
        last: errors.Error | None = None
        wanted = self._params.target_session_attrs
        # prefer-standby looks for a standby first, then accepts any server
        for requirement in (["standby", "any"] if wanted == "prefer-standby" else [wanted]):
            self._session_requirement = requirement
            for index, (host, port) in enumerate(self._params.hosts):
                addr = self._params.hostaddrs[index] if index < len(self._params.hostaddrs) else None
                for mode in modes:
                    try:
                        self._connect_host(host, port, mode, addr)
                        return
                    except errors.Error as exc:  # any startup failure moves on to the next host
                        last = exc
                    except OSError as exc:  # socket timeouts and TLS handshake failures
                        last = errors.OperationalError(f'connection to server at "{host}", port {port} failed: {exc}')
                        last.__cause__ = exc
                    self._drop_socket()
        assert last is not None
        raise last

    def _drop_socket(self) -> None:
        sock, self._sock = self._sock, None
        self._closed = True
        self._parser = P.MessageParser()
        self._pending.clear()
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _connect_host(self, host: str, port: int, mode: str, addr: str | None = None) -> None:
        p = self._params
        unix = host.startswith("/")
        try:
            if unix:
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
                sock.settimeout(p.connect_timeout)
                sock.connect(f"{host}/.s.PGSQL.{port}")
            else:
                sock = socket.create_connection((addr or host, port), timeout=p.connect_timeout)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                _tune_keepalive(sock)
        except OSError as exc:
            raise errors.OperationalError(
                f'connection to server at "{host}", port {port} failed: {exc}') from exc
        self._sock = sock
        self._host, self._port = host, port
        self._closed = False
        self._parser = P.MessageParser()
        self._pending.clear()
        self._server_params = {}
        self._tls_mode = None
        if not unix and mode != "disable":
            try:
                self._sock = self._negotiate_tls(sock, host, mode)
            except (ssl.SSLError, ConnectionError) as exc:
                self._drop_socket()
                if mode == "prefer":
                    return self._connect_host(host, port, "disable", addr)
                raise errors.OperationalError(
                    f'SSL connection to server at "{host}", port {port} failed: {exc}') from exc
        self._tls_mode = mode if isinstance(self._sock, ssl.SSLSocket) else None
        try:
            self._startup()
        except socket.timeout as exc:  # pragma: no cover - mapped in _recv
            raise errors.OperationalError(f"timeout during connection startup: {exc}") from exc
        self._check_session_attrs()
        if self._sock is not None:
            self._sock.settimeout(None)
        self._dates_ok = self._server_params.get("DateStyle", "ISO").upper().startswith("ISO")

    def _negotiate_tls(self, sock: socket.socket, host: str, mode: str) -> socket.socket:
        sock.sendall(P.SSL_REQUEST)
        answer = sock.recv(1)
        if answer == b"S":
            return _ssl_context(self._params, mode).wrap_socket(sock, server_hostname=host)
        if answer == b"N":
            if mode in ("require", "verify-ca", "verify-full"):
                raise errors.OperationalError("server does not support SSL, but SSL was required")
            return sock
        raise errors.OperationalError(f"received an invalid response to the SSL request: {answer!r}")

    def _check_session_attrs(self) -> None:
        want = self._session_requirement
        if want == "any":
            return
        row = self._simple("SELECT current_setting('transaction_read_only') = 'on', pg_is_in_recovery()")[-1].rows[0]
        read_only, standby = row[0] == b"t", row[1] == b"t"
        ok = {"read-write": not read_only, "read-only": read_only, "primary": not standby, "standby": standby}[want]
        if not ok:
            raise errors.OperationalError(f'server at "{self._host}", port {self._port} is not {want}')

    def _startup(self) -> None:
        p = self._params
        startup = {"user": p.user, "database": p.dbname, "client_encoding": "UTF8", "DateStyle": "ISO"}
        if p.application_name:
            startup["application_name"] = p.application_name
        if p.options:
            startup["options"] = p.options
        self._send(P.startup_message(startup))
        encrypted = self._tls_mode is not None or self._host.startswith("/")
        auth = Authenticator(p.user, resolve_password(p, self._host, self._port), encrypted=encrypted,
                             allow_plaintext=p.sslmode == "disable",
                             require_scram=p.require_auth == "scram-sha-256")
        while True:
            tag, payload = self._next_message()
            if tag == 0x52:  # R Authentication*
                reply = auth.step(int.from_bytes(payload[:4], "big"), payload[4:])
                if reply:
                    self._send(reply)
            elif tag == 0x45:  # E
                raise errors.error_from_fields(P.parse_error_fields(payload))
            elif tag == 0x53:  # S ParameterStatus
                name, value = P.parse_parameter_status(payload)
                self._server_params[name] = value
            elif tag == 0x4B:  # K BackendKeyData
                self._backend_pid = int.from_bytes(payload[:4], "big")
                self._backend_secret = int.from_bytes(payload[4:8], "big")
            elif tag == 0x4E:  # N
                self._notice(payload)
            elif tag == 0x5A:  # Z ReadyForQuery
                self._tx_status = chr(payload[0])
                return

    # ---------------------------------------------------------------- I/O
    def _mark_broken(self) -> None:
        self._closed = True
        self._tx_status = _BROKEN
        sock, self._sock = self._sock, None
        if sock is not None:
            try:
                sock.close()
            except OSError:
                pass

    def _send(self, data: bytes) -> None:
        sock = self._sock
        if sock is None:
            raise errors.OperationalError("the connection is closed")
        if os.getpid() != self._pid:
            raise errors.OperationalError(
                "this connection was opened in another process (the program forked); open a new connection")
        try:
            sock.sendall(data)
        except (OSError, ssl.SSLError) as exc:
            self._mark_broken()
            raise errors.OperationalError(f"connection failed while sending: {exc}") from exc

    def _next_message(self) -> tuple[int, bytes]:
        while not self._pending:
            sock = self._sock
            if sock is None:
                raise errors.OperationalError("the connection is closed")
            try:
                data = sock.recv(65536)
            except (OSError, ssl.SSLError) as exc:
                self._mark_broken()
                raise errors.OperationalError(f"connection failed while receiving: {exc}") from exc
            if not data:
                self._mark_broken()
                raise errors.OperationalError("server closed the connection unexpectedly")
            self._pending.extend(self._parser.feed(data))
        return self._pending.popleft()

    def _notice(self, payload: bytes) -> None:
        diag = errors.Diagnostic(P.parse_error_fields(payload))
        if self._notice_handlers:
            for handler in self._notice_handlers:
                handler(diag)
        else:
            log.debug("server notice: %s", diag.message_primary)

    def add_notice_handler(self, handler: Callable[[errors.Diagnostic], None]) -> None:
        self._notice_handlers.append(handler)

    def add_notify_handler(self, handler: Callable[[P.Notify], None]) -> None:
        """Call ``handler(notify)`` for every notification received while running other statements."""
        self._notify_handlers.append(handler)

    def _on_notification(self, payload: bytes) -> None:
        note = P.parse_notification(payload)
        if self._notify_handlers:
            for handler in self._notify_handlers:
                handler(note)
        else:
            self._notifies.append(note)

    def _handle_async_message(self, tag: int, payload: bytes) -> None:
        """Messages that may arrive at any time: notices, parameter changes, notifications."""
        if tag == 0x4E:
            self._notice(payload)
        elif tag == 0x53:
            name, value = P.parse_parameter_status(payload)
            self._server_params[name] = value
        elif tag == 0x41:
            self._on_notification(payload)

    def notifies(self, *, timeout: float | None = None, stop_after: int | None = None):
        """Yield :class:`Notify` objects as they arrive (``LISTEN channel`` first).

        Stops after ``timeout`` seconds without a new notification (``None`` waits forever) or
        after ``stop_after`` notifications. Holds the connection while waiting.
        """
        count = 0
        while stop_after is None or count < stop_after:
            with self._lock:
                self._check_open()
                if not self._notifies:
                    self._wait_for_data(timeout)
                note = self._notifies.popleft() if self._notifies else None
            if note is None:
                return
            count += 1
            yield note

    def _wait_for_data(self, timeout: float | None) -> None:
        """Block until something notifies us, the timeout passes, or the server goes away."""
        deadline = None if timeout is None else time.monotonic() + timeout
        while not self._notifies:
            if not self._pending:
                sock = self._sock
                if sock is None:
                    raise errors.OperationalError("the connection is closed")
                ready = bool(getattr(sock, "pending", lambda: 0)())
                if not ready:
                    wait = None if deadline is None else max(0.0, deadline - time.monotonic())
                    try:
                        ready = bool(select.select([sock], [], [], wait)[0])
                    except (OSError, ValueError) as exc:
                        self._mark_broken()
                        raise errors.OperationalError(f"connection failed while waiting: {exc}") from exc
                if not ready:
                    return
                try:
                    data = sock.recv(65536)
                except (OSError, ssl.SSLError) as exc:
                    self._mark_broken()
                    raise errors.OperationalError(f"connection failed while receiving: {exc}") from exc
                if not data:
                    self._mark_broken()
                    raise errors.OperationalError("server closed the connection unexpectedly")
                self._pending.extend(self._parser.feed(data))
            while self._pending:
                tag, payload = self._pending.popleft()
                if tag == 0x45:
                    self._mark_broken()
                    raise errors.error_from_fields(P.parse_error_fields(payload))
                self._handle_async_message(tag, payload)
            if deadline is not None and time.monotonic() >= deadline and not self._notifies:
                return

    def _read_cycle(self) -> list[P.Result]:
        """Read messages up to and including ReadyForQuery; raise the server error, if any."""
        collector = P.QueryCollector()
        error: dict[str, str] | None = None
        unsupported = False
        while True:
            tag, payload = self._next_message()
            if tag == 0x5A:  # Z
                self._tx_status = chr(payload[0])
                break
            if tag == 0x45:  # E
                fields = P.parse_error_fields(payload)
                if error is None:
                    error = fields
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    self._mark_broken()
                    raise errors.error_from_fields(fields)
            elif tag == 0x4E:
                self._notice(payload)
            elif tag == 0x53:
                name, value = P.parse_parameter_status(payload)
                self._server_params[name] = value
            elif tag == 0x41:  # A NotificationResponse
                self._on_notification(payload)
            elif tag == 0x47:  # G CopyInResponse: abort it cleanly
                unsupported = True
                self._send(P.copy_fail("COPY needs cursor.copy()"))
            elif tag in (0x48, 0x57, 0x64, 0x63):  # H W d c: drain CopyOut data
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
        if os.getpid() != self._pid:
            raise errors.OperationalError(
                "this connection was opened in another process (the program forked); open a new connection")

    def _check_alive(self) -> bool:
        """Cheap liveness check for an idle connection: no round trip, just a non-blocking read."""
        with self._lock:
            sock = self._sock
            if self._closed or sock is None:
                return False
            try:
                sock.settimeout(0.0)
                try:
                    data = sock.recv(65536)
                except (BlockingIOError, ssl.SSLWantReadError, ssl.SSLWantWriteError):
                    return True
                except (OSError, ssl.SSLError):
                    self._mark_broken()
                    return False
                if not data:
                    self._mark_broken()
                    return False
                self._pending.extend(self._parser.feed(data))
                while self._pending:
                    tag, payload = self._pending[0]
                    if tag == 0x4E:
                        self._notice(payload)
                    elif tag == 0x53:
                        name, value = P.parse_parameter_status(payload)
                        self._server_params[name] = value
                    elif tag == 0x41:
                        self._on_notification(payload)
                    elif tag == 0x45:
                        self._mark_broken()
                        return False
                    else:
                        return True
                    self._pending.popleft()
                return True
            finally:
                if self._sock is not None:
                    self._sock.settimeout(None)

    # ---------------------------------------------------------------- execution
    def _simple(self, sql: str) -> list[P.Result]:
        self._send(P.query_message(sql))
        return self._read_cycle()

    def _run(self, sql: str, params: Any) -> list[P.Result]:
        self._send_query(sql, params)
        return self._read_cycle()

    def _run_many(self, sql: str, params_seq: Iterable[Any]) -> int:
        """``executemany`` as one pipeline.

        Every statement is sent without waiting for its reply. All of them run in one
        transaction: a single Sync ends the batch, so with ``autocommit=True`` a failure
        rolls the whole batch back. Replies are read through a sliding window, so the
        server never blocks on a full socket while we are still sending. Returns the
        total rowcount. Query hooks see one call per statement, as with ``execute``."""
        from .pipeline import Pipeline, _encode  # internal helpers; imported here to avoid a cycle

        window = 64
        pg_sql, redacted = self._query_text(sql)
        if P.COPY_START.match(pg_sql):
            raise errors.ProgrammingError("COPY can't be used with executemany; use cursor.copy() instead")
        with self._lock:
            self._check_open()
            self._check_no_pipeline()
            # Outside a transaction the batch gets one: with autocommit it is BEGIN..COMMIT (so a failure,
            # even one before the Sync, rolls it all back); without it, the usual implicit transaction.
            own_tx = self._autocommit and self._tx_status == _IDLE
            if own_tx or (not self._autocommit and self._tx_status == _IDLE):
                self._send(P.query_message("BEGIN"))  # BEGIN is its own round trip, as in _send_query
                self._read_cycle()
            total = 0
            in_flight: collections.deque = collections.deque()  # (hook stack, info) per unread reply
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

            def read_one() -> errors.Error | None:
                nonlocal total
                stack, info = in_flight.popleft()
                results, error = Pipeline._read_reply(self)
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
                pending = 0  # bytes sent since the replies were last read
                for params in params_seq:
                    body = _encode(pg_sql, params)  # no Sync: the batch ends with one
                    in_flight.append(enter_hooks())
                    self._send(body)
                    pending += len(body)
                    if len(in_flight) > window or pending > P.FLUSH_BYTES:
                        self._send(P.FLUSH)  # make the server send the replies it has produced so far
                        target = window // 2 if len(in_flight) > window else 0
                        pending = 0
                        while len(in_flight) > target:
                            failure = read_one()
                            if failure is not None:
                                break  # statements sent after a failure are skipped by the server
                        if failure is not None:
                            break
                self._send(P.SYNC)
                sent_sync = True
                while failure is None and in_flight:
                    failure = read_one()
            except BaseException as exc:
                failure = exc
                if self._sock is not None:
                    # a bad parameter, a failing hook or a dropped socket mid-batch: discard the replies that are
                    # still unread, up to the batch's ReadyForQuery, so none of them can reach a later query
                    try:
                        if not sent_sync:
                            self._send(P.SYNC)
                        while True:
                            tag, payload = self._next_message()
                            if tag == 0x5A:
                                self._tx_status = chr(payload[0])
                                break
                        if own_tx and self._tx_status != _IDLE:
                            self._simple("ROLLBACK")
                    except Exception:
                        self._mark_broken()
                raise
            finally:
                # statements that were sent but never got a reply (skipped after a failure) end their hooks here
                while in_flight:
                    stack, _ = in_flight.popleft()
                    if failure is None:
                        stack.__exit__(None, None, None)
                    else:
                        stack.__exit__(type(failure), failure, None)
            # read up to ReadyForQuery
            while True:
                tag, payload = self._next_message()
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
                self._simple("ROLLBACK" if failure is not None else "COMMIT")
            if failure is not None:
                raise failure
            return total

    def _send_query(self, sql: str, params: Any, *, inline: bool = False) -> None:
        begin = not self._autocommit and self._tx_status == _IDLE
        if inline and params is not None:  # client-side binding: PostgreSQL has no parameters in COPY
            sql, params = _query.translate(sql, params, _types.render_literal)[0], None
        body = P.build_query(sql, params, _query.translate, _types.dump)
        if begin:  # implicit BEGIN rides in the same packet as the query
            self._send(P.query_message("BEGIN") + body)
            try:
                self._read_cycle()
            except BaseException:
                self._mark_broken()
                raise
        else:
            self._send(body)

    def _start_copy(self, query: Any, params: Any, stack: ExitStack) -> str:
        """Send a COPY statement and wait for its Copy*Response; returns ``"in"`` or ``"out"``."""
        sql, redacted = self._query_text(query)
        self._check_open()
        self._check_no_pipeline()  # a COPY in a pipeline waits for data the batch never sends
        if hooks._HOOKS:
            info = hooks.QueryInfo(redacted, self._host, self._port, self._params.dbname, self._params.user)
            for hook in tuple(hooks._HOOKS):
                try:
                    stack.enter_context(hook(info))
                except Exception:
                    log.exception("a pgtern query hook failed on entry; ignoring it")
        self._send_query(sql, params, inline=True)
        error: dict[str, str] | None = None
        while True:
            tag, payload = self._next_message()
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

    def _query_text(self, query: Any) -> tuple[str, str]:
        if isinstance(query, Composable):
            return query.as_string(), query.as_redacted()
        if isinstance(query, str):
            return query, query
        if isinstance(query, (bytes, bytearray)):
            text = bytes(query).decode()
            return text, text
        raise TypeError(f"the query must be str, bytes or sql.Composable, not {type(query).__name__}")

    def _prime_oids(self, oids: Iterable[int]) -> None:
        """Resolve user loaders for these types (one catalog query per connection and new type)."""
        if not _types.has_loaders():
            return
        wanted = set(oids) - self._loader_cache.keys()
        if not wanted:
            return
        ids = ", ".join(str(int(o)) for o in sorted(wanted))
        rows = self._simple(PRIME_SQL.format(ids=ids))[0].rows
        _types.resolve_loaders(rows, self._loader_cache, sorted(wanted))

    def _prime_loaders(self, results: list[P.Result]) -> None:
        self._prime_oids(c.type_code for r in results if r.columns for c in r.columns)

    def _describe(self, sql: str) -> tuple[list[int], list[P.Column] | None]:
        """Ask the server for the parameter and column types of ``sql`` without running it."""
        self._send(P.describe_statement(sql))
        param_oids: list[int] = []
        columns: list[P.Column] | None = None
        error: dict[str, str] | None = None
        while True:
            tag, payload = self._next_message()
            if tag == 0x5A:
                self._tx_status = chr(payload[0])
                break
            if tag == 0x74:  # t ParameterDescription
                param_oids = P.parse_param_description(payload)
            elif tag == 0x54:  # T RowDescription
                columns = P.parse_row_description(payload)
            elif tag == 0x45:
                fields = P.parse_error_fields(payload)
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    self._mark_broken()
                    raise errors.error_from_fields(fields)
                error = error or fields
            elif tag in (0x31, 0x6E):  # 1 ParseComplete, n NoData
                pass
            else:
                self._handle_async_message(tag, payload)
        if error is not None:
            raise errors.error_from_fields(error)
        return param_oids, columns

    def _binary_result_formats(self, columns: list[P.Column] | None) -> list[int] | None:
        if columns is None:
            return None
        self._prime_oids(c.type_code for c in columns)
        return [1 if _binary.scalar_decoder(c.type_code) is not None and self._loader_cache.get(c.type_code) is None
                else 0 for c in columns]

    def _run_binary(self, sql: str, params: Any) -> list[P.Result]:
        """Like _run, but parameters and columns use binary format where the type supports it.

        Costs one extra round trip (describe) per statement, so it is opt-in with ``binary=True``.
        """
        begin = not self._autocommit and self._tx_status == _IDLE
        if begin:
            self._simple("BEGIN")
        values: list[Any]
        if params is None:
            pg_sql, values = sql, []
        else:
            pg_sql, values = _query.translate(sql, params)
        param_oids, columns = self._describe(pg_sql)
        if len(param_oids) != len(values):
            raise errors.ProgrammingError(
                f"the query has {len(param_oids)} parameters but {len(values)} were passed")
        pformats, payloads = _types.binary_param_plan(param_oids, values)
        rformats = self._binary_result_formats(columns)
        self._send(P.extended_query(pg_sql, param_oids, payloads, pformats, rformats))
        results = self._read_cycle()
        for r in results:
            if r.columns is not None:
                r.formats = rformats
        return results

    def _check_no_pipeline(self) -> None:
        if self._pipeline is not None:
            raise errors.ProgrammingError(
                "a pipeline is active on this connection: queue statements with pipe.execute() "
                "or leave the pipeline block first")

    def _execute(self, query: Any, params: Any, binary: bool = False) -> list[P.Result]:
        sql, redacted = self._query_text(query)
        run = self._run_binary if binary else self._run
        with self._lock:
            self._check_open()
            self._check_no_pipeline()
            if not hooks._HOOKS:
                results = run(sql, params)
            else:
                info = hooks.QueryInfo(redacted, self._host, self._port, self._params.dbname, self._params.user)
                with ExitStack() as stack:
                    for hook in tuple(hooks._HOOKS):
                        try:
                            stack.enter_context(hook(info))
                        except Exception:
                            log.exception("a pgtern query hook failed on entry; ignoring it")
                    results = run(sql, params)
                    info.rowcount = results[-1].rowcount if results else -1
            self._prime_loaders(results)
            return results

    # ---------------------------------------------------------------- public API
    def cursor(self, name: str | None = None, *, row_factory: Callable | None = None,
               scrollable: bool | None = None, withhold: bool = False) -> Cursor:
        """A client-side cursor, or a server-side one (``DECLARE``) when ``name`` is given."""
        if name is not None:
            return ServerCursor(self, name, row_factory=row_factory, scrollable=scrollable, withhold=withhold)
        return Cursor(self, row_factory=row_factory)

    def execute(self, query: Any, params: Any = None, *, prepare: bool | None = None,
                binary: bool | None = None) -> Cursor:
        return self.cursor().execute(query, params, binary=bool(binary))

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def autocommit(self) -> bool:
        return self._autocommit

    @autocommit.setter
    def autocommit(self, value: bool) -> None:
        with self._lock:
            if bool(value) != self._autocommit and self._tx_status not in (_IDLE, _BROKEN):
                raise errors.ProgrammingError("can't change 'autocommit' now: connection in a transaction")
            self._autocommit = bool(value)

    def _commit_or_fail(self) -> None:
        """COMMIT that raises when the server rolled the transaction back instead (it had failed)."""
        results = self._simple("COMMIT")
        if results and results[-1].tag == "ROLLBACK":
            raise errors.error_from_fields({"S": "ERROR", "C": "25P02", "M": (
                "the transaction was rolled back, not committed: a statement in it had failed")})

    def commit(self) -> None:
        with self._lock:
            self._check_open()
            self._check_no_pipeline()
            if self._tx_stack:
                raise errors.ProgrammingError(
                    "explicit commit() is forbidden inside a transaction() block; "
                    "the block commits on a successful exit")
            if self._tx_status != _IDLE:
                self._commit_or_fail()

    def rollback(self) -> None:
        with self._lock:
            self._check_open()
            self._check_no_pipeline()
            if self._tx_stack:
                raise errors.ProgrammingError("explicit rollback() is forbidden inside a transaction() block")
            if self._tx_status != _IDLE:
                self._simple("ROLLBACK")

    def transaction(self, savepoint_name: str | None = None, force_rollback: bool = False) -> Transaction:
        self._check_no_pipeline()
        return Transaction(self, savepoint_name, force_rollback)

    def pipeline(self, batch_size: int = 64) -> "Pipeline":
        """Pipeline mode: queue many statements and read their replies together. See :mod:`pgtern.pipeline`."""
        from .pipeline import Pipeline
        return Pipeline(self, batch_size)

    def cancel(self) -> None:
        """Ask the server to cancel the statement running on this connection.

        Safe to call from another thread while a query runs: it opens its own connection to the
        server and neither waits for nor takes the lock of the query in progress."""
        pid, secret = self._backend_pid, self._backend_secret
        if self._closed or not pid:
            raise errors.OperationalError("the connection is closed")
        _send_cancel(self._host, self._port, pid, secret, self._tls_mode, self._params)

    def _reset_for_pool(self) -> None:
        """Return the session to its initial state, so the next borrower sees nothing of this one."""
        with self._lock:
            self._loader_cache.clear()
            self._tx_stack.clear()
            if self._closed:
                return
            if self._tx_status != _IDLE:
                self._simple("ROLLBACK")
            self._simple("DISCARD ALL")  # roles, search_path, temp tables, LISTEN, advisory locks, cursors
            self.row_factory = self._default_row_factory
            self._notice_handlers.clear()
            self._notify_handlers.clear()
            self._notifies.clear()

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            sock = self._sock
            self._closed = True
            self._tx_status = _BROKEN
            self._sock = None
        if sock is not None and os.getpid() != self._pid:
            sock.close()  # a forked child must not send Terminate over a socket its parent still uses
            return
        if sock is not None:
            try:
                sock.sendall(P.TERMINATE)
            except (OSError, ssl.SSLError):
                pass
            try:
                sock.close()
            except OSError:
                pass

    def __enter__(self) -> "Connection":
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if not self._closed:
                if exc_type is None:
                    self.commit()
                else:
                    try:
                        self.rollback()
                    except Exception:
                        log.warning("rollback failed while leaving a connection block", exc_info=True)
        finally:
            self.close()

    def __del__(self) -> None:
        try:
            if not self._closed and self._sock is not None:
                self._sock.close()  # no Terminate from a finalizer: a graceful close is close()'s job
        except Exception:
            pass
