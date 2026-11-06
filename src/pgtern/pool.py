# SPDX-License-Identifier: Apache-2.0
"""A synchronous, thread-safe connection pool. Built in: no separate package to install."""
from __future__ import annotations

import collections
import logging
import threading
import time
from contextlib import contextmanager
from typing import Any, Callable, Iterator

from . import errors
from ._conn import Connection

log = logging.getLogger("pgtern.pool")

__all__ = ["ConnectionPool", "PoolClosed", "PoolTimeout"]


class PoolClosed(errors.OperationalError):
    """The pool was closed."""


class PoolTimeout(errors.OperationalError):
    """No connection became available in time."""


class _Idle:
    __slots__ = ("conn", "returned")

    def __init__(self, conn: Connection, returned: float) -> None:
        self.conn = conn
        self.returned = returned


class ConnectionPool:
    """Pool of :class:`Connection`.

    ``min_size`` connections are kept open by a background thread (which also retires
    connections older than ``max_lifetime`` and surplus ones idle longer than ``max_idle``);
    up to ``max_size`` are opened on demand. A connection is checked for liveness
    (no round trip) every time it is handed out, a failed connection is replaced, and
    a connection returned mid-transaction is rolled back.
    """

    def __init__(self, conninfo: str = "", *, kwargs: dict[str, Any] | None = None, min_size: int = 4,
                 max_size: int | None = None, open: bool = True, timeout: float = 30.0,
                 max_lifetime: float = 3600.0, max_idle: float = 600.0,
                 check: Callable[[Connection], None] | None = None, name: str | None = None,
                 connection_class: type[Connection] = Connection) -> None:
        max_size = min_size if max_size is None else max_size
        if min_size < 0 or max_size < 1 or max_size < min_size:
            raise ValueError(f"bad pool sizes: min_size={min_size}, max_size={max_size}")
        self.conninfo = conninfo
        self.kwargs = dict(kwargs or {})
        self.min_size, self.max_size = min_size, max_size
        self.timeout, self.max_lifetime, self.max_idle = timeout, max_lifetime, max_idle
        self.name = name or f"pool-{id(self):x}"
        self._check = check
        self._connection_class = connection_class
        self._default_autocommit = bool(self.kwargs.get("autocommit", False))
        self._cond = threading.Condition()
        self._idle: collections.deque[_Idle] = collections.deque()
        self._out: set[Connection] = set()  # checked out and not yet returned
        self._n = 0            # open + being opened
        self._connecting = 0
        self._waiting = 0
        self._closed = True
        self._thread: threading.Thread | None = None
        self._wake = threading.Event()
        if open:
            self.open()

    # ---------------------------------------------------------------- lifecycle
    def open(self, wait: bool = False, timeout: float = 30.0) -> None:
        with self._cond:
            if not self._closed:
                return
            self._closed = False
        self._wake.clear()
        self._thread = threading.Thread(target=self._maintain, name=f"pgtern-{self.name}", daemon=True)
        self._thread.start()
        if wait:
            self.wait(timeout)

    def wait(self, timeout: float = 30.0) -> None:
        """Block until ``min_size`` connections are open."""
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._n - self._connecting < self.min_size:
                if self._closed:
                    raise PoolClosed("the pool is closed")
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeout(f"pool initialization incomplete after {timeout:.2f} sec")
                self._cond.wait(min(remaining, 0.2))

    def close(self, timeout: float = 5.0) -> None:
        with self._cond:
            if self._closed:
                return
            self._closed = True
            idle = list(self._idle)
            self._idle.clear()
            self._n -= len(idle)
            self._cond.notify_all()
        self._wake.set()
        for item in idle:
            item.conn.close()
        thread = self._thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout)

    @property
    def closed(self) -> bool:
        return self._closed

    def __enter__(self) -> "ConnectionPool":
        self.open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def get_stats(self) -> dict[str, int]:
        with self._cond:
            return {
                "pool_min": self.min_size, "pool_max": self.max_size,
                "pool_size": self._n - self._connecting, "pool_available": len(self._idle),
                "requests_waiting": self._waiting,
            }

    # ---------------------------------------------------------------- internals
    def _connect(self) -> Connection:
        conn = self._connection_class.connect(self.conninfo, **self.kwargs)
        conn._pool_created = time.monotonic()  # type: ignore[attr-defined]
        return conn

    def _reap(self) -> list[Connection]:
        """Pick idle connections to retire. Caller holds the lock and closes the result."""
        now = time.monotonic()
        keep: collections.deque[_Idle] = collections.deque()
        retire: list[Connection] = []
        for item in self._idle:
            conn = item.conn
            too_old = now - conn._pool_created > self.max_lifetime  # type: ignore[attr-defined]
            surplus_idle = now - item.returned > self.max_idle and self._n - len(retire) > self.min_size
            if conn.closed or too_old or surplus_idle:
                retire.append(conn)
            else:
                keep.append(item)
        self._idle = keep
        self._n -= len(retire)
        return retire

    def _maintain(self) -> None:
        backoff = 0.5
        while True:
            create = False
            with self._cond:
                if self._closed:
                    return
                retire = self._reap()
                if self._n < self.min_size:
                    self._n += 1
                    self._connecting += 1
                    create = True
            for conn in retire:
                conn.close()
            if create:
                try:
                    conn = self._connect()
                except Exception as exc:
                    with self._cond:
                        self._n -= 1
                        self._connecting -= 1
                    log.warning("pool %s: couldn't open a connection: %s", self.name, exc)
                    self._wake.wait(backoff)
                    self._wake.clear()
                    backoff = min(backoff * 2, 30.0)
                    continue
                backoff = 0.5
                with self._cond:
                    self._connecting -= 1
                    stored = not self._closed
                    if stored:
                        self._idle.append(_Idle(conn, time.monotonic()))
                    else:
                        self._n -= 1
                    self._cond.notify_all()
                if not stored:
                    conn.close()
                continue
            self._wake.wait(5.0)
            self._wake.clear()

    def _usable(self, conn: Connection) -> bool:
        if conn.closed or time.monotonic() - conn._pool_created > self.max_lifetime:  # type: ignore[attr-defined]
            return False
        if not conn._check_alive():
            return False
        if self._check is not None:
            try:
                self._check(conn)
            except Exception:
                return False
        return True

    def _discard(self, conn: Connection) -> None:
        with self._cond:
            self._n -= 1
            self._cond.notify_all()
        self._wake.set()
        conn.close()

    # ---------------------------------------------------------------- public API
    def getconn(self, timeout: float | None = None) -> Connection:
        timeout = self.timeout if timeout is None else timeout
        deadline = time.monotonic() + timeout
        backoff = 0.1
        while True:
            candidate: _Idle | None = None
            create = False
            with self._cond:
                if self._closed:
                    raise PoolClosed("the pool is closed")
                if self._idle:
                    candidate = self._idle.pop()
                elif self._n < self.max_size:
                    self._n += 1
                    self._connecting += 1
                    create = True
                else:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        raise PoolTimeout(f"couldn't get a connection after {timeout:.2f} sec")
                    self._waiting += 1
                    try:
                        self._cond.wait(remaining)
                    finally:
                        self._waiting -= 1
                    continue
            if candidate is not None:
                if self._usable(candidate.conn):
                    with self._cond:
                        self._out.add(candidate.conn)
                    return candidate.conn
                self._discard(candidate.conn)
                continue
            assert create
            try:
                conn = self._connect()
            except Exception as exc:
                with self._cond:
                    self._n -= 1
                    self._connecting -= 1
                    self._cond.notify_all()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise PoolTimeout(f"couldn't get a connection after {timeout:.2f} sec: {exc}") from exc
                time.sleep(min(backoff, remaining))
                backoff = min(backoff * 2, 2.0)
                continue
            with self._cond:
                self._connecting -= 1
                closed = self._closed
                if closed:
                    self._n -= 1
            if closed:
                conn.close()
                raise PoolClosed("the pool is closed")
            with self._cond:
                self._out.add(conn)
            return conn

    def putconn(self, conn: Connection) -> None:
        with self._cond:
            if conn not in self._out:
                raise errors.ProgrammingError(
                    "this connection was not checked out from the pool, or was already returned")
            self._out.discard(conn)
        ok = not conn.closed
        if ok:
            try:
                conn._reset_for_pool()
                if conn.autocommit != self._default_autocommit:
                    conn.autocommit = self._default_autocommit
            except Exception:
                ok = False
        with self._cond:
            fresh = time.monotonic() - conn._pool_created <= self.max_lifetime  # type: ignore[attr-defined]
            if ok and fresh and not self._closed:
                self._idle.append(_Idle(conn, time.monotonic()))
                self._cond.notify_all()
                return
            self._n -= 1
            self._cond.notify_all()
        self._wake.set()
        conn.close()

    @contextmanager
    def connection(self, timeout: float | None = None) -> Iterator[Connection]:
        """Borrow a connection: commit on success, roll back on error, return it to the pool."""
        conn = self.getconn(timeout)
        try:
            try:
                yield conn
            except BaseException:
                try:
                    if not conn.closed:
                        conn._reset_for_pool()
                except Exception:
                    log.warning("rollback failed while returning a connection", exc_info=True)
                raise
            else:
                if not conn.closed:
                    conn.commit()
        finally:
            self.putconn(conn)
