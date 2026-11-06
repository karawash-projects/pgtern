# SPDX-License-Identifier: Apache-2.0
"""Pipeline mode: send many statements before waiting for any reply.

    with conn.pipeline() as pipe:
        a = pipe.execute("INSERT INTO t (v) VALUES (%s)", [1])
        b = pipe.execute("SELECT count(*) FROM t")
    # leaving the block synchronizes: now a and b have their results
    print(b.fetchone())

Statements are sent immediately and their replies read, in order, at a *sync point*. Sync
points happen when you call ``sync()``, when the block ends, and every ``batch_size``
statements.

* Transactions: with ``autocommit=False`` the first statement of a batch is preceded by an
  implicit BEGIN, as usual. With ``autocommit=True`` every sync point is a commit point, so
  statements in one batch succeed or fail together. Use ``batch_size`` or ``sync()`` to choose.
* Errors: when one statement fails the server skips the rest of the batch. Those statements
  get :class:`pgtern.errors.PipelineAborted`. A sync point raises the first error of its batch,
  and the failing statement's cursor holds the same error. Inside a transaction you must roll back
  afterwards, as with any failure.
* Only text format is used (``binary=True`` is not available in pipelines).
"""
from __future__ import annotations

import logging
from typing import Any

from . import _protocol as P
from . import _query, _types, errors, hooks
from ._cursor import Cursor

log = logging.getLogger("pgtern")

_IDLE = "I"


def _enter_hooks(conn: Any, item: "_Item") -> tuple[hooks.QueryInfo | None, list]:
    if item.hidden or not hooks._HOOKS:
        return None, []
    info = hooks.QueryInfo(item.redacted, conn._host, conn._port, conn._params.dbname, conn._params.user)
    entered = []
    for hook in tuple(hooks._HOOKS):
        try:
            cm = hook(info)
            cm.__enter__()
            entered.append(cm)
        except Exception:
            log.exception("a pgtern query hook failed on entry; ignoring it")
    return info, entered


def _exit_hooks(entered: list, error: BaseException | None) -> None:
    exc_info = (type(error), error, None) if error is not None else (None, None, None)
    for cm in reversed(entered):
        try:
            cm.__exit__(*exc_info)
        except Exception:
            log.exception("a pgtern query hook failed on exit; ignoring it")


class _Item:
    __slots__ = ("cursor", "sql", "redacted", "hidden")

    def __init__(self, cursor: Cursor | None, sql: str, redacted: str, hidden: bool = False) -> None:
        self.cursor = cursor
        self.sql = sql
        self.redacted = redacted
        self.hidden = hidden


def _encode(sql: str, params: Any) -> bytes:
    if params is None:
        return P.extended_query(sql, [], [], sync=False)
    pg_sql, values = _query.translate(sql, params)
    dumped = [_types.dump(v) for v in values]
    return P.extended_query(pg_sql, [oid for oid, _ in dumped], [data for _, data in dumped], sync=False)


class Pipeline:
    """Synchronous pipeline. Create it with :meth:`pgtern.Connection.pipeline`."""

    def __init__(self, conn: Any, batch_size: int = 64) -> None:
        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")
        self._conn = conn
        self._batch_size = batch_size
        self._queue: list[_Item] = []
        self._begin_queued = False
        self._open = False

    # ---------------------------------------------------------------- block
    def __enter__(self) -> "Pipeline":
        conn = self._conn
        with conn._lock:
            conn._check_open()
            if conn._pipeline is not None:
                raise errors.ProgrammingError("a pipeline is already active on this connection")
            conn._pipeline = self
        self._open = True
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        try:
            if exc_type is None:
                self.sync()
            elif not self._conn.closed:
                try:
                    self.sync()  # read the replies so the connection stays usable
                except Exception:  # noqa: BLE001 - the original exception is what matters
                    pass
        finally:
            self._open = False
            with self._conn._lock:
                if self._conn._pipeline is self:
                    self._conn._pipeline = None

    # ---------------------------------------------------------------- queueing
    def execute(self, query: Any, params: Any = None) -> Cursor:
        """Queue a statement; returns a cursor that holds its result after the next sync."""
        conn = self._conn
        with conn._lock:
            conn._check_open()
            if not self._open:
                raise errors.ProgrammingError("the pipeline is not active (use it in a 'with' block)")
            sql, redacted = conn._query_text(query)
            if len(self._queue) >= self._batch_size:
                self.sync()
            if not conn._autocommit and conn._tx_status == _IDLE and not self._begin_queued:
                conn._send(P.extended_query("BEGIN", [], [], sync=False))
                self._queue.append(_Item(None, "BEGIN", "BEGIN", hidden=True))
                self._begin_queued = True
            cursor = Cursor(conn)
            cursor._pipeline_pending = True
            conn._send(_encode(sql, params))
            self._queue.append(_Item(cursor, sql, redacted))
            return cursor

    # ---------------------------------------------------------------- sync point
    def sync(self) -> None:
        """Send Sync, read every queued reply and fill in the cursors."""
        conn = self._conn
        with conn._lock:
            conn._check_open()
            items, self._queue = self._queue, []
            self._begin_queued = False
            conn._send(P.SYNC)
            failure: errors.Error | None = None
            collected: list[tuple[Cursor, list]] = []
            for item in items:
                if failure is not None:
                    if item.cursor is not None:
                        item.cursor._pipeline_pending = False
                        item.cursor._pipeline_error = errors.PipelineAborted(
                            "not executed: an earlier statement in the pipeline failed")
                    continue
                info, entered = _enter_hooks(conn, item)
                results, error = self._read_reply(conn)
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
            while True:  # the rest of a failed batch sends nothing but ReadyForQuery
                tag, payload = conn._next_message()
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
            conn._prime_loaders([r for _, res in collected for r in res])
            for cursor, res in collected:
                cursor._pipeline_pending = False
                cursor._set_results(res)
            if failure is not None:
                raise failure  # the cursors hold it too; raising here means a failure cannot pass silently

    @staticmethod
    def _read_reply(conn: Any) -> tuple[list | None, errors.Error | None]:
        """Read one statement's messages. Returns (results, None), or (None, error)."""
        collector = P.QueryCollector()
        while True:
            tag, payload = conn._next_message()
            if tag == 0x45:
                fields = P.parse_error_fields(payload)
                if fields.get("V", fields.get("S")) in ("FATAL", "PANIC"):
                    conn._mark_broken()
                    raise errors.error_from_fields(fields)
                return None, errors.error_from_fields(fields)
            if tag in (0x43, 0x49):  # C CommandComplete, I EmptyQueryResponse: this statement is done
                collector.handle(tag, payload)
                return collector.results, None
            if tag in (0x4E, 0x53, 0x41):
                conn._handle_async_message(tag, payload)
            else:
                collector.handle(tag, payload)
