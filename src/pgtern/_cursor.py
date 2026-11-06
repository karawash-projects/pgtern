# SPDX-License-Identifier: Apache-2.0
"""DB-API cursor. Results are fully buffered on the client."""
from __future__ import annotations

from typing import Any, Callable, Iterator, Sequence

from . import errors
from ._copy import Copy
from ._protocol import Column, Result
from ._types import load_row, make_loaders
from .sql import SQL, Composable, quote_ident


class Cursor:
    def __init__(self, connection: Any, *, row_factory: Callable | None = None) -> None:
        self.connection = connection
        self.row_factory = row_factory or connection.row_factory
        self.arraysize = 1
        self._results: list[Result] = []
        self._index = 0
        self._pos = 0
        self._make_row: Callable | None = None
        self._loaders: list | None = None
        self._total_rowcount: int | None = None
        self._closed = False
        self._pipeline_pending = False  # set by a pipeline until its batch is synchronized
        self._pipeline_error: Exception | None = None

    # ---------------------------------------------------------------- state
    def _current(self) -> Result | None:
        if self._pipeline_pending:
            raise errors.ProgrammingError("the pipeline is not synchronized yet: leave the pipeline block first")
        if self._pipeline_error is not None:
            raise self._pipeline_error
        return self._results[self._index] if self._index < len(self._results) else None

    @property
    def description(self) -> list[Column] | None:
        cur = self._current()
        return list(cur.columns) if cur is not None and cur.columns is not None else None

    @property
    def rowcount(self) -> int:
        if self._total_rowcount is not None:
            return self._total_rowcount
        cur = self._current()
        return cur.rowcount if cur is not None else -1

    @property
    def statusmessage(self) -> str | None:
        cur = self._current()
        return cur.tag if cur is not None else None

    @property
    def closed(self) -> bool:
        return self._closed

    def close(self) -> None:
        self._closed = True
        self._results = []

    def __enter__(self) -> "Cursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    def _reset_position(self, index: int) -> None:
        self._index = index
        self._pos = 0
        self._make_row = None
        self._loaders = None

    # ---------------------------------------------------------------- execute
    def execute(self, query: Any, params: Any = None, *, prepare: bool | None = None,
                binary: bool | None = None) -> "Cursor":
        if prepare:
            raise errors.NotSupportedError("prepare=True is not supported: pgtern runs statements unnamed")
        """Run a query. ``binary=True`` sends and receives binary format where the type supports
        it (one extra round trip). ``prepare`` is accepted for DB-API compatibility and ignored:
        pgtern always uses the unnamed statement."""
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        self._set_results(self.connection._execute(query, params, bool(binary)))
        return self

    def _set_results(self, results: list[Result]) -> None:
        self._results = results
        self._total_rowcount = None
        self._reset_position(0)

    def copy(self, query: Any, params: Any = None) -> Copy:
        """Run ``COPY ... FROM STDIN`` / ``TO STDOUT``; use the result as a context manager."""
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        return Copy(self, query, params)

    def executemany(self, query: Any, params_seq: Sequence[Any]) -> None:
        """Run ``query`` once per parameter set as one pipelined batch, in one transaction
        (see ``Connection._run_many``). ``rowcount`` is the total of the statements' rowcounts."""
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        total = self.connection._run_many(query, params_seq)
        self._set_results([])
        self._total_rowcount = total

    def nextset(self) -> bool | None:
        if self._index + 1 < len(self._results):
            self._reset_position(self._index + 1)
            return True
        return None

    # ---------------------------------------------------------------- fetch
    def _prepare_rows(self) -> tuple[Result, list[Any]]:
        cur = self._current()
        if cur is None or cur.columns is None:
            raise errors.ProgrammingError("the last operation didn't produce a result")
        if self._loaders is None:
            self._loaders = make_loaders([c.type_code for c in cur.columns], self.connection._dates_ok,
                                         self.connection._loader_cache, cur.formats)
        if self._make_row is None:
            self._make_row = self.row_factory(self)
        return cur, self._loaders

    def fetchone(self) -> Any:
        cur, loaders = self._prepare_rows()
        if self._pos >= len(cur.rows):
            return None
        raw = cur.rows[self._pos]
        self._pos += 1
        return self._make_row(load_row(raw, loaders, self.connection._codec))  # type: ignore[misc]

    def fetchmany(self, size: int | None = None) -> list:
        cur, loaders = self._prepare_rows()
        count = self.arraysize if size is None else size
        chunk = cur.rows[self._pos:self._pos + count]
        self._pos += len(chunk)
        return [self._make_row(load_row(raw, loaders, self.connection._codec)) for raw in chunk]  # type: ignore[misc]

    def fetchall(self) -> list:
        cur, loaders = self._prepare_rows()
        chunk = cur.rows[self._pos:]
        self._pos = len(cur.rows)
        return [self._make_row(load_row(raw, loaders, self.connection._codec)) for raw in chunk]  # type: ignore[misc]

    def __iter__(self) -> Iterator:
        return self

    def __next__(self) -> Any:
        row = self.fetchone()
        if row is None:
            raise StopIteration
        return row


class ServerCursor(Cursor):
    """A named cursor living in the server (``DECLARE``): rows are fetched in batches,
    so large results never have to fit in memory. Needs a transaction unless ``withhold``."""

    itersize = 100

    def __init__(self, connection: Any, name: str, *, row_factory: Callable | None = None,
                 scrollable: bool | None = None, withhold: bool = False) -> None:
        super().__init__(connection, row_factory=row_factory)
        self.name = name
        self._scrollable = scrollable
        self._withhold = withhold
        self._binary = False
        self._declared = False
        self._columns: list[Column] | None = None
        self._fetched = 0
        self._batch: Result | None = None

    # -------------------------------------------------------------- state
    @property
    def description(self) -> list[Column] | None:
        return list(self._columns) if self._columns is not None else None

    @property
    def rowcount(self) -> int:
        return -1 if not self._declared else self._fetched

    @property
    def rownumber(self) -> int | None:
        return self._fetched if self._declared else None

    @property
    def statusmessage(self) -> str | None:
        return self._batch.tag if self._batch is not None else None

    def close(self) -> None:
        if self._declared and not self.connection.closed and self.connection.info.transaction_status.name != "INERROR":
            try:
                self.connection._execute(f"CLOSE {quote_ident(self.name)}", None)
            except errors.Error:
                pass
        self._declared = False
        super().close()

    # -------------------------------------------------------------- execute
    def execute(self, query: Any, params: Any = None, *, binary: bool | None = None, **_: Any) -> "ServerCursor":
        if self._closed:
            raise errors.InterfaceError("the cursor is closed")
        self._binary = bool(binary)
        if self._declared:
            raise errors.ProgrammingError("the server-side cursor was already executed; create a new one")
        prefix = f"DECLARE {quote_ident(self.name)} "
        if self._scrollable is not None:
            prefix += "SCROLL " if self._scrollable else "NO SCROLL "
        prefix += "CURSOR " + ("WITH HOLD " if self._withhold else "") + "FOR "
        body = query if isinstance(query, Composable) else SQL(query if isinstance(query, str) else bytes(query).decode())
        self.connection._execute(SQL(prefix) + body, params)
        self._declared = True
        self._fetched = 0
        first = self.connection._execute(f"FETCH FORWARD 0 FROM {quote_ident(self.name)}", None, self._binary)[0]
        self._columns = first.columns
        self._set_batch(first)
        return self

    def executemany(self, query: Any, params_seq: Sequence[Any]) -> None:
        raise errors.NotSupportedError("executemany() can't be used with a server-side cursor")

    def _set_batch(self, result: Result) -> None:
        self._batch = result
        self._set_results([result])

    def _fetch(self, command: str) -> Result:
        if not self._declared:
            raise errors.ProgrammingError("the cursor has not been executed")
        result = self.connection._execute(f"{command} FROM {quote_ident(self.name)}", None, self._binary)[0]
        if result.columns is None:
            result.columns = self._columns
        self._set_batch(result)
        return result

    # -------------------------------------------------------------- fetch
    def fetchone(self) -> Any:
        rows = self.fetchmany(1)
        return rows[0] if rows else None

    def fetchmany(self, size: int | None = None) -> list:
        count = self.arraysize if size is None else size
        result = self._fetch(f"FETCH FORWARD {int(count)}")
        self._fetched += len(result.rows)
        return Cursor.fetchall(self)

    def fetchall(self) -> list:
        result = self._fetch("FETCH FORWARD ALL")
        self._fetched += len(result.rows)
        return Cursor.fetchall(self)

    def __iter__(self) -> Iterator:
        while True:
            rows = self.fetchmany(self.itersize)
            yield from rows
            if len(rows) < self.itersize:
                return

    def scroll(self, value: int, mode: str = "relative") -> None:
        if mode not in ("relative", "absolute"):
            raise ValueError(f"bad mode: {mode}; it should be 'relative' or 'absolute'")
        self._fetch(f"MOVE {mode.upper()} {int(value)}")
        self._fetched = (self._fetched + value) if mode == "relative" else value
