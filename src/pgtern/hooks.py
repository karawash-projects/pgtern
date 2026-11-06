# SPDX-License-Identifier: Apache-2.0
"""Query hooks: observe every statement without monkeypatching.

A hook is a callable ``hook(info) -> context manager``. The context manager is
entered before the statement is sent and exited when its result (or error) has
been read, with the usual ``__exit__`` exception arguments. ``info.rowcount`` is
filled in before a successful exit.

Hooks never receive bound parameter values. ``info.statement`` is the query text;
for ``sql.Composed`` queries any ``sql.Literal`` is replaced by ``?``.

    @contextmanager
    def trace(info):
        with tracer.start_as_current_span("postgresql") as span:
            span.set_attribute("db.statement", info.statement)
            yield

    unregister = pgtern.add_query_hook(trace)
"""
from __future__ import annotations

import logging
from contextlib import AbstractContextManager
from dataclasses import dataclass
from typing import Any, Callable

__all__ = ["QueryInfo", "add_query_hook"]

_log = logging.getLogger("pgtern.hooks")


@dataclass
class QueryInfo:
    statement: str
    host: str
    port: int
    dbname: str
    user: str
    rowcount: int = -1


QueryHook = Callable[[QueryInfo], AbstractContextManager]
_HOOKS: list[QueryHook] = []


class _Guarded(AbstractContextManager):
    """A hook's context manager, with failures on entry or exit logged rather than raised.

    An exit failure raised into the query made a successful statement look failed, and in executemany it
    could shift results between statements. Entry failures are already logged and skipped by the driver.
    """

    __slots__ = ("_cm",)

    def __init__(self, cm: Any) -> None:
        self._cm = cm

    def __enter__(self) -> Any:
        return self._cm.__enter__()

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
        try:
            return bool(self._cm.__exit__(exc_type, exc, tb))
        except Exception:
            _log.exception("a pgtern query hook failed on exit; ignoring it")
            return False


def guarded(cm: Any) -> AbstractContextManager:
    return _Guarded(cm)


def add_query_hook(hook: QueryHook) -> Callable[[], None]:
    """Register a hook for every connection; returns a function that unregisters it."""
    _HOOKS.append(hook)

    def remove() -> None:
        try:
            _HOOKS.remove(hook)
        except ValueError:
            pass

    return remove
