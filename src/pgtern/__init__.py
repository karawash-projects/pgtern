# SPDX-License-Identifier: Apache-2.0
"""pgtern: a zero-dependency, pure-Python PostgreSQL client with a familiar DB-API style interface.

    import pgtern
    from pgtern.rows import dict_row

    with pgtern.connect("postgresql://user:pw@host/db", row_factory=dict_row) as conn:
        for row in conn.execute("SELECT id, name FROM users WHERE id = %s", (42,)):
            ...
"""
from __future__ import annotations

from typing import Any

from . import adapters, aio, errors, hooks, rows, sql, types
from ._conn import Connection, ConnectionInfo, Transaction, TransactionStatus
from ._cursor import Cursor, ServerCursor
from ._protocol import Notify
from ._types import Interval
from .errors import (
    DatabaseError, DataError, Error, IntegrityError, InterfaceError, InternalError,
    NotSupportedError, OperationalError, ProgrammingError, Warning,
)
from .hooks import QueryInfo, add_query_hook
from .pool import ConnectionPool, PoolClosed, PoolTimeout

__version__ = "0.3.0"

# DB-API 2.0 module globals
apilevel = "2.0"
threadsafety = 2  # connections can be shared between threads
paramstyle = "pyformat"


def connect(conninfo: str = "", **kwargs: Any) -> Connection:
    """Open a connection. ``conninfo`` is a ``postgresql://`` URL or ``key=value`` string;
    keyword arguments (``autocommit``, ``row_factory``, ``connect_timeout``, ``sslmode``, ...) override it."""
    return Connection.connect(conninfo, **kwargs)


__all__ = [
    "connect", "Connection", "ConnectionInfo", "ConnectionPool", "Cursor", "ServerCursor", "Notify", "Interval", "Transaction",
    "TransactionStatus", "PoolClosed", "PoolTimeout", "QueryInfo", "add_query_hook",
    "Warning", "Error", "InterfaceError", "DatabaseError", "DataError", "OperationalError",
    "IntegrityError", "InternalError", "ProgrammingError", "NotSupportedError",
    "adapters", "aio", "errors", "hooks", "rows", "sql", "types", "apilevel", "threadsafety", "paramstyle", "__version__",
]
