# SPDX-License-Identifier: Apache-2.0
"""Row factories: callables taking a cursor and returning a ``make_row(values)`` function."""
from __future__ import annotations

from collections import namedtuple
from typing import Any, Callable

__all__ = ["tuple_row", "dict_row", "namedtuple_row"]


def _identity(values: tuple) -> tuple:
    return values


def tuple_row(cursor: Any) -> Callable[[tuple], tuple]:
    """Rows as plain tuples (the default)."""
    return _identity


def dict_row(cursor: Any) -> Callable[[tuple], dict[str, Any]]:
    """Rows as ``dict`` keyed by column name (a repeated name keeps the last value)."""
    names = [c.name for c in (cursor.description or ())]
    return lambda values: dict(zip(names, values))


def namedtuple_row(cursor: Any) -> Callable[[tuple], Any]:
    """Rows as ``namedtuple`` instances (unusable names are renamed ``_0``, ``_1``, ...)."""
    names = [c.name for c in (cursor.description or ())]
    factory: Any = namedtuple  # typed as Any: the field list is only known at run time
    row_type = factory("Row", names, rename=True)
    return row_type._make
