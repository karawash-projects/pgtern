# SPDX-License-Identifier: Apache-2.0
"""Safe composition of SQL text: ``SQL``, ``Identifier``, ``Literal``, ``Placeholder``.

Use this for statements that cannot take bind parameters (``CREATE ROLE ... PASSWORD``,
``GRANT ... TO <role>``). Everything else should use ``%s`` parameters.
"""
from __future__ import annotations

import binascii
import string
import uuid
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Iterable, Iterator

from . import errors
from .types import Json, Jsonb

__all__ = ["Composable", "Composed", "SQL", "Identifier", "Literal", "Placeholder",
           "quote_ident", "quote_literal"]


def quote_ident(name: str) -> str:
    if "\x00" in name:
        raise errors.DataError("PostgreSQL identifiers cannot contain NUL (0x00) bytes")
    return '"' + name.replace('"', '""') + '"'


def quote_literal(text: str) -> str:
    """Quote a string as a PostgreSQL literal, independent of ``standard_conforming_strings``.

    Backslashes force the ``E'...'`` form with the backslash doubled, so the result
    means the same thing whichever way the server setting is configured.
    """
    if "\x00" in text:
        raise errors.DataError("PostgreSQL text fields cannot contain NUL (0x00) bytes")
    body = text.replace("'", "''")
    if "\\" in body:
        return " E'" + body.replace("\\", "\\\\") + "'"
    return "'" + body + "'"


class Composable:
    __slots__ = ("_obj",)

    def __init__(self, obj: Any) -> None:
        self._obj = obj

    def as_string(self, context: Any = None) -> str:
        raise NotImplementedError

    def as_redacted(self) -> str:
        """The statement with every Literal hidden: safe to log or put on a trace span."""
        return self.as_string()

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self._obj!r})"

    def __add__(self, other: Any) -> "Composed":
        if isinstance(other, Composed):
            return Composed([self, *other._obj])
        if isinstance(other, Composable):
            return Composed([self, other])
        return NotImplemented

    def __mul__(self, n: int) -> "Composed":
        return Composed([self] * n)

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self._obj == other._obj  # type: ignore[attr-defined]

    def __hash__(self) -> int:
        return hash((type(self), repr(self._obj)))


class Composed(Composable):
    def __init__(self, seq: Iterable[Composable]) -> None:
        items = list(seq)
        for item in items:
            if not isinstance(item, Composable):
                raise TypeError(f"Composed elements must be Composable, got {item!r}")
        super().__init__(items)

    def __iter__(self) -> Iterator[Composable]:
        return iter(self._obj)

    def as_string(self, context: Any = None) -> str:
        return "".join(item.as_string(context) for item in self._obj)

    def as_redacted(self) -> str:
        return "".join(item.as_redacted() for item in self._obj)

    def join(self, joiner: "str | SQL") -> "Composed":
        sep = SQL(joiner) if isinstance(joiner, str) else joiner
        out: list[Composable] = []
        for index, item in enumerate(self._obj):
            if index:
                out.append(sep)
            out.append(item)
        return Composed(out)


class SQL(Composable):
    def __init__(self, text: str) -> None:
        if not isinstance(text, str):
            raise TypeError(f"SQL values must be strings, got {text!r}")
        super().__init__(text)

    def as_string(self, context: Any = None) -> str:
        return self._obj

    def format(self, *args: Composable, **kwargs: Composable) -> Composed:
        parts: list[Composable] = []
        auto = 0
        for literal, field, spec, conversion in string.Formatter().parse(self._obj):
            if spec or conversion:
                raise ValueError("no format specification supported by SQL")
            if literal:
                parts.append(SQL(literal))
            if field is None:
                continue
            if field == "":
                value = args[auto]
                auto += 1
            elif field.isdigit():
                value = args[int(field)]
            else:
                value = kwargs[field]
            if not isinstance(value, Composable):
                raise TypeError(f"SQL.format() arguments must be Composable, got {value!r}")
            parts.append(value)
        return Composed(parts)

    def join(self, seq: Iterable[Composable]) -> Composed:
        return Composed(list(seq)).join(self)


class Identifier(Composable):
    def __init__(self, *strings: str) -> None:
        if not strings:
            raise TypeError("Identifier requires at least one string")
        for s in strings:
            if not isinstance(s, str):
                raise TypeError(f"SQL identifier parts must be strings, got {s!r}")
        super().__init__(strings)

    def as_string(self, context: Any = None) -> str:
        return ".".join(quote_ident(s) for s in self._obj)


class Literal(Composable):
    def as_string(self, context: Any = None) -> str:
        return _literal(self._obj)

    def as_redacted(self) -> str:
        return "?"


class Placeholder(Composable):
    def __init__(self, name: str = "") -> None:
        super().__init__(name)

    def as_string(self, context: Any = None) -> str:
        return f"%({self._obj})s" if self._obj else "%s"


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if value is True or value is False:
        return "true" if value else "false"
    if isinstance(value, int):
        return f" {value}" if value < 0 else str(value)
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return quote_literal(str(value).replace("inf", "Infinity").replace("nan", "NaN")) + "::float8"
        return f" {value!r}" if value < 0 else repr(value)
    if isinstance(value, Decimal):
        if not value.is_finite():
            return quote_literal(str(value)) + "::numeric"
        return f" {value}" if value < 0 else str(value)
    if isinstance(value, str):
        return quote_literal(value)
    if isinstance(value, (bytes, bytearray, memoryview)):
        return "decode('" + binascii.hexlify(bytes(value)).decode() + "', 'hex')"  # no backslash for the server to read
    if isinstance(value, datetime):
        return quote_literal(value.isoformat(" ")) + ("::timestamptz" if value.tzinfo else "::timestamp")
    if isinstance(value, date):
        return quote_literal(value.isoformat()) + "::date"
    if isinstance(value, time):
        return quote_literal(value.isoformat()) + ("::timetz" if value.tzinfo else "::time")
    if isinstance(value, timedelta):
        return quote_literal(f"{value.days} days {value.seconds} seconds {value.microseconds} microseconds") + "::interval"
    if isinstance(value, uuid.UUID):
        return quote_literal(str(value)) + "::uuid"
    if isinstance(value, Json):
        return quote_literal(value.encode()) + ("::jsonb" if isinstance(value, Jsonb) else "::json")
    if isinstance(value, list):
        return "ARRAY[" + ",".join(_literal(v) for v in value) + "]" if value else "'{}'"
    raise errors.ProgrammingError(f"cannot make a SQL literal from {type(value).__name__!r}")
