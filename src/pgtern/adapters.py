# SPDX-License-Identifier: Apache-2.0
"""Custom type adapters: register how your own Python types go to and from the database.

Loaders are registered by PostgreSQL type name (``"mood"`` or ``"public.mood"``) and apply
to results (and to arrays of that type). Dumpers are registered by Python class and apply to
parameters. Registrations are process-wide.

    from pgtern import adapters

    adapters.register_loader("mood", adapters.enum_loader(Mood))
    adapters.register_loader("address", adapters.record_loader(Address, (str, int)))
    adapters.register_loader("span", adapters.range_loader(int))
    adapters.register_dumper(Money, lambda m: f"{m.cents / 100:.2f}")
"""
from __future__ import annotations

from enum import Enum
from typing import Any, Callable, Sequence

from . import _types
from ._composite import Range, format_range, format_record, parse_range, parse_record

__all__ = [
    "register_loader", "unregister_loader", "register_dumper", "unregister_dumper",
    "enum_loader", "record_loader", "range_loader", "range_dumper", "record_dumper",
    "Range", "parse_record", "format_record", "parse_range", "format_range",
]


def register_loader(type_name: str, loader: Callable[[str], Any]) -> None:
    """``loader(text)`` builds the Python value from the server's text form of the type."""
    _types._LOADERS[type_name] = loader


def unregister_loader(type_name: str) -> None:
    _types._LOADERS.pop(type_name, None)


def register_dumper(cls: type, dumper: Callable[[Any], str | None]) -> None:
    """``dumper(value)`` returns the text form (``None`` for SQL NULL). Sent untyped, so the
    server infers the type from context (``col = %s``); add a cast in SQL if needed."""
    _types._DUMPERS[cls] = dumper


def unregister_dumper(cls: type) -> None:
    _types._DUMPERS.pop(cls, None)


def enum_loader(enum_cls: type[Enum]) -> Callable[[str], Enum]:
    return lambda text: enum_cls(text)


def record_loader(factory: Callable[..., Any], fields: Sequence[Callable[[str], Any] | type | None] = ()) -> Callable[[str], Any]:
    """Build ``factory(*fields)`` from a composite value. ``fields`` converts each text field
    (e.g. ``(str, int)``); a missing or ``None`` converter keeps the text, NULL stays ``None``."""
    def load(text: str) -> Any:
        values = parse_record(text)
        converted = []
        for i, v in enumerate(values):
            conv = fields[i] if i < len(fields) else None
            converted.append(v if v is None or conv is None else conv(v))
        return factory(*converted)
    return load


def _dumped_text(value: Any) -> str:
    _oid, data = _types.dump(value)
    if data is None:
        raise TypeError(f"{type(value).__name__} cannot be sent as text")
    return data.decode()


def record_dumper(getter: Callable[[Any], Sequence[Any]]) -> Callable[[Any], str]:
    """``getter(obj)`` returns the fields; each is sent in its text form (``None`` is NULL)."""
    def dump(obj: Any) -> str:
        return format_record([None if v is None else _dumped_text(v) for v in getter(obj)])
    return dump


def range_loader(element: Callable[[str], Any] | None = None) -> Callable[[str], Range]:
    return lambda text: parse_range(text, element)


def range_dumper() -> Callable[[Range], str]:
    return lambda r: format_range(r, _dumped_text)
