# SPDX-License-Identifier: Apache-2.0
"""Text-format parsing and formatting for PostgreSQL records (composite types) and ranges."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, Sequence

from . import errors


def parse_record(text: str) -> list[str | None]:
    """``(a,"b c",,"d""e")`` -> ``["a", "b c", None, 'd"e']``. An unquoted empty field is NULL."""
    if len(text) < 2 or text[0] != "(" or text[-1] != ")":
        raise errors.DataError(f"malformed record literal: {text!r}")
    s = text[1:-1]
    n = len(s)
    if n == 0:
        return [None]
    out: list[str | None] = []
    i = 0
    while True:
        buf: list[str] = []
        quoted = False
        if i < n and s[i] == '"':
            quoted = True
            i += 1
            while i < n:
                c = s[i]
                if c == "\\":
                    if i + 1 >= n:
                        raise errors.DataError(f"malformed record literal: {text!r}")
                    buf.append(s[i + 1])
                    i += 2
                elif c == '"':
                    if i + 1 < n and s[i + 1] == '"':
                        buf.append('"')
                        i += 2
                    else:
                        i += 1
                        break
                else:
                    buf.append(c)
                    i += 1
        else:
            while i < n and s[i] != ",":
                if s[i] == "\\" and i + 1 < n:
                    buf.append(s[i + 1])
                    i += 2
                else:
                    buf.append(s[i])
                    i += 1
        out.append("".join(buf) if (quoted or buf) else None)
        if i >= n:
            break
        if s[i] != ",":
            raise errors.DataError(f"malformed record literal: {text!r}")
        i += 1
        if i == n:  # trailing comma: one more, empty, field
            out.append(None)
            break
    return out


def _quote_field(value: str) -> str:
    if value == "" or any(c in value for c in ',()"\\') or value.isspace() or value != value.strip():
        return '"' + value.replace("\\", "\\\\").replace('"', '""') + '"'
    return value


def format_record(values: Sequence[str | None]) -> str:
    """Inverse of :func:`parse_record`: values are already text (``None`` is NULL)."""
    if len(values) == 0:
        return "()"
    return "(" + ",".join("" if v is None else _quote_field(v) for v in values) + ")"


@dataclass(frozen=True)
class Range:
    """A PostgreSQL range. ``lower``/``upper`` are ``None`` when unbounded (or when ``empty``)."""

    lower: Any = None
    upper: Any = None
    lower_inc: bool = True
    upper_inc: bool = False
    empty: bool = False


def parse_range(text: str, element: Callable[[str], Any] | None = None) -> Range:
    if text == "empty":
        return Range(empty=True, lower_inc=False, upper_inc=False)
    if len(text) < 2 or text[0] not in "[(" or text[-1] not in "])":
        raise errors.DataError(f"malformed range literal: {text!r}")
    lo, hi = parse_record("(" + text[1:-1] + ")")[:2] if text[1:-1] else (None, None)
    conv = (lambda v: v if v is None or element is None else element(v))
    return Range(conv(lo), conv(hi), text[0] == "[", text[-1] == "]")


def format_range(r: Range, to_text: Callable[[Any], str] = str) -> str:
    if r.empty:
        return "empty"
    lo = "" if r.lower is None else _quote_field(to_text(r.lower))
    hi = "" if r.upper is None else _quote_field(to_text(r.upper))
    return ("[" if r.lower_inc else "(") + lo + "," + hi + ("]" if r.upper_inc else ")")
