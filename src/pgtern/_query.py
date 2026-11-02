# SPDX-License-Identifier: Apache-2.0
"""Turn ``%s`` / ``%(name)s`` placeholders into PostgreSQL ``$n`` parameters."""
from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from . import errors

_PLACEHOLDER = re.compile(r"%(?:\(([^)]*)\))?(.?)", re.S)


def translate(sql: str, params: Any, render: Any = None) -> tuple[str, list[Any]]:
    """Return ``(sql_with_$n, values)``.

    ``%s``, ``%b`` and ``%t`` are placeholders and ``%%`` is a literal percent sign.
    The query is scanned as plain text (string literals are not
    parsed), so a literal ``%`` inside a quoted string must be written ``%%``.
    With ``render`` (a function value -> SQL text) values are written into the SQL
    (client-side binding) instead of becoming ``$n``; used where PostgreSQL has no
    parameters, such as ``COPY``.
    """
    if isinstance(params, Mapping):
        named = True
    elif isinstance(params, Sequence) and not isinstance(params, (str, bytes, bytearray)):
        named = False
    else:
        raise errors.ProgrammingError(
            f"query parameters should be a sequence or a mapping, got {type(params).__name__}")

    out: list[str] = []
    values: list[Any] = []
    index_by_name: dict[str, int] = {}
    positional = 0
    last = 0
    for m in _PLACEHOLDER.finditer(sql):
        name, fmt = m.group(1), m.group(2)
        out.append(sql[last:m.start()])
        last = m.end()
        if fmt == "%" and name is None:
            out.append("%")
            continue
        if fmt == "":
            raise errors.ProgrammingError("incomplete placeholder: '%' at the end of the query")
        if fmt not in ("s", "b", "t"):
            raise errors.ProgrammingError(
                f"only '%s', '%b', '%t' are allowed as placeholders, got '{m.group(0)}'")
        if name is not None:
            if not named:
                raise errors.ProgrammingError("named placeholders require a mapping of parameters")
            if name not in index_by_name:
                if name not in params:
                    raise errors.ProgrammingError(f"query parameter missing: {name}")
                values.append(params[name])
                index_by_name[name] = len(values)
            out.append(f"${index_by_name[name]}" if render is None else render(params[name]))
        else:
            if named:
                raise errors.ProgrammingError("positional placeholders require a sequence of parameters")
            positional += 1
            out.append(f"${positional}" if render is None else render(params[positional - 1]))
    out.append(sql[last:])
    if not named:
        if positional != len(params):
            raise errors.ProgrammingError(
                f"the query has {positional} placeholders but {len(params)} parameters were passed")
        values = list(params)
    return "".join(out), values
