# SPDX-License-Identifier: Apache-2.0
"""Wrapper types that tell pgtern how to send a Python value to PostgreSQL."""
from __future__ import annotations

import json as _json
from typing import Any, Callable

__all__ = ["Json", "Jsonb"]


class Json:
    """Send a Python object as a PostgreSQL ``json`` value."""

    __slots__ = ("obj", "dumps")

    def __init__(self, obj: Any, dumps: Callable[[Any], str] | None = None) -> None:
        self.obj = obj
        self.dumps = dumps or _json.dumps

    def encode(self) -> str:
        return self.dumps(self.obj)

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.obj!r})"


class Jsonb(Json):
    """Send a Python object as a PostgreSQL ``jsonb`` value."""

    __slots__ = ()
