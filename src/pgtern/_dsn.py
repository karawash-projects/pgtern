# SPDX-License-Identifier: Apache-2.0
"""Connection strings: ``postgresql://`` URLs and ``key=value`` strings, plus ``PG*`` environment defaults."""
from __future__ import annotations

import getpass
import os
from dataclasses import dataclass
from urllib.parse import parse_qsl, unquote

from . import errors

SSL_MODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")

_ENV = {
    "host": "PGHOST", "port": "PGPORT", "user": "PGUSER", "password": "PGPASSWORD",
    "dbname": "PGDATABASE", "sslmode": "PGSSLMODE", "sslrootcert": "PGSSLROOTCERT",
    "sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY", "connect_timeout": "PGCONNECT_TIMEOUT",
    "application_name": "PGAPPNAME", "options": "PGOPTIONS",
}
REQUIRE_AUTH = ("scram-sha-256",)
_KNOWN = set(_ENV) | {"database", "require_auth"}


@dataclass
class ConnParams:
    hosts: list[tuple[str, int]]
    user: str
    password: str | None
    dbname: str
    sslmode: str = "prefer"
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None
    connect_timeout: float | None = None
    application_name: str | None = None
    options: str | None = None
    require_auth: str | None = None


def _parse_url(url: str) -> dict[str, str]:
    scheme, _, rest = url.partition("://")
    if scheme not in ("postgres", "postgresql"):
        raise errors.ProgrammingError(f"invalid connection URL scheme {scheme!r}")
    rest = rest.partition("#")[0]
    rest, _, query = rest.partition("?")
    authority, _, path = rest.partition("/")
    userinfo, _, hostpart = authority.rpartition("@")
    out: dict[str, str] = {}
    if userinfo:
        user, colon, password = userinfo.partition(":")
        if user:
            out["user"] = unquote(user)
        if colon:
            out["password"] = unquote(password)
    hosts: list[str] = []
    ports: list[str] = []
    for item in hostpart.split(","):
        if item.startswith("["):
            end = item.index("]")
            host, port = item[1:end], (item[end + 2:] if item[end + 1:end + 2] == ":" else "")
        else:
            host, _, port = item.partition(":")
        hosts.append(unquote(host))
        ports.append(port)
    if any(hosts):
        out["host"] = ",".join(hosts)
    if any(ports):
        out["port"] = ",".join(ports)
    if path:
        out["dbname"] = unquote(path)
    for key, value in parse_qsl(query, keep_blank_values=True):
        out[key] = value
    return out


def _parse_keywords(s: str) -> dict[str, str]:
    out: dict[str, str] = {}
    i, n = 0, len(s)
    while i < n:
        while i < n and s[i].isspace():
            i += 1
        if i >= n:
            break
        j = i
        while j < n and s[j] not in "= \t\r\n":
            j += 1
        key = s[i:j]
        while j < n and s[j].isspace():
            j += 1
        if j >= n or s[j] != "=":
            raise errors.ProgrammingError(f"missing '=' after {key!r} in connection info string")
        j += 1
        while j < n and s[j].isspace():
            j += 1
        if j < n and s[j] == "'":
            j += 1
            buf: list[str] = []
            while True:
                if j >= n:
                    raise errors.ProgrammingError("unterminated quoted string in connection info string")
                c = s[j]
                if c == "\\" and j + 1 < n:
                    buf.append(s[j + 1])
                    j += 2
                elif c == "'":
                    j += 1
                    break
                else:
                    buf.append(c)
                    j += 1
            value = "".join(buf)
        else:
            k = j
            while k < n and not s[k].isspace():
                k += 1
            value, j = s[j:k], k
        out[key] = value
        i = j
    return out


def parse_conninfo(conninfo: str) -> dict[str, str]:
    conninfo = conninfo.strip()
    if conninfo.startswith(("postgresql://", "postgres://")):
        return _parse_url(conninfo)
    return _parse_keywords(conninfo)


def _default_host(port: int) -> str:
    for directory in ("/var/run/postgresql", "/tmp"):
        if os.path.exists(f"{directory}/.s.PGSQL.{port}"):
            return directory
    return "localhost"


def build_params(conninfo: str = "", **kwargs: object) -> ConnParams:
    values = parse_conninfo(conninfo) if conninfo else {}
    for key, value in kwargs.items():
        if value is not None:
            values[key] = str(value)
    if "database" in values:
        values.setdefault("dbname", values.pop("database"))
    unknown = set(values) - _KNOWN
    if unknown:
        raise errors.ProgrammingError(f"invalid connection option {sorted(unknown)[0]!r}")
    for key, env in _ENV.items():
        if not values.get(key) and os.environ.get(env):
            values[key] = os.environ[env]

    user = values.get("user") or getpass.getuser()
    host_text = values.get("host", "")
    port_text = values.get("port", "")
    ports = [p for p in port_text.split(",")] if port_text else []
    try:
        port_numbers = [int(p) if p else 5432 for p in ports] or [5432]
    except ValueError:
        raise errors.ProgrammingError(f"invalid port {port_text!r}") from None
    host_list = host_text.split(",") if host_text else [_default_host(port_numbers[0])]
    hosts: list[tuple[str, int]] = []
    for index, host in enumerate(host_list):
        port = port_numbers[index] if len(port_numbers) > 1 else port_numbers[0]
        hosts.append((host or _default_host(port), port))

    sslmode = values.get("sslmode") or "prefer"
    if sslmode not in SSL_MODES:
        raise errors.ProgrammingError(f"invalid sslmode value {sslmode!r}")
    sslrootcert = values.get("sslrootcert") or None
    if sslmode in ("verify-ca", "verify-full") and sslrootcert is None:
        default_root = os.path.join(os.path.expanduser("~"), ".postgresql", "root.crt")
        if not os.path.exists(default_root):
            raise errors.ProgrammingError(
                f"sslmode={sslmode} needs a root certificate: set sslrootcert to a CA file, "
                "or to 'system' to trust the system store on purpose")
        sslrootcert = default_root
    require_auth = values.get("require_auth") or None
    if require_auth is not None and require_auth not in REQUIRE_AUTH:
        raise errors.ProgrammingError(f"invalid require_auth value {require_auth!r}")
    timeout_text = values.get("connect_timeout")
    try:
        timeout = float(timeout_text) if timeout_text else None
    except ValueError:
        raise errors.ProgrammingError(f"invalid connect_timeout {timeout_text!r}") from None
    return ConnParams(
        hosts=hosts,
        user=user,
        password=values.get("password") or None,
        dbname=values.get("dbname") or user,
        sslmode=sslmode,
        sslrootcert=sslrootcert,
        sslcert=values.get("sslcert") or None,
        sslkey=values.get("sslkey") or None,
        connect_timeout=timeout if timeout and timeout > 0 else None,
        application_name=values.get("application_name") or None,
        options=values.get("options") or None,
        require_auth=require_auth,
    )
