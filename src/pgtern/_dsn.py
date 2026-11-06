# SPDX-License-Identifier: Apache-2.0
"""Connection strings: ``postgresql://`` URLs and ``key=value`` strings, ``PG*`` environment defaults, and ``.pgpass``."""
from __future__ import annotations

import getpass
import os
import stat
from dataclasses import dataclass, field
from urllib.parse import unquote

from . import errors

SSL_MODES = ("disable", "allow", "prefer", "require", "verify-ca", "verify-full")
REQUIRE_AUTH = ("scram-sha-256",)
TARGET_SESSION_ATTRS = ("any", "read-write", "read-only", "primary", "standby", "prefer-standby")
DEFAULT_CONNECT_TIMEOUT = 30.0  # seconds for the whole connect; connect_timeout=0 means no limit

_ENV = {
    "host": "PGHOST", "hostaddr": "PGHOSTADDR", "port": "PGPORT", "user": "PGUSER", "password": "PGPASSWORD",
    "dbname": "PGDATABASE", "sslmode": "PGSSLMODE", "sslrootcert": "PGSSLROOTCERT", "sslcrl": "PGSSLCRL",
    "sslcert": "PGSSLCERT", "sslkey": "PGSSLKEY", "connect_timeout": "PGCONNECT_TIMEOUT",
    "application_name": "PGAPPNAME", "options": "PGOPTIONS",
    "target_session_attrs": "PGTARGETSESSIONATTRS",
}
_KNOWN = set(_ENV) | {"database", "require_auth", "sslpassword"}


@dataclass
class ConnParams:
    hosts: list[tuple[str, int]]
    user: str
    password: str | None = field(default=None, repr=False)  # never shown in repr(), so never in logs
    dbname: str = ""
    sslmode: str = "prefer"
    sslrootcert: str | None = None
    sslcert: str | None = None
    sslkey: str | None = None
    sslcrl: str | None = None
    sslpassword: str | None = field(default=None, repr=False)
    connect_timeout: float | None = None
    application_name: str | None = None
    options: str | None = None
    require_auth: str | None = None
    target_session_attrs: str = "any"
    hostaddrs: list[str | None] = field(default_factory=list)  # parallel to ``hosts``; None = connect to the host name


def _unescape(text: str) -> str:
    """Percent-decoding without the form-encoding rule: '+' stays a '+'."""
    return unquote(text)


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
    for pair in query.split("&"):
        if not pair:
            continue
        key, _, value = pair.partition("=")
        out[_unescape(key)] = _unescape(value)
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


# ---------------------------------------------------------------- .pgpass

def _pgpass_path() -> str | None:
    explicit = os.environ.get("PGPASSFILE")
    if explicit:
        return explicit
    return os.path.join(os.path.expanduser("~"), ".pgpass")


def _split_pgpass_line(line: str) -> list[str] | None:
    fields: list[str] = []
    current: list[str] = []
    i = 0
    while i < len(line):
        c = line[i]
        if c == "\\" and i + 1 < len(line) and line[i + 1] in ":\\":
            current.append(line[i + 1])
            i += 2
            continue
        if c == ":":
            fields.append("".join(current))
            current = []
        else:
            current.append(c)
        i += 1
    fields.append("".join(current))
    return fields if len(fields) == 5 else None


def pgpass_password(host: str, port: int, dbname: str, user: str) -> str | None:
    """The password from the .pgpass file for this target, or None. A file readable by other users is ignored."""
    path = _pgpass_path()
    if not path or not os.path.isfile(path):
        return None
    if os.name == "posix" and os.stat(path).st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        return None  # PostgreSQL client tools refuse such a file too; never trust a password from a world-readable file
    lookup_host = "localhost" if host.startswith("/") else host
    try:
        with open(path, encoding="utf-8") as handle:
            lines = handle.read().splitlines()
    except (OSError, UnicodeDecodeError):
        return None
    for line in lines:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        parts = _split_pgpass_line(line)
        if parts is None:
            continue
        h, p, d, u, password = parts
        if h not in ("*", lookup_host) or p not in ("*", str(port)):
            continue
        if d not in ("*", dbname) or u not in ("*", user):
            continue
        return password
    return None


def resolve_password(params: ConnParams, host: str, port: int) -> str | None:
    """The password for one connection attempt: the explicit one, else the .pgpass entry for that server."""
    if params.password is not None:
        return params.password
    return pgpass_password(host, port, params.dbname, params.user)


# ---------------------------------------------------------------- build

def _split_ports(port_text: str, count_hosts: int) -> list[int]:
    try:
        numbers = [int(p) if p else 5432 for p in port_text.split(",")] if port_text else [5432]
    except ValueError:
        raise errors.ProgrammingError(f"invalid port {port_text!r}") from None
    if len(numbers) not in (1, count_hosts):
        raise errors.ProgrammingError(
            f"{len(numbers)} ports were given for {count_hosts} hosts: give one port, or one per host")
    return numbers


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
    host_list = host_text.split(",") if host_text else None
    if host_list is None:
        host_list = [_default_host(_split_ports(values.get("port", ""), 1)[0])]
    port_numbers = _split_ports(values.get("port", ""), len(host_list))
    addr_text = values.get("hostaddr", "")
    addr_list: list[str | None] = [a for a in addr_text.split(",")] if addr_text else [None] * len(host_list)
    if len(addr_list) != len(host_list):
        raise errors.ProgrammingError("hostaddr must give one address per host")
    hosts: list[tuple[str, int]] = []
    for index, host in enumerate(host_list):
        port = port_numbers[index] if len(port_numbers) > 1 else port_numbers[0]
        hosts.append((host or _default_host(port), port))
    hostaddrs = [addr or None for addr in addr_list]

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
    attrs = values.get("target_session_attrs") or "any"
    if attrs not in TARGET_SESSION_ATTRS:
        raise errors.ProgrammingError(f"invalid target_session_attrs value {attrs!r}")
    timeout_text = values.get("connect_timeout")
    try:
        timeout = DEFAULT_CONNECT_TIMEOUT if timeout_text is None or timeout_text == "" else float(timeout_text)
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
        sslcrl=values.get("sslcrl") or None,
        sslpassword=values.get("sslpassword") or None,
        connect_timeout=timeout if timeout > 0 else None,
        application_name=values.get("application_name") or None,
        options=values.get("options") or None,
        require_auth=require_auth,
        target_session_attrs=attrs,
        hostaddrs=hostaddrs,
    )
