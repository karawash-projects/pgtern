# SPDX-License-Identifier: Apache-2.0
from __future__ import annotations

import os
import uuid

import pytest

import pgtern

DSN = os.environ.get("PGTERN_TEST_DSN")
HOST = os.environ.get("PGTERN_TEST_HOST", "127.0.0.1")
PORT = os.environ.get("PGTERN_TEST_PORT", "5432")
SOCKET_DIR = os.environ.get("PGTERN_TEST_SOCKET_DIR")
CA = os.environ.get("PGTERN_TEST_CA")


AUTH_ROLES = os.environ.get("PGTERN_TEST_AUTH_ROLES")


def pytest_collection_modifyitems(config, items):
    if not DSN:
        skip = pytest.mark.skip(reason="set PGTERN_TEST_DSN (see scripts/start-test-postgres.sh)")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
    if not AUTH_ROLES:
        skip_auth = pytest.mark.skip(reason="needs the local auth cluster: set PGTERN_TEST_AUTH_ROLES=1")
        for item in items:
            if "auth_roles" in item.keywords:
                item.add_marker(skip_auth)


@pytest.fixture(scope="session")
def dsn() -> str:
    assert DSN
    return DSN


def url(user: str, password: str | None = None, dbname: str = "postgres", **query: str) -> str:
    auth = user if password is None else f"{user}:{password}"
    qs = "&".join(f"{k}={v}" for k, v in query.items())
    return f"postgresql://{auth}@{HOST}:{PORT}/{dbname}" + (f"?{qs}" if qs else "")


@pytest.fixture
def conn(dsn):
    with pgtern.connect(dsn) as c:
        yield c


@pytest.fixture
def aconn(dsn):
    with pgtern.connect(dsn, autocommit=True) as c:
        yield c


@pytest.fixture
def table(dsn):
    """Create a throwaway table; yields its name; dropped afterwards."""
    name = "t_" + uuid.uuid4().hex[:10]
    with pgtern.connect(dsn, autocommit=True) as c:
        c.execute(f"CREATE TABLE {name} (id serial PRIMARY KEY, name text UNIQUE NOT NULL, n int CHECK (n >= 0))")
        yield name
        c.execute("SET lock_timeout = '5s'")  # fail fast instead of hanging if a test leaked a transaction
        c.execute(f"DROP TABLE IF EXISTS {name}")
