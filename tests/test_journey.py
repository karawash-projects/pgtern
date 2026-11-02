# SPDX-License-Identifier: Apache-2.0
"""A full application life-cycle, end to end, against a real server.

Provision database and roles -> migrate -> multi-tenant data under row-level security ->
concurrent workers -> bulk load and streaming -> job queue over LISTEN/NOTIFY ->
observability hooks -> failure handling -> the same flows on the asyncio API -> teardown.
"""
from __future__ import annotations

import asyncio
import datetime as dt
import decimal
import threading
import time
import uuid
from contextlib import contextmanager

import pytest

import pgtern
from pgtern import aio, errors, sql
from pgtern.rows import dict_row
from pgtern.types import Jsonb

from conftest import url

pytestmark = pytest.mark.integration

MIGRATION_1 = """
CREATE TABLE tenants (id uuid PRIMARY KEY DEFAULT gen_random_uuid(), name text NOT NULL UNIQUE);
CREATE TABLE items (
    id bigserial PRIMARY KEY,
    tenant_id uuid NOT NULL REFERENCES tenants(id),
    title text NOT NULL,
    qty int NOT NULL DEFAULT 0 CHECK (qty >= 0),
    price numeric(12,2),
    tags text[] NOT NULL DEFAULT '{}',
    meta jsonb NOT NULL DEFAULT '{}',
    blob bytea,
    created timestamptz NOT NULL DEFAULT now(),
    due date
);
CREATE INDEX items_tenant_idx ON items (tenant_id);
ALTER TABLE items ENABLE ROW LEVEL SECURITY;
ALTER TABLE items FORCE ROW LEVEL SECURITY;
CREATE POLICY tenant_isolation ON items
    USING (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid)
    WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), '')::uuid);
-- semicolons and quotes inside a string must not confuse a multi-statement script
INSERT INTO tenants (name) VALUES ('seed; tenant ''zero''');
"""
MIGRATION_2 = """
CREATE TABLE jobs (id bigserial PRIMARY KEY, payload jsonb NOT NULL, state text NOT NULL DEFAULT 'new', worker text);
CREATE TABLE counters (name text PRIMARY KEY, n bigint NOT NULL);
CREATE TABLE events (id bigserial PRIMARY KEY, tenant_id uuid NOT NULL, title text NOT NULL, qty int NOT NULL, tags text[] NOT NULL, meta jsonb NOT NULL);
INSERT INTO counters VALUES ('hits', 0);
CREATE FUNCTION notify_job() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN PERFORM pg_notify('jobs', NEW.id::text); RETURN NEW; END $$;
CREATE TRIGGER jobs_notify AFTER INSERT ON jobs FOR EACH ROW EXECUTE FUNCTION notify_job();
GRANT ALL ON ALL TABLES IN SCHEMA public TO journey_app;
GRANT ALL ON ALL SEQUENCES IN SCHEMA public TO journey_app;
"""


@pytest.fixture(scope="module")
def env(dsn):
    """Fresh database and an unprivileged application role, removed at the end."""
    suffix = uuid.uuid4().hex[:8]
    db = f"journey_{suffix}"
    with pgtern.connect(dsn, autocommit=True) as admin:
        admin.execute("DROP ROLE IF EXISTS journey_app")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))
        admin.execute(sql.SQL("CREATE ROLE journey_app LOGIN PASSWORD {}").format(sql.Literal("it's a \\ pass")))
    owner = url("postgres", "postgres", dbname=db)
    app = url("journey_app", "it%27s%20a%20%5C%20pass", dbname=db)
    yield {"db": db, "owner": owner, "app": app}
    with pgtern.connect(dsn, autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid()", [db])
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db)))
        admin.execute("DROP ROLE IF EXISTS journey_app")


@pytest.fixture(scope="module")
def tenants(env):
    """Run the migrations as the owner, create two tenants, return their ids."""
    with pgtern.connect(env["owner"]) as c:
        c.execute("CREATE TABLE schema_migrations (version int PRIMARY KEY, applied timestamptz NOT NULL DEFAULT now())")
        for version, script in ((1, MIGRATION_1), (2, MIGRATION_2)):
            with c.transaction():
                c.execute(script)  # a multi-statement script, no parameters
                c.execute("INSERT INTO schema_migrations (version) VALUES (%s)", [version])
        ids = {}
        for name in ("acme", "globex"):
            ids[name] = c.execute("INSERT INTO tenants (name) VALUES (%s) RETURNING id", [name]).fetchone()[0]
    return ids


def test_01_provisioning_and_auth(env):
    with pgtern.connect(env["app"], row_factory=dict_row) as c:
        row = c.execute("SELECT current_user AS u, current_database() AS d, version() AS v").fetchone()
        assert row["u"] == "journey_app" and row["d"] == env["db"]
        assert c.info.server_version >= 140000
    with pytest.raises(errors.InvalidPassword):
        pgtern.connect(env["app"].replace("%5C", "%5D"))
    with pytest.raises(errors.InvalidCatalogName):
        pgtern.connect(url("postgres", "postgres", dbname="no_such_db_zz"))


def test_02_migrations_applied_exactly_once(env, tenants):
    with pgtern.connect(env["owner"]) as c:
        assert [r[0] for r in c.execute("SELECT version FROM schema_migrations ORDER BY 1")] == [1, 2]
        assert c.execute("SELECT count(*) FROM tenants").fetchone() == (3,)
        assert c.execute("SELECT name FROM tenants WHERE name LIKE 'seed%'").fetchone() == ("seed; tenant 'zero'",)
        with pytest.raises(errors.UniqueViolation) as ei:
            c.execute("INSERT INTO schema_migrations (version) VALUES (1)")
        assert ei.value.diag.constraint_name == "schema_migrations_pkey"


def _as_tenant(conn, tenant_id):
    conn.execute("SELECT set_config('app.tenant_id', %s, true)", [str(tenant_id)])


def test_03_crud_with_all_types_under_rls(env, tenants):
    acme, globex = tenants["acme"], tenants["globex"]
    when = dt.datetime(2026, 3, 1, 9, 30, 15, 123456, tzinfo=dt.timezone(dt.timedelta(hours=2)))
    with pgtern.connect(env["app"], row_factory=dict_row) as c:
        with c.transaction():
            _as_tenant(c, acme)
            new = c.execute(
                "INSERT INTO items (tenant_id, title, qty, price, tags, meta, blob, created, due)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                [acme, "Wídget – ünïcode ✓", 5, decimal.Decimal("19.99"), ["a", "b,c", 'q"uote', None],
                 Jsonb({"nested": {"list": [1, 2.5, None, "x"]}, "ok": True}), b"\x00\x01\xfe\xff", when, dt.date(2026, 12, 31)],
            ).fetchone()
            assert new["title"] == "Wídget – ünïcode ✓"
            assert new["price"] == decimal.Decimal("19.99")
            assert new["tags"] == ["a", "b,c", 'q"uote', None]
            assert new["meta"] == {"nested": {"list": [1, 2.5, None, "x"]}, "ok": True}
            assert new["blob"] == b"\x00\x01\xfe\xff"
            assert new["created"] == when and new["due"] == dt.date(2026, 12, 31)
            c.cursor().executemany("INSERT INTO items (tenant_id, title, qty) VALUES (%s, %s, %s)", [(acme, f"bulk{i}", i) for i in range(10)])
            assert c.execute("SELECT count(*) AS n FROM items").fetchone()["n"] == 11
            upd = c.execute("UPDATE items SET qty = qty + 1 WHERE title LIKE %s", ["bulk%"])
            assert upd.rowcount == 10
            assert c.execute("DELETE FROM items WHERE title = %s", ["bulk0"]).rowcount == 1
        with c.transaction():
            _as_tenant(c, globex)
            assert c.execute("SELECT count(*) AS n FROM items").fetchone()["n"] == 0  # acme's rows are invisible
            with pytest.raises(errors.InsufficientPrivilege):  # WITH CHECK blocks writing into another tenant
                c.execute("INSERT INTO items (tenant_id, title) VALUES (%s, 'sneaky')", [acme])
        with c.transaction():
            assert c.execute("SELECT count(*) AS n FROM items").fetchone()["n"] == 0  # no tenant set: nothing visible
        with c.transaction():
            _as_tenant(c, acme)
            assert c.execute("SELECT count(*) AS n FROM items").fetchone()["n"] == 10
        with pytest.raises(errors.CheckViolation):
            with c.transaction():
                _as_tenant(c, acme)
                c.execute("UPDATE items SET qty = -1 WHERE title = 'bulk1'")
        assert c.info.transaction_status.name == "IDLE"
        with pytest.raises(errors.ForeignKeyViolation):
            with c.transaction():
                _as_tenant(c, uuid.uuid4())
                c.execute("INSERT INTO items (tenant_id, title) VALUES (current_setting('app.tenant_id')::uuid, 'orphan')")


def test_04_dynamic_sql_composition_and_injection_safety(env, tenants):
    acme = tenants["acme"]
    nasty = "x'; DROP TABLE items; --"
    with pgtern.connect(env["app"]) as c:
        with c.transaction():
            _as_tenant(c, acme)
            q = sql.SQL("SELECT {col} FROM {tbl} WHERE title = {lit} OR title = %s ORDER BY {col} LIMIT 1").format(
                col=sql.Identifier("title"), tbl=sql.Identifier("public", "items"), lit=sql.Literal(nasty))
            assert c.execute(q, [nasty]).fetchone() is None
            assert "DROP TABLE" in q.as_string() and "?" in q.as_redacted() and "DROP" not in q.as_redacted()
            weird = 'we"ird name'
            c.execute(sql.SQL("CREATE TEMP TABLE {} (x int)").format(sql.Identifier(weird)))
            c.execute(sql.SQL("INSERT INTO {} VALUES (1)").format(sql.Identifier(weird)))
        assert c.execute("SELECT count(*) FROM items").fetchone() is not None


def test_05_concurrent_workers_through_the_pool(env, tenants):
    workers, loops = 12, 25
    failures: list[BaseException] = []
    with pgtern.ConnectionPool(env["app"], min_size=2, max_size=6, timeout=30) as pool:
        def work(n):
            try:
                for i in range(loops):
                    with pool.connection() as c:
                        # lost updates are impossible with a row lock; deadlocks/serialization retried
                        c.execute("UPDATE counters SET n = n + 1 WHERE name = 'hits'")
                    with pool.connection() as c:
                        _as_tenant(c, tenants["acme"])  # set_config(..., true) -> transaction-local, never leaks
                    with pool.connection() as c:
                        assert c.execute("SELECT current_setting('app.tenant_id', true)").fetchone()[0] in (None, "")
            except BaseException as exc:  # noqa: BLE001
                failures.append(exc)

        ts = [threading.Thread(target=work, args=(n,)) for n in range(workers)]
        [t.start() for t in ts]
        [t.join(120) for t in ts]
        assert not failures, failures[0]
        with pool.connection() as c:
            assert c.execute("SELECT n FROM counters WHERE name = 'hits'").fetchone() == (workers * loops,)
        with pytest.raises(RuntimeError):
            with pool.connection() as c:
                c.execute("UPDATE counters SET n = 0")
                raise RuntimeError("boom")
        with pool.connection() as c:  # the aborted update was rolled back before reuse
            assert c.execute("SELECT n FROM counters WHERE name = 'hits'").fetchone() == (workers * loops,)


def test_06_deadlock_is_reported_with_the_right_class(env, tenants):
    with pgtern.connect(env["owner"]) as a, pgtern.connect(env["owner"]) as b:
        a.execute("INSERT INTO counters VALUES ('d1', 0), ('d2', 0)")
        a.commit()
        a.execute("UPDATE counters SET n = 1 WHERE name = 'd1'")
        b.execute("UPDATE counters SET n = 1 WHERE name = 'd2'")
        result: list[BaseException | None] = []

        def second():
            try:
                a.execute("UPDATE counters SET n = 2 WHERE name = 'd2'")
                result.append(None)
            except BaseException as exc:  # noqa: BLE001
                result.append(exc)

        t = threading.Thread(target=second)
        t.start()
        time.sleep(0.5)
        try:
            b.execute("UPDATE counters SET n = 2 WHERE name = 'd1'")
            outcome_b = None
        except errors.DeadlockDetected as exc:
            outcome_b = exc
            b.rollback()
        t.join(30)
        outcomes = [outcome_b, result[0]]
        assert any(isinstance(o, errors.DeadlockDetected) for o in outcomes)
        assert isinstance(errors.DeadlockDetected("x"), errors.OperationalError)
        for conn in (a, b):
            conn.rollback()
        a.execute("DELETE FROM counters WHERE name IN ('d1','d2')")
        a.commit()


def test_07_bulk_load_and_streaming(env, tenants):
    acme, n = tenants["acme"], 20_000  # COPY FROM is not allowed on RLS tables, so it loads a plain one
    with pgtern.connect(env["app"]) as c:
        with c.transaction():
            with c.cursor() as cur:
                with cur.copy("COPY events (tenant_id, title, qty, tags, meta) FROM STDIN") as copy:
                    for i in range(n):
                        copy.write_row((acme, f"bulk-{i}", i % 100, [f"t{i % 7}"], Jsonb({"i": i})))
                assert cur.rowcount == n
            assert c.execute("SELECT count(*) FROM events").fetchone()[0] == n
            total = 0
            seen = 0
            with c.cursor("stream") as cur:
                cur.itersize = 500
                cur.execute("SELECT id, qty, meta FROM events ORDER BY id")
                for _id, qty, meta in cur:
                    total += qty
                    seen += 1
                    assert isinstance(meta["i"], int)
            assert seen == n and total == sum(i % 100 for i in range(n))
            with c.cursor() as cur, cur.copy("COPY (SELECT title, qty, tags FROM events WHERE title = 'bulk-7') TO STDOUT") as copy:
                assert list(copy.rows()) == [("bulk-7", "7", "{t0}")]
            c.execute("DELETE FROM events")


def test_08_job_queue_with_listen_notify_and_skip_locked(env, tenants):
    done: list[tuple[str, int]] = []
    stop = threading.Event()

    def worker(name):
        with pgtern.connect(env["app"], autocommit=True) as c:
            c.execute("LISTEN jobs")
            while not stop.is_set():
                while True:  # drain the queue
                    with c.transaction():
                        row = c.execute("SELECT id FROM jobs WHERE state = 'new' ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED").fetchone()
                        if row is None:
                            break
                        c.execute("UPDATE jobs SET state = 'done', worker = %s WHERE id = %s", [name, row[0]])
                        done.append((name, row[0]))
                for _ in c.notifies(timeout=0.2, stop_after=1):
                    pass

    threads = [threading.Thread(target=worker, args=(f"w{i}",)) for i in range(3)]
    [t.start() for t in threads]
    time.sleep(0.3)
    with pgtern.connect(env["app"]) as producer:
        for i in range(60):
            producer.execute("INSERT INTO jobs (payload) VALUES (%s)", [Jsonb({"n": i})])
            producer.commit()  # NOTIFY is delivered at commit
    deadline = time.monotonic() + 20
    while len(done) < 60 and time.monotonic() < deadline:
        time.sleep(0.1)
    stop.set()
    [t.join(10) for t in threads]
    assert sorted(i for _, i in done) == sorted(set(i for _, i in done)) and len(done) == 60  # each exactly once
    assert len({w for w, _ in done}) >= 1
    with pgtern.connect(env["app"]) as c:
        assert c.execute("SELECT count(*) FILTER (WHERE state = 'done'), count(*) FROM jobs").fetchone() == (60, 60)


def test_09_observability_hooks_never_see_parameters(env, tenants):
    seen = []

    @contextmanager
    def hook(info):
        t = time.perf_counter()
        try:
            yield
        finally:
            seen.append((info.statement, info.dbname, info.user, info.rowcount, time.perf_counter() - t))

    remove = pgtern.add_query_hook(hook)
    try:
        with pgtern.connect(env["app"]) as c:
            c.execute("SELECT %s::text", ["super-secret-password"])
            c.execute(sql.SQL("SELECT {}").format(sql.Literal("another-secret")))
            with pytest.raises(errors.UndefinedTable):
                c.execute("SELECT * FROM missing_zz")
    finally:
        remove()
    blob = repr(seen)
    assert "super-secret-password" not in blob and "another-secret" not in blob
    assert any(s[0] == "SELECT %s::text" and s[1] == env["db"] and s[2] == "journey_app" for s in seen)


def test_10_failure_handling(env, tenants, dsn):
    # statement timeout, cancellation of the connection, server-side kill, bad SQL, closed connection
    with pgtern.connect(env["app"], options="-c statement_timeout=300") as c:
        with pytest.raises(errors.QueryCanceled):
            c.execute("SELECT pg_sleep(5)")
        c.rollback()
        assert c.execute("SELECT 1").fetchone() == (1,)
    c = pgtern.connect(env["app"])
    with pgtern.connect(env["owner"], autocommit=True) as admin:
        admin.execute("SELECT pg_terminate_backend(%s)", [c.info.backend_pid])
    with pytest.raises(errors.OperationalError):
        c.execute("SELECT 1")
    assert c.closed
    with pytest.raises(errors.OperationalError):
        c.execute("SELECT 1")
    with pytest.raises(errors.OperationalError):
        pgtern.connect("postgresql://x@127.0.0.1:1/x", connect_timeout=2)
    with pgtern.connect(env["app"]) as c2:
        for bad in ("SELEC 1", "SELECT 1/0", "SELECT 'a'::int", "SELECT $1"):
            with pytest.raises(errors.DatabaseError):
                c2.execute(bad)
            c2.rollback()
        with pytest.raises(errors.ProgrammingError):
            c2.execute("SELECT %s, %s", [1])
        with pytest.raises(errors.ProgrammingError):
            c2.execute("SELECT 1", [1])
        assert c2.execute("SELECT 1").fetchone() == (1,)


def test_11_async_journey(env, tenants):
    acme, globex = tenants["acme"], tenants["globex"]

    async def go():
        async with aio.AsyncConnectionPool(env["app"], min_size=2, max_size=8) as pool:
            async def insert(i):
                async with pool.connection() as c:
                    async with c.transaction():
                        await c.execute("SELECT set_config('app.tenant_id', %s, true)", [str(globex)])
                        await c.execute("INSERT INTO items (tenant_id, title, qty) VALUES (%s, %s, %s)", [globex, f"async{i}", i])
            await asyncio.gather(*(insert(i) for i in range(40)))
            async with pool.connection() as c:
                async with c.transaction():
                    await c.execute("SELECT set_config('app.tenant_id', %s, true)", [str(globex)])
                    cur = await c.execute("SELECT count(*), sum(qty) FROM items")
                    assert cur.fetchone() == (40, sum(range(40)))
                    async with c.cursor().copy("COPY (SELECT title FROM items ORDER BY qty LIMIT 3) TO STDOUT") as copy:
                        assert [r async for r in copy.rows()] == [("async0",), ("async1",), ("async2",)]
                    scur = c.cursor("s")
                    scur.itersize = 7
                    await scur.execute("SELECT qty FROM items ORDER BY qty")
                    assert [r[0] async for r in scur] == list(range(40))
                    await scur.close()
                    await c.execute("DELETE FROM items")
            async with pool.connection() as c:
                async with c.transaction():
                    await c.execute("SELECT set_config('app.tenant_id', %s, true)", [str(acme)])
                    assert (await c.execute("SELECT count(*) FROM items")).fetchone()[0] == 10  # acme untouched
        # async listener + async producer
        async with await aio.connect(env["app"], autocommit=True) as listener, await aio.connect(env["app"]) as prod:
            await listener.execute("LISTEN jobs")
            async def produce():
                await asyncio.sleep(0.2)
                await prod.execute("INSERT INTO jobs (payload, state) VALUES ('{}', 'async')")
                await prod.commit()
            t = asyncio.create_task(produce())
            notes = [n async for n in listener.notifies(timeout=5, stop_after=1)]
            await t
            assert len(notes) == 1 and notes[0].channel == "jobs"

    asyncio.run(asyncio.wait_for(go(), 90))


def test_12_teardown_leaves_nothing_behind(env, tenants, dsn):
    with pgtern.connect(dsn, autocommit=True) as admin:
        active = admin.execute("SELECT count(*) FROM pg_stat_activity WHERE datname = %s AND pid <> pg_backend_pid() AND state <> 'idle'", [env["db"]]).fetchone()[0]
        assert active == 0  # no leaked in-flight statements from any earlier step
