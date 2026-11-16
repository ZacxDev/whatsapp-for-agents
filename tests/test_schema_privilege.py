#!/usr/bin/env python3
"""The app role must be able to apply its own DDL with NO database privilege.

`whatsapp_app` owns the `whatsapp` schema and can use it, and holds nothing beyond
it — that is what keeps a compromised ingester away from wuzapi's session tokens,
which live in a separate database.

Postgres, though, checks CREATE privilege ON THE DATABASE before taking the
`IF NOT EXISTS` shortcut, so `CREATE SCHEMA IF NOT EXISTS whatsapp` FAILS for such
a role even though the schema is sitting right there. Verified against a real
cluster: with the app role, statement 0 of 24 raised
`InsufficientPrivilege: permission denied for database …` and the other 23
passed.

These tests pin the catalog check that replaces it — hermetically, with a fake
connection, because the failure is about which statements we CHOOSE to run.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import _wa_db  # noqa: E402


class _FakeCursor:
    """Records SQL; answers the `pg_namespace` probe with `schema_present`."""

    def __init__(self, schema_present: bool):
        self.schema_present = schema_present
        self.sql: list[str] = []
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.sql.append(" ".join(sql.split()))
        # The catalog probe is the only statement handed parameters.
        self._row = (1,) if (params is not None and self.schema_present) else None

    def fetchone(self):
        return self._row


class _FakeConn:
    def __init__(self, cur):
        self._cur = cur

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return self._cur


def _statements_run(schema_present: bool) -> list[str]:
    cur = _FakeCursor(schema_present)
    original = _wa_db.connect
    _wa_db.connect = lambda dsn=None, autocommit=False: _FakeConn(cur)
    try:
        _wa_db.ensure_schema("postgresql://fake/fake")
    finally:
        _wa_db.connect = original
    return cur.sql


class TestEnsureSchemaWithoutDatabasePrivilege(unittest.TestCase):
    def test_with_the_schema_PRESENT_it_never_runs_CREATE_SCHEMA(self):
        # This is the whole fix: a role with no CREATE on the database must still
        # be able to start, because the schema it uses already exists.
        sql = _statements_run(schema_present=True)
        offenders = [s for s in sql if s.upper().startswith("CREATE SCHEMA")]
        self.assertEqual(offenders, [],
                         "ensure_schema tried to CREATE SCHEMA for a role that "
                         "owns no database — it would die at startup")

    def test_with_the_schema_present_EVERY_object_statement_still_runs(self):
        # The skip must be surgical. Dropping the object DDL would make the
        # schema self-heal silently stop working.
        sql = _statements_run(schema_present=True)
        body = [s for s in sql if not s.upper().startswith("SELECT 1 FROM PG_NAMESPACE")]
        self.assertEqual(len(body), len(_wa_db.OBJECT_STATEMENTS))

    def test_with_the_schema_ABSENT_it_creates_it_FIRST(self):
        sql = _statements_run(schema_present=False)
        self.assertTrue(sql[0].upper().startswith("SELECT 1 FROM PG_NAMESPACE"))
        self.assertTrue(sql[1].upper().startswith("CREATE SCHEMA"),
                        f"a missing schema must still be created, got {sql[1][:60]}")
        self.assertEqual(len(sql), 1 + len(_wa_db.SCHEMA_STATEMENTS))

    def test_the_probe_actually_asks_the_catalog(self):
        # POSITIVE CONTROL: if `_schema_exists` returned a constant, the two
        # branches above would be indistinguishable. The probe must ask
        # `pg_namespace` with the schema name as a parameter.
        sql = _statements_run(schema_present=True)
        self.assertIn("PG_NAMESPACE", sql[0].upper())
        self.assertIn("NSPNAME", sql[0].upper())

    def test_the_INIT_path_still_includes_the_create(self):
        # `consumer.py init-schema` and the schema-init Job apply the whole list,
        # so the statement must not have been dropped, only reordered out of the
        # runtime path.
        self.assertEqual(_wa_db.SCHEMA_STATEMENTS[0], _wa_db.CREATE_SCHEMA_STATEMENT)
        self.assertEqual(len(_wa_db.SCHEMA_STATEMENTS),
                         1 + len(_wa_db.OBJECT_STATEMENTS))


if __name__ == "__main__":
    unittest.main()