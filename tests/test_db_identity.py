#!/usr/bin/env python3
"""Live-Postgres tests for the identity merge — the module's highest-risk SQL.

🔴 WHY THIS FILE IS SKIPPED BY DEFAULT, AND WHY THAT IS NOT A VACUOUS TEST.
`_merge_contacts` is a sequence of UPDATE/DELETE statements whose correctness
depends on real constraints: `group_participants` has a composite PRIMARY KEY, so
the loser's duplicate rows must be deleted BEFORE the repoint or the UPDATE
aborts; the `contacts_pn_key` / `contacts_lid_key` partial unique indexes are what
make "one human, one row" true. None of that can be exercised by the in-memory
fake in `test_ingest.py` — a fake would happily accept a merge that real Postgres
rejects.

So it is gated on `WHATSAPP_TEST_DSN` rather than faked, and the skill documents
running it as a deploy step. It is self-cleaning: every fixture uses the reserved
`1999…` prefix and setUp/tearDown delete exactly those rows.

    WHATSAPP_TEST_DSN=postgresql://… python3 -m unittest test_db_identity -v
"""
from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import psycopg2  # noqa: F401  (_wa_db imports it)
except ImportError:  # pragma: no cover
    raise unittest.SkipTest("psycopg2 not installed")

# Outside the guard above, for the reason spelled out in `test_read_path.py`:
# a missing symbol in `_wa_db` is a failure, not a skip.
from _wa_db import SCHEMA, WaDB, cursor, ensure_schema

DSN = os.environ.get("WHATSAPP_TEST_DSN")

FIXTURE_CHAT = "19990000000@s.whatsapp.net"
FIXTURE_PN = "19990000001"
FIXTURE_PN_2 = "19990000002"
FIXTURE_LID = "199900000000001"

# 🔴 ONE skip for the WHOLE module, not one per test method.
# `run-tests.sh`'s GUARD 2 compares the skip TOTAL against the number of
# applicable `EXPECTED_SKIPS` entries (one pin per skipped test). A class-level
# `skipUnless` skips each of the six methods separately and would therefore need
# six identical pins — brittle in the direction that hurts: adding a seventh test
# would silently turn the gate red for the next author. A module-level SkipTest is
# reported as exactly one skip, so one pin describes it and new tests do not
# perturb the accounting.
if not DSN:
    raise unittest.SkipTest(
        "set WHATSAPP_TEST_DSN to run the live-Postgres identity tests"
    )


@unittest.skipUnless(DSN, "set WHATSAPP_TEST_DSN to run the live-Postgres identity tests")
class TestIdentityMerge(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ensure_schema(DSN)

    def setUp(self):
        self.db = WaDB(DSN)
        self._cleanup()

    def tearDown(self):
        self._cleanup()

    # -- fixtures ----------------------------------------------------------
    def _cleanup(self):
        """Delete fixtures in FOREIGN-KEY ORDER.

        🔴 The order is load-bearing and was learned from a live run: the first
        version deleted `contacts` before `group_participants`, and every test
        errored in setUp with
            update or delete on table "contacts" violates foreign key constraint
            "group_participants_contact_id_fkey"
        A hermetic fake would never have caught it — only the real schema has the
        constraints. Dependents first, then the rows they point at.
        """
        fixture_chat = "jid LIKE '1999%'"
        with cursor(DSN) as cur:
            # 1. reactions reference both a chat and a contact.
            cur.execute(
                f"""DELETE FROM {SCHEMA}.reactions WHERE chat_id IN
                    (SELECT id FROM {SCHEMA}.chats WHERE {fixture_chat})"""
            )
            # 2. messages reference both too.
            cur.execute(
                f"""DELETE FROM {SCHEMA}.messages WHERE chat_id IN
                    (SELECT id FROM {SCHEMA}.chats WHERE {fixture_chat})"""
            )
            # 3. group_participants reference contacts AND groups -> before both.
            cur.execute(
                f"""DELETE FROM {SCHEMA}.group_participants WHERE contact_id IN
                    (SELECT id FROM {SCHEMA}.contacts
                      WHERE pn LIKE '1999%' OR lid LIKE '1999%')"""
            )
            # 4. now the rows everyone above pointed at.
            cur.execute(f"DELETE FROM {SCHEMA}.chats WHERE {fixture_chat}")
            cur.execute(f"DELETE FROM {SCHEMA}.groups WHERE jid LIKE '1999%'")
            cur.execute(
                f"""DELETE FROM {SCHEMA}.contacts
                    WHERE pn LIKE '1999%' OR lid LIKE '1999%'"""
            )
            # 5. the audit log is plain BIGINTs (no FK), so it is independent.
            cur.execute(f"DELETE FROM {SCHEMA}.contact_merges WHERE evidence LIKE 'dual_address%'")

    def _contact_rows(self):
        with cursor(DSN) as cur:
            cur.execute(
                f"""SELECT id, pn, lid FROM {SCHEMA}.contacts
                    WHERE pn LIKE '1999%' OR lid LIKE '1999%'"""
            )
            return [dict(r) for r in cur.fetchall()]

    # -- tests -------------------------------------------------------------
    def test_two_identities_stay_separate_until_a_dual_event_arrives(self):
        """No guessing: without evidence tying them together, two addresses are two people."""
        lid_row = self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID)
        pn_row = self.db.find_or_create_contact(pn=FIXTURE_PN_2, lid=None)
        self.assertNotEqual(lid_row, pn_row)
        self.assertEqual(len(self._contact_rows()), 2)

        # Resolving the same single address again must NOT mint a third row.
        self.assertEqual(self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID), lid_row)
        self.assertEqual(len(self._contact_rows()), 2)

    def test_dual_address_collapses_to_a_single_row_carrying_both(self):
        lid_row = self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID)
        assert lid_row is not None
        pn_row = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=None)
        assert pn_row is not None

        merged = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=FIXTURE_LID)
        rows = self._contact_rows()
        self.assertEqual(len(rows), 1, f"expected ONE contact, got {rows}")
        self.assertEqual(rows[0]["pn"], FIXTURE_PN)
        self.assertEqual(rows[0]["lid"], FIXTURE_LID)
        self.assertIn(merged, (lid_row, pn_row))

    def test_merge_repoints_existing_messages(self):
        chat_id = self.db.upsert_chat(FIXTURE_CHAT, "dm")
        lid_row = self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID)
        assert lid_row is not None
        self.db.insert_message(
            chat_id=chat_id, whatsapp_id="19990000-FIXTURE-1",
            sender_contact_id=lid_row, from_me=False, kind="text",
            body="stored before we knew the phone number",
            caption=None, quoted_whatsapp_id=None,
            message_ts=datetime.now(timezone.utc), is_view_once=False,
            expires_at=None, raw={"fixture": True},
        )

        merged = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=FIXTURE_LID)
        with cursor(DSN) as cur:
            cur.execute(
                f"""SELECT sender_contact_id FROM {SCHEMA}.messages
                    WHERE whatsapp_id = '19990000-FIXTURE-1'"""
            )
            row = cur.fetchone()
        self.assertIsNotNone(row, "the message must survive the merge")
        self.assertEqual(row["sender_contact_id"], merged,
                         "the message must follow the surviving contact, not the deleted one")

    def test_merge_is_idempotent(self):
        self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID)
        self.db.find_or_create_contact(pn=FIXTURE_PN, lid=None)
        first = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=FIXTURE_LID)
        second = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=FIXTURE_LID)
        self.assertEqual(first, second)
        self.assertEqual(len(self._contact_rows()), 1)

    def test_merge_repoints_group_participants_without_a_pk_violation(self):
        """The composite PK on group_participants is what makes this ordering load-bearing."""
        group_jid = "199900000000001@g.us"
        group_id = self.db.upsert_group(group_jid, "fixture group")
        lid_row = self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID)
        pn_row = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=None)
        assert lid_row is not None and pn_row is not None

        with cursor(DSN) as cur:
            cur.execute(
                f"INSERT INTO {SCHEMA}.group_participants (group_id, contact_id) VALUES (%s, %s)",
                (group_id, lid_row),
            )
            cur.execute(
                f"INSERT INTO {SCHEMA}.group_participants (group_id, contact_id) VALUES (%s, %s)",
                (group_id, pn_row),
            )

        merged = self.db.find_or_create_contact(pn=FIXTURE_PN, lid=FIXTURE_LID)
        with cursor(DSN) as cur:
            cur.execute(
                f"""SELECT contact_id FROM {SCHEMA}.group_participants
                    WHERE group_id = %s""",
                (group_id,),
            )
            rows = [r["contact_id"] for r in cur.fetchall()]
        self.assertEqual(rows, [merged],
                         "the two participant rows must collapse to one, not raise or duplicate")

    def test_merge_leaves_an_audit_row(self):
        self.db.find_or_create_contact(pn=None, lid=FIXTURE_LID)
        self.db.find_or_create_contact(pn=FIXTURE_PN, lid=None)
        self.db.find_or_create_contact(pn=FIXTURE_PN, lid=FIXTURE_LID)
        with cursor(DSN) as cur:
            cur.execute(
                f"""SELECT kept_id, merged_id, evidence FROM {SCHEMA}.contact_merges
                    WHERE evidence LIKE 'dual_address%'"""
            )
            rows = [dict(r) for r in cur.fetchall()]
        self.assertEqual(len(rows), 1, "a merge with no audit row is unrecoverable after the fact")
        self.assertNotEqual(rows[0]["kept_id"], rows[0]["merged_id"])


if __name__ == "__main__":
    unittest.main()