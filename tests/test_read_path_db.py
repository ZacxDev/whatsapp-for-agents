#!/usr/bin/env python3
"""Live-Postgres tests for the READ PATH's SQL — what each query FETCHES.

🔴 WHY THIS FILE IS SKIPPED BY DEFAULT, AND WHY THAT IS NOT A VACUOUS TEST.
`test_read_path.py` pins the ORDER in which candidate names win, and it runs in
the gate. What it structurally cannot see is the SQL: whether `conversations`,
`search` and `show` each actually SELECT the candidates the resolver reads. That
gap is not hypothetical — the first version of this change shipped `show()`
without `c.chat_type`, and the resolver quietly returned `None` for every chat it
could name. Nothing hermetic could have caught it, because the defect is a
missing COLUMN, not a wrong branch.

So the wiring is checked here, against a real Postgres, in self-cleaning fixtures
that use the reserved `1999…` prefix (same convention, same delete-everything-
prefixed discipline as `test_db_identity.py`).

It is gated on `WHATSAPP_TEST_DSN` rather than faked, and the whatsapp skill
documents running it against the live schema as a deploy step:

    WHATSAPP_TEST_DSN=postgresql://… python3 -m unittest test_read_path_db -v
"""
from __future__ import annotations

import os
import sys
import unittest
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

# Fixtures. The three names are deliberately unrelated strings so that a test
# asserts WHICH source won and not merely that something non-empty came back.
WIRE_NAME = "THEIR OWN WIRE NAME"
CONTACT_NAME = "THEIR CONTACT ROW NAME"
OWN_NAME = "OUR OWN PUSH NAME"          # what a `from_me` message writes
PN_NAME = "A NAME FROM THE PN-SIDE ROW"

LID_CHAT = "199900000000042@lid"        # every source present, all different
PN_CHAT = "19990000099@s.whatsapp.net"  # contact row only; chat column poisoned
USERNAME_CHAT = "19990000098@s.whatsapp.net"
COLLIDE_CHAT = "19990000055@s.whatsapp.net"
GROUP_CHAT = "199900000000042@g.us"

BODY_MARKER = "zebraquiltfixture"

# 🔴 ONE skip for the WHOLE module, not one per test method — see the longer note
# in `test_db_identity.py`: a class-level skip reports one skip per method, so a
# test added later silently reds the gate. `run-tests.sh` carries the matching
# conditional pin.
if not DSN:
    raise unittest.SkipTest("set WHATSAPP_TEST_DSN to run the live read-path tests")


class TestReadPathNames(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ensure_schema(DSN)

    def setUp(self):
        self.db = WaDB(DSN)
        self._cleanup()
        self._fixtures()

    def tearDown(self):
        self._cleanup()

    # -- fixtures ----------------------------------------------------------
    def _cleanup(self):
        """Delete every `1999…` fixture, dependents first (same order as
        `test_db_identity._cleanup`, for the same foreign-key reason)."""
        with cursor(DSN) as cur:
            cur.execute(f"""DELETE FROM {SCHEMA}.messages WHERE chat_id IN
                            (SELECT id FROM {SCHEMA}.chats WHERE jid LIKE '1999%')""")
            cur.execute(f"""DELETE FROM {SCHEMA}.chats WHERE jid LIKE '1999%'""")
            cur.execute(f"""DELETE FROM {SCHEMA}.groups WHERE jid LIKE '1999%'""")
            cur.execute(f"""DELETE FROM {SCHEMA}.contacts
                            WHERE pn LIKE '1999%' OR lid LIKE '1999%'""")

    def _chat(self, cur, jid, chat_type, display_name=None, username=None):
        cur.execute(
            f"""INSERT INTO {SCHEMA}.chats (jid, chat_type, display_name, username)
                VALUES (%s, %s, %s, %s) RETURNING id""",
            (jid, chat_type, display_name, username))
        return cur.fetchone()["id"]

    def _message(self, cur, chat_id, whatsapp_id, *, from_me, push_name,
                 body="fixture body", minutes_ago=0):
        """Insert one message with an EXPLICIT timestamp.

        🔴 `minutes_ago` EXISTS BECAUSE `now()` INSIDE ONE TRANSACTION IS ONE
        VALUE — the first version of this fixture inserted every message at the
        same instant, so "the newest message" was a tie and the `from_me` filter
        could not be distinguished from an arbitrary row order. Each fixture is
        now ordered on purpose.
        """
        raw = {"event": {"Info": {"Chat": None, "PushName": push_name,
                                  "IsFromMe": from_me}}}
        cur.execute(
            f"""INSERT INTO {SCHEMA}.messages
                  (chat_id, whatsapp_id, from_me, kind, body, message_ts, raw)
                VALUES (%s, %s, %s, 'text', %s,
                        now() - make_interval(mins => %s), %s)""",
            (chat_id, whatsapp_id, from_me, body, minutes_ago,
             __import__("json").dumps(raw)))

    def _fixtures(self):
        with cursor(DSN) as cur:
            # (a) a LID-keyed DM where EVERY name source exists and they differ.
            lid_chat = self._chat(cur, LID_CHAT, "dm", OWN_NAME)
            cur.execute(f"""INSERT INTO {SCHEMA}.contacts (pn, lid, push_name)
                            VALUES ('19990000042', '199900000000042', %s)""",
                        (CONTACT_NAME,))
            # An INCOMING message: this is the one that names the counterpart.
            self._message(cur, lid_chat, "FIXTURELID1", from_me=False,
                          push_name=WIRE_NAME, minutes_ago=5)
            # 🔴 A from_me message carrying OUR name, and it is the NEWEST on this
            # chat on purpose — so a query that stopped filtering on `from_me`
            # would pick `OWN_NAME` HERE rather than depending on row order.
            self._message(cur, lid_chat, "FIXTURELID2", from_me=True,
                          push_name=OWN_NAME, minutes_ago=1)

            # (b) a PN-keyed DM with a contact row and NO wire name — the shape
            # the live archive is mostly made of.
            pn_chat = self._chat(cur, PN_CHAT, "dm", OWN_NAME)
            cur.execute(f"""INSERT INTO {SCHEMA}.contacts (pn, lid, push_name)
                            VALUES ('19990000099', NULL, %s)""", (PN_NAME,))
            self._message(cur, pn_chat, "FIXTUREPN1", from_me=True,
                          push_name=OWN_NAME)

            # (c) a DM whose only name is the chat's stable username.
            un_chat = self._chat(cur, USERNAME_CHAT, "dm", None, "the.handle")
            self._message(cur, un_chat, "FIXTUREUN1", from_me=False, push_name=None)

            # (d) the CROSS-SERVER collision: a LID that numerically equals a
            # PN-keyed chat's local part, belonging to a DIFFERENT person.
            col_chat = self._chat(cur, COLLIDE_CHAT, "dm", "A PN CHAT")
            cur.execute(f"""INSERT INTO {SCHEMA}.contacts (pn, lid, push_name)
                            VALUES (NULL, '19990000055', 'WRONG PERSON')""")
            self._message(cur, col_chat, "FIXTURECOL1", from_me=True, push_name=None)

            # (e) a group, to pin that the DM rule does not reach it.
            grp_chat = self._chat(cur, GROUP_CHAT, "group", "The Group")
            self._message(cur, grp_chat, "FIXTUREGRP1", from_me=False,
                          push_name="A MEMBER", body=BODY_MARKER)

            # one searchable body on a DM too — INCOMING, and newer than LID1 but
            # OLDER than the from_me row above, so the wire name is still WIRE_NAME.
            self._message(cur, lid_chat, "FIXTURELID3", from_me=False,
                          push_name=WIRE_NAME, body=BODY_MARKER, minutes_ago=2)

    def _one(self, jid):
        rows = [r for r in self.db.conversations(500) if r["jid"] == jid]
        self.assertEqual(len(rows), 1, f"expected exactly one row for {jid}: {rows}")
        return rows[0]

    # -- tests -------------------------------------------------------------
    def test_conversations_names_a_dm_after_the_counterpart_not_the_chat_column(self):
        """THE REGRESSION PIN, end to end.

        On the pre-change code `conversations()` returned `chats.display_name`,
        which on the live archive reads `'Our Own Push Name'` — OUR own name — for a chat whose
        counterpart is somebody else. Here the chat column is `OWN_NAME` and the
        answer must be the name that belongs to the counterpart.
        """
        self.assertEqual(self._one(PN_CHAT)["display_name"], PN_NAME)
        self.assertNotEqual(self._one(PN_CHAT)["display_name"], OWN_NAME)

    def test_the_wire_push_name_wins_through_the_query_not_just_the_resolver(self):
        """An INCOMING message names the counterpart; a `from_me` message names us.

        Both are stored on the same chat, so this also fails if the query stops
        filtering `from_me` — order alone would then depend on which row is
        newest, and `OWN_NAME` is the newest here on purpose.
        """
        self.assertEqual(self._one(LID_CHAT)["display_name"], WIRE_NAME)

    def test_the_chats_stable_username_is_used_when_nothing_else_has_a_name(self):
        self.assertEqual(self._one(USERNAME_CHAT)["display_name"], "the.handle")

    def test_a_group_keeps_its_stored_name_and_its_rows_carry_it_too(self):
        self.assertEqual(self._one(GROUP_CHAT)["display_name"], "The Group")
        rows = self.db.show(GROUP_CHAT, 5)
        self.assertTrue(rows)
        for row in rows:
            self.assertEqual(row["display_name"], "The Group")

    def test_the_contact_join_does_not_cross_the_PN_LID_server_boundary(self):
        """A number that is one person's PN must not name another person's chat.

        `contacts.pn`/`contacts.lid` are bare addresses, so the join compares the
        SERVER part as well. Without that, the fixture in (d) — a contact whose
        LID numerically equals a PN-keyed chat's local part — would inject
        `'WRONG PERSON'` into a chat it has nothing to do with.
        """
        self.assertEqual(self._one(COLLIDE_CHAT)["display_name"], "A PN CHAT")

    def test_show_names_the_chat_it_is_showing(self):
        """🔴 THE BUG THIS FILE EXISTS FOR: `show()` did not SELECT `chat_type`,
        so the resolver took its non-DM branch and returned None for every row."""
        rows = self.db.show(LID_CHAT, 10)
        self.assertTrue(rows, "fixture chat has no messages")
        for row in rows:
            self.assertEqual(row["display_name"], WIRE_NAME)

    def test_search_names_the_chat_it_found(self):
        hits = [h for h in self.db.search(BODY_MARKER, 50)
                if h["jid"] in (LID_CHAT, GROUP_CHAT)]
        self.assertEqual(len(hits), 2, f"expected both fixture hits: {hits}")
        by_jid = {h["jid"]: h["display_name"] for h in hits}
        self.assertEqual(by_jid[LID_CHAT], WIRE_NAME)
        self.assertEqual(by_jid[GROUP_CHAT], "The Group")


if __name__ == "__main__":
    unittest.main()
