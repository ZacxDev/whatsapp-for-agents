#!/usr/bin/env python3
"""The READ PATH's naming rule: which name a chat row SHOWS.

WHY A PURE-PYTHON TEST OF A FUNCTION THE QUERIES ONLY CALL
---------------------------------------------------------
The rule is an ORDER over candidate columns, and that order IS the behaviour. As
a SQL `CASE` it could only be exercised against a live Postgres, which the gate
never runs (see `test_db_identity.py`'s docstring) — so the ordering lives in
`_wa_db._resolve_chat_name` and the queries only FETCH the candidates. This file
pins the order; `test_read_path_db.py` pins that each query fetches them.

🔴 WHAT THIS FILE STRUCTURALLY CANNOT SEE, so that a green run is not read as
more than it is: it does not exercise the SQL. A query that forgot to select
`chat_type` would raise at runtime (pinned below) but this file would still pass.
Only `test_read_path_db.py` covers the queries, and only against a real
Postgres.

The regression pin is `test_the_contact_row_outranks_the_chats_own_columns`: on
the archive of 2026-11-13 that chat's `chats.display_name` was `'Our Own Push Name'` — OUR own
name — while the counterpart's own messages name them differently. Fixture values
here are deliberately neutral: a counterpart's real name is chat content and does
not belong in a public repo; the same goes for counterpart phone numbers and
LIDs beyond the synthetic shapes below.

🔴 NOT the `WHATSAPP_REAL_NAME` conflation: `'Our Own Push Name'` stays because it is OUR push
name and the whole defect is that OUR name was written onto someone else's chat.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

try:
    import psycopg2  # noqa: F401  (_wa_db imports it)
except ImportError:  # pragma: no cover
    raise unittest.SkipTest("psycopg2 not installed")

# 🔴 DELIBERATELY OUTSIDE THE GUARD ABOVE. `_wa_db` not exposing these names is a
# REAL FAILURE — it is the fix this module exists to pin — and a broad
# `except ImportError` around it would report a REVERTED module as a SKIP, leaving
# the suite green. Only a genuinely absent psycopg2 skips.
from _wa_db import DM_NAME_SOURCES, _resolve_chat_name


def row(**kw):
    """A `conversations()` row shape with every candidate EMPTY by default, so a
    test that sets one source proves that source and not a leftover."""
    base = dict(chat_type="dm", jid="1999000000300000@lid",
                chats_display_name=None, chats_username=None,
                contact_push_name=None, contact_pn=None, wire_push_name=None)
    base.update(kw)
    return base


class TestTheOrder(unittest.TestCase):
    def test_a_wire_push_name_outranks_every_stored_column(self):
        """An incoming message names the counterpart; the stored columns do not.

        Live 2026-11-13, a 42-in / 42-out chat: `chats.display_name` AND
        `contacts.push_name` both read `'Our Own Push Name'` (our own push name, written by a
        `from_me` message) while the counterpart's OWN messages name them
        something else. Shipping `Our Own Push Name` there would be a confidently wrong
        answer, i.e. worse than the raw LID this change exists to remove.
        """
        self.assertEqual(
            _resolve_chat_name(row(jid="1999000000300001@lid",
                                   chats_display_name="Our Own Push Name",
                                   contact_push_name="Our Own Push Name",
                                   wire_push_name="Their Wire Name")),
            "Their Wire Name")

    def test_the_contact_row_outranks_the_chats_own_columns(self):
        """THE REGRESSION PIN. Live, the busiest chat in the archive (1,207 messages):
        `chats.display_name` is `'Our Own Push Name'` (ours) while the counterpart's own
        message carries their name. Before this change `conversations()`
        returned `'Our Own Push Name'`."""
        self.assertEqual(
            _resolve_chat_name(row(jid="1999000000300002@lid",
                                   chats_display_name="Our Own Push Name",
                                   chats_username="their.stable.handle",
                                   contact_push_name="Their Contact Name ")),
            "Their Contact Name")   # the stored value is trimmed on the way out

    def test_a_stable_username_outranks_the_chat_display_name(self):
        # `chats.username` is written ONLY from a history conversation's
        # `username`, so unlike `chats.display_name` it cannot hold our own name.
        self.assertEqual(
            _resolve_chat_name(row(chats_display_name="Our Own Push Name",
                                   chats_username="lorenauchoffen")),
            "lorenauchoffen")

    def test_the_chat_display_name_is_the_last_NAME_and_still_beats_a_number(self):
        # Live: no contact row name at all, and the chat column holds a handle —
        # A `@handle` in the chat column is the only name there is — so a number
        # must not win here.
        self.assertEqual(
            _resolve_chat_name(row(jid="5491155554444@s.whatsapp.net",
                                   chats_display_name="@theirhandle",
                                   contact_pn="5491155554444")),
            "@theirhandle")


class TestThePhoneNumberFallback(unittest.TestCase):
    def test_a_known_PN_is_shown_when_the_chat_is_keyed_by_a_LID(self):
        # A bare LID is unreadable; the PN it maps to is not.
        self.assertEqual(
            _resolve_chat_name(row(jid="1999000000300003@lid",
                                   contact_pn="12045550100")),
            "12045550100")

    def test_a_known_PN_is_NOT_shown_when_the_chat_jid_already_IS_that_number(self):
        # Printing the same number twice on one line is noise, not a name.
        self.assertIsNone(
            _resolve_chat_name(row(jid="12045550101@s.whatsapp.net",
                                   contact_pn="12045550101")))

    def test_a_dm_with_no_name_and_no_PN_is_left_nameless(self):
        # Exactly today's behaviour: no invented name, the jid still identifies it.
        self.assertIsNone(_resolve_chat_name(row()))


class TestOtherChatTypes(unittest.TestCase):
    def test_a_group_keeps_its_stored_name(self):
        self.assertEqual(
            _resolve_chat_name(row(chat_type="group", jid="120363123456789012@g.us",
                                   chats_display_name="Test Group",
                                   contact_push_name="not a group name",
                                   contact_pn="5491155554444")),
            "Test Group")

    def test_an_incoming_member_push_name_does_not_leak_into_a_group_name(self):
        """In a group, an incoming `PushName` is a MEMBER's — the DM rule must not
        reach it, and this is the pin that the branch is on `chat_type` and not on
        "does this row have a wire name"."""
        self.assertEqual(
            _resolve_chat_name(row(chat_type="group", jid="120363123456789012@g.us",
                                   chats_display_name="Test Group",
                                   wire_push_name="Some Member")),
            "Test Group")

    def test_other_types_are_returned_as_stored(self):
        for chat_type in ("broadcast", "newsletter", "unknown"):
            with self.subTest(chat_type=chat_type):
                self.assertEqual(
                    _resolve_chat_name(row(chat_type=chat_type,
                                           chats_display_name="WhatsApp",
                                           wire_push_name="ignored")),
                    "WhatsApp")

    def test_a_MISSING_chat_type_is_LOUD_rather_than_silently_a_group(self):
        """🔴 THE SEAM PIN. `show()` was missing `c.chat_type` in its SELECT list
        and the first version of this resolver — which defaulted a missing key to
        "not a dm" — silently returned `None` for every chat it could name. The
        queries all select it now, and a fourth one that does not must fail here
        rather than quietly fall back to the contaminated column."""
        incomplete = row()
        del incomplete["chat_type"]
        with self.assertRaises(KeyError):
            _resolve_chat_name(incomplete)


class TestTheTableIsNotDegenerate(unittest.TestCase):
    """POSITIVE CONTROL. Every test above sets one source and reads one answer;
    a resolver that returned a constant would pass none of them, but a resolver
    that IGNORED a source (e.g. dropped `wire_push_name` from the tuple) would
    still pass the tests that do not set it. So: prove each source can win, and
    that the answers are distinct."""

    def test_every_named_source_can_be_the_winner(self):
        winners = {
            "wire_push_name": _resolve_chat_name(row(wire_push_name="W")),
            "contact_push_name": _resolve_chat_name(row(contact_push_name="C")),
            "chats_username": _resolve_chat_name(row(chats_username="U")),
            "chats_display_name": _resolve_chat_name(row(chats_display_name="D")),
        }
        for source, got in winners.items():
            with self.subTest(source=source):
                self.assertIsNotNone(got, f"{source} never wins — dead source")
        self.assertEqual(set(winners.values()), {"W", "C", "U", "D"})

    def test_the_declared_source_tuple_matches_the_winners_asserted_above(self):
        # If a source is added to the tuple, the control above must be widened
        # too — this is how that is noticed instead of forgotten.
        self.assertEqual(DM_NAME_SOURCES,
                         ("wire_push_name", "contact_push_name",
                          "chats_username", "chats_display_name"))

    def test_whitespace_is_not_a_name(self):
        """The queries trim with `btrim`/`NULLIF` where they can, but a value that
        is only spaces must not win — a blank label reads as "this chat has no
        name" while looking like it was resolved."""
        self.assertEqual(
            _resolve_chat_name(row(wire_push_name="   ",
                                   contact_push_name="  Real Name  ")),
            "Real Name")
        self.assertIsNone(_resolve_chat_name(row(chats_display_name="   ")))


if __name__ == "__main__":
    unittest.main()
