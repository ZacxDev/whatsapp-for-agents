#!/usr/bin/env python3
"""The SEND half and the INGEST half must agree about the ARCHIVE.

Repo-only, by necessity: this is the one test that needs `_wa_outbound` (a send
module) and `_wa_parse` (an ingest module) in the same process, and the whole
point of the image split is that neither image holds both. So it is deliberately
not COPYed into either Dockerfile — same as `test_image_split.py`.

WHY IT EXISTS
-------------
wuzapi does not echo its own API sends back as webhooks, so the sender writes the
archive row for what it sent. That row is then read back through the CONSUMER's
query paths (`search`, `show`, `conversations`), which is a seam nobody owns: a
send row shaped differently from an ingest row is silently unsearchable rather
than visibly broken.

`body` is the load-bearing column — `whatsapp.messages.search` is a generated
tsvector over `coalesce(body, '')` — so a caption kept out of `body` would make
our own sent documents unfindable.
"""
from __future__ import annotations

import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import _wa_outbound as ob  # noqa: E402
import _wa_parse as P  # noqa: E402

SENT_AT = datetime(2026, 11, 12, 12, 0, tzinfo=timezone.utc)
MSG_ID = "3EB0F2CCC45B174EC322BC"


def _row(**kw):
    base = dict(draft_id=4, to_jid="19990000003@s.whatsapp.net",
                body="Sounds good — see you at 6.", whatsapp_id=MSG_ID,
                sent_at=SENT_AT, attachments=())
    base.update(kw)
    return ob.outbound_archive_row(**base)


class TestChatTypeAgreesWithTheParser(unittest.TestCase):
    """The sender cannot import `_wa_parse` (image split), so it carries its own
    copy of the JID -> chat_type rule. This is the pin that stops the two
    drifting into writing rows the consumer's queries cannot group."""

    JIDS = [
        "19990000003@s.whatsapp.net",      # dm, phone-number address
        "19990000004@lid",              # dm, LID address
        "120363123456789012@g.us",         # group
        "status@broadcast",                # broadcast
        "123456789012345678@newsletter",   # newsletter
        "nonsense",                        # unparseable
        "",
    ]

    def test_every_jid_classifies_IDENTICALLY(self):
        for jid in self.JIDS:
            with self.subTest(jid=jid):
                self.assertEqual(ob.archive_chat_type(jid), P.chat_type(jid))

    def test_the_table_EXERCISES_more_than_one_answer(self):
        # POSITIVE CONTROL. A stub returning a constant would pass the loop
        # above against an equally-constant parser, so prove the table has
        # spread and that the comparison can distinguish values.
        answers = {ob.archive_chat_type(j) for j in self.JIDS}
        self.assertGreater(len(answers), 2, f"the table is degenerate: {answers}")

    def test_the_copy_is_not_just_the_parser_imported(self):
        # NEGATIVE CONTROL for the split itself: if `_wa_outbound` imported
        # `_wa_parse`, the sender image would fail to build — but the guard is
        # that this module must not MERELY re-export it either, which would
        # hide the duplication from the test above.
        self.assertFalse(hasattr(ob, "_wa_parse"))


class TestATextSendLandsInTheArchiveShape(unittest.TestCase):
    def test_it_is_from_me_and_keyed_on_the_wuzapi_id(self):
        row = _row()
        self.assertTrue(row["from_me"])
        self.assertEqual(row["whatsapp_id"], MSG_ID)
        self.assertEqual(row["message_ts"], SENT_AT)

    def test_no_contact_row_is_invented_for_the_operator(self):
        # A `from_me` author is the linked account; giving it a contact row
        # would enter the PN/LID merge machinery that exists for OTHER people.
        self.assertIsNone(_row()["sender_contact_id"])

    def test_its_kind_body_and_caption_match_the_INGEST_row_for_the_same_message(self):
        body = "Sounds good — see you at 6."
        row = _row(body=body)
        parsed = P.classify({"conversation": body})
        self.assertEqual(row["kind"], parsed["kind"])
        self.assertEqual(row["body"], parsed["body"])
        self.assertEqual(row["caption"], parsed["caption"])

    def test_the_body_is_present_so_the_archive_can_SEARCH_it(self):
        # `messages.search` is generated from `coalesce(body, '')`.
        self.assertEqual(_row()["body"], "Sounds good — see you at 6.")


class TestAnAttachmentSendLandsInTheArchiveShape(unittest.TestCase):
    ATTS = [{"filename": "notes.md", "content_type": "text/markdown",
             "bytes": b"# notes\nhello\n"}]

    def test_its_kind_and_columns_match_the_INGEST_row_for_a_captioned_document(self):
        body = "details attached"
        row = _row(body=body, attachments=self.ATTS)
        parsed = P.classify({"documentMessage": {
            "caption": body, "fileName": "notes.md", "mimetype": "text/markdown"}})
        self.assertEqual(row["kind"], parsed["kind"])
        self.assertEqual(row["body"], parsed["body"])
        self.assertEqual(row["caption"], parsed["caption"])

    def test_the_caption_is_in_BODY_too_so_a_sent_document_stays_searchable(self):
        body = "details attached"
        row = _row(body=body, attachments=self.ATTS)
        self.assertEqual(row["body"], body)

    def test_the_attachment_row_names_the_exact_bytes_and_says_why_it_is_not_stored(self):
        rows = ob.outbound_attachment_rows("document", self.ATTS)
        self.assertEqual(len(rows), 1)
        a = rows[0]
        self.assertEqual(a["filename"], "notes.md")
        self.assertEqual(a["mimetype"], "text/markdown")
        self.assertEqual(a["file_size"], len(self.ATTS[0]["bytes"]))
        # The fingerprint of the bytes we actually sent — a LITERAL, so a change
        # that starts hashing something else (the row, the path, nothing) reds.
        self.assertEqual(
            a["sha256"],
            "8756865da2e3eeb49046a0e58453f09735a5f8bdd5ff6f06d9f1253b8612e034")
        self.assertIsNone(a["minio_key"])
        self.assertIn("skipped: outbound", a["download_error"])


if __name__ == "__main__":
    unittest.main()