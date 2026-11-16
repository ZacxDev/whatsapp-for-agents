#!/usr/bin/env python3
"""Hermetic tests for the outbound gate (`_wa_outbound`). No DB, no network.

Every guard here is paired with a test that FAILS when the guard is removed —
the approval-digest binding, the one-shot capability, and the length policy.
"""
from __future__ import annotations

import unittest

import _wa_outbound as ob


def _draft(**kw):
    base = {
        "id": 7,
        "to_jid": "19990000004@lid",
        "body": "Sounds good — see you at 6.",
        "state": ob.STATE_APPROVED,
        "attachments": (),
    }
    base.update(kw)
    base["approval_digest"] = kw.get(
        "approval_digest", ob.draft_payload_digest(base))
    return base


class TestLengthPolicy(unittest.TestCase):
    def test_a_short_reply_passes(self):
        ob.enforce_length_policy("See you at 6.")

    def test_a_body_at_the_ceiling_passes(self):
        ob.enforce_length_policy("x" * ob.MAX_BODY_CHARS)

    def test_one_char_over_the_ceiling_is_refused(self):
        with self.assertRaises(ob.LengthPolicyError) as ctx:
            ob.enforce_length_policy("x" * (ob.MAX_BODY_CHARS + 1))
        self.assertIn("ATTACHMENT", str(ctx.exception))

    def test_an_attachment_lowers_the_body_ceiling_to_a_cover_note(self):
        att = [{"filename": "notes.md", "content_type": "text/markdown",
                "bytes": b"# detail\n"}]
        # Fine as a plain body, refused as a cover note.
        body = "x" * (ob.MAX_COVER_NOTE_CHARS + 1)
        self.assertLessEqual(len(body), ob.MAX_BODY_CHARS)
        ob.enforce_length_policy(body)
        with self.assertRaises(ob.LengthPolicyError):
            ob.enforce_length_policy(body, att)

    def test_the_refusal_names_the_constant_so_it_is_not_worked_around_blindly(self):
        with self.assertRaises(ob.LengthPolicyError) as ctx:
            ob.enforce_length_policy("x" * (ob.MAX_BODY_CHARS + 1))
        self.assertIn("MAX_BODY_CHARS", str(ctx.exception))

    def test_the_cover_note_ceiling_is_strictly_below_the_body_ceiling(self):
        # A RELATIONSHIP guard, not a value pin: if a later edit raised
        # MAX_COVER_NOTE_CHARS to or past MAX_BODY_CHARS, the attachment rule
        # would stop meaning anything while every other test stayed green.
        self.assertLess(ob.MAX_COVER_NOTE_CHARS, ob.MAX_BODY_CHARS)


class TestDigest(unittest.TestCase):
    def test_the_same_payload_hashes_the_same(self):
        a = ob.payload_digest(to_jid="a@s", body="hi")
        b = ob.payload_digest(to_jid="a@s", body="hi", attachments=())
        self.assertEqual(a, b)

    def test_a_text_only_digest_is_byte_identical_with_or_without_the_empty_tuple(self):
        # 🔴 Pins that the `attachments` key is OMITTED when empty: adding
        # "attachments": [] would change every text-only draft's digest.
        self.assertEqual(
            ob.payload_digest(to_jid="a@s", body="hi"),
            ob.payload_digest(to_jid="a@s", body="hi", attachments=None),
        )

    def test_body_recipient_and_attachment_each_change_the_digest(self):
        base = ob.payload_digest(to_jid="a@s", body="hi")
        self.assertNotEqual(base, ob.payload_digest(to_jid="b@s", body="hi"))
        self.assertNotEqual(base, ob.payload_digest(to_jid="a@s", body="hi!"))
        withatt = ob.payload_digest(
            to_jid="a@s", body="hi",
            attachments=[("n.md", "text/markdown", "a" * 64, 3)])
        self.assertNotEqual(base, withatt)

    def test_attachment_ORDER_does_not_change_the_digest(self):
        one = ("a.md", "text/markdown", "a" * 64, 1)
        two = ("b.md", "text/markdown", "b" * 64, 2)
        self.assertEqual(
            ob.payload_digest(to_jid="a@s", body="hi", attachments=[one, two]),
            ob.payload_digest(to_jid="a@s", body="hi", attachments=[two, one]),
        )

    def test_the_FILENAME_is_part_of_the_digest(self):
        base = ob.payload_digest(
            to_jid="a@s", body="hi",
            attachments=[("invoice.md", "text/markdown", "a" * 64, 1)])
        renamed = ob.payload_digest(
            to_jid="a@s", body="hi",
            attachments=[("final.md", "text/markdown", "a" * 64, 1)])
        self.assertNotEqual(base, renamed)


class TestAttachments(unittest.TestCase):
    def test_a_row_with_no_readable_digest_fails_closed(self):
        with self.assertRaises(ob.AttachmentError):
            ob.canonical_attachments([("n.md", "text/markdown", None, 1)])

    def test_duplicate_filenames_are_refused(self):
        with self.assertRaises(ob.AttachmentError):
            ob.validated_attachments([
                {"filename": "n.md", "bytes": b"a"},
                {"filename": "n.md", "bytes": b"b"},
            ])

    def test_a_SQL_ROW_and_a_BYTES_MAPPING_fingerprint_IDENTICALLY(self):
        # The exact bug the first live send hit: the approve side reads tuples
        # from SQL, the send side loads mappings carrying bytes. If the two
        # disagree, `_mint_send_authorization()` mints and `spend_authorization()`
        # refuses, so EVERY send fails while every unit test stays green.
        data = b"# detail\n"
        digest = ob.attachment_sha256(data)
        from_sql = ob.canonical_attachments(
            [("n.md", "text/markdown", digest, len(data))])
        from_bytes = ob.canonical_attachments(
            [{"filename": "n.md", "content_type": "text/markdown", "bytes": data}])
        self.assertEqual(from_sql, from_bytes,
                         "the SQL-row and bytes-mapping shapes must fingerprint "
                         "identically or the mint/spend comparison always fails")

    def test_the_per_draft_cap_is_enforced(self):
        with self.assertRaises(ob.AttachmentError) as ctx:
            ob.validated_attachments([{
                "filename": "big.bin",
                "bytes": b"x" * (ob.MAX_DRAFT_ATTACHMENT_BYTES + 1),
            }])
        self.assertIn("MAX_DRAFT_ATTACHMENT_BYTES", str(ctx.exception))


class TestMint(unittest.TestCase):
    def test_a_pending_draft_cannot_mint(self):
        with self.assertRaises(ob.SendGateError) as ctx:
            ob._mint_send_authorization(_draft(state=ob.STATE_PENDING))
        self.assertIn("pending", str(ctx.exception))

    def test_an_approved_draft_with_a_matching_digest_mints(self):
        auth = ob._mint_send_authorization(_draft())
        self.assertIsInstance(auth, ob.SendAuthorization)

    def test_a_MISSING_digest_fails_closed(self):
        d = _draft()
        d["approval_digest"] = None
        with self.assertRaises(ob.SendGateError) as ctx:
            ob._mint_send_authorization(d)
        self.assertIn("NO approval digest", str(ctx.exception))

    def test_a_BODY_edited_after_approval_refuses_to_mint(self):
        d = _draft()
        d["body"] = "totally different"
        with self.assertRaises(ob.SendGateError) as ctx:
            ob._mint_send_authorization(d)
        self.assertIn("CHANGED after it was approved", str(ctx.exception))

    def test_an_ATTACHMENT_added_after_approval_refuses_to_mint(self):
        d = _draft()
        d["attachments"] = [("added.md", "text/markdown", "a" * 64, 5)]
        with self.assertRaises(ob.SendGateError):
            ob._mint_send_authorization(d)

    def test_the_capability_cannot_be_constructed_directly(self):
        with self.assertRaises(ob.SendGateError):
            ob.SendAuthorization()


class TestSpend(unittest.TestCase):
    def test_a_real_capability_spends_once(self):
        d = _draft()
        auth = ob._mint_send_authorization(d)
        ob.spend_authorization(auth, to_jid=d["to_jid"], body=d["body"],
                               attachments=d["attachments"])
        with self.assertRaises(ob.SendGateError) as ctx:
            ob.spend_authorization(auth, to_jid=d["to_jid"], body=d["body"],
                                   attachments=d["attachments"])
        self.assertIn("already been spent", str(ctx.exception))

    def test_a_forged_object_is_refused(self):
        class Fake:
            draft_id = 7
            to_jid = "a@s"
            body = "hi"
            attachments = ()
            _nonce = "deadbeef"
        with self.assertRaises(ob.SendGateError):
            ob.spend_authorization(Fake(), to_jid="a@s", body="hi", attachments=())

    def test_a_different_payload_is_refused(self):
        d = _draft()
        auth = ob._mint_send_authorization(d)
        with self.assertRaises(ob.SendGateError) as ctx:
            ob.spend_authorization(auth, to_jid=d["to_jid"], body="swapped",
                                   attachments=d["attachments"])
        self.assertIn("body changed", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
