#!/usr/bin/env python3
"""Parsing rules: JID normalisation, LID/PN identity, message classification.

Hermetic — `_wa_parse` imports nothing but stdlib.
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import _wa_parse as P  # noqa: E402


class TestJid(unittest.TestCase):
    def test_strips_device_and_agent_suffix(self):
        # whatsmeow reports device-qualified addresses on sync echoes.
        self.assertEqual(P.normalize_jid("5491155554444.0:52@s.whatsapp.net"),
                         "5491155554444@s.whatsapp.net")
        self.assertEqual(P.normalize_jid("5491155554444:12@s.whatsapp.net"),
                         "5491155554444@s.whatsapp.net")

    def test_group_jid_is_untouched(self):
        # Group JIDs contain '-' and no '.', so the agent-id rule must not fire.
        jid = "120363123456789012@g.us"
        self.assertEqual(P.normalize_jid(jid), jid)

    def test_lid_keeps_its_server(self):
        self.assertEqual(P.normalize_jid("123456789012345@lid"), "123456789012345@lid")

    def test_kinds(self):
        self.assertEqual(P.jid_kind("5491155554444@s.whatsapp.net"), "pn")
        self.assertEqual(P.jid_kind("123456789012345@lid"), "lid")
        self.assertEqual(P.jid_kind("120363123456789012@g.us"), "group")
        self.assertEqual(P.jid_kind("status@broadcast"), "broadcast")
        self.assertEqual(P.jid_kind(""), "unknown")
        self.assertEqual(P.jid_kind(None), "unknown")

    def test_chat_type(self):
        self.assertEqual(P.chat_type("5491155554444@s.whatsapp.net"), "dm")
        self.assertEqual(P.chat_type("123456789012345@lid"), "dm")
        self.assertEqual(P.chat_type("120363123456789012@g.us"), "group")
        self.assertEqual(P.chat_type("status@broadcast"), "broadcast")


class TestIdentity(unittest.TestCase):
    """The LID/PN dual-address rule. This is the module's highest-risk area."""

    def test_pn_only(self):
        got = P.identity_from_info({"Sender": "5491155554444@s.whatsapp.net"})
        self.assertEqual(got, {"pn": "5491155554444", "lid": None})

    def test_lid_only(self):
        got = P.identity_from_info({"Sender": "123456789012345@lid"})
        self.assertEqual(got, {"pn": None, "lid": "123456789012345"})

    def test_dual_address_yields_both_so_the_db_can_merge(self):
        got = P.identity_from_info({
            "Sender": "123456789012345@lid",
            "SenderAlt": "5491155554444@s.whatsapp.net",
        })
        self.assertEqual(got, {"pn": "5491155554444", "lid": "123456789012345"})

    def test_dual_address_other_order(self):
        got = P.identity_from_info({
            "Sender": "5491155554444@s.whatsapp.net",
            "SenderAlt": "123456789012345@lid",
        })
        self.assertEqual(got, {"pn": "5491155554444", "lid": "123456789012345"})

    def test_group_message_uses_participant_when_sender_is_absent(self):
        got = P.identity_from_info({"Participant": "5491155554444@s.whatsapp.net"})
        self.assertEqual(got["pn"], "5491155554444")

    def test_no_identity_at_all_is_not_an_error(self):
        self.assertEqual(P.identity_from_info({}), {"pn": None, "lid": None})


class TestTimestamps(unittest.TestCase):
    def test_rfc3339_z(self):
        got = P.parse_timestamp("2026-11-09T12:34:56Z")
        self.assertEqual(got.year, 2026)
        self.assertEqual(got.utcoffset().total_seconds(), 0)

    def test_epoch_milliseconds(self):
        got = P.parse_timestamp(1_700_000_000_000)
        self.assertEqual(int(got.timestamp()), 1_700_000_000)

    def test_garbage_returns_none_not_raises(self):
        for bad in ("", "not-a-date", None, object()):
            self.assertIsNone(P.parse_timestamp(bad))


class TestClassify(unittest.TestCase):
    def test_plain_text(self):
        got = P.classify({"conversation": "hello"})
        self.assertEqual((got["kind"], got["body"]), ("text", "hello"))

    def test_extended_text(self):
        got = P.classify({"extendedTextMessage": {"text": "hi there"}})
        self.assertEqual((got["kind"], got["body"]), ("text", "hi there"))

    def test_image_with_caption(self):
        got = P.classify({"imageMessage": {"caption": "look"}})
        self.assertEqual(got["kind"], "image")
        self.assertEqual(got["caption"], "look")

    def test_voice_note_is_distinguished_from_audio_file(self):
        self.assertEqual(P.classify({"pttMessage": {}})["kind"], "voice")
        self.assertEqual(P.classify({"audioMessage": {}})["kind"], "audio")

    def test_document_and_sticker(self):
        self.assertEqual(P.classify({"documentMessage": {}})["kind"], "document")
        self.assertEqual(P.classify({"stickerMessage": {}})["kind"], "sticker")

    def test_ephemeral_wrapper_is_unwrapped(self):
        got = P.classify({"ephemeralMessage": {"message": {"conversation": "secret"}}})
        self.assertEqual((got["kind"], got["body"]), ("text", "secret"))
        self.assertEqual(got["wrapper"], "ephemeralMessage")

    def test_view_once_wrapper_is_unwrapped_and_flagged(self):
        got = P.classify({"viewOnceMessage": {"message": {"imageMessage": {}}}})
        self.assertEqual(got["kind"], "image")
        self.assertTrue(got["wrapper"].startswith("viewOnce"))

    def test_nested_wrappers_terminate(self):
        payload = {"ephemeralMessage": {"message": {"viewOnceMessage": {"message": {"conversation": "x"}}}}}
        got = P.classify(payload)
        self.assertEqual((got["kind"], got["body"]), ("text", "x"))

    def test_unknown_shape_is_labelled_not_dropped(self):
        got = P.classify({"someFutureMessageType": {"a": 1}})
        self.assertEqual(got["kind"], "unknown")

    def test_empty_message(self):
        self.assertEqual(P.classify({})["kind"], "unknown")


class TestControlMessages(unittest.TestCase):
    def test_reaction(self):
        got = P.reaction_info({"reactionMessage": {
            "key": {"ID": "ABC"}, "text": "\U0001f44d"}})
        self.assertEqual(got["target_id"], "ABC")
        self.assertEqual(got["emoji"], "\U0001f44d")
        self.assertFalse(got["removed"])

    def test_reaction_removal_is_an_empty_text(self):
        got = P.reaction_info({"reactionMessage": {"key": {"ID": "ABC"}, "text": ""}})
        self.assertTrue(got["removed"])

    def test_revoke(self):
        got = P.protocol_info({"protocolMessage": {"type": 0, "key": {"ID": "XYZ"}}})
        self.assertTrue(got["is_revoke"])
        self.assertFalse(got["is_edit"])
        self.assertEqual(got["target_id"], "XYZ")

    def test_edit(self):
        got = P.protocol_info({"protocolMessage": {"type": 14, "key": {"ID": "XYZ"}}})
        self.assertTrue(got["is_edit"])
        self.assertFalse(got["is_revoke"])

    def test_non_control_message_yields_none(self):
        self.assertIsNone(P.reaction_info({"conversation": "x"}))
        self.assertIsNone(P.protocol_info({"conversation": "x"}))

    def test_quoted_id_from_context_info(self):
        got = P.quoted_id({"extendedTextMessage": {
            "text": "re", "contextInfo": {"stanzaID": "PARENT"}}})
        self.assertEqual(got, "PARENT")

    def test_quoted_id_absent(self):
        self.assertIsNone(P.quoted_id({"conversation": "x"}))


class TestHistoryShape(unittest.TestCase):
    """The REAL payload shape, taken from a live capture.

    🔴 These exist because the first parser read `event.conversations` and the
    payload actually nests them at `event.Data.conversations`. It reported ZERO
    conversations for every payload — including ones carrying thousands of
    messages — and the earlier tests passed, because they asserted the shape the
    author imagined rather than the shape that arrived.
    """

    def payload(self, *, sync_type=3, conversations=(), pushnames=()):
        data = {"syncType": sync_type}
        if conversations:
            data["conversations"] = list(conversations)
        if pushnames:
            data["pushnames"] = list(pushnames)
        return {"type": "HistorySync",
                "event": {"Data": data, "Notification": {}},
                "userID": "1", "instanceName": "test-instance"}

    def test_conversations_are_found_under_event_data(self):
        got = P.history_conversations(self.payload(conversations=[{"ID": "a@g.us"}]))
        self.assertEqual(got, [{"ID": "a@g.us"}])

    def test_a_push_name_payload_has_no_conversations(self):
        self.assertEqual(P.history_conversations(self.payload(sync_type=4)), [])

    def test_missing_event_does_not_raise(self):
        for bad in ({}, {"event": None}, {"event": {}}, {"event": {"Data": None}}):
            self.assertEqual(P.history_conversations(bad), [])

    def test_pushnames_are_harvested(self):
        got = P.pushnames(self.payload(sync_type=4, pushnames=[
            {"ID": "19990000008@s.whatsapp.net", "pushname": "Alice"},
            {"ID": "19990000009@s.whatsapp.net", "pushname": "alice"},
            {"ID": "1@s.whatsapp.net"},                      # no name -> skipped
        ]))
        self.assertEqual(got, {"19990000008@s.whatsapp.net": "Alice",
                               "19990000009@s.whatsapp.net": "alice"})

    def test_pushnames_on_a_payload_without_them(self):
        self.assertEqual(P.pushnames(self.payload()), {})


class TestFromWebMessage(unittest.TestCase):
    def wmi(self, *, chat, msg_id="M1", from_me=False, participant=None,
            timestamp=1794247443, body="hi"):
        key = {"ID": msg_id, "fromMe": from_me, "remoteJID": chat}
        if participant:
            key["participant"] = participant
        return {"key": key, "message": {"conversation": body},
                "messageTimestamp": timestamp}

    def test_dm_inbound_attributes_the_peer(self):
        got = P.from_web_message(self.wmi(chat="5491155554444@s.whatsapp.net"))
        info = got["event"]["Info"]
        self.assertEqual(info["Chat"], "5491155554444@s.whatsapp.net")
        self.assertEqual(info["Sender"], "5491155554444@s.whatsapp.net")
        self.assertFalse(info["IsFromMe"])

    def test_group_inbound_attributes_the_participant(self):
        got = P.from_web_message(self.wmi(
            chat="120363123456789012@g.us", participant="19990000005@lid"))
        info = got["event"]["Info"]
        self.assertEqual(info["Chat"], "120363123456789012@g.us")
        self.assertEqual(info["Sender"], "19990000005@lid")

    def test_outbound_history_row_gets_NO_sender_rather_than_the_recipient(self):
        # The trap this guards: `Chat` is the PEER, so using it as the sender would
        # name the recipient as the author of a message the account sent.
        got = P.from_web_message(self.wmi(chat="5491155554444@s.whatsapp.net", from_me=True))
        self.assertNotIn("Sender", got["event"]["Info"])
        self.assertTrue(got["event"]["Info"]["IsFromMe"])

    def test_outbound_row_uses_the_account_when_supplied(self):
        got = P.from_web_message(
            self.wmi(chat="5491155554444@s.whatsapp.net", from_me=True),
            account_jid="19990000003@s.whatsapp.net")
        self.assertEqual(got["event"]["Info"]["Sender"], "19990000003@s.whatsapp.net")

    def test_falls_back_to_the_conversation_id(self):
        got = P.from_web_message({"key": {"ID": "M9"}, "message": {}},
                                 fallback_chat="120363123456789012@g.us")
        self.assertEqual(got["event"]["Info"]["Chat"], "120363123456789012@g.us")

    def test_push_name_falls_through(self):
        got = P.from_web_message(self.wmi(chat="1@s.whatsapp.net"), push_name="Alice")
        self.assertEqual(got["event"]["Info"]["PushName"], "Alice")


class TestTimestampEpochStrings(unittest.TestCase):
    def test_numeric_string_is_an_epoch_not_a_date(self):
        # protojson renders int64 as a STRING. Without this branch fromisoformat
        # raises, the caller falls back to now(), and backfilled history silently
        # gets today's date while looking perfectly fine.
        got = P.parse_timestamp("1794247443")
        self.assertIsNotNone(got)
        self.assertEqual(int(got.timestamp()), 1794247443)

    def test_epoch_seconds_int(self):
        self.assertEqual(int(P.parse_timestamp(1794247443).timestamp()), 1794247443)

    def test_epoch_milliseconds_int(self):
        self.assertEqual(int(P.parse_timestamp(1794247443000).timestamp()), 1794247443)


class TestStripBlobs(unittest.TestCase):
    def test_base64_removed_from_top_level_and_event(self):
        payload = {"base64": "A" * 100, "event": {"base64": "B" * 100, "Info": {"ID": "1"}}}
        got = P.strip_blobs(payload)
        self.assertNotIn("base64", got)
        self.assertNotIn("base64", got["event"])
        self.assertEqual(got["event"]["Info"]["ID"], "1")

    def test_non_dict_is_tolerated(self):
        self.assertEqual(P.strip_blobs(None), {})


if __name__ == "__main__":
    unittest.main()